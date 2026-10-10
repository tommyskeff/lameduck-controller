//go:build e2e

package e2e

import (
	"context"
	"os/exec"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

const (
	namespace       = "lameduck-e2e"
	setName         = "drain-app"
	appImage        = "lameduck-e2e-app:e2e"
	appDrainDelay   = 5 * time.Second
	minSidecarMinor = 29
	readyTimeout    = 3 * time.Minute
	drainTimeout    = 3 * time.Minute
	pollInterval    = time.Second
	evictionTimeout = 5 * time.Minute
	clockSkewMargin = 2 * time.Second
	podSelector     = lameduckv1alpha1.LabelSet + "=" + setName
)

func newClient(t *testing.T) client.WithWatch {
	t.Helper()
	restConfig, err := ctrl.GetConfig()
	if err != nil {
		t.Fatalf("load kubeconfig: %v", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		t.Fatalf("create discovery client: %v", err)
	}
	version, err := discoveryClient.ServerVersion()
	if err != nil {
		t.Fatalf("read server version: %v", err)
	}
	minor, err := strconv.Atoi(version.Minor)
	if err != nil || version.Major != "1" || minor < minSidecarMinor {
		t.Fatalf("cluster runs Kubernetes %s.%s; native sidecars need 1.%d or later",
			version.Major, version.Minor, minSidecarMinor)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(lameduckv1alpha1.AddToScheme(scheme))
	c, err := client.NewWithWatch(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	return c
}

func createDrainSet(t *testing.T, c client.Client) {
	t.Helper()
	ctx := context.Background()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), ns)
	})

	labels := map[string]string{"app": setName}
	set := &lameduckv1alpha1.DrainSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: setName},
		Spec: lameduckv1alpha1.DrainSetSpec{
			Replicas: ptr.To[int32](1),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            "app",
						Image:           appImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
					}},
				},
			},
			Drain: lameduckv1alpha1.DrainPolicy{
				Timeouts: lameduckv1alpha1.DrainTimeouts{
					Eviction: &metav1.Duration{Duration: evictionTimeout},
				},
			},
			Protection: lameduckv1alpha1.Protection{
				Evictions: lameduckv1alpha1.EvictionPolicyDrain,
			},
		},
	}
	if err := c.Create(ctx, set); err != nil {
		t.Fatalf("create drain set: %v", err)
	}
}

func waitForActivePod(t *testing.T, c client.Client) corev1.Pod {
	t.Helper()
	deadline := time.Now().Add(readyTimeout)
	for {
		var list corev1.PodList
		err := c.List(context.Background(), &list,
			client.InNamespace(namespace),
			client.MatchingLabels{lameduckv1alpha1.LabelSet: setName},
		)
		if err != nil {
			t.Fatalf("list pods: %v", err)
		}
		for _, pod := range list.Items {
			if pod.Labels[lameduckv1alpha1.LabelPhase] == string(lameduckv1alpha1.PodPhaseActive) &&
				pod.Spec.NodeName != "" && pod.DeletionTimestamp == nil {
				return pod
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no active pod after %s", readyTimeout)
		}
		time.Sleep(pollInterval)
	}
}

func drainNode(ctx context.Context, node string) <-chan error {
	result := make(chan error, 1)
	go func() {
		output, err := exec.CommandContext(ctx, "kubectl", "drain", node,
			"--pod-selector="+podSelector,
			"--ignore-daemonsets",
			"--delete-emptydir-data",
			"--timeout="+drainTimeout.String(),
		).CombinedOutput()
		if err != nil {
			err = &drainError{err: err, output: string(output)}
		}
		result <- err
	}()
	return result
}

type drainError struct {
	err    error
	output string
}

func (e *drainError) Error() string {
	return e.err.Error() + ": " + e.output
}

func uncordon(t *testing.T, node string) {
	t.Helper()
	if output, err := exec.Command("kubectl", "uncordon", node).CombinedOutput(); err != nil {
		t.Logf("uncordon %s: %v: %s", node, err, output)
	}
}

func TestNodeDrainDrainsPodBeforeDeletingIt(t *testing.T) {
	c := newClient(t)
	createDrainSet(t, c)
	pod := waitForActivePod(t, c)
	t.Logf("draining node %s running pod %s", pod.Spec.NodeName, pod.Name)

	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout+time.Minute)
	defer cancel()
	watcher, err := c.Watch(ctx, &corev1.PodList{},
		client.InNamespace(namespace),
		client.MatchingFields{"metadata.name": pod.Name},
	)
	if err != nil {
		t.Fatalf("watch pod: %v", err)
	}
	defer watcher.Stop()

	t.Cleanup(func() { uncordon(t, pod.Spec.NodeName) })
	drained := drainNode(ctx, pod.Spec.NodeName)

	sawDrainBeforeDelete := false
	var deleting *corev1.Pod
	for deleting == nil {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				t.Fatal("pod watch closed before the pod was deleted")
			}
			if event.Type == watch.Error {
				t.Fatalf("pod watch failed: %v", apierrors.FromObject(event.Object))
			}
			observed, isPod := event.Object.(*corev1.Pod)
			if !isPod {
				continue
			}
			if observed.DeletionTimestamp != nil || event.Type == watch.Deleted {
				deleting = observed
				continue
			}
			if observed.Annotations[lameduckv1alpha1.AnnotationDrainReason] == string(lameduckv1alpha1.DrainReasonEviction) {
				sawDrainBeforeDelete = true
			}
		case err := <-drained:
			t.Fatalf("kubectl drain finished before the pod was deleted: %v", err)
		case <-ctx.Done():
			t.Fatal("pod was never deleted")
		}
	}

	if !sawDrainBeforeDelete {
		t.Fatal("pod was deleted without first being marked draining for the eviction")
	}
	reason := deleting.Annotations[lameduckv1alpha1.AnnotationDrainReason]
	if reason != string(lameduckv1alpha1.DrainReasonEviction) {
		t.Fatalf("deleted pod drain reason = %q, want %q", reason, lameduckv1alpha1.DrainReasonEviction)
	}
	started, err := time.Parse(time.RFC3339, deleting.Annotations[lameduckv1alpha1.AnnotationDrainStarted])
	if err != nil {
		t.Fatalf("parse drain start: %v", err)
	}
	deadline, err := time.Parse(time.RFC3339, deleting.Annotations[lameduckv1alpha1.AnnotationDrainDeadline])
	if err != nil {
		t.Fatalf("parse drain deadline: %v", err)
	}
	deletedAt := time.Now()
	if deleting.DeletionTimestamp != nil {
		grace := time.Duration(ptr.Deref(deleting.DeletionGracePeriodSeconds, 0)) * time.Second
		deletedAt = deleting.DeletionTimestamp.Add(-grace)
	}
	if waited := deletedAt.Sub(started); waited < appDrainDelay-clockSkewMargin {
		t.Fatalf("pod deleted %s after its drain started; the app only reports drained after %s", waited, appDrainDelay)
	}
	if !deletedAt.Before(deadline) {
		t.Fatalf("pod deleted at %s, not before its drain deadline %s; it was killed, not drained", deletedAt, deadline)
	}

	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("kubectl drain: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("kubectl drain did not finish")
	}
}
