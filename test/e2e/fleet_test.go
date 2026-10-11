//go:build e2e

package e2e

import (
	"context"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	namespacePrefix = "lameduck-e2e-"
	namespaceLabel  = "e2e"
	namespaceValue  = "lameduck"
	setName         = "e2e-app"
	appLabel        = "app"
	appImage        = "lameduck-e2e-app:e2e"
	appPort         = 8080
	appReadyPath    = "/readyz"
	appDrainDelay   = 5 * time.Second
	drainDeadline   = 2 * time.Minute
	minSidecarMinor = 29
	waitTimeout     = 3 * time.Minute
	clockSkewMargin = 2 * time.Second
)

type fleet struct {
	t         *testing.T
	client    client.WithWatch
	namespace string
	watcher   watch.Interface
	pods      map[string]*corev1.Pod
	deleted   map[string]deletion
}

type deletion struct {
	pod      *corev1.Pod
	observed time.Time
}

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

func newFleet(t *testing.T, replicas int32) *fleet {
	t.Helper()
	c := newClient(t)
	ctx := context.Background()

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		GenerateName: namespacePrefix,
		Labels:       map[string]string{namespaceLabel: namespaceValue},
	}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	t.Logf("using namespace %s", ns.Name)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("keeping namespace %s for inspection", ns.Name)
			return
		}
		_ = c.Delete(context.Background(), ns)
	})

	watcher, err := c.Watch(ctx, &corev1.PodList{},
		client.InNamespace(ns.Name),
		client.MatchingLabels{lameduckv1alpha1.LabelSet: setName},
	)
	if err != nil {
		t.Fatalf("watch pods: %v", err)
	}
	t.Cleanup(watcher.Stop)

	if err := c.Create(ctx, drainSet(ns.Name, replicas)); err != nil {
		t.Fatalf("create drain set: %v", err)
	}
	return &fleet{
		t:         t,
		client:    c,
		namespace: ns.Name,
		watcher:   watcher,
		pods:      map[string]*corev1.Pod{},
		deleted:   map[string]deletion{},
	}
}

func drainSet(namespace string, replicas int32) *lameduckv1alpha1.DrainSet {
	labels := map[string]string{appLabel: setName}
	deadline := &metav1.Duration{Duration: drainDeadline}
	return &lameduckv1alpha1.DrainSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: setName},
		Spec: lameduckv1alpha1.DrainSetSpec{
			Replicas: new(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            "app",
						Image:           appImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
								Path: appReadyPath,
								Port: intstr.FromInt32(appPort),
							}},
							PeriodSeconds: 1,
						},
					}},
				},
			},
			Drain: lameduckv1alpha1.DrainPolicy{
				Timeouts: lameduckv1alpha1.DrainTimeouts{
					Rollout:   deadline,
					ScaleDown: deadline,
					Eviction:  deadline,
					Manual:    deadline,
				},
			},
			Protection: lameduckv1alpha1.Protection{
				Evictions: lameduckv1alpha1.EvictionPolicyDrain,
				Deletes:   lameduckv1alpha1.DeletePolicyDrain,
			},
		},
	}
}

func (f *fleet) until(description string, done func() bool) {
	f.t.Helper()
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	for !done() {
		select {
		case event, ok := <-f.watcher.ResultChan():
			if !ok {
				f.t.Fatalf("pod watch closed while waiting for %s", description)
			}
			f.observe(event)
		case <-timer.C:
			f.t.Fatalf("timed out after %s waiting for %s", waitTimeout, description)
		}
	}
}

func (f *fleet) observe(event watch.Event) {
	f.t.Helper()
	if event.Type == watch.Error {
		f.t.Fatalf("pod watch failed: %v", apierrors.FromObject(event.Object))
	}
	pod, ok := event.Object.(*corev1.Pod)
	if !ok {
		return
	}
	if _, gone := f.deleted[pod.Name]; gone {
		return
	}
	if pod.DeletionTimestamp != nil || event.Type == watch.Deleted {
		delete(f.pods, pod.Name)
		f.deleted[pod.Name] = deletion{pod: pod, observed: time.Now()}
		return
	}
	f.pods[pod.Name] = pod
}

func (f *fleet) routablePods() []*corev1.Pod {
	var routable []*corev1.Pod
	for _, pod := range f.pods {
		if pod.Labels[lameduckv1alpha1.LabelPhase] == string(lameduckv1alpha1.PodPhaseActive) &&
			pod.Labels[lameduckv1alpha1.LabelRoutable] == "true" && podReady(pod) {
			routable = append(routable, pod)
		}
	}
	return routable
}

func (f *fleet) waitForRoutable(count int) []*corev1.Pod {
	f.t.Helper()
	f.until(strconv.Itoa(count)+" ready, active and routable pods", func() bool {
		return len(f.pods) == count && len(f.routablePods()) == count
	})
	return f.routablePods()
}

func (f *fleet) draining(reason lameduckv1alpha1.DrainReason) *corev1.Pod {
	for _, pod := range f.pods {
		if pod.Annotations[lameduckv1alpha1.AnnotationDrainReason] == string(reason) {
			return pod
		}
	}
	return nil
}

func (f *fleet) waitForDrain(reason lameduckv1alpha1.DrainReason) *corev1.Pod {
	f.t.Helper()
	var pod *corev1.Pod
	f.until("a pod draining for "+string(reason), func() bool {
		pod = f.draining(reason)
		return pod != nil
	})
	return pod
}

func (f *fleet) waitForDeletion(name string) {
	f.t.Helper()
	f.until("pod "+name+" to be deleted", func() bool {
		_, ok := f.deleted[name]
		return ok
	})
}

func (f *fleet) assertDrainedBeforeDeadline(name string, reason lameduckv1alpha1.DrainReason) {
	f.t.Helper()
	gone := f.deleted[name]
	pod := gone.pod
	if got := pod.Annotations[lameduckv1alpha1.AnnotationDrainReason]; got != string(reason) {
		f.t.Fatalf("deleted pod %s drain reason = %q, want %q", name, got, reason)
	}
	if pod.Labels[lameduckv1alpha1.LabelRoutable] != "false" {
		f.t.Fatalf("pod %s was still routable when it was deleted", name)
	}
	started, err := time.Parse(time.RFC3339, pod.Annotations[lameduckv1alpha1.AnnotationDrainStarted])
	if err != nil {
		f.t.Fatalf("parse drain start of %s: %v", name, err)
	}
	deadline, err := time.Parse(time.RFC3339, pod.Annotations[lameduckv1alpha1.AnnotationDrainDeadline])
	if err != nil {
		f.t.Fatalf("parse drain deadline of %s: %v", name, err)
	}
	deletedAt := gone.observed
	if pod.DeletionTimestamp != nil {
		grace := time.Duration(ptr.Deref(pod.DeletionGracePeriodSeconds, 0)) * time.Second
		deletedAt = pod.DeletionTimestamp.Add(-grace)
	}
	if waited := deletedAt.Sub(started); waited < appDrainDelay-clockSkewMargin {
		f.t.Fatalf("pod %s deleted %s after its drain started; the app only reports drained after %s",
			name, waited, appDrainDelay)
	}
	if !deletedAt.Before(deadline) {
		f.t.Fatalf("pod %s deleted at %s, not before its drain deadline %s; it was killed, not drained",
			name, deletedAt, deadline)
	}
}

func (f *fleet) drainSet() *lameduckv1alpha1.DrainSet {
	f.t.Helper()
	var set lameduckv1alpha1.DrainSet
	key := client.ObjectKey{Namespace: f.namespace, Name: setName}
	if err := f.client.Get(context.Background(), key, &set); err != nil {
		f.t.Fatalf("get drain set: %v", err)
	}
	return &set
}

func (f *fleet) updateDrainSet(mutate func(set *lameduckv1alpha1.DrainSet)) {
	f.t.Helper()
	set := f.drainSet()
	original := set.DeepCopy()
	mutate(set)
	if err := f.client.Patch(context.Background(), set, client.MergeFrom(original)); err != nil {
		f.t.Fatalf("patch drain set: %v", err)
	}
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
