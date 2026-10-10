package fleet

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

const controllerNamespace = "lameduck-system"

func controllerPod(namespace, name string, leader bool) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      name,
		Labels:    map[string]string{"app.kubernetes.io/name": "lameduck"},
	}}
	if leader {
		pod.Labels[lameduckv1alpha1.LabelLeader] = "true"
	}
	return pod
}

func newLabeler(objects ...client.Object) (*LeaderLabeler, client.Client) {
	c := fake.NewClientBuilder().WithObjects(objects...).Build()
	return &LeaderLabeler{
		Reader: c,
		Writer: c,
		Pod:    types.NamespacedName{Namespace: controllerNamespace, Name: "controller-a"},
		Log:    logr.Discard(),
	}, c
}

func isLeader(t *testing.T, c client.Client, namespace, name string) bool {
	t.Helper()
	var pod corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, &pod); err != nil {
		t.Fatalf("get pod %s: %v", name, err)
	}
	if pod.Labels["app.kubernetes.io/name"] != "lameduck" {
		t.Fatalf("pod %s lost its other labels: %v", name, pod.Labels)
	}
	_, ok := pod.Labels[lameduckv1alpha1.LabelLeader]
	return ok
}

func TestAcquireLabelsOwnPod(t *testing.T) {
	labeler, c := newLabeler(controllerPod(controllerNamespace, "controller-a", false))
	if err := labeler.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !isLeader(t, c, controllerNamespace, "controller-a") {
		t.Fatal("own pod is not labelled leader")
	}
}

func TestAcquireRemovesStaleLeaderLabels(t *testing.T) {
	labeler, c := newLabeler(
		controllerPod(controllerNamespace, "controller-a", false),
		controllerPod(controllerNamespace, "controller-b", true),
		controllerPod(controllerNamespace, "controller-c", false),
	)
	if err := labeler.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !isLeader(t, c, controllerNamespace, "controller-a") {
		t.Fatal("own pod is not labelled leader")
	}
	if isLeader(t, c, controllerNamespace, "controller-b") {
		t.Fatal("previous leader keeps its label")
	}
	if isLeader(t, c, controllerNamespace, "controller-c") {
		t.Fatal("unlabelled pod gained the leader label")
	}
}

func TestAcquireKeepsExistingOwnLabel(t *testing.T) {
	labeler, c := newLabeler(controllerPod(controllerNamespace, "controller-a", true))
	if err := labeler.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !isLeader(t, c, controllerNamespace, "controller-a") {
		t.Fatal("own pod is not labelled leader")
	}
}

func TestAcquireIgnoresOtherNamespaces(t *testing.T) {
	labeler, c := newLabeler(
		controllerPod(controllerNamespace, "controller-a", false),
		controllerPod("other", "controller-b", true),
	)
	if err := labeler.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !isLeader(t, c, "other", "controller-b") {
		t.Fatal("leader label removed from a pod in another namespace")
	}
}

func TestAcquireFailsWhenOwnPodIsMissing(t *testing.T) {
	labeler, _ := newLabeler()
	if err := labeler.Acquire(context.Background()); err == nil {
		t.Fatal("acquire succeeded without its own pod")
	}
}

func TestReleaseRemovesOwnLabel(t *testing.T) {
	labeler, c := newLabeler(controllerPod(controllerNamespace, "controller-a", true))
	if err := labeler.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if isLeader(t, c, controllerNamespace, "controller-a") {
		t.Fatal("own pod keeps the leader label")
	}
}

func TestReleaseIgnoresMissingPod(t *testing.T) {
	labeler, _ := newLabeler()
	if err := labeler.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartLabelsUntilStopped(t *testing.T) {
	labeler, c := newLabeler(
		controllerPod(controllerNamespace, "controller-a", false),
		controllerPod(controllerNamespace, "controller-b", true),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- labeler.Start(ctx)
	}()

	err := wait.PollUntilContextTimeout(context.Background(), 10*time.Millisecond, 5*time.Second, true,
		func(context.Context) (bool, error) {
			return isLeader(t, c, controllerNamespace, "controller-a"), nil
		})
	if err != nil {
		t.Fatalf("own pod never labelled leader: %v", err)
	}
	if isLeader(t, c, controllerNamespace, "controller-b") {
		t.Fatal("previous leader keeps its label")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if isLeader(t, c, controllerNamespace, "controller-a") {
		t.Fatal("own pod keeps the leader label after stopping")
	}
}
