//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

const (
	statusTimeout = time.Minute
	pollInterval  = time.Second
	rolloutEnv    = "E2E_ROLLOUT"
)

func TestSidecarConnectsAndPodBecomesRoutable(t *testing.T) {
	t.Parallel()
	f := newFleet(t, 1)
	pod := f.waitForRoutable(1)[0]

	sidecarStarted := false
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name == lameduckv1alpha1.SidecarContainerName {
			sidecarStarted = ptr.Deref(status.Started, false) && status.State.Running != nil
		}
	}
	if !sidecarStarted {
		t.Fatalf("pod %s is routable but its %s container is not running", pod.Name, lameduckv1alpha1.SidecarContainerName)
	}

	deadline := time.Now().Add(statusTimeout)
	for {
		status := f.drainSet().Status
		if status.ReadyReplicas == 1 && status.AvailableReplicas == 1 && status.RoutableReplicas == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("drain set status never reported the pod: %+v", status)
		}
		time.Sleep(pollInterval)
	}
}

func TestScaleDownDrainsOnePod(t *testing.T) {
	t.Parallel()
	f := newFleet(t, 2)
	f.waitForRoutable(2)

	f.updateDrainSet(func(set *lameduckv1alpha1.DrainSet) {
		set.Spec.Replicas = ptr.To[int32](1)
	})
	victim := f.waitForDrain(lameduckv1alpha1.DrainReasonScaleDown)
	t.Logf("scale-down is draining pod %s", victim.Name)
	f.waitForDeletion(victim.Name)
	f.assertDrainedBeforeDeadline(victim.Name, lameduckv1alpha1.DrainReasonScaleDown)
	f.waitForRoutable(1)
}

func TestDeleteDrainsPodInstead(t *testing.T) {
	t.Parallel()
	f := newFleet(t, 1)
	pod := f.waitForRoutable(1)[0]

	err := f.client.Delete(context.Background(), pod)
	if !apierrors.IsForbidden(err) {
		t.Fatalf("delete pod %s: got %v, want the delete webhook to deny it", pod.Name, err)
	}
	f.until("pod "+pod.Name+" draining for the delete", func() bool {
		current, ok := f.pods[pod.Name]
		return ok && current.Annotations[lameduckv1alpha1.AnnotationDrainReason] == string(lameduckv1alpha1.DrainReasonManual)
	})
	f.waitForDeletion(pod.Name)
	f.assertDrainedBeforeDeadline(pod.Name, lameduckv1alpha1.DrainReasonManual)
	f.waitForRoutable(1)
}

func TestRolloutReplacesPodBeforeDrainingIt(t *testing.T) {
	t.Parallel()
	f := newFleet(t, 1)
	old := f.waitForRoutable(1)[0]
	oldRevision := old.Labels[lameduckv1alpha1.LabelRevision]

	f.updateDrainSet(func(set *lameduckv1alpha1.DrainSet) {
		container := &set.Spec.Template.Spec.Containers[0]
		container.Env = append(container.Env, corev1.EnvVar{Name: rolloutEnv, Value: "1"})
	})

	var replacementReady bool
	f.until("pod "+old.Name+" draining for the rollout", func() bool {
		for _, pod := range f.pods {
			if pod.Labels[lameduckv1alpha1.LabelRevision] != oldRevision && podReady(pod) {
				replacementReady = true
			}
		}
		current, ok := f.pods[old.Name]
		if !ok {
			t.Fatalf("pod %s was deleted before it drained for the rollout", old.Name)
		}
		return current.Annotations[lameduckv1alpha1.AnnotationDrainReason] == string(lameduckv1alpha1.DrainReasonRollout)
	})
	if !replacementReady {
		t.Fatalf("pod %s started draining before a pod on the new revision was ready", old.Name)
	}
	f.waitForDeletion(old.Name)
	f.assertDrainedBeforeDeadline(old.Name, lameduckv1alpha1.DrainReasonRollout)

	replacement := f.waitForRoutable(1)[0]
	if replacement.Labels[lameduckv1alpha1.LabelRevision] == oldRevision {
		t.Fatalf("pod %s left after the rollout is still on the old revision %s", replacement.Name, oldRevision)
	}
}
