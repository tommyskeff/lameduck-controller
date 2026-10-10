package controller

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

func observedPod(name string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func newTestExpectations() (*Expectations, *clocktesting.FakePassiveClock) {
	clock := clocktesting.NewFakePassiveClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	return NewExpectations(clock), clock
}

func TestExpectationsSatisfiedWithNothingExpected(t *testing.T) {
	expectations, _ := newTestExpectations()
	if !expectations.Satisfied(recordingSet, nil) {
		t.Fatal("unsatisfied with no expectations")
	}
}

func TestExpectCreateWaitsForPod(t *testing.T) {
	expectations, _ := newTestExpectations()
	expectations.ExpectCreate(recordingSet, "pod-a")
	if expectations.Satisfied(recordingSet, nil) {
		t.Fatal("satisfied before the created pod was observed")
	}
	if !expectations.Satisfied(recordingSet, []corev1.Pod{observedPod("pod-a")}) {
		t.Fatal("unsatisfied after the created pod was observed")
	}
	if !expectations.Satisfied(recordingSet, nil) {
		t.Fatal("met expectation was not cleared")
	}
}

func TestExpectDeleteWaitsForPodToGoOrTerminate(t *testing.T) {
	expectations, _ := newTestExpectations()
	expectations.ExpectDelete(recordingSet, "pod-a")
	if expectations.Satisfied(recordingSet, []corev1.Pod{observedPod("pod-a")}) {
		t.Fatal("satisfied while the deleted pod is still live")
	}
	terminating := observedPod("pod-a")
	terminating.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	if !expectations.Satisfied(recordingSet, []corev1.Pod{terminating}) {
		t.Fatal("unsatisfied while the deleted pod is terminating")
	}

	expectations.ExpectDelete(recordingSet, "pod-b")
	if !expectations.Satisfied(recordingSet, nil) {
		t.Fatal("unsatisfied after the deleted pod disappeared")
	}
}

func TestExpectDrainReasonWaitsForAnnotation(t *testing.T) {
	expectations, _ := newTestExpectations()
	reason := string(lameduckv1alpha1.DrainReasonRollout)
	expectations.ExpectDrainReason(recordingSet, "pod-a", reason)

	pod := observedPod("pod-a")
	if expectations.Satisfied(recordingSet, []corev1.Pod{pod}) {
		t.Fatal("satisfied before the drain reason was observed")
	}
	pod.Annotations = map[string]string{lameduckv1alpha1.AnnotationDrainReason: string(lameduckv1alpha1.DrainReasonManual)}
	if expectations.Satisfied(recordingSet, []corev1.Pod{pod}) {
		t.Fatal("satisfied by a different drain reason")
	}
	pod.Annotations[lameduckv1alpha1.AnnotationDrainReason] = reason
	if !expectations.Satisfied(recordingSet, []corev1.Pod{pod}) {
		t.Fatal("unsatisfied after the drain reason was observed")
	}
}

func TestExpectCancelledDrainWaitsForAnnotationRemoval(t *testing.T) {
	expectations, _ := newTestExpectations()
	expectations.ExpectDrainReason(recordingSet, "pod-a", "")
	pod := observedPod("pod-a")
	pod.Annotations = map[string]string{lameduckv1alpha1.AnnotationDrainReason: string(lameduckv1alpha1.DrainReasonRollout)}
	if expectations.Satisfied(recordingSet, []corev1.Pod{pod}) {
		t.Fatal("satisfied while the cancelled drain is still annotated")
	}
	delete(pod.Annotations, lameduckv1alpha1.AnnotationDrainReason)
	if !expectations.Satisfied(recordingSet, []corev1.Pod{pod}) {
		t.Fatal("unsatisfied after the drain annotation was removed")
	}
}

func TestExpectDrainReasonSatisfiedWhenPodGone(t *testing.T) {
	expectations, _ := newTestExpectations()
	expectations.ExpectDrainReason(recordingSet, "pod-a", string(lameduckv1alpha1.DrainReasonRollout))
	if !expectations.Satisfied(recordingSet, nil) {
		t.Fatal("unsatisfied for a pod that no longer exists")
	}
}

func TestExpectationsExpireAfterTimeout(t *testing.T) {
	expectations, clock := newTestExpectations()
	expectations.ExpectCreate(recordingSet, "pod-a")
	clock.SetTime(clock.Now().Add(expectationTimeout))
	if expectations.Satisfied(recordingSet, nil) {
		t.Fatal("expired at exactly the timeout")
	}
	clock.SetTime(clock.Now().Add(time.Millisecond))
	if !expectations.Satisfied(recordingSet, nil) {
		t.Fatal("still waiting after the timeout")
	}
}

func TestExpectationsTimeOutPerPod(t *testing.T) {
	expectations, clock := newTestExpectations()
	expectations.ExpectCreate(recordingSet, "pod-a")
	clock.SetTime(clock.Now().Add(20 * time.Second))
	expectations.ExpectCreate(recordingSet, "pod-b")
	clock.SetTime(clock.Now().Add(20 * time.Second))
	if expectations.Satisfied(recordingSet, nil) {
		t.Fatal("satisfied while the younger expectation is still pending")
	}
	if !expectations.Satisfied(recordingSet, []corev1.Pod{observedPod("pod-b")}) {
		t.Fatal("unsatisfied once the older expired and the younger was met")
	}
}

func TestLaterExpectationReplacesEarlierForSamePod(t *testing.T) {
	expectations, _ := newTestExpectations()
	expectations.ExpectCreate(recordingSet, "pod-a")
	expectations.ExpectDelete(recordingSet, "pod-a")
	if !expectations.Satisfied(recordingSet, nil) {
		t.Fatal("create expectation survived a later delete expectation")
	}
}

func TestExpectationsAreScopedPerSet(t *testing.T) {
	expectations, _ := newTestExpectations()
	other := types.NamespacedName{Namespace: recordingSet.Namespace, Name: "skywars"}
	expectations.ExpectCreate(recordingSet, "pod-a")
	if !expectations.Satisfied(other, nil) {
		t.Fatal("another set's expectation blocked this set")
	}
	expectations.Forget(recordingSet)
	if !expectations.Satisfied(recordingSet, nil) {
		t.Fatal("expectation survived Forget")
	}
}
