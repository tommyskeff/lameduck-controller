package controller

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/tommyskeff/lameduck-controller/internal/metrics"
)

const (
	removalFinished     = "Finished"
	removalTerminated   = "Terminated"
	removalDrained      = "Drained"
	removalDrainTimeout = "DrainTimeout"
)

const (
	eventReasonCreated            = "SuccessfulCreate"
	eventReasonDeleted            = "SuccessfulDelete"
	eventReasonDrainStarted       = "DrainStarted"
	eventReasonDrainReasonChanged = "DrainReasonChanged"
	eventReasonDrainCancelled     = "DrainCancelled"
	eventReasonDrained            = "Drained"
	eventReasonDrainTimeout       = "DrainDeadlineExceeded"

	actionCreate = "Create"
	actionDelete = "Delete"
	actionDrain  = "Drain"
)

func (r *DrainSetReconciler) recordRemoval(state *reconcileState, pod *corev1.Pod, reason string) {
	r.Recorder.Eventf(state.set, pod, corev1.EventTypeNormal, eventReasonDeleted, actionDelete,
		"Deleted pod %s: %s", pod.Name, reason)
	drain := readDrain(pod)
	if drain == nil {
		return
	}
	elapsed := state.now.Sub(drain.Started)
	switch reason {
	case removalDrained:
		r.Recorder.Eventf(pod, state.set, corev1.EventTypeNormal, eventReasonDrained, actionDelete,
			"Sidecar reported the pod drained after %s", elapsed.Round(time.Second))
		r.Metrics.DrainCompleted(state.key, metrics.OutcomeDrained, elapsed)
	case removalDrainTimeout:
		r.Recorder.Eventf(pod, state.set, corev1.EventTypeWarning, eventReasonDrainTimeout, actionDelete,
			"Drain deadline %s passed; deleting the pod", drain.Deadline.UTC().Format(time.RFC3339))
		r.Metrics.DrainCompleted(state.key, metrics.OutcomeTimeout, elapsed)
	}
}

func (r *DrainSetReconciler) recordDrainChange(state *reconcileState, original, desired *corev1.Pod) {
	before := readDrain(original)
	after := readDrain(desired)
	switch {
	case before == nil && after != nil:
		r.Recorder.Eventf(desired, state.set, corev1.EventTypeNormal, eventReasonDrainStarted, actionDrain,
			"Draining for %s until %s", after.Reason, after.Deadline.UTC().Format(time.RFC3339))
		r.Metrics.DrainStarted(state.key, after.Reason)
	case before != nil && after == nil:
		r.Recorder.Eventf(desired, state.set, corev1.EventTypeNormal, eventReasonDrainCancelled, actionDrain,
			"Cancelled %s drain", before.Reason)
		r.Metrics.DrainCompleted(state.key, metrics.OutcomeCancelled, state.now.Sub(before.Started))
	case before != nil && after != nil:
		r.Recorder.Eventf(desired, state.set, corev1.EventTypeNormal, eventReasonDrainReasonChanged, actionDrain,
			"Draining for %s instead of %s until %s", after.Reason, before.Reason, after.Deadline.UTC().Format(time.RFC3339))
	}
}
