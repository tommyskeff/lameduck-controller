package controller

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/planner"
	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

var protoDrainReasons = map[lameduckv1alpha1.DrainReason]lameduckv1.DrainReason{
	lameduckv1alpha1.DrainReasonRollout:   lameduckv1.DrainReason_DRAIN_REASON_ROLLOUT,
	lameduckv1alpha1.DrainReasonScaleDown: lameduckv1.DrainReason_DRAIN_REASON_SCALE_DOWN,
	lameduckv1alpha1.DrainReasonEviction:  lameduckv1.DrainReason_DRAIN_REASON_EVICTION,
	lameduckv1alpha1.DrainReasonManual:    lameduckv1.DrainReason_DRAIN_REASON_MANUAL,
}

var protoPhases = map[lameduckv1alpha1.PodPhase]lameduckv1.Phase{
	lameduckv1alpha1.PodPhaseStarting: lameduckv1.Phase_PHASE_STARTING,
	lameduckv1alpha1.PodPhaseActive:   lameduckv1.Phase_PHASE_ACTIVE,
	lameduckv1alpha1.PodPhaseDraining: lameduckv1.Phase_PHASE_DRAINING,
}

func readDrain(pod *corev1.Pod) *planner.Drain {
	reason, ok := pod.Annotations[lameduckv1alpha1.AnnotationDrainReason]
	if !ok {
		return nil
	}
	started, _ := time.Parse(time.RFC3339, pod.Annotations[lameduckv1alpha1.AnnotationDrainStarted])
	deadline, _ := time.Parse(time.RFC3339, pod.Annotations[lameduckv1alpha1.AnnotationDrainDeadline])
	return &planner.Drain{
		Reason:   lameduckv1alpha1.DrainReason(reason),
		Started:  started,
		Deadline: deadline,
	}
}

func readRepairedDrain(
	pod *corev1.Pod,
	timeouts map[lameduckv1alpha1.DrainReason]time.Duration,
	now time.Time,
) (*planner.Drain, bool) {
	drain := readDrain(pod)
	if drain == nil {
		return nil, false
	}
	repaired := false
	if drain.Started.IsZero() {
		drain.Started = now
		repaired = true
	}
	if drain.Deadline.IsZero() {
		timeout, ok := timeouts[drain.Reason]
		if !ok {
			timeout = timeouts[lameduckv1alpha1.DrainReasonManual]
		}
		drain.Deadline = drain.Started.Add(timeout)
		repaired = true
	}
	return drain, repaired
}

func writeDrain(pod *corev1.Pod, drain *planner.Drain) {
	if drain == nil {
		delete(pod.Annotations, lameduckv1alpha1.AnnotationDrainReason)
		delete(pod.Annotations, lameduckv1alpha1.AnnotationDrainStarted)
		delete(pod.Annotations, lameduckv1alpha1.AnnotationDrainDeadline)
		return
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[lameduckv1alpha1.AnnotationDrainReason] = string(drain.Reason)
	pod.Annotations[lameduckv1alpha1.AnnotationDrainStarted] = drain.Started.UTC().Format(time.RFC3339)
	pod.Annotations[lameduckv1alpha1.AnnotationDrainDeadline] = drain.Deadline.UTC().Format(time.RFC3339)
}

func podState(pod *corev1.Pod, updateRevision string) *lameduckv1.PodState {
	revision := pod.Labels[lameduckv1alpha1.LabelRevision]
	state := &lameduckv1.PodState{
		Phase:    protoPhases[lameduckv1alpha1.PodPhase(pod.Labels[lameduckv1alpha1.LabelPhase])],
		Current:  revision == updateRevision,
		Routable: pod.Labels[lameduckv1alpha1.LabelRoutable] == "true",
		Revision: revision,
	}
	if drain := readDrain(pod); drain != nil {
		state.DrainReason = protoDrainReasons[drain.Reason]
		state.DrainDeadline = timestamppb.New(drain.Deadline)
	}
	return state
}
