package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/fleet"
	"github.com/tommyskeff/lameduck-controller/internal/planner"
	"github.com/tommyskeff/lameduck-controller/internal/revision"
)

const (
	maxRequeue     = 5 * time.Minute
	invalidRequeue = 30 * time.Second
)

type DrainSetReconciler struct {
	Client       client.Client
	Scheme       *runtime.Scheme
	Clock        clock.Clock
	Registry     *fleet.Registry
	History      revision.History
	Pods         PodFactory
	Expectations *Expectations

	swapsMu sync.Mutex
	swaps   map[types.NamespacedName]map[planner.SwapPair]time.Time
}

type reconcileState struct {
	set      *lameduckv1alpha1.DrainSet
	snapshot *lameduckv1alpha1.DrainSet
	key      types.NamespacedName
	now      time.Time
	update   revision.Revision
	original map[string]*corev1.Pod
	desired  map[string]*corev1.Pod
	created  int
	requeue  time.Duration
}

// +kubebuilder:rbac:groups=lameduck.tommyjs.dev,resources=drainsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=lameduck.tommyjs.dev,resources=drainsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=lameduck.tommyjs.dev,resources=drainsets/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create

func (r *DrainSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var set lameduckv1alpha1.DrainSet
	if err := r.Client.Get(ctx, req.NamespacedName, &set); err != nil {
		if apierrors.IsNotFound(err) {
			r.Expectations.Forget(req.NamespacedName)
			r.Registry.Retain(req.NamespacedName, nil)
			r.storeSwaps(req.NamespacedName, nil)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if set.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	pods, err := r.listPods(ctx, &set)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !r.Expectations.Satisfied(req.NamespacedName, pods) {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	if err := set.Spec.ValidateSelector(); err != nil {
		return ctrl.Result{RequeueAfter: invalidRequeue}, r.markInvalidSelector(ctx, &set, err)
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return ctrl.Result{}, err
	}

	update, revisions, err := r.History.Sync(ctx, &set)
	if err != nil {
		return ctrl.Result{}, err
	}

	state := &reconcileState{
		set:      &set,
		snapshot: set.DeepCopy(),
		key:      req.NamespacedName,
		now:      r.Clock.Now(),
		update:   update,
		original: map[string]*corev1.Pod{},
		desired:  map[string]*corev1.Pod{},
		requeue:  maxRequeue,
	}

	live := map[types.UID]bool{}
	for i := range pods {
		live[pods[i].UID] = true
	}
	r.Registry.Retain(req.NamespacedName, live)

	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if reason := r.removalReason(state, pod); reason != "" {
			if err := r.deletePod(ctx, state, pod, reason); err != nil {
				return ctrl.Result{}, err
			}
			continue
		}
		state.original[pod.Name] = pod
		state.desired[pod.Name] = pod.DeepCopy()
	}

	r.applyDrainRequests(state)
	plan := planner.Compute(r.plannerInput(state))
	r.storeSwaps(state.key, plan.Qualified)
	if plan.Swapped {
		set.Status.LastSwapTime = &metav1.Time{Time: state.now}
	}
	if plan.NextCheck > 0 {
		state.requeue = min(state.requeue, plan.NextCheck)
	}

	for name, pod := range state.desired {
		writeDrain(pod, plan.Drains[name])
		r.writeLabels(state, pod, plan.Routable[name])
	}
	if err := r.patchPods(ctx, state); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.createPods(ctx, state, plan.Create); err != nil {
		return ctrl.Result{}, err
	}
	r.publishStates(state)

	inUse := map[string]bool{update.Hash: true}
	for _, pod := range state.desired {
		inUse[pod.Labels[lameduckv1alpha1.LabelRevision]] = true
	}
	limit := int(ptr.Deref(set.Spec.RevisionHistoryLimit, 10))
	if err := r.History.Prune(ctx, revisions, inUse, limit); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, state, selector); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: state.requeue}, nil
}

func (r *DrainSetReconciler) listPods(ctx context.Context, set *lameduckv1alpha1.DrainSet) ([]corev1.Pod, error) {
	var list corev1.PodList
	err := r.Client.List(ctx, &list,
		client.InNamespace(set.Namespace),
		client.MatchingLabels{lameduckv1alpha1.LabelSet: set.Name},
	)
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	var pods []corev1.Pod
	for _, pod := range list.Items {
		if metav1.IsControlledBy(&pod, set) {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

func (r *DrainSetReconciler) removalReason(state *reconcileState, pod *corev1.Pod) string {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return "Finished"
	}
	if _, ok := pod.Annotations[lameduckv1alpha1.AnnotationTerminate]; ok {
		return "Terminated"
	}
	drain := readDrain(pod)
	if drain == nil {
		return ""
	}
	if r.Registry.Report(pod.UID).Drained {
		return "Drained"
	}
	if !state.now.Before(drain.Deadline) {
		return "DrainTimeout"
	}
	state.requeue = min(state.requeue, drain.Deadline.Sub(state.now))
	return ""
}

func (r *DrainSetReconciler) deletePod(ctx context.Context, state *reconcileState, pod *corev1.Pod, reason string) error {
	grace := state.set.Spec.Drain.TerminationGracePeriodSeconds
	err := r.Client.Delete(ctx, pod,
		client.Preconditions{UID: &pod.UID},
		client.GracePeriodSeconds(ptr.Deref(grace, 30)),
	)
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete pod %s: %w", pod.Name, err)
	}
	r.Expectations.ExpectDelete(state.key, pod.Name)
	logf.FromContext(ctx).Info("Deleted pod", "pod", pod.Name, "reason", reason)
	return nil
}

func (r *DrainSetReconciler) applyDrainRequests(state *reconcileState) {
	for _, pod := range state.desired {
		value, ok := pod.Annotations[lameduckv1alpha1.AnnotationDrain]
		if !ok {
			continue
		}
		delete(pod.Annotations, lameduckv1alpha1.AnnotationDrain)
		reason := lameduckv1alpha1.DrainReasonManual
		if value == string(lameduckv1alpha1.DrainReasonEviction) {
			reason = lameduckv1alpha1.DrainReasonEviction
		}
		drain := &planner.Drain{
			Reason:   reason,
			Started:  state.now,
			Deadline: state.now.Add(r.timeouts(state.set)[reason]),
		}
		if existing := readDrain(pod); existing != nil {
			drain.Started = existing.Started
			drain.Deadline = minTime(existing.Deadline, drain.Deadline)
		}
		writeDrain(pod, drain)
	}
}

func (r *DrainSetReconciler) plannerInput(state *reconcileState) planner.Input {
	set := state.set
	replicas := int(ptr.Deref(set.Spec.Replicas, 1))
	maxSurge := scaled(set.Spec.Rollout.MaxSurge, replicas, true)
	maxUnavailable := scaled(set.Spec.Rollout.MaxUnavailable, replicas, false)
	if maxSurge == 0 && maxUnavailable == 0 {
		maxSurge = 1
	}
	staleAfter := durationOr(set.Spec.VictimSelection.Load.StaleAfter, 30*time.Second)

	pods := make([]planner.Pod, 0, len(state.desired))
	for _, pod := range sortedPods(state.desired) {
		ready, available, pending := r.readiness(set, pod, state.now)
		if pending > 0 {
			state.requeue = min(state.requeue, pending)
		}
		item := planner.Pod{
			Name:      pod.Name,
			Revision:  pod.Labels[lameduckv1alpha1.LabelRevision],
			Created:   pod.CreationTimestamp.Time,
			Ready:     ready,
			Available: available,
			Drain:     readDrain(pod),
		}
		report := r.Registry.Report(pod.UID)
		if report.Load != nil && (report.Connected || state.now.Sub(report.LoadAt) <= staleAfter) {
			item.Load = report.Load
		}
		pods = append(pods, item)
	}

	reselection := set.Spec.VictimSelection.Reselection
	var applyTo []lameduckv1alpha1.DrainReason
	for _, reason := range reselection.ApplyTo {
		if reason == lameduckv1alpha1.DrainReasonRollout || reason == lameduckv1alpha1.DrainReasonScaleDown {
			applyTo = append(applyTo, reason)
		}
	}
	var lastSwap time.Time
	if set.Status.LastSwapTime != nil {
		lastSwap = set.Status.LastSwapTime.Time
	}

	return planner.Input{
		Now:              state.now,
		Replicas:         replicas,
		UpdateRevision:   state.update.Hash,
		Pods:             pods,
		MaxSurge:         maxSurge,
		MaxUnavailable:   maxUnavailable,
		MaxDraining:      max(scaled(set.Spec.Rollout.MaxDraining, replicas, true), 1),
		Order:            set.Spec.VictimSelection.Order,
		CancelOnRollback: ptr.Deref(set.Spec.Drain.CancelOn.Rollback, true),
		CancelOnScaleUp:  ptr.Deref(set.Spec.Drain.CancelOn.ScaleUp, true),
		Timeouts:         r.timeouts(set),
		OutdatedRouting:  set.Spec.Routing.OutdatedPods,
		CurrentThreshold: scaled(set.Spec.Routing.CurrentThreshold, replicas, true),
		AllowRollout:     !set.Spec.Paused,
		Reselection: planner.Reselection{
			Enabled:        reselection.Policy == lameduckv1alpha1.ReselectionPolicyHysteresis,
			SwapWhenIdle:   ptr.Deref(reselection.SwapWhenIdle, true),
			MinLoadDelta:   reselection.MinLoadDelta,
			MaxLoadPercent: reselection.MaxLoadPercent,
			SustainFor:     durationOr(reselection.SustainFor, time.Minute),
			Cooldown:       durationOr(reselection.Cooldown, 5*time.Minute),
			ApplyTo:        applyTo,
			LastSwap:       lastSwap,
			QualifiedSince: r.loadSwaps(state.key),
		},
	}
}

func (r *DrainSetReconciler) readiness(set *lameduckv1alpha1.DrainSet, pod *corev1.Pod, now time.Time) (bool, bool, time.Duration) {
	for _, condition := range pod.Status.Conditions {
		if condition.Type != corev1.PodReady || condition.Status != corev1.ConditionTrue {
			continue
		}
		availableAt := condition.LastTransitionTime.Add(time.Duration(set.Spec.MinReadySeconds) * time.Second)
		if now.Before(availableAt) {
			return true, false, availableAt.Sub(now)
		}
		return true, true, 0
	}
	return false, false, 0
}

func (r *DrainSetReconciler) timeouts(set *lameduckv1alpha1.DrainSet) map[lameduckv1alpha1.DrainReason]time.Duration {
	timeouts := set.Spec.Drain.Timeouts
	return map[lameduckv1alpha1.DrainReason]time.Duration{
		lameduckv1alpha1.DrainReasonRollout:   durationOr(timeouts.Rollout, time.Hour),
		lameduckv1alpha1.DrainReasonScaleDown: durationOr(timeouts.ScaleDown, time.Hour),
		lameduckv1alpha1.DrainReasonEviction:  durationOr(timeouts.Eviction, 10*time.Minute),
		lameduckv1alpha1.DrainReasonManual:    durationOr(timeouts.Manual, 15*time.Minute),
	}
}

func (r *DrainSetReconciler) writeLabels(state *reconcileState, pod *corev1.Pod, routable bool) {
	phase := lameduckv1alpha1.PodPhaseActive
	_, available, _ := r.readiness(state.set, pod, state.now)
	switch {
	case readDrain(pod) != nil:
		phase = lameduckv1alpha1.PodPhaseDraining
	case !available:
		phase = lameduckv1alpha1.PodPhaseStarting
	}
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[lameduckv1alpha1.LabelPhase] = string(phase)
	pod.Labels[lameduckv1alpha1.LabelRoutable] = fmt.Sprint(routable)
}

func (r *DrainSetReconciler) patchPods(ctx context.Context, state *reconcileState) error {
	for _, desired := range sortedPods(state.desired) {
		original := state.original[desired.Name]
		if maps.Equal(original.Labels, desired.Labels) && maps.Equal(original.Annotations, desired.Annotations) {
			continue
		}
		if err := r.Client.Patch(ctx, desired, client.MergeFrom(original)); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("patch pod %s: %w", desired.Name, err)
		}
		before := original.Annotations[lameduckv1alpha1.AnnotationDrainReason]
		after := desired.Annotations[lameduckv1alpha1.AnnotationDrainReason]
		if before == after {
			continue
		}
		r.Expectations.ExpectDrainReason(state.key, desired.Name, after)
		log := logf.FromContext(ctx)
		if after == "" {
			log.Info("Cancelled drain", "pod", desired.Name, "reason", before)
		} else {
			log.Info("Draining pod", "pod", desired.Name, "reason", after)
		}
	}
	return nil
}

func (r *DrainSetReconciler) createPods(ctx context.Context, state *reconcileState, count int) error {
	for range count {
		pod, err := r.Pods.Build(state.set, state.update)
		if err != nil {
			return err
		}
		if err := r.Client.Create(ctx, pod); err != nil {
			return fmt.Errorf("create pod: %w", err)
		}
		r.Expectations.ExpectCreate(state.key, pod.Name)
		state.created++
		logf.FromContext(ctx).Info("Created pod", "pod", pod.Name, "revision", state.update.Hash)
	}
	return nil
}

func (r *DrainSetReconciler) publishStates(state *reconcileState) {
	for _, pod := range state.desired {
		r.Registry.Publish(pod.UID, state.key, podState(pod, state.update.Hash))
	}
}

func (r *DrainSetReconciler) updateStatus(ctx context.Context, state *reconcileState, selector labels.Selector) error {
	set := state.set
	status := &set.Status
	replicas := ptr.Deref(set.Spec.Replicas, 1)

	var active, ready, available, updated, updatedAvailable, draining, routable int32
	outdatedRevision := ""
	for _, pod := range sortedPods(state.desired) {
		if pod.Labels[lameduckv1alpha1.LabelRoutable] == "true" {
			routable++
		}
		if readDrain(pod) != nil {
			draining++
			continue
		}
		active++
		isReady, isAvailable, _ := r.readiness(set, pod, state.now)
		current := pod.Labels[lameduckv1alpha1.LabelRevision] == state.update.Hash
		if isReady {
			ready++
		}
		if isAvailable {
			available++
		}
		if current {
			updated++
			if isAvailable {
				updatedAvailable++
			}
		} else if outdatedRevision == "" {
			outdatedRevision = pod.Labels[lameduckv1alpha1.LabelRevision]
		}
	}
	active += int32(state.created)
	updated += int32(state.created)

	if status.UpdateRevision != state.update.Hash || updatedAvailable > status.UpdatedAvailableReplicas ||
		status.LastProgressTime == nil {
		status.LastProgressTime = &metav1.Time{Time: state.now}
	}
	switch {
	case outdatedRevision == "":
		status.CurrentRevision = state.update.Hash
	case status.CurrentRevision == "":
		status.CurrentRevision = outdatedRevision
	}

	status.ObservedGeneration = set.Generation
	status.Replicas = active
	status.ReadyReplicas = ready
	status.AvailableReplicas = available
	status.UpdatedReplicas = updated
	status.UpdatedAvailableReplicas = updatedAvailable
	status.DrainingReplicas = draining
	status.RoutableReplicas = routable
	status.UpdateRevision = state.update.Hash
	status.Selector = selector.String()

	maxUnavailable := int32(scaled(set.Spec.Rollout.MaxUnavailable, int(replicas), false))
	availableCondition := metav1.Condition{
		Type:   lameduckv1alpha1.ConditionAvailable,
		Status: metav1.ConditionTrue,
		Reason: "MinimumReplicasAvailable",
	}
	if available < replicas-maxUnavailable {
		availableCondition.Status = metav1.ConditionFalse
		availableCondition.Reason = "MinimumReplicasUnavailable"
	}
	availableCondition.ObservedGeneration = set.Generation
	meta.SetStatusCondition(&status.Conditions, availableCondition)

	progressing := metav1.Condition{
		Type:               lameduckv1alpha1.ConditionProgressing,
		Status:             metav1.ConditionTrue,
		Reason:             "Complete",
		ObservedGeneration: set.Generation,
	}
	deadline := time.Duration(ptr.Deref(set.Spec.Rollout.ProgressDeadlineSeconds, 600)) * time.Second
	stalledAt := status.LastProgressTime.Add(deadline)
	switch {
	case set.Spec.Paused:
		progressing.Status = metav1.ConditionUnknown
		progressing.Reason = "Paused"
	case outdatedRevision == "" && updatedAvailable >= replicas:
	case !state.now.Before(stalledAt):
		progressing.Status = metav1.ConditionFalse
		progressing.Reason = "ProgressDeadlineExceeded"
	default:
		progressing.Reason = "RollingOut"
		state.requeue = min(state.requeue, stalledAt.Sub(state.now))
	}
	meta.SetStatusCondition(&status.Conditions, progressing)

	if err := r.Client.Status().Patch(ctx, set, client.MergeFrom(state.snapshot)); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

func (r *DrainSetReconciler) markInvalidSelector(
	ctx context.Context,
	set *lameduckv1alpha1.DrainSet,
	cause error,
) error {
	original := set.DeepCopy()
	set.Status.ObservedGeneration = set.Generation
	meta.SetStatusCondition(&set.Status.Conditions, metav1.Condition{
		Type:               lameduckv1alpha1.ConditionProgressing,
		Status:             metav1.ConditionFalse,
		Reason:             "InvalidSelector",
		Message:            cause.Error(),
		ObservedGeneration: set.Generation,
	})
	if err := r.Client.Status().Patch(ctx, set, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

func (r *DrainSetReconciler) loadSwaps(key types.NamespacedName) map[planner.SwapPair]time.Time {
	r.swapsMu.Lock()
	defer r.swapsMu.Unlock()
	return maps.Clone(r.swaps[key])
}

func (r *DrainSetReconciler) storeSwaps(key types.NamespacedName, qualified map[planner.SwapPair]time.Time) {
	r.swapsMu.Lock()
	defer r.swapsMu.Unlock()
	if r.swaps == nil {
		r.swaps = map[types.NamespacedName]map[planner.SwapPair]time.Time{}
	}
	if len(qualified) == 0 {
		delete(r.swaps, key)
		return
	}
	r.swaps[key] = qualified
}

func (r *DrainSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&lameduckv1alpha1.DrainSet{}).
		Owns(&corev1.Pod{}).
		Owns(&appsv1.ControllerRevision{}).
		WatchesRawSource(source.Channel(r.Registry.Events(), &handler.EnqueueRequestForObject{})).
		Named("drainset").
		Complete(r)
}

func sortedPods(pods map[string]*corev1.Pod) []*corev1.Pod {
	names := slices.Sorted(maps.Keys(pods))
	result := make([]*corev1.Pod, 0, len(names))
	for _, name := range names {
		result = append(result, pods[name])
	}
	return result
}

func scaled(value *intstr.IntOrString, total int, roundUp bool) int {
	if value == nil {
		return 0
	}
	result, err := intstr.GetScaledValueFromIntOrPercent(value, total, roundUp)
	if err != nil {
		return 0
	}
	return max(result, 0)
}

func durationOr(value *metav1.Duration, fallback time.Duration) time.Duration {
	if value == nil {
		return fallback
	}
	return value.Duration
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
