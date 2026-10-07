package planner

import (
	"cmp"
	"slices"
	"time"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

type Drain struct {
	Reason   lameduckv1alpha1.DrainReason
	Started  time.Time
	Deadline time.Time
}

type Pod struct {
	Name      string
	Revision  string
	Created   time.Time
	Ready     bool
	Available bool
	Load      *int64
	Drain     *Drain
}

type SwapPair struct {
	Draining  string
	Candidate string
}

type Reselection struct {
	Enabled        bool
	SwapWhenIdle   bool
	MinLoadDelta   int64
	MaxLoadPercent int32
	SustainFor     time.Duration
	Cooldown       time.Duration
	ApplyTo        []lameduckv1alpha1.DrainReason
	LastSwap       time.Time
	QualifiedSince map[SwapPair]time.Time
}

type Input struct {
	Now              time.Time
	Replicas         int
	UpdateRevision   string
	Pods             []Pod
	MaxSurge         int
	MaxUnavailable   int
	MaxDraining      int
	Order            []lameduckv1alpha1.VictimCriterion
	CancelOnRollback bool
	CancelOnScaleUp  bool
	Timeouts         map[lameduckv1alpha1.DrainReason]time.Duration
	OutdatedRouting  lameduckv1alpha1.OutdatedRouting
	CurrentThreshold int
	Reselection      Reselection
	AllowRollout     bool
}

type Plan struct {
	Create    int
	Drains    map[string]*Drain
	Swapped   bool
	Qualified map[SwapPair]time.Time
	Routable  map[string]bool
	NextCheck time.Duration
}

var DefaultOrder = []lameduckv1alpha1.VictimCriterion{
	lameduckv1alpha1.VictimCriterionNotReady,
	lameduckv1alpha1.VictimCriterionOutdated,
	lameduckv1alpha1.VictimCriterionLoad,
	lameduckv1alpha1.VictimCriterionAge,
}

type fleet struct {
	input      Input
	pods       map[string]*Pod
	plan       Plan
	budget     int
	drainedNow map[string]bool
}

func Compute(input Input) Plan {
	f := &fleet{
		input:      input,
		pods:       make(map[string]*Pod, len(input.Pods)),
		drainedNow: map[string]bool{},
		plan: Plan{
			Qualified: map[SwapPair]time.Time{},
			Routable:  map[string]bool{},
		},
	}
	for i := range input.Pods {
		pod := input.Pods[i]
		f.pods[pod.Name] = &pod
	}
	f.budget = max(input.MaxDraining, 1) - f.countBudgetedDrains()

	f.cancelDrains()
	f.createPods()
	f.drainPods()
	f.reselect()
	f.route()
	f.plan.Drains = make(map[string]*Drain, len(f.pods))
	for name, pod := range f.pods {
		f.plan.Drains[name] = pod.Drain
	}
	return f.plan
}

func (f *fleet) current(pod *Pod) bool {
	return pod.Revision == f.input.UpdateRevision
}

func (f *fleet) active() []*Pod {
	var pods []*Pod
	for _, pod := range f.sorted() {
		if pod.Drain == nil {
			pods = append(pods, pod)
		}
	}
	return pods
}

func (f *fleet) draining() []*Pod {
	var pods []*Pod
	for _, pod := range f.sorted() {
		if pod.Drain != nil {
			pods = append(pods, pod)
		}
	}
	return pods
}

func (f *fleet) sorted() []*Pod {
	pods := make([]*Pod, 0, len(f.pods))
	for _, pod := range f.pods {
		pods = append(pods, pod)
	}
	slices.SortFunc(pods, func(a, b *Pod) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return pods
}

func (f *fleet) countBudgetedDrains() int {
	count := 0
	for _, pod := range f.pods {
		if pod.Drain != nil && budgeted(pod.Drain.Reason) {
			count++
		}
	}
	return count
}

func budgeted(reason lameduckv1alpha1.DrainReason) bool {
	return reason == lameduckv1alpha1.DrainReasonRollout || reason == lameduckv1alpha1.DrainReasonScaleDown
}

func (f *fleet) cancelDrains() {
	if f.input.CancelOnRollback {
		for _, pod := range f.draining() {
			if pod.Drain.Reason == lameduckv1alpha1.DrainReasonRollout && f.current(pod) {
				f.undrain(pod)
			}
		}
	}
	if !f.input.CancelOnScaleUp {
		return
	}
	missing := f.input.Replicas - len(f.active())
	if missing <= 0 {
		return
	}
	var candidates []*Pod
	for _, pod := range f.draining() {
		if pod.Drain.Reason == lameduckv1alpha1.DrainReasonScaleDown && f.current(pod) {
			candidates = append(candidates, pod)
		}
	}
	slices.SortStableFunc(candidates, func(a, b *Pod) int {
		return compareLoad(a.Load, b.Load)
	})
	for _, pod := range candidates[:min(missing, len(candidates))] {
		f.undrain(pod)
	}
}

func (f *fleet) undrain(pod *Pod) {
	if budgeted(pod.Drain.Reason) {
		f.budget++
	}
	pod.Drain = nil
}

func (f *fleet) drain(pod *Pod, reason lameduckv1alpha1.DrainReason) {
	drain := Drain{
		Reason:   reason,
		Started:  f.input.Now,
		Deadline: f.input.Now.Add(f.input.Timeouts[reason]),
	}
	pod.Drain = &drain
	f.drainedNow[pod.Name] = true
	if budgeted(reason) {
		f.budget--
	}
}

func (f *fleet) createPods() {
	active := f.active()
	currentActive := 0
	for _, pod := range active {
		if f.current(pod) {
			currentActive++
		}
	}
	outdated := len(active) - currentActive
	var create int
	if outdated == 0 || !f.input.AllowRollout {
		create = f.input.Replicas - len(active)
	} else {
		create = min(f.input.Replicas-currentActive, f.input.Replicas+f.input.MaxSurge-len(active))
	}
	f.plan.Create = max(create, 0)
}

func (f *fleet) drainPods() {
	available := 0
	for _, pod := range f.active() {
		if pod.Available {
			available++
		}
	}
	activeCount := len(f.active()) + f.plan.Create
	minAvailable := f.input.Replicas - f.input.MaxUnavailable

	for f.budget > 0 {
		var outdated []*Pod
		for _, pod := range f.active() {
			if !f.current(pod) {
				outdated = append(outdated, pod)
			}
		}
		rolling := f.input.AllowRollout && len(outdated) > 0
		excess := activeCount - f.input.Replicas

		var candidates []*Pod
		switch {
		case rolling:
			candidates = outdated
		case excess > 0:
			candidates = f.active()
		default:
			return
		}

		f.order(candidates)
		var victim *Pod
		for _, pod := range candidates {
			if rolling && pod.Available && available-1 < minAvailable {
				continue
			}
			victim = pod
			break
		}
		if victim == nil {
			return
		}

		reason := lameduckv1alpha1.DrainReasonScaleDown
		if !f.current(victim) {
			reason = lameduckv1alpha1.DrainReasonRollout
		}
		f.drain(victim, reason)
		activeCount--
		if victim.Available {
			available--
		}
	}
}

func (f *fleet) order(pods []*Pod) {
	order := f.input.Order
	if len(order) == 0 {
		order = DefaultOrder
	}
	slices.SortStableFunc(pods, func(a, b *Pod) int {
		for _, criterion := range order {
			var c int
			switch criterion {
			case lameduckv1alpha1.VictimCriterionNotReady:
				c = compareBool(!a.Available, !b.Available)
			case lameduckv1alpha1.VictimCriterionOutdated:
				c = compareBool(!f.current(a), !f.current(b))
			case lameduckv1alpha1.VictimCriterionLoad:
				c = compareLoad(a.Load, b.Load)
			case lameduckv1alpha1.VictimCriterionAge:
				c = a.Created.Compare(b.Created)
			}
			if c != 0 {
				return c
			}
		}
		return cmp.Compare(a.Name, b.Name)
	})
}

func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return -1
	default:
		return 1
	}
}

func compareLoad(a, b *int64) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return 1
	case b == nil:
		return -1
	default:
		return cmp.Compare(*a, *b)
	}
}

func (f *fleet) reselect() {
	r := f.input.Reselection
	if !r.Enabled {
		return
	}
	cooledDown := r.LastSwap.IsZero() || !f.input.Now.Before(r.LastSwap.Add(r.Cooldown))

	for _, draining := range f.draining() {
		if !slices.Contains(r.ApplyTo, draining.Drain.Reason) || draining.Load == nil || *draining.Load == 0 {
			continue
		}
		if f.drainedNow[draining.Name] {
			continue
		}
		candidate := f.lightestCandidate(draining)
		if candidate == nil {
			continue
		}
		pair := SwapPair{Draining: draining.Name, Candidate: candidate.Name}
		since, ok := r.QualifiedSince[pair]
		if !ok {
			since = f.input.Now
		}
		f.plan.Qualified[pair] = since

		remaining := since.Add(r.SustainFor).Sub(f.input.Now)
		if !cooledDown {
			remaining = max(remaining, r.LastSwap.Add(r.Cooldown).Sub(f.input.Now))
		}
		if remaining > 0 {
			f.scheduleCheck(remaining)
			continue
		}
		if f.plan.Swapped {
			continue
		}
		reason := draining.Drain.Reason
		f.undrain(draining)
		f.drain(candidate, reason)
		f.plan.Swapped = true
		cooledDown = false
		delete(f.plan.Qualified, pair)
	}
}

func (f *fleet) lightestCandidate(draining *Pod) *Pod {
	r := f.input.Reselection
	var best *Pod
	for _, pod := range f.active() {
		if pod.Load == nil || !pod.Available {
			continue
		}
		if draining.Drain.Reason == lameduckv1alpha1.DrainReasonRollout && f.current(pod) {
			continue
		}
		idle := r.SwapWhenIdle && *pod.Load == 0
		lighter := *pod.Load <= *draining.Load-r.MinLoadDelta &&
			*pod.Load*100 <= *draining.Load*int64(r.MaxLoadPercent)
		if !idle && !lighter {
			continue
		}
		if best == nil || *pod.Load < *best.Load {
			best = pod
		}
	}
	return best
}

func (f *fleet) route() {
	currentAvailable := 0
	for _, pod := range f.active() {
		if f.current(pod) && pod.Available {
			currentAvailable++
		}
	}
	outdatedRoutable := f.input.OutdatedRouting != lameduckv1alpha1.OutdatedRoutingUntilCurrentAvailable ||
		currentAvailable < f.input.CurrentThreshold
	for _, pod := range f.sorted() {
		f.plan.Routable[pod.Name] = pod.Drain == nil && pod.Ready && (f.current(pod) || outdatedRoutable)
	}
}

func (f *fleet) scheduleCheck(after time.Duration) {
	if f.plan.NextCheck == 0 || after < f.plan.NextCheck {
		f.plan.NextCheck = after
	}
}
