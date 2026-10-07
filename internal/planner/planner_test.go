package planner

import (
	"fmt"
	"testing"
	"time"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func load(n int64) *int64 {
	return new(n)
}

func pods(revision string, count int, from int) []Pod {
	result := make([]Pod, 0, count)
	for i := range count {
		index := from + i
		result = append(result, Pod{
			Name:      fmt.Sprintf("%s-%d", revision, index),
			Revision:  revision,
			Created:   epoch.Add(time.Duration(index) * time.Minute),
			Ready:     true,
			Available: true,
			Load:      load(10),
		})
	}
	return result
}

func baseInput(replicas int, fleet []Pod) Input {
	return Input{
		Now:              epoch.Add(time.Hour),
		Replicas:         replicas,
		UpdateRevision:   "new",
		Pods:             fleet,
		MaxSurge:         1,
		MaxUnavailable:   0,
		MaxDraining:      1,
		CancelOnRollback: true,
		CancelOnScaleUp:  true,
		Timeouts: map[lameduckv1alpha1.DrainReason]time.Duration{
			lameduckv1alpha1.DrainReasonRollout:   time.Hour,
			lameduckv1alpha1.DrainReasonScaleDown: time.Hour,
		},
		OutdatedRouting:  lameduckv1alpha1.OutdatedRoutingUntilDraining,
		CurrentThreshold: 1,
		AllowRollout:     true,
	}
}

func draining(plan Plan) map[string]lameduckv1alpha1.DrainReason {
	result := map[string]lameduckv1alpha1.DrainReason{}
	for name, drain := range plan.Drains {
		if drain != nil {
			result[name] = drain.Reason
		}
	}
	return result
}

func TestScaleUpCreatesMissingPods(t *testing.T) {
	plan := Compute(baseInput(3, pods("new", 1, 0)))
	if plan.Create != 2 {
		t.Fatalf("create = %d, want 2", plan.Create)
	}
	if len(draining(plan)) != 0 {
		t.Fatalf("unexpected drains %v", draining(plan))
	}
}

func TestRolloutSurgesBeforeDraining(t *testing.T) {
	plan := Compute(baseInput(3, pods("old", 3, 0)))
	if plan.Create != 1 {
		t.Fatalf("create = %d, want 1", plan.Create)
	}
	if len(draining(plan)) != 0 {
		t.Fatalf("drained %v before the surge pod was available", draining(plan))
	}
}

func TestRolloutDrainsOutdatedOnceSurgeAvailable(t *testing.T) {
	fleet := append(pods("old", 3, 0), pods("new", 1, 10)...)
	plan := Compute(baseInput(3, fleet))
	got := draining(plan)
	if len(got) != 1 || got["old-0"] != lameduckv1alpha1.DrainReasonRollout {
		t.Fatalf("drains = %v, want old-0 for Rollout", got)
	}
	if plan.Create != 0 {
		t.Fatalf("create = %d, want 0", plan.Create)
	}
}

func TestRolloutRespectsMaxDraining(t *testing.T) {
	fleet := append(pods("old", 3, 0), pods("new", 1, 10)...)
	fleet[1].Drain = &Drain{Reason: lameduckv1alpha1.DrainReasonRollout, Started: epoch}
	plan := Compute(baseInput(3, fleet))
	if got := draining(plan); len(got) != 1 {
		t.Fatalf("drains = %v, want only the existing drain", got)
	}
	if plan.Create != 1 {
		t.Fatalf("create = %d, want 1 replacing the draining pod", plan.Create)
	}
}

func TestEvictionDrainsDoNotUseBudget(t *testing.T) {
	fleet := append(pods("old", 3, 0), pods("new", 2, 10)...)
	fleet[1].Drain = &Drain{Reason: lameduckv1alpha1.DrainReasonEviction, Started: epoch}
	plan := Compute(baseInput(3, fleet))
	got := draining(plan)
	if len(got) != 2 || got["old-0"] != lameduckv1alpha1.DrainReasonRollout {
		t.Fatalf("drains = %v, want old-0 drained alongside the eviction", got)
	}
}

func TestRollbackCancelsRolloutDrain(t *testing.T) {
	fleet := append(pods("new", 3, 0), pods("old", 1, 10)...)
	fleet[0].Drain = &Drain{Reason: lameduckv1alpha1.DrainReasonRollout, Started: epoch}
	plan := Compute(baseInput(3, fleet))
	got := draining(plan)
	if _, ok := got["new-0"]; ok {
		t.Fatalf("new-0 still draining after rollback: %v", got)
	}
	if got["old-10"] != lameduckv1alpha1.DrainReasonRollout {
		t.Fatalf("drains = %v, want old-10 drained", got)
	}
}

func TestScaleUpCancelsScaleDownDrain(t *testing.T) {
	fleet := pods("new", 3, 0)
	fleet[2].Drain = &Drain{Reason: lameduckv1alpha1.DrainReasonScaleDown, Started: epoch}
	plan := Compute(baseInput(3, fleet))
	if len(draining(plan)) != 0 {
		t.Fatalf("drains = %v, want none", draining(plan))
	}
	if plan.Create != 0 {
		t.Fatalf("create = %d, want 0", plan.Create)
	}
}

func TestScaleDownPicksLeastLoaded(t *testing.T) {
	fleet := pods("new", 3, 0)
	fleet[0].Load = load(50)
	fleet[1].Load = load(2)
	fleet[2].Load = load(30)
	plan := Compute(baseInput(2, fleet))
	got := draining(plan)
	if len(got) != 1 || got["new-1"] != lameduckv1alpha1.DrainReasonScaleDown {
		t.Fatalf("drains = %v, want new-1 for ScaleDown", got)
	}
}

func TestScaleDownPrefersNotReady(t *testing.T) {
	fleet := pods("new", 3, 0)
	fleet[0].Load = load(0)
	fleet[2].Available = false
	plan := Compute(baseInput(2, fleet))
	if got := draining(plan); got["new-2"] != lameduckv1alpha1.DrainReasonScaleDown {
		t.Fatalf("drains = %v, want new-2", got)
	}
}

func TestUnknownLoadSortsHeaviest(t *testing.T) {
	fleet := pods("new", 2, 0)
	fleet[0].Load = nil
	fleet[1].Load = load(100)
	plan := Compute(baseInput(1, fleet))
	if got := draining(plan); got["new-1"] != lameduckv1alpha1.DrainReasonScaleDown {
		t.Fatalf("drains = %v, want new-1", got)
	}
}

func TestPausedDoesNotRollOut(t *testing.T) {
	input := baseInput(3, pods("old", 3, 0))
	input.AllowRollout = false
	plan := Compute(input)
	if plan.Create != 0 || len(draining(plan)) != 0 {
		t.Fatalf("create = %d drains = %v, want nothing", plan.Create, draining(plan))
	}
}

func reselectionInput(qualifiedSince time.Time) Input {
	fleet := append(pods("old", 2, 0), pods("new", 2, 10)...)
	fleet[0].Drain = &Drain{Reason: lameduckv1alpha1.DrainReasonRollout, Started: epoch}
	fleet[0].Load = load(100)
	fleet[1].Load = load(0)
	input := baseInput(3, fleet)
	input.Reselection = Reselection{
		Enabled:        true,
		SwapWhenIdle:   true,
		MaxLoadPercent: 50,
		SustainFor:     time.Minute,
		Cooldown:       5 * time.Minute,
		ApplyTo:        []lameduckv1alpha1.DrainReason{lameduckv1alpha1.DrainReasonRollout},
	}
	if !qualifiedSince.IsZero() {
		input.Reselection.QualifiedSince = map[SwapPair]time.Time{
			{Draining: "old-0", Candidate: "old-1"}: qualifiedSince,
		}
	}
	return input
}

func TestReselectionWaitsForSustain(t *testing.T) {
	input := reselectionInput(time.Time{})
	plan := Compute(input)
	if plan.Swapped {
		t.Fatal("swapped before sustainFor elapsed")
	}
	pair := SwapPair{Draining: "old-0", Candidate: "old-1"}
	if since, ok := plan.Qualified[pair]; !ok || !since.Equal(input.Now) {
		t.Fatalf("qualified = %v, want pair tracked from now", plan.Qualified)
	}
	if plan.NextCheck != time.Minute {
		t.Fatalf("next check = %v, want 1m", plan.NextCheck)
	}
}

func TestReselectionSwapsAfterSustain(t *testing.T) {
	input := reselectionInput(epoch)
	plan := Compute(input)
	if !plan.Swapped {
		t.Fatal("did not swap")
	}
	got := draining(plan)
	if _, ok := got["old-0"]; ok || got["old-1"] != lameduckv1alpha1.DrainReasonRollout {
		t.Fatalf("drains = %v, want old-1 draining instead of old-0", got)
	}
}

func TestReselectionHonoursCooldown(t *testing.T) {
	input := reselectionInput(epoch)
	input.Reselection.LastSwap = input.Now.Add(-time.Minute)
	plan := Compute(input)
	if plan.Swapped {
		t.Fatal("swapped during cooldown")
	}
	if plan.NextCheck != 4*time.Minute {
		t.Fatalf("next check = %v, want 4m", plan.NextCheck)
	}
}

func TestReselectionNeedsLighterCandidate(t *testing.T) {
	input := reselectionInput(epoch)
	input.Pods[1].Load = load(60)
	plan := Compute(input)
	if plan.Swapped || len(plan.Qualified) != 0 {
		t.Fatalf("swapped = %v qualified = %v, want neither", plan.Swapped, plan.Qualified)
	}
}

func TestRoutingUntilCurrentAvailable(t *testing.T) {
	fleet := append(pods("old", 2, 0), pods("new", 2, 10)...)
	input := baseInput(4, fleet)
	input.OutdatedRouting = lameduckv1alpha1.OutdatedRoutingUntilCurrentAvailable
	input.CurrentThreshold = 2
	plan := Compute(input)
	if plan.Routable["old-0"] || !plan.Routable["new-10"] {
		t.Fatalf("routable = %v, want only new pods", plan.Routable)
	}

	input.CurrentThreshold = 3
	plan = Compute(input)
	if !plan.Routable["old-0"] {
		t.Fatalf("routable = %v, want old pods routable below threshold", plan.Routable)
	}
}

func TestDrainingPodsAreNotRoutable(t *testing.T) {
	fleet := pods("new", 2, 0)
	fleet[0].Drain = &Drain{Reason: lameduckv1alpha1.DrainReasonManual, Started: epoch}
	plan := Compute(baseInput(1, fleet))
	if plan.Routable["new-0"] || !plan.Routable["new-1"] {
		t.Fatalf("routable = %v", plan.Routable)
	}
}
