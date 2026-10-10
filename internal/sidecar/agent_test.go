package sidecar

import (
	"testing"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

func phase(value lameduckv1.Phase) *lameduckv1.PodState {
	return &lameduckv1.PodState{Phase: value}
}

func TestStateIsUnknownBeforeFirstState(t *testing.T) {
	agent := NewAgent()
	if got := agent.State().GetPhase(); got != lameduckv1.Phase_PHASE_UNSPECIFIED {
		t.Fatalf("phase = %v, want unspecified", got)
	}
}

func TestWatchWaitsForFirstState(t *testing.T) {
	agent := NewAgent()
	states, stop := agent.Watch()
	defer stop()
	select {
	case state := <-states:
		t.Fatalf("received %v before any state was known", state.GetPhase())
	default:
	}
	agent.SetState(phase(lameduckv1.Phase_PHASE_DRAINING))
	select {
	case state := <-states:
		if state.GetPhase() != lameduckv1.Phase_PHASE_DRAINING {
			t.Fatalf("phase = %v, want draining", state.GetPhase())
		}
	default:
		t.Fatal("no state delivered after the first state")
	}
}

func TestWatchDeliversKnownStateImmediately(t *testing.T) {
	agent := NewAgent()
	agent.SetState(phase(lameduckv1.Phase_PHASE_ACTIVE))
	states, stop := agent.Watch()
	defer stop()
	select {
	case state := <-states:
		if state.GetPhase() != lameduckv1.Phase_PHASE_ACTIVE {
			t.Fatalf("phase = %v, want active", state.GetPhase())
		}
	default:
		t.Fatal("no state delivered on watch")
	}
}

func TestSetStateIgnoresUnknownState(t *testing.T) {
	agent := NewAgent()
	agent.SetState(phase(lameduckv1.Phase_PHASE_DRAINING))
	states, stop := agent.Watch()
	defer stop()
	<-states
	agent.SetState(phase(lameduckv1.Phase_PHASE_UNSPECIFIED))
	select {
	case state := <-states:
		t.Fatalf("received %v after an unknown state", state.GetPhase())
	default:
	}
	if got := agent.State().GetPhase(); got != lameduckv1.Phase_PHASE_DRAINING {
		t.Fatalf("phase = %v, want draining", got)
	}
}
