package fleet

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

var set = types.NamespacedName{Namespace: "games", Name: "bedwars"}

func draining() *lameduckv1.PodState {
	return &lameduckv1.PodState{Phase: lameduckv1.Phase_PHASE_DRAINING}
}

func TestAttachDeliversLastPublishedState(t *testing.T) {
	registry := NewRegistry(clocktesting.NewFakeClock(time.Now()))
	registry.Publish("pod-a", set, draining())
	session := registry.Attach("pod-a", set)
	select {
	case state := <-session.Outbox():
		if state.GetPhase() != lameduckv1.Phase_PHASE_DRAINING {
			t.Fatalf("phase = %v", state.GetPhase())
		}
	default:
		t.Fatal("no state delivered on attach")
	}
}

func TestPublishKeepsOnlyLatestState(t *testing.T) {
	registry := NewRegistry(clocktesting.NewFakeClock(time.Now()))
	session := registry.Attach("pod-a", set)
	registry.Publish("pod-a", set, &lameduckv1.PodState{Phase: lameduckv1.Phase_PHASE_STARTING})
	registry.Publish("pod-a", set, &lameduckv1.PodState{Phase: lameduckv1.Phase_PHASE_ACTIVE})
	if state := <-session.Outbox(); state.GetPhase() != lameduckv1.Phase_PHASE_ACTIVE {
		t.Fatalf("phase = %v, want latest", state.GetPhase())
	}
}

func TestDrainedResetsWhenDrainCancelled(t *testing.T) {
	registry := NewRegistry(clocktesting.NewFakeClock(time.Now()))
	registry.Publish("pod-a", set, draining())
	registry.RecordDrained("pod-a")
	if !registry.Report("pod-a").Drained {
		t.Fatal("drained not recorded")
	}
	registry.Publish("pod-a", set, &lameduckv1.PodState{Phase: lameduckv1.Phase_PHASE_ACTIVE})
	if registry.Report("pod-a").Drained {
		t.Fatal("drained survived cancellation")
	}
}

func TestDetachStartsLoadStaleness(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	registry := NewRegistry(clock)
	session := registry.Attach("pod-a", set)
	registry.RecordLoad("pod-a", 7)
	clock.Step(time.Minute)
	registry.Detach(session)
	report := registry.Report("pod-a")
	if report.Connected || *report.Load != 7 || !report.LoadAt.Equal(clock.Now()) {
		t.Fatalf("report = %+v", report)
	}
}

func TestRetainForgetsDeletedPods(t *testing.T) {
	registry := NewRegistry(clocktesting.NewFakeClock(time.Now()))
	session := registry.Attach("pod-a", set)
	registry.Attach("pod-b", set)
	registry.Retain(set, map[types.UID]bool{"pod-b": true})
	select {
	case <-session.Done():
	default:
		t.Fatal("session for deleted pod left open")
	}
	registry.Detach(session)
	if registry.Report("pod-b").Connected != true {
		t.Fatal("retained pod lost its session")
	}
}

func TestConnectedSidecarsCountsOpenSessions(t *testing.T) {
	registry := NewRegistry(clocktesting.NewFakeClock(time.Now()))
	other := types.NamespacedName{Namespace: "games", Name: "skywars"}
	registry.Attach("pod-a", set)
	session := registry.Attach("pod-b", set)
	registry.Attach("pod-c", other)
	registry.Publish("pod-d", set, draining())
	registry.Detach(session)
	counts := registry.ConnectedSidecars()
	if counts[set] != 1 || counts[other] != 1 || len(counts) != 2 {
		t.Fatalf("counts = %v", counts)
	}
}
