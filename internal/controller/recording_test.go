package controller

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/metrics"
	"github.com/tommyskeff/lameduck-controller/internal/planner"
)

var recordingSet = types.NamespacedName{Namespace: "games", Name: "bedwars"}

type recordingFixture struct {
	reconciler *DrainSetReconciler
	recorder   *events.FakeRecorder
	registry   *prometheus.Registry
	state      *reconcileState
}

func newRecordingFixture(t *testing.T) recordingFixture {
	t.Helper()
	drainMetrics := metrics.NewDrainMetrics()
	registry := prometheus.NewRegistry()
	if err := drainMetrics.Register(registry); err != nil {
		t.Fatalf("register: %v", err)
	}
	recorder := events.NewFakeRecorder(10)
	set := &lameduckv1alpha1.DrainSet{}
	set.Namespace = recordingSet.Namespace
	set.Name = recordingSet.Name
	return recordingFixture{
		reconciler: &DrainSetReconciler{Recorder: recorder, Metrics: drainMetrics},
		recorder:   recorder,
		registry:   registry,
		state: &reconcileState{
			set: set,
			key: recordingSet,
			now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}
}

func (f recordingFixture) events(t *testing.T) []string {
	t.Helper()
	var received []string
	for {
		select {
		case event := <-f.recorder.Events:
			received = append(received, event)
		default:
			return received
		}
	}
}

func (f recordingFixture) expectMetrics(t *testing.T, expected string, names ...string) {
	t.Helper()
	if err := testutil.GatherAndCompare(f.registry, strings.NewReader(expected), names...); err != nil {
		t.Fatal(err)
	}
}

func drainingPod(drain *planner.Drain) *corev1.Pod {
	pod := &corev1.Pod{}
	pod.Namespace = recordingSet.Namespace
	pod.Name = "bedwars-abc"
	writeDrain(pod, drain)
	return pod
}

func TestRecordDrainChangeStartsDrain(t *testing.T) {
	fixture := newRecordingFixture(t)
	now := fixture.state.now
	original := drainingPod(nil)
	desired := drainingPod(&planner.Drain{Reason: lameduckv1alpha1.DrainReasonRollout, Started: now, Deadline: now.Add(time.Hour)})
	fixture.reconciler.recordDrainChange(fixture.state, original, desired)

	received := fixture.events(t)
	if len(received) != 1 || !strings.HasPrefix(received[0], "Normal DrainStarted Draining for Rollout") {
		t.Fatalf("events = %v", received)
	}
	fixture.expectMetrics(t, `
# HELP lameduck_drains_started_total Drains started, by DrainSet and drain reason.
# TYPE lameduck_drains_started_total counter
lameduck_drains_started_total{drainset="bedwars",namespace="games",reason="Rollout"} 1
`, "lameduck_drains_started_total")
}

func TestRecordDrainChangeCancelsDrain(t *testing.T) {
	fixture := newRecordingFixture(t)
	now := fixture.state.now
	original := drainingPod(&planner.Drain{Reason: lameduckv1alpha1.DrainReasonScaleDown, Started: now.Add(-time.Minute), Deadline: now.Add(time.Hour)})
	fixture.reconciler.recordDrainChange(fixture.state, original, drainingPod(nil))

	received := fixture.events(t)
	if len(received) != 1 || !strings.HasPrefix(received[0], "Normal DrainCancelled Cancelled ScaleDown drain") {
		t.Fatalf("events = %v", received)
	}
	fixture.expectMetrics(t, `
# HELP lameduck_drains_completed_total Drains that ended, by DrainSet and outcome.
# TYPE lameduck_drains_completed_total counter
lameduck_drains_completed_total{drainset="bedwars",namespace="games",outcome="cancelled"} 1
`, "lameduck_drains_completed_total")
}

func TestRecordRemovalAfterDeadlineWarns(t *testing.T) {
	fixture := newRecordingFixture(t)
	now := fixture.state.now
	pod := drainingPod(&planner.Drain{Reason: lameduckv1alpha1.DrainReasonEviction, Started: now.Add(-10 * time.Minute), Deadline: now})
	fixture.reconciler.recordRemoval(fixture.state, pod, removalDrainTimeout)

	received := fixture.events(t)
	if len(received) != 2 ||
		!strings.HasPrefix(received[0], "Normal SuccessfulDelete Deleted pod bedwars-abc: DrainTimeout") ||
		!strings.HasPrefix(received[1], "Warning DrainDeadlineExceeded") {
		t.Fatalf("events = %v", received)
	}
	fixture.expectMetrics(t, `
# HELP lameduck_drains_completed_total Drains that ended, by DrainSet and outcome.
# TYPE lameduck_drains_completed_total counter
lameduck_drains_completed_total{drainset="bedwars",namespace="games",outcome="timeout"} 1
`, "lameduck_drains_completed_total")
}

func TestRecordRemovalOfFinishedPodLeavesDrainMetrics(t *testing.T) {
	fixture := newRecordingFixture(t)
	fixture.reconciler.recordRemoval(fixture.state, drainingPod(nil), removalFinished)

	if received := fixture.events(t); len(received) != 1 {
		t.Fatalf("events = %v", received)
	}
	fixture.expectMetrics(t, "", "lameduck_drains_completed_total")
}
