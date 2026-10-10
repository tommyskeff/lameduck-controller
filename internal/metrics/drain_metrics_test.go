package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/types"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

var set = types.NamespacedName{Namespace: "games", Name: "bedwars"}

func TestRegisterRejectsDuplicates(t *testing.T) {
	registry := prometheus.NewRegistry()
	if err := NewDrainMetrics().Register(registry); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := NewDrainMetrics().Register(registry); err == nil {
		t.Fatal("duplicate registration succeeded")
	}
}

func TestDrainStartedCountsByReason(t *testing.T) {
	metrics := NewDrainMetrics()
	metrics.DrainStarted(set, lameduckv1alpha1.DrainReasonRollout)
	metrics.DrainStarted(set, lameduckv1alpha1.DrainReasonRollout)
	metrics.DrainStarted(set, lameduckv1alpha1.DrainReasonEviction)
	rollout := testutil.ToFloat64(metrics.drainsStarted.WithLabelValues("games", "bedwars", "Rollout"))
	eviction := testutil.ToFloat64(metrics.drainsStarted.WithLabelValues("games", "bedwars", "Eviction"))
	if rollout != 2 || eviction != 1 {
		t.Fatalf("rollout = %v, eviction = %v", rollout, eviction)
	}
}

func TestDrainCompletedObservesDuration(t *testing.T) {
	metrics := NewDrainMetrics()
	metrics.DrainCompleted(set, OutcomeDrained, 90*time.Second)
	metrics.DrainCompleted(set, OutcomeDrained, 30*time.Second)
	metrics.DrainCompleted(set, OutcomeTimeout, time.Hour)
	if got := testutil.ToFloat64(metrics.drainsCompleted.WithLabelValues("games", "bedwars", "drained")); got != 2 {
		t.Fatalf("drained = %v", got)
	}
	families, err := gatherer(t, metrics).Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "lameduck_drain_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if outcome(metric.GetLabel()) != "drained" {
				continue
			}
			histogram := metric.GetHistogram()
			if histogram.GetSampleCount() != 2 || histogram.GetSampleSum() != 120 {
				t.Fatalf("count = %d, sum = %v", histogram.GetSampleCount(), histogram.GetSampleSum())
			}
			return
		}
	}
	t.Fatal("no drained duration series")
}

func outcome(labels []*dto.LabelPair) string {
	for _, label := range labels {
		if label.GetName() == labelOutcome {
			return label.GetValue()
		}
	}
	return ""
}

func TestEvictionInterceptedCountsPerSet(t *testing.T) {
	metrics := NewDrainMetrics()
	metrics.EvictionIntercepted(set)
	if got := testutil.ToFloat64(metrics.evictionsIntercepted.WithLabelValues("games", "bedwars")); got != 1 {
		t.Fatalf("evictions = %v", got)
	}
}

func TestForgetRemovesSetSeries(t *testing.T) {
	metrics := NewDrainMetrics()
	other := types.NamespacedName{Namespace: "games", Name: "skywars"}
	metrics.DrainStarted(set, lameduckv1alpha1.DrainReasonManual)
	metrics.DrainStarted(other, lameduckv1alpha1.DrainReasonManual)
	metrics.DrainCompleted(set, OutcomeCancelled, time.Minute)
	metrics.EvictionIntercepted(set)
	metrics.Forget(set)
	gatherer := gatherer(t, metrics)
	for name, want := range map[string]int{
		"lameduck_drains_started_total":        1,
		"lameduck_drains_completed_total":      0,
		"lameduck_drain_duration_seconds":      0,
		"lameduck_evictions_intercepted_total": 0,
	} {
		count, err := testutil.GatherAndCount(gatherer, name)
		if err != nil || count != want {
			t.Fatalf("%s series = %d, want %d (%v)", name, count, want, err)
		}
	}
}

type staticCounter map[types.NamespacedName]int

func (c staticCounter) ConnectedSidecars() map[types.NamespacedName]int {
	return c
}

func TestSidecarCollectorReportsConnections(t *testing.T) {
	collector := SidecarCollector{Counter: staticCounter{set: 3}}
	expected := `
# HELP lameduck_connected_sidecars Sidecars with an open fleet stream, by DrainSet.
# TYPE lameduck_connected_sidecars gauge
lameduck_connected_sidecars{drainset="bedwars",namespace="games"} 3
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}
}

func gatherer(t *testing.T, metrics *DrainMetrics) prometheus.Gatherer {
	t.Helper()
	registry := prometheus.NewRegistry()
	if err := metrics.Register(registry); err != nil {
		t.Fatalf("register: %v", err)
	}
	return registry
}
