package metrics

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

type Outcome string

const (
	OutcomeDrained   Outcome = "drained"
	OutcomeTimeout   Outcome = "timeout"
	OutcomeCancelled Outcome = "cancelled"
)

const (
	labelNamespace = "namespace"
	labelDrainSet  = "drainset"
	labelReason    = "reason"
	labelOutcome   = "outcome"
)

type DrainMetrics struct {
	drainsStarted        *prometheus.CounterVec
	drainsCompleted      *prometheus.CounterVec
	drainDuration        *prometheus.HistogramVec
	evictionsIntercepted *prometheus.CounterVec
}

func NewDrainMetrics() *DrainMetrics {
	return &DrainMetrics{
		drainsStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lameduck_drains_started_total",
			Help: "Drains started, by DrainSet and drain reason.",
		}, []string{labelNamespace, labelDrainSet, labelReason}),
		drainsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lameduck_drains_completed_total",
			Help: "Drains that ended, by DrainSet and outcome.",
		}, []string{labelNamespace, labelDrainSet, labelOutcome}),
		drainDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "lameduck_drain_duration_seconds",
			Help:    "Time from the start of a drain to its end, by DrainSet and outcome.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 14),
		}, []string{labelNamespace, labelDrainSet, labelOutcome}),
		evictionsIntercepted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lameduck_evictions_intercepted_total",
			Help: "Pod evictions turned into drains, by DrainSet.",
		}, []string{labelNamespace, labelDrainSet}),
	}
}

func (m *DrainMetrics) Register(registerer prometheus.Registerer) error {
	collectors := []prometheus.Collector{m.drainsStarted, m.drainsCompleted, m.drainDuration, m.evictionsIntercepted}
	var errs []error
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("register drain metrics: %w", err)
	}
	return nil
}

func (m *DrainMetrics) DrainStarted(set types.NamespacedName, reason lameduckv1alpha1.DrainReason) {
	m.drainsStarted.WithLabelValues(set.Namespace, set.Name, string(reason)).Inc()
}

func (m *DrainMetrics) DrainCompleted(set types.NamespacedName, outcome Outcome, duration time.Duration) {
	m.drainsCompleted.WithLabelValues(set.Namespace, set.Name, string(outcome)).Inc()
	m.drainDuration.WithLabelValues(set.Namespace, set.Name, string(outcome)).Observe(max(duration, 0).Seconds())
}

func (m *DrainMetrics) EvictionIntercepted(set types.NamespacedName) {
	m.evictionsIntercepted.WithLabelValues(set.Namespace, set.Name).Inc()
}

func (m *DrainMetrics) Forget(set types.NamespacedName) {
	labels := prometheus.Labels{labelNamespace: set.Namespace, labelDrainSet: set.Name}
	m.drainsStarted.DeletePartialMatch(labels)
	m.drainsCompleted.DeletePartialMatch(labels)
	m.drainDuration.DeletePartialMatch(labels)
	m.evictionsIntercepted.DeletePartialMatch(labels)
}
