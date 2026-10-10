package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"
)

type ConnectionCounter interface {
	ConnectedSidecars() map[types.NamespacedName]int
}

type SidecarCollector struct {
	Counter ConnectionCounter
}

var connectedSidecars = prometheus.NewDesc(
	"lameduck_connected_sidecars",
	"Sidecars with an open fleet stream, by DrainSet.",
	[]string{labelNamespace, labelDrainSet}, nil,
)

func (c SidecarCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- connectedSidecars
}

func (c SidecarCollector) Collect(metrics chan<- prometheus.Metric) {
	for set, count := range c.Counter.ConnectedSidecars() {
		metrics <- prometheus.MustNewConstMetric(connectedSidecars, prometheus.GaugeValue, float64(count), set.Namespace, set.Name)
	}
}
