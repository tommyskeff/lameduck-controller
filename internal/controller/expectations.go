package controller

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

const expectationTimeout = 30 * time.Second

type expectationKind int

const (
	expectCreate expectationKind = iota
	expectDelete
	expectDrainReason
)

type expectation struct {
	kind   expectationKind
	reason string
	at     time.Time
}

type Expectations struct {
	clock clock.PassiveClock
	mu    sync.Mutex
	sets  map[types.NamespacedName]map[string]expectation
}

func NewExpectations(passiveClock clock.PassiveClock) *Expectations {
	return &Expectations{
		clock: passiveClock,
		sets:  map[types.NamespacedName]map[string]expectation{},
	}
}

func (e *Expectations) ExpectCreate(set types.NamespacedName, pod string) {
	e.expect(set, pod, expectation{kind: expectCreate})
}

func (e *Expectations) ExpectDelete(set types.NamespacedName, pod string) {
	e.expect(set, pod, expectation{kind: expectDelete})
}

func (e *Expectations) ExpectDrainReason(set types.NamespacedName, pod string, reason string) {
	e.expect(set, pod, expectation{kind: expectDrainReason, reason: reason})
}

func (e *Expectations) expect(set types.NamespacedName, pod string, item expectation) {
	e.mu.Lock()
	defer e.mu.Unlock()
	pods, ok := e.sets[set]
	if !ok {
		pods = map[string]expectation{}
		e.sets[set] = pods
	}
	item.at = e.clock.Now()
	pods[pod] = item
}

func (e *Expectations) Forget(set types.NamespacedName) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.sets, set)
}

func (e *Expectations) Satisfied(set types.NamespacedName, observed []corev1.Pod) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	pods := e.sets[set]
	if len(pods) == 0 {
		return true
	}
	byName := make(map[string]*corev1.Pod, len(observed))
	for i := range observed {
		byName[observed[i].Name] = &observed[i]
	}
	now := e.clock.Now()
	for name, item := range pods {
		if now.Sub(item.at) > expectationTimeout || met(item, byName[name]) {
			delete(pods, name)
		}
	}
	return len(pods) == 0
}

func met(item expectation, pod *corev1.Pod) bool {
	switch item.kind {
	case expectCreate:
		return pod != nil
	case expectDelete:
		return pod == nil || pod.DeletionTimestamp != nil
	default:
		return pod == nil || pod.Annotations[lameduckv1alpha1.AnnotationDrainReason] == item.reason
	}
}
