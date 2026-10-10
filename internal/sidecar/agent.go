package sidecar

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

type Agent struct {
	mu       sync.Mutex
	state    *lameduckv1.PodState
	load     *int64
	drained  bool
	watchers map[chan *lameduckv1.PodState]struct{}
	reports  chan struct{}
}

func NewAgent() *Agent {
	return &Agent{
		state:    &lameduckv1.PodState{Phase: lameduckv1.Phase_PHASE_UNSPECIFIED},
		watchers: map[chan *lameduckv1.PodState]struct{}{},
		reports:  make(chan struct{}, 1),
	}
}

func (a *Agent) State() *lameduckv1.PodState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}

func (a *Agent) SetState(state *lameduckv1.PodState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if state.GetPhase() == lameduckv1.Phase_PHASE_UNSPECIFIED || proto.Equal(a.state, state) {
		return
	}
	a.state = state
	if state.GetPhase() != lameduckv1.Phase_PHASE_DRAINING {
		a.drained = false
	}
	for watcher := range a.watchers {
		select {
		case <-watcher:
		default:
		}
		watcher <- state
	}
}

func (a *Agent) Watch() (<-chan *lameduckv1.PodState, func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	watcher := make(chan *lameduckv1.PodState, 1)
	if a.state.GetPhase() != lameduckv1.Phase_PHASE_UNSPECIFIED {
		watcher <- a.state
	}
	a.watchers[watcher] = struct{}{}
	return watcher, func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		delete(a.watchers, watcher)
	}
}

func (a *Agent) SetLoad(load int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.load != nil && *a.load == load {
		return
	}
	a.load = &load
	a.signal()
}

func (a *Agent) MarkDrained() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.drained {
		return
	}
	a.drained = true
	a.signal()
}

func (a *Agent) Reports() <-chan struct{} {
	return a.reports
}

func (a *Agent) Pending() (*int64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.load == nil {
		return nil, a.drained
	}
	load := *a.load
	return &load, a.drained
}

func (a *Agent) signal() {
	select {
	case a.reports <- struct{}{}:
	default:
	}
}
