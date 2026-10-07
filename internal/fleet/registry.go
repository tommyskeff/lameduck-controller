package fleet

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

type Report struct {
	Load      *int64
	LoadAt    time.Time
	Drained   bool
	Connected bool
}

type Session struct {
	uid    types.UID
	outbox chan *lameduckv1.PodState
	done   chan struct{}
}

func (s *Session) Outbox() <-chan *lameduckv1.PodState {
	return s.outbox
}

func (s *Session) Done() <-chan struct{} {
	return s.done
}

type podRecord struct {
	set     types.NamespacedName
	load    *int64
	loadAt  time.Time
	drained bool
	state   *lameduckv1.PodState
	session *Session
}

type Registry struct {
	clock  clock.PassiveClock
	events chan event.GenericEvent
	mu     sync.Mutex
	pods   map[types.UID]*podRecord
}

func NewRegistry(passiveClock clock.PassiveClock) *Registry {
	return &Registry{
		clock:  passiveClock,
		events: make(chan event.GenericEvent, 1024),
		pods:   map[types.UID]*podRecord{},
	}
}

func (r *Registry) Events() <-chan event.GenericEvent {
	return r.events
}

func (r *Registry) Attach(uid types.UID, set types.NamespacedName) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := r.record(uid, set)
	if record.session != nil {
		close(record.session.done)
	}
	session := &Session{
		uid:    uid,
		outbox: make(chan *lameduckv1.PodState, 1),
		done:   make(chan struct{}),
	}
	record.session = session
	if record.state != nil {
		session.outbox <- record.state
	}
	r.notify(set)
	return session
}

func (r *Registry) Detach(session *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.pods[session.uid]
	if !ok || record.session != session {
		return
	}
	record.session = nil
	record.loadAt = r.clock.Now()
	close(session.done)
	r.notify(record.set)
}

func (r *Registry) RecordLoad(uid types.UID, load int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.pods[uid]
	if !ok {
		return
	}
	changed := record.load == nil || *record.load != load
	record.load = &load
	record.loadAt = r.clock.Now()
	if changed {
		r.notify(record.set)
	}
}

func (r *Registry) RecordDrained(uid types.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.pods[uid]
	if !ok || record.drained {
		return
	}
	record.drained = true
	r.notify(record.set)
}

func (r *Registry) Report(uid types.UID) Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.pods[uid]
	if !ok {
		return Report{}
	}
	report := Report{
		LoadAt:    record.loadAt,
		Drained:   record.drained,
		Connected: record.session != nil,
	}
	if record.load != nil {
		load := *record.load
		report.Load = &load
	}
	return report
}

func (r *Registry) Publish(uid types.UID, set types.NamespacedName, state *lameduckv1.PodState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := r.record(uid, set)
	if record.state != nil && proto.Equal(record.state, state) {
		return
	}
	record.state = state
	if state.GetPhase() != lameduckv1.Phase_PHASE_DRAINING {
		record.drained = false
	}
	if record.session == nil {
		return
	}
	select {
	case <-record.session.outbox:
	default:
	}
	record.session.outbox <- state
}

func (r *Registry) Retain(set types.NamespacedName, live map[types.UID]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for uid, record := range r.pods {
		if record.set != set || live[uid] {
			continue
		}
		if record.session != nil {
			close(record.session.done)
		}
		delete(r.pods, uid)
	}
}

func (r *Registry) record(uid types.UID, set types.NamespacedName) *podRecord {
	record, ok := r.pods[uid]
	if !ok {
		record = &podRecord{set: set}
		r.pods[uid] = record
	}
	return record
}

func (r *Registry) notify(set types.NamespacedName) {
	object := &lameduckv1alpha1.DrainSet{}
	object.Namespace = set.Namespace
	object.Name = set.Name
	select {
	case r.events <- event.GenericEvent{Object: client.Object(object)}:
	default:
	}
}
