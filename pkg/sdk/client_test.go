package sdk

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

type drainEvents struct {
	drains     []time.Time
	cancels    int
	observed   []lameduckv1.Phase
	lastReason lameduckv1.DrainReason
}

func (e *drainEvents) handler() DrainHandler {
	return DrainHandler{
		OnDrain: func(reason lameduckv1.DrainReason, deadline time.Time) {
			e.drains = append(e.drains, deadline)
			e.lastReason = reason
		},
		OnCancelled: func() { e.cancels++ },
		OnState:     func(state *lameduckv1.PodState) { e.observed = append(e.observed, state.GetPhase()) },
	}
}

func draining(deadline time.Time) *lameduckv1.PodState {
	return &lameduckv1.PodState{
		Phase:         lameduckv1.Phase_PHASE_DRAINING,
		DrainReason:   lameduckv1.DrainReason_DRAIN_REASON_ROLLOUT,
		DrainDeadline: timestamppb.New(deadline),
	}
}

func phase(value lameduckv1.Phase) *lameduckv1.PodState {
	return &lameduckv1.PodState{Phase: value}
}

func TestDrainTrackerReportsDrainAndCancellation(t *testing.T) {
	events := &drainEvents{}
	tracker := &drainTracker{handler: events.handler()}
	deadline := time.Unix(1000, 0).UTC()
	tracker.observe(phase(lameduckv1.Phase_PHASE_ACTIVE))
	tracker.observe(draining(deadline))
	tracker.observe(draining(deadline))
	tracker.observe(phase(lameduckv1.Phase_PHASE_ACTIVE))
	if len(events.drains) != 1 || !events.drains[0].Equal(deadline) {
		t.Fatalf("drains = %v, want one at %v", events.drains, deadline)
	}
	if events.lastReason != lameduckv1.DrainReason_DRAIN_REASON_ROLLOUT {
		t.Fatalf("reason = %v, want rollout", events.lastReason)
	}
	if events.cancels != 1 {
		t.Fatalf("cancels = %d, want 1", events.cancels)
	}
}

func TestDrainTrackerReportsMovedDeadline(t *testing.T) {
	events := &drainEvents{}
	tracker := &drainTracker{handler: events.handler()}
	tracker.observe(draining(time.Unix(1000, 0)))
	tracker.observe(draining(time.Unix(2000, 0)))
	if len(events.drains) != 2 {
		t.Fatalf("drains = %d, want 2", len(events.drains))
	}
	if events.cancels != 0 {
		t.Fatalf("cancels = %d, want 0", events.cancels)
	}
}

func TestDrainTrackerIgnoresUnknownState(t *testing.T) {
	events := &drainEvents{}
	tracker := &drainTracker{handler: events.handler()}
	deadline := time.Unix(1000, 0)
	tracker.observe(phase(lameduckv1.Phase_PHASE_UNSPECIFIED))
	tracker.observe(draining(deadline))
	tracker.observe(phase(lameduckv1.Phase_PHASE_UNSPECIFIED))
	tracker.observe(draining(deadline))
	if len(events.drains) != 1 {
		t.Fatalf("drains = %d, want 1", len(events.drains))
	}
	if events.cancels != 0 {
		t.Fatalf("cancels = %d, want 0", events.cancels)
	}
	for _, observed := range events.observed {
		if observed == lameduckv1.Phase_PHASE_UNSPECIFIED {
			t.Fatal("unknown state passed to OnState")
		}
	}
}

type droppingSidecar struct {
	lameduckv1.UnimplementedSidecarServiceServer

	calls atomic.Int64
}

func (s *droppingSidecar) WatchState(
	_ *lameduckv1.WatchStateRequest,
	stream grpc.ServerStreamingServer[lameduckv1.WatchStateResponse],
) error {
	call := s.calls.Add(1)
	state := &lameduckv1.PodState{Phase: lameduckv1.Phase_PHASE_ACTIVE, Revision: strconv.FormatInt(call, 10)}
	if err := stream.Send(&lameduckv1.WatchStateResponse{State: state}); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "sidecar restarting")
}

func TestWatchReconnectsAfterStreamError(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	lameduckv1.RegisterSidecarServiceServer(server, &droppingSidecar{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough:///sidecar",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := &Client{conn: conn, sidecar: lameduckv1.NewSidecarServiceClient(conn)}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var revisions []string
	err = client.Watch(ctx, func(state *lameduckv1.PodState) {
		revisions = append(revisions, state.GetRevision())
		if len(revisions) == 3 {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("watch returned %v, want context cancelled", err)
	}
	want := []string{"1", "2", "3"}
	for i, revision := range want {
		if i >= len(revisions) || revisions[i] != revision {
			t.Fatalf("revisions = %v, want %v", revisions, want)
		}
	}
}
