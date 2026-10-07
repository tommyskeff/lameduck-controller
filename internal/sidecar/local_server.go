package sidecar

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

type LocalServer struct {
	lameduckv1.UnimplementedSidecarServiceServer

	Agent *Agent
}

func (s *LocalServer) GetState(_ context.Context, _ *lameduckv1.GetStateRequest) (*lameduckv1.GetStateResponse, error) {
	return &lameduckv1.GetStateResponse{State: s.Agent.State()}, nil
}

func (s *LocalServer) WatchState(_ *lameduckv1.WatchStateRequest, stream grpc.ServerStreamingServer[lameduckv1.WatchStateResponse]) error {
	states, stop := s.Agent.Watch()
	defer stop()
	for {
		select {
		case state := <-states:
			if err := stream.Send(&lameduckv1.WatchStateResponse{State: state}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *LocalServer) SetLoad(_ context.Context, req *lameduckv1.SetLoadRequest) (*lameduckv1.SetLoadResponse, error) {
	if req.GetLoad() < 0 {
		return nil, status.Error(codes.InvalidArgument, "load must not be negative")
	}
	s.Agent.SetLoad(req.GetLoad())
	return &lameduckv1.SetLoadResponse{}, nil
}

func (s *LocalServer) MarkDrained(_ context.Context, _ *lameduckv1.MarkDrainedRequest) (*lameduckv1.MarkDrainedResponse, error) {
	if s.Agent.State().GetPhase() != lameduckv1.Phase_PHASE_DRAINING {
		return nil, status.Error(codes.FailedPrecondition, "pod is not draining")
	}
	s.Agent.MarkDrained()
	return &lameduckv1.MarkDrainedResponse{}, nil
}
