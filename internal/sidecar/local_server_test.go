package sidecar

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

func TestMarkDrainedRejectedWhileStateUnknown(t *testing.T) {
	server := &LocalServer{Agent: NewAgent()}
	_, err := server.MarkDrained(context.Background(), &lameduckv1.MarkDrainedRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want failed precondition", status.Code(err))
	}
}

func TestGetStateReportsUnknownBeforeFirstState(t *testing.T) {
	server := &LocalServer{Agent: NewAgent()}
	response, err := server.GetState(context.Background(), &lameduckv1.GetStateRequest{})
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	if got := response.GetState().GetPhase(); got != lameduckv1.Phase_PHASE_UNSPECIFIED {
		t.Fatalf("phase = %v, want unspecified", got)
	}
}
