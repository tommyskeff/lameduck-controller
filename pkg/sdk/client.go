package sdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

const AddressEnv = "LAMEDUCK_SIDECAR_ADDRESS"

type Client struct {
	conn    *grpc.ClientConn
	sidecar lameduckv1.SidecarServiceClient
}

type DrainHandler struct {
	OnDrain     func(reason lameduckv1.DrainReason, deadline time.Time)
	OnCancelled func()
	OnState     func(state *lameduckv1.PodState)
}

func AddressFromEnvironment() (string, error) {
	address := os.Getenv(AddressEnv)
	if address == "" {
		return "", errors.New(AddressEnv + " is not set; is the pod managed by a DrainSet?")
	}
	return address, nil
}

func Connect(address string) (*Client, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial sidecar: %w", err)
	}
	return &Client{conn: conn, sidecar: lameduckv1.NewSidecarServiceClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) State(ctx context.Context) (*lameduckv1.PodState, error) {
	response, err := c.sidecar.GetState(ctx, &lameduckv1.GetStateRequest{})
	if err != nil {
		return nil, err
	}
	return response.GetState(), nil
}

func (c *Client) SetLoad(ctx context.Context, load int64) error {
	_, err := c.sidecar.SetLoad(ctx, &lameduckv1.SetLoadRequest{Load: load})
	return err
}

func (c *Client) MarkDrained(ctx context.Context) error {
	_, err := c.sidecar.MarkDrained(ctx, &lameduckv1.MarkDrainedRequest{})
	return err
}

func (c *Client) Watch(ctx context.Context, handle func(state *lameduckv1.PodState)) error {
	stream, err := c.sidecar.WatchState(ctx, &lameduckv1.WatchStateRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for {
		response, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		handle(response.GetState())
	}
}

func (c *Client) WatchDrain(ctx context.Context, handler DrainHandler) error {
	draining := false
	var deadline time.Time
	return c.Watch(ctx, func(state *lameduckv1.PodState) {
		if handler.OnState != nil {
			handler.OnState(state)
		}
		now := state.GetPhase() == lameduckv1.Phase_PHASE_DRAINING
		next := state.GetDrainDeadline().AsTime()
		switch {
		case now && (!draining || !next.Equal(deadline)):
			if handler.OnDrain != nil {
				handler.OnDrain(state.GetDrainReason(), next)
			}
		case !now && draining:
			if handler.OnCancelled != nil {
				handler.OnCancelled()
			}
		}
		draining = now
		deadline = next
	})
}
