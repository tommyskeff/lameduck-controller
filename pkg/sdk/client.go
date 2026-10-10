package sdk

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tommyskeff/lameduck-controller/internal/backoff"
	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

const AddressEnv = "LAMEDUCK_SIDECAR_ADDRESS"

const (
	minimumReconnectDelay = 500 * time.Millisecond
	maximumReconnectDelay = 10 * time.Second
)

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
	retry := backoff.NewExponential(minimumReconnectDelay, maximumReconnectDelay, rand.Int64N)
	for {
		c.receive(ctx, retry, handle)
		timer := time.NewTimer(retry.Next())
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func (c *Client) receive(ctx context.Context, retry *backoff.Exponential, handle func(state *lameduckv1.PodState)) {
	stream, err := c.sidecar.WatchState(ctx, &lameduckv1.WatchStateRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return
	}
	for {
		response, err := stream.Recv()
		if err != nil {
			return
		}
		retry.Reset()
		handle(response.GetState())
	}
}

func (c *Client) WatchDrain(ctx context.Context, handler DrainHandler) error {
	tracker := &drainTracker{handler: handler}
	return c.Watch(ctx, tracker.observe)
}

type drainTracker struct {
	handler  DrainHandler
	draining bool
	deadline time.Time
}

func (d *drainTracker) observe(state *lameduckv1.PodState) {
	if state.GetPhase() == lameduckv1.Phase_PHASE_UNSPECIFIED {
		return
	}
	if d.handler.OnState != nil {
		d.handler.OnState(state)
	}
	now := state.GetPhase() == lameduckv1.Phase_PHASE_DRAINING
	next := state.GetDrainDeadline().AsTime()
	switch {
	case now && (!d.draining || !next.Equal(d.deadline)):
		if d.handler.OnDrain != nil {
			d.handler.OnDrain(state.GetDrainReason(), next)
		}
	case !now && d.draining:
		if d.handler.OnCancelled != nil {
			d.handler.OnCancelled()
		}
	}
	d.draining = now
	d.deadline = next
}
