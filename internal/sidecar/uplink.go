package sidecar

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

const (
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
)

type Uplink struct {
	Agent     *Agent
	Fleet     lameduckv1.FleetServiceClient
	TokenPath string
	Log       *slog.Logger
}

func (u *Uplink) Run(ctx context.Context) {
	backoff := minBackoff
	for ctx.Err() == nil {
		started := time.Now()
		err := u.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > maxBackoff {
			backoff = minBackoff
		}
		u.Log.Warn("Fleet connection lost", "error", err.Error(), "retry", backoff.String())
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (u *Uplink) session(ctx context.Context) error {
	token, err := os.ReadFile(u.TokenPath)
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+strings.TrimSpace(string(token)))
	stream, err := u.Fleet.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	received := make(chan error, 1)
	go func() {
		for {
			response, err := stream.Recv()
			if err != nil {
				received <- err
				return
			}
			if state := response.GetState(); state != nil {
				u.Agent.SetState(state)
			}
		}
	}()

	if err := u.report(stream); err != nil {
		return err
	}
	u.Log.Info("Connected to fleet")
	for {
		select {
		case <-u.Agent.Reports():
			if err := u.report(stream); err != nil {
				return err
			}
		case err := <-received:
			return fmt.Errorf("receive: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (u *Uplink) report(stream lameduckv1.FleetService_ConnectClient) error {
	load, drained := u.Agent.Pending()
	if load != nil {
		err := stream.Send(&lameduckv1.ConnectRequest{
			Message: &lameduckv1.ConnectRequest_Load{Load: &lameduckv1.LoadReport{Load: *load}},
		})
		if err != nil {
			return fmt.Errorf("send load: %w", err)
		}
	}
	if drained {
		err := stream.Send(&lameduckv1.ConnectRequest{
			Message: &lameduckv1.ConnectRequest_Drained{Drained: &lameduckv1.DrainedReport{}},
		})
		if err != nil {
			return fmt.Errorf("send drained: %w", err)
		}
	}
	return nil
}
