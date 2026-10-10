package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
	"github.com/tommyskeff/lameduck-controller/pkg/sdk"
)

const drainDelay = 5 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log); err != nil && ctx.Err() == nil {
		log.Error("Test app failed", "error", err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	address, err := sdk.AddressFromEnvironment()
	if err != nil {
		return err
	}
	client, err := sdk.Connect(address)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	return client.WatchDrain(ctx, sdk.DrainHandler{
		OnDrain: func(reason lameduckv1.DrainReason, _ time.Time) {
			log.Info("Draining", "reason", reason.String())
			time.AfterFunc(drainDelay, func() {
				if err := client.MarkDrained(ctx); err != nil {
					log.Error("Failed to mark drained", "error", err.Error())
					return
				}
				log.Info("Drained")
			})
		},
	})
}
