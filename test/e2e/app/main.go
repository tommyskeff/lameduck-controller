package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
	"github.com/tommyskeff/lameduck-controller/pkg/sdk"
)

const (
	drainDelay     = 5 * time.Second
	listenAddress  = ":8080"
	readinessPath  = "/readyz"
	shutdownWindow = 5 * time.Second
)

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

	var fleetAnswered atomic.Bool
	server := readinessServer(&fleetAnswered)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("Readiness server failed", "error", err.Error())
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownWindow)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	return client.WatchDrain(ctx, sdk.DrainHandler{
		OnState: func(state *lameduckv1.PodState) {
			if !fleetAnswered.Swap(true) {
				log.Info("Fleet answered", "phase", state.GetPhase().String())
			}
		},
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

func readinessServer(fleetAnswered *atomic.Bool) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc(readinessPath, func(w http.ResponseWriter, _ *http.Request) {
		if !fleetAnswered.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return &http.Server{Addr: listenAddress, Handler: mux, ReadHeaderTimeout: time.Second}
}
