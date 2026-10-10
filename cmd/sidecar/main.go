package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/tommyskeff/lameduck-controller/internal/sidecar"
	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

type config struct {
	listenAddress string
	fleetAddress  string
	fleetCA       []byte
	tokenPath     string
}

func loadConfig() (config, error) {
	cfg := config{
		listenAddress: os.Getenv("LAMEDUCK_SIDECAR_LISTEN_ADDRESS"),
		fleetAddress:  os.Getenv("LAMEDUCK_FLEET_ADDRESS"),
		fleetCA:       []byte(os.Getenv("LAMEDUCK_FLEET_CA")),
		tokenPath:     os.Getenv("LAMEDUCK_TOKEN_PATH"),
	}
	if cfg.listenAddress == "" {
		cfg.listenAddress = "127.0.0.1:9450"
	}
	if cfg.fleetAddress == "" {
		return config{}, errors.New("LAMEDUCK_FLEET_ADDRESS is required")
	}
	if len(cfg.fleetCA) == 0 {
		return config{}, errors.New("LAMEDUCK_FLEET_CA is required")
	}
	if cfg.tokenPath == "" {
		return config{}, errors.New("LAMEDUCK_TOKEN_PATH is required")
	}
	return cfg, nil
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("Invalid configuration", "error", err.Error())
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fleetCredentials, err := sidecar.FleetCredentials(cfg.fleetCA)
	if err != nil {
		log.Error("Invalid fleet certificate authority", "error", err.Error())
		os.Exit(1)
	}
	conn, err := grpc.NewClient(cfg.fleetAddress,
		grpc.WithTransportCredentials(fleetCredentials),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 10 * time.Second}),
	)
	if err != nil {
		log.Error("Invalid fleet address", "error", err.Error())
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	agent := sidecar.NewAgent()
	uplink := &sidecar.Uplink{
		Agent:     agent,
		Fleet:     lameduckv1.NewFleetServiceClient(conn),
		TokenPath: cfg.tokenPath,
		Log:       log,
	}
	go uplink.Run(ctx)

	listener, err := net.Listen("tcp", cfg.listenAddress)
	if err != nil {
		log.Error("Failed to listen", "address", cfg.listenAddress, "error", err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	lameduckv1.RegisterSidecarServiceServer(server, &sidecar.LocalServer{Agent: agent})
	go func() {
		<-ctx.Done()
		server.Stop()
	}()
	log.Info("Serving sidecar", "address", cfg.listenAddress)
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Error("Sidecar server failed", "error", err.Error())
		os.Exit(1)
	}
}
