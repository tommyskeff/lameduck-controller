package fleet

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

type Authenticator interface {
	Authenticate(ctx context.Context, token string) (PodIdentity, error)
}

type Server struct {
	lameduckv1.UnimplementedFleetServiceServer

	Address           string
	CertificateDir    string
	CertificatesReady <-chan struct{}
	AcceptPlaintext   bool
	Registry          *Registry
	Authenticator     Authenticator
	Reader            client.Reader
	Log               logr.Logger
}

func (s *Server) NeedLeaderElection() bool {
	return true
}

func (s *Server) Start(ctx context.Context) error {
	select {
	case <-s.CertificatesReady:
	case <-ctx.Done():
		return nil
	}
	watcher, err := certwatcher.New(filepath.Join(s.CertificateDir, certificateName), filepath.Join(s.CertificateDir, keyName))
	if err != nil {
		return fmt.Errorf("load fleet certificate: %w", err)
	}
	watched := make(chan error, 1)
	go func() {
		watched <- watcher.Start(ctx)
	}()

	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.Address, err)
	}
	transport := credentials.NewTLS(&tls.Config{
		GetCertificate: watcher.GetCertificate,
		MinVersion:     tls.VersionTLS13,
	})
	if s.AcceptPlaintext {
		transport = plaintextFallbackCredentials{TransportCredentials: transport}
	}
	server := grpc.NewServer(
		grpc.Creds(transport),
		grpc.ConnectionTimeout(10*time.Second),
		grpc.MaxConcurrentStreams(4),
		grpc.MaxHeaderListSize(32<<10),
		grpc.MaxRecvMsgSize(64<<10),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
	lameduckv1.RegisterFleetServiceServer(server, s)
	failed := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
		case err := <-watched:
			if ctx.Err() == nil {
				failed <- fmt.Errorf("watch fleet certificate: %w", err)
			}
		}
		server.Stop()
	}()
	s.Log.Info("Serving fleet", "address", s.Address, "acceptPlaintext", s.AcceptPlaintext)
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serve fleet: %w", err)
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

func (s *Server) Connect(stream grpc.BidiStreamingServer[lameduckv1.ConnectRequest, lameduckv1.ConnectResponse]) error {
	ctx := stream.Context()
	identity, set, err := s.identify(ctx)
	if err != nil {
		return err
	}
	log := s.Log.WithValues("pod", types.NamespacedName{Namespace: identity.Namespace, Name: identity.Name})
	session := s.Registry.Attach(identity.UID, set)
	defer s.Registry.Detach(session)
	log.V(1).Info("Sidecar connected")

	received := make(chan error, 1)
	go func() {
		received <- s.receive(stream, identity.UID)
	}()

	for {
		select {
		case state := <-session.Outbox():
			err := stream.Send(&lameduckv1.ConnectResponse{
				Message: &lameduckv1.ConnectResponse_State{State: state},
			})
			if err != nil {
				return err
			}
		case err := <-received:
			log.V(1).Info("Sidecar disconnected")
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-session.Done():
			return status.Error(codes.Aborted, "session replaced")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) receive(stream grpc.BidiStreamingServer[lameduckv1.ConnectRequest, lameduckv1.ConnectResponse], uid types.UID) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		switch message := request.Message.(type) {
		case *lameduckv1.ConnectRequest_Load:
			s.Registry.RecordLoad(uid, message.Load.GetLoad())
		case *lameduckv1.ConnectRequest_Drained:
			s.Registry.RecordDrained(uid)
		}
	}
}

func (s *Server) identify(ctx context.Context) (PodIdentity, types.NamespacedName, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return PodIdentity{}, types.NamespacedName{}, status.Error(codes.Unauthenticated, "missing bearer token")
	}
	identity, err := s.Authenticator.Authenticate(ctx, strings.TrimPrefix(values[0], "Bearer "))
	if errors.Is(err, ErrReviewsSaturated) || errors.Is(err, ErrSourceThrottled) {
		return PodIdentity{}, types.NamespacedName{}, status.Error(codes.ResourceExhausted, "authentication throttled")
	}
	if err != nil {
		s.Log.V(1).Info("Rejected sidecar", "reason", err.Error())
		return PodIdentity{}, types.NamespacedName{}, status.Error(codes.Unauthenticated, "invalid token")
	}

	var pod corev1.Pod
	key := types.NamespacedName{Namespace: identity.Namespace, Name: identity.Name}
	if err := s.Reader.Get(ctx, key, &pod); err != nil {
		return PodIdentity{}, types.NamespacedName{}, status.Error(codes.NotFound, "pod not found")
	}
	owner := metav1.GetControllerOf(&pod)
	setName := pod.Labels[lameduckv1alpha1.LabelSet]
	if pod.UID != identity.UID || setName == "" || owner == nil ||
		owner.Kind != "DrainSet" || owner.Name != setName {
		return PodIdentity{}, types.NamespacedName{}, status.Error(codes.PermissionDenied, "pod is not managed")
	}
	return identity, types.NamespacedName{Namespace: pod.Namespace, Name: setName}, nil
}
