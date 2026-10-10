package fleet

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/sidecar"
	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

const (
	fleetAddress = "lameduck-fleet.lameduck-system.svc:9500"
	sidecarToken = "sidecar-token"
)

type rejectingAuthenticator struct{}

func (rejectingAuthenticator) Authenticate(context.Context, string) (PodIdentity, error) {
	return PodIdentity{}, errors.New("rejected")
}

func issue(t *testing.T, dir string) {
	t.Helper()
	rotator, err := NewCertificateRotator(types.NamespacedName{Namespace: "lameduck-system", Name: "fleet-cert"},
		dir, fleetAddress, make(chan struct{}))
	if err != nil {
		t.Fatalf("NewCertificateRotator: %v", err)
	}
	begin := time.Now().Add(-time.Hour)
	ca, err := rotator.CreateCACert(begin, begin.Add(48*time.Hour))
	if err != nil {
		t.Fatalf("CreateCACert: %v", err)
	}
	certificate, key, err := rotator.CreateCertPEM(ca, begin, begin.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("CreateCertPEM: %v", err)
	}
	for name, data := range map[string][]byte{certificateName: certificate, keyName: key, certificateAuthorityName: ca.CertPEM} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

func TestCertificateRotatorIssuesForFleetAddress(t *testing.T) {
	dir := t.TempDir()
	issue(t, dir)
	caPEM, err := CertificateAuthorityIn(dir).PEM()
	if err != nil {
		t.Fatalf("PEM: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	data, err := os.ReadFile(filepath.Join(dir, certificateName))
	if err != nil {
		t.Fatalf("read certificate: %v", err)
	}
	block, _ := pem.Decode(data)
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	for _, name := range []string{"lameduck-fleet.lameduck-system.svc", "lameduck-fleet.lameduck-system.svc.cluster.local"} {
		_, err := certificate.Verify(x509.VerifyOptions{DNSName: name, Roots: roots})
		if err != nil {
			t.Errorf("verify %s: %v", name, err)
		}
	}
	if _, err := certificate.Verify(x509.VerifyOptions{DNSName: "other.lameduck-system.svc", Roots: roots}); err == nil {
		t.Error("certificate verified for another service")
	}
}

func TestCertificateRotatorNeedsDNSName(t *testing.T) {
	for _, address := range []string{"10.0.0.1:9500", "fleet", ":9500"} {
		_, err := NewCertificateRotator(types.NamespacedName{Namespace: "ns", Name: "secret"}, "/certs", address, nil)
		if err == nil {
			t.Errorf("NewCertificateRotator accepted %q", address)
		}
	}
}

func serve(t *testing.T, acceptPlaintext bool) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	issue(t, dir)
	caPEM, err := CertificateAuthorityIn(dir).PEM()
	if err != nil {
		t.Fatalf("PEM: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()

	ready := make(chan struct{})
	close(ready)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	server := &Server{
		Address:           address,
		CertificateDir:    dir,
		CertificatesReady: ready,
		AcceptPlaintext:   acceptPlaintext,
		Registry:          NewRegistry(clocktesting.NewFakeClock(time.Now())),
		Authenticator:     rejectingAuthenticator{},
		Log:               logr.Discard(),
	}
	go func() { stopped <- server.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Errorf("Start: %v", err)
		}
	})
	for {
		conn, err := net.Dial("tcp", address)
		if err == nil {
			_ = conn.Close()
			return address, caPEM
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func connect(t *testing.T, target, address string, transport credentials.TransportCredentials) codes.Code {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+target,
		grpc.WithTransportCredentials(transport),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer token")
	stream, err := lameduckv1.NewFleetServiceClient(conn).Connect(ctx)
	if err == nil {
		_, err = stream.Recv()
	}
	return status.Code(err)
}

func TestServerVerifiedBySidecarCredentials(t *testing.T) {
	address, caPEM := serve(t, false)
	transport, err := sidecar.FleetCredentials(caPEM)
	if err != nil {
		t.Fatalf("FleetCredentials: %v", err)
	}
	if code := connect(t, fleetAddress, address, transport); code != codes.Unauthenticated {
		t.Errorf("code = %v; want Unauthenticated from the handler", code)
	}
	if code := connect(t, "other.lameduck-system.svc:9500", address, transport); code != codes.Unavailable {
		t.Errorf("code = %v for the wrong server name; want Unavailable", code)
	}
	if code := connect(t, fleetAddress, address, insecure.NewCredentials()); code != codes.Unavailable {
		t.Errorf("code = %v for plaintext; want Unavailable", code)
	}
}

func TestServerAcceptsPlaintextWhenAllowed(t *testing.T) {
	address, caPEM := serve(t, true)
	transport, err := sidecar.FleetCredentials(caPEM)
	if err != nil {
		t.Fatalf("FleetCredentials: %v", err)
	}
	if code := connect(t, fleetAddress, address, transport); code != codes.Unauthenticated {
		t.Errorf("code = %v over TLS; want Unauthenticated from the handler", code)
	}
	if code := connect(t, fleetAddress, address, insecure.NewCredentials()); code != codes.Unauthenticated {
		t.Errorf("code = %v over plaintext; want Unauthenticated from the handler", code)
	}
}

type staticAuthenticator struct{}

func (staticAuthenticator) Authenticate(_ context.Context, token string) (PodIdentity, error) {
	if token != sidecarToken {
		return PodIdentity{}, errors.New("token rejected")
	}
	return identity, nil
}

func managedPod(t *testing.T) *corev1.Pod {
	t.Helper()
	owner := &lameduckv1alpha1.DrainSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: set.Namespace, Name: set.Name, UID: "set-uid"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: set.Namespace,
			Name:      identity.Name,
			UID:       identity.UID,
			Labels:    map[string]string{lameduckv1alpha1.LabelSet: set.Name},
		},
	}
	if err := controllerutil.SetControllerReference(owner, pod, testScheme()); err != nil {
		t.Fatalf("own pod: %v", err)
	}
	return pod
}

type serverFixture struct {
	registry *Registry
	conn     *grpc.ClientConn
}

func startServer(t *testing.T, pod *corev1.Pod) serverFixture {
	t.Helper()
	builder := fake.NewClientBuilder().WithScheme(testScheme())
	if pod != nil {
		builder = builder.WithObjects(pod)
	}
	registry := NewRegistry(clocktesting.NewFakeClock(time.Now()))
	server := &Server{
		Registry:      registry,
		Authenticator: staticAuthenticator{},
		Reader:        builder.Build(),
		Log:           logr.Discard(),
	}

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	lameduckv1.RegisterFleetServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})
	return serverFixture{registry: registry, conn: conn}
}

func (f serverFixture) connect(t *testing.T, authorization ...string) grpc.BidiStreamingClient[lameduckv1.ConnectRequest, lameduckv1.ConnectResponse] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	for _, value := range authorization {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", value)
	}
	stream, err := lameduckv1.NewFleetServiceClient(f.conn).Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return stream
}

func expectCode(t *testing.T, stream grpc.BidiStreamingClient[lameduckv1.ConnectRequest, lameduckv1.ConnectResponse], code codes.Code) {
	t.Helper()
	_, err := stream.Recv()
	if status.Code(err) != code {
		t.Fatalf("err = %v, want code %v", err, code)
	}
}

func TestConnectRequiresBearerToken(t *testing.T) {
	f := startServer(t, managedPod(t))
	cases := map[string][]string{
		"missing":     nil,
		"not bearer":  {"Basic " + sidecarToken},
		"two headers": {"Bearer " + sidecarToken, "Bearer " + sidecarToken},
	}
	for name, authorization := range cases {
		t.Run(name, func(t *testing.T) {
			expectCode(t, f.connect(t, authorization...), codes.Unauthenticated)
		})
	}
}

func TestConnectRejectsInsidecarToken(t *testing.T) {
	f := startServer(t, managedPod(t))
	expectCode(t, f.connect(t, "Bearer forged"), codes.Unauthenticated)
}

func TestConnectRejectsMissingPod(t *testing.T) {
	f := startServer(t, nil)
	expectCode(t, f.connect(t, "Bearer "+sidecarToken), codes.NotFound)
}

func TestConnectRejectsUnmanagedPod(t *testing.T) {
	cases := map[string]func(pod *corev1.Pod){
		"replaced pod": func(pod *corev1.Pod) {
			pod.UID = "previous-pod-uid"
		},
		"no set label": func(pod *corev1.Pod) {
			pod.Labels = nil
		},
		"no owner": func(pod *corev1.Pod) {
			pod.OwnerReferences = nil
		},
		"other kind": func(pod *corev1.Pod) {
			pod.OwnerReferences[0].Kind = "ReplicaSet"
		},
		"label names another set": func(pod *corev1.Pod) {
			pod.Labels[lameduckv1alpha1.LabelSet] = "skywars"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			pod := managedPod(t)
			mutate(pod)
			f := startServer(t, pod)
			expectCode(t, f.connect(t, "Bearer "+sidecarToken), codes.PermissionDenied)
		})
	}
}

func TestConnectAttachesManagedPod(t *testing.T) {
	f := startServer(t, managedPod(t))
	f.registry.Publish(identity.UID, set, draining())
	stream := f.connect(t, "Bearer "+sidecarToken)

	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive state: %v", err)
	}
	if response.GetState().GetPhase() != lameduckv1.Phase_PHASE_DRAINING {
		t.Fatalf("phase = %v", response.GetState().GetPhase())
	}

	err = stream.Send(&lameduckv1.ConnectRequest{
		Message: &lameduckv1.ConnectRequest_Drained{Drained: &lameduckv1.DrainedReport{}},
	})
	if err != nil {
		t.Fatalf("send drained: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !f.registry.Report(identity.UID).Drained {
		if time.Now().After(deadline) {
			t.Fatal("drained report never reached the registry")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !f.registry.Report(identity.UID).Connected {
		t.Fatal("session not attached")
	}
}
