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
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/tommyskeff/lameduck-controller/internal/sidecar"
	"github.com/tommyskeff/lameduck-controller/pkg/proto/lameduckv1"
)

const fleetAddress = "lameduck-fleet.lameduck-system.svc:9500"

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
