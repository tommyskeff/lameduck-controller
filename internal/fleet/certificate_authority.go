package fleet

import (
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/open-policy-agent/cert-controller/pkg/rotator"
	"k8s.io/apimachinery/pkg/types"
)

const (
	certificateName          = "tls.crt"
	keyName                  = "tls.key"
	certificateAuthorityName = "ca.crt"
)

type CertificateAuthorityFile string

func CertificateAuthorityIn(dir string) CertificateAuthorityFile {
	return CertificateAuthorityFile(filepath.Join(dir, certificateAuthorityName))
}

func (f CertificateAuthorityFile) PEM() ([]byte, error) {
	data, err := os.ReadFile(string(f))
	if err != nil {
		return nil, fmt.Errorf("read certificate authority: %w", err)
	}
	return data, nil
}

func NewCertificateRotator(secret types.NamespacedName, dir, fleetAddress string, ready chan struct{}) (*rotator.CertRotator, error) {
	host, _, err := net.SplitHostPort(fleetAddress)
	if err != nil {
		return nil, fmt.Errorf("parse fleet address: %w", err)
	}
	if host == "" || net.ParseIP(host) != nil {
		return nil, fmt.Errorf("fleet address %q must use a DNS name", fleetAddress)
	}
	var extra []string
	if strings.HasSuffix(host, ".svc") {
		extra = append(extra, host+".cluster.local")
	}
	return &rotator.CertRotator{
		SecretKey:      secret,
		CertDir:        dir,
		CertName:       certificateName,
		KeyName:        keyName,
		CAName:         "lameduck-fleet-ca",
		CAOrganization: "lameduck",
		DNSName:        host,
		ExtraDNSNames:  extra,
		ExtKeyUsages:   &[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsReady:        ready,
		ControllerName: "fleet-cert-rotator",
		FieldOwner:     "lameduck-controller",
	}, nil
}
