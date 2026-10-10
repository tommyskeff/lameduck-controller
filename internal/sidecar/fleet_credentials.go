package sidecar

import (
	"crypto/tls"
	"crypto/x509"
	"errors"

	"google.golang.org/grpc/credentials"
)

func FleetCredentials(certificateAuthority []byte) (credentials.TransportCredentials, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certificateAuthority) {
		return nil, errors.New("fleet certificate authority holds no PEM certificates")
	}
	return credentials.NewTLS(&tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
	}), nil
}
