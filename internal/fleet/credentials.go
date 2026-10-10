package fleet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc/credentials"
)

const tlsRecordHandshake = 0x16

type plaintextFallbackCredentials struct {
	credentials.TransportCredentials
}

func (c plaintextFallbackCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	reader := bufio.NewReader(conn)
	first, err := reader.Peek(1)
	if err != nil {
		return nil, nil, fmt.Errorf("read first byte: %w", err)
	}
	peeked := &peekedConn{Conn: conn, reader: reader}
	if first[0] == tlsRecordHandshake {
		return c.TransportCredentials.ServerHandshake(peeked)
	}
	return peeked, plaintextAuthInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}, nil
}

func (c plaintextFallbackCredentials) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("plaintext fallback credentials are server only")
}

func (c plaintextFallbackCredentials) Clone() credentials.TransportCredentials {
	return plaintextFallbackCredentials{TransportCredentials: c.TransportCredentials.Clone()}
}

type plaintextAuthInfo struct {
	credentials.CommonAuthInfo
}

func (plaintextAuthInfo) AuthType() string {
	return "insecure"
}

type peekedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *peekedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}
