package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// defaultRequestTimeout bounds a SendRequest call when the caller's context
// carries no deadline of its own. Without this, a server that accepts the
// connection and never answers blocks the caller forever — the driver
// client has no built-in idle timeout the way the TCP listener does.
const defaultRequestTimeout = 30 * time.Second

// DriverClient is a single TCP+TLS connection to hoardDB's driver listener
// (the tcp-protocol-spec record), used by the CLI and any Go caller that
// wants a plain client rather than the internode peer pool
// (TCPPeerClient, which authenticates internally as NodeUser and is wrong
// for a real user login). DriverClient does not authenticate on its own —
// the caller sends its own AUTH request after Connect, exactly like every
// other opcode.
type DriverClient struct {
	conn   *tls.Conn
	br     *bufio.Reader
	addr   string
	mu     sync.Mutex
	closed bool
}

// NewDriverClient dials the driver address over TCP+TLS and completes the
// handshake. It does not send AUTH; the caller does that as its first
// request.
func NewDriverClient(ctx context.Context, addr string, tlsCfg *tls.Config) (*DriverClient, error) {
	dialer := &net.Dialer{}
	if dl, ok := ctx.Deadline(); ok {
		dialer.Deadline = dl
	} else {
		dialer.Timeout = 10 * time.Second
	}
	raw, err := tls.DialWithDialer(dialer, "tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	if err := raw.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("handshake %s: %w", addr, err)
	}
	return &DriverClient{
		conn: raw,
		br:   bufio.NewReader(raw),
		addr: addr,
	}, nil
}

// SendRequest sends a frame and reads the response on the same connection.
// Safe for concurrent use — calls serialize on the connection's mutex, so
// concurrency does not corrupt the wire, but a caller wanting real request
// parallelism needs multiple DriverClients (there is no per-request stream
// multiplexing over a single TCP socket).
//
// The read honors the caller's context deadline; if none is set, a default
// timeout applies so a hung server cannot block the caller forever.
func (c *DriverClient) SendRequest(ctx context.Context, op Opcode, payload []byte) (*Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultRequestTimeout)
	}
	_ = c.conn.SetWriteDeadline(deadline)
	_ = c.conn.SetReadDeadline(deadline)

	// Refused here, not by the server: an oversize frame cannot be read past, so
	// writing one costs this connection and every request pipelined on it.
	if bodyBytes := 1 + RequestIDLen + len(payload); bodyBytes > MaxFrameLength {
		return nil, &WireError{Code: ErrInvalidArgument, Message: requestTooLargeMessage(op, int64(bodyBytes))}
	}

	frame := NewFrame(op, payload)
	if _, err := c.conn.Write(frame.Encode()); err != nil {
		return nil, fmt.Errorf("write frame: %w", err)
	}

	resp, err := DecodeFrame(c.br)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	// A connection-level refusal carries an all-zero id (tcp.go refuse), so it
	// is the one frame allowed to differ from the id sent.
	if resp.RequestID != frame.RequestID && resp.RequestID != ([RequestIDLen]byte{}) {
		c.closed = true
		_ = c.conn.Close()
		return nil, fmt.Errorf("response carries request id %s but the request sent %s: the connection is out of step (a stale answer to an earlier request), so it was closed; reconnect and retry",
			resp.RequestIDString(), frame.RequestIDString())
	}
	return resp, nil
}

// SendRequestBSON marshals the request as BSON, sends it, and unmarshals
// the response payload as BSON into dst.
func (c *DriverClient) SendRequestBSON(ctx context.Context, op Opcode, req interface{}, dst interface{}) (*Frame, error) {
	payload, err := bson.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	resp, err := c.SendRequest(ctx, op, payload)
	if err != nil {
		return nil, err
	}
	if dst != nil && len(resp.Payload) > 0 {
		if err := bson.Unmarshal(resp.Payload, dst); err != nil {
			return nil, fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return resp, nil
}

// SendGetFast performs a compact point read. A miss is a successful call
// with status GetFastNotFound, not an error.
func (c *DriverClient) SendGetFast(ctx context.Context, req GetFastRequest) (*Frame, error) {
	payload, err := MarshalGetFast(req)
	if err != nil {
		return nil, fmt.Errorf("encoding get-fast request: %w", err)
	}
	return c.SendRequest(ctx, OpGetFast, payload)
}

// Close shuts down the connection.
func (c *DriverClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// RemoteAddr returns the server address this client was dialed to.
func (c *DriverClient) RemoteAddr() string {
	return c.addr
}
