// bench/tcp_client.go — TCP-transport client for the hoardDB benchmark.
//
// One pooled socket per unit of concurrency: a TCP connection is
// single-request-safe, not multiplexed, so N concurrent calls take N
// sockets — a pool of one socket behind a mutex would serialize the
// workload and silently report conc=1 numbers.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// benchClient is the minimal client interface the benchmark actually uses
// (grep bench/hoarddb_bench.go for client.SendRequest/SendGetFast/Close).
// *tcpBenchClient is the only implementer.
type benchClient interface {
	SendRequest(ctx context.Context, op transport.Opcode, payload []byte) (*transport.Frame, error)
	SendGetFast(ctx context.Context, req transport.GetFastRequest) (*transport.Frame, error)
	Close() error
}

// tcpBenchClient is a pool of authenticated TLS sockets, one per slot.
// SendRequest takes a free socket for the request and returns it after the
// response is read; with N slots, N requests are genuinely in flight.
type tcpBenchClient struct {
	conns chan *tcpPoolConn
}

// tcpPoolConn is one pooled TLS socket with a buffered frame reader.
type tcpPoolConn struct {
	conn *tls.Conn
	br   *bufio.Reader
}

// newTCPBenchClient opens one socket per pool slot, spread round-robin over
// the given addresses, and authenticates each through the bench's login
// flow (the server requires AUTH before any other opcode on a new
// connection).
func newTCPBenchClient(ctx context.Context, addrs []string, tlsCfg *tls.Config, size int) (*tcpBenchClient, error) {
	if size < 1 {
		size = 1
	}
	pool := &tcpBenchClient{conns: make(chan *tcpPoolConn, size)}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	for i := 0; i < size; i++ {
		addr := addrs[i%len(addrs)]
		raw, err := tls.DialWithDialer(dialer, "tcp", addr, tlsCfg)
		if err != nil {
			_ = pool.Close()
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		if err := raw.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			_ = pool.Close()
			return nil, fmt.Errorf("handshake %s: %w", addr, err)
		}
		pc := &tcpPoolConn{conn: raw, br: bufio.NewReader(raw)}
		payload, err := bson.Marshal(&server.LoginRequest{Username: "admin", Password: "bench-root-password"})
		if err != nil {
			_ = raw.Close()
			_ = pool.Close()
			return nil, fmt.Errorf("marshal login: %w", err)
		}
		frame := transport.NewFrame(transport.OpAuth, payload)
		if _, err := raw.Write(frame.Encode()); err != nil {
			_ = raw.Close()
			_ = pool.Close()
			return nil, fmt.Errorf("send auth: %w", err)
		}
		resp, err := transport.DecodeFrame(pc.br)
		if err != nil {
			_ = raw.Close()
			_ = pool.Close()
			return nil, fmt.Errorf("read auth response: %w", err)
		}
		if resp.Opcode != transport.OpAuthOK {
			_ = raw.Close()
			_ = pool.Close()
			return nil, fmt.Errorf("login rejected at %s: %s", addr, resp.Opcode)
		}
		pool.conns <- pc
	}
	return pool, nil
}

// SendRequest writes one frame on a pooled socket and reads its response.
// It takes ownership of the socket for the duration of the request — a TCP
// connection cannot multiplex, so this blocks until a socket is free, which
// is exactly the pool sizing: `-conc` sockets, `-conc` requests in flight.
func (c *tcpBenchClient) SendRequest(ctx context.Context, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	pc := <-c.conns
	defer func() { c.conns <- pc }()
	if dl, ok := ctx.Deadline(); ok {
		_ = pc.conn.SetDeadline(dl)
	}
	frame := transport.NewFrame(op, payload)
	if _, err := pc.conn.Write(frame.Encode()); err != nil {
		return nil, fmt.Errorf("write frame: %w", err)
	}
	resp, err := transport.DecodeFrame(pc.br)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	return resp, nil
}

// SendGetFast performs the compact point read over TCP.
func (c *tcpBenchClient) SendGetFast(ctx context.Context, req transport.GetFastRequest) (*transport.Frame, error) {
	payload, err := transport.MarshalGetFast(req)
	if err != nil {
		return nil, fmt.Errorf("encoding get-fast request: %w", err)
	}
	return c.SendRequest(ctx, transport.OpGetFast, payload)
}

// Close shuts every pooled socket down.
func (c *tcpBenchClient) Close() error {
	var firstErr error
	for {
		select {
		case pc := <-c.conns:
			if err := pc.conn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		default:
			return firstErr
		}
	}
}
