package main

// This file holds the hoardDB half of the social bench's clients: every BSON
// encode and decode of a server request or response. It must not import the
// MongoDB client, whose half of the bench passes bytes only (BSON codec spec
// 7.5), so the library swap can move this file and leave that one on the vendor.

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// wireConn is one authenticated TLS socket, used by one goroutine at a time.
// It mirrors the dial and roundtrip of bench/smokeperf; each worker owns its
// own so the run measures -conc sockets in flight, not a shared pool.
type wireConn struct {
	raw     *tls.Conn
	reader  *bufio.Reader
	timeout time.Duration
	sent    int64 // requests sent, including the login; read by requests
}

// serverError is an OpError frame the server sent. The socket is still in
// sync after one, unlike after a timeout or a short read.
type serverError struct {
	op      transport.Opcode
	code    transport.ErrorCode
	message string
}

func (e *serverError) Error() string {
	return fmt.Sprintf("%s refused: [%s] %s", e.op, e.code, e.message)
}

// insecureTLSConfig is built once per run: the call logs a warning every time,
// and a benchmark against a self-signed local server has no certificate to
// check.
func insecureTLSConfig(addr string) (*tls.Config, error) {
	tlsConfig, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "bench/social tool"}}, addr)
	if err != nil {
		return nil, fmt.Errorf("building TLS config: %w", err)
	}
	return tlsConfig, nil
}

// dialWire opens one authenticated socket. The login goes through the gate in
// login.go, so a leg that dials -depth sockets at once does not trip the
// node's limit on logins in flight.
func dialWire(addr, password string, timeout time.Duration) (*wireConn, error) {
	var conn *wireConn
	err := logins.run(func() error {
		var dialErr error
		conn, dialErr = dialAndLogin(addr, password, timeout)
		return dialErr
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func dialAndLogin(addr, password string, timeout time.Duration) (*wireConn, error) {
	tlsConfig, err := sharedTLSConfig(addr)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	raw, err := tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", addr, err)
	}
	conn := &wireConn{raw: raw, reader: bufio.NewReader(raw), timeout: timeout}
	loginPayload, err := bson.Marshal(&server.LoginRequest{Username: "admin", Password: password})
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("encoding login: %w", err)
	}
	loginReply, err := conn.roundtrip(transport.OpAuth, loginPayload)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("logging in to %s: %w", addr, err)
	}
	if err := loginReplyError(addr, loginReply); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return conn, nil
}

// loginReplyError turns a reply to OpAuth into an error, or nil on success.
// An AUTH_FAIL with no payload or the CREDENTIALS reason means the password is
// wrong; any other reason (RATE_LIMITED above all: no credential was checked)
// and any other refusal (a connection limit, a server still starting) carries
// its own text, which the operator needs verbatim.
func loginReplyError(addr string, reply *transport.Frame) error {
	switch reply.Opcode {
	case transport.OpAuthOK:
		return nil
	case transport.OpAuthFail:
		return authFailError(addr, reply)
	case transport.OpError:
		var refusal server.ErrorResponse
		if err := bson.Unmarshal(reply.Payload, &refusal); err != nil {
			return fmt.Errorf("login to %s refused with an undecodable error frame (%d bytes): %w", addr, len(reply.Payload), err)
		}
		return fmt.Errorf("login to %s refused by the server: %s", addr, refusal.Message)
	default:
		return fmt.Errorf("login to %s answered with unexpected opcode %s", addr, reply.Opcode)
	}
}

// authFailError decodes the reason an AUTH_FAIL carries. A nil payload is the
// CREDENTIALS reason by the wire contract; every other reason is reported as
// itself, because telling an operator to fix a password the server never
// checked sends them the wrong way.
func authFailError(addr string, reply *transport.Frame) error {
	passwordAdvice := fmt.Errorf("login to %s rejected with %s; pass the root password with -password", addr, reply.Opcode)
	if len(reply.Payload) == 0 {
		return passwordAdvice
	}
	var failure server.AuthFailResponse
	if err := bson.Unmarshal(reply.Payload, &failure); err != nil {
		return fmt.Errorf("login to %s rejected with %s and an undecodable reason (%d bytes): %w", addr, reply.Opcode, len(reply.Payload), err)
	}
	reason := transport.AuthFailReason(failure.Reason)
	switch reason {
	case transport.AuthFailCredentials:
		return passwordAdvice
	case transport.AuthFailRateLimited:
		return fmt.Errorf("login to %s refused with %s: %s; no password was checked, so wait and retry instead of changing the password: %w", addr, reason, failure.Message, errLoginRateLimited)
	default:
		return fmt.Errorf("login to %s rejected with %s %s: %s", addr, reply.Opcode, reason, failure.Message)
	}
}

func (c *wireConn) roundtrip(op transport.Opcode, payload []byte) (*transport.Frame, error) {
	c.sent++
	if err := c.raw.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return nil, fmt.Errorf("setting deadline: %w", err)
	}
	if _, err := c.raw.Write(transport.NewFrame(op, payload).Encode()); err != nil {
		return nil, fmt.Errorf("writing %s: %w", op, err)
	}
	reply, err := transport.DecodeFrame(c.reader)
	if err != nil {
		return nil, fmt.Errorf("reading %s reply: %w", op, err)
	}
	return reply, nil
}

// call sends request as op and decodes the reply into response (when not nil).
func (c *wireConn) call(op transport.Opcode, request, response interface{}) error {
	payload, err := bson.Marshal(request)
	if err != nil {
		return fmt.Errorf("encoding %s request: %w", op, err)
	}
	reply, err := c.roundtrip(op, payload)
	if err != nil {
		return err
	}
	if reply.Opcode == transport.OpError {
		var refusal server.ErrorResponse
		if err := bson.Unmarshal(reply.Payload, &refusal); err != nil {
			return &serverError{op: op, code: transport.ErrInternal, message: "undecodable error frame"}
		}
		return &serverError{op: op, code: transport.ErrorCode(refusal.Code), message: refusal.Message}
	}
	if response == nil {
		return nil
	}
	if err := bson.Unmarshal(reply.Payload, response); err != nil {
		return fmt.Errorf("decoding %s reply (opcode %s, %d bytes): %w", op, reply.Opcode, len(reply.Payload), err)
	}
	return nil
}

var (
	tlsConfigOnce   sync.Once
	tlsConfigShared *tls.Config
	tlsConfigErr    error
)

func sharedTLSConfig(addr string) (*tls.Config, error) {
	tlsConfigOnce.Do(func() { tlsConfigShared, tlsConfigErr = insecureTLSConfig(addr) })
	return tlsConfigShared, tlsConfigErr
}

func (c *wireConn) close() { _ = c.raw.Close() }

// requests is how many requests the connection has sent. A connection is used
// by one goroutine at a time, so the count needs no lock.
func (c *wireConn) requests() int64 { return c.sent }
