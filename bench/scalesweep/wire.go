package main

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file is the part of hoardDB/bench/social's client (client.go, login.go)
// that the sweep needs: dial over TLS, log in, send one frame, read one reply.
// social is package main and cannot be imported, so the lines are copied, not
// re-designed; a change to the login or frame handling there belongs here too.

const (
	loginSlots        = 16
	loginAttempts     = 8
	loginFirstBackoff = 100 * time.Millisecond
	loginMaxBackoff   = 4 * time.Second
)

// errLoginRateLimited marks a login the node refused with RATE_LIMITED; no
// password was checked, so waiting and trying again is the right response.
var errLoginRateLimited = errors.New("login refused as RATE_LIMITED")

// logins admits a bounded number of logins at once: every worker of a leg
// dials together, and a default node refuses that many from one address.
var logins = make(chan struct{}, loginSlots)

// wireConn is one authenticated TLS socket, used by one goroutine at a time.
type wireConn struct {
	raw     *tls.Conn
	reader  *bufio.Reader
	timeout time.Duration
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

// isDesynced reports whether the socket can no longer be trusted. After a
// timeout the reply may still arrive and would be read as the next request's.
func isDesynced(err error) bool {
	var refusal *serverError
	return err != nil && !errors.As(err, &refusal)
}

var (
	tlsOnce   sync.Once
	tlsShared *tls.Config
	tlsErr    error
)

func sharedTLSConfig(addr string) (*tls.Config, error) {
	tlsOnce.Do(func() {
		tlsShared, tlsErr = transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "bench/scalesweep tool"}}, addr)
		if tlsErr != nil {
			tlsErr = fmt.Errorf("building TLS config: %w", tlsErr)
		}
	})
	return tlsShared, tlsErr
}

// dialWire opens one authenticated socket, retrying a RATE_LIMITED login with
// a doubling pause for about ten seconds.
func dialWire(addr, password string, timeout time.Duration) (*wireConn, error) {
	backoff := loginFirstBackoff
	for tried := 1; ; tried++ {
		logins <- struct{}{}
		conn, err := dialAndLogin(addr, password, timeout)
		<-logins
		if !errors.Is(err, errLoginRateLimited) {
			return conn, err
		}
		if tried >= loginAttempts {
			return nil, fmt.Errorf("%d login attempts, each refused: %w; lower -workers or check the node's login limits", tried, err)
		}
		time.Sleep(backoff)
		backoff = min(backoff*2, loginMaxBackoff)
	}
}

func dialAndLogin(addr, password string, timeout time.Duration) (*wireConn, error) {
	tlsConfig, err := sharedTLSConfig(addr)
	if err != nil {
		return nil, err
	}
	raw, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("dialing the hoardDB node at the -addr given: %w", err)
	}
	conn := &wireConn{raw: raw, reader: bufio.NewReader(raw), timeout: timeout}
	loginPayload, err := bson.Marshal(&server.LoginRequest{Username: "admin", Password: password})
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("encoding login: %w", err)
	}
	reply, err := conn.roundtrip(transport.OpAuth, loginPayload)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("logging in: %w", err)
	}
	if err := loginReplyError(reply); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return conn, nil
}

// loginReplyError turns a reply to OpAuth into an error, or nil on success.
func loginReplyError(reply *transport.Frame) error {
	switch reply.Opcode {
	case transport.OpAuthOK:
		return nil
	case transport.OpAuthFail:
		if len(reply.Payload) > 0 {
			var failure server.AuthFailResponse
			if err := bson.Unmarshal(reply.Payload, &failure); err == nil && transport.AuthFailReason(failure.Reason) == transport.AuthFailRateLimited {
				return fmt.Errorf("login refused: %s: %w", failure.Message, errLoginRateLimited)
			}
		}
		return fmt.Errorf("login rejected with %s; pass the root password with -password", reply.Opcode)
	case transport.OpError:
		var refusal server.ErrorResponse
		if err := bson.Unmarshal(reply.Payload, &refusal); err != nil {
			return fmt.Errorf("login refused with an undecodable error frame (%d bytes): %w", len(reply.Payload), err)
		}
		return fmt.Errorf("login refused by the server: %s", refusal.Message)
	default:
		return fmt.Errorf("login answered with unexpected opcode %s", reply.Opcode)
	}
}

func (c *wireConn) roundtrip(op transport.Opcode, payload []byte) (*transport.Frame, error) {
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

func (c *wireConn) close() { _ = c.raw.Close() }
