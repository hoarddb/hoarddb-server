package main

import (
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// credentialsStub answers every login with AUTH_FAIL and the CREDENTIALS reason
// (or a nil payload, the wire contract's spelling of the same thing) and counts
// the logins that reach it.
type credentialsStub struct {
	nilPayload bool
	attempts   atomic.Int64
}

func (stub *credentialsStub) serve(t *testing.T) string {
	certificate := selfSignedCertificate(t)
	return serveStub(t, func() (net.Listener, error) {
		return tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
	}, func(conn net.Conn) {
		if _, err := transport.DecodeFrame(conn); err != nil {
			return
		}
		stub.attempts.Add(1)
		var payload []byte
		if !stub.nilPayload {
			var err error
			if payload, err = bson.Marshal(&server.AuthFailResponse{Reason: uint8(transport.AuthFailCredentials)}); err != nil {
				return
			}
		}
		_, _ = conn.Write(transport.NewFrame(transport.OpAuthFail, payload).Encode())
	})
}

// TestWrongPasswordFailsOnTheFirstAttempt: a rejected password is not a rate
// limit, so it is neither retried nor made to wait out a backoff.
func TestWrongPasswordFailsOnTheFirstAttempt(t *testing.T) {
	setLoginConcurrency(t, 4)
	setLoginRetry(t, 8, time.Second)
	for _, nilPayload := range []bool{false, true} {
		stub := &credentialsStub{nilPayload: nilPayload}
		started := time.Now()
		_, err := dialWire(stub.serve(t), "wrong", 5*time.Second)
		if err == nil {
			t.Fatal("a wrong password was accepted")
		}
		if got := stub.attempts.Load(); got != 1 {
			t.Errorf("nilPayload=%v: %d logins reached the server, want exactly 1", nilPayload, got)
		}
		if errors.Is(err, errLoginRateLimited) {
			t.Errorf("nilPayload=%v: a credentials failure is marked rate limited: %v", nilPayload, err)
		}
		if !strings.Contains(err.Error(), "-password") {
			t.Errorf("nilPayload=%v: %q does not tell the operator to check -password", nilPayload, err)
		}
		if time.Since(started) >= time.Second {
			t.Errorf("nilPayload=%v: took %v, so it waited out a backoff", nilPayload, time.Since(started))
		}
	}
}

// TestFailedLoginsReleaseTheirSlot: a failure of any kind gives the gate slot
// back. 20 sequential failures through a gate of 4 must all return; a slot leak
// is reported as a failure by name instead of hanging the package.
func TestFailedLoginsReleaseTheirSlot(t *testing.T) {
	failures := map[string]error{
		"plain failure":         errors.New("connection refused"),
		"rate limited, retried": errLoginRateLimited,
	}
	for name, failure := range failures {
		t.Run(name, func(t *testing.T) {
			gate := newLoginGate(4, 2, time.Microsecond)
			finished := make(chan int, 1)
			go func() {
				returned := 0
				for range 20 {
					if gate.run(func() error { return failure }) == nil {
						break
					}
					returned++
				}
				finished <- returned
			}()
			select {
			case returned := <-finished:
				if returned != 20 {
					t.Fatalf("%d of 20 failed logins returned an error", returned)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("20 sequential failed logins through a gate of 4 did not return: a failed login keeps its slot")
			}
		})
	}
}
