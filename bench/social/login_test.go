package main

import (
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// admissionStub answers OpAuth like a default hoardDB node under a login
// burst: a login that arrives while more than refuseAbove are being checked is
// refused as RATE_LIMITED, and every login takes checkTime. It counts the most
// it ever had in flight.
type admissionStub struct {
	refuseAbove int64
	checkTime   time.Duration
	// lingerAfterReply keeps the handler alive after its reply is written, like a
	// node that closes the connection and logs after answering.
	lingerAfterReply time.Duration
	inFlight         atomic.Int64
	peak             atomic.Int64
	refusals         atomic.Int64
}

func (stub *admissionStub) serve(t *testing.T) string {
	certificate := selfSignedCertificate(t)
	return serveStub(t, func() (net.Listener, error) {
		return tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
	}, func(conn net.Conn) {
		if _, err := transport.DecodeFrame(conn); err != nil {
			return
		}
		inFlight := stub.inFlight.Add(1)
		// The login ends for the client when it reads the reply, so the count
		// must drop before the reply is written: a decrement after it can land
		// after the client has freed its slot and started the next login.
		finished := sync.OnceFunc(func() { stub.inFlight.Add(-1) })
		defer finished()
		for peak := stub.peak.Load(); inFlight > peak && !stub.peak.CompareAndSwap(peak, inFlight); peak = stub.peak.Load() {
		}
		time.Sleep(stub.checkTime)
		finished()
		if inFlight > stub.refuseAbove {
			stub.refusals.Add(1)
			payload, err := bson.Marshal(&server.AuthFailResponse{Reason: uint8(transport.AuthFailRateLimited), Message: "logins in flight"})
			if err != nil {
				return
			}
			_, _ = conn.Write(transport.NewFrame(transport.OpAuthFail, payload).Encode())
			return
		}
		_, _ = conn.Write(transport.NewFrame(transport.OpAuthOK, nil).Encode())
		time.Sleep(stub.lingerAfterReply)
	})
}

// dialMany logs workers connections in at once and returns the errors.
func dialMany(addr string, workers int) []error {
	var group sync.WaitGroup
	failures := make(chan error, workers)
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			conn, err := dialWire(addr, "pw", 5*time.Second)
			if err != nil {
				failures <- err
				return
			}
			conn.close()
		}()
	}
	group.Wait()
	close(failures)
	var collected []error
	for err := range failures {
		collected = append(collected, err)
	}
	return collected
}

// TestDialWireAdmitsABurstOf128: 128 workers dialing at once against a node
// that refuses more than 20 logins in flight all get in, because the setup
// lets only a bounded number of logins through at a time.
func TestDialWireAdmitsABurstOf128(t *testing.T) {
	stub := &admissionStub{refuseAbove: 20, checkTime: 20 * time.Millisecond}
	failures := dialMany(stub.serve(t), 128)
	if len(failures) != 0 {
		t.Fatalf("%d of 128 logins failed, first: %v (stub refused %d, peak in flight %d)", len(failures), failures[0], stub.refusals.Load(), stub.peak.Load())
	}
	if peak := stub.peak.Load(); peak > 16 {
		t.Errorf("peak logins in flight %d, want at most 16", peak)
	}
}

// TestLoginConcurrencyIsConfigurable: -login-concurrency bounds the logins in flight.
func TestLoginConcurrencyIsConfigurable(t *testing.T) {
	setLoginConcurrency(t, 4)
	stub := &admissionStub{refuseAbove: 1000, checkTime: 10 * time.Millisecond}
	if failures := dialMany(stub.serve(t), 40); len(failures) != 0 {
		t.Fatalf("logins failed: %v", failures[0])
	}
	if peak := stub.peak.Load(); peak > 4 {
		t.Errorf("peak logins in flight %d with -login-concurrency 4", peak)
	}
}

// TestLoginConcurrencyHoldsWhenTheServerLingersAfterReplying: the bound is on
// logins the client has in flight, so a server that is slow to finish with a
// connection it has already answered is not counted as a login in flight.
func TestLoginConcurrencyHoldsWhenTheServerLingersAfterReplying(t *testing.T) {
	setLoginConcurrency(t, 4)
	stub := &admissionStub{refuseAbove: 1000, checkTime: 10 * time.Millisecond, lingerAfterReply: 30 * time.Millisecond}
	if failures := dialMany(stub.serve(t), 40); len(failures) != 0 {
		t.Fatalf("logins failed: %v", failures[0])
	}
	if peak := stub.peak.Load(); peak > 4 {
		t.Errorf("peak logins in flight %d with -login-concurrency 4", peak)
	}
}

// TestLoginGateNeverAdmitsMoreThanItsSlots measures the gate on the client
// side, with no server: the attempts running inside run at once never exceed
// the slots, however many callers wait.
func TestLoginGateNeverAdmitsMoreThanItsSlots(t *testing.T) {
	const slots = 4
	gate := newLoginGate(slots, 1, time.Millisecond)
	var running, peak atomic.Int64
	var group sync.WaitGroup
	for range 200 {
		group.Add(1)
		go func() {
			defer group.Done()
			err := gate.run(func() error {
				now := running.Add(1)
				for seen := peak.Load(); now > seen && !peak.CompareAndSwap(seen, now); seen = peak.Load() {
				}
				time.Sleep(time.Millisecond)
				running.Add(-1)
				return nil
			})
			if err != nil {
				t.Errorf("attempt failed: %v", err)
			}
		}()
	}
	group.Wait()
	if got := peak.Load(); got > slots {
		t.Errorf("gate admitted %d attempts at once, want at most %d", got, slots)
	}
	if got := peak.Load(); got < slots {
		t.Errorf("gate admitted at most %d attempts at once, want it to reach %d", got, slots)
	}
}

// TestRateLimitedLoginIsRetriedThenFailsClearly: a login the node keeps
// refusing is tried the configured number of times, then ends the run with a
// message that names the reason and the flag.
func TestRateLimitedLoginIsRetriedThenFailsClearly(t *testing.T) {
	setLoginConcurrency(t, 1)
	setLoginRetry(t, 4, time.Millisecond)
	always := &admissionStub{refuseAbove: 0}
	failures := dialMany(always.serve(t), 1)
	if len(failures) != 1 {
		t.Fatal("a login the node always refuses was accepted")
	}
	if got := always.refusals.Load(); got != 4 {
		t.Errorf("tried %d times, want the 4 attempts configured", got)
	}
	message := failures[0].Error()
	for _, want := range []string{"RATE_LIMITED", "-login-concurrency"} {
		if !strings.Contains(message, want) {
			t.Errorf("message %q lacks %q", message, want)
		}
	}
}

// TestRateLimitedLoginSucceedsOnRetry: a refusal that clears is retried into a success.
func TestRateLimitedLoginSucceedsOnRetry(t *testing.T) {
	setLoginConcurrency(t, 8)
	setLoginRetry(t, 6, 5*time.Millisecond)
	stub := &admissionStub{refuseAbove: 2, checkTime: 15 * time.Millisecond}
	if failures := dialMany(stub.serve(t), 8); len(failures) != 0 {
		t.Fatalf("a refusal that clears should be retried: %v", failures[0])
	}
	if stub.refusals.Load() == 0 {
		t.Fatal("the stub never refused, so the retry was not exercised")
	}
}

// setLoginConcurrency swaps the process-wide login gate for the test.
func setLoginConcurrency(t *testing.T, slots int) {
	t.Helper()
	previous := logins
	logins = newLoginGate(slots, previous.attempts, previous.firstBackoff)
	t.Cleanup(func() { logins = previous })
}

// setLoginRetry swaps the retry budget of the login gate for the test.
func setLoginRetry(t *testing.T, attempts int, firstBackoff time.Duration) {
	t.Helper()
	previous := logins
	logins = newLoginGate(cap(previous.slots), attempts, firstBackoff)
	t.Cleanup(func() { logins = previous })
}
