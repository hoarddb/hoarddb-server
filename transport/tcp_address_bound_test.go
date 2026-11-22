package transport

import (
	"bufio"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dialRawTCP opens a plain TCP connection that never starts TLS.
func dialRawTCP(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// refusedCount reads how many connections the listener has refused.
func refusedCount(ln *TCPListener) uint64 {
	ln.refuseMu.Lock()
	defer ln.refuseMu.Unlock()
	return ln.refused
}

// TestOneAddressCannotHoldEveryClientSlot is the regression test: the per-address cap
// is applied when a connection is accepted, so one address holds at most its
// cap of the client budget, whether or not its connections finish TLS.
func TestOneAddressCannotHoldEveryClientSlot(t *testing.T) {
	const budget, perAddress = 20, 4
	limiter := NewConnLimiter(budget, 4)
	ln := startTestTCPListenerWithLimiter(t, testTCPConfig(TCPConfig{MaxConnsPerIP: perAddress}), authEchoHandler, nil, limiter)

	raws := make([]net.Conn, 0, budget)
	for range budget {
		raws = append(raws, dialRawTCP(t, ln.Addr().String()))
	}
	deadline := time.Now().Add(5 * time.Second)
	for refusedCount(ln) < budget-perAddress {
		if clients, _ := limiter.Counts(); clients > perAddress {
			t.Fatalf("one address holds %d client slots, want at most its cap of %d", clients, perAddress)
		}
		if time.Now().After(deadline) {
			clients, _ := limiter.Counts()
			t.Fatalf("after 5s: %d refused, %d slots held; want %d refused at accept", refusedCount(ln), clients, budget-perAddress)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if clients, _ := limiter.Counts(); clients > perAddress {
		t.Fatalf("one address holds %d client slots, want at most its cap of %d", clients, perAddress)
	}

	// Once the address lets its connections go, a real client is served.
	for _, raw := range raws {
		_ = raw.Close()
	}
	for {
		if clients, _ := limiter.Counts(); clients == 0 {
			break
		}
		if time.Now().After(deadline.Add(5 * time.Second)) {
			t.Fatal("slots were not released after the raw connections closed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(NewFrame(OpAuth, []byte("any")).Encode()); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if answer := mustDecode(t, bufio.NewReader(conn)); answer.Opcode != OpAuthOK {
		t.Fatalf("auth answered %s, want AUTH_OK", answer.Opcode)
	}
}

// TestRefusalsInProgressAreBounded is the regression test: the connections being
// refused with a message are bounded in number; past maxPendingRefusals a
// refused connection is closed at once.
func TestRefusalsInProgressAreBounded(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{MaxConnsPerIP: 1}), authEchoHandler, nil)

	// maxPendingRefusals is 64; the literal keeps this test compiling against
	// a listener that has no such bound.
	const maxPendingRefusals, beyond = 64, 8
	raws := make([]net.Conn, 0, 1+maxPendingRefusals+beyond)
	for range cap(raws) {
		raws = append(raws, dialRawTCP(t, ln.Addr().String()))
	}
	// Read concurrently: once one read has waited out the deadline, a later
	// read on the same deadline returns a timeout without looking.
	readBy := time.Now().Add(3 * time.Second)
	var closed atomic.Int64
	var reads sync.WaitGroup
	for _, raw := range raws {
		reads.Go(func() {
			_ = raw.SetReadDeadline(readBy)
			var one [1]byte
			_, err := raw.Read(one[:])
			if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
				closed.Add(1)
			}
		})
	}
	reads.Wait()
	if closed := int(closed.Load()); closed < beyond {
		t.Fatalf("%d connections were closed at once, want at least the %d past the bound of %d refusals in progress",
			closed, beyond, maxPendingRefusals)
	}
}
