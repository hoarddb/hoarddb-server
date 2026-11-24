package cli

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/server"
)

// newRingTestHost starts the real server in process on a loopback port and returns the
// address it listens on and a client logged in as root. The port is reserved by listening on
// port 0 and closing, because the service does not report the port it bound.
func newRingTestHost(t *testing.T) (addr string, client *ringclient.Client) {
	t.Helper()
	return newRingTestHostOn(t, reserveLoopbackPort)
}

// reserveLoopbackPort returns a loopback address that was free a moment ago.
func reserveLoopbackPort() (string, error) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

// newRingTestHostOn is newRingTestHost with the port reservation passed in, so a test can make
// the reserved port be taken before the service binds it.
func newRingTestHostOn(t *testing.T, reserve func() (string, error)) (addr string, client *ringclient.Client) {
	t.Helper()
	addr, service := startRingTestService(t, reserve)
	t.Cleanup(func() { _ = service.Stop() })
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	client, err = ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "root", Password: "live-test-password", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return addr, client
}

// ringTestHostAttempts is how many ports newRingTestHostOn tries before it gives up.
const ringTestHostAttempts = 5

// startRingTestService reserves a port and starts the service on it. Another process can take
// the port between the reservation and the bind, so an address-in-use failure starts the whole
// attempt again with a fresh port and data directory.
func startRingTestService(t *testing.T, reserve func() (string, error)) (string, *server.Service) {
	t.Helper()
	for attempt := 1; ; attempt++ {
		addr, err := reserve()
		if err != nil {
			t.Fatal(err)
		}
		cfg := config.DefaultConfig()
		cfg.Storage.DataDir = t.TempDir()
		cfg.Server.Listen = "127.0.0.1:0"
		cfg.TCP.Listen = addr
		service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		if err := service.SeedRootUser("root", "live-test-password"); err != nil {
			t.Fatal(err)
		}
		err = service.Start()
		if err == nil {
			return addr, service
		}
		_ = service.Stop() // releases the data directory the failed start opened
		if !errors.Is(err, syscall.EADDRINUSE) || attempt == ringTestHostAttempts {
			t.Fatalf("starting the ring test host (attempt %d of %d): %v", attempt, ringTestHostAttempts, err)
		}
	}
}

// droppingRelay is a transparent TCP relay in front of the ring server that can lose one
// reply: after DropNextReply it forwards the next request and closes both sides as soon as
// the reply comes back, before forwarding it. The resend tests need exactly that: the server
// applied the request, and the client never learned whether it did.
type droppingRelay struct {
	upstream string
	listener net.Listener
	dropped  chan struct{}
	dropOnce sync.Once
	wg       sync.WaitGroup

	state sync.Mutex
	// armed loses the next reply; forwarded is set once the request that reply answers has
	// been passed upstream, so a byte the server sends unasked (a post-handshake TLS record)
	// is copied on instead of being mistaken for the reply.
	armed, forwarded bool

	// afterUpstreamWrite, when set, runs in forward right after the upstream write, so a test
	// can hold that goroutine there and decide what the server's reply does meanwhile.
	afterUpstreamWrite func()
}

// newDroppingRelay listens on a loopback port and forwards every connection to upstream.
func newDroppingRelay(t *testing.T, upstream string) *droppingRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	relay := &droppingRelay{upstream: upstream, listener: listener, dropped: make(chan struct{})}
	relay.wg.Add(1)
	go relay.accept()
	t.Cleanup(func() {
		_ = listener.Close()
		relay.wg.Wait()
	})
	return relay
}

// Addr is the address a client dials instead of the server's.
func (r *droppingRelay) Addr() string { return r.listener.Addr().String() }

// DropNextReply arms the relay to lose the next reply.
func (r *droppingRelay) DropNextReply() {
	r.state.Lock()
	defer r.state.Unlock()
	r.armed, r.forwarded = true, false
}

// Dropped is closed once a reply was lost, which tells a test that the client's error came
// from the dropped reply and not from some other end of the connection.
func (r *droppingRelay) Dropped() <-chan struct{} { return r.dropped }

func (r *droppingRelay) accept() {
	defer r.wg.Done()
	for {
		client, err := r.listener.Accept()
		if err != nil {
			return // the test's cleanup closed the listener
		}
		r.wg.Add(1)
		go r.pipe(client)
	}
}

// pipe serves one connection both ways and returns once both sides are closed.
func (r *droppingRelay) pipe(client net.Conn) {
	defer r.wg.Done()
	upstream, err := net.Dial("tcp", r.upstream)
	if err != nil {
		_ = client.Close()
		return
	}
	copied := make(chan struct{})
	go func() {
		defer close(copied)
		r.forward(upstream, client)
		_ = upstream.Close()
	}()
	r.reply(client, upstream)
	_ = client.Close()
	_ = upstream.Close()
	<-copied
}

// forward copies requests to the server and notes the first one that arrived after arming.
func (r *droppingRelay) forward(upstream, client net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := client.Read(buf)
		if n > 0 {
			// Noted before the write: the server can answer before Write returns here, and
			// a reply that finds the flag unset is copied to the client instead of dropped.
			// If the write fails the connection is ending anyway.
			r.state.Lock()
			if r.armed {
				r.forwarded = true
			}
			r.state.Unlock()
			if _, writeErr := upstream.Write(buf[:n]); writeErr != nil {
				return
			}
			if r.afterUpstreamWrite != nil {
				r.afterUpstreamWrite()
			}
		}
		if err != nil {
			return
		}
	}
}

// reply copies answers to the client, except the armed one, which is dropped: the pipe then
// closes both sides, so the client sees the connection end instead of the answer.
func (r *droppingRelay) reply(client, upstream net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := upstream.Read(buf)
		if n > 0 {
			if r.takeArmedReply() {
				r.dropOnce.Do(func() { close(r.dropped) })
				return
			}
			if _, writeErr := client.Write(buf[:n]); writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// takeArmedReply consumes the armed reply, once a request has been forwarded since arming.
func (r *droppingRelay) takeArmedReply() bool {
	r.state.Lock()
	defer r.state.Unlock()
	if !r.armed || !r.forwarded {
		return false
	}
	r.armed, r.forwarded = false, false
	return true
}

// TestRingTestHostCreatesDatabaseAndBucketAndReadsItBack is the helper's contract: one call
// starts the server in process, logs in as root and hands back an address and a client that
// writes and reads a document.
func TestRingTestHostCreatesDatabaseAndBucketAndReadsItBack(t *testing.T) {
	addr, client := newRingTestHost(t)
	if addr == "" {
		t.Fatal("newRingTestHost returned an empty address")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	users := client.Bucket("app", "User")
	if _, err := users.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	raw, err := users.Get(ctx, "ada")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := raw.Lookup("Name").StringValue(); got != "Ada" {
		t.Fatalf("read Name %q, want %q", got, "Ada")
	}
}

// TestDroppingRelayClosesAfterForwardingTheRequest: with the relay armed, the request reaches
// the server and the client sees the connection end instead of the reply.
func TestDroppingRelayClosesAfterForwardingTheRequest(t *testing.T) {
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = upstream.Close() })
	received := make(chan string, 1)
	go func() {
		conn, acceptErr := upstream.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 64)
		n, readErr := conn.Read(buf)
		if readErr != nil {
			return
		}
		received <- string(buf[:n])
		// The reply the relay must not forward.
		_, _ = conn.Write([]byte("reply"))
	}()

	relay := newDroppingRelay(t, upstream.Addr().String())
	client, err := net.Dial("tcp", relay.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	relay.DropNextReply()
	if _, err := client.Write([]byte("request")); err != nil {
		t.Fatalf("write the request: %v", err)
	}
	if err := client.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, readErr := client.Read(buf)
	if readErr == nil {
		t.Fatalf("the relay forwarded %q, want the connection closed before the reply", buf[:n])
	}
	select {
	case got := <-received:
		if got != "request" {
			t.Fatalf("the server received %q, want %q", got, "request")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the request never reached the server")
	}
	select {
	case <-relay.Dropped():
	case <-time.After(10 * time.Second):
		t.Fatal("the relay never dropped a reply")
	}
}

// TestDroppingRelayDropsAReplyThatBeatsTheForwardedFlag: the server's reply is already in the
// relay when the forward goroutine is still between the upstream write and its bookkeeping.
// The reply answers the armed request, so it must be dropped, not copied to the client.
func TestDroppingRelayDropsAReplyThatBeatsTheForwardedFlag(t *testing.T) {
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = upstream.Close() })
	go func() {
		conn, acceptErr := upstream.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 64)
		if _, readErr := conn.Read(buf); readErr != nil {
			return
		}
		_, _ = conn.Write([]byte("reply"))
	}()

	relay := newDroppingRelay(t, upstream.Addr().String())
	// The forward goroutine waits here until the client has seen what the relay did with the
	// reply, which orders the reply ahead of the flag without a sleep.
	clientSawOutcome := make(chan struct{})
	relay.afterUpstreamWrite = func() { <-clientSawOutcome }
	client, err := net.Dial("tcp", relay.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	relay.DropNextReply()
	if _, err := client.Write([]byte("request")); err != nil {
		t.Fatalf("write the request: %v", err)
	}
	if err := client.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, readErr := client.Read(buf)
	close(clientSawOutcome)
	if readErr == nil {
		t.Fatalf("the relay forwarded %q, want the connection closed before the reply", buf[:n])
	}
	select {
	case <-relay.Dropped():
	case <-time.After(10 * time.Second):
		t.Fatal("the relay never dropped the reply")
	}
}

// TestRingTestHostRetriesWhenItsReservedPortIsTaken (BUG-0917): another process takes the port
// between the reservation and the service's bind. The helper starts the host on a fresh port.
func TestRingTestHostRetriesWhenItsReservedPortIsTaken(t *testing.T) {
	thief, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = thief.Close() })
	reservations := 0
	reserve := func() (string, error) {
		reservations++
		if reservations == 1 {
			return thief.Addr().String(), nil // taken: the service cannot bind it
		}
		return reserveLoopbackPort()
	}
	addr, client := newRingTestHostOn(t, reserve)
	if addr == thief.Addr().String() {
		t.Fatal("the host reports the port another listener holds")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatalf("the host started after the retry does not serve: %v", err)
	}
}
