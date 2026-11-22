package server

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// QP-3's tests (query-pipelining spec §3.4 and §8: T-7, T-7b, T-7c, T-7d,
// T-7e and T-18). A query whose client has gone stops at its walk's next
// look at its context, so how far a walk got is how these tests tell a
// cancelled query from one that ran to its end.

const (
	// wideBucketEntries is how many documents a held query's bucket holds. A
	// walk that sees its cancellation stops at its first look at the
	// context, which is its first entry; one that does not walks them all.
	wideBucketEntries = 300
	// peerGoneSettle is how long a test waits after closing its connection
	// before it lets the held walks go on: the server's reader sees the EOF
	// within it.
	peerGoneSettle = 300 * time.Millisecond
)

// visitGate counts the entries every query method's walks visit, and holds
// each visit of one method while the gate is shut. queryVisitHook is
// process-global; no server test runs in parallel.
type visitGate struct {
	held     string
	arrived  atomic.Int64
	gateOpen chan struct{}
	opened   sync.Once
	mu       sync.Mutex
	visits   map[string]int
}

func holdQueryVisits(t *testing.T, method string) *visitGate {
	t.Helper()
	gate := &visitGate{held: method, gateOpen: make(chan struct{}), visits: make(map[string]int)}
	hook := func(visited string) {
		gate.mu.Lock()
		gate.visits[visited]++
		gate.mu.Unlock()
		if visited != gate.held {
			return
		}
		select {
		case <-gate.gateOpen:
		default:
			gate.arrived.Add(1)
			<-gate.gateOpen
		}
	}
	queryVisitHook.Store(&hook)
	t.Cleanup(func() {
		gate.open()
		queryVisitHook.Store(nil)
	})
	return gate
}

func (g *visitGate) open() {
	g.opened.Do(func() { close(g.gateOpen) })
}

func (g *visitGate) visitsOf(method string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.visits[method]
}

// waitHeld waits until want walks are held at their first entry.
func (g *visitGate) waitHeld(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for g.arrived.Load() < int64(want) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d %s walks reached their first entry within 10s", g.arrived.Load(), want, g.held)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func seedWideQueryBucket(t *testing.T, s *Server, bucket string) {
	t.Helper()
	createTestBucket(t, s, "app", bucket, BucketTypeHash)
	for i := 0; i < wideBucketEntries; i++ {
		insertScanDoc(t, s, "app", bucket, fmt.Sprintf("k%04d", i), map[string]interface{}{"n": int32(i)})
	}
}

// matchNothingScan walks every document of the wide bucket and matches none.
func matchNothingScan(t *testing.T) *transport.Frame {
	t.Helper()
	return queryFrame(t, transport.OpScan, ScanRequest{Database: "app", Bucket: "wide", Filter: map[string]interface{}{"n": -1}})
}

// abandonment is what abandonHeldScans saw once every slot was free.
type abandonment struct {
	held, visits int
	// cancelled is how far hoarddb_query_cancelled_total rose, and counted
	// whether the registry exports it at all.
	cancelled float64
	counted   bool
}

// abandonHeldScans is the scene of T-7 to T-7d: held SCANs on one
// connection, then extra frames, then the client goes away.
func abandonHeldScans(t *testing.T, mutate func(*config.Config), held int, extra func(c *queryConn)) abandonment {
	t.Helper()
	qs, addr := newTCPEnabledServer(t, mutate)
	seedWideQueryBucket(t, qs.core, "wide")
	gate := holdQueryVisits(t, queryMethodScan)
	client := mustDialQueryConn(t, addr, "admin", "test123")
	cancelledBefore, _ := cancelledQueries(t)

	for i := 0; i < held; i++ {
		if err := client.send(matchNothingScan(t)); err != nil {
			t.Fatal(err)
		}
	}
	gate.waitHeld(t, held)
	if extra != nil {
		extra(client)
	}
	if err := client.conn.Close(); err != nil {
		t.Fatalf("closing the client: %v", err)
	}
	time.Sleep(peerGoneSettle)
	gate.open()
	waitForZero(t, "the node's query slots", qs.core.queryInFlight)

	cancelledAfter, counted := cancelledQueries(t)
	return abandonment{held: held, visits: gate.visitsOf(queryMethodScan), cancelled: cancelledAfter - cancelledBefore, counted: counted}
}

// cancelledQueries reads hoarddb_query_cancelled_total, and whether the
// registry exports it.
func cancelledQueries(t *testing.T) (float64, bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "hoarddb_query_cancelled_total" && len(family.GetMetric()) == 1 {
			return family.GetMetric()[0].GetCounter().GetValue(), true
		}
	}
	return 0, false
}

// assertAbandoned fails unless each held walk stopped at its first entry and
// each was counted as cancelled.
func assertAbandoned(t *testing.T, seen abandonment, why string) {
	t.Helper()
	if seen.visits != seen.held {
		t.Fatalf("%d held SCAN(s) visited %d entries after their client had closed; each should have stopped at its first, "+
			"%d in all. A query whose client is gone must be abandoned (QP-3): %s", seen.held, seen.visits, seen.held, why)
	}
	if !seen.counted {
		t.Fatal("the registry exports no hoarddb_query_cancelled_total")
	}
	if seen.cancelled < float64(seen.held) {
		t.Fatalf("hoarddb_query_cancelled_total rose by %v for %d abandoned queries", seen.cancelled, seen.held)
	}
}

// TestQueryAbandonedOnConnectionClose is T-7: one SCAN held mid-walk, the
// client closes, and the walk stops at its next look at its context.
func TestQueryAbandonedOnConnectionClose(t *testing.T) {
	visits := abandonHeldScans(t, nil, 1, nil)
	assertAbandoned(t, visits, "serveConn drains the dispatches before it cancels the connection's context")
}

// TestQueryAbandonedWithFullWindow is T-7b: the window (4 frames) is full of
// held SCANs, so the reader cannot be waiting in its peek.
func TestQueryAbandonedWithFullWindow(t *testing.T) {
	visits := abandonHeldScans(t, nil, 4, nil)
	assertAbandoned(t, visits, "with the window full the reader waits in window.admit and never reads the EOF")
}

// TestQueryAbandonedAtPipelineFramesOne is T-7c: at a window of one frame,
// one query is a full window.
func TestQueryAbandonedAtPipelineFramesOne(t *testing.T) {
	visits := abandonHeldScans(t, func(cfg *config.Config) { cfg.TCP.PipelineFrames = 1 }, 1, nil)
	assertAbandoned(t, visits, "at HOARDDB_TCP_PIPELINE_FRAMES=1 one query fills the window, and the reader waits in window.admit")
}

// TestQueryAbandonedWhenPipelinedPastTheWindow is T-7d, over TLS like every
// driver connection: a fifth request sent past a full window is buffered,
// not executed, and the client then leaves.
func TestQueryAbandonedWhenPipelinedPastTheWindow(t *testing.T) {
	visits := abandonHeldScans(t, nil, 4, func(c *queryConn) {
		if err := c.send(matchNothingScan(t)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	})
	assertAbandoned(t, visits, "the fifth request must not run, and the four held ones must stop")
}

// TestQueryWaitingForASlotLeavesWithItsClient: a query waiting for an
// execution slot on a connection whose client has gone leaves the queue
// without running (spec §3.4, "Admission waits observe ctx too").
func TestQueryWaitingForASlotLeavesWithItsClient(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) { setQueryConcurrency(cfg, 1) })
	seedWideQueryBucket(t, qs.core, "wide")
	gate := holdQueryVisits(t, queryMethodScan)
	holder := mustDialQueryConn(t, addr, "admin", "test123")
	waiter := mustDialQueryConn(t, addr, "admin", "test123")
	cancelledBefore, _ := cancelledQueries(t)

	if err := holder.send(matchNothingScan(t)); err != nil {
		t.Fatal(err)
	}
	gate.waitHeld(t, 1)
	if err := waiter.send(queryFrame(t, transport.OpCount, CountRequest{Database: "app", Bucket: "wide", Filter: map[string]interface{}{"n": -1}})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := waiter.conn.Close(); err != nil {
		t.Fatalf("closing the waiting client: %v", err)
	}
	time.Sleep(peerGoneSettle)
	gate.open()
	if answer, err := holder.read(); err != nil || answer.Opcode != transport.OpScan {
		t.Fatalf("the slot's holder got %s, %v", opcodeOf(answer), err)
	}
	waitForZero(t, "the node's query slots", qs.core.queryInFlight)
	time.Sleep(100 * time.Millisecond)

	if counted := gate.visitsOf(queryMethodCount); counted != 0 {
		t.Fatalf("a COUNT whose client left while it waited for a slot ran once the slot freed (%d entries visited)", counted)
	}
	if cancelledAfter, counted := cancelledQueries(t); !counted || cancelledAfter-cancelledBefore < 1 {
		t.Fatalf("hoarddb_query_cancelled_total rose by %v (exported: %v) for a query cancelled in the admission queue", cancelledAfter-cancelledBefore, counted)
	}
}

// TestQueryPipelinedPastTheWindowStaysReadable is T-7d's second case. The
// window is full, a fifth request arrives whole and waits in the reader's
// buffer while the reader probes for more, and a sixth request's TLS record
// reaches the server in two halves. The slots free while only the first half
// has arrived, so the probe is interrupted in the middle of a record. Both
// requests must still be answered: the interrupt consumes nothing from the
// buffer, and a read timeout on a crypto/tls connection keeps the partial
// record for the next read.
func TestQueryPipelinedPastTheWindowStaysReadable(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, nil)
	seedWideQueryBucket(t, qs.core, "wide")
	gate := holdQueryVisits(t, queryMethodScan)
	client, split := dialSplittingQueryConn(t, addr, 600*time.Millisecond)

	for i := 0; i < 5; i++ {
		if err := client.send(matchNothingScan(t)); err != nil {
			t.Fatal(err)
		}
	}
	gate.waitHeld(t, 4)
	// The fifth is read into the buffer, and the reader probes past it.
	time.Sleep(100 * time.Millisecond)
	split.Store(true)
	sent := make(chan error, 1)
	go func() { sent <- client.send(matchNothingScan(t)) }()
	// The sixth's first half is on the wire; the slots free before the second.
	time.Sleep(200 * time.Millisecond)
	gate.open()

	_ = client.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for i := 1; i <= 6; i++ {
		answer, err := client.read()
		if err != nil {
			t.Fatalf("answer %d of 6: %v; a request read past the full window did not survive the interrupted probe", i, err)
		}
		if answer.Opcode != transport.OpScan {
			t.Fatalf("answer %d of 6 is %s: %s", i, answer.Opcode, answer.Payload)
		}
	}
	if err := <-sent; err != nil {
		t.Fatalf("sending the sixth request: %v", err)
	}
}

// TestQueryLongerThanIdleTimeoutStillAnswers is T-7e: the idle timeout
// measures the gap between requests, never a query's running time.
func TestQueryLongerThanIdleTimeoutStillAnswers(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) { cfg.TCP.IdleTimeoutSeconds = 1 })
	seedWideQueryBucket(t, qs.core, "wide")
	gate := holdQueryVisits(t, queryMethodScan)
	client := mustDialQueryConn(t, addr, "admin", "test123")

	if err := client.send(matchNothingScan(t)); err != nil {
		t.Fatal(err)
	}
	gate.waitHeld(t, 1)
	time.Sleep(2500 * time.Millisecond)
	gate.open()

	_ = client.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	answer, err := client.read()
	if err != nil {
		t.Fatalf("a SCAN that ran 2.5s on a connection with a 1s idle timeout was not answered: %v", err)
	}
	if answer.Opcode != transport.OpScan {
		t.Fatalf("a SCAN that ran 2.5s on a connection with a 1s idle timeout answered %s: %s", answer.Opcode, answer.Payload)
	}
	if err := client.send(queryFrame(t, transport.OpScan, ScanRequest{Database: "app", Bucket: "wide", Limit: 1})); err != nil {
		t.Fatalf("the connection did not stay up after the long query: %v", err)
	}
	if next, err := client.read(); err != nil || next.Opcode != transport.OpScan {
		t.Fatalf("the connection did not stay up after the long query: the next SCAN got %s, %v", opcodeOf(next), err)
	}
}

// TestFailedResponseWriteClosesConnection is T-18. The client stops reading,
// so a large page's write misses the frame deadline. The server must close
// the connection rather than keep reading, and must not run what the client
// sends after the failure.
func TestFailedResponseWriteClosesConnection(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) { cfg.TCP.FrameTimeoutSeconds = 1 })
	createTestBucket(t, qs.core, "app", "large", BucketTypeHash)
	page := make([]byte, 1<<20)
	for i := 0; i < 8; i++ {
		insertScanDoc(t, qs.core, "app", "large", fmt.Sprintf("big%d", i), map[string]interface{}{"blob": page})
	}
	seedWideQueryBucket(t, qs.core, "wide")
	gate := holdQueryVisits(t, "none")
	client := dialSmallReceiveBufferQueryConn(t, addr)

	// An 8 MiB page, more than the loopback buffers hold while nobody reads.
	if err := client.send(queryFrame(t, transport.OpScan, ScanRequest{Database: "app", Bucket: "large", Limit: 8})); err != nil {
		t.Fatal(err)
	}
	// The slot is released once the write has returned: here, once it failed.
	deadline := time.Now().Add(10 * time.Second)
	for gate.visitsOf(queryMethodScan) == 0 || qs.core.queryInFlight() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the large SCAN still holds its slot 10s after it was sent: its write never failed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	failedAt := time.Now()

	for i := 0; i < 3; i++ {
		// The connection may already be gone; a refused write is the point.
		_ = client.send(queryFrame(t, transport.OpCount, CountRequest{Database: "app", Bucket: "wide", Filter: map[string]interface{}{"n": -1}}))
	}
	closedErr := drainUntilClosed(client, failedAt.Add(time.Second))
	time.Sleep(200 * time.Millisecond)
	if counted := gate.visitsOf(queryMethodCount); counted != 0 {
		t.Errorf("COUNTs sent after the failed write ran (%d entries visited): the server kept reading a connection it could no longer answer", counted)
	}
	if closedErr != nil {
		t.Errorf("the server did not close the connection within 1s of the failed write: %v", closedErr)
	}
	waitForZero(t, "the node's query slots", qs.core.queryInFlight)
}

// drainUntilClosed reads and discards until the connection ends, and returns
// an error if it is still open at the deadline.
func drainUntilClosed(client *queryConn, deadline time.Time) error {
	_ = client.conn.SetReadDeadline(deadline)
	_, err := io.Copy(io.Discard, client.conn)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("still open at the deadline: %w", err)
	}
	return nil
}

// splitWriteConn writes each buffer in two halves with a pause between them
// while split is set, so the far end reads a TLS record in two parts.
type splitWriteConn struct {
	net.Conn
	split *atomic.Bool
	pause time.Duration
}

func (c *splitWriteConn) Write(p []byte) (int, error) {
	if !c.split.Load() || len(p) < 2 {
		return c.Conn.Write(p)
	}
	first, err := c.Conn.Write(p[:len(p)/2])
	if err != nil {
		return first, err
	}
	time.Sleep(c.pause)
	rest, err := c.Conn.Write(p[len(p)/2:])
	return first + rest, err
}

// dialSplittingQueryConn logs in over a connection whose writes are split in
// two once the returned flag is set.
func dialSplittingQueryConn(t *testing.T, addr string, pause time.Duration) (*queryConn, *atomic.Bool) {
	t.Helper()
	split := &atomic.Bool{}
	client := dialQueryConnOver(t, addr, func(raw net.Conn) net.Conn {
		return &splitWriteConn{Conn: raw, split: split, pause: pause}
	})
	return client, split
}

// dialSmallReceiveBufferQueryConn logs in over a connection whose receive
// buffer is as small as the kernel allows, so a response nobody reads fills it.
func dialSmallReceiveBufferQueryConn(t *testing.T, addr string) *queryConn {
	t.Helper()
	return dialQueryConnOver(t, addr, func(raw net.Conn) net.Conn {
		if tcp, ok := raw.(*net.TCPConn); ok {
			if err := tcp.SetReadBuffer(4096); err != nil {
				t.Fatalf("shrinking the receive buffer: %v", err)
			}
		}
		return raw
	})
}

// dialQueryConnOver is dialQueryConn over a TCP connection the caller may
// wrap before TLS runs on it.
func dialQueryConnOver(t *testing.T, addr string, wrap func(net.Conn) net.Conn) *queryConn {
	t.Helper()
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "server test driver client"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}
	conn := tls.Client(wrap(raw), tlsCfg)
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Handshake(); err != nil {
		t.Fatalf("TLS handshake with %s: %v", addr, err)
	}
	client := &queryConn{conn: conn, br: bufio.NewReader(conn)}
	login, err := bson.Marshal(&LoginRequest{Username: "admin", Password: "test123"})
	if err != nil {
		t.Fatalf("encoding the login: %v", err)
	}
	if err := client.send(transport.NewFrame(transport.OpAuth, login)); err != nil {
		t.Fatal(err)
	}
	answer, err := client.read()
	if err != nil {
		t.Fatal(err)
	}
	if answer.Opcode != transport.OpAuthOK {
		t.Fatalf("login answered %s: %s", answer.Opcode, answer.Payload)
	}
	return client
}
