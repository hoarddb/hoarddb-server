package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// QP-1's tests (query-pipelining spec §8 T-8, T-9, T-10, T-10b, T-10c) and the
// build doc's TestQuerySlotReleasedOnEveryPath.

// setQueryConcurrency sets the node's query slots on a test configuration.
// It is set after the clamp, so a test may use fewer than the four a
// configured value is clamped to.
func setQueryConcurrency(cfg *config.Config, slots int) {
	cfg.Query.Concurrency = slots
}

// fireWrittenHook is ServeFrame's part for a test that calls the admission
// wrapper directly: the written frame fires its hook.
func fireWrittenHook(frame *transport.Frame) {
	if hook := frame.TakeWrittenHook(); hook != nil {
		hook()
	}
}

// queryConn is a raw, logged-in driver connection that returns errors rather
// than failing the test, so many of them can run on their own goroutines.
type queryConn struct {
	conn *tls.Conn
	br   *bufio.Reader
}

func dialQueryConn(addr, user, password string) (*queryConn, error) {
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "server test driver client"}}, addr)
	if err != nil {
		return nil, fmt.Errorf("client TLS config: %w", err)
	}
	conn, err := tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("dialling %s: %w", addr, err)
	}
	qc := &queryConn{conn: conn, br: bufio.NewReader(conn)}
	payload, err := bson.Marshal(&LoginRequest{Username: user, Password: password})
	if err != nil {
		return nil, fmt.Errorf("encoding the login: %w", err)
	}
	if err := qc.send(transport.NewFrame(transport.OpAuth, payload)); err != nil {
		return nil, err
	}
	answer, err := qc.read()
	if err != nil {
		return nil, err
	}
	if answer.Opcode != transport.OpAuthOK {
		return nil, fmt.Errorf("login answered %s: %s", answer.Opcode, answer.Payload)
	}
	return qc, nil
}

func (c *queryConn) send(frame *transport.Frame) error {
	if _, err := c.conn.Write(frame.Encode()); err != nil {
		return fmt.Errorf("writing %s: %w", frame.Opcode, err)
	}
	return nil
}

func (c *queryConn) read() (*transport.Frame, error) {
	answer, err := transport.DecodeFrame(c.br)
	if err != nil {
		return nil, fmt.Errorf("reading an answer: %w", err)
	}
	return answer, nil
}

// readAsync reads one answer on its own goroutine.
func (c *queryConn) readAsync() <-chan frameOrError {
	answers := make(chan frameOrError, 1)
	go func() {
		answer, err := c.read()
		answers <- frameOrError{frame: answer, err: err}
	}()
	return answers
}

type frameOrError struct {
	frame *transport.Frame
	err   error
}

func mustDialQueryConn(t *testing.T, addr, user, password string) *queryConn {
	t.Helper()
	qc, err := dialQueryConn(addr, user, password)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = qc.conn.Close() })
	return qc
}

func queryFrame(t *testing.T, op transport.Opcode, request interface{}) *transport.Frame {
	t.Helper()
	payload, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("encoding %s: %v", op, err)
	}
	return transport.NewFrame(op, payload)
}

// walkHold holds the first walk of one query method at its first entry until
// released, so a test can keep a query's slot taken. queryVisitHook is
// process-global; no server test runs in parallel.
type walkHold struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	closed  atomic.Bool
}

func holdFirstWalk(t *testing.T, method string) *walkHold {
	t.Helper()
	hold := &walkHold{entered: make(chan struct{}), release: make(chan struct{})}
	hook := func(visited string) {
		if visited != method {
			return
		}
		first := false
		hold.once.Do(func() { first = true })
		if first {
			close(hold.entered)
			<-hold.release
		}
	}
	queryVisitHook.Store(&hook)
	t.Cleanup(func() {
		hold.let()
		queryVisitHook.Store(nil)
	})
	return hold
}

func (h *walkHold) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-h.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the held query never reached its walk")
	}
}

func (h *walkHold) let() {
	if h.closed.CompareAndSwap(false, true) {
		close(h.release)
	}
}

func seedSmallQueryBucket(t *testing.T, s *Server, bucket string) {
	t.Helper()
	createTestBucket(t, s, "app", bucket, BucketTypeHash)
	for i := 0; i < 10; i++ {
		insertScanDoc(t, s, "app", bucket, fmt.Sprintf("k%02d", i), map[string]interface{}{"n": int32(i)})
	}
}

// TestQueryAdmissionIsBackpressureNotError is T-8: with one query slot held
// by connection A's SCAN, connection B's SCAN gets no answer until A is
// released, and then a success — never OpError.
func TestQueryAdmissionIsBackpressureNotError(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) { setQueryConcurrency(cfg, 1) })
	seedSmallQueryBucket(t, qs.core, "events")
	hold := holdFirstWalk(t, queryMethodScan)

	a := mustDialQueryConn(t, addr, "admin", "test123")
	b := mustDialQueryConn(t, addr, "admin", "test123")
	scan := ScanRequest{Database: "app", Bucket: "events", Limit: 5}
	if err := a.send(queryFrame(t, transport.OpScan, scan)); err != nil {
		t.Fatal(err)
	}
	hold.waitEntered(t)
	answerA := a.readAsync()

	if err := b.send(queryFrame(t, transport.OpScan, scan)); err != nil {
		t.Fatal(err)
	}
	answerB := b.readAsync()
	select {
	case got := <-answerB:
		t.Fatalf("B's SCAN was answered (%v, %v) while A held the only query slot: nothing admits queries (QP-1)", opcodeOf(got.frame), got.err)
	case <-time.After(500 * time.Millisecond):
	}

	hold.let()
	for name, answer := range map[string]<-chan frameOrError{"A": answerA, "B": answerB} {
		select {
		case got := <-answer:
			if got.err != nil {
				t.Fatalf("%s's SCAN: %v", name, got.err)
			}
			if got.frame.Opcode != transport.OpScan {
				t.Fatalf("%s's SCAN answered %s (%s): a query waiting for a slot must be answered, never refused (R-5)",
					name, got.frame.Opcode, got.frame.Payload)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s's SCAN was not answered within 10s of A's release", name)
		}
	}
}

func opcodeOf(frame *transport.Frame) string {
	if frame == nil {
		return "no frame"
	}
	return frame.Opcode.String()
}

// TestPointReadsBypassQueryAdmission is T-9, a regression guard for R-7:
// with the only query slot held, GET, GET_FAST and EXISTS still complete.
func TestPointReadsBypassQueryAdmission(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) { setQueryConcurrency(cfg, 1) })
	seedSmallQueryBucket(t, qs.core, "events")
	hold := holdFirstWalk(t, queryMethodScan)

	a := mustDialQueryConn(t, addr, "admin", "test123")
	if err := a.send(queryFrame(t, transport.OpScan, ScanRequest{Database: "app", Bucket: "events"})); err != nil {
		t.Fatal(err)
	}
	hold.waitEntered(t)

	c := mustDialQueryConn(t, addr, "admin", "test123")
	getFast, err := transport.MarshalGetFast(transport.GetFastRequest{Database: "app", Bucket: "events", Key: "k01"})
	if err != nil {
		t.Fatalf("encoding GET_FAST: %v", err)
	}
	// R-7 is that a point read never waits on query admission: each is
	// answered promptly, with its own response and not an error.
	pointReads := []struct {
		name  string
		frame *transport.Frame
		want  transport.Opcode
	}{
		{"GET", queryFrame(t, transport.OpGet, FindRequest{Database: "app", Bucket: "events", Key: "k01"}), transport.OpGetResponse},
		{"GET_FAST", transport.NewFrame(transport.OpGetFast, getFast), transport.OpGetFastResponse},
		{"EXISTS", queryFrame(t, transport.OpExists, FindRequest{Database: "app", Bucket: "events", Key: "k01"}), transport.OpExistsResponse},
	}
	for _, read := range pointReads {
		if err := c.send(read.frame); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-c.readAsync():
			if got.err != nil {
				t.Fatalf("%s: %v", read.name, got.err)
			}
			if got.frame.Opcode != read.want {
				t.Fatalf("%s answered %s (%q); want %s", read.name, got.frame.Opcode, got.frame.Payload, read.want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not complete within 5s while a SCAN held the only query slot: point reads wait on query admission (R-7)", read.name)
		}
	}
}

// peakSampler samples its probes at a fixed interval until stopped and keeps
// the largest value of each.
type peakSampler struct {
	stop  chan struct{}
	done  chan struct{}
	peaks []atomic.Int64
}

func startPeakSampler(probes ...func() int64) *peakSampler {
	return startPeakSamplerEvery(time.Millisecond, probes...)
}

// startPeakSamplerEvery is startPeakSampler with a sampling interval, for a
// probe that costs more than a millisecond to read.
func startPeakSamplerEvery(interval time.Duration, probes ...func() int64) *peakSampler {
	sampler := &peakSampler{stop: make(chan struct{}), done: make(chan struct{}), peaks: make([]atomic.Int64, len(probes))}
	go func() {
		defer close(sampler.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			for i, probe := range probes {
				if value := probe(); value > sampler.peaks[i].Load() {
					sampler.peaks[i].Store(value)
				}
			}
			select {
			case <-sampler.stop:
				return
			case <-ticker.C:
			}
		}
	}()
	return sampler
}

func (p *peakSampler) finish() []int64 {
	close(p.stop)
	<-p.done
	peaks := make([]int64, len(p.peaks))
	for i := range p.peaks {
		peaks[i] = p.peaks[i].Load()
	}
	return peaks
}

// pipelinedQueryLoad is T-10's and T-10b's client side: conns connections
// each pipeline one request per frame, and a reader answers each in turn,
// sleeping between reads when slow. abandon connections close after sending
// without reading, so their answers' writes fail on the server.
type pipelinedQueryLoad struct {
	addr, user, password string
	abandon              int
	// frames holds each connection's pipelined requests, built on the test
	// goroutine.
	frames    [][]*transport.Frame
	readDelay time.Duration
	// readAfter is how long each connection waits, after sending, before
	// its first read.
	readAfter time.Duration
}

// connect dials and logs in one connection per entry of frames. It is kept
// apart from the load because a login is an Argon2 derivation of 64 MiB
// (config Argon2Memory): 32 of them inside a measured window are 2 GiB of
// heap that has nothing to do with queries.
func (load pipelinedQueryLoad) connect() ([]*queryConn, error) {
	conns := make([]*queryConn, len(load.frames))
	for i := range conns {
		qc, err := dialQueryConn(load.addr, load.user, load.password)
		if err != nil {
			for _, opened := range conns[:i] {
				_ = opened.conn.Close()
			}
			return nil, fmt.Errorf("connection %d: %w", i, err)
		}
		conns[i] = qc
	}
	return conns, nil
}

// run is connect then runOn.
func (load pipelinedQueryLoad) run() error {
	conns, err := load.connect()
	if err != nil {
		return err
	}
	return load.runOn(conns)
}

// runOn sends each connection's frames and reads its answers, and returns
// every connection's error, joined: a failed send or read, an answer to
// nothing, or a second answer to one request. It closes the connections.
func (load pipelinedQueryLoad) runOn(conns []*queryConn) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(conns))
	for conn, qc := range conns {
		wg.Add(1)
		go func(conn int, qc *queryConn) {
			defer wg.Done()
			// A close error after the answers are read changes no result.
			defer func() { _ = qc.conn.Close() }()
			frames := load.frames[conn]
			sent := map[[transport.RequestIDLen]byte]bool{}
			for _, frame := range frames {
				if err := qc.send(frame); err != nil {
					errs <- fmt.Errorf("connection %d: %w", conn, err)
					return
				}
				sent[frame.RequestID] = true
			}
			if conn < load.abandon {
				return
			}
			time.Sleep(load.readAfter)
			if err := qc.conn.SetReadDeadline(time.Now().Add(3 * time.Minute)); err != nil {
				errs <- err
				return
			}
			for range frames {
				time.Sleep(load.readDelay)
				answer, err := qc.read()
				if err != nil {
					errs <- fmt.Errorf("connection %d: %w", conn, err)
					return
				}
				if !sent[answer.RequestID] {
					errs <- fmt.Errorf("connection %d: an answer for a request it did not send, or a second answer for one it did", conn)
					return
				}
				delete(sent, answer.RequestID)
			}
		}(conn, qc)
	}
	wg.Wait()
	close(errs)
	return errors.Join(drain(errs)...)
}

func drain(errs <-chan error) []error {
	var all []error
	for err := range errs {
		all = append(all, err)
	}
	return all
}

// boundedLoadFrames is four pipelined scans per connection over the bucket:
// sorted and filtered, with pages near the 1 MiB ceiling and some over it.
func boundedLoadFrames(t *testing.T, conns int, database, bucket string) [][]*transport.Frame {
	t.Helper()
	all := make([][]*transport.Frame, conns)
	for conn := range all {
		for _, request := range []ScanRequest{
			{Database: database, Bucket: bucket, Sort: map[string]int{"n": -1}, Skip: conn * 10, Limit: 900},
			{Database: database, Bucket: bucket, Filter: map[string]interface{}{"group": "g1"}, Limit: 900},
			{Database: database, Bucket: bucket, Sort: map[string]int{"n": 1}, Limit: 1100},
			{Database: database, Bucket: bucket, Filter: map[string]interface{}{"group": "g2"}, Skip: 100, Limit: 700},
		} {
			all[conn] = append(all[conn], queryFrame(t, transport.OpScan, request))
		}
	}
	return all
}

// repeatedScans is conns connections each pipelining perConn scans.
func repeatedScans(t *testing.T, conns, perConn int, request func(conn, i int) ScanRequest) [][]*transport.Frame {
	t.Helper()
	all := make([][]*transport.Frame, conns)
	for conn := range all {
		for i := 0; i < perConn; i++ {
			all[conn] = append(all[conn], queryFrame(t, transport.OpScan, request(conn, i)))
		}
	}
	return all
}

// waitForZero polls probe until it reads 0, or fails after five seconds.
func waitForZero(t *testing.T, what string, probe func() int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for probe() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s is still %d five seconds after the last answer: a slot was not released", what, probe())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const (
	boundedSlots   = 4
	boundedCeiling = 1 << 20
)

// TestQueryMemoryBoundedAtCeiling is T-10, the boundedness test: S = 4 slots,
// C = 1 MiB, 32 connections × W = 4 pipelined sorted and filtered scans of a
// 20,000 × 1 KiB bucket, some pages over C, some connections abandoned.
func TestQueryMemoryBoundedAtCeiling(t *testing.T) {
	if testing.Short() {
		t.Skip("128 scans of a 20,000-document bucket")
	}
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) {
		cfg.TCP.PipelineFrames = 4
		setQueryConcurrency(cfg, boundedSlots)
		setQueryCeiling(cfg, boundedCeiling)
		// Badger allocates a memtable's arena (about 1.3 x its size) on the Go
		// heap, so one more memtable allocated during the run adds 166 MiB to the
		// live heap at the 128 MiB default and 42 MiB at the floor. The bound
		// below has 64 MiB of slack, sized for the floor (a run under -race over
		// the full package measured 171.8 MiB at the default).
		cfg.Storage.MemtableMB = config.MinMemtableMB
	})
	createTestBucket(t, qs.core, "app", "retained", BucketTypeHash)
	seedRetentionBucket(t, qs.core, "app", "retained", 20000)

	var worstHighWater atomic.Int64
	budgetHook := func(_ string, _, highWater int64) {
		if highWater > worstHighWater.Load() {
			worstHighWater.Store(highWater)
		}
	}
	queryBudgetHook.Store(&budgetHook)
	t.Cleanup(func() { queryBudgetHook.Store(nil) })

	load := pipelinedQueryLoad{
		addr: addr, user: "admin", password: "test123", abandon: 4,
		frames: boundedLoadFrames(t, 32, "app", "retained"),
	}
	conns, err := load.connect()
	if err != nil {
		t.Fatal(err)
	}
	baseline := qs.core.queryRetainedBytes()
	slotSampler := startPeakSampler(func() int64 { return int64(qs.core.queryInFlight()) })
	byteSampler := startPeakSampler(func() int64 { return qs.core.queryRetainedBytes() })
	loadErr := load.runOn(conns)
	peaks := append(slotSampler.finish(), byteSampler.finish()...)
	if loadErr != nil {
		t.Fatalf("every request is answered exactly once: %v", loadErr)
	}

	if peaks[0] > boundedSlots {
		t.Errorf("%d queries held slots at once; at most %d may (QP-1)", peaks[0], boundedSlots)
	}
	if worstHighWater.Load() > boundedCeiling {
		t.Errorf("a query's budget held %d bytes, over the %d-byte ceiling", worstHighWater.Load(), boundedCeiling)
	}
	// The measured bytes are the retained elements, one copy of the role
	// table's three, so the reached bound is S × C.
	if bound := int64(boundedSlots * boundedCeiling); peaks[1]-baseline > bound {
		t.Errorf("queries held %.1f MiB at once; S = %d slots of C = %d MiB allow %.1f MiB (S × C): "+
			"query memory is not bounded by S × C", float64(peaks[1]-baseline)/(1<<20), boundedSlots, boundedCeiling>>20, float64(bound)/(1<<20))
	}
	waitForZero(t, "the query in-flight count", qs.core.queryInFlight)
}

// TestForwardedResultsBoundedOnCoordinator is T-10b: on a two-node cluster
// with RF = 1, clients of the coordinator pipeline forwarded sorted scans
// with pages near C and hold off reading. The coordinator holds at most S
// forwarded results at once.
//
// The spec's load is 32 connections × W = 4. On loopback that cannot hold a
// result on the coordinator: a connection's four answers of 0.9 MiB fit its
// kernel socket buffers, so every write returns and nothing waits in the
// heap, with or without the bound (measured: T-10b passed on its parent).
// Four connections × W = 64, each waiting a second before reading, queue
// about 58 MiB per connection, far past what the kernel absorbs, so the
// coordinator's writes block and what it holds is what the bound governs.
func TestForwardedResultsBoundedOnCoordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("256 forwarded scans")
	}
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) {
		cfg.TCP.PipelineFrames = 64
		setQueryConcurrency(cfg, boundedSlots)
		setQueryCeiling(cfg, boundedCeiling)
	})
	defer tc.stop()
	const coordinator = 0
	bucket := bucketNameOwnedBy(t, tc.nodes[coordinator].server.core.cluster, "app", tc.addrs[1])
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(coordinator, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	owner := tc.nodes[1].server.core
	seedRetentionBucket(t, owner, "app", bucket, 5000)

	coordinatorCore := tc.nodes[coordinator].server.core
	load := pipelinedQueryLoad{
		addr: tc.driverAddrs[coordinator], user: testRootUser, password: testRootPassword,
		readAfter: time.Second,
		frames: repeatedScans(t, 4, 64, func(conn, i int) ScanRequest {
			return ScanRequest{Database: "app", Bucket: bucket, Sort: map[string]int{"n": -1}, Skip: conn + i, Limit: 900}
		}),
	}
	conns, err := load.connect()
	if err != nil {
		t.Fatal(err)
	}
	baseline := coordinatorCore.forwardedResultBytes()
	slotSampler := startPeakSampler(func() int64 { return int64(coordinatorCore.forwardInFlight()) })
	byteSampler := startPeakSampler(func() int64 { return coordinatorCore.forwardedResultBytes() })
	loadErr := load.runOn(conns)
	peaks := append(slotSampler.finish(), byteSampler.finish()...)
	if loadErr != nil {
		t.Fatalf("every request is answered exactly once: %v", loadErr)
	}
	if peaks[0] > boundedSlots {
		t.Errorf("the coordinator held %d forwarded results at once; at most %d may (§3.1.1)", peaks[0], boundedSlots)
	}
	// The measured bytes are the client-frame payload held per forwarded
	// result, one copy of the role table's three, so the reached bound is
	// S × C and not 3 × S × C + 24 × C (BUG-0229: measured 3.6 MiB bounded,
	// 10.0 MiB with the cap removed).
	if bound := int64(boundedSlots * boundedCeiling); peaks[1]-baseline > bound {
		t.Errorf("the coordinator held %.1f MiB of forwarded results; S = %d slots of C = %d MiB allow %.1f MiB (S × C): "+
			"nothing bounds the forwarded results a coordinator holds (N-4)", float64(peaks[1]-baseline)/(1<<20), boundedSlots, boundedCeiling>>20, float64(bound)/(1<<20))
	}
	waitForZero(t, "the coordinator's forwarded in-flight count", coordinatorCore.forwardInFlight)
}

// TestCrossForwardingDoesNotDeadlock is T-10c, a regression guard for RC-2:
// nodes X and Y each own a bucket the other's clients query, with one slot of
// each kind on both, and every owner's walk held briefly. Every request
// completes within 10s.
func TestCrossForwardingDoesNotDeadlock(t *testing.T) {
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) {
		cfg.TCP.PipelineFrames = 4
		setQueryConcurrency(cfg, 1)
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	bucketOnY := bucketNameOwnedBy(t, tc.nodes[0].server.core.cluster, "app", tc.addrs[1])
	bucketOnX := bucketNameOwnedBy(t, tc.nodes[0].server.core.cluster, "app", tc.addrs[0])
	for _, bucket := range []string{bucketOnX, bucketOnY} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
		for i := 0; i < 5; i++ {
			tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: fmt.Sprintf("k%d", i), Data: map[string]interface{}{"n": int32(i)}})
		}
	}
	slowWalk := func(method string) {
		if method == queryMethodScan {
			time.Sleep(2 * time.Millisecond)
		}
	}
	queryVisitHook.Store(&slowWalk)
	t.Cleanup(func() { queryVisitHook.Store(nil) })

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for node, bucket := range map[int]string{0: bucketOnY, 1: bucketOnX} {
		load := pipelinedQueryLoad{
			addr: tc.driverAddrs[node], user: testRootUser, password: testRootPassword,
			frames: repeatedScans(t, 4, 4, func(int, int) ScanRequest { return ScanRequest{Database: "app", Bucket: bucket} }),
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- load.run()
		}()
	}
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(errs)
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("cross-forwarded queries did not all complete within 10s: X and Y deadlocked on their slots (RC-2)")
	}
	if err := errors.Join(drain(errs)...); err != nil {
		t.Fatal(err)
	}
}

// TestQuerySlotReleasedOnEveryPath walks every row of spec §3.1's "Release,
// per path" table and checks that no slot outlives its path.
func TestQuerySlotReleasedOnEveryPath(t *testing.T) {
	t.Run("the admission wrapper", func(t *testing.T) {
		s := newTestServer(t)
		scan := &transport.Frame{Opcode: transport.OpScan}
		rows := []struct {
			name string
			run  func(ctx context.Context) (*transport.Frame, error)
		}{
			{"handler error", func(context.Context) (*transport.Frame, error) {
				return transport.NewFrameWithID(transport.OpError, scan.RequestID, nil), errors.New("a handler's own error")
			}},
			{"nil frame", func(context.Context) (*transport.Frame, error) { return nil, nil }},
			{"success", func(context.Context) (*transport.Frame, error) {
				return transport.NewFrameWithID(transport.OpScan, scan.RequestID, nil), nil
			}},
			{"errorResponse frame", func(context.Context) (*transport.Frame, error) {
				return errorResponse(scan, errors.New("bucket not found"))
			}},
		}
		for _, row := range rows {
			resp, err := s.admitQuery(context.Background(), scan, row.run)
			if resp != nil && err == nil {
				// ServeFrame's part: the written frame fires its hook.
				fireWrittenHook(resp)
			}
			if n := s.queryInFlight(); n != 0 {
				t.Errorf("%s: %d query slot(s) still held after the path ended", row.name, n)
			}
		}

		// An admission wait cancelled by its context takes no slot, and a
		// query already holding one keeps it.
		held, err := s.admitQuery(context.Background(), scan, func(context.Context) (*transport.Frame, error) {
			return transport.NewFrameWithID(transport.OpScan, scan.RequestID, nil), nil
		})
		if err != nil {
			t.Fatalf("admitting the holder: %v", err)
		}
		for s.queryInFlight() < queryConcurrencyOf(s) {
			if _, err := s.admitQuery(context.Background(), scan, func(context.Context) (*transport.Frame, error) {
				return transport.NewFrameWithID(transport.OpScan, scan.RequestID, nil), nil
			}); err != nil {
				t.Fatalf("filling the pool: %v", err)
			}
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := s.admitQuery(cancelled, scan, func(context.Context) (*transport.Frame, error) {
			t.Error("a query whose admission wait was cancelled ran")
			return nil, nil
		}); !errors.Is(err, context.Canceled) {
			t.Errorf("a cancelled admission wait returned %v, want context.Canceled", err)
		}
		fireWrittenHook(held)
		if n, want := s.queryInFlight(), queryConcurrencyOf(s)-1; n != want {
			t.Errorf("after a cancelled wait and one release, %d slots are held, want %d", n, want)
		}
	})

	t.Run("unauthenticated frame", func(t *testing.T) {
		qs, addr := newTCPEnabledServer(t, nil)
		seedSmallQueryBucket(t, qs.core, "events")
		c := dialTCPServer(t, addr)
		if _, err := c.conn.Write(queryFrame(t, transport.OpScan, ScanRequest{Database: "app", Bucket: "events"}).Encode()); err != nil {
			t.Fatal(err)
		}
		answer, err := transport.DecodeFrame(c.br)
		if err != nil || answer.Opcode != transport.OpError {
			t.Fatalf("an unauthenticated SCAN answered %v, %v; want OpError", opcodeOf(answer), err)
		}
		if n := qs.core.queryInFlight(); n != 0 {
			t.Errorf("an unauthenticated SCAN left %d query slot(s) held", n)
		}
	})

	t.Run("forwarded query", func(t *testing.T) {
		tc := newTestCluster(t, 2, 1)
		defer tc.stop()
		bucket := bucketNameOwnedBy(t, tc.nodes[0].server.core.cluster, "app", tc.addrs[1])
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": int32(1)}})

		// The owner executes; the coordinator holds the result.
		tc.ok(0, transport.OpScan, ScanRequest{Database: "app", Bucket: bucket})
		waitForZero(t, "the owner's query in-flight count", tc.nodes[1].server.core.queryInFlight)
		waitForZero(t, "the coordinator's forwarded in-flight count", tc.nodes[0].server.core.forwardInFlight)

		// An owner-side execution that answers an error.
		tc.frame(0, transport.OpScan, ScanRequest{Database: "app", Bucket: bucket, Sort: map[string]int{"a": 1, "b": 1}})
		waitForZero(t, "the owner's query in-flight count after an error answer", tc.nodes[1].server.core.queryInFlight)

		// A coordinator forward that fails: the owner is gone.
		if err := tc.nodes[1].server.Stop(); err != nil {
			t.Fatalf("stopping the owner: %v", err)
		}
		tc.frame(0, transport.OpScan, ScanRequest{Database: "app", Bucket: bucket})
		waitForZero(t, "the coordinator's forwarded in-flight count after a failed forward", tc.nodes[0].server.core.forwardInFlight)
	})
}
