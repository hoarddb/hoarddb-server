package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// peerAction is what a scriptedPeer does with one request it has read.
type peerAction int

const (
	// answerRequest echoes the request's opcode and payload back.
	answerRequest peerAction = iota
	// closeSocket closes the socket without answering, so the pool's reader
	// sees the connection end mid-request.
	closeSocket
	// holdThenAnswer answers only once the test releases held requests,
	// keeping the request in flight until then.
	holdThenAnswer
	// closeSocketAndRefuseRedials closes the listener and then the socket, so
	// the pool's replacement dial for this socket is refused.
	closeSocketAndRefuseRedials
	// holdThenCloseAndRefuseRedials is closeSocketAndRefuseRedials once the
	// test releases held requests.
	holdThenCloseAndRefuseRedials
	// holdForAnswer answers only when the test answers this request by its
	// number (scriptedPeer.answer), so a test can choose the order responses
	// arrive in.
	holdForAnswer
)

// scriptedPeer is a bare TLS internode peer: it answers every socket's AUTH
// with AUTH_OK, then treats each request as its script says, numbering
// requests across all sockets from 1 in the order it reads them. It is bare
// rather than a TCPListener so a test can make a socket fail at an exact
// request without depending on how the real listener handles a handler error.
//
// A serial peer handles one socket's requests one at a time, so a held
// request leaves the requests behind it on that socket unread. A concurrent
// peer handles each request on its own goroutine, with its writes serialised,
// the way the real internode listener dispatches.
type scriptedPeer struct {
	ln          net.Listener
	script      func(requestNumber int64) peerAction
	concurrent  bool
	requests    atomic.Int64
	accepted    atomic.Int64
	release     chan struct{}
	releaseOnce sync.Once
	stopped     chan struct{}

	mu       sync.Mutex
	sockets  map[net.Conn]struct{}
	arrivals map[int64]requestArrival
	answers  map[int64]chan struct{}
	serving  sync.WaitGroup
}

// requestArrival is where and what one numbered request was.
type requestArrival struct {
	connection int64
	payload    string
}

func startScriptedPeer(t *testing.T, script func(requestNumber int64) peerAction) *scriptedPeer {
	return startScriptedPeerMode(t, script, false)
}

func startConcurrentScriptedPeer(t *testing.T, script func(requestNumber int64) peerAction) *scriptedPeer {
	return startScriptedPeerMode(t, script, true)
}

func startScriptedPeerMode(t *testing.T, script func(requestNumber int64) peerAction, concurrent bool) *scriptedPeer {
	t.Helper()
	serverTLS, _, err := MakeServerTLSConfig(t.TempDir())
	if err != nil {
		t.Fatalf("server TLS config: %v", err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	peer := &scriptedPeer{
		ln:         ln,
		script:     script,
		concurrent: concurrent,
		release:    make(chan struct{}),
		stopped:    make(chan struct{}),
		sockets:    make(map[net.Conn]struct{}),
		arrivals:   make(map[int64]requestArrival),
		answers:    make(map[int64]chan struct{}),
	}
	peer.serving.Add(1)
	go peer.acceptSockets()
	t.Cleanup(peer.stop)
	return peer
}

func (sp *scriptedPeer) acceptSockets() {
	defer sp.serving.Done()
	for {
		conn, err := sp.ln.Accept()
		if err != nil {
			return
		}
		connection := sp.accepted.Add(1)
		sp.mu.Lock()
		sp.sockets[conn] = struct{}{}
		sp.mu.Unlock()
		sp.serving.Add(1)
		go sp.serveSocket(conn, connection)
	}
}

func (sp *scriptedPeer) serveSocket(conn net.Conn, connection int64) {
	defer sp.serving.Done()
	defer func() {
		_ = conn.Close()
		sp.mu.Lock()
		delete(sp.sockets, conn)
		sp.mu.Unlock()
	}()
	br := bufio.NewReader(conn)
	auth, err := DecodeFrame(br)
	if err != nil {
		return
	}
	if _, err := conn.Write(NewFrameWithID(OpAuthOK, auth.RequestID, nil).Encode()); err != nil {
		return
	}
	var writeMu sync.Mutex
	for {
		request, err := DecodeFrame(br)
		if err != nil {
			return
		}
		requestNumber := sp.requests.Add(1)
		sp.mu.Lock()
		sp.arrivals[requestNumber] = requestArrival{connection: connection, payload: string(request.Payload)}
		sp.mu.Unlock()
		if !sp.concurrent {
			if !sp.handle(conn, &writeMu, request, requestNumber) {
				return
			}
			continue
		}
		sp.serving.Add(1)
		go func() {
			defer sp.serving.Done()
			if !sp.handle(conn, &writeMu, request, requestNumber) {
				_ = conn.Close()
			}
		}()
	}
}

// handle carries out the script for one request and reports whether the
// socket stays open.
func (sp *scriptedPeer) handle(conn net.Conn, writeMu *sync.Mutex, request *Frame, requestNumber int64) bool {
	switch sp.script(requestNumber) {
	case closeSocket:
		return false
	case holdThenAnswer:
		<-sp.release
	case closeSocketAndRefuseRedials:
		sp.refuseNewSockets()
		return false
	case holdThenCloseAndRefuseRedials:
		<-sp.release
		sp.refuseNewSockets()
		return false
	case holdForAnswer:
		select {
		case <-sp.answerSignal(requestNumber):
		case <-sp.stopped:
			return false
		}
	case answerRequest:
	}
	writeMu.Lock()
	defer writeMu.Unlock()
	_, err := conn.Write(NewFrameWithID(request.Opcode, request.RequestID, request.Payload).Encode())
	return err == nil
}

func (sp *scriptedPeer) answerSignal(requestNumber int64) chan struct{} {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	signal, ok := sp.answers[requestNumber]
	if !ok {
		signal = make(chan struct{})
		sp.answers[requestNumber] = signal
	}
	return signal
}

// answer lets one holdForAnswer request answer.
func (sp *scriptedPeer) answer(requestNumber int64) {
	close(sp.answerSignal(requestNumber))
}

// numberOf returns the number of the request that carried payload.
func (sp *scriptedPeer) numberOf(t *testing.T, payload string) int64 {
	t.Helper()
	sp.mu.Lock()
	defer sp.mu.Unlock()
	for requestNumber, arrival := range sp.arrivals {
		if arrival.payload == payload {
			return requestNumber
		}
	}
	t.Fatalf("the peer never read a request carrying %q", payload)
	return 0
}

// connectionOf returns which accepted connection, numbered from 1, a request
// arrived on.
func (sp *scriptedPeer) connectionOf(requestNumber int64) int64 {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.arrivals[requestNumber].connection
}

// releaseHeld lets every holdThenAnswer request answer.
func (sp *scriptedPeer) releaseHeld() {
	sp.releaseOnce.Do(func() { close(sp.release) })
}

// refuseNewSockets closes the listener, so any later dial to this peer fails.
func (sp *scriptedPeer) refuseNewSockets() {
	_ = sp.ln.Close()
}

// openSockets counts the sockets the peer has accepted and not yet seen
// closed.
func (sp *scriptedPeer) openSockets() int {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return len(sp.sockets)
}

func (sp *scriptedPeer) stop() {
	sp.releaseHeld()
	close(sp.stopped)
	_ = sp.ln.Close()
	sp.mu.Lock()
	for conn := range sp.sockets {
		_ = conn.Close()
	}
	sp.mu.Unlock()
	sp.serving.Wait()
}

func newScriptedPeerClient(t *testing.T, peer *scriptedPeer, size int) *TCPPeerClient {
	t.Helper()
	addr := peer.ln.Addr().String()
	tlsCfg, err := MakeClientTLSConfig("", "", TrustPolicy{Insecure: &InsecureTrust{Reason: "transport peer-pool test"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	client, err := NewTCPPeerClient(context.Background(), addr, tlsCfg, "test-token", size)
	if err != nil {
		t.Fatalf("peer client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// sendOnLane sends one request on lane.
func sendOnLane(ctx context.Context, client *TCPPeerClient, lane Lane, op Opcode, payload []byte) (*Frame, error) {
	return client.SendRequest(ctx, lane, op, payload)
}

// laneInFlight counts the requests holding a slot on lane's sockets.
func laneInFlight(client *TCPPeerClient, lane Lane) int {
	client.poolMu.Lock()
	defer client.poolMu.Unlock()
	inFlight := 0
	for _, ms := range client.laneSlots(lane) {
		if ms == nil {
			continue
		}
		ms.mu.Lock()
		inFlight += len(ms.pending)
		ms.mu.Unlock()
	}
	return inFlight
}

// readersExited reports whether every reader goroutine of the pool has
// exited within limit. The pool must be out of service, so no reader is
// added while this waits.
func readersExited(client *TCPPeerClient, limit time.Duration) bool {
	exited := make(chan struct{})
	go func() {
		client.readers.Wait()
		close(exited)
	}()
	select {
	case <-exited:
		return true
	case <-time.After(limit):
		return false
	}
}

// sendOutcome is one SendRequest's return, carried out of the goroutine a
// test runs it on so the test can bound how long it waits.
type sendOutcome struct {
	response *Frame
	err      error
}

func sendInBackground(ctx context.Context, client *TCPPeerClient, lane Lane, payload []byte) <-chan sendOutcome {
	outcome := make(chan sendOutcome, 1)
	go func() {
		response, err := sendOnLane(ctx, client, lane, OpReplPush, payload)
		outcome <- sendOutcome{response: response, err: err}
	}()
	return outcome
}

// awaitOutcome waits up to a second for a background send's return.
func awaitOutcome(t *testing.T, what string, outcome <-chan sendOutcome) sendOutcome {
	t.Helper()
	select {
	case got := <-outcome:
		return got
	case <-time.After(time.Second):
		t.Fatalf("%s was still blocked after 1s", what)
		return sendOutcome{}
	}
}

// waitForRequests waits until the peer has read at least count requests, so
// a test knows a request is in flight before it depends on that.
func waitForRequests(t *testing.T, peer *scriptedPeer, count int64) {
	t.Helper()
	if reached := waitForRequestsUpTo(peer, count, 5*time.Second); reached < count {
		t.Fatalf("peer read %d requests in 5s, want %d", reached, count)
	}
}

// waitForRequestsUpTo waits until the peer has read count requests or limit
// has passed, and returns how many it had read.
func waitForRequestsUpTo(peer *scriptedPeer, count int64, limit time.Duration) int64 {
	deadline := time.Now().Add(limit)
	for peer.requests.Load() < count && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	return peer.requests.Load()
}

func waitUntil(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after 1s waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// requestLaneCapacity is how many frames a production pool's request lane
// carries at once: every socket but the control socket, each up to the
// request lane's per-socket bound. It is not the control lane's bound
// (BUG-0723): a test that filled the request lane to the control figure
// would leave it with room and pin nothing.
const requestLaneCapacity = (InternodePoolSize - 1) * PeerRequestSocketFrames

// TestTCPPeerPoolSurvivesFailedRequests reproduces a pool failure, on the
// request lane. The peer closes the socket on each of the first four requests
// and answers every later one, so it is healthy again by the fifth call.
//
// The pool used to close a socket that failed mid-request and never replace
// it, trusting the caller to throw the whole pool away (peer.dropClient). Six
// of the thirteen internode callers never do, so four failures emptied the
// pool, and the fifth SendRequest waited on it forever. The fifth call here
// carries no deadline on purpose: that is the forwarded path, and it is why
// observing ctx while waiting (F-9) cannot fix this on its own.
func TestTCPPeerPoolSurvivesFailedRequests(t *testing.T) {
	const failingRequests = InternodePoolSize
	peer := startScriptedPeer(t, func(requestNumber int64) peerAction {
		if requestNumber <= failingRequests {
			return closeSocket
		}
		return answerRequest
	})
	client := newScriptedPeerClient(t, peer, InternodePoolSize)

	for attempt := 1; attempt <= failingRequests; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := sendOnLane(ctx, client, LaneRequest, OpReplPush, []byte("doomed"))
		cancel()
		if err == nil {
			t.Fatalf("request %d: SendRequest succeeded, but the peer closed the socket on it", attempt)
		}
	}

	want := []byte("after the failures")
	select {
	case outcome := <-sendInBackground(context.Background(), client, LaneRequest, want):
		if outcome.err != nil {
			t.Fatalf("SendRequest to a peer that is answering again failed: %v", outcome.err)
		}
		if string(outcome.response.Payload) != string(want) {
			t.Fatalf("response payload = %q, want %q", outcome.response.Payload, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("SendRequest blocked for 1s after %d failed requests: the pool lost a socket on each failure and was never refilled", failingRequests)
	}
}

// TestPeerClientHonoursContextWhileWaiting is F-9 on a multiplexed pool. It
// fills the only socket of a size-1 pool to its frame limit with requests the
// serial peer leaves unread behind the first, then asks for one more with a
// 100 ms context: it must give up with the context's error rather than wait
// for a slot. Without the fill the request would be written at once and would
// no longer test the wait. Spec §3.6 moves "the query-pipelining spec's T-13"
// that this test used to be to TestPeerClientAbandonedRequestHoldsItsSlotUntilAnswered.
func TestPeerClientHonoursContextWhileWaiting(t *testing.T) {
	peer := startScriptedPeer(t, func(int64) peerAction { return holdThenAnswer })
	client := newScriptedPeerClient(t, peer, 1)

	held := make([]<-chan sendOutcome, PeerControlSocketFrames)
	for i := range held {
		held[i] = sendInBackground(context.Background(), client, LaneControl, []byte(fmt.Sprintf("fills slot %d", i)))
	}
	waitUntil(t, fmt.Sprintf("%d requests in flight on the control socket", PeerControlSocketFrames),
		func() bool { return laneInFlight(client, LaneControl) == PeerControlSocketFrames })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	select {
	case outcome := <-sendInBackground(ctx, client, LaneControl, []byte("waits for a slot")):
		if !errors.Is(outcome.err, context.DeadlineExceeded) {
			t.Fatalf("SendRequest while every slot is held: err = %v, want context.DeadlineExceeded", outcome.err)
		}
		if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
			t.Fatalf("SendRequest took %v to honour a 100ms context, want under 250ms", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("SendRequest ignored its context while waiting for a slot: still blocked after 1s")
	}

	peer.releaseHeld()
	for i, outcome := range held {
		if got := awaitOutcome(t, fmt.Sprintf("held request %d", i), outcome); got.err != nil {
			t.Fatalf("held request %d failed once released: %v", i, got.err)
		}
	}
}

// TestTCPPeerPoolPairsSocketWithReader drives a multi-socket pool from more
// concurrent callers than it has sockets and requires every caller to get its
// own response back. Under multiplexing the 32 callers share the three
// request sockets, so this is also the proof that responses are matched to
// requests by request ID.
//
// The pool used to hand out connections from one channel and buffered readers
// from another. Two channel receives are not one atomic pair, so under
// concurrency a caller could take socket A together with socket B's reader and
// then read B's response frame — acting on a reply to a request it never sent.
// In the cluster that surfaced as a forwarded read told "owner ... returned 0
// responses for 1 entry" (it had read a REPL_PUSH acknowledgement, and those
// carry no responses) and as "use of closed network connection" when the
// crossed socket's owner closed it.
func TestTCPPeerPoolPairsSocketWithReader(t *testing.T) {
	cfg := testTCPConfig(TCPConfig{PeerMode: true})
	ln := startTestTCPListener(t, cfg, authEchoHandler, nil)
	addr := ln.Addr().String()

	tlsCfg, err := MakeClientTLSConfig("", "", TrustPolicy{Insecure: &InsecureTrust{Reason: "transport peer-pool test"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	client, err := NewTCPPeerClient(context.Background(), addr, tlsCfg, "test-token", InternodePoolSize)
	if err != nil {
		t.Fatalf("peer client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// Several callers per socket, so requests share sockets rather than each
	// caller owning an uncontended one.
	const callers = 8 * InternodePoolSize

	failures := make(chan error, callers)
	var wg sync.WaitGroup
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func(caller int) {
			defer wg.Done()
			want := make([]byte, 8)
			binary.BigEndian.PutUint64(want, uint64(caller))

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			resp, err := sendOnLane(ctx, client, LaneRequest, OpReplPush, want)
			if err != nil {
				failures <- fmt.Errorf("caller %d: send: %w", caller, err)
				return
			}
			if len(resp.Payload) != len(want) {
				failures <- fmt.Errorf("caller %d: response payload is %d bytes, want %d — this is another caller's frame",
					caller, len(resp.Payload), len(want))
				return
			}
			if got := binary.BigEndian.Uint64(resp.Payload); got != uint64(caller) {
				failures <- fmt.Errorf("caller %d: response payload is request %d — the response was delivered to the wrong caller",
					caller, got)
			}
		}(index)
	}
	wg.Wait()
	close(failures)

	for err := range failures {
		t.Error(err)
	}
}

// TestTCPPeerPoolLeavesServiceWhenReplacementFails covers the other half of
// the self-healing pool: when a failed socket's replacement cannot be dialed
// (the peer is gone), the pool does not shrink and leave callers waiting. It
// goes out of service: Failed reports it, every later SendRequest fails at
// once with ErrPeerClientFailed, and the pool closes its idle control socket
// rather than leak it.
func TestTCPPeerPoolLeavesServiceWhenReplacementFails(t *testing.T) {
	peer := startScriptedPeer(t, func(requestNumber int64) peerAction {
		if requestNumber == 1 {
			return closeSocketAndRefuseRedials
		}
		return answerRequest
	})
	client := newScriptedPeerClient(t, peer, 2)

	if _, err := sendOnLane(context.Background(), client, LaneRequest, OpReplPush, []byte("doomed")); err == nil {
		t.Fatal("SendRequest succeeded, but the peer closed the socket on it")
	}
	waitUntil(t, "the pool to leave service after its replacement dial was refused", client.Failed)

	started := time.Now()
	_, err := sendOnLane(context.Background(), client, LaneRequest, OpReplPush, []byte("after the pool failed"))
	if !errors.Is(err, ErrPeerClientFailed) {
		t.Fatalf("SendRequest on a failed pool: err = %v, want ErrPeerClientFailed", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("SendRequest on a failed pool took %v, want an immediate error", elapsed)
	}
	waitUntil(t, "the failed pool to close its idle socket", func() bool { return peer.openSockets() == 0 })
}

// TestTCPPeerPoolWakesWaitersWhenItLeavesService fills the only socket of a
// size-1 pool to its frame limit, parks one more request with no deadline
// waiting for a slot, then fails the socket with the peer refusing a
// replacement. The eight in flight fail with the socket's transport error;
// the parked one, waiting on a lane whose socket is being replaced, must fail
// with ErrPeerClientFailed rather than wait for a socket nothing will
// install. (A multiplexed send is written at once, so the waiter has to be
// one blocked on a full socket, not one behind a busy socket.)
func TestTCPPeerPoolWakesWaitersWhenItLeavesService(t *testing.T) {
	peer := startScriptedPeer(t, func(requestNumber int64) peerAction {
		if requestNumber == 1 {
			return holdThenCloseAndRefuseRedials
		}
		return answerRequest
	})
	client := newScriptedPeerClient(t, peer, 1)

	held := make([]<-chan sendOutcome, PeerControlSocketFrames)
	for i := range held {
		held[i] = sendInBackground(context.Background(), client, LaneControl, []byte(fmt.Sprintf("in flight %d", i)))
	}
	waitUntil(t, fmt.Sprintf("%d requests in flight on the only socket", PeerControlSocketFrames),
		func() bool { return laneInFlight(client, LaneControl) == PeerControlSocketFrames })
	waiting := sendInBackground(context.Background(), client, LaneControl, []byte("waits with no deadline"))
	time.Sleep(50 * time.Millisecond) // let it park on the full socket

	peer.releaseHeld()
	for i, outcome := range held {
		got := awaitOutcome(t, fmt.Sprintf("in-flight request %d", i), outcome)
		if got.err == nil {
			t.Fatalf("in-flight request %d succeeded, but the peer closed its socket", i)
		}
		if errors.Is(got.err, ErrPeerClientFailed) {
			t.Fatalf("in-flight request %d failed with %v, want the socket's own transport error", i, got.err)
		}
	}
	select {
	case outcome := <-waiting:
		if !errors.Is(outcome.err, ErrPeerClientFailed) {
			t.Fatalf("waiting SendRequest: err = %v, want ErrPeerClientFailed", outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("a SendRequest waiting for a slot was still blocked 1s after the pool left service")
	}
}

// TestTCPPeerPoolSocketFailureSparesInFlightRequests fails one socket while a
// request is in flight on another, and requires the in-flight request to
// complete and the pool to keep serving. It is why the pool replaces the one
// broken socket instead of being dropped whole: dropping it closes every
// socket, failing unrelated in-flight requests as collateral damage. The two
// requests are on different lanes so they are on different sockets, which is
// what the test is about.
func TestTCPPeerPoolSocketFailureSparesInFlightRequests(t *testing.T) {
	peer := startScriptedPeer(t, func(requestNumber int64) peerAction {
		switch requestNumber {
		case 1:
			return holdThenAnswer
		case 2:
			return closeSocket
		default:
			return answerRequest
		}
	})
	client := newScriptedPeerClient(t, peer, 2)

	inFlight := sendInBackground(context.Background(), client, LaneControl, []byte("in flight"))
	waitForRequests(t, peer, 1)
	if _, err := sendOnLane(context.Background(), client, LaneRequest, OpReplPush, []byte("doomed")); err == nil {
		t.Fatal("SendRequest succeeded, but the peer closed the socket on it")
	}

	peer.releaseHeld()
	outcome := awaitOutcome(t, "the in-flight request", inFlight)
	if outcome.err != nil {
		t.Fatalf("a request in flight on a healthy socket failed when another socket did: %v", outcome.err)
	}
	if client.Failed() {
		t.Fatal("the pool left service after one socket failed while the peer was still accepting")
	}
	for attempt := 1; attempt <= 2; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := sendOnLane(ctx, client, LaneRequest, OpReplPush, []byte("after the replacement"))
		cancel()
		if err != nil {
			t.Fatalf("request %d after the failed socket was replaced: %v", attempt, err)
		}
	}
}

// TestPeerClientMultiplexesPastPoolSize is T-11. The peer holds as many
// requests as the pool has sockets, and one more request-lane send must still
// complete; then the held ones are answered in reverse order, and each reaches
// the caller that sent it.
func TestPeerClientMultiplexesPastPoolSize(t *testing.T) {
	const held = InternodePoolSize
	peer := startConcurrentScriptedPeer(t, func(requestNumber int64) peerAction {
		if requestNumber <= held {
			return holdForAnswer
		}
		return answerRequest
	})
	client := newScriptedPeerClient(t, peer, InternodePoolSize)

	outcomes := make([]<-chan sendOutcome, held)
	for i := range outcomes {
		outcomes[i] = sendInBackground(context.Background(), client, LaneRequest, []byte(fmt.Sprintf("held %d", i)))
	}
	waitForRequests(t, peer, held)

	select {
	case outcome := <-sendInBackground(context.Background(), client, LaneRequest, []byte("past the pool size")):
		if outcome.err != nil {
			t.Fatalf("request %d: %v", held+1, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatalf("request %d on the request lane did not complete within 1s while the peer held %d: "+
			"each socket carries one request at a time (S-1)", held+1, held)
	}

	for requestNumber := int64(held); requestNumber >= 1; requestNumber-- {
		peer.answer(requestNumber)
	}
	for i, outcome := range outcomes {
		got := awaitOutcome(t, fmt.Sprintf("held request %d", i), outcome)
		if got.err != nil {
			t.Fatalf("held request %d: %v", i, got.err)
		}
		if want := fmt.Sprintf("held %d", i); string(got.response.Payload) != want {
			t.Fatalf("held request %d received %q, want %q: a response answered out of order reached the wrong caller", i, got.response.Payload, want)
		}
	}
}

// TestPeerControlLaneNotBehindRequests is T-12: with the request lane
// saturated by held requests, a control-lane PING completes.
func TestPeerControlLaneNotBehindRequests(t *testing.T) {
	peer := startConcurrentScriptedPeer(t, func(requestNumber int64) peerAction {
		if requestNumber <= requestLaneCapacity {
			return holdThenAnswer
		}
		return answerRequest
	})
	client := newScriptedPeerClient(t, peer, InternodePoolSize)

	held := make([]<-chan sendOutcome, requestLaneCapacity)
	for i := range held {
		held[i] = sendInBackground(context.Background(), client, LaneRequest, []byte(fmt.Sprintf("held %d", i)))
	}
	reached := waitForRequestsUpTo(peer, requestLaneCapacity, 5*time.Second)

	ping := make(chan error, 1)
	go func() {
		_, err := sendOnLane(context.Background(), client, LaneControl, OpPing, nil)
		ping <- err
	}()
	select {
	case err := <-ping:
		if err != nil {
			t.Errorf("control-lane PING: %v", err)
		}
	case <-time.After(time.Second):
		t.Errorf("a control-lane PING was blocked for 1s behind %d held request-lane requests: "+
			"control traffic waits for sockets forwarded requests hold (S-1)", reached)
	}
	if reached != requestLaneCapacity {
		t.Errorf("the peer read %d of %d request-lane requests: the request lane does not carry %d in flight",
			reached, requestLaneCapacity, requestLaneCapacity)
	}

	peer.releaseHeld()
	for i, outcome := range held {
		if got := awaitOutcome(t, fmt.Sprintf("held request %d", i), outcome); got.err != nil {
			t.Errorf("held request %d failed once released: %v", i, got.err)
		}
	}
}

// TestPeerLaneBoundsFitTheListener pins BUG-0723's split. The request lane
// carries exactly requestLaneCapacity frames, more than the old 24, and parks
// the next; the internode listener reads ahead at least as many frames as the
// client keeps on its fullest socket of either lane, so no frame the client
// counts as in flight sits unread behind a full window; and the listener's
// byte window is still 32 MiB, so the larger frame bound does not grow the
// worst-case read-ahead bytes per connection.
//
// Break: raise PeerRequestSocketFrames without raising the listener floor
// (set PeerPipelineFramesFloor to PeerControlSocketFrames); the floor
// assertion fails.
func TestPeerLaneBoundsFitTheListener(t *testing.T) {
	const oldRequestLane = 24 // 3 request sockets x 8 frames before BUG-0723
	if requestLaneCapacity <= oldRequestLane {
		t.Fatalf("the request lane carries %d frames, no more than the %d BUG-0723 found capping forwarded writes",
			requestLaneCapacity, oldRequestLane)
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{PeerMode: true, PipelineFrames: 1, PipelineBytes: 1}), authEchoHandler, nil)
	for _, lane := range []Lane{LaneControl, LaneRequest} {
		if ln.cfg.PipelineFrames < lane.socketFrames() {
			t.Errorf("the internode listener reads ahead %d frames, fewer than the %d the client keeps on one %s socket: "+
				"the client's extra frames would wait unread", ln.cfg.PipelineFrames, lane.socketFrames(), lane)
		}
	}
	if ln.cfg.PipelineBytes != 32<<20 {
		t.Errorf("the internode listener's byte window is %d, want 32 MiB: the frame split must not move it", ln.cfg.PipelineBytes)
	}

	peer := startConcurrentScriptedPeer(t, func(int64) peerAction { return holdThenAnswer })
	client := newScriptedPeerClient(t, peer, InternodePoolSize)
	held := make([]<-chan sendOutcome, requestLaneCapacity+1)
	for i := range held {
		held[i] = sendInBackground(context.Background(), client, LaneRequest, []byte(fmt.Sprintf("held %d", i)))
	}
	waitForRequests(t, peer, requestLaneCapacity)
	time.Sleep(50 * time.Millisecond) // give the extra request time to be written, if the lane wrongly had room
	if got := peer.requests.Load(); got != requestLaneCapacity {
		t.Errorf("the peer read %d request-lane requests, want exactly %d: the lane's bound is not its capacity", got, requestLaneCapacity)
	}
	if got := laneInFlight(client, LaneRequest); got != requestLaneCapacity {
		t.Errorf("%d request-lane requests hold a slot, want %d", got, requestLaneCapacity)
	}

	peer.releaseHeld()
	for i, outcome := range held {
		if got := awaitOutcome(t, fmt.Sprintf("held request %d", i), outcome); got.err != nil {
			t.Errorf("held request %d failed once released: %v", i, got.err)
		}
	}
}

// TestSizeOnePoolIsControlOnly is T-12b (OC-3): a size-1 pool carries the
// control lane, and a request-lane send on it fails at once with the named
// error, writes no frame and takes no slot.
func TestSizeOnePoolIsControlOnly(t *testing.T) {
	peer := startConcurrentScriptedPeer(t, func(int64) peerAction { return answerRequest })
	client := newScriptedPeerClient(t, peer, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := sendOnLane(ctx, client, LaneControl, OpGossip, []byte("membership probe")); err != nil {
		t.Fatalf("control-lane GOSSIP on a size-1 pool: %v", err)
	}

	started := time.Now()
	_, err := sendOnLane(context.Background(), client, LaneRequest, OpReplPush, []byte("has no lane"))
	elapsed := time.Since(started)
	if !errors.Is(err, ErrNoRequestLane) {
		t.Errorf("a request-lane send on a size-1 pool: err = %v, want ErrNoRequestLane", err)
	} else if !strings.Contains(err.Error(), "1 socket") || !strings.Contains(err.Error(), "control lane") {
		t.Errorf("the no-request-lane error %q does not name the pool size and the lane", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("the request-lane send took %v, want an immediate error", elapsed)
	}
	time.Sleep(50 * time.Millisecond) // a frame, had one been written, reaches the peer
	if got := peer.requests.Load(); got != 1 {
		t.Errorf("the peer read %d requests, want 1 (the GOSSIP): the request-lane send wrote a frame on the control socket", got)
	}
	if got := laneInFlight(client, LaneControl); got != 0 {
		t.Errorf("the control socket has %d request(s) in flight after the refused send, want 0", got)
	}
}

// TestPeerClientAbandonedRequestHoldsItsSlotUntilAnswered is T-13 (rev 2.2's
// name). On a size-1 pool's control socket, A is in flight with no deadline
// and B with 100 ms. B gives up after its frame has reached the peer, but its
// slot stays taken until its late response is read, which then releases it
// without failing the socket; A completes on the same connection.
func TestPeerClientAbandonedRequestHoldsItsSlotUntilAnswered(t *testing.T) {
	peer := startConcurrentScriptedPeer(t, func(int64) peerAction { return holdForAnswer })
	client := newScriptedPeerClient(t, peer, 1)

	requestA := sendInBackground(context.Background(), client, LaneControl, []byte("A"))
	waitForRequests(t, peer, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	outcomeB := awaitOutcome(t, "B, under a 100ms context,", sendInBackground(ctx, client, LaneControl, []byte("B")))
	if !errors.Is(outcomeB.err, context.DeadlineExceeded) {
		t.Fatalf("B: err = %v, want context.DeadlineExceeded", outcomeB.err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Errorf("B took %v to honour a 100ms context, want under 250ms", elapsed)
	}
	if got := peer.requests.Load(); got != 2 {
		t.Fatalf("B gave up (%v) but its frame never reached the peer (the peer read %d request(s)): "+
			"it waited for the one socket A held instead of being written beside it", outcomeB.err, got)
	}
	for _, when := range []string{"at once", "50ms later"} {
		if got := laneInFlight(client, LaneControl); got != 2 {
			t.Fatalf("%s after B gave up the socket has %d request(s) in flight, want 2: "+
				"an abandoned request keeps its slot until its response is read (spec §3.6)", when, got)
		}
		time.Sleep(50 * time.Millisecond)
	}

	peer.answer(peer.numberOf(t, "B"))
	waitUntil(t, "B's late response to release its slot", func() bool { return laneInFlight(client, LaneControl) == 1 })
	peer.answer(peer.numberOf(t, "A"))
	outcomeA := awaitOutcome(t, "A", requestA)
	if outcomeA.err != nil {
		t.Fatalf("A failed after B's late response: %v", outcomeA.err)
	}
	if string(outcomeA.response.Payload) != "A" {
		t.Fatalf("A received %q, want %q", outcomeA.response.Payload, "A")
	}
	if got := peer.accepted.Load(); got != 1 {
		t.Fatalf("the peer accepted %d connections, want 1: B's late response failed the socket and the pool redialed", got)
	}
}

// TestPeerDeadlineIsPerRequest is T-14 (rev 2.2). On a size-1 pool's control
// socket the peer reads A (50 ms deadline) and B (none) before it answers
// either. A's deadline ends A only: B completes on the same connection.
func TestPeerDeadlineIsPerRequest(t *testing.T) {
	peer := startConcurrentScriptedPeer(t, func(int64) peerAction { return holdForAnswer })
	client := newScriptedPeerClient(t, peer, 1)

	ctxA, cancelA := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelA()
	requestA := sendInBackground(ctxA, client, LaneControl, []byte("A"))
	requestB := sendInBackground(context.Background(), client, LaneControl, []byte("B"))
	waitForRequests(t, peer, 2)
	numberA, numberB := peer.numberOf(t, "A"), peer.numberOf(t, "B")
	if connectionA, connectionB := peer.connectionOf(numberA), peer.connectionOf(numberB); connectionA != connectionB {
		t.Fatalf("A reached the peer on connection %d and B on connection %d: B was not written until A's deadline "+
			"had failed the only socket and the pool had redialed", connectionA, connectionB)
	}

	if outcomeA := awaitOutcome(t, "A, under a 50ms deadline,", requestA); !errors.Is(outcomeA.err, context.DeadlineExceeded) {
		t.Fatalf("A: err = %v, want context.DeadlineExceeded", outcomeA.err)
	}
	peer.answer(numberB)
	outcomeB := awaitOutcome(t, "B", requestB)
	if outcomeB.err != nil {
		t.Fatalf("B failed after A's deadline passed: %v (a deadline on the shared socket ends every request on it)", outcomeB.err)
	}
	if string(outcomeB.response.Payload) != "B" {
		t.Fatalf("B received %q, want %q", outcomeB.response.Payload, "B")
	}
	peer.answer(numberA)
	if got := peer.accepted.Load(); got != 1 {
		t.Fatalf("the peer accepted %d connections, want 1: A's deadline failed the socket", got)
	}
}

// TestReplacedControlSocketRejoinsTheControlLane is the build doc's: with the
// request lane saturated, the control socket fails; once its replacement is
// dialed, a control-lane PING completes while every request-lane request is
// still held. A replacement that lands anywhere but the control lane leaves
// control traffic behind the saturated request lane.
func TestReplacedControlSocketRejoinsTheControlLane(t *testing.T) {
	peer := startConcurrentScriptedPeer(t, func(requestNumber int64) peerAction {
		switch {
		case requestNumber <= requestLaneCapacity:
			return holdThenAnswer
		case requestNumber == requestLaneCapacity+1:
			return closeSocket
		default:
			return answerRequest
		}
	})
	client := newScriptedPeerClient(t, peer, InternodePoolSize)

	held := make([]<-chan sendOutcome, requestLaneCapacity)
	for i := range held {
		held[i] = sendInBackground(context.Background(), client, LaneRequest, []byte(fmt.Sprintf("held %d", i)))
	}
	waitForRequests(t, peer, requestLaneCapacity)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := sendOnLane(ctx, client, LaneControl, OpPing, []byte("fails the control socket")); err == nil {
		t.Fatal("the control-lane request succeeded, but the peer closed its socket")
	}
	waitUntil(t, "the replacement socket to be dialed", func() bool { return peer.accepted.Load() == InternodePoolSize+1 })

	ping := make(chan error, 1)
	go func() {
		_, err := sendOnLane(context.Background(), client, LaneControl, OpPing, []byte("on the replacement"))
		ping <- err
	}()
	select {
	case err := <-ping:
		if err != nil {
			t.Fatalf("control-lane PING on the replacement socket: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("a control-lane PING was blocked for 1s after the control socket was replaced: the replacement did not rejoin the control lane")
	}
	for i, outcome := range held {
		select {
		case got := <-outcome:
			t.Fatalf("held request %d returned (%v) before it was released", i, got.err)
		default:
		}
	}

	peer.releaseHeld()
	for i, outcome := range held {
		if got := awaitOutcome(t, fmt.Sprintf("held request %d", i), outcome); got.err != nil {
			t.Errorf("held request %d failed once released: %v", i, got.err)
		}
	}
}

// TestFailedPoolReadersExitWhenPendingDrains is the build doc's: a pool that
// leaves service with a request still pending on a live socket lets that
// request finish, then closes the socket and ends its reader, without anyone
// calling Close. peer.installClient replaces a failed pool without closing it
// (server/cluster_peer_test.go:TestPeerDialReplacesFailedClient), so nothing
// else would ever reclaim the socket or its goroutine.
func TestFailedPoolReadersExitWhenPendingDrains(t *testing.T) {
	peer := startConcurrentScriptedPeer(t, func(requestNumber int64) peerAction {
		if requestNumber == 1 {
			return holdThenAnswer
		}
		return closeSocketAndRefuseRedials
	})
	client := newScriptedPeerClient(t, peer, 2)

	pending := sendInBackground(context.Background(), client, LaneControl, []byte("pending on the control socket"))
	waitForRequests(t, peer, 1)
	if _, err := sendOnLane(context.Background(), client, LaneRequest, OpReplPush, []byte("fails the request socket")); err == nil {
		t.Fatal("the request-lane send succeeded, but the peer closed its socket")
	}
	waitUntil(t, "the pool to leave service after its replacement dial was refused", client.Failed)
	if got := peer.openSockets(); got != 1 {
		t.Fatalf("the peer has %d open socket(s) after the pool left service, want 1: the socket with a pending request must stay open until it is answered", got)
	}

	peer.releaseHeld()
	if outcome := awaitOutcome(t, "the pending request", pending); outcome.err != nil {
		t.Fatalf("the request pending on a live socket failed when the pool left service: %v", outcome.err)
	}
	waitUntil(t, "the failed pool to close its drained socket", func() bool { return peer.openSockets() == 0 })
	if !readersExited(client, time.Second) {
		t.Fatal("reader goroutines of a failed pool were still running 1s after its last socket drained")
	}
}

// TestConditionalSendIsRefusedOnceASocketIsReplaced: a caller that learnt
// something about the peer over the pool's sockets reads the generation and
// sends conditionally on it. Once a socket has been re-established — the pool
// dialed the address again, and whatever answers there now may not be what
// answered before — the conditional send is refused, and a send at the new
// generation goes through. The server's plaintext-peer rule depends on it
// (server/cluster_peer.go:peer.confirmSealed).
func TestConditionalSendIsRefusedOnceASocketIsReplaced(t *testing.T) {
	peer := startConcurrentScriptedPeer(t, func(requestNumber int64) peerAction {
		if requestNumber == 2 {
			return closeSocket
		}
		return answerRequest
	})
	client := newScriptedPeerClient(t, peer, InternodePoolSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	confirmed := client.Generation()
	if confirmed == 0 {
		t.Fatalf("a pool with its sockets installed reports generation 0, which callers use to mean \"unconditional\"")
	}
	if _, err := client.SendRequestIfUnchanged(ctx, LaneControl, OpPing, []byte("first"), confirmed); err != nil {
		t.Fatalf("a conditional send at the current generation: %v", err)
	}
	if _, err := sendOnLane(ctx, client, LaneControl, OpPing, []byte("the peer closes this socket")); err == nil {
		t.Fatal("the request the peer closed its socket on succeeded")
	}
	waitUntil(t, "the replacement socket to be installed", func() bool { return client.Generation() != confirmed })

	_, err := client.SendRequestIfUnchanged(ctx, LaneControl, OpPing, []byte("after the replacement"), confirmed)
	if !errors.Is(err, ErrPeerSocketsChanged) {
		t.Fatalf("a conditional send at the generation before the replacement answered %v; want %v", err, ErrPeerSocketsChanged)
	}
	if _, err := client.SendRequestIfUnchanged(ctx, LaneControl, OpPing, []byte("at the new generation"), client.Generation()); err != nil {
		t.Fatalf("a conditional send at the new generation: %v", err)
	}
	if _, err := sendOnLane(ctx, client, LaneControl, OpPing, []byte("unconditional")); err != nil {
		t.Fatalf("an unconditional send after the replacement: %v", err)
	}
}
