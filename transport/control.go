package transport

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The control kinds of conventions section 6.1.
const (
	KindLeaseReq      byte = 0x08
	KindProbe         byte = 0x09
	KindProbeReply    byte = 0x0A
	KindLeaseGrant    byte = 0x0B
	KindFailureReport byte = 0x0C
	KindClusterNotice byte = 0x0D
)

// ErrControlBody is the refusal for a control message whose body does not
// match its kind's layout. Its text is the INVALID_ARGUMENT code of the
// cluster package, which this package cannot import.
var ErrControlBody = errors.New("INVALID_ARGUMENT")

// ControlMessage is one control-channel body. The envelope that precedes it
// on the wire carries the sender's epoch and state_seq.
type ControlMessage interface {
	controlKind() byte
	appendBody(dst []byte) ([]byte, error)
}

// LeaseReq is LEASE_REQ: the heartbeat that asks for a grant.
type LeaseReq struct{ Nonce uint64 }

// Probe is PROBE.
type Probe struct{ Nonce uint64 }

// ProbeReply is PROBE_REPLY. State is the member-state byte of conventions
// section 14; the cluster package validates it, because this package cannot
// name the states.
type ProbeReply struct {
	Nonce           uint64
	State           byte
	AppliedStateSeq uint64
}

// LeaseGrant is LEASE_GRANT, echoing the request's nonce. RTTMicros is the
// round trip the grantor measured; a negative one is refused at decode and at
// encode, because it would extend the lease it was meant to bound.
type LeaseGrant struct {
	Nonce, GrantEpoch, GrantTerm uint64
	RTTMicros                    int32
}

// FailureReport is FAILURE_REPORT: a probe observer naming a failed member.
type FailureReport struct {
	Subject              string
	FirstFailureAtMillis uint64
}

// ClusterNotice is CLUSTER_NOTICE: the leader telling everyone state moved.
type ClusterNotice struct{ Epoch, StateSeq uint64 }

func (LeaseReq) controlKind() byte      { return KindLeaseReq }
func (Probe) controlKind() byte         { return KindProbe }
func (ProbeReply) controlKind() byte    { return KindProbeReply }
func (LeaseGrant) controlKind() byte    { return KindLeaseGrant }
func (FailureReport) controlKind() byte { return KindFailureReport }
func (ClusterNotice) controlKind() byte { return KindClusterNotice }

func (m LeaseReq) appendBody(dst []byte) ([]byte, error) {
	return binary.BigEndian.AppendUint64(dst, m.Nonce), nil
}
func (m Probe) appendBody(dst []byte) ([]byte, error) {
	return binary.BigEndian.AppendUint64(dst, m.Nonce), nil
}

func (m ProbeReply) appendBody(dst []byte) ([]byte, error) {
	dst = binary.BigEndian.AppendUint64(dst, m.Nonce)
	dst = append(dst, m.State)
	return binary.BigEndian.AppendUint64(dst, m.AppliedStateSeq), nil
}

func (m LeaseGrant) appendBody(dst []byte) ([]byte, error) {
	if m.RTTMicros < 0 {
		return nil, fmt.Errorf("%w: LEASE_GRANT rttMicros %d is negative", ErrControlBody, m.RTTMicros)
	}
	dst = binary.BigEndian.AppendUint64(dst, m.Nonce)
	dst = binary.BigEndian.AppendUint64(dst, m.GrantEpoch)
	dst = binary.BigEndian.AppendUint64(dst, m.GrantTerm)
	return binary.BigEndian.AppendUint32(dst, uint32(m.RTTMicros)), nil
}

func (m FailureReport) appendBody(dst []byte) ([]byte, error) {
	if len(m.Subject) == 0 || len(m.Subject) > 255 {
		return nil, fmt.Errorf("%w: FAILURE_REPORT subject is %d bytes, it must be 1 to 255", ErrControlBody, len(m.Subject))
	}
	dst = append(dst, byte(len(m.Subject)))
	dst = append(dst, m.Subject...)
	return binary.BigEndian.AppendUint64(dst, m.FirstFailureAtMillis), nil
}

func (m ClusterNotice) appendBody(dst []byte) ([]byte, error) {
	dst = binary.BigEndian.AppendUint64(dst, m.Epoch)
	return binary.BigEndian.AppendUint64(dst, m.StateSeq), nil
}

// EncodeControl returns the payload for a control message: the envelope, with
// its version and kind set from the message, then the body. Slot and database
// id are zero, because no control message is scoped to either.
func EncodeControl(env Envelope, msg ControlMessage) ([]byte, error) {
	m, err := NewControlMessage(env, msg)
	if err != nil {
		return nil, err
	}
	return append(EncodeEnvelope(m.Env), m.Body...), nil
}

// DecodeControl reads a control payload. It refuses a kind that is not a
// control kind, a body whose length is not its kind's, a negative rttMicros
// and an empty subject. What it accepts encodes back to the same bytes.
func DecodeControl(payload []byte) (Envelope, ControlMessage, error) {
	env, err := DecodeEnvelope(payload)
	if err != nil {
		return Envelope{}, nil, err
	}
	msg, err := Message{Env: env, Body: payload[EnvelopeSize:]}.Control()
	if err != nil {
		return Envelope{}, nil, err
	}
	return env, msg, nil
}

// Control decodes the message's body as a control message, checking its kind
// belongs to the control channel.
func (m Message) Control() (ControlMessage, error) {
	if err := CheckKind(m.Env.Kind, ChannelControl); err != nil {
		return nil, err
	}
	if m.Env.Slot != 0 || m.Env.DBID != 0 {
		return nil, fmt.Errorf("%w: a control message is not slot or database scoped, yet slot %d database %d are set", ErrControlBody, m.Env.Slot, m.Env.DBID)
	}
	return decodeControlBody(m.Env.Kind, m.Body)
}

// NewControlMessage builds the Message for a control message, setting the
// envelope's version and kind from it.
func NewControlMessage(env Envelope, msg ControlMessage) (Message, error) {
	env.Version, env.Kind, env.Slot, env.DBID = EnvelopeVersion, msg.controlKind(), 0, 0
	body, err := msg.appendBody(nil)
	if err != nil {
		return Message{}, err
	}
	return Message{Env: env, Body: body}, nil
}

func controlBodyLength(kind byte, body []byte, want int) error {
	if len(body) != want {
		return fmt.Errorf("%w: kind 0x%02x body is %d bytes, it must be %d", ErrControlBody, kind, len(body), want)
	}
	return nil
}

func decodeControlBody(kind byte, body []byte) (ControlMessage, error) {
	switch kind {
	case KindLeaseReq, KindProbe:
		if err := controlBodyLength(kind, body, 8); err != nil {
			return nil, err
		}
		nonce := binary.BigEndian.Uint64(body)
		if kind == KindProbe {
			return Probe{Nonce: nonce}, nil
		}
		return LeaseReq{Nonce: nonce}, nil
	case KindProbeReply:
		if err := controlBodyLength(kind, body, 17); err != nil {
			return nil, err
		}
		return ProbeReply{Nonce: binary.BigEndian.Uint64(body), State: body[8], AppliedStateSeq: binary.BigEndian.Uint64(body[9:])}, nil
	case KindLeaseGrant:
		if err := controlBodyLength(kind, body, 28); err != nil {
			return nil, err
		}
		rtt := int32(binary.BigEndian.Uint32(body[24:]))
		if rtt < 0 {
			return nil, fmt.Errorf("%w: LEASE_GRANT rttMicros %d is negative", ErrControlBody, rtt)
		}
		return LeaseGrant{Nonce: binary.BigEndian.Uint64(body), GrantEpoch: binary.BigEndian.Uint64(body[8:]),
			GrantTerm: binary.BigEndian.Uint64(body[16:]), RTTMicros: rtt}, nil
	case KindFailureReport:
		if len(body) < 1 || body[0] == 0 {
			return nil, fmt.Errorf("%w: FAILURE_REPORT names no subject", ErrControlBody)
		}
		if err := controlBodyLength(kind, body, 1+int(body[0])+8); err != nil {
			return nil, err
		}
		subjectEnd := 1 + int(body[0])
		return FailureReport{Subject: string(body[1:subjectEnd]), FirstFailureAtMillis: binary.BigEndian.Uint64(body[subjectEnd:])}, nil
	case KindClusterNotice:
		if err := controlBodyLength(kind, body, 16); err != nil {
			return nil, err
		}
		return ClusterNotice{Epoch: binary.BigEndian.Uint64(body), StateSeq: binary.BigEndian.Uint64(body[8:])}, nil
	case KindRelayProposal:
		return decodeRelayProposal(body)
	case KindRelayProposalReply:
		return decodeRelayReply(body)
	case KindMembershipRelay:
		return decodeMembershipRelay(body)
	case KindMembershipRelayReply:
		return decodeMembershipRelayReply(body)
	case KindPepperRequest:
		return decodePepperRequest(body)
	case KindPepperReply:
		return decodePepperReply(body)
	}
	return nil, unknownKindError(kind)
}

// Message is one node-to-node message: the envelope and the body behind it.
type Message struct {
	Env  Envelope
	Body []byte
}

// ChannelHandler answers one request. A nil reply with a nil error is an
// acknowledgement with no message (FAILURE_REPORT and CLUSTER_NOTICE have no
// reply kind); an error is sent to the caller as an OpError frame.
type ChannelHandler func(ctx context.Context, from Handshake, req Message) (*Message, error)

// channelOpcodes returns the request and reply opcodes of a channel (conventions
// section 6): data and seed share one pair, the kind separates them.
func channelOpcodes(ch Channel) (request, reply Opcode, ok bool) {
	switch ch {
	case ChannelData, ChannelSeed:
		return OpReplPush, OpReplAck, true
	case ChannelControl, ChannelIdentity:
		// The identity channel shares the control opcodes; the kind separates them
		// (decisions section 15, item 7).
		return OpClusterProbe, OpClusterProbeReply, true
	}
	return 0, 0, false
}

const (
	// channelHandshakeTimeout bounds an inbound connection's handshake.
	channelHandshakeTimeout = 5 * time.Second
	// channelDialTimeout bounds a dial when the call's context sets no deadline.
	channelDialTimeout = 5 * time.Second
)

// ChannelServer serves one node-to-node channel: one request frame, one
// response frame, in order, on each connection that opened with a handshake
// for that channel.
type ChannelServer struct {
	limits  ChannelLimits
	ln      net.Listener
	channel Channel
	request Opcode
	reply   Opcode
	admit   func(Handshake) error
	handle  ChannelHandler

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	inFlight  chan struct{}
	priority  chan struct{}
	perPeer   map[string]int
	closeOnce sync.Once
	closeErr  error
}

// ChannelLimits bound what a channel server holds for peers that have connected.
type ChannelLimits struct {
	// MaxConns is the most connections served at once; a connection past it is closed at
	// accept. Zero takes 256.
	MaxConns int
	// Idle is how long an authenticated connection may stay silent between frames before
	// it is closed. Zero takes two minutes.
	Idle time.Duration
	// MaxInFlight is the most requests being handled at once across the channel; a request
	// past it is answered with an overloaded error and the connection stays open. Zero
	// takes 256. A connection carries one request at a time, so MaxConns also bounds it.
	MaxInFlight int
	// PerPeerInFlight is the most requests one authenticated peer may have being handled
	// at once, so one member cannot take every slot of MaxInFlight. Zero takes a quarter of
	// MaxInFlight, at least one.
	PerPeerInFlight int
	// Priority, when set, names the request kinds that never queue behind the shared pool:
	// they have a pool of their own of PriorityInFlight (zero takes 64). The control
	// channel uses it for LEASE_REQ, which keeps every follower's lease alive and must not
	// be refused because probes are slow.
	Priority         func(kind byte) bool
	PriorityInFlight int
	// FrameTimeout bounds the rest of a frame once its first byte has arrived, and the
	// write of each reply, so a peer that trickles a frame or never reads its answer
	// cannot hold the connection. Zero takes ten seconds.
	FrameTimeout time.Duration
	// OneShot closes a connection after it has answered one request, so a caller that asks
	// again dials again. The identity channel is served this way: it carries one question.
	OneShot bool
}

// ServeChannel serves ch on ln until Close with the default limits.
func ServeChannel(ln net.Listener, ch Channel, admit func(Handshake) error, handle ChannelHandler) *ChannelServer {
	return ServeChannelWithLimits(ln, ch, admit, handle, ChannelLimits{})
}

// ServeChannelWithLimits is ServeChannel with explicit limits. admit, when set, may refuse a
// peer after its handshake (cluster id, incarnation); the connection is then
// dropped. A channel with no opcode pair (raft) is served by nothing.
func ServeChannelWithLimits(ln net.Listener, ch Channel, admit func(Handshake) error, handle ChannelHandler, limits ChannelLimits) *ChannelServer {
	if limits.MaxConns <= 0 {
		limits.MaxConns = 256
	}
	if limits.Idle <= 0 {
		limits.Idle = 2 * time.Minute
	}
	if limits.MaxInFlight <= 0 {
		limits.MaxInFlight = 256
	}
	if limits.FrameTimeout <= 0 {
		limits.FrameTimeout = 10 * time.Second
	}
	if limits.PerPeerInFlight <= 0 {
		limits.PerPeerInFlight = max(1, limits.MaxInFlight/4)
	}
	if limits.PriorityInFlight <= 0 {
		limits.PriorityInFlight = 64
	}
	request, reply, _ := channelOpcodes(ch)
	ctx, cancel := context.WithCancel(context.Background())
	s := &ChannelServer{limits: limits, ln: ln, channel: ch, request: request, reply: reply, admit: admit, handle: handle,
		ctx: ctx, cancel: cancel, conns: map[net.Conn]struct{}{}, inFlight: make(chan struct{}, limits.MaxInFlight), priority: make(chan struct{}, limits.PriorityInFlight), perPeer: map[string]int{}}
	s.wg.Add(1)
	go s.acceptLoop()
	return s
}

// Limits returns the bounds the server runs under, with the defaults filled in.
func (s *ChannelServer) Limits() ChannelLimits { return s.limits }

// Addr is the listener's address.
func (s *ChannelServer) Addr() net.Addr { return s.ln.Addr() }

// Close stops accepting, ends every connection and waits for the goroutines.
func (s *ChannelServer) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.closeErr = s.ln.Close()
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close() // best-effort: ends the connection's read
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s.closeErr
}

func (s *ChannelServer) acceptLoop() {
	defer s.wg.Done()
	for {
		raw, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.ctx.Err() != nil {
			s.mu.Unlock()
			_ = raw.Close() // best-effort: the server is closing
			return
		}
		if len(s.conns) >= s.limits.MaxConns {
			s.mu.Unlock()
			_ = raw.Close() // best-effort: over the cap, the peer redials after its backoff
			continue
		}
		s.conns[raw] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(raw)
	}
}

func (s *ChannelServer) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close() // best-effort: the peer is gone or refused
	}()
	if err := conn.SetReadDeadline(time.Now().Add(channelHandshakeTimeout)); err != nil {
		return
	}
	reader := bufio.NewReader(conn)
	hello, err := ReadHandshake(reader)
	if err != nil || Channel(hello.Channel) != s.channel {
		return
	}
	hello.PeerFingerprint = ConnFingerprint(conn)
	if s.admit != nil && s.admit(hello) != nil {
		return
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	for {
		// Silence between frames is bounded by Idle; once a frame has begun, the rest of it
		// is bounded by FrameTimeout, so a trickle does not hold the connection for Idle
		// per byte.
		if err := conn.SetReadDeadline(time.Now().Add(s.limits.Idle)); err != nil {
			return
		}
		if _, err := reader.Peek(1); err != nil {
			return
		}
		if err := conn.SetReadDeadline(time.Now().Add(s.limits.FrameTimeout)); err != nil {
			return
		}
		frame, err := DecodeFrame(reader)
		if err != nil {
			return
		}
		if s.answer(conn, hello, frame) != nil || s.limits.OneShot {
			return
		}
	}
}

// answer writes exactly one response frame for the request frame.
func (s *ChannelServer) answer(conn net.Conn, from Handshake, frame *Frame) error {
	refuseWith := func(code ErrorCode, message string) error {
		if err := conn.SetWriteDeadline(time.Now().Add(s.limits.FrameTimeout)); err != nil {
			return fmt.Errorf("arm the reply deadline: %w", err)
		}
		return WriteErrorResponse(conn, code, message, frame.RequestID)
	}
	refuse := func(message string) error { return refuseWith(ErrInvalidArgument, message) }
	if frame.Opcode != s.request {
		return refuse(fmt.Sprintf("the %s channel carries %s frames, not %s", s.channel, s.request, frame.Opcode))
	}
	env, err := DecodeEnvelope(frame.Payload)
	if err == nil {
		err = CheckKind(env.Kind, s.channel)
	}
	if err != nil {
		return refuse(err.Error())
	}
	release, admitted := s.acquire(from.NodeID, env.Kind)
	if !admitted {
		return refuseWith(ErrOverloaded, fmt.Sprintf("the %s channel of this node is handling its limit of requests (%d in all, %d for one peer). Retry shortly; the limits are cluster.peer_max_in_flight", s.channel, cap(s.inFlight), s.limits.PerPeerInFlight))
	}
	reply, err := s.handle(s.ctx, from, Message{Env: env, Body: frame.Payload[EnvelopeSize:]})
	// The slot limits requests being handled, so it is freed before the reply is written: a
	// caller that sends its next request the moment it reads this reply is then not refused
	// for a slot this request still holds.
	release()
	if err != nil {
		return refuseWith(ErrInternal, err.Error())
	}
	if err := conn.SetWriteDeadline(time.Now().Add(s.limits.FrameTimeout)); err != nil {
		return fmt.Errorf("arm the reply deadline: %w", err)
	}
	var payload []byte
	if reply != nil {
		payload = append(EncodeEnvelope(reply.Env), reply.Body...)
	}
	return WriteFrame(conn, NewFrameWithID(s.reply, frame.RequestID, payload))
}

// acquire takes a place for one request: from the priority pool for a priority kind, else
// from the shared pool, within the peer's own share of it. It never waits.
func (s *ChannelServer) acquire(peer string, kind byte) (release func(), ok bool) {
	if s.limits.Priority != nil && s.limits.Priority(kind) {
		select {
		case s.priority <- struct{}{}:
			return func() { <-s.priority }, true
		default:
			return nil, false
		}
	}
	s.mu.Lock()
	if s.perPeer[peer] >= s.limits.PerPeerInFlight {
		s.mu.Unlock()
		return nil, false
	}
	s.perPeer[peer]++
	s.mu.Unlock()
	leave := func() {
		s.mu.Lock()
		if s.perPeer[peer]--; s.perPeer[peer] <= 0 {
			delete(s.perPeer, peer)
		}
		s.mu.Unlock()
	}
	select {
	case s.inFlight <- struct{}{}:
		return func() { <-s.inFlight; leave() }, true
	default:
		leave()
		return nil, false
	}
}

// ChannelClient is a caller's side of one channel to one peer: a connection
// opened with a handshake and re-opened after any failure. Calls are serial.
type ChannelClient struct {
	addr    string
	channel Channel
	hello   func() Handshake
	turn    chan struct{}
	// holder is the message kind of the call that has the turn now, 0 when none: a call that
	// has to wait reads it to say what it waited behind. Two atomic stores per call.
	holder atomic.Uint32
	// life ends when Close is called, which cancels a call in flight.
	life    context.Context
	endLife context.CancelFunc

	// dialer opens the connection; nil dials plain TCP. A node with pinned TLS and the
	// harness's fault layer both set it before the first call.
	dialer func(ctx context.Context, addr string) (net.Conn, error)

	conn   net.Conn
	reader *bufio.Reader
	closed bool
	// served is true once the current connection has carried one reply.
	served bool
}

// ErrClosedAfterHandshake marks a call whose peer closed the connection after the handshake and
// before saying anything: a peer on another build refuses a handshake it does not read and, on
// most channels, closes without a word.
var ErrClosedAfterHandshake = errors.New("peer closed the connection right after the handshake")

// SetDialer replaces how the client opens its connection. It must be called before the
// first Call; the dialer returns a connection ready for the handshake (TLS already done).
func (c *ChannelClient) SetDialer(dialer func(ctx context.Context, addr string) (net.Conn, error)) {
	c.dialer = dialer
}

// NewChannelClient returns a client that dials addr on first use. hello is
// read at each dial, so a new connection carries the node's current epoch.
func NewChannelClient(addr string, ch Channel, hello func() Handshake) *ChannelClient {
	life, endLife := context.WithCancel(context.Background())
	return &ChannelClient{addr: addr, channel: ch, hello: hello, turn: make(chan struct{}, 1), life: life, endLife: endLife}
}

// Close ends a call in flight, drops the connection and refuses later calls.
func (c *ChannelClient) Close() error {
	c.endLife()
	c.turn <- struct{}{}
	defer func() { <-c.turn }()
	c.closed = true
	return c.drop()
}

// DropConn closes the connection, if any, without closing the client: the next call dials
// again. It waits for a call in flight, so a caller that holds the client exclusively (a pool
// that took it) never waits.
func (c *ChannelClient) DropConn() error {
	c.turn <- struct{}{}
	defer func() { <-c.turn }()
	return c.drop()
}

func (c *ChannelClient) drop() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn, c.reader = nil, nil
	return err
}

func (c *ChannelClient) dial(ctx context.Context) error {
	request, _, ok := channelOpcodes(c.channel)
	if !ok || request == 0 {
		return fmt.Errorf("the %s channel has no frame carrier", c.channel)
	}
	dialCtx, cancel := context.WithTimeout(ctx, channelDialTimeout)
	defer cancel()
	dial := c.dialer
	if dial == nil {
		dial = func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}
	}
	conn, err := dial(dialCtx, c.addr)
	if err != nil {
		return fmt.Errorf("dial %s peer %s: %w", c.channel, c.addr, err)
	}
	hello := c.hello()
	hello.Channel = byte(c.channel)
	raw, err := hello.Encode()
	if err == nil {
		_, err = conn.Write(raw)
	}
	if err != nil {
		return errors.Join(fmt.Errorf("send %s handshake to %s: %w", c.channel, c.addr, err), conn.Close())
	}
	c.conn, c.reader, c.served = conn, bufio.NewReader(conn), false
	return nil
}

// Call sends one request and returns its reply. A nil reply with a nil error
// is an acknowledgement with no message. An error frame from the peer comes
// back as a *WireError. When ctx ends, the connection is dropped, because a
// reply to an abandoned request must never be read as another's.
func (c *ChannelClient) Call(ctx context.Context, req Message) (*Message, error) {
	reply, _, err := c.CallTimed(ctx, req)
	return reply, err
}

// TurnWait is what a call found when it asked for the connection's turn. Holder is the message
// kind of the call it waited behind, as it was when the wait began, and 0 when the turn was free.
type TurnWait struct {
	Waited time.Duration
	Holder byte
}

// HolderName is the kind of call that held a turn, in the words the lapse log line uses.
func HolderName(kind byte) string {
	switch kind {
	case 0:
		return "none"
	case KindProbe, KindProbeReply:
		return "probe"
	case KindFailureReport:
		return "report"
	case KindClusterNotice:
		return "notice"
	case KindRelayProposal, KindMembershipRelay:
		return "relay"
	case KindLeaseReq, KindLeaseGrant:
		return "lease"
	}
	return "other"
}

// CallTimed is Call that also says how long the call waited for the connection's turn and which
// kind of call held it. A call that finds the turn free reports a zero TurnWait.
func (c *ChannelClient) CallTimed(ctx context.Context, req Message) (*Message, TurnWait, error) {
	var wait TurnWait
	select {
	case c.turn <- struct{}{}:
	default:
		wait.Holder = byte(c.holder.Load())
		began := time.Now()
		select {
		case c.turn <- struct{}{}:
			wait.Waited = time.Since(began)
		case <-c.life.Done():
			return nil, wait, fmt.Errorf("the %s channel client for %s is closed", c.channel, c.addr)
		case <-ctx.Done():
			return nil, wait, fmt.Errorf("waiting to call the %s channel of %s: %w", c.channel, c.addr, context.Cause(ctx))
		}
	}
	c.holder.Store(uint32(req.Env.Kind))
	defer func() { c.holder.Store(0); <-c.turn }()
	reply, err := c.callWithTurn(ctx, req)
	return reply, wait, err
}

// callWithTurn is the body of a call made while holding the connection's turn.
func (c *ChannelClient) callWithTurn(ctx context.Context, req Message) (*Message, error) {
	if c.closed {
		return nil, fmt.Errorf("the %s channel client for %s is closed", c.channel, c.addr)
	}
	if c.conn == nil {
		if err := c.dial(ctx); err != nil {
			return nil, err
		}
	}
	reply, err := c.exchange(ctx, req)
	var wire *WireError
	if err != nil && !errors.As(err, &wire) {
		return nil, errors.Join(err, c.drop())
	}
	return reply, err
}

func (c *ChannelClient) exchange(ctx context.Context, req Message) (*Message, error) {
	request, replyOp, _ := channelOpcodes(c.channel)
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetDeadline(deadline); err != nil {
			return nil, fmt.Errorf("set the %s call deadline: %w", c.channel, err)
		}
	} else if err := c.conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("clear the %s call deadline: %w", c.channel, err)
	}
	conn := c.conn
	interrupt := func() { _ = conn.SetDeadline(time.Unix(1, 0)) }
	defer context.AfterFunc(ctx, interrupt)()
	defer context.AfterFunc(c.life, interrupt)()
	frame := NewFrame(request, append(EncodeEnvelope(req.Env), req.Body...))
	if err := WriteFrame(conn, frame); err != nil {
		if !c.served && (errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)) {
			return nil, fmt.Errorf("write to %s peer %s: %w: %w", c.channel, c.addr, ErrClosedAfterHandshake, err)
		}
		return nil, fmt.Errorf("write to %s peer %s: %w", c.channel, c.addr, err)
	}
	response, err := DecodeFrame(c.reader)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("call %s peer %s: %w", c.channel, c.addr, context.Cause(ctx))
		}
		if c.life.Err() != nil {
			return nil, fmt.Errorf("call %s peer %s ended because the client was closed", c.channel, c.addr)
		}
		if !c.served && (errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) {
			return nil, fmt.Errorf("read from %s peer %s: %w: %w", c.channel, c.addr, ErrClosedAfterHandshake, err)
		}
		return nil, fmt.Errorf("read from %s peer %s: %w", c.channel, c.addr, err)
	}
	c.served = true
	if response.RequestID != frame.RequestID {
		return nil, fmt.Errorf("%s peer %s answered request %s with %s", c.channel, c.addr, frame.RequestIDString(), response.RequestIDString())
	}
	switch response.Opcode {
	case OpError:
		return nil, decodeWireError(response.Payload)
	case replyOp:
	default:
		return nil, fmt.Errorf("%s peer %s answered with %s, want %s", c.channel, c.addr, response.Opcode, replyOp)
	}
	if len(response.Payload) == 0 {
		return nil, nil
	}
	env, err := DecodeEnvelope(response.Payload)
	if err == nil {
		err = CheckKind(env.Kind, c.channel)
	}
	if err != nil {
		return nil, fmt.Errorf("%s peer %s sent a bad reply: %w", c.channel, c.addr, err)
	}
	return &Message{Env: env, Body: response.Payload[EnvelopeSize:]}, nil
}

// decodeWireError reads an OpError payload.
func decodeWireError(payload []byte) error {
	var doc struct {
		Code    int32  `bson:"code"`
		Message string `bson:"message"`
	}
	if err := bson.Unmarshal(payload, &doc); err != nil {
		return fmt.Errorf("peer sent an unreadable error frame: %w", err)
	}
	return &WireError{Code: ErrorCode(doc.Code), Message: doc.Message}
}
