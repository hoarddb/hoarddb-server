package ringclient

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// DefaultRequestTimeout is how long a call waits for its answer when Options.RequestTimeout is
// zero. A server bounds its own work below it so a default client reads the server's answer.
const DefaultRequestTimeout = 30 * time.Second

const (
	defaultDialTimeout    = 10 * time.Second
	defaultRequestTimeout = DefaultRequestTimeout
	clientName            = "ringclient"
)

// errClosed is why a Client that Close was called on refuses calls.
var errClosed = errors.New("ringclient: the client is closed")

// loginRequest is the AUTH body. It is the client-side twin of the server's LoginRequest,
// declared here so this package does not import the server.
type loginRequest struct {
	Username        string   `bson:"username"`
	Password        string   `bson:"password"`
	ProtocolVersion uint32   `bson:"protocol_version"`
	Capabilities    []string `bson:"capabilities"`
	Client          string   `bson:"client"`
}

// loginReply is the part of the AUTH_OK body this package reads.
type loginReply struct {
	// PasswordMinLength is the server's HOARDDB_PASSWORD_MIN_LENGTH; zero when it reports none.
	PasswordMinLength int32 `bson:"password_min_length"`
}

// authFailure is the body of an AUTH_FAIL frame.
type authFailure struct {
	Reason  uint8  `bson:"reason"`
	Message string `bson:"message,omitempty"`
}

// Client is one authenticated connection to a node. It is safe for concurrent use: requests
// from many goroutines are pipelined on the connection and each reply is matched to its
// request by request id.
type Client struct {
	opts Options
	conn net.Conn

	writeLock sync.Mutex

	state   sync.Mutex
	pending map[[16]byte]chan *transport.Frame
	failure error // why the connection ended; set once, under state

	reader sync.WaitGroup

	idPrefix  [8]byte
	idCounter atomic.Uint64

	headerLock sync.Mutex
	headers    map[string]BucketInfo

	// passwordMinLength is what the server reported at login; it is set before Dial returns.
	passwordMinLength int
}

func trustPolicy(opts Options) (transport.TrustPolicy, error) {
	var pinned []string
	for _, fingerprint := range append([]string{opts.Fingerprint}, opts.Fingerprints...) {
		if fingerprint != "" {
			pinned = append(pinned, fingerprint)
		}
	}
	switch {
	case len(pinned) > 0:
		return transport.TrustPolicy{Expected: pinned}, nil
	case opts.Insecure:
		return transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "ringclient Options.Insecure, local development only"}}, nil
	}
	return transport.TrustPolicy{}, errors.New("ringclient: Options trusts no server. Set Fingerprint to the value the server prints when it starts, " +
		"or set Insecure for a node you started yourself on this machine (it skips certificate checks and is for local development only)")
}

// Dial connects to opts.Addr over TLS, logs in and returns the ready client. The context
// bounds the connect and the login only; it does not outlive the call.
func Dial(ctx context.Context, opts Options) (*Client, error) {
	trust, err := trustPolicy(opts)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := transport.MakeClientTLSConfig("", "", trust, opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("ringclient: building the TLS config for %s: %w", opts.Addr, err)
	}
	timeout := opts.DialTimeout
	if timeout <= 0 {
		timeout = defaultDialTimeout
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dialer := tls.Dialer{NetDialer: &net.Dialer{}, Config: tlsConfig}
	raw, err := dialer.DialContext(dialCtx, "tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("ringclient: connecting to %s: %w. Check that hoarddb-server is running there and, with Fingerprint, that it matches the one the server printed", opts.Addr, err)
	}
	client := &Client{opts: opts, conn: raw, pending: map[[16]byte]chan *transport.Frame{}, headers: map[string]BucketInfo{}}
	if _, err := rand.Read(client.idPrefix[:]); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("ringclient: reading random bytes for request ids: %w", err)
	}
	reader := bufio.NewReader(raw)
	if err := client.login(dialCtx, reader); err != nil {
		_ = raw.Close()
		return nil, err
	}
	client.reader.Add(1)
	go client.readLoop(reader)
	return client, nil
}

// login runs AUTH before the reader goroutine starts, so it reads the reply itself.
func (c *Client) login(ctx context.Context, reader *bufio.Reader) error {
	body, err := bson.Marshal(loginRequest{Username: c.opts.User, Password: c.opts.Password, Client: clientName})
	if err != nil {
		return fmt.Errorf("ringclient: encoding the login: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("ringclient: setting the login deadline: %w", err)
		}
		defer func() { _ = c.conn.SetDeadline(time.Time{}) }()
	}
	if _, err := c.conn.Write(transport.NewFrame(transport.OpAuth, body).Encode()); err != nil {
		return fmt.Errorf("ringclient: sending the login to %s: %w", c.opts.Addr, err)
	}
	reply, err := transport.DecodeFrame(reader)
	if err != nil {
		return fmt.Errorf("ringclient: reading the login reply from %s: %w", c.opts.Addr, err)
	}
	switch reply.Opcode {
	case transport.OpAuthOK:
		if len(reply.Payload) == 0 {
			return nil
		}
		var accepted loginReply
		if err := bson.Unmarshal(reply.Payload, &accepted); err != nil {
			return fmt.Errorf("ringclient: decoding the login reply from %s: %w. Check that the server is a hoarddb server of this version", c.opts.Addr, err)
		}
		c.passwordMinLength = int(accepted.PasswordMinLength)
		return nil
	case transport.OpAuthFail:
		var failure authFailure
		detail := "wrong user or password"
		if len(reply.Payload) > 0 && bson.Unmarshal(reply.Payload, &failure) == nil && failure.Message != "" {
			detail = failure.Message
		}
		return fmt.Errorf("ringclient: %s rejected the login of user %q: %s. Check the user name and the password in Options", c.opts.Addr, c.opts.User, detail)
	case transport.OpError:
		return fmt.Errorf("ringclient: %s refused the login of user %q: %w", c.opts.Addr, c.opts.User, refusalError(reply.Payload))
	}
	return fmt.Errorf("ringclient: %s answered the login with opcode %s", c.opts.Addr, reply.Opcode)
}

// PasswordMinLength is the shortest new password, in characters, the server accepts
// (its HOARDDB_PASSWORD_MIN_LENGTH), as it reported at login; 0 when it reported none. The server
// never receives a password, only the credential derived from it, so it cannot measure one: a
// caller that takes a new password for [Client.CreateUser] or [Client.AlterUser] checks it here.
func (c *Client) PasswordMinLength() int { return c.passwordMinLength }

// NextRequestID returns a request id no other request of this client uses: 8 random bytes
// chosen at Dial and an 8-byte counter. Keep it to retry a write with [WithRequestID].
func (c *Client) NextRequestID() [16]byte {
	var id [16]byte
	copy(id[:8], c.idPrefix[:])
	binary.BigEndian.PutUint64(id[8:], c.idCounter.Add(1))
	return id
}

// readLoop hands every reply frame to the request that is waiting for it. It exits when the
// connection ends, and Close waits for it.
func (c *Client) readLoop(reader *bufio.Reader) {
	defer c.reader.Done()
	for {
		frame, err := transport.DecodeFrame(reader)
		if err != nil {
			c.fail(fmt.Errorf("ringclient: the connection to %s ended: %w", c.opts.Addr, err))
			return
		}
		c.state.Lock()
		waiter, ok := c.pending[frame.RequestID]
		delete(c.pending, frame.RequestID)
		c.state.Unlock()
		if ok {
			waiter <- frame // buffered, and this goroutine is its only sender
		}
	}
}

// fail records why the connection ended and wakes every waiter. Only the first reason is kept.
func (c *Client) fail(reason error) {
	c.state.Lock()
	if c.failure == nil {
		c.failure = reason
	}
	waiters := c.pending
	c.pending = map[[16]byte]chan *transport.Frame{}
	c.state.Unlock()
	for _, waiter := range waiters {
		close(waiter) // removed from the map above, so the reader can no longer send on it
	}
	_ = c.conn.Close()
}

// endedError is what a call gets once the connection is over.
func (c *Client) endedError() error {
	c.state.Lock()
	defer c.state.Unlock()
	if c.failure == nil {
		return errClosed
	}
	return c.failure
}

// Close ends the connection, wakes every request still waiting (they fail with an error) and
// waits for the reader goroutine to exit. It is safe to call more than once.
func (c *Client) Close() error {
	c.fail(errClosed)
	c.reader.Wait()
	return nil
}

// roundtrip sends one frame under id and waits for the frame that answers it.
func (c *Client) roundtrip(ctx context.Context, op transport.Opcode, id [16]byte, payload []byte) (*transport.Frame, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		timeout := c.opts.RequestTimeout
		if timeout <= 0 {
			timeout = defaultRequestTimeout
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	waiter := make(chan *transport.Frame, 1)
	c.state.Lock()
	if c.failure != nil {
		failure := c.failure
		c.state.Unlock()
		return nil, failure
	}
	if _, inFlight := c.pending[id]; inFlight {
		c.state.Unlock()
		return nil, fmt.Errorf("ringclient: request id %x is already in flight on this client; ids must be unique among the requests waiting for an answer", id)
	}
	c.pending[id] = waiter
	c.state.Unlock()

	if err := c.write(ctx, transport.NewFrameWithID(op, id, payload)); err != nil {
		c.forget(id)
		return nil, err
	}
	select {
	case frame, ok := <-waiter:
		if !ok {
			return nil, c.endedError()
		}
		return frame, nil
	case <-ctx.Done():
		c.forget(id)
		return nil, fmt.Errorf("ringclient: no answer to %s (request id %x) before the deadline, so the server may or may not have applied it. "+
			"To retry a write safely, pass WithRequestID(%x): %w", op, id, id, ctx.Err())
	}
}

func (c *Client) forget(id [16]byte) {
	c.state.Lock()
	delete(c.pending, id)
	c.state.Unlock()
}

// write sends one frame. A write that fails or times out may have sent half a frame, so the
// connection is ended: the stream cannot be trusted afterwards.
func (c *Client) write(ctx context.Context, frame *transport.Frame) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			c.fail(fmt.Errorf("ringclient: setting the write deadline on the connection to %s: %w", c.opts.Addr, err))
			return c.endedError()
		}
	}
	if _, err := c.conn.Write(frame.Encode()); err != nil {
		c.fail(fmt.Errorf("ringclient: writing to %s: %w", c.opts.Addr, err))
		return c.endedError()
	}
	return nil
}

// call sends a request and returns the payload of its RING_REPLY, or the server's refusal as
// an *Error.
func (c *Client) call(ctx context.Context, op transport.Opcode, id [16]byte, payload []byte) ([]byte, error) {
	frame, err := c.roundtrip(ctx, op, id, payload)
	if err != nil {
		return nil, err
	}
	switch frame.Opcode {
	case transport.OpRingReply:
		return frame.Payload, nil
	case transport.OpError:
		return nil, refusalError(frame.Payload)
	}
	return nil, frameError(frame.Opcode, op.String())
}
