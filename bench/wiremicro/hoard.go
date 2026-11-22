package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// hoardEngine drives the entry-ring server over its real TLS listener: AUTH as
// the bootstrap user, then RING_ADMIN to create the database and bucket and
// RING_KEYED for every insert and get.
type hoardEngine struct {
	cfg       *config
	tlsConfig *tls.Config
	database  string
	bucket    hoardBucket
}

func newHoardEngine(cfg *config) (*hoardEngine, error) {
	trust := transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "bench/wiremicro against a self-signed benchmark node"}}
	if cfg.HoardFingerprint != "" {
		trust = transport.TrustPolicy{Expected: []string{cfg.HoardFingerprint}}
	}
	tlsConfig, err := transport.MakeClientTLSConfig("", "", trust, cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("building the TLS config for %s: %w", cfg.Addr, err)
	}
	return &hoardEngine{cfg: cfg, tlsConfig: tlsConfig, database: fmt.Sprintf("%s_%s", cfg.Name, cfg.RunID)}, nil
}

func (e *hoardEngine) target() string { return e.cfg.Addr }

func (e *hoardEngine) close() {}

// readVariables reads the launch environment file. The server reports none of
// its durability settings on the wire (HealthResponse has no field for them),
// so the file and the stated table-sync mode are the record.
func (e *hoardEngine) readVariables(context.Context) (map[string]string, error) {
	text, err := os.ReadFile(e.cfg.HoardEnv)
	if err != nil {
		return nil, fmt.Errorf("reading the launch environment %s (-hoard-env): %w", e.cfg.HoardEnv, err)
	}
	vars := hoardLaunchVariables(string(text), e.cfg.HoardTableSync)
	vars["table_sync_reported_by_server"] = "no: stated by the operator with -hoard-table-sync"
	return vars, nil
}

func (e *hoardEngine) setup(ctx context.Context, _ int) error {
	conn, err := e.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.close()
	if _, err := conn.admin(wire.RingAdminRequest{Cmd: wire.AdminCreateDatabase, Name: e.database}); err != nil {
		return fmt.Errorf("creating database %q: %w", e.database, err)
	}
	created, err := conn.admin(wire.RingAdminRequest{
		Cmd: wire.AdminCreateBucket, Database: e.database, Name: hoardBucketName,
		Fields: []wire.RingAdminField{{Name: hoardPayloadField, Type: "string"}},
	})
	if err != nil {
		return fmt.Errorf("creating bucket %q in database %q: %w", hoardBucketName, e.database, err)
	}
	e.bucket, err = bucketFromAdminReply(created)
	return err
}

func (e *hoardEngine) newWorker(ctx context.Context) (worker, error) {
	conn, err := e.dial(ctx)
	if err != nil {
		return nil, err
	}
	return &hoardWorker{conn: conn, bucket: e.bucket}, nil
}

// hoardConn is one authenticated TLS socket, used by one goroutine.
type hoardConn struct {
	raw     *tls.Conn
	reader  *bufio.Reader
	timeout time.Duration
}

func (e *hoardEngine) dial(ctx context.Context) (*hoardConn, error) {
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: e.tlsConfig}
	raw, err := dialer.DialContext(ctx, "tcp", e.cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("dialing %s over TLS: %w", e.cfg.Addr, err)
	}
	tlsConn, ok := raw.(*tls.Conn)
	if !ok {
		_ = raw.Close()
		return nil, fmt.Errorf("dialing %s gave a %T, not a TLS connection", e.cfg.Addr, raw)
	}
	conn := &hoardConn{raw: tlsConn, reader: bufio.NewReader(tlsConn), timeout: e.cfg.Timeout}
	if err := conn.login(ctx, e.cfg.User, e.cfg.Password); err != nil {
		conn.close()
		return nil, fmt.Errorf("logging in to %s as %q: %w", e.cfg.Addr, e.cfg.User, err)
	}
	return conn, nil
}

func (c *hoardConn) close() { _ = c.raw.Close() }

func (c *hoardConn) roundtrip(ctx context.Context, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	deadline := time.Now().Add(c.timeout)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	if err := c.raw.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("setting the deadline: %w", err)
	}
	if _, err := c.raw.Write(transport.NewFrame(op, payload).Encode()); err != nil {
		return nil, fmt.Errorf("writing %s: %w", op, err)
	}
	reply, err := transport.DecodeFrame(c.reader)
	if err != nil {
		return nil, fmt.Errorf("reading the %s reply: %w", op, err)
	}
	return reply, nil
}

func (c *hoardConn) login(ctx context.Context, user, password string) error {
	payload, err := bson.Marshal(&server.LoginRequest{Username: user, Password: password})
	if err != nil {
		return fmt.Errorf("encoding the login: %w", err)
	}
	reply, err := c.roundtrip(ctx, transport.OpAuth, payload)
	if err != nil {
		return err
	}
	switch reply.Opcode {
	case transport.OpAuthOK:
		return nil
	case transport.OpAuthFail:
		var failure server.AuthFailResponse
		if len(reply.Payload) > 0 && bson.Unmarshal(reply.Payload, &failure) == nil && failure.Message != "" {
			return fmt.Errorf("rejected: %s", failure.Message)
		}
		return errors.New("rejected: wrong user or password; set WIREMICRO_PASSWORD to the bootstrap user's password")
	case transport.OpError:
		return fmt.Errorf("refused: %s", errorFrameText(reply))
	default:
		return fmt.Errorf("answered with unexpected opcode %s", reply.Opcode)
	}
}

// errorFrameText is the text of an OpError frame, ring or legacy shape.
func errorFrameText(frame *transport.Frame) string {
	if refusal, err := wire.DecodeRingError(frame.Payload); err == nil {
		return fmt.Sprintf("[%s] %s", refusal.Code, refusal.Message)
	}
	var legacy server.ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &legacy); err == nil && legacy.Message != "" {
		return legacy.Message
	}
	return fmt.Sprintf("an error frame of %d bytes that does not decode", len(frame.Payload))
}

func (c *hoardConn) admin(req wire.RingAdminRequest) (wire.RingAdminReply, error) {
	body, err := wire.EncodeRingAdmin(req)
	if err != nil {
		return wire.RingAdminReply{}, fmt.Errorf("encoding %s: %w", req.Cmd, err)
	}
	frame, err := c.roundtrip(context.Background(), transport.OpRingAdmin, body)
	if err != nil {
		return wire.RingAdminReply{}, err
	}
	if frame.Opcode != transport.OpRingReply {
		return wire.RingAdminReply{}, fmt.Errorf("%s was answered %s: %s", req.Cmd, frame.Opcode, errorFrameText(frame))
	}
	reply, err := wire.DecodeRingAdminReply(frame.Payload)
	if err != nil {
		return wire.RingAdminReply{}, fmt.Errorf("decoding the %s reply: %w", req.Cmd, err)
	}
	return reply, nil
}

// keyed sends one RING_KEYED request and returns its reply.
func (c *hoardConn) keyed(ctx context.Context, req wire.RingKeyedRequest) (wire.RingReply, error) {
	body, err := wire.EncodeRingKeyed(req)
	if err != nil {
		return wire.RingReply{}, fmt.Errorf("encoding the request: %w", err)
	}
	frame, err := c.roundtrip(ctx, transport.OpRingKeyed, body)
	if err != nil {
		return wire.RingReply{}, err
	}
	if frame.Opcode != transport.OpRingReply {
		return wire.RingReply{}, fmt.Errorf("a ring request was answered %s: %s", frame.Opcode, errorFrameText(frame))
	}
	reply, err := wire.DecodeRingReply(frame.Payload)
	if err != nil {
		return wire.RingReply{}, fmt.Errorf("decoding the reply: %w", err)
	}
	return reply, nil
}

type hoardWorker struct {
	conn   *hoardConn
	bucket hoardBucket
}

func (w *hoardWorker) close() { w.conn.close() }

func (w *hoardWorker) insert(ctx context.Context, id, payload string) error {
	op, err := w.bucket.insertOp(id, payload)
	if err != nil {
		return err
	}
	reply, err := w.conn.keyed(ctx, wire.RingKeyedRequest{Op: op})
	if err != nil {
		return fmt.Errorf("insert %s: %w", id, err)
	}
	return requireApplied(id, reply)
}

// requireApplied accepts only a write the server says it applied. An
// applied_degraded or unknown outcome is not a durable acknowledgement, so it
// fails the run instead of counting as an insert.
func requireApplied(id string, reply wire.RingReply) error {
	if reply.WriteOutcome != "applied" {
		return fmt.Errorf("insert %s: write_outcome is %q, want applied", id, reply.WriteOutcome)
	}
	return nil
}

func (w *hoardWorker) get(ctx context.Context, id string) error {
	read, err := w.bucket.getRead(id)
	if err != nil {
		return err
	}
	reply, err := w.conn.keyed(ctx, wire.RingKeyedRequest{Read: &read})
	if err != nil {
		return fmt.Errorf("get %s: %w", id, err)
	}
	if reply.Found == nil || !*reply.Found || len(reply.Elements) != 1 {
		return fmt.Errorf("get %s: %w", id, errNotFound)
	}
	return nil
}

// ledgerHeader is what a verify run needs to address the bucket this run created.
func (e *hoardEngine) ledgerHeader() string {
	b := e.bucket
	return fmt.Sprintf("database=%s bucket=%d decl=%d seen=%d payload_field=%d", e.database, b.BucketID, b.DeclVersion, b.SchemaVersionSeen, b.PayloadFieldID)
}

// adoptLedgerHeader points the engine at the bucket a ledger header names.
func (e *hoardEngine) adoptLedgerHeader(header string) error {
	var database string
	var b hoardBucket
	var fieldID uint16
	if _, err := fmt.Sscanf(header, "database=%s bucket=%d decl=%d seen=%d payload_field=%d", &database, &b.BucketID, &b.DeclVersion, &b.SchemaVersionSeen, &fieldID); err != nil {
		return fmt.Errorf("the ledger header %q does not parse: %w", header, err)
	}
	b.PayloadFieldID = fieldID
	e.database, e.bucket = database, b
	return nil
}
