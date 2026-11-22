package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// frameVectors are the codec-only vectors: frames built by the server's own
// encoder, each checked against the server's own decoder.
func frameVectors() ([]vector, error) {
	getPayload, err := bson.Marshal(server.FindRequest{Database: "v000", Bucket: "users", Key: "ada"})
	if err != nil {
		return nil, fmt.Errorf("encoding the frame vectors' payload: %w", err)
	}
	accept := []struct {
		name  string
		frame *transport.Frame
		notes string
	}{
		{"frame: an empty payload", transport.NewFrameWithID(transport.OpPing, fixedRequestID(1), nil),
			"The smallest frame: length 17 (opcode and request ID, no payload)."},
		{"frame: a BSON payload", transport.NewFrameWithID(transport.OpGet, fixedRequestID(2), getPayload),
			"Length counts opcode, request ID and payload, little-endian; not the version byte or itself."},
		{"frame: a zero request ID", transport.NewFrameWithID(transport.OpError, [transport.RequestIDLen]byte{}, nil),
			"An all-zero request ID is legal on the wire; the server uses it only for connection-level refusals."},
	}
	var vectors []vector
	for _, frameCase := range accept {
		encoded := frameCase.frame.Encode()
		decoded, err := transport.DecodeFrame(bytes.NewReader(encoded))
		if err != nil {
			return nil, fmt.Errorf("%s: the server's decoder refuses its own frame: %w", frameCase.name, err)
		}
		version := decoded.Version
		opcode := uint8(decoded.Opcode)
		vectors = append(vectors, vector{
			Name: frameCase.name, Group: "frame", Kind: "frame", Status: "frozen", Notes: frameCase.notes,
			FrameHex: hex.EncodeToString(encoded),
			FrameExpect: &frameExpect{
				Result: "accept", Version: &version, Length: binary.LittleEndian.Uint32(encoded[1:5]), Opcode: &opcode,
				RequestIDHex: hex.EncodeToString(decoded.RequestID[:]), PayloadHex: hex.EncodeToString(decoded.Payload),
			},
		})
	}

	header := func(version byte, length uint32) []byte {
		raw := make([]byte, transport.FrameHeaderSize)
		raw[0] = version
		binary.LittleEndian.PutUint32(raw[1:5], length)
		raw[5] = byte(transport.OpGet)
		id := fixedRequestID(3)
		copy(raw[6:], id[:])
		return raw
	}
	truncated := append(header(transport.ProtocolVersion, 1+transport.RequestIDLen+10), 0x01, 0x02)
	refuse := []struct {
		name, because, notes string
		raw                  []byte
	}{
		{"frame refused: length under 17", "length-too-small", "A length that cannot hold the opcode and request ID.",
			header(transport.ProtocolVersion, 16)},
		{"frame refused: length over 32 MiB", "length-too-large",
			"33,554,433: one byte over the cap. A decoder refuses it from the 5-byte prefix, before allocating the body.",
			header(transport.ProtocolVersion, transport.MaxFrameLength+1)},
		{"frame refused: version 2", "bad-version", "The version byte must be 1.", header(2, 1+transport.RequestIDLen)},
		{"frame refused: truncated body", "truncated", "The length promises 10 payload bytes and the stream ends after 2.", truncated},
	}
	for _, frameCase := range refuse {
		if _, err := transport.DecodeFrame(bufio.NewReader(bytes.NewReader(frameCase.raw))); err == nil {
			return nil, fmt.Errorf("%s: the server's decoder accepts it", frameCase.name)
		}
		vectors = append(vectors, vector{
			Name: frameCase.name, Group: "frame", Kind: "frame", Status: "frozen", Notes: frameCase.notes,
			FrameHex:    hex.EncodeToString(frameCase.raw),
			FrameExpect: &frameExpect{Result: "refuse", RefuseBecause: frameCase.because},
		})
	}
	return vectors, nil
}

// decodeVector builds a decode vector from a frame the server's types and
// encoder produce.
func decodeVector(name, notes string, frame *transport.Frame, expect frameExpect) vector {
	return vector{
		Name: name, Group: "decode", Kind: "decode", Status: "frozen", Notes: notes,
		FrameHex: hex.EncodeToString(frame.Encode()), FrameExpect: &expect,
	}
}

func intPointer(value int) *int { return &value }

// decodeVectors are the answers a test cannot provoke on demand, built with
// the server's own response types and encoder. firstElement is the live frame
// captureFirstElementRefusal took; it is placed here, at its row's place in
// the table, rather than with the other live captures.
func decodeVectors(firstElement *transport.Frame) ([]vector, error) {
	encode := func(value any) ([]byte, error) {
		encoded, err := bson.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encoding a decode vector: %w", err)
		}
		return encoded, nil
	}
	codeZero, err := encode(server.ErrorResponse{Code: 0, Message: "an error frame whose code was never set"})
	if err != nil {
		return nil, err
	}
	noCode, err := encode(struct {
		Message string `bson:"message"`
	}{"an error frame with no code at all"})
	if err != nil {
		return nil, err
	}
	unknownCode, err := encode(server.ErrorResponse{Code: 0x63, Message: "a code this driver does not know"})
	if err != nil {
		return nil, err
	}
	resultTooLarge, err := encode(server.ErrorResponse{
		Code: int(transport.ErrResultTooLarge), Message: "this query's result is larger than the node holds for one answer",
		MaxResultBytes: 16 << 20, RetainedBytes: 16<<20 + 4096,
	})
	if err != nil {
		return nil, err
	}
	// 5,033,164 bytes and 52,428 entries are the bound of a node at the 32 MiB
	// floor memtable; 60 documents of 100 KiB are what refuses at it.
	batchTooLarge, err := encode(server.ErrorResponse{
		Code:         int(transport.ErrBatchTooLarge),
		Message:      server.OverBoundBatchMessage(5_033_164, 52_428, false, 60, 6_144_000),
		WriteOutcome: "not_applied",
	})
	if err != nil {
		return nil, err
	}
	timeout, err := encode(server.ErrorResponse{
		Code: int(transport.ErrTimeout),
		Message: "write concern all was not met in 5s: the write is stored on the owner and confirmed by 1 of 3 in-sync copies, " +
			"and will reach the others when they are reachable. Do not retry a push or an $inc — it is already stored",
		WriteOutcome: "applied", CopiesConfirmed: 1, ReplicationFactor: 3,
	})
	if err != nil {
		return nil, err
	}
	ownershipMoved, err := encode(server.ErrorResponse{
		Code:         int(transport.ErrUnavailable),
		Message:      server.OwnershipMovedMessage("v000", "users"),
		WriteOutcome: "unknown",
	})
	if err != nil {
		return nil, err
	}
	tokenUnsupported, err := encode(server.AuthFailResponse{Reason: uint8(transport.AuthFailTokenUnsupported)})
	if err != nil {
		return nil, err
	}
	configMissing, err := encode(server.AuthFailResponse{
		Reason:  uint8(transport.AuthFailInternal),
		Message: "this node has no auth config yet; it is waiting for the cluster master to send it — retry shortly",
	})
	if err != nil {
		return nil, err
	}
	skipped, err := encode(server.DeleteResponse{
		Success: false, Skipped: true,
		Message: `Document "s1" in bucket "sessions" of database "v000" has expired and was not deleted: its store fields ` +
			`hold too many elements to delete in one step (Events: 125,000). It is left whole and every TTL sweep tries ` +
			`it again. Empty its store fields — db.sessions["s1"].Events.pop() until len() is 0 — and the next sweep ` +
			`expires it, or delete the document yourself once they are empty.`,
	})
	if err != nil {
		return nil, err
	}
	skippedJSON, err := extendedJSON(skipped)
	if err != nil {
		return nil, err
	}
	id := fixedRequestID(9000)
	zero := [transport.RequestIDLen]byte{}
	return []vector{
		decodeVector("error with code 0", "Code 0 on an ERROR frame is INTERNAL, never success.",
			transport.NewFrameWithID(transport.OpError, id, codeZero),
			frameExpect{Result: "error", Code: intPointer(0), Name: "INTERNAL", RequestIDZero: boolPointer(false), ConnectionFatal: boolPointer(false)}),
		decodeVector("error with no code", "No code on an ERROR frame is INTERNAL, never success.",
			transport.NewFrameWithID(transport.OpError, id, noCode),
			frameExpect{Result: "error", Code: intPointer(0), Name: "INTERNAL", RequestIDZero: boolPointer(false), ConnectionFatal: boolPointer(false)}),
		decodeVector("error with a code the driver does not know", "Kept as UNKNOWN(99) with the message, and treated like INTERNAL.",
			transport.NewFrameWithID(transport.OpError, id, unknownCode),
			frameExpect{Result: "error", Code: intPointer(0x63), Name: "UNKNOWN(99)", RequestIDZero: boolPointer(false), ConnectionFatal: boolPointer(false)}),
		decodeVector("result too large with its two numbers",
			"max_result_bytes and retained_bytes (a lower bound) come only with RESULT_TOO_LARGE; the connection is unaffected. "+
				"The live capture is the vector \"a query over the result ceiling\".",
			transport.NewFrameWithID(transport.OpError, id, resultTooLarge),
			frameExpect{Result: "error", Code: intPointer(int(transport.ErrResultTooLarge)), Name: "RESULT_TOO_LARGE", RequestIDZero: boolPointer(false), ConnectionFatal: boolPointer(false)}),
		liveDecodeAnswer("result too large with retained_bytes absent",
			"Captured live with HOARDDB_QUERY_MAX_RESULT_BYTES=1048576 over one document of 1.5 MB: the first result alone is "+
				"over the ceiling, so nothing was retained and the server sends no retained_bytes (it does not send a zero), which a "+
				"driver reads as 0. max_result_bytes is present, and the connection is unaffected.",
			firstElement, transport.ErrResultTooLarge),
		decodeVector("batch over the store's atomic bound",
			"BATCH_TOO_LARGE with write_outcome not_applied: nothing of the batch was written; halve it and resend. "+
				"A driver splits on the code and never on the message. Built with the server's own message "+
				"(server/errors.go:OverBoundBatchMessage) because the live request is about 5 MB, which the file's "+
				"size budget leaves to harness leg 6.",
			transport.NewFrameWithID(transport.OpError, id, batchTooLarge),
			frameExpect{Result: "error", Code: intPointer(int(transport.ErrBatchTooLarge)), Name: "BATCH_TOO_LARGE", RequestIDZero: boolPointer(false), ConnectionFatal: boolPointer(false)}),
		decodeVector("write concern not met in time",
			"TIMEOUT with write_outcome applied: the write is stored; never resend it. Message format of "+
				"server/write_path.go:concernNotMet.",
			transport.NewFrameWithID(transport.OpError, id, timeout),
			frameExpect{Result: "error", Code: intPointer(int(transport.ErrTimeout)), Name: "TIMEOUT", RequestIDZero: boolPointer(false), ConnectionFatal: boolPointer(false)}),
		decodeVector("unavailable with an unknown write outcome",
			"The owner changed under the write: write_outcome unknown, so the caller reads back before resending. "+
				"Message format of server/write_path.go:ownershipMovedErr.",
			transport.NewFrameWithID(transport.OpError, id, ownershipMoved),
			frameExpect{Result: "error", Code: intPointer(int(transport.ErrUnavailable)), Name: "UNAVAILABLE", RequestIDZero: boolPointer(false), ConnectionFatal: boolPointer(false)}),
		decodeVector("auth refused with an empty payload", "An empty AUTH_FAIL, which older servers send, is CREDENTIALS.",
			transport.NewFrameWithID(transport.OpAuthFail, id, nil),
			frameExpect{Result: "auth_fail", AuthFailReason: intPointer(int(transport.AuthFailCredentials)), Name: "CREDENTIALS", RequestIDZero: boolPointer(false)}),
		decodeVector("auth refused: tokens unsupported",
			"Reason 4: a server without the auth_token capability. This server never sends it; a driver discards its tokens.",
			transport.NewFrameWithID(transport.OpAuthFail, id, tokenUnsupported),
			frameExpect{Result: "auth_fail", AuthFailReason: intPointer(int(transport.AuthFailTokenUnsupported)), Name: "TOKEN_UNSUPPORTED", RequestIDZero: boolPointer(false)}),
		decodeVector("auth refused: internal",
			"Reason 8: here, a cluster node that has not received its user configuration. Not a credential failure; "+
				"retry on another node or after a short wait. Message of server/authconfig.go:Server.authConfigUnavailableReason.",
			transport.NewFrameWithID(transport.OpAuthFail, id, configMissing),
			frameExpect{Result: "auth_fail", AuthFailReason: intPointer(int(transport.AuthFailInternal)), Name: "INTERNAL", RequestIDZero: boolPointer(false)}),
		decodeVector("delete answered skipped",
			"a conditional delete (expire_if) of an expired document whose store fields hold more elements "+
				"than one atomic batch deletes answers success false and skipped true, and leaves the document whole. "+
				"Provoking it takes about 125,000 store-field elements, so it is built with the server's own "+
				"DeleteResponse; the message follows server/fieldstore.go:expirySkippedMessage. The live delete "+
				"vectors with expire_if (expired, and spared) are exchanges.",
			transport.NewFrameWithID(transport.OpDelete, id, skipped),
			frameExpect{Result: "answer", PayloadJSON: skippedJSON}),
		decodeVector("error frame with a zero request ID and no payload fields", "A refusal is read as the answer to whatever was in flight.",
			transport.NewFrameWithID(transport.OpError, zero, codeZero),
			frameExpect{Result: "error", Code: intPointer(0), Name: "INTERNAL", RequestIDZero: boolPointer(true), ConnectionFatal: boolPointer(true)}),
	}, nil
}

// liveDecodeVectors are connection-level answers captured from a running
// server: they answer no request, so they are decode vectors.
func (g *generation) liveDecodeVectors(ctx context.Context) ([]vector, error) {
	var vectors []vector

	// ALPN: a client that offers none is refused with a zero request ID.
	single, release, err := g.target(ctx, exchangeCase{server: "single"})
	if err != nil {
		return nil, err
	}
	defer release()
	refusal, err := g.captureConnectionRefusal(ctx, single, func(cfg *tls.Config) { cfg.NextProtos = nil }, nil)
	if err != nil {
		return nil, fmt.Errorf("capturing the ALPN refusal: %w", err)
	}
	vectors = append(vectors, liveDecode("connection refused: no ALPN offered",
		"Captured live: a client that offers no ALPN completes TLS and receives this, then the connection closes.",
		refusal, transport.ErrUnsupportedVersion))

	// OVERLOADED: the per-address cap is full.
	overloaded, releaseOverloaded, err := g.target(ctx, exchangeCase{server: "overload", fresh: true})
	if err != nil {
		return nil, err
	}
	defer releaseOverloaded()
	holder, err := holdTheOnlyConnection(ctx, overloaded)
	if err != nil {
		return nil, err
	}
	defer holder.close()
	refusal, err = g.captureConnectionRefusal(ctx, overloaded, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("capturing the OVERLOADED refusal: %w", err)
	}
	overload := liveDecode("connection refused: overloaded",
		"Captured live with HOARDDB_TCP_MAX_CONNS_PER_IP=1 and one connection held: the refusal carries a zero request "+
			"ID and the connection closes. Not fatal to a pool: back off that one dial.",
		refusal, transport.ErrOverloaded)
	overload.ServerEnv = profiles["overload"].env
	vectors = append(vectors, overload)

	// A request over 32 MiB, before AUTH: answered by its request ID, then closed.
	oversizeID := fixedRequestID(9001)
	header := make([]byte, transport.FrameHeaderSize)
	header[0] = transport.ProtocolVersion
	binary.LittleEndian.PutUint32(header[1:5], 3*transport.MaxFrameLength)
	header[5] = byte(transport.OpBatchPut)
	copy(header[6:], oversizeID[:])
	answer, err := g.captureConnectionRefusal(ctx, single, nil, header)
	if err != nil {
		return nil, fmt.Errorf("capturing the oversize answer: %w", err)
	}
	oversize := liveDecode("an oversize request is answered by its request ID",
		"Captured live: request_header_hex is a BATCH_PUT header whose length word is 96 MiB, sent before AUTH with no "+
			"body. The server answers INVALID_ARGUMENT echoing the request ID, naming the size, the 32 MiB limit and "+
			"the remedy, then closes (before AUTH, or over 64 MiB). A driver refuses such a frame itself, before writing it.",
		answer, transport.ErrInvalidArgument)
	oversize.RequestHeaderHex = hex.EncodeToString(header)
	oversize.FrameExpect.RequestIDZero = boolPointer(false)
	oversize.FrameExpect.MessageContains = []string{"over the 32 MiB frame limit"}
	vectors = append(vectors, oversize)
	return vectors, nil
}

// resultTooLargeDatabase holds the document whose size alone is over the
// results profile's ceiling. It is a database of its own so no exchange's
// bucket counts towards the find.
const resultTooLargeDatabase = "v900"

// captureFirstElementRefusal runs a find whose first result alone is over the
// result ceiling and returns the ERROR frame it answered with. The document is
// 1.5 MB, over the 1 MiB of the results profile on its own, so the query
// retained nothing and the answer carries max_result_bytes and no
// retained_bytes. The connection is checked to survive the refusal.
func (g *generation) captureFirstElementRefusal(ctx context.Context) (*transport.Frame, error) {
	const bucket = "big"
	prepared := append(bucketSetup(resultTooLargeDatabase, bucket, server.BucketTypeHash),
		put(resultTooLargeDatabase, bucket, "one", map[string]any{"Text": strings.Repeat("a", 1_500_000)}))
	if err := g.requestsOn(ctx, "results", prepared); err != nil {
		return nil, fmt.Errorf("writing the document over the ceiling: %w", err)
	}
	target, release, err := g.target(ctx, exchangeCase{server: "results"})
	if err != nil {
		return nil, err
	}
	defer release()
	conn, err := dial(ctx, target)
	if err != nil {
		return nil, err
	}
	defer conn.close()
	if err := conn.authenticate(); err != nil {
		return nil, fmt.Errorf("authenticating: %w", err)
	}

	request := call{op: transport.OpScan, payload: server.ScanRequest{Database: resultTooLargeDatabase, Bucket: bucket}}
	payload, err := request.encode()
	if err != nil {
		return nil, err
	}
	requestID := fixedRequestID(9002)
	answer, err := conn.roundTrip(request.op, requestID, payload)
	if err != nil {
		return nil, fmt.Errorf("sending the find: %w", err)
	}
	if answer.Opcode != transport.OpError {
		return nil, fmt.Errorf("the find answered %s, not RESULT_TOO_LARGE: %s", answer.Opcode, describePayload(answer.Payload))
	}
	var refused server.ErrorResponse
	if err := bson.Unmarshal(answer.Payload, &refused); err != nil {
		return nil, fmt.Errorf("reading the refusal: %w", err)
	}
	if transport.ErrorCode(refused.Code) != transport.ErrResultTooLarge {
		return nil, fmt.Errorf("the find was refused with code %d (%s): %s", refused.Code, transport.ErrorCode(refused.Code), refused.Message)
	}
	if refused.MaxResultBytes != 1<<20 {
		return nil, fmt.Errorf("max_result_bytes is %d, not the results profile's 1 MiB: %s", refused.MaxResultBytes, refused.Message)
	}
	if fieldPresent(answer.Payload, "retained_bytes") {
		return nil, fmt.Errorf("the answer carries retained_bytes %d, but a first document over the ceiling retains nothing", refused.RetainedBytes)
	}
	if answer.RequestID != requestID {
		return nil, fmt.Errorf("the answer carries request ID %x, not the find's %x", answer.RequestID, requestID)
	}
	pong, err := conn.roundTrip(transport.OpPing, fixedRequestID(9003), nil)
	if err != nil {
		return nil, fmt.Errorf("the connection did not survive the refusal: %w", err)
	}
	if pong.Opcode != transport.OpPong {
		return nil, fmt.Errorf("the connection did not survive the refusal: PING answered %s", pong.Opcode)
	}

	norm, err := normaliseAnswer(answer.Payload, normaliseRules{replacer: g.replacer()})
	if err != nil {
		return nil, err
	}
	answer.Payload = norm.payload
	return answer, nil
}

// liveDecodeAnswer makes a decode vector of a captured answer to a request.
// The connection stays open after it, unlike a connection-level refusal's.
func liveDecodeAnswer(name, notes string, frame *transport.Frame, code transport.ErrorCode) vector {
	answer := liveDecode(name, notes, frame, code)
	answer.FrameExpect.ConnectionFatal = boolPointer(false)
	return answer
}

// holdTheOnlyConnection dials target and logs in on the one connection its
// per-address cap of 1 allows. The readiness PING's connection is closed by
// this client before the server has given its place back, and the server
// counts a connection from the moment it accepts it, so the first dial can be
// refused by that cap; it is retried for up to 5 s.
func holdTheOnlyConnection(ctx context.Context, target endpoint) (*wireConn, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := dial(ctx, target)
		if err != nil {
			return nil, err
		}
		err = conn.authenticate()
		if err == nil {
			return conn, nil
		}
		conn.close()
		if !strings.Contains(err.Error(), "HOARDDB_TCP_MAX_CONNS_PER_IP") || time.Now().After(deadline) {
			return nil, fmt.Errorf("holding the one allowed connection: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// liveDecode makes a decode vector of a captured refusal frame.
func liveDecode(name, notes string, frame *transport.Frame, code transport.ErrorCode) vector {
	return vector{
		Name: name, Group: "decode", Kind: "decode", Status: "frozen", Notes: notes,
		FrameHex: hex.EncodeToString(frame.Encode()),
		FrameExpect: &frameExpect{
			Result: "error", Code: intPointer(int(code)), Name: code.String(),
			RequestIDZero: boolPointer(frame.RequestID == [transport.RequestIDLen]byte{}), ConnectionFatal: boolPointer(true),
		},
	}
}

// captureConnectionRefusal dials (with an optional change to the client's TLS
// config), optionally writes raw bytes, and reads the one frame the server
// sends before closing. The frame's payload is normalised like an answer.
func (g *generation) captureConnectionRefusal(ctx context.Context, target endpoint, adjust func(*tls.Config), write []byte) (*transport.Frame, error) {
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Expected: []string{target.fingerprint}}, target.driverAddr)
	if err != nil {
		return nil, err
	}
	if adjust != nil {
		adjust(tlsCfg)
	}
	dialer := &tls.Dialer{Config: tlsCfg}
	raw, err := dialer.DialContext(ctx, "tcp", target.driverAddr)
	if err != nil {
		return nil, fmt.Errorf("dialling %s: %w", target.name, err)
	}
	conn := raw.(*tls.Conn)
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return nil, err
	}
	if write != nil {
		if _, err := conn.Write(write); err != nil {
			return nil, fmt.Errorf("writing: %w", err)
		}
	}
	frame, err := transport.DecodeFrame(bufio.NewReader(conn))
	if err != nil {
		return nil, fmt.Errorf("reading the refusal: %w", err)
	}
	if frame.Opcode != transport.OpError {
		return nil, fmt.Errorf("expected an ERROR frame, got %s: %s", frame.Opcode, describePayload(frame.Payload))
	}
	norm, err := normaliseAnswer(frame.Payload, normaliseRules{replacer: g.replacer(), sortTop: mapEncodedError(frame.Payload)})
	if err != nil {
		return nil, err
	}
	frame.Payload = norm.payload
	return frame, nil
}

// trustVectors are the fingerprint and known_servers vectors: fixed keys and
// certificates, fingerprinted by the server's own function, and
// known_servers texts loaded by the server's own loader.
func trustVectors(workDir string) ([]vector, error) {
	var vectors []vector
	keys := make([]nodeKey, 0, 2)
	for _, seed := range []byte{0x21, 0x22} {
		key, err := newNodeKey(seed)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
		block, _ := pem.Decode(key.certPEM)
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing the trust certificate: %w", err)
		}
		fromCertificate := transport.Fingerprint(certificate.PublicKey.(ed25519.PublicKey))
		if fromCertificate != key.fingerprint() {
			return nil, fmt.Errorf("the certificate's fingerprint %s differs from the key's %s", fromCertificate, key.fingerprint())
		}
		vectors = append(vectors, vector{
			Name: fmt.Sprintf("fingerprint of key %d", len(keys)), Group: "trust", Kind: "trust", Status: "frozen",
			Notes: "SHA256: and the lowercase hex of SHA-256 over the raw 32-byte ed25519 public key (the last 32 bytes " +
				"of the certificate's SubjectPublicKeyInfo), from the key and from the certificate alike.",
			PublicKeyHex: hex.EncodeToString(key.public), CertificatePEM: string(key.certPEM), Fingerprint: key.fingerprint(),
		})
	}
	first, second := hex.EncodeToString(keys[0].public), hex.EncodeToString(keys[1].public)
	files := []struct{ name, text string }{
		{"known_servers: one server", "127.0.0.1:7433 " + first + "\n"},
		{"known_servers: comments and blank lines", "# trusted with the hoardDB command-line tool\n\n127.0.0.1:7433 " + first + "\n\n# another\ndb.internal:7433 " + second + "\n"},
		{"known_servers: malformed lines are skipped", "127.0.0.1:7433\n127.0.0.1:7434 not-hex\n127.0.0.1:7435 " + first[:62] + "\n127.0.0.1:7436 " + second + "\n"},
		{"known_servers: an IPv6 host", "[::1]:7433 " + first + "\n"},
		{"known_servers: the same key twice for one host", "127.0.0.1:7433 " + first + "\n127.0.0.1:7433 " + first + "\n"},
		{"known_servers: two keys for one host", "127.0.0.1:7433 " + first + "\n127.0.0.1:7433 " + second + "\n"},
		{"known_servers: text after the key", "127.0.0.1:7433 " + first + " added 2026-10-05\n"},
	}
	for index, file := range files {
		path := filepath.Join(workDir, fmt.Sprintf("known_servers_%d", index))
		if err := os.WriteFile(path, []byte(file.text), 0o600); err != nil {
			return nil, fmt.Errorf("writing %s: %w", path, err)
		}
		knownCase := &knownServersCase{FileText: file.text}
		loaded, err := transport.NewKnownServers(path)
		if err != nil {
			knownCase.ErrorContains = "records two different keys"
			if !strings.Contains(err.Error(), knownCase.ErrorContains) {
				return nil, fmt.Errorf("%s: the loader refused it with %q", file.name, err)
			}
		} else {
			knownCase.Entries = knownEntries(loaded, file.text, keys)
		}
		vectors = append(vectors, vector{
			Name: file.name, Group: "trust", Kind: "trust", Status: "frozen",
			Notes:        "Loaded by the server's own known_servers reader; entries map host:port to the raw key's hex.",
			KnownServers: knownCase,
		})
	}
	return vectors, nil
}

// knownEntries asks the loaded store which candidate host holds which
// candidate key: the store does not list itself.
func knownEntries(loaded *transport.KnownServers, text string, keys []nodeKey) map[string]string {
	entries := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		host := fields[0]
		for _, key := range keys {
			if loaded.IsKnown(host, key.public) {
				entries[host] = hex.EncodeToString(key.public)
			}
		}
	}
	return entries
}
