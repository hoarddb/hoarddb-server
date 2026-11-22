package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0539: the validating scan (refuseDeepRequest at the top of NewHandler,
// and restoreChunk's scan of each record) answers bytes it refuses before any
// of our own code reads them. requestDecodeHook observes the reads; these
// tests send, for each request class, bytes the scan refuses (a document
// nested one level past the bound, built from bytes) and bytes it accepts.

// decodeRead is one read requestDecodeHook observed.
type decodeRead struct {
	site string
	op   transport.Opcode
}

func (r decodeRead) String() string { return fmt.Sprintf("%s (opcode %s)", r.site, r.op) }

// scanAnswer is what one request was answered: the error frame's code and
// message, or refused false for any other answer.
type scanAnswer struct {
	refused bool
	code    int
	message string
	opcode  transport.Opcode
}

// readScanAnswer reads one answer frame.
func readScanAnswer(t *testing.T, frame *transport.Frame) scanAnswer {
	t.Helper()
	if frame.Opcode != transport.OpError {
		return scanAnswer{opcode: frame.Opcode}
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding the error answer: %v", err)
	}
	return scanAnswer{refused: true, code: refusal.Code, message: refusal.Message, opcode: frame.Opcode}
}

// isScanRefusal reports an answer that is the depth scan's refusal.
func (a scanAnswer) isScanRefusal() bool {
	return a.refused && a.code == int(transport.ErrInvalidArgument) && strings.Contains(a.message, "levels deep")
}

// orderCase is one request class: the request the scan refuses, the words
// that must not reach the refusal, and an ordinary request of the same class.
type orderCase struct {
	name string
	// subject opens the refusal: it is named from the opcode alone.
	subject string
	// distinctive are strings carried only by the refused bytes.
	distinctive []string
	// refused and accepted each send one request and return its answer;
	// watched is the bytes whose reads count.
	refused  func(t *testing.T) (answer *transport.Frame, watched []byte)
	accepted func(t *testing.T) (answer *transport.Frame, watched []byte)
	// acceptedOK is true when the ordinary request must succeed, not merely
	// pass the scan.
	acceptedOK bool
}

// pastTheBound is a document nested one level past the bound. Carried as a
// member of a request, it puts the request past requestDepthLimit for every
// opcode below, and a restore record past the record's own limit.
func pastTheBound() []byte { return nestedDocumentBytes(maxNestingDepthUnderTest+1, false) }

// rootSession is a session authenticated as the test server's root user,
// with the root role on every database, so the authorization gate reads the
// scope of every scoped frame and allows it.
func rootSession() context.Context {
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "admin", Roles: []string{"root"}, Databases: []string{"*"},
		Grants: []transport.ScopedGrant{{Role: "root", DB: "*"}}})
	return transport.WithSession(context.Background(), sess)
}

// sendOrderFrame sends one frame through handler and fails on a Go error, which
// would reset the connection.
func sendOrderFrame(t *testing.T, ctx context.Context, handler transport.RequestHandler, op transport.Opcode, payload []byte) *transport.Frame {
	t.Helper()
	answer, err := handler(ctx, transport.NewFrame(op, payload))
	if err != nil {
		t.Fatalf("%s: the handler returned a Go error: %v", op, err)
	}
	return answer
}

// requestOrderCases builds one standalone node for the client classes and one
// cluster-only node for the node-to-node class.
func requestOrderCases(t *testing.T) []orderCase {
	t.Helper()
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})
	root := rootSession()
	unauthenticated := transport.WithSession(context.Background(), &transport.Session{})

	frameCase := func(ctx context.Context, h transport.RequestHandler, op transport.Opcode, elements ...[]byte) func(t *testing.T) (*transport.Frame, []byte) {
		return func(t *testing.T) (*transport.Frame, []byte) {
			payload := wireRequest(elements...)
			return sendOrderFrame(t, ctx, h, op, payload), payload
		}
	}

	// A node whose cluster knows only its token, as replicatedPushSite builds
	// it: a node-to-node request reaches its handler, which reads it.
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generating a cluster token: %v", err)
	}
	node := &Server{}
	node.cluster = &Cluster{s: node, token: ClusterToken{Token: token}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	nodeHandler := NewHandler(node)
	ordinaryEntry := mustMarshalBSON(t, ReplEntry{Seq: 1, Op: int32(transport.OpPut), Bucket: "docs",
		Payload: wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", "k1"), wireDocument("data", wireRequest(wireString("n", "1"))))})

	return []orderCase{
		{
			name: "scoped data opcode PUT", subject: "The document", acceptedOK: true,
			distinctive: []string{"zqdb-refused-3317", "zqbucket-refused-8842", "zqkey-refused-1207"},
			refused: frameCase(root, handler, transport.OpPut, wireString("database", "zqdb-refused-3317"), wireString("bucket", "zqbucket-refused-8842"),
				wireString("key", "zqkey-refused-1207"), wireDocument("data", pastTheBound())),
			accepted: frameCase(root, handler, transport.OpPut, wireString("database", "app"), wireString("bucket", "docs"),
				wireString("key", "ordinary"), wireDocument("data", wireRequest(wireString("n", "1")))),
		},
		{
			name: "DDL opcode CREATE_BUCKET", subject: "The bucket declaration", acceptedOK: true,
			distinctive: []string{"zqdb-refused-3317", "zqbucket-declared-6610"},
			refused: frameCase(root, handler, transport.OpCreateBucket, wireString("database", "zqdb-refused-3317"), wireString("name", "zqbucket-declared-6610"),
				wireString("type", string(BucketTypeHash)), wireDocument("options", pastTheBound())),
			accepted: frameCase(root, handler, transport.OpCreateBucket, wireString("database", "app"), wireString("name", "declared"),
				wireString("type", string(BucketTypeHash))),
		},
		{
			name: "auth-class AUTH", subject: "The document",
			distinctive: []string{"zquser-refused-2290", "zqdb-refused-3317", "zqbucket-refused-8842"},
			refused: frameCase(unauthenticated, handler, transport.OpAuth, wireString("username", "zquser-refused-2290"), wireString("password", "x"),
				wireString("database", "zqdb-refused-3317"), wireString("bucket", "zqbucket-refused-8842"), wireDocument("a", pastTheBound())),
			accepted: frameCase(unauthenticated, handler, transport.OpAuth, wireString("username", "nobody"), wireString("password", "x")),
		},
		{
			name: "auth-class PONG", subject: "The document",
			distinctive: []string{"zqdb-refused-3317", "zqbucket-refused-8842"},
			refused: frameCase(unauthenticated, handler, transport.OpPong, wireString("database", "zqdb-refused-3317"), wireString("bucket", "zqbucket-refused-8842"),
				wireDocument("a", pastTheBound())),
			accepted: frameCase(unauthenticated, handler, transport.OpPong, wireString("ok", "1")),
		},
		{
			name: "node-to-node REPL_PUSH", subject: "The node-to-node request",
			distinctive: []string{"zqnode-refused-9153", "zqdb-refused-3317", "zqbucket-refused-8842"},
			refused: frameCase(context.Background(), nodeHandler, transport.OpReplPush, wireString("token", token), wireString("from_node", "zqnode-refused-9153"),
				wireString("database", "zqdb-refused-3317"), wireString("bucket", "zqbucket-refused-8842"), wireDocument("a", pastTheBound())),
			accepted: frameCase(context.Background(), nodeHandler, transport.OpReplPush, wireString("token", token), wireString("from_node", "peer"),
				bsoncore.AppendArrayElement(nil, "entries", wireRequest(wireDocument("0", ordinaryEntry)))),
		},
		restoreOrderCase(t, s, handler, root),
	}
}

// restoreOrderCase sends one heap record in a restore chunk. The chunk's
// envelope carries the record inside a binary value, which the request's scan
// does not descend into; restoreChunk scans the record itself.
func restoreOrderCase(t *testing.T, s *Server, handler transport.RequestHandler, root context.Context) orderCase {
	t.Helper()
	start := func(t *testing.T, bucket string) string {
		t.Helper()
		answer := sendOrderFrame(t, root, handler, transport.OpRestoreStart, mustMarshalBSON(t, RestoreStartRequest{
			Database: "app", Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHeap, Bytes: 4096}))
		var started RestoreStartResponse
		if answer.Opcode == transport.OpError || bson.Unmarshal(answer.Payload, &started) != nil || started.SessionID == "" {
			t.Fatalf("starting the restore of app/%s: answered %s %q", bucket, answer.Opcode, answer.Payload)
		}
		return started.SessionID
	}
	chunk := func(bucket string, record []byte) func(t *testing.T) (*transport.Frame, []byte) {
		return func(t *testing.T) (*transport.Frame, []byte) {
			session := start(t, bucket)
			payload := mustMarshalBSON(t, RestoreChunkRequest{Database: "app", Bucket: bucket, SessionID: session, Data: record})
			// Only the record's reads count: the envelope is bytes the
			// request's own scan accepted.
			return sendOrderFrame(t, root, handler, transport.OpRestoreChunk, payload), record
		}
	}
	return orderCase{
		name: "restore chunk record", subject: "The dumped record", acceptedOK: true,
		distinctive: []string{"zqrecord-refused-5521"},
		refused: chunk("restoredrefused", wireRequest(wireString("_id_", "zqrecord-refused-5521"),
			bsoncore.AppendInt64Element(nil, "priority", 1), wireDocument("value", pastTheBound()))),
		accepted: chunk("restoredaccepted", wireRequest(wireString("_id_", "ordinary"),
			bsoncore.AppendInt64Element(nil, "priority", 1), wireDocument("value", wireRequest(wireString("n", "1"))))),
	}
}

// TestRefusedRequestBytesAreNeverRead: for each request class, bytes the
// validating scan refuses are answered with the scan's own refusal, worded
// from the opcode alone, before any of our code reads them; bytes it accepts
// are read and answered as before, which shows the observer is live.
func TestRefusedRequestBytesAreNeverRead(t *testing.T) {
	for _, tc := range requestOrderCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			var (
				answer  *transport.Frame
				watched []byte
			)
			reads := readsDuring(t, func() []byte {
				var sent []byte
				answer, sent = tc.refused(t)
				watched = sent
				return sent
			})
			refusal := readScanAnswer(t, answer)
			if !refusal.isScanRefusal() {
				t.Errorf("answered %s (code %d) %q, want the scan's INVALID_ARGUMENT refusal", refusal.opcode, refusal.code, refusal.message)
			}
			if want := tc.subject + " nests more than 100 levels deep"; !strings.HasPrefix(refusal.message, want) {
				t.Errorf("the refusal %q does not open with %q", refusal.message, want)
			}
			if len(reads) > 0 {
				t.Errorf("the refused bytes (%d) were read before the scan answered: %v", len(watched), reads)
			}
			for _, word := range tc.distinctive {
				if strings.Contains(refusal.message, word) {
					t.Errorf("the refusal carries %q, taken from the refused bytes: %q", word, refusal.message)
				}
			}

			reads = readsDuring(t, func() []byte {
				var sent []byte
				answer, sent = tc.accepted(t)
				return sent
			})
			ordinary := readScanAnswer(t, answer)
			switch {
			case ordinary.isScanRefusal():
				t.Errorf("an ordinary request was refused by the scan: %q", ordinary.message)
			case tc.acceptedOK && ordinary.refused:
				t.Errorf("an ordinary request was refused: code %d %q", ordinary.code, ordinary.message)
			}
			t.Logf("the ordinary request was read at: %v", reads)
			if len(reads) == 0 {
				t.Errorf("the observer saw no read of an ordinary request's bytes: it is not live at this class's sites")
			}
		})
	}
}

// readsDuring runs send with requestDecodeHook installed and returns the
// reads of bytes lying within what send reports it sent: the request, or the
// record inside it. A read of anything else (an envelope the scan accepted, a
// restore's start, another goroutine's request) is not one of them.
func readsDuring(t *testing.T, send func() []byte) []decodeRead {
	t.Helper()
	type seen struct {
		read    decodeRead
		payload []byte
	}
	var (
		mu  sync.Mutex
		all []seen
	)
	hook := func(site string, op transport.Opcode, payload []byte) {
		mu.Lock()
		defer mu.Unlock()
		all = append(all, seen{read: decodeRead{site: site, op: op}, payload: append([]byte(nil), payload...)})
	}
	requestDecodeHook.Store(&hook)
	defer requestDecodeHook.Store(nil)
	sent := send()
	requestDecodeHook.Store(nil)
	mu.Lock()
	defer mu.Unlock()
	var reads []decodeRead
	for _, observed := range all {
		if len(observed.payload) > 0 && bytes.Contains(sent, observed.payload) {
			reads = append(reads, observed.read)
		}
	}
	return reads
}

// TestRefusedReplicatedEntryIsNeverRead: a replicated push whose envelope the
// request's scan accepts carries each entry's payload as a binary value, which
// that scan does not descend into; handleReplPush scans each entry before
// anything reads it. For a stream push and for a forwarded request, sent
// through NewHandler, an entry one level past the bound is refused by that
// per-entry scan and its bytes are never read.
//
// The node knows only its token, so it applies nothing: an entry past the
// per-entry scan is answered by the stream's incarnation check or the epoch
// check instead, which is why the refusal's wording is asserted.
func TestRefusedReplicatedEntryIsNeverRead(t *testing.T) {
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generating a cluster token: %v", err)
	}
	node := &Server{}
	node.cluster = &Cluster{s: node, token: ClusterToken{Token: token}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		streamFromMu: make(map[string]*sync.Mutex), gossipWake: make(chan struct{})}
	handler := NewHandler(node)

	entryPayload := func(database, bucket, key string, data []byte) []byte {
		return wireRequest(wireString("database", database), wireString("bucket", bucket), wireString("key", key), wireDocument("data", data))
	}
	distinctive := []string{"zqdb-refused-3317", "zqbucket-refused-8842", "zqkey-refused-1207"}
	refusedEntry := entryPayload("zqdb-refused-3317", "zqbucket-refused-8842", "zqkey-refused-1207", pastTheBound())
	ordinaryEntry := entryPayload("app", "docs", "ordinary", wireRequest(wireString("n", "1")))

	variants := []struct {
		name string
		push func(entry []byte) ReplPushRequest
	}{
		{name: "stream push", push: func(entry []byte) ReplPushRequest {
			return ReplPushRequest{Token: token, FromNode: "peer", Stream: &ReplStream{Incarnation: []byte("peer-incarnation")},
				Entries: []ReplEntry{{Seq: 1, Op: int32(transport.OpPut), Bucket: "docs", Payload: entry}}}
		}},
		{name: "forwarded request", push: func(entry []byte) ReplPushRequest {
			return ReplPushRequest{Token: token, FromNode: "peer", ReturnResponse: true, Epoch: 7,
				Entries: []ReplEntry{{Seq: 1, Op: int32(transport.OpPut), Bucket: "docs", Payload: entry}}}
		}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			send := func(entry []byte) ReplPushResponse {
				t.Helper()
				answer := sendOrderFrame(t, context.Background(), handler, transport.OpReplPush, mustMarshalBSON(t, variant.push(entry)))
				if answer.Opcode != transport.OpReplAck {
					t.Fatalf("answered %s %q, want a REPL_ACK", answer.Opcode, answer.Payload)
				}
				var ack ReplPushResponse
				if err := bson.Unmarshal(answer.Payload, &ack); err != nil {
					t.Fatalf("decoding the REPL_ACK: %v", err)
				}
				return ack
			}

			var ack ReplPushResponse
			reads := readsDuring(t, func() []byte {
				ack = send(refusedEntry)
				return refusedEntry
			})
			if ack.Success || !strings.Contains(ack.Error, "The document nests more than 100 levels deep") {
				t.Errorf("answered success=%v %q, want the per-entry scan's refusal", ack.Success, ack.Error)
			}
			if len(reads) > 0 {
				t.Errorf("the refused entry's bytes were read before the scan answered: %v", reads)
			}
			for _, word := range distinctive {
				if strings.Contains(ack.Error, word) {
					t.Errorf("the refusal carries %q, taken from the refused entry: %q", word, ack.Error)
				}
			}

			if ordinary := send(ordinaryEntry); strings.Contains(ordinary.Error, nestingRefusalPhrase) {
				t.Errorf("an ordinary entry was refused by the scan: %q", ordinary.Error)
			}
		})
	}
}

// TestReplayDoesNotReadALoggedEntryTheDepthScanRefuses: replayWALOnce meets
// each logged entry with the same depth scan the request door uses, and an
// entry the scan refuses is passed over before any of our code reads it. The
// entry is logged as over-bound bytes, the node restarts, and replay meets it.
func TestReplayDoesNotReadALoggedEntryTheDepthScanRefuses(t *testing.T) {
	s := newTestServer(t)
	w, err := storage.OpenWAL(filepath.Join(t.TempDir(), "wal"), nil)
	if err != nil {
		t.Fatalf("opening the WAL: %v", err)
	}
	defer func() { _ = w.Close() }()
	attachWALToStartedServer(s, w)
	logEntry := func(op transport.Opcode, bucket, key string, payload []byte) {
		t.Helper()
		if _, err := w.Append(bucket, byte(op), []byte(key), payload); err != nil {
			t.Fatalf("logging %s: %v", op, err)
		}
	}
	logEntry(transport.OpCreateDB, "", "", mustMarshalBSON(t, CreateDatabaseRequest{Name: "app"}))
	logEntry(transport.OpCreateBucket, "docs", "", mustMarshalBSON(t, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash}))
	replayed := wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", "over"),
		wireDocument("data", pastTheBound()))
	logEntry(transport.OpPut, "docs", "over", replayed)

	reads := readsDuring(t, func() []byte {
		if err := s.replayWAL(context.Background()); err != nil {
			t.Fatalf("replaying the WAL: %v", err)
		}
		return replayed
	})
	if len(reads) > 0 {
		t.Errorf("replay read the entry the depth scan refused: %v", reads)
	}
}
