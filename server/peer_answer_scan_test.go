package server

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// An answer another member sends this node is held to the depth scan a client
// request gets, before anything decodes it, with the same bound on documents:
// one past the deepest a legitimate answer reaches is refused, and an answer
// carrying a document at the bound is read.

// pastThePeerAnswerBound is one level deeper than any answer a member sends:
// a document at the bound, carried in an answer's list of entries.
const pastThePeerAnswerBound = maxDocumentDepth + 4

// substituteAnswerConn is a member's connection that answers the requests a
// test picks with bytes the test chose, in place of the member's own answer.
// Every other request reaches the member.
type substituteAnswerConn struct {
	peerConn
	mu      sync.Mutex
	matches func(op transport.Opcode, payload []byte) bool
	answer  *transport.Frame
}

// answerWith makes the connection answer every request matches picks with
// answer, until the subtest ends.
func (s *substituteAnswerConn) answerWith(t *testing.T, matches func(op transport.Opcode, payload []byte) bool, answer *transport.Frame) {
	t.Helper()
	s.mu.Lock()
	s.matches, s.answer = matches, answer
	s.mu.Unlock()
	t.Cleanup(func() {
		s.mu.Lock()
		s.matches, s.answer = nil, nil
		s.mu.Unlock()
	})
}

func (s *substituteAnswerConn) substitute(op transport.Opcode, payload []byte) (*transport.Frame, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.matches == nil || !s.matches(op, payload) {
		return nil, false
	}
	return &transport.Frame{Opcode: s.answer.Opcode, Payload: bytes.Clone(s.answer.Payload)}, true
}

func (s *substituteAnswerConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if answer, ok := s.substitute(op, payload); ok {
		return answer, nil
	}
	return s.peerConn.SendRequest(ctx, lane, op, payload)
}

func (s *substituteAnswerConn) SendRequestIfUnchanged(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte, generation uint64) (*transport.Frame, error) {
	if answer, ok := s.substitute(op, payload); ok {
		return answer, nil
	}
	return s.peerConn.SendRequestIfUnchanged(ctx, lane, op, payload, generation)
}

// installSubstituteConn puts a substituteAnswerConn in front of node from's
// connection to node to.
func installSubstituteConn(t *testing.T, tc *testCluster, from, to int) *substituteAnswerConn {
	t.Helper()
	p := peerOf(t, tc, from, to)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	member, err := p.dial(ctx)
	if err != nil {
		t.Fatalf("dialing node %d from node %d: %v", to+1, from+1, err)
	}
	substitute := &substituteAnswerConn{peerConn: member}
	p.mu.Lock()
	p.client = substitute
	p.mu.Unlock()
	return substitute
}

// opcodeIs picks every request of op.
func opcodeIs(op transport.Opcode) func(transport.Opcode, []byte) bool {
	return func(sent transport.Opcode, _ []byte) bool { return sent == op }
}

// pushCarrying picks the replication pushes for which carries holds.
func pushCarrying(carries func(ReplPushRequest) bool) func(transport.Opcode, []byte) bool {
	return func(op transport.Opcode, payload []byte) bool {
		if op != transport.OpReplPush {
			return false
		}
		var req ReplPushRequest
		return bson.Unmarshal(payload, &req) == nil && carries(req)
	}
}

// probeBucket names the entry of the pushes this file sends itself, so the
// substitute answers them and no push of the cluster's own.
const probeBucket = "peer-answer-probe"

func isProbe(req ReplPushRequest) bool {
	return len(req.Entries) == 1 && req.Entries[0].Bucket == probeBucket
}

// atTheBoundInAList is an answer carrying a document at the bound as deep as
// any answer carries one: in a list of entries, each with its document.
func atTheBoundInAList() []byte {
	entry := bsoncore.BuildDocument(nil, wireString("key", "k"), wireDocument("value", nestedDocumentBytes(maxDocumentDepth, false)))
	return wireRequest(wireDocumentList("entries", entry))
}

// wantRefusedPastTheBound fails unless err is the depth scan's refusal.
func wantRefusedPastTheBound(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, ErrDocumentTooDeep) {
		t.Fatalf("%s: an answer nested past the bound was read; want the depth scan's refusal, got %v", what, err)
	}
}

// TestPeerAnswersPastTheBoundAreRefusedBeforeTheyAreRead covers every family
// of answer a node reads back from a member it asked: a replication push's
// answer and a forwarded request's answer inside it, gossip, a catalog
// question, a hand-off's copies question, a versioned get, a pull's start,
// chunk and done, an asked migrate and its refusal, an auth config ack and a
// membership change.
func TestPeerAnswersPastTheBoundAreRefusedBeforeTheyAreRead(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, testBucket, 0)
	owner, err := clusterOf(tc, 0).Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolving the owner of the test bucket: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	putDocument(tc, ownerIndex, testDatabase, testBucket, "k")
	askerIndex := 1 - ownerIndex
	asker := clusterOf(tc, askerIndex)
	member := mustNode(t, asker, tc.addrs[ownerIndex])
	substitute := installSubstituteConn(t, tc, askerIndex, ownerIndex)
	deep := &transport.Frame{Opcode: transport.OpReplAck, Payload: nestedDocumentBytes(pastThePeerAnswerBound, false)}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	probe := func() *ReplPushRequest {
		return &ReplPushRequest{Token: asker.localToken(), FromNode: asker.self.ID, Epoch: asker.Epoch(), ReturnResponse: true,
			Entries: []ReplEntry{{Op: int32(transport.OpGet), Bucket: probeBucket, Payload: mustMarshal(t, FindRequest{Database: testDatabase, Bucket: probeBucket, Key: "k"})}}}
	}

	t.Run("replication push answer", func(t *testing.T) {
		substitute.answerWith(t, pushCarrying(isProbe), deep)
		_, err := asker.peerFor(member).call(ctx, transport.LaneRequest, probe())
		wantRefusedPastTheBound(t, "peer.call", err)
	})
	t.Run("forwarded request answer", func(t *testing.T) {
		answer := mustMarshal(t, ReplPushResponse{Success: true, Responses: []ReplResponse{
			{Opcode: int32(transport.OpGetResponse), Payload: nestedDocumentBytes(pastThePeerAnswerBound, false)},
		}})
		substitute.answerWith(t, pushCarrying(isProbe), &transport.Frame{Opcode: transport.OpReplAck, Payload: answer})
		_, err := asker.peerFor(member).call(ctx, transport.LaneRequest, probe())
		wantRefusedPastTheBound(t, "peer.call, the forwarded answer", err)
	})
	t.Run("gossip answer", func(t *testing.T) {
		substitute.answerWith(t, opcodeIs(transport.OpGossip), deep)
		_, err := asker.peerFor(member).exchangeGossip(ctx, substitute, 0)
		wantRefusedPastTheBound(t, "exchangeGossip", err)
	})
	t.Run("catalog question answer", func(t *testing.T) {
		substitute.answerWith(t, pushCarrying(func(req ReplPushRequest) bool { return req.Catalog != nil }), deep)
		_, err := asker.askCatalog(ctx, member, ReplCatalogAsk{Database: testDatabase, Bucket: testBucket})
		wantRefusedPastTheBound(t, "askCatalog", err)
	})
	t.Run("hand-off copies answer", func(t *testing.T) {
		substitute.answerWith(t, pushCarrying(func(req ReplPushRequest) bool { return req.Handoff != nil }), deep)
		_, err := asker.askCopiesOnce(ctx, member, []ReplBucketRef{{Database: testDatabase, Bucket: testBucket}})
		wantRefusedPastTheBound(t, "askCopiesOnce", err)
	})
	t.Run("versioned get answer", func(t *testing.T) {
		substitute.answerWith(t, opcodeIs(transport.OpVersionedGet), deep)
		_, err := asker.readVersionedAnswer(ctx, member, ConsistentGetRequest{Database: testDatabase, Bucket: testBucket, Key: "k"})
		wantRefusedPastTheBound(t, "readVersionedAnswer", err)
	})
	t.Run("versioned get document past the document bound", func(t *testing.T) {
		// The envelope is within the answer bound; the document in it is one
		// level deeper than a client may store.
		answer := mustMarshal(t, VersionedGetResponse{Found: true, Version: 1, Data: bson.Raw(nestedDocumentBytes(maxDocumentDepth+1, false))})
		substitute.answerWith(t, opcodeIs(transport.OpVersionedGet), &transport.Frame{Opcode: transport.OpVersionedGet, Payload: answer})
		_, err := asker.readVersionedAnswer(ctx, member, ConsistentGetRequest{Database: testDatabase, Bucket: testBucket, Key: "k"})
		wantRefusedPastTheBound(t, "readVersionedAnswer, the document", err)
	})
	t.Run("asked migrate answer", func(t *testing.T) {
		substitute.answerWith(t, opcodeIs(transport.OpMigrate), &transport.Frame{Opcode: transport.OpMigrate, Payload: deep.Payload})
		wantRefusedPastTheBound(t, "askNodeToMigrate", asker.askNodeToMigrate(ctx, member, testDatabase, testBucket))
	})
	t.Run("asked migrate refusal", func(t *testing.T) {
		substitute.answerWith(t, opcodeIs(transport.OpMigrate), &transport.Frame{Opcode: transport.OpError, Payload: deep.Payload})
		wantRefusedPastTheBound(t, "askNodeToMigrate, the refusal", asker.askNodeToMigrate(ctx, member, testDatabase, testBucket))
	})
	t.Run("auth config ack", func(t *testing.T) {
		substitute.answerWith(t, opcodeIs(transport.OpAuthConfigPush), deep)
		_, err := asker.sendAuthConfigPush(ctx, substitute, AuthConfigPushRequest{})
		wantRefusedPastTheBound(t, "sendAuthConfigPush", err)
	})
	t.Run("membership change answer", func(t *testing.T) {
		substitute.answerWith(t, opcodeIs(transport.OpMembershipChange), &transport.Frame{Opcode: transport.OpMembershipChange, Payload: deep.Payload})
		if _, answered := asker.askMember(ctx, member, MembershipChangeRequest{Phase: "prepare", FromNode: asker.self.ID}, 5*time.Second); answered {
			t.Fatal("askMember read a membership change answer nested past the bound as an answer")
		}
	})
	for _, op := range []transport.Opcode{transport.OpMigrateStart, transport.OpMigrateData, transport.OpMigrateDone} {
		t.Run("pull "+op.String()+" answer", func(t *testing.T) {
			substitute.answerWith(t, opcodeIs(op), &transport.Frame{Opcode: op, Payload: deep.Payload})
			wantRefusedPastTheBound(t, "pullBucket", asker.pullBucket(ctx, testDatabase, testBucket, member))
		})
	}
}

// TestPeerAnswersAtTheBoundAreRead is the other side: the scan refuses
// nothing a member legitimately sends. A forwarded answer carrying a document
// at the bound in a list of entries passes, and so does a versioned get of a
// document at the bound, read back unchanged.
func TestPeerAnswersAtTheBoundAreRead(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	asker := clusterOf(tc, 0)
	member := mustNode(t, asker, tc.addrs[1])
	substitute := installSubstituteConn(t, tc, 0, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("forwarded answer", func(t *testing.T) {
		carried := atTheBoundInAList()
		answer := mustMarshal(t, ReplPushResponse{Success: true, Responses: []ReplResponse{{Opcode: int32(transport.OpBTreeResponse), Payload: carried}}})
		substitute.answerWith(t, pushCarrying(isProbe), &transport.Frame{Opcode: transport.OpReplAck, Payload: answer})
		resp, err := asker.peerFor(member).call(ctx, transport.LaneRequest, &ReplPushRequest{Token: asker.localToken(), FromNode: asker.self.ID,
			Epoch: asker.Epoch(), ReturnResponse: true, Entries: []ReplEntry{{Op: int32(transport.OpBTreeRange), Bucket: probeBucket}}})
		if err != nil {
			t.Fatalf("a forwarded answer carrying a document at the bound was refused: %v", err)
		}
		if len(resp.Responses) != 1 || !bytes.Equal(resp.Responses[0].Payload, carried) {
			t.Fatalf("the forwarded answer did not come back unchanged: %+v", resp.Responses)
		}
	})
	t.Run("versioned get", func(t *testing.T) {
		document := nestedDocumentBytes(maxDocumentDepth, false)
		answer := mustMarshal(t, VersionedGetResponse{Found: true, Version: 7, Data: bson.Raw(document)})
		substitute.answerWith(t, opcodeIs(transport.OpVersionedGet), &transport.Frame{Opcode: transport.OpVersionedGet, Payload: answer})
		read, err := asker.readVersionedAnswer(ctx, member, ConsistentGetRequest{Database: testDatabase, Bucket: testBucket, Key: "k"})
		if err != nil {
			t.Fatalf("a versioned get of a document at the bound was refused: %v", err)
		}
		if !read.found || read.version != 7 || !bytes.Equal(read.data, document) {
			t.Fatalf("the versioned get did not read back the document unchanged: found %v, version %d", read.found, read.version)
		}
	})
}

// migrationRecord is one record of a migration stream.
func migrationRecord(t *testing.T, record migrateStreamRecord) []byte {
	t.Helper()
	return mustMarshal(t, record)
}

// TestMigrationStreamRecordsPastTheBoundAreRefused: a pull reads the stream a
// source sends record by record, and decodes the bucket's state document and
// every blob's metadata from it. Each is scanned before it is read: a record,
// a state document or a blob's metadata past the bound refuses the stream,
// and a blob's metadata carrying a document at the bound a blob write allows
// is read.
func TestMigrationStreamRecordsPastTheBoundAreRefused(t *testing.T) {
	blobMetaKey := append([]byte{storage.NSBlobMeta}, "0123456789abcdef"...)
	blobMetaAtTheBound := wireRequest(wireString("filename", "f"), wireDocument("extra", nestedDocumentBytes(maxDocumentDepth, false)))
	blobMetaPastTheBound := wireRequest(wireString("filename", "f"), wireDocument("extra", nestedDocumentBytes(maxDocumentDepth+1, false)))
	ordinary := migrationRecord(t, migrateStreamRecord{Key: []byte("k"), Value: []byte("v")})

	refused := []struct {
		name   string
		stream []byte
	}{
		{"a record", append(bytes.Clone(ordinary), nestedDocumentBytes(pastThePeerAnswerBound, false)...)},
		{"the state document", migrationRecord(t, migrateStreamRecord{Key: []byte{storage.NSState}, Value: nestedDocumentBytes(pastThePeerAnswerBound, false)})},
		{"a blob's metadata", migrationRecord(t, migrateStreamRecord{Key: blobMetaKey, Value: blobMetaPastTheBound})},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readMigrateStream(tc.stream, migratedLayout{storeType: BucketTypeHash})
			wantRefusedPastTheBound(t, "readMigrateStream", err)
		})
	}
	t.Run("a blob's metadata at the bound", func(t *testing.T) {
		records, err := readMigrateStream(append(bytes.Clone(ordinary), migrationRecord(t, migrateStreamRecord{Key: blobMetaKey, Value: blobMetaAtTheBound})...), migratedLayout{storeType: BucketTypeBlob})
		if err != nil {
			t.Fatalf("a blob's metadata at the bound a blob write allows was refused: %v", err)
		}
		if len(records) != 2 || !bytes.Equal(records[1].Value, blobMetaAtTheBound) {
			t.Fatalf("the stream did not read back unchanged: %d records", len(records))
		}
	})
}

// TestAMemberErrorFramePastTheBoundIsNotRead: the text this node logs for a
// member's error frame is read only from bytes the scan accepted.
func TestAMemberErrorFramePastTheBoundIsNotRead(t *testing.T) {
	const words = "the member's own words"
	deep := nestedDocumentBytes(pastThePeerAnswerBound, false, wireString("message", words))
	if text := decodeErrorText(&transport.Frame{Opcode: transport.OpError, Payload: deep}); strings.Contains(text, words) {
		t.Fatalf("decodeErrorText read an error frame nested past the bound: %q", text)
	}
	ordinary := wireRequest(wireString("message", words))
	if text := decodeErrorText(&transport.Frame{Opcode: transport.OpError, Payload: ordinary}); text != words {
		t.Fatalf("decodeErrorText = %q for an ordinary error frame, want %q", text, words)
	}
}
