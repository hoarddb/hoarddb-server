package wire

import (
	"bytes"
	"errors"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

func peerCodecID() []byte {
	enc, err := store.EncodeComponent("k")
	if err != nil {
		panic(err) // test fixture is a constant
	}
	return enc
}

func peerCodecRecord(seq uint64, body string) PairRecord {
	op, err := cluster.EncodeClientOp(cluster.Op{Class: cluster.OpDelete, BucketID: 7, EncID: peerCodecID(), DeclVersion: 1, SchemaVersionSeen: 1})
	if err != nil {
		panic(err) // test fixture is a constant
	}
	_ = body
	return PairRecord{
		Pos: cluster.Position{Epoch: 3, Seq: seq}, Cursor: seq * 10,
		Record: storage.WALRecord{Class: cluster.OpDelete, OpIndex: 2, RequestID: [16]byte{1, 2, 3}, Body: op},
	}
}

func TestPeerCodecRoundTrips(t *testing.T) {
	rec := peerCodecRecord(5, "x")
	got, err := DecodeWriteBody(EncodeWriteBody(rec))
	if err != nil || got.Pos != rec.Pos || got.Cursor != rec.Cursor || !bytes.Equal(got.Record.Body, rec.Record.Body) || got.Record.RequestID != rec.Record.RequestID {
		t.Fatalf("WRITE round trip: %+v, %v", got, err)
	}
	ack, err := DecodeWriteAckBody(EncodeWriteAckBody(WriteAck{Pos: rec.Pos, Outcome: AckRefused}))
	if err != nil || ack.Pos != rec.Pos || ack.Outcome != AckRefused {
		t.Fatalf("WRITE_ACK round trip: %+v, %v", ack, err)
	}
	req, err := DecodeCatchUpReqBody(EncodeCatchUpReqBody(CatchUpRequest{From: rec.Pos, FromCursor: 9}))
	if err != nil || req.From != rec.Pos || req.FromCursor != 9 {
		t.Fatalf("CATCHUP_REQ round trip: %+v, %v", req, err)
	}
	raw, err := EncodeCatchUpChunkBody(CatchUpChunk{SourceAppended: rec.Pos, CaughtUp: true, Divergent: true, LastPosition: rec.Pos, Records: []PairRecord{rec, peerCodecRecord(6, "y")}})
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := DecodeCatchUpChunkBody(raw)
	if err != nil || len(chunk.Records) != 2 || !chunk.CaughtUp || !chunk.Divergent || chunk.Records[1].Pos.Seq != 6 {
		t.Fatalf("CATCHUP_CHUNK round trip: %+v, %v", chunk, err)
	}
	write, err := EncodeForwardWriteBody(WriteRequest{
		Op:        cluster.Op{Class: cluster.OpDelete, BucketID: 7, EncID: peerCodecID(), DeclVersion: 1, SchemaVersionSeen: 1},
		RequestID: [16]byte{9}, Hop: 1, OpIndex: 3, BatchSize: 4, Concern: machine.ConcernMajority, ClientEpoch: 8, Deadline: 1500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	fw, err := DecodeForwardWriteBody(write)
	if err != nil || fw.Hop != 1 || fw.OpIndex != 3 || fw.BatchSize != 4 || fw.Concern != machine.ConcernMajority || fw.ClientEpoch != 8 || fw.Deadline != 1500*time.Millisecond || fw.RequestID[0] != 9 {
		t.Fatalf("FORWARD_WRITE round trip: %+v, %v", fw, err)
	}
	answer, err := DecodeForwardWriteReply(EncodeForwardWriteAnswer(WriteAnswer{Outcome: machine.OutcomeApplied, Pos: rec.Pos, CopiesConfirmed: 2, Result: []byte("r")}))
	if err != nil || answer.Pos != rec.Pos || answer.CopiesConfirmed != 2 || string(answer.Result) != "r" {
		t.Fatalf("FORWARD_WRITE_REPLY answer round trip: %+v, %v", answer, err)
	}
	_, err = DecodeForwardWriteReply(EncodeForwardRefusal(&cluster.Error{Code: cluster.CodeNotFound, Message: "no such thing. Run 'x'"}))
	var refusal *cluster.Error
	if !asClusterError(err, &refusal) || refusal.Code != cluster.CodeNotFound || !strings.Contains(refusal.Message, "Run 'x'") {
		t.Fatalf("a forwarded refusal came back as %v, want the owner's own code and words", err)
	}
	read, err := EncodeForwardReadBody(ForwardReadBody{Stored: true, Req: ReadRequest{BucketID: 4, EncID: []byte("e"), ID: "id", Hop: 1, RequestID: [16]byte{5}}})
	if err != nil {
		t.Fatal(err)
	}
	fr, err := DecodeForwardReadBody(read)
	if err != nil || !fr.Stored || fr.Req.BucketID != 4 || fr.Req.ID != "id" || fr.Req.Hop != 1 || string(fr.Req.EncID) != "e" {
		t.Fatalf("FORWARD_READ round trip: %+v, %v", fr, err)
	}
	doc, err := EncodeForwardReadDocument(Document{Slot: 77, EncID: []byte("e"), Body: []byte("bson"), ServedBy: "n2"})
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeForwardReadDocument(doc)
	if err != nil || back.Slot != 77 || back.ServedBy != "n2" || string(back.Body) != "bson" {
		t.Fatalf("FORWARD_READ_REPLY document round trip: %+v, %v", back, err)
	}
	stored, err := EncodeForwardReadStored(map[uint16][]byte{3: []byte("c"), 1: []byte("a")}, true)
	if err != nil {
		t.Fatal(err)
	}
	fields, found, err := DecodeForwardReadStored(stored)
	if err != nil || !found || string(fields[1]) != "a" || string(fields[3]) != "c" {
		t.Fatalf("stored read round trip: %v %v %v", fields, found, err)
	}
}

func asClusterError(err error, target **cluster.Error) bool {
	for err != nil {
		if e, ok := err.(*cluster.Error); ok {
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// TestAForwardCarryingAClosureIsRefused: a Rewrite closes over the forwarder's memory and
// cannot cross; sending it would run it on the wrong node or drop it silently.
func TestAForwardCarryingAClosureIsRefused(t *testing.T) {
	_, err := EncodeForwardWriteBody(WriteRequest{Op: cluster.Op{Class: cluster.OpDelete}, Rewrite: func() ([]cluster.FieldOp, error) { return nil, nil }})
	if err == nil || !strings.Contains(err.Error(), "cannot be forwarded") {
		t.Fatalf("forwarding a request with a Rewrite: %v, want a refusal naming that it cannot be forwarded", err)
	}
}

// TestAChunkCountBeyondTheBodyIsRefusedBeforeAllocating: the count is checked against the
// bytes that remain before any slice is sized from it.
func TestAChunkCountBeyondTheBodyIsRefusedBeforeAllocating(t *testing.T) {
	raw, err := EncodeCatchUpChunkBody(CatchUpChunk{})
	if err != nil {
		t.Fatal(err)
	}
	raw[16+1], raw[16+2] = 0xFF, 0xFF // recordCount = 65535 with no record behind it
	if _, err := DecodeCatchUpChunkBody(raw); err == nil {
		t.Fatal("a chunk that announces 65535 records and carries none was accepted")
	}
}

// TestPeerDecodersSurviveMutation: 100k random mutations of valid bodies never panic, and
// whatever a decoder accepts it re-encodes to the same bytes.
func TestPeerDecodersSurviveMutation(t *testing.T) {
	rec := peerCodecRecord(5, "x")
	chunk, _ := EncodeCatchUpChunkBody(CatchUpChunk{SourceAppended: rec.Pos, LastPosition: rec.Pos, Records: []PairRecord{rec}})
	write, _ := EncodeForwardWriteBody(WriteRequest{Op: cluster.Op{Class: cluster.OpDelete, BucketID: 7, EncID: peerCodecID(), DeclVersion: 1, SchemaVersionSeen: 1}, Hop: 1})
	read, _ := EncodeForwardReadBody(ForwardReadBody{Req: ReadRequest{BucketID: 4, EncID: []byte("e"), ID: "id"}})
	projected, _ := EncodeForwardReadBody(ForwardReadBody{Req: ReadRequest{BucketID: 4, EncID: []byte("e"), ID: "id", Fields: []uint16{2, 5, 9}}})
	doc, _ := EncodeForwardReadDocument(Document{Slot: 1, EncID: []byte("e"), Body: []byte("b"), ServedBy: "n"})
	stored, _ := EncodeForwardReadStored(map[uint16][]byte{1: []byte("a")}, true)
	seeds := []struct {
		name   string
		seed   []byte
		decode func([]byte) ([]byte, error)
	}{
		{"WRITE", EncodeWriteBody(rec), func(b []byte) ([]byte, error) { r, err := DecodeWriteBody(b); return EncodeWriteBody(r), err }},
		{"WRITE_ACK", EncodeWriteAckBody(WriteAck{Pos: rec.Pos, Outcome: AckApplied}), func(b []byte) ([]byte, error) {
			a, err := DecodeWriteAckBody(b)
			return EncodeWriteAckBody(a), err
		}},
		{"CATCHUP_REQ", EncodeCatchUpReqBody(CatchUpRequest{From: rec.Pos, FromCursor: 3}), func(b []byte) ([]byte, error) {
			r, err := DecodeCatchUpReqBody(b)
			return EncodeCatchUpReqBody(r), err
		}},
		{"CATCHUP_CHUNK", chunk, func(b []byte) ([]byte, error) {
			c, err := DecodeCatchUpChunkBody(b)
			if err != nil {
				return nil, err
			}
			return EncodeCatchUpChunkBody(c)
		}},
		{"FORWARD_WRITE", write, func(b []byte) ([]byte, error) {
			w, err := DecodeForwardWriteBody(b)
			if err != nil {
				return nil, err
			}
			return EncodeForwardWriteBody(w)
		}},
		{"FORWARD_READ", read, func(b []byte) ([]byte, error) {
			r, err := DecodeForwardReadBody(b)
			if err != nil {
				return nil, err
			}
			return EncodeForwardReadBody(r)
		}},
		{"FORWARD_READ projected", projected, func(b []byte) ([]byte, error) {
			r, err := DecodeForwardReadBody(b)
			if err != nil {
				return nil, err
			}
			return EncodeForwardReadBody(r)
		}},
		{"FORWARD_READ_REPLY document", doc, func(b []byte) ([]byte, error) {
			d, err := DecodeForwardReadDocument(b)
			if err != nil {
				return nil, err
			}
			return EncodeForwardReadDocument(d)
		}},
		{"FORWARD_READ_REPLY stored", stored, func(b []byte) ([]byte, error) {
			m, found, err := DecodeForwardReadStored(b)
			if err != nil {
				return nil, err
			}
			return EncodeForwardReadStored(m, found)
		}},
	}
	rng := rand.New(rand.NewSource(57))
	for _, tc := range seeds {
		for range 12500 {
			mutated := append([]byte(nil), tc.seed...)
			for range 1 + rng.Intn(3) {
				switch rng.Intn(3) {
				case 0:
					mutated[rng.Intn(len(mutated))] ^= byte(1 << rng.Intn(8))
				case 1:
					mutated = mutated[:rng.Intn(len(mutated)+1)]
				default:
					mutated = append(mutated, byte(rng.Intn(256)))
				}
				if len(mutated) == 0 {
					mutated = []byte{0}
				}
			}
			again, err := tc.decode(mutated)
			if err == nil && !bytes.Equal(again, mutated) {
				t.Fatalf("%s accepted %x and re-encoded it as %x", tc.name, mutated, again)
			}
		}
	}
}

// TestAForwardCarryingAClaimSinkIsRefused: a claim sink is the forwarder's memory too.
func TestAForwardCarryingAClaimSinkIsRefused(t *testing.T) {
	_, err := EncodeForwardWriteBody(WriteRequest{Op: cluster.Op{Class: cluster.OpClaim}, ClaimSink: func([][]byte) {}})
	if err == nil || !strings.Contains(err.Error(), "cannot be forwarded") {
		t.Fatalf("forwarding a claim with a sink: %v, want a refusal", err)
	}
}

// TestAForwardedEpochFenceNeverReachesAClient: EPOCH_FENCED is node-to-node only
// (membership 7); the forwarder shows its caller STALE_EPOCH with the fix.
func TestAForwardedEpochFenceNeverReachesAClient(t *testing.T) {
	fenced := &cluster.Error{Code: cluster.CodeEpochFenced, Message: "refused WRITE from n1 at epoch 4: this node is at epoch 5"}
	got := clientFacing(fenced, "n3")
	var coded *cluster.Error
	if !errors.As(got, &coded) || coded.Code != cluster.CodeStaleEpoch {
		t.Fatalf("a forwarded EPOCH_FENCED became %v, want STALE_EPOCH", got)
	}
	other := &cluster.Error{Code: cluster.CodeNotFound, Message: "x"}
	if clientFacing(other, "n3") != error(other) {
		t.Fatal("a refusal other than EPOCH_FENCED must pass through unchanged")
	}
}

// TestTheWriteAndAckDecodersSurvive100kMutations (BUG-0745): the WRITE body and the WRITE_ACK body
// are the frames the stream now relies on for its receipt/ack split, so each takes 100k random
// mutations: no panic, bounded allocation, and whatever is accepted re-encodes identically.
// There is no new frame kind to mutate: the receipt is an empty reply.
func TestTheWriteAndAckDecodersSurvive100kMutations(t *testing.T) {
	rec := peerCodecRecord(5, "x")
	cases := []struct {
		name   string
		seed   []byte
		decode func([]byte) ([]byte, error)
	}{
		{"WRITE", EncodeWriteBody(rec), func(b []byte) ([]byte, error) { r, err := DecodeWriteBody(b); return EncodeWriteBody(r), err }},
		{"WRITE_ACK", EncodeWriteAckBody(WriteAck{Pos: rec.Pos, Outcome: AckApplied}), func(b []byte) ([]byte, error) {
			a, err := DecodeWriteAckBody(b)
			return EncodeWriteAckBody(a), err
		}},
	}
	rng := rand.New(rand.NewSource(745))
	for _, tc := range cases {
		for range 100_000 {
			mutated := append([]byte(nil), tc.seed...)
			for range 1 + rng.Intn(3) {
				switch rng.Intn(3) {
				case 0:
					mutated[rng.Intn(len(mutated))] ^= byte(1 << rng.Intn(8))
				case 1:
					mutated = mutated[:rng.Intn(len(mutated)+1)]
				default:
					mutated = append(mutated, byte(rng.Intn(256)))
				}
				if len(mutated) == 0 {
					mutated = []byte{0}
				}
			}
			again, err := tc.decode(mutated)
			if err == nil && !bytes.Equal(again, mutated) {
				t.Fatalf("%s accepted %x and re-encoded it as %x", tc.name, mutated, again)
			}
		}
	}
}
