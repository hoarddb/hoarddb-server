package wire

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// p4Marker is in the key of every position the stand-in applier writes, so a test can find
// the batches that carry drained data.
const p4Marker = "drain-"

// p4WindowConfig is the dedupe window the drain tests record under.
var p4WindowConfig = machine.WindowConfig{Window: 8192, MaxResult: 1 << 16}

// p4Applier stands in for the document applier. Each position it fills writes one key under
// the pair and records the order it was asked in. Its result is "popped", the value a retried
// pop reads back from its own dedupe entry.
type p4Applier struct {
	mu    sync.Mutex
	order []uint64
}

func (*p4Applier) Handles(byte) bool { return true }

func (*p4Applier) Precondition(uint16, uint64, cluster.Op) (cluster.Code, error) { return 0, nil }

func (a *p4Applier) Fill(batch storage.KVBatch, rec AppliedRecord) error {
	_, err := a.FillWithResult(batch, rec)
	return err
}

func (a *p4Applier) FillWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	a.mu.Lock()
	a.order = append(a.order, rec.Pos.Seq)
	a.mu.Unlock()
	if err := batch.Set(p4DataKey(rec.Slot, rec.Pos.Seq), []byte("applied")); err != nil {
		return nil, err
	}
	return []byte("popped"), nil
}

// filled is the sequence numbers the applier was asked to fill, in the order it was asked.
func (a *p4Applier) filled() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]uint64(nil), a.order...)
}

func p4DataKey(slot uint16, seq uint64) []byte {
	return append(storage.PairPrefix(slot, reconDB), fmt.Sprintf("%s%06d", p4Marker, seq)...)
}

// p4Insert is an encoded insert of one document into bucket 9, the bucket the source's
// dedupe entries name.
func p4Insert(t *testing.T) []byte {
	t.Helper()
	enc := peerCodecID()
	body, err := cluster.EncodeOp(cluster.Op{Class: cluster.OpInsert, BucketID: 9, EncID: enc, IDValue: enc, DeclVersion: 1, SchemaVersionSeen: 1})
	if err != nil {
		t.Fatalf("EncodeOp: %v", err)
	}
	return body
}

// p4Frame is one WRITE above the cut of the rig's pair, carrying op under requestID.
func p4Frame(t *testing.T, rig *t3Rig, seq uint64, body []byte, requestID byte) WriteFrame {
	t.Helper()
	return WriteFrame{
		Slot: rig.slot, DB: reconDB, Epoch: uint64(rig.state.Epoch),
		PairRecord: PairRecord{
			Pos:    cluster.Position{Epoch: uint64(rig.state.Epoch), Seq: seq},
			Cursor: seq,
			Record: storage.WALRecord{Class: cluster.OpInsert, RequestID: [16]byte{requestID}, Body: body},
		},
	}
}

// p4Rig is a rig whose one source holds six keys at the cut, ready to pull, with the drain's
// applier and window wired. The buffer budget holds any test's frames.
func p4Rig(t *testing.T, applier Applier, kv storage.KV, ordinal uint64, seed SeedOptions) (*t3Rig, *seedBuffers) {
	t.Helper()
	buffers := newSeedBuffers(1 << 20)
	rig := newT3Rig(t, t3Options{Buffers: buffers, Applier: applier, Window: p4WindowConfig, kv: kv, seed: seed})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 6, t3Cut(t, rig), ordinal)
	return rig, buffers
}

// p4OfferAtTheFinalChunk offers frames to the pull's buffer just before the source's final
// chunk is applied, so the drain is the first thing that sees them. The offer happens once. The
// owner ships these writes, so it holds them in its log too, unless the test says otherwise.
func p4OfferAtTheFinalChunk(rig *t3Rig, buffers *seedBuffers, frames ...WriteFrame) {
	var once sync.Once
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if msg.Env.Kind == transport.KindSeedAck && reply != nil && reply.Env.Kind == transport.KindSeedChunk {
			if chunk, decodeErr := transport.DecodeSeedChunk(payloadOf(*reply)); decodeErr == nil && chunk.Final {
				once.Do(func() {
					for _, frame := range frames {
						buffers.Offer(rig.slot, reconDB, frame)
						if !rig.sourceLacksOffered {
							rig.sources[rig.owner].hold(frame.PairRecord)
						}
					}
				})
			}
		}
		return reply, err
	}
}

// p4BatchTally counts, for every commit that writes drained data, whether the same batch also
// writes the pair's seed record.
type p4BatchTally struct {
	storage.KV
	mu          sync.Mutex
	dataCommits int
	withoutRec  int
}

func (k *p4BatchTally) NewBatch() storage.KVBatch {
	return &p4BatchCommit{KVBatch: k.KV.NewBatch(), tally: k}
}

type p4BatchCommit struct {
	storage.KVBatch
	tally  *p4BatchTally
	data   bool
	record bool
}

func (b *p4BatchCommit) Set(key, value []byte) error {
	if bytes.Contains(key, []byte(p4Marker)) {
		b.data = true
	}
	if len(key) == len(SeedRecordKey(0, 0)) && key[0] == 0xFF {
		b.record = true
	}
	return b.KVBatch.Set(key, value)
}

func (b *p4BatchCommit) Commit(sync bool) error {
	err := b.KVBatch.Commit(sync)
	if err == nil && b.data {
		b.tally.mu.Lock()
		b.tally.dataCommits++
		if !b.record {
			b.tally.withoutRec++
		}
		b.tally.mu.Unlock()
	}
	return err
}

// TestTheDrainAppliesOnlyContiguousPositions: the buffered writes are applied in position
// order however they arrived, and a hole stops the drain where it is, with the record untouched
// beyond the last position before the hole. [BREAK: pop the buffer in arrival order]
func TestTheDrainAppliesOnlyContiguousPositions(t *testing.T) {
	t.Run("positions arrive out of order and are applied in order", func(t *testing.T) {
		applier := &p4Applier{}
		rig, buffers := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3})
		cut := t3Cut(t, rig)
		body := p4Insert(t)
		// Arrival order 3, 1, 2.
		p4OfferAtTheFinalChunk(rig, buffers,
			p4Frame(t, rig, cut.Seq+3, body, 3), p4Frame(t, rig, cut.Seq+1, body, 1), p4Frame(t, rig, cut.Seq+2, body, 2))
		rig.pullUntil("the pull to finish with the buffered writes applied", rig.isReady)
		if got, want := applier.filled(), []uint64{cut.Seq + 1, cut.Seq + 2, cut.Seq + 3}; !slices.Equal(got, want) {
			t.Fatalf("the drain applied positions %v, want %v in position order", got, want)
		}
	})

	t.Run("a hole stops the drain and nothing past it is applied", func(t *testing.T) {
		applier := &p4Applier{}
		// The source holds nothing above its cut, so the hole at cut+2 is one it cannot fill: the
		// attempt restarts on a fresh cut and the write above the hole is never applied by the
		// attempt that met it.
		rig, buffers := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3, RetryBackoff: time.Hour})
		rig.sourceLacksOffered = true
		cut := t3Cut(t, rig)
		body := p4Insert(t)
		p4OfferAtTheFinalChunk(rig, buffers, p4Frame(t, rig, cut.Seq+1, body, 1), p4Frame(t, rig, cut.Seq+3, body, 3))
		rig.pullUntil("the restarted pull to become ready", rig.isReady)
		if got, want := applier.filled(), []uint64{cut.Seq + 1}; !slices.Equal(got, want) {
			t.Fatalf("the drain applied positions %v, want %v: nothing past the hole may apply", got, want)
		}
		if registers := len(rig.requests(transport.KindSeedRegister)); registers != 2 {
			t.Fatalf("the pull registered %d times, want 2: the hole restarts the attempt on a fresh cut", registers)
		}
	})
}

// TestADrainedPositionAtOrBelowTheCutIsDropped: a live write the snapshot already holds (at the
// cut) is not applied twice. [BREAK: apply the frame at the cut]
func TestADrainedPositionAtOrBelowTheCutIsDropped(t *testing.T) {
	applier := &p4Applier{}
	rig, buffers := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3})
	cut := t3Cut(t, rig)
	body := p4Insert(t)
	p4OfferAtTheFinalChunk(rig, buffers, p4Frame(t, rig, cut.Seq, body, 1), p4Frame(t, rig, cut.Seq+1, body, 2))
	rig.pullUntil("the pull to finish", rig.isReady)
	if got, want := applier.filled(), []uint64{cut.Seq + 1}; !slices.Equal(got, want) {
		t.Fatalf("the drain applied positions %v, want %v: the write at the cut is the snapshot's", got, want)
	}
}

// TestADrainedWriteRidesTheRecordsBatch: each drained write is one batch that also carries the
// pair's seed record, so the record's position and the data it names commit together.
// [BREAK: stage the data in its own batch, committed before the record's]
func TestADrainedWriteRidesTheRecordsBatch(t *testing.T) {
	counted := &p4BatchTally{KV: t3OpenKV(t, t.TempDir())}
	t.Cleanup(func() {
		if err := counted.Close(); err != nil {
			t.Errorf("closing the receiver store: %v", err)
		}
	})
	applier := &p4Applier{}
	rig, buffers := p4Rig(t, applier, counted, 0, SeedOptions{ChunkEntries: 3})
	cut := t3Cut(t, rig)
	body := p4Insert(t)
	p4OfferAtTheFinalChunk(rig, buffers,
		p4Frame(t, rig, cut.Seq+1, body, 1), p4Frame(t, rig, cut.Seq+2, body, 2), p4Frame(t, rig, cut.Seq+3, body, 3))
	rig.pullUntil("the pull to finish", rig.isReady)
	counted.mu.Lock()
	defer counted.mu.Unlock()
	if counted.dataCommits != 3 {
		t.Fatalf("the drained data committed in %d batches, want one per position (3)", counted.dataCommits)
	}
	if counted.withoutRec != 0 {
		t.Fatalf("%d drained batch(es) committed without the seed record, want the record in each", counted.withoutRec)
	}
}

// TestTheDrainsOrdinalContinuesFromTheScan: a drained write that carries a request id is
// numbered after the highest dedupe ordinal already on disk, not from 1. [BREAK: start the
// running ordinal at 1]
func TestTheDrainsOrdinalContinuesFromTheScan(t *testing.T) {
	applier := &p4Applier{}
	rig, buffers := p4Rig(t, applier, nil, 7, SeedOptions{ChunkEntries: 3})
	cut := t3Cut(t, rig)
	p4OfferAtTheFinalChunk(rig, buffers, p4Frame(t, rig, cut.Seq+1, p4Insert(t), 2))
	rig.pullUntil("the pull to finish", rig.isReady)
	entry, err := NewKVDedupeReader(rig.kv).Lookup(rig.slot, reconDB, 9, machine.DedupeKey{RequestID: [16]byte{2}})
	if err != nil {
		t.Fatalf("reading the drained write's dedupe entry: %v", err)
	}
	if entry == nil {
		t.Fatal("the drained write left no dedupe entry")
	}
	if entry.Ordinal != 8 {
		t.Fatalf("the drained write's ordinal is %d, want 8: the scan found 7 and the drain continues from it", entry.Ordinal)
	}
}

// TestADrainedCopyAnswersARetriedPopFromItsOwnEntry: a result-carrying write the copy drains
// leaves the dedupe entry a retry reads, with its result and position, so a retried pop after a
// failover is answered from that entry. The stand-in applier stands for the pop; the entry is
// the real one. [BREAK: stage the drained write through Applier.Fill, not fillApplied]
func TestADrainedCopyAnswersARetriedPopFromItsOwnEntry(t *testing.T) {
	applier := &p4Applier{}
	rig, buffers := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3})
	cut := t3Cut(t, rig)
	pop := cluster.Op{Class: cluster.OpStoreVerb, BucketID: 9, EncID: peerCodecID(), DeclVersion: 1, SchemaVersionSeen: 1,
		Fields: []cluster.FieldOp{{FieldID: 1, Verb: cluster.VerbPop}}}
	body, err := cluster.EncodeOp(pop)
	if err != nil {
		t.Fatalf("EncodeOp: %v", err)
	}
	frame := p4Frame(t, rig, cut.Seq+1, body, 5)
	frame.Record.Class = cluster.OpStoreVerb
	p4OfferAtTheFinalChunk(rig, buffers, frame)
	rig.pullUntil("the pull to finish", rig.isReady)
	entry, err := NewKVDedupeReader(rig.kv).Lookup(rig.slot, reconDB, 9, machine.DedupeKey{RequestID: [16]byte{5}})
	if err != nil {
		t.Fatalf("reading the pop's dedupe entry: %v", err)
	}
	if entry == nil {
		t.Fatal("the drained pop left no dedupe entry for a retry to read")
	}
	if string(entry.Result) != "popped" || entry.Pos != frame.Pos {
		t.Fatalf("the dedupe entry holds result %q at %v, want %q at %v", entry.Result, entry.Pos, "popped", frame.Pos)
	}
}
