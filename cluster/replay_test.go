package cluster

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

type loggedWrite struct {
	slot uint16
	db   uint64
	pos  Position
}

// appendWrites writes one position-keyed record per entry, in order, to a real
// log, and returns the log.
func appendWrites(t *testing.T, writes []loggedWrite) *storage.WAL {
	t.Helper()
	w, err := storage.OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	for _, write := range writes {
		key := storage.PositionKey(write.slot, write.db, storage.LogPosition(write.pos))
		record := storage.EncodeWALRecord(0, [16]byte{byte(write.pos.Seq)}, []byte{0x01, 0x00, 'b', 'o', 'd', 'y'})
		if _, err := w.AppendFrom("app.users", 0x01, key, record, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: write.pos.Epoch}); err != nil {
			t.Fatalf("append %v: %v", write.pos, err)
		}
	}
	return w
}

func noneApplied(uint16, uint64) Position { return Position{} }

// TestAPairIsNotCurrentUntilItsReplayReachesTheTail interleaves two pairs so
// that pair 1's last record comes before pair 2's remaining records. A
// per-pair mark set once a pair's last record is applied would make pair 1
// current while pair 2's records are still ahead in the log, and pair 1 would
// then serve from a store that is behind the log it acknowledged from (write
// spec §3.3). Every callback asserts that neither pair is current.
func TestAPairIsNotCurrentUntilItsReplayReachesTheTail(t *testing.T) {
	log := appendWrites(t, []loggedWrite{
		{1, 10, Position{1, 1}}, {2, 10, Position{1, 1}}, {1, 10, Position{1, 2}}, {2, 10, Position{1, 2}}, {2, 10, Position{1, 3}},
	})
	replayer := NewReplayer(log, noneApplied)
	if replayer.AtTail(1, 10) || replayer.AtTail(2, 10) {
		t.Fatal("a pair is current before any replay ran")
	}
	var delivered, afterPairOneLast int
	err := replayer.Replay(1, Position{}, func(record WALRecord) error {
		delivered++
		if record.Slot == 2 && record.Pos.Seq >= 2 {
			// Pair 1's last record, 1:2, was applied before this one.
			afterPairOneLast++
		}
		if replayer.AtTail(1, 10) || replayer.AtTail(2, 10) {
			t.Errorf("a pair is current while record %d:%v is being replayed and the log has records ahead", record.Slot, record.Pos)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if delivered != 5 || afterPairOneLast != 2 {
		t.Fatalf("delivered %d records, %d of them after pair 1's last; want 5 and 2", delivered, afterPairOneLast)
	}
	if !replayer.AtTail(1, 10) || !replayer.AtTail(2, 10) {
		t.Error("a pair is not current after its replay reached the tail")
	}
}

func TestResumeRespectsTheDatabaseWithinASlot(t *testing.T) {
	log := appendWrites(t, []loggedWrite{
		{1, 10, Position{1, 1}}, {1, 20, Position{1, 1}}, {1, 10, Position{1, 2}}, {1, 20, Position{1, 2}}, {1, 20, Position{1, 3}},
	})
	applied := func(_ uint16, db uint64) Position {
		if db == 10 {
			return Position{1, 2}
		}
		return Position{1, 1}
	}
	var got []loggedWrite
	err := NewReplayer(log, applied).Replay(1, Position{}, func(record WALRecord) error {
		got = append(got, loggedWrite{record.Slot, record.DB, record.Pos})
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	want := []loggedWrite{{1, 20, Position{1, 2}}, {1, 20, Position{1, 3}}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("replayed %v, want %v: database 10 is applied through 1:2 and database 20 through 1:1", got, want)
	}
}

func TestABoundedReplayDoesNotMarkTheTail(t *testing.T) {
	log := appendWrites(t, []loggedWrite{{1, 10, Position{1, 1}}, {1, 10, Position{1, 2}}})
	replayer := NewReplayer(log, noneApplied)
	if err := replayer.Replay(1, Position{1, 1}, func(WALRecord) error { return nil }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if replayer.AtTail(1, 10) {
		t.Error("a replay bounded by until marked the pair current although records remain")
	}
}

// TestReplayRefusesAKeyOf25Or27BytesNamingTheSequence: a key one byte off the
// position layout would otherwise be read as another pair's record.
func TestReplayRefusesAKeyOf25Or27BytesNamingTheSequence(t *testing.T) {
	for _, keyLen := range []int{25, 27} {
		w, err := storage.OpenWAL(t.TempDir(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Close() })
		record := storage.EncodeWALRecord(0, [16]byte{}, []byte{0x01, 0x00})
		if _, err := w.Append("app.users", 0x01, storage.PositionKey(1, 1, storage.LogPosition{Epoch: 1, Seq: 1}), record); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Append("app.users", 0x01, make([]byte, keyLen), record); err != nil {
			t.Fatal(err)
		}
		err = NewReplayer(w, noneApplied).Replay(1, Position{}, func(WALRecord) error { return nil })
		if err == nil || !strings.Contains(err.Error(), "sequence 2") {
			t.Errorf("a %d-byte key gave %v, want an error naming sequence 2", keyLen, err)
		}
	}
}

func TestAFailedReplayLeavesThePairNotCurrent(t *testing.T) {
	log := appendWrites(t, []loggedWrite{{1, 10, Position{1, 1}}, {1, 10, Position{1, 2}}})
	replayer := NewReplayer(log, noneApplied)
	refusal := errors.New("apply refused")
	err := replayer.Replay(1, Position{}, func(WALRecord) error { return refusal })
	if !errors.Is(err, refusal) {
		t.Fatalf("Replay returned %v, want the apply's error", err)
	}
	if replayer.AtTail(1, 10) {
		t.Error("a pair whose replay failed is current")
	}
}

func TestReplayResumesEachPairAfterItsOwnPosition(t *testing.T) {
	log := appendWrites(t, []loggedWrite{
		{1, 10, Position{1, 1}}, {2, 10, Position{1, 1}}, {1, 10, Position{1, 2}}, {2, 10, Position{1, 2}},
	})
	applied := func(slot uint16, _ uint64) Position {
		if slot == 1 {
			return Position{1, 1}
		}
		return Position{}
	}
	var got []loggedWrite
	err := NewReplayer(log, applied).Replay(1, Position{}, func(record WALRecord) error {
		got = append(got, loggedWrite{record.Slot, record.DB, record.Pos})
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	want := []loggedWrite{{2, 10, Position{1, 1}}, {1, 10, Position{1, 2}}, {2, 10, Position{1, 2}}}
	if len(got) != len(want) {
		t.Fatalf("replayed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d is %v, want %v", i, got[i], want[i])
		}
	}
}

func TestReplayStopsAfterUntilAndFromSkipsEarlierEntries(t *testing.T) {
	log := appendWrites(t, []loggedWrite{{1, 10, Position{1, 1}}, {1, 10, Position{1, 2}}, {1, 10, Position{1, 3}}})
	var got []Position
	err := NewReplayer(log, noneApplied).Replay(2, Position{1, 2}, func(record WALRecord) error {
		got = append(got, record.Pos)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 || got[0] != (Position{1, 2}) {
		t.Errorf("replayed %v from cursor 2 until 1:2, want only 1:2", got)
	}
}

func TestReplayCarriesTheRecordFields(t *testing.T) {
	log := appendWrites(t, []loggedWrite{{7, 99, Position{3, 5}}})
	var got WALRecord
	err := NewReplayer(log, noneApplied).Replay(1, Position{}, func(record WALRecord) error {
		got = record
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if got.Slot != 7 || got.DB != 99 || got.Pos != (Position{3, 5}) || got.Cursor != 1 ||
		got.RequestID[0] != 5 || string(got.Body[2:]) != "body" || got.Class != 0x01 {
		t.Errorf("record = %+v", got)
	}
}

func TestReplayRefusesAnEntryThatIsNotPositionKeyed(t *testing.T) {
	w, err := storage.OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	if _, err := w.Append("app.users", 0x01, []byte("k"), []byte("d")); err != nil {
		t.Fatal(err)
	}
	replayer := NewReplayer(w, noneApplied)
	if err := replayer.Replay(1, Position{}, func(WALRecord) error { return nil }); err == nil {
		t.Error("an entry keyed by 1 byte was replayed")
	}
	if replayer.AtTail(1, 1) {
		t.Error("a pair is current after a replay that refused an entry")
	}
}

func TestReplayOfAnEmptyLogReachesTheTail(t *testing.T) {
	log := appendWrites(t, nil)
	replayer := NewReplayer(log, noneApplied)
	if err := replayer.Replay(1, Position{}, func(WALRecord) error { return nil }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !replayer.AtTail(1, 1) {
		t.Error("a pair with no records is not current after an empty replay")
	}
}

// TestAVersionOneRecordStopsReplayLoudlyAtTheReplayer is R1 at the replayer: a record of
// another version stops the replay with the text naming the sequence, the version and the
// fix, and nothing is delivered.
func TestAVersionOneRecordStopsReplayLoudlyAtTheReplayer(t *testing.T) {
	w, err := storage.OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	v1 := append([]byte{0x01, 0x01, 0x00}, make([]byte, 16)...)
	v1 = append(v1, 0, 0, 0, 2, 0x01, 0x00)
	key := storage.PositionKey(7, 99, storage.LogPosition{Epoch: 3, Seq: 5})
	if _, err := w.AppendFrom("7.1", 0x01, key, v1, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: 3}); err != nil {
		t.Fatal(err)
	}
	delivered := 0
	err = NewReplayer(w, noneApplied).Replay(1, Position{}, func(WALRecord) error { delivered++; return nil })
	if err == nil || delivered != 0 {
		t.Fatalf("Replay = %v with %d records delivered; want R1 and nothing applied", err, delivered)
	}
	for _, want := range []string{
		"retained-log record at sequence 1: version 0x01 is not the 0x02 this release reads. Nothing was applied: ",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Replay error %q\n  lacks %q", err, want)
		}
	}
	requireNoReseedAdvice(t, err)
	requireNoOtherBuild(t, err.Error())
}

// requireNoReseedAdvice fails when a replay refusal gives advice: the replayer does not know
// whether its node is a cluster member, and a fresh or empty data directory loses committed
// cluster state on a member (store-vouched-cut review R-1). The ring node adds the advice.
func requireNoReseedAdvice(t *testing.T, err error) {
	t.Helper()
	var refusal *ReplayRecordError
	if !errors.As(err, &refusal) {
		t.Errorf("the refusal is %T, want a *ReplayRecordError the node can add its advice to: %v", err, err)
	}
	for _, banned := range []string{"seeded", "fresh data directory", "empty directory", "storage.data_dir", "aside"} {
		if strings.Contains(err.Error(), banned) {
			t.Errorf("the replay refusal says %q, advice the replayer cannot give: %v", banned, err)
		}
	}
}

// otherBuildWords are the phrases that send an operator to a build other than this one. No other
// build was released (owner ruling 2026-11-21), so no refusal about a node's own data says them.
var otherBuildWords = []string{"earlier build", "earlier development build", "another build", "previous release", "the build that wrote",
	"earlier version", "older version", "previous version", "older build", "earlier hoardDB", "earlier engine"}

// requireNoOtherBuild fails when text names a build that was never released.
func requireNoOtherBuild(t *testing.T, text string) {
	t.Helper()
	for _, banned := range otherBuildWords {
		if strings.Contains(text, banned) {
			t.Errorf("the text says %q, and no other build was released: %s", banned, text)
		}
	}
}

// TestAReservedFlagBitIsRefused is R2's second form at the replayer: a record whose body
// flags carry a bit this build does not know (a packed body of a newer build) stops the
// replay naming the sequence and the position, and is never applied as one operation.
func TestAReservedFlagBitIsRefused(t *testing.T) {
	w, err := storage.OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	body, err := EncodeOp(opcodecHeader(Op{Class: OpDelete}))
	if err != nil {
		t.Fatal(err)
	}
	body[1] = 0x02
	key := storage.PositionKey(7, 99, storage.LogPosition{Epoch: 3, Seq: 5})
	if _, err := w.AppendFrom("7.1", OpDelete, key, storage.EncodeWALRecord(0, [16]byte{1}, body), storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: 3}); err != nil {
		t.Fatal(err)
	}
	applied := 0
	err = NewReplayer(w, noneApplied).Replay(1, Position{}, func(rec WALRecord) error {
		if _, err := DecodeOp(rec.Body); err != nil {
			return err
		}
		applied++
		return nil
	})
	if err == nil || applied != 0 {
		t.Fatalf("Replay = %v with %d records applied; want R2 and nothing applied", err, applied)
	}
	for _, want := range []string{
		"retained-log record at sequence 1 (position 3:5): flags 0x02 carry a bit this build does not know, so the record cannot be trusted. Nothing was applied. This release does not know how that record was written: ",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Replay error %q\n  lacks %q", err, want)
		}
	}
	requireNoReseedAdvice(t, err)
	requireNoOtherBuild(t, err.Error())
	if strings.Contains(err.Error(), "damaged") {
		t.Errorf("R2 names damage as the cause of a record that passed the log's checksum: %v", err)
	}
}

// TestARecordWhoseBodyClassDiffersStopsReplay is R2's first form at the replayer.
func TestARecordWhoseBodyClassDiffersStopsReplay(t *testing.T) {
	w, err := storage.OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	key := storage.PositionKey(7, 99, storage.LogPosition{Epoch: 3, Seq: 5})
	if _, err := w.AppendFrom("7.1", 0x05, key, storage.EncodeWALRecord(0, [16]byte{1}, []byte{0x01, 0x00, 'x'}), storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: 3}); err != nil {
		t.Fatal(err)
	}
	err = NewReplayer(w, noneApplied).Replay(1, Position{}, func(WALRecord) error { t.Fatal("delivered"); return nil })
	if err == nil || !strings.Contains(err.Error(), "retained-log record at sequence 1 (position 3:5): the body says class 0x01 and the entry 0x05, so the record cannot be trusted. Nothing was applied.") {
		t.Fatalf("Replay = %v, want R2's first form", err)
	}
}
