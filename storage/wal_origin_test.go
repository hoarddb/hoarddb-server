package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The origin block is the one part of the log format an earlier build cannot
// read (the bucket-replication-spec record 20.3), so these tests pin bytes,
// not just a round trip: a round trip passes for any encoding that agrees with
// itself.

type originVector struct {
	Name string `json:"name"`
	Seq  uint64 `json:"seq"`
	// Op is the entry's op byte. The retained-log rows carry the record's
	// class (0x01), which DecodeWALRecord cross-checks; older rows are 0.
	Op        byte   `json:"op"`
	Kind      byte   `json:"kind"`
	Epoch     uint64 `json:"epoch"`
	Source    string `json:"source"`
	SourceSeq uint64 `json:"source_seq"`
	Outcome   byte   `json:"owner_outcome"`
	Term      uint64 `json:"term"`
	// DecodeOnly marks bytes this build reads and never writes: the kind 2
	// replica block of a build before BUG-0581.
	DecodeOnly bool   `json:"decode_only"`
	Hex        string `json:"hex"`
	// Bucket, KeyHex and DataHex replace the q, k, d every older row carries:
	// the retained-log rows of the entry ring (conv §9) log a position key and
	// a 23-byte-header record, so their bytes cannot be the placeholders.
	Bucket  string `json:"bucket"`
	KeyHex  string `json:"key_hex"`
	DataHex string `json:"data_hex"`
}

// loggedBytes is the bucket label, key and record the vector's bytes carry. The rows
// that name none are the original placeholders.
func (v originVector) loggedBytes(t *testing.T) (bucket string, key, record []byte) {
	t.Helper()
	if v.KeyHex == "" {
		return "q", []byte("k"), []byte("d")
	}
	key, err := hex.DecodeString(v.KeyHex)
	if err != nil {
		t.Fatalf("vector %q key_hex: %v", v.Name, err)
	}
	record, err = hex.DecodeString(v.DataHex)
	if err != nil {
		t.Fatalf("vector %q data_hex: %v", v.Name, err)
	}
	return v.Bucket, key, record
}

// origin is the origin the vector's bytes decode to. Kind 2 reads as a
// replica entry whose outcome and term the earlier build did not log.
func (v originVector) origin() WALOrigin {
	if v.Kind == byte(walOriginReplicaBeforeOutcome) {
		return WALOrigin{Kind: WALOriginReplica, Source: v.Source, Seq: v.SourceSeq, OutcomeNotLogged: true}
	}
	return WALOrigin{Kind: WALOriginKind(v.Kind), Epoch: v.Epoch, Source: v.Source, Seq: v.SourceSeq, OwnerOutcome: v.Outcome, Term: v.Term}
}

func loadOriginVectors(t *testing.T) []originVector {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "wal-origin-vectors.json"))
	if err != nil {
		t.Fatalf("reading the frozen origin vectors: %v", err)
	}
	var file struct {
		Vectors []originVector `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decoding the frozen origin vectors: %v", err)
	}
	if len(file.Vectors) != 11 {
		t.Fatalf("the frozen origin vectors hold %d entries, want the 4 of the build doc, BUG-0581's kind 2, the 4 segment format 2 twins and the 2 retained-log rows of the entry ring", len(file.Vectors))
	}
	return file.Vectors
}

// appendWithOrigin writes through the API that carries an origin, and through
// the plain Append for a legacy one: AppendFrom refuses kind 0 on purpose.
func appendWithOrigin(w *WAL, bucket string, op byte, key, data []byte, origin WALOrigin) (uint64, error) {
	if origin.Kind == WALOriginLegacy {
		return w.Append(bucket, op, key, data)
	}
	return w.AppendFrom(bucket, op, key, data, origin)
}

func TestWALEntryOriginRoundTrips(t *testing.T) {
	origins := []struct {
		name   string
		origin WALOrigin
	}{
		{"own epoch 5", WALOrigin{Kind: WALOriginOwn, Epoch: 5}},
		{"own epoch 300", WALOrigin{Kind: WALOriginOwn, Epoch: 300}},
		{"own epoch 0", WALOrigin{Kind: WALOriginOwn}},
		{"replica", WALOrigin{Kind: WALOriginReplica, Source: "127.0.0.1:7433", Seq: 41}},
		{"replica with the owner's outcome and term 300", WALOrigin{Kind: WALOriginReplica, Source: "127.0.0.1:7433", Seq: 41, OwnerOutcome: 1, Term: 300}},
		{"legacy", WALOrigin{}},
	}

	for _, sealedLog := range []bool{false, true} {
		layout := "plain"
		if sealedLog {
			layout = "sealed"
		}
		t.Run(layout, func(t *testing.T) {
			dir := t.TempDir()
			var keys WALKeySource
			if sealedLog {
				keys = newSealedTestKeySource(t)
			}
			w, err := OpenWAL(dir, keys)
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			seqs := make([]uint64, len(origins))
			for i, tc := range origins {
				seq, err := appendWithOrigin(w, "bucket-"+tc.name, OpInsert, []byte("key-"+tc.name), []byte("data-"+tc.name), tc.origin)
				if err != nil {
					t.Fatalf("%s: append: %v", tc.name, err)
				}
				seqs[i] = seq
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if sealedLog {
				// The origin block is inside the sealed body: nothing in the file
				// names the node an entry was applied from.
				segment, err := os.ReadFile(segmentFilePath(dir, 1))
				if err != nil {
					t.Fatalf("reading the sealed segment: %v", err)
				}
				if bytes.Contains(segment, []byte("127.0.0.1:7433")) {
					t.Error("the source id of a replica entry is readable in a sealed segment")
				}
			}

			reopened, err := OpenWAL(dir, keys)
			if err != nil {
				t.Fatalf("reopening: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			entries, err := reopened.ReadFrom(1)
			if err != nil {
				t.Fatalf("ReadFrom: %v", err)
			}
			if len(entries) != len(origins) {
				t.Fatalf("read %d entries, want %d", len(entries), len(origins))
			}
			for i, tc := range origins {
				got := entries[i]
				if got.Seq != seqs[i] || got.Bucket != "bucket-"+tc.name ||
					string(got.Key) != "key-"+tc.name || string(got.Data) != "data-"+tc.name {
					t.Errorf("%s: fields read back as seq %d bucket %q key %q data %q", tc.name, got.Seq, got.Bucket, got.Key, got.Data)
				}
				if got.Origin != tc.origin {
					t.Errorf("%s: origin read back as %+v, want %+v", tc.name, got.Origin, tc.origin)
				}
			}
		})
	}

	t.Run("a segment written by the starting tree reads back as legacy", func(t *testing.T) {
		dir := t.TempDir()
		fixture, err := os.ReadFile(filepath.Join("testdata", "wal-segment-before-origin.bin"))
		if err != nil {
			t.Fatalf("reading the fixture: %v", err)
		}
		if err := os.WriteFile(segmentFilePath(dir, 1), fixture, 0o600); err != nil {
			t.Fatalf("placing the fixture: %v", err)
		}
		w, err := OpenWAL(dir, nil)
		if err != nil {
			t.Fatalf("OpenWAL on a log written by the starting tree: %v", err)
		}
		defer func() { _ = w.Close() }()
		if recovery := w.Recovery(); recovery != nil {
			t.Fatalf("a log written by the starting tree needed recovery: %+v", recovery)
		}

		var want []struct {
			Seq    uint64 `json:"seq"`
			Op     byte   `json:"op"`
			Bucket string `json:"bucket"`
			Key    string `json:"key_hex"`
			Data   string `json:"data_hex"`
		}
		sidecar, err := os.ReadFile(filepath.Join("testdata", "wal-segment-before-origin.json"))
		if err != nil {
			t.Fatalf("reading the fixture's expected entries: %v", err)
		}
		if err := json.Unmarshal(sidecar, &want); err != nil {
			t.Fatalf("decoding the fixture's expected entries: %v", err)
		}
		entries, err := w.ReadFrom(1)
		if err != nil {
			t.Fatalf("ReadFrom: %v", err)
		}
		if len(entries) != len(want) || len(want) != 7 {
			t.Fatalf("read %d entries, the fixture holds %d (want 7)", len(entries), len(want))
		}
		for i, got := range entries {
			if got.Origin != (WALOrigin{}) {
				t.Errorf("entry %d: origin %+v, want legacy", got.Seq, got.Origin)
			}
			if got.Seq != want[i].Seq || got.Op != want[i].Op || got.Bucket != want[i].Bucket ||
				hex.EncodeToString(got.Key) != want[i].Key || hex.EncodeToString(got.Data) != want[i].Data {
				t.Errorf("entry %d does not match what the starting tree's reader returned for it", got.Seq)
			}
		}

		// The log keeps working: a new entry continues the sequence and the old
		// bytes are untouched.
		next, err := w.AppendFrom("docs", OpInsert, nil, []byte("after"), WALOrigin{Kind: WALOriginOwn, Epoch: 3})
		if err != nil {
			t.Fatalf("AppendFrom after the fixture: %v", err)
		}
		if next != 8 {
			t.Errorf("sequence after the fixture = %d, want 8", next)
		}
		after, err := os.ReadFile(segmentFilePath(dir, 1))
		if err != nil {
			t.Fatalf("reading the segment back: %v", err)
		}
		if !bytes.HasPrefix(after, fixture) {
			t.Error("appending to a log written by the starting tree changed the bytes already in it")
		}
	})
}

// requireEncodesTo requires vector's entry, appended to a fresh log, to be
// written as wantBytes: the whole segment, header and entry, with the
// generation the vectors fix.
func requireEncodesTo(t *testing.T, vector originVector, wantBytes []byte) {
	t.Helper()
	fixWALGeneration(t, frozenWALGeneration)
	encodeDir := t.TempDir()
	w, err := OpenWAL(encodeDir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	// Sequences are the log's to assign; the vectors fix them, so the fresh
	// log is told where it stands.
	w.lastSeq = vector.Seq - 1
	bucket, key, record := vector.loggedBytes(t)
	gotSeq, err := appendWithOrigin(w, bucket, vector.Op, key, record, vector.origin())
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if gotSeq != vector.Seq {
		t.Fatalf("append assigned sequence %d, want %d", gotSeq, vector.Seq)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	gotBytes, err := os.ReadFile(segmentFilePath(encodeDir, 1))
	if err != nil {
		t.Fatalf("reading the segment: %v", err)
	}
	if !bytes.Equal(gotBytes, wantBytes) {
		t.Errorf("encoded bytes differ from the frozen vector\n got %x\nwant %x", gotBytes, wantBytes)
	}
}

func TestWALOriginVectors(t *testing.T) {
	for _, vector := range loadOriginVectors(t) {
		t.Run(vector.Name, func(t *testing.T) {
			wantBytes, err := hex.DecodeString(vector.Hex)
			if err != nil {
				t.Fatalf("vector hex: %v", err)
			}

			if !vector.DecodeOnly {
				requireEncodesTo(t, vector, wantBytes)
			}

			decodeDir := t.TempDir()
			if err := os.WriteFile(segmentFilePath(decodeDir, 1), wantBytes, 0o600); err != nil {
				t.Fatalf("writing the vector as a segment: %v", err)
			}
			reader, err := OpenWAL(decodeDir, nil)
			if err != nil {
				t.Fatalf("OpenWAL on the vector: %v", err)
			}
			defer func() { _ = reader.Close() }()
			entries, err := reader.ReadFrom(1)
			if err != nil {
				t.Fatalf("ReadFrom: %v", err)
			}
			if len(entries) != 1 {
				t.Fatalf("the vector decoded to %d entries, want 1", len(entries))
			}
			got := entries[0]
			bucket, key, record := vector.loggedBytes(t)
			if got.Seq != vector.Seq || got.Op != vector.Op || got.Bucket != bucket ||
				!bytes.Equal(got.Key, key) || !bytes.Equal(got.Data, record) || got.Origin != vector.origin() {
				t.Errorf("the vector decoded to %+v, want seq %d op %d bucket %q key %x record %x origin %+v",
					got, vector.Seq, vector.Op, bucket, key, record, vector.origin())
			}
		})
	}
}

// rawPlainEntry frames a plain-log entry body by hand, so a test can write a
// shape the writer refuses to produce. withChecksum false leaves the
// framing word's high half zero, which is how a build without the checksum
// wrote entries.
func rawPlainEntry(seq uint64, bucketWord uint32, bucket string, key string, tail []byte, withChecksum bool) []byte {
	body := rawPlainBody(seq, bucketWord, bucket, key, tail)
	header := walHeader(len(body), body)
	if !withChecksum {
		header = make([]byte, 8)
		binary.BigEndian.PutUint64(header, uint64(uint32(len(body))))
	}
	return append(header, body...)
}

// rawEntryForSegment frames a hand-built plain entry, with its checksum, for
// the segment at path, in that segment's own format: v1 framing on a v1
// segment, the generation inside the CRC on a v2 one. An entry framed for
// another format fails its CRC and never reaches the check a test is about.
func rawEntryForSegment(t *testing.T, path string, seq uint64, bucketWord uint32, bucket string, key string, tail []byte) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the segment to frame an entry for it: %v", err)
	}
	format, err := readSegmentFormat(bytes.NewReader(raw), int64(len(raw)), filepath.Base(path))
	if err != nil {
		t.Fatalf("reading the segment's format: %v", err)
	}
	body := rawPlainBody(seq, bucketWord, bucket, key, tail)
	return append(frameWord(len(body), format.checksum(nil, body)), body...)
}

// rawPlainBody is a plain entry's body: seq, op, bucket word, bucket, key
// length, key, then tail (an origin block and data, or data).
func rawPlainBody(seq uint64, bucketWord uint32, bucket string, key string, tail []byte) []byte {
	body := make([]byte, 0, 32+len(tail))
	body = binary.BigEndian.AppendUint64(body, seq)
	body = append(body, OpInsert)
	body = binary.BigEndian.AppendUint32(body, bucketWord)
	body = append(body, bucket...)
	body = binary.BigEndian.AppendUint32(body, uint32(len(key)))
	body = append(body, key...)
	return append(body, tail...)
}

// TestAChecksummedEntryThatDoesNotParseStopsTheOpen: an entry whose checksum
// passes was written whole. If it does not parse, the writer was a build this
// one cannot read, and truncating it would make the next append reuse its
// sequence number -- exactly what the starting tree's reader does to a flagged
// entry (review F-7).
func TestAChecksummedEntryThatDoesNotParseStopsTheOpen(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendTestEntries(t, w, 2)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := segmentFilePath(dir, 1)
	unknownKind := rawEntryForSegment(t, path, 3, 1<<31|1, "q", "k", []byte{9, 'd'})
	segment, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("opening the segment: %v", err)
	}
	if _, err := segment.Write(unknownKind); err != nil {
		t.Fatalf("writing the entry of an unknown kind: %v", err)
	}
	if err := segment.Close(); err != nil {
		t.Fatalf("closing the segment: %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	reopened, err := OpenWAL(dir, nil)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("OpenWAL accepted a checksummed entry of an unknown kind")
	}
	if !strings.Contains(err.Error(), "kind 9") || !strings.Contains(err.Error(), "newer build") {
		t.Errorf("the error does not name the kind and the newer build: %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after the failed open: %v", err)
	}
	if after.Size() != before.Size() {
		t.Errorf("the failed open changed the segment from %d to %d bytes", before.Size(), after.Size())
	}
}

// TestAFlaggedEntryWithOriginKindZeroIsCorruptionNotANewerBuild: kind 0 is the
// legacy marker, which a writer never flags an origin block with, so a flagged
// entry that carries it is damaged. Telling the operator to upgrade would send
// them after the wrong fix.
func TestAFlaggedEntryWithOriginKindZeroIsCorruptionNotANewerBuild(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendTestEntries(t, w, 2)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	path := segmentFilePath(dir, 1)
	flaggedLegacy := rawEntryForSegment(t, path, 3, 1<<31|1, "q", "k", []byte{0, 'd'})
	segment, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("opening the segment: %v", err)
	}
	if _, err := segment.Write(flaggedLegacy); err != nil {
		t.Fatalf("writing the entry: %v", err)
	}
	if err := segment.Close(); err != nil {
		t.Fatalf("closing the segment: %v", err)
	}

	reopened, err := OpenWAL(dir, nil)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("OpenWAL accepted a flagged entry with origin kind 0")
	}
	message := err.Error()
	if strings.Contains(message, "newer build") {
		t.Errorf("kind 0 is corruption, but the error blames a newer build: %v", err)
	}
	for _, want := range []string{"corrupt", "kind 0", "offset", "sequence 3", segmentFileName(1)} {
		if !strings.Contains(message, want) {
			t.Errorf("the error does not contain %q: %v", want, err)
		}
	}
}

// TestAReplicaEntryOfAnEarlierBuildIsReadWithItsOutcomeUnknown: before
// BUG-0581 a replica entry's block was kind 2, with no owner outcome or term,
// and an upgraded node's log holds such entries (a retention pass keeps the
// segment that holds its bound). The owner decided on 2026-10-16 that they are
// read as before: the open accepts them, their fields and data read back
// whole, the outcome reads unknown and the term absent, and an entry this
// build appends after them is kind 3 and reads back with both.
func TestAReplicaEntryOfAnEarlierBuildIsReadWithItsOutcomeUnknown(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendTestEntries(t, w, 2)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	earlier := append([]byte{2, 6}, "node-a"...)
	earlier = binary.BigEndian.AppendUint64(earlier, 41)
	earlier = append(earlier, 'd', 'a', 't', 'a')
	segment, err := os.OpenFile(segmentFilePath(dir, 1), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("opening the segment: %v", err)
	}
	if _, err := segment.Write(rawEntryForSegment(t, segmentFilePath(dir, 1), 3, 1<<31|1, "q", "k", earlier)); err != nil {
		t.Fatalf("writing the entry: %v", err)
	}
	if err := segment.Close(); err != nil {
		t.Fatalf("closing the segment: %v", err)
	}

	upgraded, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL refused a log holding a replica entry of an earlier build: %v", err)
	}
	current := WALOrigin{Kind: WALOriginReplica, Source: "node-a", Seq: 42, OwnerOutcome: 1, Term: 7}
	if seq, err := upgraded.AppendFrom("q", OpInsert, []byte("k2"), []byte("later"), current); err != nil || seq != 4 {
		t.Fatalf("appending after the earlier build's entry: sequence %d, %v; want 4", seq, err)
	}
	// Every open starts a new segment, so the entry is in the segment the
	// upgraded open started, not beside the earlier build's.
	appendedTo := upgraded.activeIndex
	if err := upgraded.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	segmentBytes, err := os.ReadFile(segmentFilePath(dir, appendedTo))
	if err != nil {
		t.Fatalf("reading the segment: %v", err)
	}
	if !bytes.Contains(segmentBytes, append([]byte{byte(WALOriginReplica), 6}, "node-a"...)) {
		t.Error("the entry appended after the earlier build's was not written as kind 3")
	}

	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("reopening the mixed log: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	entries, err := reopened.ReadFrom(3)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("read %d entries from sequence 3, want 2", len(entries))
	}
	old := entries[0]
	if old.Seq != 3 || old.Bucket != "q" || string(old.Key) != "k" || string(old.Data) != "data" ||
		old.Origin.Kind != WALOriginReplica || old.Origin.Source != "node-a" || old.Origin.Seq != 41 ||
		old.Origin.OwnerOutcome != 0 || old.Origin.Term != 0 || !old.Origin.OutcomeNotLogged {
		t.Errorf("the earlier build's entry read back as %+v; want sequence 3 of q/k, data \"data\", from node-a at 41, outcome unknown, no term, marked as not logged", old)
	}
	later := entries[1]
	if later.Seq != 4 || string(later.Key) != "k2" || string(later.Data) != "later" || later.Origin != current {
		t.Errorf("the entry appended after it read back as %+v; want sequence 4 of q/k2, data \"later\", origin %+v", later, current)
	}
}

// TestAnEntryWithNoChecksumThatDoesNotParseIsStillATornTail keeps the other
// half of the rule: only the entry that cannot vouch for itself is a torn tail.
//
// An entry without a checksum exists only in format 1 (in format 2 every CRC is
// verified), so the log is a v1 segment on purpose: the frozen fixture.
func TestAnEntryWithNoChecksumThatDoesNotParseIsStillATornTail(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(segmentFilePath(dir, 1), fixtureRefPlaintextWALSegment, 0o600); err != nil {
		t.Fatalf("placing the v1 fixture: %v", err)
	}
	goodEnd := int64(len(fixtureRefPlaintextWALSegment))

	unvouched := rawPlainEntry(4, 1<<31|1, "q", "k", []byte{9, 'd'}, false)
	segment, err := os.OpenFile(segmentFilePath(dir, 1), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("opening the segment: %v", err)
	}
	if _, err := segment.Write(unvouched); err != nil {
		t.Fatalf("writing the entry: %v", err)
	}
	_ = segment.Close()

	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("an entry with no checksum that does not parse must be repaired as a torn tail: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if recovery := reopened.Recovery(); recovery == nil || recovery.RecoveredTo != goodEnd {
		t.Errorf("recovery = %+v, want the log cut back to %d", recovery, goodEnd)
	}
}

func TestAppendFromRefusesWhatCannotBeParsedBack(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer func() { _ = w.Close() }()

	longSource := strings.Repeat("n", 256)
	cases := []struct {
		name   string
		origin WALOrigin
		want   string
	}{
		{"legacy kind", WALOrigin{}, "legacy"},
		{"kind 2, the replica block before BUG-0581", WALOrigin{Kind: 2, Source: "n1", Seq: 1}, "kind 2"},
		{"a replica entry read from kind 2", WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: 1, OutcomeNotLogged: true}, "kind 2"},
		{"kind 4", WALOrigin{Kind: 4}, "kind 4"},
		{"a source id of 256 bytes", WALOrigin{Kind: WALOriginReplica, Source: longSource, Seq: 1}, longSource},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := w.AppendFrom("q", OpInsert, []byte("k"), []byte("d"), tc.origin)
			if err == nil {
				t.Fatal("AppendFrom accepted an origin that cannot be read back")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
	if last := w.LastSequence(); last != 0 {
		t.Errorf("refused appends moved the sequence to %d", last)
	}
}
