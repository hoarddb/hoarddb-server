package server

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// allocationRuns is how many times each measured call repeats;
// testing.AllocsPerRun returns the average, so a stray allocation from a
// background goroutine rounds away.
const allocationRuns = 20

// mapDecodeAllocations is the allocations of decoding one stored document into
// a generic Go map: the round trip the raw paths no longer make per entry.
// Measuring it in the same test as the paths it bounds is what keeps the bound
// honest across codec libraries and Go versions: nobody picks the number.
func mapDecodeAllocations(tb testing.TB, stored []byte) float64 {
	tb.Helper()
	return testing.AllocsPerRun(allocationRuns, func() {
		var fields map[string]interface{}
		if err := bson.Unmarshal(stored, &fields); err != nil {
			tb.Fatalf("decoding a stored document into a map: %v", err)
		}
	})
}

// addedAllocationsPerEntry runs smallCall, which builds a response of
// smallEntries entries, and largeCall, which builds one of largeEntries, and
// returns the allocations each extra entry cost. The fixed cost of a call
// (resolving the bucket, the snapshot, the response frame) cancels in the
// difference.
func addedAllocationsPerEntry(tb testing.TB, smallEntries, largeEntries int, smallCall, largeCall func()) float64 {
	tb.Helper()
	smallAllocations := testing.AllocsPerRun(allocationRuns, smallCall)
	largeAllocations := testing.AllocsPerRun(allocationRuns, largeCall)
	return (largeAllocations - smallAllocations) / float64(largeEntries-smallEntries)
}

// findStoredBytes returns the stored BSON Find hands back for key.
func findStoredBytes(tb testing.TB, s *Server, database, bucket, key string) []byte {
	tb.Helper()
	found, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: key})
	if err != nil || !found.Found {
		tb.Fatalf("Find(%q) = %+v, %v", key, found, err)
	}
	return found.Data
}

// TestRangeQueryDoesNotDecodePerEntry holds the range response to the bar
// Find already meets: the stored BSON is handed to the wire untouched, with no
// decode into a map and no re-encode from one. It is self-calibrating: in one
// run it measures a range query over N entries and over 2N, and one map decode
// of one entry, and requires that each extra entry cost fewer allocations than
// that decode. Re-inserting a per-entry Unmarshal into a map on the range path
// adds a whole decode per entry and fails it.
func TestRangeQueryDoesNotDecodePerEntry(t *testing.T) {
	s := newTestServer(t)
	smallEntries, largeEntries := rangeBandWidth, 2*rangeBandWidth
	seedRangeBand(t, s, "app", "small", smallEntries)
	seedRangeBand(t, s, "app", "large", largeEntries)

	rangeOver := func(bucket string, entries int) func() {
		request := BTreeRangeRequest{Database: "app", Bucket: bucket, Low: "k0000000", High: fmt.Sprintf("k%07d", entries-1)}
		return func() {
			ranged, err := s.RangeQuery(request)
			if err != nil || len(ranged.Entries) != entries {
				t.Fatalf("RangeQuery over %q = %v entries, %v; want %d", bucket, ranged, err, entries)
			}
			if _, err := bson.Marshal(ranged); err != nil {
				t.Fatalf("marshaling the range response: %v", err)
			}
		}
	}
	perEntry := addedAllocationsPerEntry(t, smallEntries, largeEntries, rangeOver("small", smallEntries), rangeOver("large", largeEntries))
	oneDecode := mapDecodeAllocations(t, findStoredBytes(t, s, "app", "large", "k0000000"))
	if oneDecode < 1 {
		t.Fatalf("one map decode measured %v allocations; the measurement is broken, so the bound below would mean nothing", oneDecode)
	}
	t.Logf("each extra range entry costs %.2f allocations; one map decode of an entry costs %.0f", perEntry, oneDecode)
	if perEntry >= oneDecode {
		t.Errorf("each extra range entry cost %.2f allocations building the response; one map decode of an entry costs %.0f, "+
			"so the range path is decoding or re-encoding every entry", perEntry, oneDecode)
	}

	// Edge walks the same store and must not decode either. Its one entry is
	// compared with a one-entry range, which is the raw path.
	lastKey := fmt.Sprintf("k%07d", largeEntries-1)
	edgeAllocations := testing.AllocsPerRun(allocationRuns, func() {
		edge, err := s.Edge("app", "large", true)
		if err != nil || len(edge.Entries) != 1 {
			t.Fatalf("Edge(last) = %+v, %v", edge, err)
		}
		if _, err := bson.Marshal(edge); err != nil {
			t.Fatalf("marshaling the edge response: %v", err)
		}
	})
	oneEntryAllocations := testing.AllocsPerRun(allocationRuns, func() {
		ranged, err := s.RangeQuery(BTreeRangeRequest{Database: "app", Bucket: "large", Low: lastKey, High: lastKey})
		if err != nil || len(ranged.Entries) != 1 {
			t.Fatalf("RangeQuery(%q) = %+v, %v", lastKey, ranged, err)
		}
		if _, err := bson.Marshal(ranged); err != nil {
			t.Fatalf("marshaling the range response: %v", err)
		}
	})
	if edgeAllocations-oneEntryAllocations >= oneDecode {
		t.Errorf("an edge lookup cost %.0f allocations against %.0f for a one-entry range; the %.0f extra is a map decode",
			edgeAllocations, oneEntryAllocations, edgeAllocations-oneEntryAllocations)
	}
}

// wireRangeResponse decodes a range response frame independently of the
// server's own types, so the check below reads the bytes a client receives.
type wireRangeResponse struct {
	Entries []struct {
		Key   string   `bson:"key"`
		Value bson.Raw `bson:"value"`
	} `bson:"entries"`
}

// TestRangeQueryReturnsStoredBytesUnchanged proves the range response carries
// each stored document byte for byte — the bytes Find returns for the same key
// — through documents a map round trip could disturb: many keys, a nested
// document, an array, an int64, a NaN, a datetime and binary data.
func TestRangeQueryReturnsStoredBytesUnchanged(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "mixed", BucketTypeBTree)

	stamp := time.Date(2026, 9, 24, 18, 30, 15, 123_000_000, time.UTC)
	documents := map[string]map[string]interface{}{
		"doc-a": {"seq": int32(1), "big": int64(math.MaxInt64), "ratio": 0.1, "name": "alpha", "flag": true, "gone": nil},
		"doc-b": {"seq": int32(2), "nan": math.NaN(), "at": stamp, "tags": []interface{}{"x", int32(7), 2.5}},
		"doc-c": {"seq": int32(3), "blob": primitive.Binary{Subtype: 0x00, Data: []byte{0x00, 0xff, 0x10, 0x80}},
			"nested": map[string]interface{}{"z": int32(1), "a": "first", "m": map[string]interface{}{"deep": 1.5}}},
		"doc-d": {"seq": int32(4), "k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4", "k5": "v5", "k6": "v6", "k7": "v7"},
	}
	keys := []string{"doc-a", "doc-b", "doc-c", "doc-d"}
	storedBytes := make(map[string][]byte, len(keys))
	for _, key := range keys {
		inserted, err := s.Insert(InsertRequest{Database: "app", Bucket: "mixed", Key: key, Data: documents[key]})
		if err != nil || !inserted.Success {
			t.Fatalf("Insert(%q) = %+v, %v", key, inserted, err)
		}
		found, err := s.Find(FindRequest{Database: "app", Bucket: "mixed", Key: key})
		if err != nil || !found.Found {
			t.Fatalf("Find(%q) = %+v, %v", key, found, err)
		}
		storedBytes[key] = found.Data
	}

	ranged, err := s.RangeQuery(BTreeRangeRequest{Database: "app", Bucket: "mixed", Low: "doc-a", High: "doc-d"})
	if err != nil {
		t.Fatalf("RangeQuery: %v", err)
	}
	frame, err := bson.Marshal(ranged)
	if err != nil {
		t.Fatalf("marshaling the range response: %v", err)
	}
	var onWire wireRangeResponse
	if err := bson.Unmarshal(frame, &onWire); err != nil {
		t.Fatalf("decoding the range response frame: %v", err)
	}
	if len(onWire.Entries) != len(keys) {
		t.Fatalf("the range frame carries %d entries, want %d", len(onWire.Entries), len(keys))
	}
	for position, entry := range onWire.Entries {
		if entry.Key != keys[position] {
			t.Errorf("entry %d has key %q, want %q", position, entry.Key, keys[position])
			continue
		}
		if !bytes.Equal(entry.Value, storedBytes[entry.Key]) {
			t.Errorf("entry %q changed on the way to the wire:\n  stored %x\n  wire   %x", entry.Key, storedBytes[entry.Key], []byte(entry.Value))
		}
	}
}
