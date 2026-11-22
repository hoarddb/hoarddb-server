package store

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// ttlFixtureNow is the sweep's clock in these tests.
var ttlFixtureNow = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

// ttlIndexOf indexes one value per document on field "At" and returns the
// index: a value of every tag, and an expired and a live value of each of the
// two tags a TTL index expires by.
func ttlIndexOf(t *testing.T) *SecondaryIndex {
	t.Helper()
	kv := kvfault.New()
	im, err := NewIndexManager(kv, 1, 1, 1, oneFieldSpecs("At"))
	if err != nil {
		t.Fatalf("NewIndexManager: %v", err)
	}
	expired, live := ttlFixtureNow.Add(-2*time.Hour), ttlFixtureNow.Add(time.Hour)
	for key, value := range map[string]interface{}{
		"null":         nil,
		"bool":         true,
		"string":       "yesterday",
		"binary":       primitive.Binary{Data: []byte{0x01}},
		"expired-date": primitive.NewDateTimeFromTime(expired),
		"live-date":    primitive.NewDateTimeFromTime(live),
		"expired-secs": expired.Unix(),
		"live-secs":    float64(live.Unix()),
		// The same 64-bit integer as expired-date, as a number: Unix seconds
		// ~50,000 years ahead. It and expired-date differ only in the tag byte.
		"date-as-number": int64(primitive.NewDateTimeFromTime(expired)),
	} {
		doc := map[string]interface{}{"At": value}
		if err := inBatch(kv, func(b storage.KVBatch) error { return im.OnInsert(b, []byte(key), doc) }); err != nil {
			t.Fatalf("OnInsert(%s): %v", key, err)
		}
	}
	return im.indexes[0]
}

// TestTTLBoundCarriesItsTypeTagAtBothEnds (guard 4, key level): each bracket's
// upper bound carries its bracket's tag, and a walk of the two brackets
// visits the expired dates and the expired numbers — never a null, a boolean,
// a string, a binary, or a value of the other bracket's type.
func TestTTLBoundCarriesItsTypeTagAtBothEnds(t *testing.T) {
	index := ttlIndexOf(t)
	var visited []string
	for _, bracket := range TTLBounds(ttlFixtureNow, time.Hour) {
		if len(bracket.Upper) == 0 || bracket.Upper[0] != bracket.Tag {
			t.Errorf("bracket 0x%02x's upper bound %x does not carry its tag", bracket.Tag, bracket.Upper)
		}
		if err := index.ScanBracket(bracket.Tag, bracket.Upper, func(docKey []byte) error {
			visited = append(visited, string(docKey))
			return nil
		}); err != nil {
			t.Fatalf("ScanBracket: %v", err)
		}
	}
	sort.Strings(visited)
	if got := strings.Join(visited, ","); got != "expired-date,expired-secs" {
		t.Fatalf("the TTL brackets visited %v, want [expired-date expired-secs]", visited)
	}
}

// TestTTLScanReturnsTwoBracketsAlways: a date bracket and a number bracket,
// whatever the expiry — never one walk, never fewer.
func TestTTLScanReturnsTwoBracketsAlways(t *testing.T) {
	for _, expireAfter := range []time.Duration{0, time.Second, 24 * time.Hour, 100 * 365 * 24 * time.Hour} {
		brackets := TTLBounds(ttlFixtureNow, expireAfter)
		if len(brackets) != 2 || brackets[0].Tag != tagDateTime || brackets[1].Tag != tagNumber {
			t.Errorf("expire after %v: brackets %+v, want exactly [date, number]", expireAfter, brackets)
		}
	}
}
