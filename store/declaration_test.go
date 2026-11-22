package store

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

const (
	personSlot     uint16 = 402 // SlotOf(PlacementKey("app", "_buckets", encID("Person")))
	personDatabase uint64 = 1
)

// personTableHex is the 100-byte six-field table of conv Formats 5.
const personTableHex = "01 0006 0007" +
	"0001 03 616e79 04 5f69645f" +
	"0002 06 737472696e67 04 4e616d65" +
	"0003 03 696e74 03 416765" +
	"0004 09 6669666f53746f7265 09 507572636861736573" +
	"0005 09 6c69666f53746f7265 06 50686f746f73" +
	"0006 09 626c6f6253746f7265 04 44617461" +
	"0000"

func personTable() FieldTable {
	return FieldTable{
		NextFieldID: 7,
		Entries: []machine.FieldEntry{
			{ID: 1, Type: "any", Name: "_id_"},
			{ID: 2, Type: "string", Name: "Name"},
			{ID: 3, Type: "int", Name: "Age"},
			{ID: 4, Type: "fifoStore", Name: "Purchases"},
			{ID: 5, Type: "lifoStore", Name: "Photos"},
			{ID: 6, Type: "blobStore", Name: "Data"},
		},
	}
}

func personDeclaration() Declaration {
	return Declaration{
		BucketID: 0x1F4A2C9B7DE30015,
		Version:  7,
		State:    machine.StateActive,
		Fields:   personTable(),
	}
}

// writeDeclaration inserts decl as the document named name, the way a create does, and
// returns every key of the _buckets range with its value.
func writeDeclaration(t *testing.T, name string, decl Declaration) map[string][]byte {
	t.Helper()
	encID, err := EncodeComponent(name)
	if err != nil {
		t.Fatalf("EncodeComponent(%q): %v", name, err)
	}
	stored, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatalf("EncodeDeclaration: %v", err)
	}
	req := DocumentInsert{Ref: storage.BucketRef{Slot: personSlot, DatabaseID: personDatabase, BucketID: BucketsBucketID}, EncID: encID, Header: storedString(name)}
	for fieldID, value := range stored {
		req.Fields = append(req.Fields, DocumentField{FieldID: fieldID, Stored: value})
	}
	kv := kvfault.New()
	batch := kv.NewBatch()
	if err := WriteInsert(batch, req); err != nil {
		t.Fatalf("WriteInsert: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit: %v", err)
	}
	snap := kv.NewSnapshot()
	defer snap.Discard()
	lower := storage.Scope(personSlot, personDatabase, BucketsBucketID)
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: storage.PairPrefix(personSlot, personDatabase+1)})
	defer it.Close()
	found := map[string][]byte{}
	for it.Seek(lower); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			t.Fatalf("reading %x: %v", it.Key(), err)
		}
		found[string(it.Key())] = value
	}
	if err := it.Err(); err != nil {
		t.Fatalf("walking the range: %v", err)
	}
	return found
}

func hexBytes(t *testing.T, text string) []byte {
	t.Helper()
	return mustHexBytes(t, text)
}

func sizeOf(found map[string][]byte) int {
	total := 0
	for key, stored := range found {
		total += len(key) + len(stored)
	}
	return total
}

// TestDeclarationDocumentLayout is conv Formats 5, byte for byte: the header key is 28
// bytes, four scalar keys are 30, and the declaration is 301 bytes over 5 keys. The
// binary lengths are BSON's (the byte count without the subtype), see the build report.
func TestDeclarationDocumentLayout(t *testing.T) {
	table := hexBytes(t, personTableHex)
	if len(table) != 100 {
		t.Fatalf("the example table is %d bytes, want 100", len(table))
	}
	if got := EncodeFieldTable(personTable()); !bytes.Equal(got, table) {
		t.Fatalf("EncodeFieldTable is %x, want %x", got, table)
	}
	decoded, err := DecodeFieldTable(table)
	if err != nil || !reflect.DeepEqual(decoded, personTable()) {
		t.Fatalf("the 100-byte table decodes to %+v, %v", decoded, err)
	}

	found := writeDeclaration(t, "Person", personDeclaration())
	header := hexBytes(t, "01 92 0000000000000001 FFFFFFFFFFFFFFFF 01 50 506572736f6e 0000")
	if len(header) != 28 {
		t.Fatalf("example header key is %d bytes", len(header))
	}
	want := map[string][]byte{
		string(header): hexBytes(t, "02 07000000 506572736f6e 00"),
		string(append(bytes.Clone(header), 0, 2)): hexBytes(t, "05 08000000 00 1f4a2c9b7de30015"),
		string(append(bytes.Clone(header), 0, 3)): hexBytes(t, "12 0700000000000000"),
		string(append(bytes.Clone(header), 0, 4)): hexBytes(t, "02 07000000 616374697665 00"),
		string(append(bytes.Clone(header), 0, 5)): append(hexBytes(t, "05 64000000 00"), table...),
	}
	if len(found) != 5 {
		t.Fatalf("the declaration is %d keys, want 5", len(found))
	}
	for key, stored := range found {
		wantStored, ok := want[key]
		if !ok {
			t.Fatalf("unexpected key %x", key)
		}
		if !bytes.Equal(stored, wantStored) {
			t.Errorf("key %x holds %x, want %x", key, stored, wantStored)
		}
	}
	if got := sizeOf(found); got != 301 {
		t.Errorf("the declaration is %d bytes, want 301 (40 + 44 + 39 + 42 + 136)", got)
	}

	withPending := personDeclaration()
	withPending.Pending = &Pending{Version: 8, Table: personTable()}
	grown := writeDeclaration(t, "Person", withPending)
	if len(grown) != 6 || sizeOf(grown) != 445 {
		t.Errorf("a pending table makes %d keys and %d bytes, want 6 and 445", len(grown), sizeOf(grown))
	}
	fieldEight := grown[string(append(bytes.Clone(header), 0, 8))]
	if len(fieldEight) != 114 {
		t.Errorf("field 8 value is %d bytes, want 114", len(fieldEight))
	}

	reverseID, err := EncodeComponent([]byte{0x1f, 0x4a, 0x2c, 0x9b, 0x7d, 0xe3, 0x00, 0x15})
	if err != nil || !bytes.Equal(reverseID, hexBytes(t, "60 1F 4A 2C 9B 7D E3 00 FF 15 00 00")) {
		t.Errorf("the reverse-index encID is %x (%v), want the 12 bytes with 00 FF", reverseID, err)
	}
}

func TestFieldTableRoundTripsAtTheCap(t *testing.T) {
	table := FieldTable{NextFieldID: 1001, Retired: []RetiredID{{ID: 40, Epoch: 9}, {ID: 41, Epoch: 1 << 40}}}
	table.Entries = append(table.Entries, machine.FieldEntry{ID: 1, Type: "any", Name: "_id_"})
	for id := uint16(2); id <= MaxFieldTableEntries; id++ {
		table.Entries = append(table.Entries, machine.FieldEntry{ID: id, Type: "string", Name: strings.Repeat("n", 255)})
	}
	encoded := EncodeFieldTable(table)
	decoded, err := DecodeFieldTable(encoded)
	if err != nil {
		t.Fatalf("DecodeFieldTable at the cap: %v", err)
	}
	if !reflect.DeepEqual(decoded, table) {
		t.Fatal("a 1000-entry table did not round trip")
	}
	if len(decoded.Entries) != 1000 {
		t.Fatalf("got %d entries", len(decoded.Entries))
	}

	over := table
	over.Entries = append(append([]machine.FieldEntry(nil), table.Entries...), machine.FieldEntry{ID: 1001, Type: "int", Name: "x"})
	if _, err := EncodeDeclaration(Declaration{State: machine.StateActive, Fields: over}); err == nil {
		t.Error("EncodeDeclaration accepted 1001 entries")
	}
	if _, err := DecodeFieldTable(EncodeFieldTable(over)); !errors.Is(err, ErrMalformedDeclaration) {
		t.Errorf("DecodeFieldTable accepted 1001 entries: %v", err)
	}
}

func TestDeclarationIDsNeverCollideAcrossFamilies(t *testing.T) {
	names := []string{"Person", "", "a\x00b", "\x60", "\x00\xff"}
	ids := [][]byte{{0x1f, 0x4a, 0x2c, 0x9b, 0x7d, 0xe3, 0x00, 0x15}, {0, 0, 0, 0, 0, 0, 0, 1}, {0x50, 0x50, 0, 0, 0, 0, 0, 0}}
	var encoded [][]byte
	var families []BucketsFamily
	for _, name := range names {
		encID, err := EncodeComponent(name)
		if err != nil {
			t.Fatalf("EncodeComponent(%q): %v", name, err)
		}
		encoded, families = append(encoded, encID), append(families, BucketsDeclaration)
	}
	for _, id := range ids {
		encID, err := EncodeComponent(id)
		if err != nil {
			t.Fatalf("EncodeComponent(%x): %v", id, err)
		}
		encoded, families = append(encoded, encID), append(families, BucketsReverseIndex)
	}
	for position, encID := range encoded {
		got, err := BucketsFamilyOf(encID)
		if err != nil || got != families[position] {
			t.Errorf("BucketsFamilyOf(%x) = %v, %v; want %v", encID, got, err, families[position])
		}
		length, err := EncodedComponentLen(encID)
		if err != nil || length != len(encID) {
			t.Errorf("component %x has length %d, %v; want %d", encID, length, err, len(encID))
		}
		for other := range encoded {
			if other != position && families[other] != families[position] && bytes.HasPrefix(encoded[other], encID) {
				t.Errorf("id %x of one family is a prefix of %x of the other", encID, encoded[other])
			}
		}
	}
	if _, err := BucketsFamilyOf([]byte{0x30, 1}); err == nil {
		t.Error("a number-tagged _id_ was classified as a _buckets family")
	}
	if _, err := BucketsFamilyOf(nil); err == nil {
		t.Error("an empty encID was classified")
	}
}

func TestSystemBucketVersionIsZero(t *testing.T) {
	if BucketsBucketID != machine.SystemBucketID {
		t.Errorf("BucketsBucketID %#x != machine.SystemBucketID %#x", BucketsBucketID, machine.SystemBucketID)
	}
	if BucketsBucketDecl.Version != 0 {
		t.Errorf("the system bucket's version is %d, want 0 so that version <= schema_version always holds", BucketsBucketDecl.Version)
	}
	if BucketsBucketDecl.State != machine.StateActive {
		t.Errorf("the system bucket's state is %q", BucketsBucketDecl.State)
	}
	if !reflect.DeepEqual(BucketsBucketDecl, machine.SystemDeclaration()) {
		t.Errorf("store.BucketsBucketDecl and machine.SystemDeclaration() differ:\n%+v\n%+v", BucketsBucketDecl, machine.SystemDeclaration())
	}
	if got := len(BucketsBucketDecl.Fields.Entries); got != 8 {
		t.Errorf("the system bucket declares %d fields, want 8", got)
	}
	stored, err := EncodeDeclaration(BucketsBucketDecl)
	if err != nil {
		t.Fatalf("EncodeDeclaration(system): %v", err)
	}
	back, err := DecodeDeclaration(stored)
	if err != nil || !reflect.DeepEqual(back, BucketsBucketDecl) {
		t.Errorf("the system declaration round trips to %+v, %v", back, err)
	}
}

func TestPendingIsFieldEight(t *testing.T) {
	base := personDeclaration()
	quiet, err := EncodeDeclaration(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := quiet[8]; ok || len(quiet) != 4 {
		t.Errorf("a quiet declaration writes fields %v, want 2 to 5 only", keys(quiet))
	}

	altered := base
	altered.Pending = &Pending{Version: 8, Table: FieldTable{NextFieldID: 3, Entries: personTable().Entries[:2]}}
	stored, err := EncodeDeclaration(altered)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored[5], quiet[5]) {
		t.Error("an alter's table was folded into field 5 at encode time; it must stay inert in field 8")
	}
	wantPending := append([]byte{0, 0, 0, 0, 0, 0, 0, 8}, EncodeFieldTable(altered.Pending.Table)...)
	if !bytes.Equal(stored[8], storedBinary(wantPending)) {
		t.Errorf("field 8 holds %x, want [version:8 BE][FieldTable]", stored[8])
	}
	back, err := DecodeDeclaration(stored)
	if err != nil || !reflect.DeepEqual(back, altered) {
		t.Errorf("a pending declaration round trips to %+v, %v", back, err)
	}
}

func TestSweptExistsOnlyWhileDropping(t *testing.T) {
	for _, state := range []string{machine.StateActive, machine.StateDropping} {
		decl := personDeclaration()
		decl.State = state
		stored, err := EncodeDeclaration(decl)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := stored[6]; ok {
			t.Errorf("EncodeDeclaration wrote field 6 for a %s declaration; the drop writes it", state)
		}
		stored[6] = storedBinary(make([]byte, SweptBytes))
		back, err := DecodeDeclaration(stored)
		switch state {
		case machine.StateActive:
			if !errors.Is(err, ErrMalformedDeclaration) {
				t.Errorf("an active declaration with field 6 decoded: %+v, %v", back, err)
			}
		case machine.StateDropping:
			if err != nil || back.State != machine.StateDropping {
				t.Errorf("a dropping declaration with field 6 was refused: %v", err)
			}
		}
	}
	stored, err := EncodeDeclaration(func() Declaration { d := personDeclaration(); d.State = machine.StateDropping; return d }())
	if err != nil {
		t.Fatal(err)
	}
	stored[6] = storedBinary(make([]byte, SweptBytes-1))
	if _, err := DecodeDeclaration(stored); !errors.Is(err, ErrMalformedDeclaration) {
		t.Errorf("a 1023-byte swept bitmap was accepted: %v", err)
	}
}

func keys(stored map[uint16][]byte) []uint16 {
	var ids []uint16
	for id := range stored {
		ids = append(ids, id)
	}
	return ids
}

// TestDecodersRefuseWhatTheyCannotRead walks the declaration decoder's refusals.
func TestDecodersRefuseWhatTheyCannotRead(t *testing.T) {
	good, err := EncodeDeclaration(personDeclaration())
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(map[uint16][]byte){
		"missing bucket_id":     func(m map[uint16][]byte) { delete(m, 2) },
		"short bucket_id":       func(m map[uint16][]byte) { m[2] = storedBinary([]byte{1, 2, 3}) },
		"missing fields":        func(m map[uint16][]byte) { delete(m, 5) },
		"unknown state":         func(m map[uint16][]byte) { m[4] = storedString("gone") },
		"reverse-index field 7": func(m map[uint16][]byte) { m[7] = storedString("x") },
		"negative version":      func(m map[uint16][]byte) { m[3] = storedInt64(-1) },
		"string for int":        func(m map[uint16][]byte) { m[3] = storedString("7") },
		"truncated pending":     func(m map[uint16][]byte) { m[8] = storedBinary([]byte{1, 2}) },
	}
	for name, mutate := range cases {
		stored := map[uint16][]byte{}
		for id, stored2 := range good {
			stored[id] = bytes.Clone(stored2)
		}
		mutate(stored)
		if _, err := DecodeDeclaration(stored); !errors.Is(err, ErrMalformedDeclaration) {
			t.Errorf("%s: err = %v, want ErrMalformedDeclaration", name, err)
		}
	}
}

// TestDecodeFieldTableSurvivesMutation: 100,000 mutations of valid tables never panic,
// and whatever is accepted re-encodes to the same bytes.
func TestDecodeFieldTableSurvivesMutation(t *testing.T) {
	rng := rand.New(rand.NewSource(37))
	seeds := [][]byte{
		EncodeFieldTable(personTable()),
		EncodeFieldTable(FieldTable{NextFieldID: 3, Entries: personTable().Entries[:2], Retired: []RetiredID{{ID: 9, Epoch: 4}}}),
		EncodeFieldTable(FieldTable{}),
	}
	accepted := 0
	for round := 0; round < 100_000; round++ {
		input := bytes.Clone(seeds[rng.Intn(len(seeds))])
		for edits := rng.Intn(4) + 1; edits > 0; edits-- {
			switch rng.Intn(4) {
			case 0:
				if len(input) > 0 {
					input[rng.Intn(len(input))] = byte(rng.Intn(256))
				}
			case 1:
				input = input[:rng.Intn(len(input)+1)]
			case 2:
				input = append(input, byte(rng.Intn(256)))
			default:
				at := rng.Intn(len(input) + 1)
				input = append(input[:at], append([]byte{byte(rng.Intn(256))}, input[at:]...)...)
			}
		}
		table, err := DecodeFieldTable(input)
		if err != nil {
			continue
		}
		accepted++
		if again := EncodeFieldTable(table); !bytes.Equal(again, input) {
			t.Fatalf("accepted %x re-encodes to %x", input, again)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutation was ever accepted; the test mutates too hard to prove the re-encode rule")
	}
}

func droppingDeclaration() Declaration {
	decl := personDeclaration()
	decl.State = machine.StateDropping
	return decl
}

func sweptPattern() []byte {
	swept := make([]byte, SweptBytes)
	for position := range swept {
		swept[position] = byte(position*7 + 1)
	}
	return swept
}

func TestSweptSurvivesAReadModifyWrite(t *testing.T) {
	stored, err := EncodeDeclaration(droppingDeclaration())
	if err != nil {
		t.Fatal(err)
	}
	stored[6] = storedBinary(sweptPattern())

	decl, swept, err := DecodeDeclarationWithSwept(stored)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(swept, sweptPattern()) {
		t.Fatal("DecodeDeclarationWithSwept lost the swept bitmap")
	}
	decl.Version++
	again, err := EncodeDeclarationWithSwept(decl, swept)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again[6], stored[6]) {
		t.Errorf("field 6 changed across a read-modify-write: %d bytes became %d", len(stored[6]), len(again[6]))
	}

	plain, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plain[6]; ok {
		t.Error("EncodeDeclaration wrote field 6; only the WithSwept pair may")
	}
	_, swept, err = DecodeDeclarationWithSwept(plain)
	if err != nil || swept != nil {
		t.Errorf("a declaration without field 6 gave swept %v, err %v; want nil, nil", swept, err)
	}
}

func TestSweptRefusedOnAnActiveDeclaration(t *testing.T) {
	_, err := EncodeDeclarationWithSwept(personDeclaration(), sweptPattern())
	if !errors.Is(err, ErrMalformedDeclaration) {
		t.Errorf("an active declaration took a swept bitmap: %v", err)
	}
}

func TestSweptMustBe1024Bytes(t *testing.T) {
	for _, size := range []int{0, 1, SweptBytes - 1, SweptBytes + 1} {
		_, err := EncodeDeclarationWithSwept(droppingDeclaration(), make([]byte, size))
		if !errors.Is(err, ErrMalformedDeclaration) {
			t.Errorf("a %d-byte swept bitmap was accepted: %v", size, err)
		}
	}
}

func TestDeclarationWithSweptSurvivesMutation(t *testing.T) {
	rng := rand.New(rand.NewSource(38))
	var seeds []map[uint16][]byte
	for _, decl := range []Declaration{personDeclaration(), droppingDeclaration()} {
		stored, err := EncodeDeclaration(decl)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, stored)
		if decl.State == machine.StateDropping {
			withSwept := droppingSeedWithSwept(t, decl)
			seeds = append(seeds, withSwept)
		}
	}
	accepted := 0
	for round := 0; round < 100_000; round++ {
		input := make(map[uint16][]byte)
		for id, raw := range seeds[rng.Intn(len(seeds))] {
			input[id] = bytes.Clone(raw)
		}
		for edits := rng.Intn(3) + 1; edits > 0; edits-- {
			id := uint16(rng.Intn(8) + 1)
			raw, ok := input[id]
			switch {
			case !ok:
				input[id] = []byte{byte(rng.Intn(256)), byte(rng.Intn(256))}
			case rng.Intn(5) == 0:
				delete(input, id)
			case len(raw) > 0 && rng.Intn(2) == 0:
				raw[rng.Intn(len(raw))] = byte(rng.Intn(256))
			default:
				input[id] = raw[:rng.Intn(len(raw)+1)]
			}
		}
		decl, swept, err := DecodeDeclarationWithSwept(input)
		if err != nil {
			continue
		}
		accepted++
		again, err := EncodeDeclarationWithSwept(decl, swept)
		if err != nil {
			t.Fatalf("accepted %v but could not re-encode: %v", keys(input), err)
		}
		if len(again) != len(input) {
			t.Fatalf("key set %v became %v", keys(input), keys(again))
		}
		for id, raw := range input {
			if !bytes.Equal(again[id], raw) {
				t.Fatalf("field %d: %x re-encodes to %x", id, raw, again[id])
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no mutation was ever accepted")
	}
}

// droppingSeedWithSwept builds a dropping seed that carries field 6.
func droppingSeedWithSwept(t *testing.T, decl Declaration) map[uint16][]byte {
	t.Helper()
	stored, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatal(err)
	}
	stored[6] = storedBinary(sweptPattern())
	return stored
}

// TestDecodeDeclarationRefusesEverySingleByteCorruption walks the canonical 301-byte
// example (conv Formats 5) and corrupts one thing at a time. Each corruption is bytes the
// encoder would never write, so accepting any of them installs a document that a later
// rewrite silently changes.
func TestDecodeDeclarationRefusesEverySingleByteCorruption(t *testing.T) {
	good, err := EncodeDeclaration(personDeclaration())
	if err != nil {
		t.Fatal(err)
	}
	// The packed table starts after the 6-byte binary envelope of field 5; entry 1 is
	// [id:2][typeLen:1]["any"][nameLen:1]["_id_"] after the 5-byte table header.
	const entryOne = 6 + 5
	cases := map[string]struct {
		field  uint16
		offset int
		xor    byte
	}{
		"bucket_id length prefix": {2, 1, 0x01},
		"bucket_id subtype":       {2, 5, 0x80},
		"bucket_id type byte":     {2, 0, 0x01},
		"state length prefix":     {4, 1, 0x01},
		"state terminator":        {4, len(good[4]) - 1, 0x01},
		"fields length prefix":    {5, 1, 0x01},
		"fields length high byte": {5, 4, 0x01},
		"fields subtype":          {5, 5, 0x01},
		"entry 1 id":              {5, entryOne, 0x01},
		"entry 1 type word":       {5, entryOne + 3, 0x01},
		"entry 1 name first byte": {5, entryOne + 3 + 3 + 1, 0x20},
		"entry 1 name last byte":  {5, entryOne + 3 + 3 + 1 + 3, 0x01},
		"entry 1 name case":       {5, entryOne + 3 + 3 + 1 + 1, 0x20},
	}
	for name, corruption := range cases {
		stored := map[uint16][]byte{}
		for id, raw := range good {
			stored[id] = bytes.Clone(raw)
		}
		stored[corruption.field][corruption.offset] ^= corruption.xor
		if _, err := DecodeDeclaration(stored); !errors.Is(err, ErrMalformedDeclaration) {
			t.Errorf("%s: accepted (err = %v)", name, err)
		}
	}
	for _, field := range []uint16{2, 3, 4, 5} {
		stored := map[uint16][]byte{}
		for id, raw := range good {
			stored[id] = bytes.Clone(raw)
		}
		stored[field] = append(stored[field], 0)
		if _, err := DecodeDeclaration(stored); !errors.Is(err, ErrMalformedDeclaration) {
			t.Errorf("trailing byte on field %d: accepted (err = %v)", field, err)
		}
	}
	table := EncodeFieldTable(personTable())
	if _, err := DecodeFieldTable(append(table, 0)); !errors.Is(err, ErrMalformedDeclaration) {
		t.Errorf("a trailing byte after the table was accepted: %v", err)
	}
	renamed := bytes.Clone(table)
	renamed[5+3+3+1+1] = 'x'
	if _, err := DecodeFieldTable(renamed); !errors.Is(err, ErrMalformedDeclaration) {
		t.Errorf("a table whose entry 1 is not _id_ was accepted: %v", err)
	}
}

// TestDecodeDeclarationAcceptedInputIsCanonical mutates the seeds 200,000 times, in the
// bytes of each value rather than the map shape, and requires that whatever decodes
// re-encodes to the identical values.
func TestDecodeDeclarationAcceptedInputIsCanonical(t *testing.T) {
	rng := rand.New(rand.NewSource(39))
	seeds := []map[uint16][]byte{}
	for _, decl := range []Declaration{personDeclaration(), droppingDeclaration()} {
		stored, err := EncodeDeclaration(decl)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, stored)
	}
	seeds = append(seeds, droppingSeedWithSwept(t, droppingDeclaration()))
	pendingDecl := personDeclaration()
	pendingDecl.Pending = &Pending{Version: 8, Table: personTable()}
	pendingStored, err := EncodeDeclaration(pendingDecl)
	if err != nil {
		t.Fatal(err)
	}
	seeds = append(seeds, pendingStored)
	accepted := 0
	for round := 0; round < 200_000; round++ {
		input := map[uint16][]byte{}
		for id, raw := range seeds[rng.Intn(len(seeds))] {
			input[id] = bytes.Clone(raw)
		}
		for edits := rng.Intn(3) + 1; edits > 0; edits-- {
			id := uint16(rng.Intn(4) + 2)
			raw, ok := input[id]
			if !ok || len(raw) == 0 {
				continue
			}
			switch rng.Intn(3) {
			case 0:
				raw[rng.Intn(len(raw))] ^= byte(1 << rng.Intn(8))
			case 1:
				input[id] = append(raw, byte(rng.Intn(256)))
			default:
				input[id] = raw[:rng.Intn(len(raw)+1)]
			}
		}
		decl, swept, err := DecodeDeclarationWithSwept(input)
		if err != nil {
			continue
		}
		accepted++
		again, err := EncodeDeclarationWithSwept(decl, swept)
		if err != nil {
			t.Fatalf("accepted but could not re-encode: %v", err)
		}
		if len(again) != len(input) {
			t.Fatalf("key set %v became %v", keys(input), keys(again))
		}
		for id, raw := range input {
			if !bytes.Equal(again[id], raw) {
				t.Fatalf("field %d: %x re-encodes to %x", id, raw, again[id])
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no mutation was ever accepted")
	}
}

// indexFields is a table with one field of each indexable word and one of a store word.
func indexFields() FieldTable {
	return FieldTable{
		NextFieldID: 9,
		Entries: []machine.FieldEntry{
			{ID: 1, Type: "any", Name: "_id_"},
			{ID: 2, Type: "int", Name: "A"},
			{ID: 3, Type: "string", Name: "B"},
			{ID: 4, Type: "datetime", Name: "C"},
			{ID: 5, Type: "double", Name: "D"},
			{ID: 6, Type: "bool", Name: "E"},
			{ID: 7, Type: "fifoStore", Name: "F"},
			{ID: 8, Type: "decimal", Name: "G"},
		},
	}
}

// TestIndexTableRoundTrips pins the byte layout of field 10: the id is two bytes before
// the direction, and an entry's state is its last byte.
func TestIndexTableRoundTrips(t *testing.T) {
	table := machine.IndexTable{NextIndexID: 3, Entries: []machine.IndexEntry{
		{ID: 1, Name: "ByChannelTime", Components: []machine.IndexComponent{{FieldID: 2}, {FieldID: 5, Desc: true}}},
		{ID: 2, Name: "ByFacility", Components: []machine.IndexComponent{{FieldID: 3}, {FieldID: 4}, {FieldID: 5}, {FieldID: 6}}, State: 1},
	}}
	encoded := EncodeIndexTable(table)
	if want := 1 + 1 + (1 + 1 + 13 + 1 + 2*3 + 1) + (1 + 1 + 10 + 1 + 4*3 + 1) + 1; len(encoded) != want {
		t.Fatalf("encoded length %d, want %d", len(encoded), want)
	}
	decoded, err := DecodeIndexTable(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, table) {
		t.Fatalf("round trip\n got %+v\nwant %+v", decoded, table)
	}
}

func TestDeclarationWithoutField10DecodesToAnEmptyTable(t *testing.T) {
	stored, err := EncodeDeclaration(personDeclaration())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stored[10]; ok {
		t.Fatal("a declaration with no ordered index wrote field 10")
	}
	decl, err := DecodeDeclaration(stored)
	if err != nil {
		t.Fatalf("a declaration without field 10 is refused: %v", err)
	}
	if !reflect.DeepEqual(decl.Indexes, machine.IndexTable{}) {
		t.Fatalf("Indexes = %+v, want the zero table", decl.Indexes)
	}
}

func TestIndexTableRefusesFiveComponentsAndDuplicateNames(t *testing.T) {
	four := machine.IndexTable{NextIndexID: 2, Entries: []machine.IndexEntry{
		{ID: 1, Name: "Wide", Components: []machine.IndexComponent{{FieldID: 2}, {FieldID: 3}, {FieldID: 4}, {FieldID: 5}}},
	}}
	if _, err := DecodeIndexTable(EncodeIndexTable(four)); err != nil {
		t.Fatalf("a four-component index is refused: %v", err)
	}
	five := four
	five.Entries = []machine.IndexEntry{{ID: 1, Name: "Wide", Components: append(four.Entries[0].Components, machine.IndexComponent{FieldID: 6})}}
	if _, err := DecodeIndexTable(EncodeIndexTable(five)); err == nil {
		t.Fatal("a five-component index decoded")
	}
	twoNames := machine.IndexTable{NextIndexID: 3, Entries: []machine.IndexEntry{
		{ID: 1, Name: "Same", Components: []machine.IndexComponent{{FieldID: 2}}},
		{ID: 2, Name: "Same", Components: []machine.IndexComponent{{FieldID: 3}}},
	}}
	if _, err := DecodeIndexTable(EncodeIndexTable(twoNames)); err == nil {
		t.Fatal("two indexes with one name decoded")
	}
	twoIDs := twoNames
	twoIDs.Entries = []machine.IndexEntry{
		{ID: 1, Name: "One", Components: []machine.IndexComponent{{FieldID: 2}}},
		{ID: 1, Name: "Two", Components: []machine.IndexComponent{{FieldID: 3}}},
	}
	if _, err := DecodeIndexTable(EncodeIndexTable(twoIDs)); err == nil {
		t.Fatal("two indexes with one id decoded")
	}
}

func TestIndexNameMayNotEqualAFieldName(t *testing.T) {
	table := machine.IndexTable{NextIndexID: 2, Entries: []machine.IndexEntry{
		{ID: 1, Name: "A", Components: []machine.IndexComponent{{FieldID: 3}}},
	}}
	if err := checkIndexTable(table, indexFields()); err == nil {
		t.Fatal("an index named like field A was accepted")
	}
	table.Entries[0].Name = "ByA"
	if err := checkIndexTable(table, indexFields()); err != nil {
		t.Fatalf("index ByA: %v", err)
	}
}

// TestIndexComponentsMustBeIndexable: the type words of index spec 1.2 are the only ones a
// component may name; the store words, decimal and the _id_ field are refused.
func TestIndexComponentsMustBeIndexable(t *testing.T) {
	for _, fieldID := range []uint16{1, 7, 8} {
		table := machine.IndexTable{NextIndexID: 2, Entries: []machine.IndexEntry{
			{ID: 1, Name: "X", Components: []machine.IndexComponent{{FieldID: fieldID}}},
		}}
		if err := checkIndexTable(table, indexFields()); err == nil {
			t.Errorf("field %d is indexable, want refused", fieldID)
		}
	}
}

func TestAnEmptyIndexTableWritesNoField10(t *testing.T) {
	cases := map[string]machine.IndexTable{
		"zero table":        {},
		"next id 1, empty":  {NextIndexID: 1},
		"next id 2, empty":  {NextIndexID: 2},
		"one ordered index": {NextIndexID: 2, Entries: []machine.IndexEntry{{ID: 1, Name: "ByA", Components: []machine.IndexComponent{{FieldID: 2}}}}},
	}
	for name, table := range cases {
		decl := personDeclaration()
		decl.Indexes = table
		stored, err := EncodeDeclaration(decl)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		_, has := stored[10]
		wantField := name == "next id 2, empty" || name == "one ordered index"
		if has != wantField {
			t.Errorf("%s: field 10 present = %v, want %v", name, has, wantField)
		}
	}
}

// TestExpireTableRoundTrips (expiry spec §2.2, build E1a): the store form of an
// expiring bucket is a fixed layout that E2b reads back, so a round trip and the
// exact byte count both hold.
func TestExpireTableRoundTrips(t *testing.T) {
	table := machine.ExpireTable{
		BucketField: 4,
		Stores:      []machine.ExpireStore{{FieldID: 7, Path: "ts"}, {FieldID: 9, Path: "meta.at"}},
	}
	encoded := EncodeExpireTable(table)
	if want := 1 + 2 + 1 + (2 + 1 + 2) + (2 + 1 + 7); len(encoded) != want {
		t.Fatalf("encoded table is %d bytes, want %d", len(encoded), want)
	}
	decoded, err := DecodeExpireTable(encoded)
	if err != nil {
		t.Fatalf("DecodeExpireTable: %v", err)
	}
	if !reflect.DeepEqual(decoded, table) {
		t.Fatalf("round trip = %+v, want %+v", decoded, table)
	}

	decl := personDeclaration()
	decl.Expire = table
	stored, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatalf("EncodeDeclaration with an expire table: %v", err)
	}
	back, err := DecodeDeclaration(stored)
	if err != nil {
		t.Fatalf("DecodeDeclaration of an expiring declaration: %v", err)
	}
	if !reflect.DeepEqual(back.Expire, table) {
		t.Fatalf("declaration round trip Expire = %+v, want %+v", back.Expire, table)
	}
}

// TestDeclarationWithoutField11DecodesToAnEmptyTable: every declaration written
// before expiry exists has no field 11, and it must still decode.
func TestDeclarationWithoutField11DecodesToAnEmptyTable(t *testing.T) {
	stored, err := EncodeDeclaration(personDeclaration())
	if err != nil {
		t.Fatalf("EncodeDeclaration: %v", err)
	}
	if _, present := stored[declFieldExpire]; present {
		t.Fatal("a declaration with no expiry wrote field 11")
	}
	decl, err := DecodeDeclaration(stored)
	if err != nil {
		t.Fatalf("DecodeDeclaration without field 11: %v", err)
	}
	if !reflect.DeepEqual(decl.Expire, machine.ExpireTable{}) {
		t.Fatalf("absent field 11 decoded to %+v, want the zero table", decl.Expire)
	}
}

// TestAnEmptyExpireTableWritesNoField11: the 2 B per-bucket figure in the cost
// statement holds only while an empty table writes nothing.
func TestAnEmptyExpireTableWritesNoField11(t *testing.T) {
	plain, err := EncodeDeclaration(personDeclaration())
	if err != nil {
		t.Fatalf("EncodeDeclaration: %v", err)
	}
	decl := personDeclaration()
	decl.Expire = machine.ExpireTable{}
	empty, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatalf("EncodeDeclaration with an empty table: %v", err)
	}
	if _, present := empty[declFieldExpire]; present || len(empty) != len(plain) {
		t.Fatalf("an empty expire table wrote field 11 or changed the document: %d fields, want %d", len(empty), len(plain))
	}
}

func TestAPendingExpireSectionRoundTripsAndAnAbsentOneStaysAbsent(t *testing.T) {
	withSection := machine.ExpireTable{BucketField: 4, Stores: []machine.ExpireStore{{FieldID: 7, Path: "ts"}}}
	cleared := machine.ExpireTable{}
	for name, section := range map[string]*machine.ExpireTable{"set": &withSection, "cleared": &cleared, "absent": nil} {
		decl := machine.Declaration{BucketID: 9, State: machine.StateActive, Fields: machine.SystemDeclaration().Fields,
			Pending: &machine.Pending{Version: 2, Table: machine.SystemDeclaration().Fields, Expire: section}}
		encoded, err := EncodeDeclaration(decl)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		decoded, err := DecodeDeclaration(encoded)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(decoded.Pending.Expire, section) {
			t.Fatalf("%s: pending expire = %+v, want %+v", name, decoded.Pending.Expire, section)
		}
	}
}

// TestThePendingExpireSectionBytesAreFixed pins field 8 of a pending with an expire section:
// [version:8][0xE1][fieldTableLen:4][field table][expire table]. A change here is an on-disk
// declaration format change.
func TestThePendingExpireSectionBytesAreFixed(t *testing.T) {
	fields := machine.FieldTable{NextFieldID: 3, Entries: []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "datetime", Name: "At"}}}
	expire := machine.ExpireTable{BucketField: 2, Stores: []machine.ExpireStore{{FieldID: 5, Path: "ts"}}}
	decl := machine.Declaration{BucketID: 9, State: machine.StateActive, Fields: fields, Pending: &machine.Pending{Version: 2, Table: fields, Expire: &expire}}
	encoded, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatal(err)
	}
	table := EncodeFieldTable(fields)
	want := binary.BigEndian.AppendUint64(nil, 2)
	want = append(want, 0xE1)
	want = binary.BigEndian.AppendUint32(want, uint32(len(table)))
	want = append(want, table...)
	want = append(want, 1, 0, 2, 1, 0, 5, 2, 't', 's') // expire table: format 1, bucket field 2, one store: field 5, path "ts"
	if !bytes.Contains(encoded[declFieldPending], want) {
		t.Fatalf("field 8 does not carry the pending section\n got %x\nwant it to contain %x", encoded[declFieldPending], want)
	}
	if got := hex.EncodeToString(EncodeExpireTable(expire)); got != "010002010005027473" {
		t.Fatalf("expire table bytes = %s, want 010002010005027473", got)
	}
}

// A malformed field 8 must be refused, never panic: the table length of the expire section is
// checked against the bytes that follow it.
func TestAMalformedPendingExpireSectionIsRefusedNotPanicked(t *testing.T) {
	good := machine.SystemDeclaration().Fields
	expire := machine.ExpireTable{BucketField: 2}
	base, err := EncodeDeclaration(machine.Declaration{BucketID: 9, State: machine.StateActive, Fields: good,
		Pending: &machine.Pending{Version: 2, Table: good, Expire: &expire}})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := requireBinary(base, declFieldPending)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"a table length past the bytes": func(raw []byte) []byte {
			out := append([]byte(nil), raw...)
			binary.BigEndian.PutUint32(out[pendingHeader+1:], 0xFFFFFFF0)
			return out
		},
		"a section cut after the marker":        func(raw []byte) []byte { return append([]byte(nil), raw[:pendingHeader+3]...) },
		"trailing bytes after the expire table": func(raw []byte) []byte { return append(append([]byte(nil), raw...), 0xFF) },
	} {
		stored := map[uint16][]byte{}
		for id, value := range base {
			stored[id] = value
		}
		stored[declFieldPending] = storedBinary(mutate(pending))
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Errorf("%s: DecodeDeclaration panicked: %v", name, recovered)
				}
			}()
			if _, err := decodePending(stored); err == nil {
				t.Errorf("%s: accepted", name)
			}
		}()
	}
}

// TestReferencesRoundTripInTheDeclaration: [GUARD field 13 carries the references and absent means none].
func TestReferencesRoundTripInTheDeclaration(t *testing.T) {
	decl := machine.SystemDeclaration()
	decl.BucketID = 77
	decl.Fields = machine.FieldTable{NextFieldID: 4, Entries: []machine.FieldEntry{
		{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Author"}, {ID: 3, Type: "hashStore", Name: "Members"}}}
	plain, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatal(err)
	}
	if _, has := plain[13]; has {
		t.Fatal("a declaration without references wrote field 13")
	}
	decl.References = []machine.Reference{{FieldID: 2, BucketID: 0x0102030405060708}, {FieldID: 3, BucketID: 9}}
	stored, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDeclaration(stored)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.References, decl.References) {
		t.Fatalf("references = %+v, want %+v", got.References, decl.References)
	}
	if bucket, ok := got.ReferenceOf(3); !ok || bucket != 9 {
		t.Fatalf("ReferenceOf(3) = %d, %v", bucket, ok)
	}
	if _, ok := got.ReferenceOf(4); ok {
		t.Fatal("ReferenceOf names a field that refers to nothing")
	}
	if back, _ := DecodeDeclaration(plain); len(back.References) != 0 {
		t.Fatal("no field 13 decoded to references")
	}
}

// TestReferencesRefuseWhatHasNoEncoding: [GUARD one list, one encoding].
func TestReferencesRefuseWhatHasNoEncoding(t *testing.T) {
	good := EncodeReferences([]machine.Reference{{FieldID: 2, BucketID: 5}, {FieldID: 3, BucketID: 6}})
	cases := map[string][]byte{
		"empty":       {},
		"zero count":  {0},
		"truncated":   good[:len(good)-1],
		"trailing":    append(append([]byte{}, good...), 0),
		"zero field":  EncodeReferences([]machine.Reference{{FieldID: 0, BucketID: 5}}),
		"id field":    EncodeReferences([]machine.Reference{{FieldID: 1, BucketID: 5}}),
		"zero bucket": EncodeReferences([]machine.Reference{{FieldID: 2, BucketID: 0}}),
		"descending":  EncodeReferences([]machine.Reference{{FieldID: 3, BucketID: 5}, {FieldID: 2, BucketID: 6}}),
		"duplicate":   EncodeReferences([]machine.Reference{{FieldID: 2, BucketID: 5}, {FieldID: 2, BucketID: 6}}),
	}
	for name, encoded := range cases {
		if _, err := DecodeReferences(encoded); err == nil {
			t.Errorf("%s: %x decoded", name, encoded)
		}
	}
	if _, err := DecodeReferences(good); err != nil {
		t.Fatalf("a good list is refused: %v", err)
	}
}

// TestIdleReferencesAddNoResidentBytes: [GUARD rule 11: a bucket with no reference carries no
// reference bytes on disk or on the heap beyond the slice header every cached declaration has
// (24 B, by the type); a reference costs 10 B on disk and 16 B on the heap].
func TestIdleReferencesAddNoResidentBytes(t *testing.T) {
	decl := machine.SystemDeclaration()
	decl.BucketID = 78
	decl.Fields = machine.FieldTable{NextFieldID: 3, Entries: []machine.FieldEntry{
		{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Name"}}}
	stored, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatal(err)
	}
	if _, has := stored[declFieldReferences]; has {
		t.Fatal("a bucket with no reference stores field 13")
	}
	got, err := DecodeDeclaration(stored)
	if err != nil {
		t.Fatal(err)
	}
	if got.References != nil || cap(got.References) != 0 {
		t.Fatalf("a decoded bucket with no reference holds a reference array of capacity %d", cap(got.References))
	}
	if allocs := testing.AllocsPerRun(100, func() { got.ReferenceOf(2) }); allocs != 0 {
		t.Fatalf("ReferenceOf allocated %v times", allocs)
	}
	if size := unsafe.Sizeof(got.References); size != 24 {
		t.Fatalf("the per-declaration cost is the slice header, %d B, and the CHANGELOG and the spec say 24", size)
	}
	if size := unsafe.Sizeof(machine.Reference{}); size != 16 {
		t.Fatalf("a reference is %d B on the heap, and the spec says 16", size)
	}
	if size := len(EncodeReferences([]machine.Reference{{FieldID: 2, BucketID: 1}})); size != 1+10 {
		t.Fatalf("one reference is %d B on disk, and the spec says 10 and a count byte", size)
	}
}
