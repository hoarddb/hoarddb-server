package store

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// aggregateDeclaration is a bucket with one btreeStore (field id 3) and one fifoStore
// (field id 4) that declare aggregates.
func aggregateDeclaration() Declaration {
	decl := Declaration{
		BucketID: 0x1F4A2C9B7DE30015,
		Version:  7,
		State:    machine.StateActive,
		Fields: FieldTable{
			NextFieldID: 5,
			Entries: []machine.FieldEntry{
				{ID: 1, Type: "any", Name: "_id_"},
				{ID: 2, Type: "string", Name: "Name"},
				{ID: 3, Type: "btreeStore", Name: "Expenses"},
				{ID: 4, Type: "fifoStore", Name: "Tickets"},
			},
		},
	}
	decl.Aggregates = machine.AggregateTable{Sets: []machine.AggregateSet{
		{FieldID: 3, NextID: 4, Entries: []machine.AggregateEntry{
			{ID: 1, Kind: machine.AggSum, Field: "Amount"},
			{ID: 2, Kind: machine.AggSumBy, Field: "Amount", By: "Currency"},
			{ID: 3, Kind: machine.AggCountBy, By: "Category"},
		}},
		{FieldID: 4, NextID: 2, Entries: []machine.AggregateEntry{
			{ID: 1, Kind: machine.AggCountBy, By: "Status"},
		}},
	}}
	return decl
}

// TestAggregateDeclarationRoundTrips: field 12 survives encode and decode, and the
// id, kind and state bytes keep their order (the apply derives keys from the kind).
func TestAggregateDeclarationRoundTrips(t *testing.T) {
	decl := aggregateDeclaration()
	stored, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatalf("EncodeDeclaration: %v", err)
	}
	back, err := DecodeDeclaration(stored)
	if err != nil {
		t.Fatalf("DecodeDeclaration: %v", err)
	}
	if !reflect.DeepEqual(back.Aggregates, decl.Aggregates) {
		t.Fatalf("round trip = %+v, want %+v", back.Aggregates, decl.Aggregates)
	}
	encoded := EncodeAggregateTable(decl.Aggregates)
	// [v][nsets] then set 3: [id:2][next][n] and its first entry [id][kind][state][fLen]Amount[bLen]
	if encoded[3] != 3 || encoded[4] != 4 || encoded[5] != 3 || encoded[6] != 1 || encoded[7] != machine.AggSum || encoded[8] != machine.AggStateReady {
		t.Fatalf("layout is %v, want id then kind then state", encoded[:10])
	}
}

// TestATableWithoutAggregatesEncodesAsBefore: a bucket with no aggregate writes no
// field 12, so every existing declaration keeps its bytes.
func TestATableWithoutAggregatesEncodesAsBefore(t *testing.T) {
	stored, err := EncodeDeclaration(personDeclaration())
	if err != nil {
		t.Fatalf("EncodeDeclaration: %v", err)
	}
	if _, present := stored[declFieldAggregates]; present {
		t.Fatal("a declaration with no aggregates wrote field 12")
	}
	decl, err := DecodeDeclaration(stored)
	if err != nil {
		t.Fatalf("DecodeDeclaration: %v", err)
	}
	if len(decl.Aggregates.Sets) != 0 {
		t.Fatalf("absent field 12 decoded to %+v", decl.Aggregates)
	}
}

// TestAggregateListRefusesNineEntriesAndDuplicates: the cap and the duplicate rule hold
// on encode and on decode, so no copy ever reads a list the owner could not write.
func TestAggregateListRefusesNineEntriesAndDuplicates(t *testing.T) {
	cases := map[string]func(*machine.AggregateSet){
		"nine entries": func(set *machine.AggregateSet) {
			set.Entries = nil
			for id := uint8(1); id <= 9; id++ {
				set.Entries = append(set.Entries, machine.AggregateEntry{ID: id, Kind: machine.AggSum, Field: string(rune('a' + id))})
			}
			set.NextID = 10
		},
		"duplicate": func(set *machine.AggregateSet) {
			set.Entries = append(set.Entries, machine.AggregateEntry{ID: 4, Kind: machine.AggSum, Field: "Amount"})
			set.NextID = 5
		},
		"id at next":        func(set *machine.AggregateSet) { set.NextID = 3 },
		"zero id":           func(set *machine.AggregateSet) { set.Entries[0].ID = 0 },
		"by on a sum":       func(set *machine.AggregateSet) { set.Entries[0].By = "Currency" },
		"sum by without by": func(set *machine.AggregateSet) { set.Entries[1].By = "" },
		"count with field":  func(set *machine.AggregateSet) { set.Entries[2].Field = "Amount" },
		"deep path":         func(set *machine.AggregateSet) { set.Entries[0].Field = "a.b.c.d" },
		"empty segment":     func(set *machine.AggregateSet) { set.Entries[0].Field = "a..b" },
		"unknown kind":      func(set *machine.AggregateSet) { set.Entries[0].Kind = 4 },
		"unknown state":     func(set *machine.AggregateSet) { set.Entries[0].State = 2 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			decl := aggregateDeclaration()
			mutate(&decl.Aggregates.Sets[0])
			if _, err := EncodeDeclaration(decl); !errors.Is(err, ErrMalformedDeclaration) {
				t.Fatalf("EncodeDeclaration = %v, want a malformed declaration", err)
			}
		})
	}
	t.Run("a scalar field", func(t *testing.T) {
		decl := aggregateDeclaration()
		decl.Aggregates.Sets[0].FieldID = 2
		if _, err := EncodeDeclaration(decl); !errors.Is(err, ErrMalformedDeclaration) {
			t.Fatalf("EncodeDeclaration = %v, want a refusal of an aggregate on a scalar", err)
		}
	})
	t.Run("trailing bytes", func(t *testing.T) {
		encoded := append(EncodeAggregateTable(aggregateDeclaration().Aggregates), 0)
		if _, err := DecodeAggregateTable(encoded); !errors.Is(err, ErrMalformedDeclaration) {
			t.Fatalf("DecodeAggregateTable = %v, want trailing bytes refused", err)
		}
	})
}

// TestIdleAggregatesCostOnlyDeclarationBytes: an aggregate on a bucket with no elements costs
// the bytes of its declaration entry and nothing else: it adds only field 12, whose size is a 2-byte
// header, a 4-byte header per store field and 4 + fLen + 1 + bLen bytes per aggregate (the id, kind, state
// and field length, the field path, the by length, the by path). It writes no key under any store. The
// resident cost of a cached declaration is stated in spec section 9, not measured here.
func TestIdleAggregatesCostOnlyDeclarationBytes(t *testing.T) {
	plain, err := EncodeDeclaration(personDeclaration())
	if err != nil {
		t.Fatal(err)
	}
	decl := personDeclaration()
	decl.Aggregates = machine.AggregateTable{Sets: []machine.AggregateSet{{FieldID: 4, NextID: 3, Entries: []machine.AggregateEntry{
		{ID: 1, Kind: machine.AggSum, Field: "Amount"},
		{ID: 2, Kind: machine.AggSumBy, Field: "Amount", By: "Currency"},
	}}}}
	withAggregates, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatal(err)
	}
	if len(withAggregates) != len(plain)+1 {
		t.Fatalf("aggregates added %d stored fields, want exactly field 12", len(withAggregates)-len(plain))
	}
	table := EncodeAggregateTable(decl.Aggregates)
	want := 2 + 4 + (4 + len("Amount") + 1) + (4 + len("Amount") + 1 + len("Currency"))
	if len(table) != want {
		t.Fatalf("field 12 is %d bytes, want %d: a 2-byte header, a 4-byte set header and 4 + paths + 1 per aggregate", len(table), want)
	}
	if got := len(withAggregates[declFieldAggregates]) - 6; got != len(table) { // the BSON binary envelope is 6 bytes
		t.Fatalf("the stored value is %d bytes of table, want %d", got, len(table))
	}
}

// TestADeclarationWithASetOnARetiredFieldStillDecodes: a set on a field an alter retired is
// accepted, so a declaration written between a drop and its fold never becomes unreadable.
func TestADeclarationWithASetOnARetiredFieldStillDecodes(t *testing.T) {
	decl := aggregateDeclaration()
	decl.Fields.Entries = decl.Fields.Entries[:3] // field 4 (Tickets) leaves the table
	decl.Fields.Retired = []machine.RetiredID{{ID: 4, Epoch: 3}}
	stored, err := EncodeDeclaration(decl)
	if err != nil {
		t.Fatalf("a set on a retired field is refused on encode: %v", err)
	}
	if _, err := DecodeDeclaration(stored); err != nil {
		t.Fatalf("a set on a retired field is refused on decode: %v", err)
	}
	decl.Fields.Retired = nil
	if _, err := EncodeDeclaration(decl); !errors.Is(err, ErrMalformedDeclaration) {
		t.Fatalf("a set on a field that was never declared = %v, want a malformed declaration", err)
	}
}
