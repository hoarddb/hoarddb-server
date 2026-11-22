// bench/propertyshape — a synthetic dataset with the shape of a real
// property-management application's data, for the BSON codec's corpus capture
// and its benchmarks (bson-codec-spec.md §9.1.6 and §9.2).
//
// The shapes are the numbers a one-time read of an application's dev dump
// gave: how many fields a document has, which BSON types they are, how long
// the strings run, how deep documents nest, how many documents a collection
// holds. Nothing else came out of that read. No key name, value or record is
// in this program or in its output: every key is a type word and a number
// (oid_03, str_01, padded to ten bytes), every string is lower-case letters and spaces from a
// seeded generator, and every id and date is random. The twelve shapes cover
// the dump's spread, from a 5-field, 107-byte record to a 25-field one with
// arrays of sub-documents and a 5-level one with binary values.
//
// Run:
//
//	go run ./bench/propertyshape -out /tmp/propshape            # writes <shape>.bson files
//	hoarddb ingest /tmp/propshape --into-db propshape ...        # one bucket per file
//
// Like bench/social it is a measuring instrument, not product code, and it
// adds no go.mod entries.
package main

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// shape describes one synthetic collection by counts, not by names. The
// counts are per document; arrays and sub-documents take a length from a
// range.
type shape struct {
	name string
	docs int

	// Scalar top-level fields. objectIDs counts _id.
	objectIDs, dateTimes, bools, nulls, int32s, int64s, doubles int
	// One string field per entry, of about that many bytes.
	stringLens []int
	// One binary field per entry, of that many bytes.
	binaryLens []int

	// Arrays of ObjectIDs, of strings (eight bytes each), and of
	// sub-documents of subFields scalars each. An array may come out empty, so
	// depth is the deepest a document of the shape reaches, not every one's.
	idArrays, stringArrays, docArrays int
	arrayLen                          [2]int
	docArrayLen                       [2]int
	subFields                         int
	// embeddedDocs are single sub-documents of subFields scalars.
	embeddedDocs int
	// chainDepth is the levels of a nest_01 -> nest_02 -> ... chain ending in
	// three scalars, 0 for none.
	chainDepth int

	// minBytes and maxBytes bound an encoded document: the test pins them so
	// a change to a shape cannot go unnoticed.
	minBytes, maxBytes int
}

// topLevelFields is how many fields every document of the shape has.
func (s shape) topLevelFields() int {
	count := s.objectIDs + s.dateTimes + s.bools + s.nulls + s.int32s + s.int64s + s.doubles +
		len(s.stringLens) + len(s.binaryLens) + s.idArrays + s.stringArrays + s.docArrays + s.embeddedDocs
	if s.chainDepth > 0 {
		count++
	}
	return count
}

// depth is the deepest nesting a document of the shape reaches, counted as the server counts
// it: the root is level 1, each sub-document or array adds one.
func (s shape) depth() int {
	deepest := 1
	if s.idArrays > 0 || s.stringArrays > 0 || s.embeddedDocs > 0 {
		deepest = 2
	}
	if s.docArrays > 0 {
		deepest = 3
	}
	if s.chainDepth > 0 {
		deepest = max(deepest, 1+s.chainDepth)
	}
	return deepest
}

// shapes is the table. Counts come from the dump's per-collection statistics;
// the table has no other trace of it.
var shapes = []shape{
	{name: "wide_a", docs: 998, objectIDs: 8, dateTimes: 3, bools: 2, nulls: 2, doubles: 1,
		stringLens: []int{0, 0, 0, 0, 12, 12, 56}, idArrays: 1, docArrays: 1, arrayLen: [2]int{0, 1}, docArrayLen: [2]int{0, 1}, subFields: 5,
		minBytes: 380, maxBytes: 900},
	{name: "medium_b", docs: 572, objectIDs: 3, dateTimes: 1, int32s: 4, stringLens: []int{6, 14},
		minBytes: 150, maxBytes: 700},
	{name: "flagged_c", docs: 386, objectIDs: 3, dateTimes: 1, bools: 1, stringLens: []int{16, 23},
		minBytes: 130, maxBytes: 300},
	{name: "tiny_d", docs: 325, objectIDs: 2, int32s: 2, stringLens: []int{7},
		minBytes: 100, maxBytes: 115},
	{name: "wide_e", docs: 233, objectIDs: 7, dateTimes: 3, nulls: 1, stringLens: []int{0, 0, 0, 65},
		minBytes: 280, maxBytes: 520},
	{name: "arrays_f", docs: 185, objectIDs: 3, dateTimes: 2, bools: 1, nulls: 6, int32s: 1, doubles: 1,
		stringLens: []int{0, 0, 7, 7, 26, 31, 33}, stringArrays: 2, arrayLen: [2]int{1, 2},
		minBytes: 380, maxBytes: 700},
	{name: "mid_g", docs: 184, objectIDs: 6, dateTimes: 2, nulls: 1, doubles: 1, stringLens: []int{7, 7},
		minBytes: 190, maxBytes: 330},
	{name: "nested_h", docs: 143, objectIDs: 3, dateTimes: 5, bools: 3, nulls: 3, int32s: 3, doubles: 2,
		stringLens: []int{12, 49}, docArrays: 1, docArrayLen: [2]int{1, 6}, subFields: 8, embeddedDocs: 1,
		minBytes: 500, maxBytes: 2800},
	{name: "nested_i", docs: 64, objectIDs: 6, dateTimes: 5, bools: 4, nulls: 2, int32s: 1,
		stringLens: []int{5, 60}, idArrays: 1, docArrays: 1, arrayLen: [2]int{3, 9}, docArrayLen: [2]int{3, 9}, subFields: 4, embeddedDocs: 1,
		minBytes: 500, maxBytes: 2000},
	{name: "small_j", docs: 124, objectIDs: 3, dateTimes: 2, bools: 1, nulls: 1, stringLens: []int{9},
		minBytes: 130, maxBytes: 200},
	{name: "mixed_k", docs: 62, objectIDs: 3, dateTimes: 2, bools: 1, nulls: 1, stringLens: []int{12, 57},
		docArrays: 1, docArrayLen: [2]int{1, 2}, subFields: 5,
		minBytes: 220, maxBytes: 900},
	{name: "deep_l", docs: 40, objectIDs: 1, dateTimes: 1, nulls: 2, stringLens: []int{9, 300}, binaryLens: []int{2, 39}, chainDepth: 4, docArrays: 1, docArrayLen: [2]int{2, 4}, subFields: 7,
		minBytes: 700, maxBytes: 1500},
}

// generate returns docs*scale documents of the shape, the same on every run
// for one seed.
func (s shape) generate(scale int, seed uint64) []bson.D {
	shapeIndex := uint64(0)
	for position, candidate := range shapes {
		if candidate.name == s.name {
			shapeIndex = uint64(position)
		}
	}
	randomness := rand.New(rand.NewPCG(seed, shapeIndex+1))
	documents := make([]bson.D, 0, s.docs*scale)
	for range s.docs * scale {
		documents = append(documents, s.document(randomness))
	}
	return documents
}

func (s shape) document(randomness *rand.Rand) bson.D {
	document := make(bson.D, 0, s.topLevelFields())
	add := func(prefix string, number int, value any) {
		document = append(document, bson.E{Key: keyName(prefix, number), Value: value})
	}
	for position := range s.objectIDs {
		if position == 0 {
			document = append(document, bson.E{Key: "_id", Value: syntheticObjectID(randomness)})
			continue
		}
		add("oid", position, syntheticObjectID(randomness))
	}
	for position, length := range s.stringLens {
		add("str", position+1, syntheticText(randomness, length))
	}
	for position := range s.dateTimes {
		add("dt", position+1, syntheticDate(randomness))
	}
	for position := range s.bools {
		add("flag", position+1, randomness.IntN(2) == 0)
	}
	for position := range s.nulls {
		add("nul", position+1, nil)
	}
	for position := range s.int32s {
		add("i32", position+1, int32(randomness.IntN(100000)))
	}
	for position := range s.int64s {
		add("i64", position+1, randomness.Int64N(1<<40))
	}
	for position := range s.doubles {
		add("dbl", position+1, randomness.Float64()*1000)
	}
	for position, length := range s.binaryLens {
		blob := make([]byte, length)
		for byteIndex := range blob {
			blob[byteIndex] = byte(randomness.IntN(256))
		}
		add("blob", position+1, primitive.Binary{Subtype: 0, Data: blob})
	}
	for position := range s.idArrays {
		identifiers := bson.A{}
		for range inRange(randomness, s.arrayLen) {
			identifiers = append(identifiers, syntheticObjectID(randomness))
		}
		add("ids", position+1, identifiers)
	}
	for position := range s.stringArrays {
		names := bson.A{}
		for range inRange(randomness, s.arrayLen) {
			names = append(names, syntheticText(randomness, 8))
		}
		add("names", position+1, names)
	}
	for position := range s.docArrays {
		rows := bson.A{}
		for range inRange(randomness, s.docArrayLen) {
			rows = append(rows, subDocument(randomness, s.subFields))
		}
		add("rows", position+1, rows)
	}
	for position := range s.embeddedDocs {
		add("sub", position+1, subDocument(randomness, s.subFields))
	}
	if s.chainDepth > 0 {
		document = append(document, bson.E{Key: keyName("nest", 1), Value: nestedChain(randomness, s.chainDepth, 1)})
	}
	return document
}

// nestedChain is the value of nest_<level>: a document holding the next level,
// or at the last level three scalars.
func nestedChain(randomness *rand.Rand, levels, level int) bson.D {
	if level == levels {
		return subDocument(randomness, 3)
	}
	return bson.D{{Key: fmt.Sprintf("nest_%02d", level+1), Value: nestedChain(randomness, levels, level+1)}}
}

// subDocument is a document of count scalars cycling through int32, a short
// string, a date, a bool and null.
func subDocument(randomness *rand.Rand, count int) bson.D {
	document := make(bson.D, 0, count)
	for position := range count {
		var value any
		switch position % 5 {
		case 0:
			value = int32(randomness.IntN(1000))
		case 1:
			value = syntheticText(randomness, 8)
		case 2:
			value = syntheticDate(randomness)
		case 3:
			value = randomness.IntN(2) == 0
		default:
			value = nil
		}
		document = append(document, bson.E{Key: keyName("sub", position+1), Value: value})
	}
	return document
}

// keyWidth pads every key but _id to this many bytes with underscores. The
// dump's keys average about ten bytes; a synthetic "oid_01" is six, which
// would make every document a fifth smaller than the one it stands for.
const keyWidth = 10

func keyName(prefix string, number int) string {
	return padded(fmt.Sprintf("%s_%02d", prefix, number))
}

func inRange(randomness *rand.Rand, bounds [2]int) int {
	return bounds[0] + randomness.IntN(bounds[1]-bounds[0]+1)
}

func syntheticObjectID(randomness *rand.Rand) primitive.ObjectID {
	var identifier primitive.ObjectID
	for position := range identifier {
		identifier[position] = byte(randomness.IntN(256))
	}
	return identifier
}

// syntheticDate is a random instant between 2020 and 2026, at millisecond
// resolution.
func syntheticDate(randomness *rand.Rand) primitive.DateTime {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	return primitive.DateTime(start + randomness.Int64N(6*365*24*3600*1000))
}

// syntheticText is lower-case letters and single spaces, about length bytes
// (within a fifth for the longer ones, so lengths are not all identical).
func syntheticText(randomness *rand.Rand, length int) string {
	if length > 12 {
		length += randomness.IntN(length/5+1) - length/10
	}
	var text strings.Builder
	for text.Len() < length {
		wordLength := min(2+randomness.IntN(8), length-text.Len())
		for range wordLength {
			text.WriteByte(byte('a' + randomness.IntN(26)))
		}
		if text.Len() < length-1 {
			text.WriteByte(' ')
		}
	}
	return text.String()
}

// padded right-fills name with underscores to keyWidth bytes.
func padded(name string) string {
	return name + strings.Repeat("_", max(0, keyWidth-len(name)))
}
