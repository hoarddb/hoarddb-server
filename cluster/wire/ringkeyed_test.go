package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

var (
	u65EncID   = []byte{0x50, 'p', '1', 0, 0}
	u65IDValue = []byte{0x02, 0x03, 0, 0, 0, 'p', '1', 0}
	u65Name    = []byte{0x02, 0x04, 0, 0, 0, 'A', 'd', 'a', 0}
)

func u65Prologue() RingPrologue {
	return RingPrologue{Epoch: 42, StateSeq: 41, DeadlineMs: 1500}
}

func u65Header(op cluster.Op) cluster.Op {
	op.BucketID, op.DeclVersion, op.SchemaVersionSeen = 41, 7, 7
	op.EncID = u65EncID
	return op
}

// u65Verb is the {v: bin} argument of a push: a 4+1+... BSON document.
func u65Verb() []byte {
	doc := []byte{0, 0, 0, 0, 0x05, 'v', 0, 3, 0, 0, 0, 0, 1, 2, 3, 0}
	binary.LittleEndian.PutUint32(doc, uint32(len(doc)))
	return doc
}

// u65Doc is a one-element BSON document {name: value} whose element type is tag.
func u65Doc(tag byte, name string, value []byte) []byte {
	element := append([]byte{tag}, name...)
	element = append(element, 0)
	element = append(element, value...)
	doc := binary.LittleEndian.AppendUint32(nil, uint32(len(element)+5))
	return append(append(doc, element...), 0)
}

func u65EveryWriteClass() []cluster.Op {
	lease := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	return []cluster.Op{
		u65Header(cluster.Op{Class: cluster.OpInsert, IDValue: u65IDValue, Fields: []cluster.FieldOp{{FieldID: 2, Value: u65Name}}}),
		u65Header(cluster.Op{Class: cluster.OpUpsert, IDValue: u65IDValue, Fields: []cluster.FieldOp{{FieldID: 2, Value: u65Name}}}),
		u65Header(cluster.Op{Class: cluster.OpUpdate, Fields: []cluster.FieldOp{{FieldID: 2, OpCode: cluster.UpdateSet, Value: u65Name}}}),
		u65Header(cluster.Op{Class: cluster.OpDelete}),
		u65Header(cluster.Op{Class: cluster.OpStoreVerb, Fields: []cluster.FieldOp{{FieldID: 4, Verb: cluster.VerbPush, Value: u65Verb()}}}),
		u65Header(cluster.Op{Class: cluster.OpStoreVerb, Fields: []cluster.FieldOp{{FieldID: 4, Verb: cluster.VerbPop}}}),
		u65Header(cluster.Op{Class: cluster.OpClaim, Fields: []cluster.FieldOp{{FieldID: 4}}, Count: 3}),
		u65Header(cluster.Op{Class: cluster.OpAck, Fields: []cluster.FieldOp{{FieldID: 4}}, LeaseID: lease}),
		u65Header(cluster.Op{Class: cluster.OpRelease, Fields: []cluster.FieldOp{{FieldID: 4}}, LeaseID: lease}),
		u65Header(cluster.Op{Class: cluster.OpExtend, Fields: []cluster.FieldOp{{FieldID: 4}}, LeaseID: lease, Until: 99}),
	}
}

func u65EveryRead() []RingRead {
	base := RingRead{BucketID: 41, DeclVersion: 7, SchemaVersionSeen: 7, EncID: u65EncID}
	get, peek, list, count := base, base, base, base
	get.Class = RingClassGet
	peek.Class, peek.FieldID, peek.Limit = RingClassPeek, 4, 5
	list.Class, list.FieldID, list.Limit, list.After = RingClassStoreList, 4, 100, []byte{9, 9}
	count.Class, count.FieldID = RingClassStoreCount, 4
	first := list
	first.After = nil
	leaseGet := base
	leaseGet.Class, leaseGet.FieldID, leaseGet.Lease = RingClassLeaseGet, 4, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	storeGet := base
	storeGet.Class, storeGet.FieldID, storeGet.Key = RingClassStoreGet, 4, "grace"
	getFields := base
	getFields.Class, getFields.Fields = RingClassGetFields, []string{"Name", "Bio"}
	return []RingRead{get, peek, list, first, count, leaseGet, storeGet, getFields}
}

func u65AssertRefused(t *testing.T, err error, mustSay ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("accepted, want INVALID_ARGUMENT")
	}
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
		t.Fatalf("error %v is not an INVALID_ARGUMENT cluster.Error", err)
	}
	for _, word := range mustSay {
		if !strings.Contains(refusal.Message, word) {
			t.Fatalf("message %q does not name %q", refusal.Message, word)
		}
	}
}

func TestU65KeyedWorkedExampleIsPrologueThenFormats1Body(t *testing.T) {
	op := u65Header(cluster.Op{Class: cluster.OpInsert, IDValue: u65IDValue, Fields: []cluster.FieldOp{{FieldID: 2, Value: u65Name}}})
	raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: op})
	if err != nil {
		t.Fatal(err)
	}
	// conventions Formats 1: the 75-byte body; the client prologue before it.
	body := "01" +
		"0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" +
		"5070310000" + "00000008" + "0203000000703100" + "0001" + "0002" + "00000009" + "020400000041646100"
	want := "000000000000002a" + "0000000000000029" + "000005dc" + body
	if got := hex.EncodeToString(raw); got != want {
		t.Fatalf("payload\n got %s\nwant %s", got, want)
	}
	if len(raw) != RingPrologueLen+75 {
		t.Fatalf("payload is %d bytes, want %d", len(raw), RingPrologueLen+75)
	}
}

func TestU65KeyedEveryWriteClassRoundTrips(t *testing.T) {
	ops := u65EveryWriteClass()
	for _, op := range ops {
		req := RingKeyedRequest{Prologue: u65Prologue(), Op: op}
		raw, err := EncodeRingKeyed(req)
		if err != nil {
			t.Fatalf("class 0x%02x: %v", op.Class, err)
		}
		got, err := DecodeRingKeyed(raw)
		if err != nil {
			t.Fatalf("class 0x%02x: %v", op.Class, err)
		}
		if got.Read != nil || got.Prologue != req.Prologue {
			t.Fatalf("class 0x%02x decoded as read=%v prologue=%+v", op.Class, got.Read, got.Prologue)
		}
		again, err := EncodeRingKeyed(got)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("class 0x%02x re-encodes as %x (%v), want %x", op.Class, again, err, raw)
		}
	}
}

func TestU65KeyedReadClassesHaveTheProposedLayout(t *testing.T) {
	want := map[string]string{
		"get":        "81" + "0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" + "5070310000",
		"peek":       "82" + "0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" + "5070310000" + "0004" + "0005",
		"list":       "83" + "0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" + "5070310000" + "0004" + "0064" + "0002" + "0909",
		"list-first": "83" + "0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" + "5070310000" + "0004" + "0064" + "0000",
		"count":      "84" + "0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" + "5070310000" + "0004",
		"lease_get":  "87" + "0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" + "5070310000" + "0004" + "0102030405060708090a0b0c0d0e0f10",
		"store_get":  "88" + "0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" + "5070310000" + "0004" + "0005" + "6772616365",
		"get_fields": "89" + "0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" + "5070310000" + "02" + "04" + "4e616d65" + "03" + "42696f",
	}
	names := []string{"get", "peek", "list", "list-first", "count", "lease_get", "store_get", "get_fields"}
	for i, read := range u65EveryRead() {
		raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
		if err != nil {
			t.Fatalf("%s: %v", names[i], err)
		}
		if got := hex.EncodeToString(raw[RingPrologueLen:]); got != want[names[i]] {
			t.Fatalf("%s form\n got %s\nwant %s", names[i], got, want[names[i]])
		}
		decoded, err := DecodeRingKeyed(raw)
		if err != nil || decoded.Read == nil {
			t.Fatalf("%s decodes as %+v, %v", names[i], decoded, err)
		}
		again, err := EncodeRingKeyed(decoded)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("%s re-encodes as %x (%v)", names[i], again, err)
		}
	}
}

func TestU65KeyedDecodeRefusesEveryTruncationAndTrailingBytes(t *testing.T) {
	var payloads [][]byte
	for _, op := range u65EveryWriteClass() {
		raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: op})
		if err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, raw)
	}
	for _, read := range u65EveryRead() {
		raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
		if err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, raw)
	}
	for _, raw := range payloads {
		for cut := 0; cut < len(raw); cut++ {
			if _, err := DecodeRingKeyed(raw[:cut]); err == nil {
				// A list with no cursor is a prefix-free form, so a cut can never be valid.
				t.Fatalf("payload %x cut to %d bytes was accepted", raw, cut)
			}
		}
		_, err := DecodeRingKeyed(append(bytes.Clone(raw), 0))
		u65AssertRefused(t, err, "after")
	}
}

func TestU65KeyedRefusalsNameTheFieldAndTheFix(t *testing.T) {
	insert := RingKeyedRequest{Prologue: u65Prologue(), Op: u65EveryWriteClass()[0]}
	valid, err := EncodeRingKeyed(insert)
	if err != nil {
		t.Fatal(err)
	}
	for _, class := range []byte{0x00, 0x0A, 0x7F, 0x80, 0x85, 0xFF} {
		raw := bytes.Clone(valid)
		raw[RingPrologueLen] = class
		_, err := DecodeRingKeyed(raw)
		u65AssertRefused(t, err, "class", "0x01 to 0x09", "0x81 to 0x84")
	}
	_, err = DecodeRingKeyed(valid[:RingPrologueLen-1])
	u65AssertRefused(t, err, "prologue", "20")
	_, err = DecodeRingKeyed(valid[:RingPrologueLen])
	u65AssertRefused(t, err, "class")

	withPrevEnd := insert
	withPrevEnd.Op.PrevEnd = cluster.Position{Epoch: 1, Seq: 2}
	_, err = EncodeRingKeyed(withPrevEnd)
	u65AssertRefused(t, err, "prevEnd")
	raw := bytes.Clone(valid)
	raw[RingPrologueLen+1+24+15] = 1
	_, err = DecodeRingKeyed(raw)
	u65AssertRefused(t, err, "prevEnd")

	read := u65EveryRead()[0]
	both := RingKeyedRequest{Prologue: u65Prologue(), Op: insert.Op, Read: &read}
	_, err = EncodeRingKeyed(both)
	u65AssertRefused(t, err, "read", "write")
}

func TestU65KeyedReadFieldRules(t *testing.T) {
	reads := u65EveryRead()
	cases := map[string]func(*RingRead){
		"get with a field id":     func(r *RingRead) { *r = reads[0]; r.FieldID = 1 },
		"get with a limit":        func(r *RingRead) { *r = reads[0]; r.Limit = 1 },
		"get with a cursor":       func(r *RingRead) { *r = reads[0]; r.After = []byte{1} },
		"peek without a field":    func(r *RingRead) { *r = reads[1]; r.FieldID = 0 },
		"peek count 0":            func(r *RingRead) { *r = reads[1]; r.Limit = 0 },
		"peek count over 100":     func(r *RingRead) { *r = reads[1]; r.Limit = 101 },
		"peek count 1000":         func(r *RingRead) { *r = reads[1]; r.Limit = 1000 },
		"store_get without a key": func(r *RingRead) { *r = reads[6]; r.Key = "" },
		"store_get key over 1024": func(r *RingRead) { *r = reads[6]; r.Key = strings.Repeat("k", 1025) },
		"store_get without field": func(r *RingRead) { *r = reads[6]; r.FieldID = 0 },
		"store_get with a limit":  func(r *RingRead) { *r = reads[6]; r.Limit = 1 },
		"store_get with a cursor": func(r *RingRead) { *r = reads[6]; r.After = []byte{1} },
		"store_get with a lease":  func(r *RingRead) { *r = reads[6]; r.Lease = [16]byte{1} },
		"peek with a key":         func(r *RingRead) { *r = reads[1]; r.Key = "k" },
		"list with a key":         func(r *RingRead) { *r = reads[2]; r.Key = "k" },
		"get with a key":          func(r *RingRead) { *r = reads[0]; r.Key = "k" },
		"peek with a cursor":      func(r *RingRead) { *r = reads[1]; r.After = []byte{1} },
		"list limit 0":            func(r *RingRead) { *r = reads[2]; r.Limit = 0 },
		"list limit over 1000":    func(r *RingRead) { *r = reads[2]; r.Limit = 1001 },
		"list cursor over cap":    func(r *RingRead) { *r = reads[2]; r.After = make([]byte, ringMaxAfter+1) },
		"count with a limit":      func(r *RingRead) { *r = reads[4]; r.Limit = 1 },
		"count without a field":   func(r *RingRead) { *r = reads[4]; r.FieldID = 0 },
		"negative decl version":   func(r *RingRead) { *r = reads[0]; r.DeclVersion = -1 },
		"empty document id":       func(r *RingRead) { *r = reads[0]; r.EncID = nil },
		"document id not a value": func(r *RingRead) { *r = reads[0]; r.EncID = []byte{0xEE, 1} },
		"document id over 1024": func(r *RingRead) {
			*r = reads[0]
			r.EncID = append(append([]byte{0x50}, bytes.Repeat([]byte("a"), 1100)...), 0, 0)
		},
		"unknown read class": func(r *RingRead) { *r = reads[0]; r.Class = 0x85 },
	}
	for name, mutate := range cases {
		var read RingRead
		mutate(&read)
		_, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
		if err == nil {
			t.Errorf("%s: encoder accepted it", name)
			continue
		}
		u65AssertRefused(t, err)
	}
	// The decoder holds the same line for the cases the wire can express.
	raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &reads[1]})
	if err != nil {
		t.Fatal(err)
	}
	zero := bytes.Clone(raw)
	binary.BigEndian.PutUint16(zero[len(zero)-2:], 0)
	_, err = DecodeRingKeyed(zero)
	u65AssertRefused(t, err, "count", "1 to 100")
	big := bytes.Clone(raw)
	binary.BigEndian.PutUint16(big[len(big)-2:], 101)
	_, err = DecodeRingKeyed(big)
	u65AssertRefused(t, err, "count", "1 to 100")
}

func TestU65KeyedStoreVerbArgRules(t *testing.T) {
	verb := func(kind byte, arg []byte) RingKeyedRequest {
		return RingKeyedRequest{Prologue: u65Prologue(), Op: u65Header(cluster.Op{Class: cluster.OpStoreVerb,
			Fields: []cluster.FieldOp{{FieldID: 4, Verb: kind, Value: arg}}})}
	}
	trailing := append(u65Verb(), 0)
	cases := map[string]struct {
		req  RingKeyedRequest
		said string
	}{
		"pop with garbage":               {verb(cluster.VerbPop, []byte{1, 2, 3}), "BSON"},
		"push without an arg":            {verb(cluster.VerbPush, nil), "needs a BSON arg"},
		"put with garbage":               {verb(cluster.VerbPut, []byte{1, 2, 3}), "BSON"},
		"take with trailing bytes":       {verb(cluster.VerbTake, trailing), "arg"},
		"arg over 1 MiB":                 {verb(cluster.VerbPush, make([]byte, 1<<20+1)), "1048576"},
		"array keyed out of sequence":    {verb(cluster.VerbPush, u65Doc(0x04, "v", u65Doc(0x10, "z", []byte{1, 0, 0, 0}))), "canonical"},
		"arg length lies about its body": {verb(cluster.VerbPush, append([]byte{0xff, 0xff, 0, 0}, u65Verb()[4:]...)), "BSON"},
	}
	for name, tc := range cases {
		_, err := EncodeRingKeyed(tc.req)
		if err == nil {
			t.Errorf("%s: encoder accepted it", name)
			continue
		}
		u65AssertRefused(t, err, tc.said)
	}
	// The decoder refuses the same bytes: build them past the encoder.
	for name, tc := range cases {
		op := tc.req.Op
		op.Fields = []cluster.FieldOp{{FieldID: 4, Verb: cluster.VerbPush, Value: u65Verb()}}
		good, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: op})
		if err != nil {
			t.Fatal(err)
		}
		offset := len(good) - len(u65Verb())
		forged := append(bytes.Clone(good[:offset-5]), tc.req.Op.Fields[0].Verb)
		forged = binary.BigEndian.AppendUint32(forged, uint32(len(tc.req.Op.Fields[0].Value)))
		forged = append(forged, tc.req.Op.Fields[0].Value...)
		if _, err := DecodeRingKeyed(forged); err == nil {
			t.Errorf("%s: decoder accepted it", name)
		}
	}
}

func u65Mutate(rng *rand.Rand, raw []byte) []byte {
	out := bytes.Clone(raw)
	for flips := 1 + rng.IntN(4); flips > 0; flips-- {
		switch rng.IntN(3) {
		case 0:
			out[rng.IntN(len(out))] = byte(rng.IntN(256))
		case 1:
			out = out[:rng.IntN(len(out)+1)]
		default:
			out = append(out, byte(rng.IntN(256)))
		}
		if len(out) == 0 {
			out = []byte{byte(rng.IntN(256))}
		}
	}
	return out
}

func TestU65KeyedDecoderSurvivesMutation(t *testing.T) {
	var seeds [][]byte
	for _, op := range u65EveryWriteClass() {
		raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: op})
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, raw)
	}
	for _, read := range u65EveryRead() {
		raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, raw)
	}
	rng := rand.New(rand.NewPCG(65, 65))
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, seeds[rng.IntN(len(seeds))])
		req, err := DecodeRingKeyed(mutated)
		if err != nil {
			continue
		}
		accepted++
		again, err := EncodeRingKeyed(req)
		if err != nil || !bytes.Equal(again, mutated) {
			t.Fatalf("accepted %x re-encodes as %x, %v", mutated, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated payload was accepted, so the re-encode check never ran")
	}
}

func TestU65KeyedDecodeAllocationIsBoundedByTheInput(t *testing.T) {
	good, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: u65EveryWriteClass()[0]})
	if err != nil {
		t.Fatal(err)
	}
	// Header lies: an id length, a field count and a store-verb arg length of 2 GiB.
	idLie := bytes.Clone(good)
	binary.BigEndian.PutUint32(idLie[RingPrologueLen+1+40+5:], 0x7fffffff)
	verb, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: u65EveryWriteClass()[4]})
	if err != nil {
		t.Fatal(err)
	}
	argLie := bytes.Clone(verb)
	binary.BigEndian.PutUint32(argLie[RingPrologueLen+1+40+5+2+1:], 0x7fffffff)
	countLie := bytes.Clone(good)
	binary.BigEndian.PutUint16(countLie[RingPrologueLen+1+40+5+4+len(u65IDValue):], 0xffff)
	lies := [][]byte{idLie, argLie, countLie}
	for _, lie := range lies {
		if _, err := DecodeRingKeyed(lie); err == nil {
			t.Fatalf("lying payload %x was accepted", lie)
		}
	}
	bytesPerRun := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, lie := range lies {
				_, _ = DecodeRingKeyed(lie)
			}
		}
	}).AllocedBytesPerOp()
	if bytesPerRun > 1<<14 {
		t.Fatalf("three lying payloads of ~%d bytes allocated %d bytes", len(good), bytesPerRun)
	}
}

func TestU65KeyedPayloadPastTheFrameCeilingIsRefusedUnread(t *testing.T) {
	huge := make([]byte, maxSubScanBody+1)
	huge[RingPrologueLen] = cluster.OpInsert
	_, err := DecodeRingKeyed(huge)
	u65AssertRefused(t, err, "32", "frame")
	big := u65EveryWriteClass()[0]
	big.Fields = []cluster.FieldOp{{FieldID: 2, Value: make([]byte, maxSubScanBody)}}
	_, err = EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: big})
	u65AssertRefused(t, err, "frame")
}

// nearestRead is the request of the build note's worked example: cosine, k 5, field Vector,
// the vectors (1, 0) as little-endian float32 pairs.
func nearestRead() RingRead {
	return RingRead{Class: RingClassNearest, BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1,
		EncID: u65EncID, FieldID: 4,
		Nearest: &RingNearest{Metric: RingMetricCosine, K: 5, Field: "Vector", Vector: []byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0}}}
}

func TestNearestCodecRoundTrip(t *testing.T) {
	read := nearestRead()
	raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRingKeyed(raw)
	if err != nil || decoded.Read == nil {
		t.Fatalf("decoded %+v, %v", decoded, err)
	}
	if !reflect.DeepEqual(*decoded.Read, read) {
		t.Fatalf("round trip\n got %+v\nwant %+v", *decoded.Read, read)
	}
	// header (class, 40 bytes), the 5-byte document id, the 2-byte field id, then the form.
	form := raw[RingPrologueLen+1+ringHeaderLen+len(u65EncID)+2:]
	want := []byte{0x01, 0x05, 0x06, 'V', 'e', 'c', 't', 'o', 'r', 0x00, 0x08, 0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(form, want) {
		t.Fatalf("form %x, want %x", form, want)
	}
	if raw[RingPrologueLen] != 0x8A {
		t.Fatalf("class byte 0x%02x, want 0x8a", raw[RingPrologueLen])
	}
}

func TestNearestRefusesTrailingBytes(t *testing.T) {
	read := nearestRead()
	raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeRingKeyed(append(bytes.Clone(raw), 0))
	u65AssertRefused(t, err, "0x8a", "1 bytes after its last field")
	for cut := 0; cut < len(raw); cut++ {
		if _, err := DecodeRingKeyed(raw[:cut]); err == nil {
			t.Fatalf("payload cut to %d bytes was accepted", cut)
		}
	}
}

func TestNearestRefusesEveryMalformedForm(t *testing.T) {
	vector := func(n int) []byte { return make([]byte, n) }
	cases := []struct {
		name   string
		mutate func(read *RingRead)
		want   string
	}{
		{"no query", func(r *RingRead) { r.Nearest = nil }, "a nearest names a query and this one has none. Send the metric, k, field name and vector"},
		{"query on a peek", func(r *RingRead) { r.Class, r.Limit = RingClassPeek, 5 }, "read class 0x82 names no nearest query; drop it (only a nearest, 0x8a, carries one)"},
		{"metric 0", func(r *RingRead) { r.Nearest.Metric = 0 }, "a nearest names metric 0x00; send 0x01 (cosine), 0x02 (dot) or 0x03 (l2)"},
		{"metric 4", func(r *RingRead) { r.Nearest.Metric = 4 }, "a nearest names metric 0x04; send 0x01 (cosine), 0x02 (dot) or 0x03 (l2)"},
		{"k 0", func(r *RingRead) { r.Nearest.K = 0 }, "a nearest asks for k 0 and it must be 1 to 100"},
		{"k 101", func(r *RingRead) { r.Nearest.K = 101 }, "a nearest asks for k 101 and it must be 1 to 100"},
		{"empty name", func(r *RingRead) { r.Nearest.Field = "" }, "a nearest names its vector field in 0 bytes of valid UTF-8 and it must be 1 to 255 bytes"},
		{"long name", func(r *RingRead) { r.Nearest.Field = strings.Repeat("a", 256) }, "a nearest names its vector field in 256 bytes of valid UTF-8 and it must be 1 to 255 bytes"},
		{"bad UTF-8 name", func(r *RingRead) { r.Nearest.Field = "\xff\xfe" }, "a nearest names its vector field in 2 bytes of valid UTF-8 and it must be 1 to 255 bytes"},
		{"empty vector", func(r *RingRead) { r.Nearest.Vector = nil }, "a nearest vector is 0 bytes; send 4 to 16384 bytes, a multiple of 4 (little-endian float32 values)"},
		{"3 byte vector", func(r *RingRead) { r.Nearest.Vector = vector(3) }, "a nearest vector is 3 bytes; send 4 to 16384 bytes, a multiple of 4 (little-endian float32 values)"},
		{"16388 byte vector", func(r *RingRead) { r.Nearest.Vector = vector(16388) }, "a nearest vector is 16388 bytes; send 4 to 16384 bytes, a multiple of 4 (little-endian float32 values)"},
		{"NaN value", func(r *RingRead) { r.Nearest.Vector = u90Vector(1, float32(math.NaN())) }, "a nearest vector holds a NaN or infinite value at element 1; send finite float32 values"},
		{"+Inf value", func(r *RingRead) { r.Nearest.Vector = u90Vector(float32(math.Inf(1)), 0) }, "a nearest vector holds a NaN or infinite value at element 0; send finite float32 values"},
		{"-Inf value", func(r *RingRead) { r.Nearest.Vector = u90Vector(0, 0, float32(math.Inf(-1))) }, "a nearest vector holds a NaN or infinite value at element 2; send finite float32 values"},
	}
	for _, tc := range cases {
		read := nearestRead()
		nearest := *read.Nearest
		read.Nearest = &nearest
		tc.mutate(&read)
		_, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
		t.Run(tc.name, func(t *testing.T) { u65AssertRefused(t, err, tc.want) })
	}
	// The largest legal vector and the largest legal name are accepted.
	read := nearestRead()
	read.Nearest = &RingNearest{Metric: RingMetricL2, K: 100, Field: strings.Repeat("a", 255), Vector: vector(16384)}
	raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRingKeyed(raw); err != nil {
		t.Fatal(err)
	}

	// The decoder names the part that is missing.
	good := nearestRead()
	full, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &good})
	if err != nil {
		t.Fatal(err)
	}
	head := full[:RingPrologueLen+1+ringHeaderLen+len(u65EncID)+2]
	decoderCases := []struct {
		tail []byte
		want string
	}{
		{[]byte{0x01, 0x05}, "a nearest needs a 1-byte metric, a 1-byte k and a 1-byte name length after the field id and 2 bytes remain"},
		{[]byte{0x01, 0x05, 0x06, 'V', 'e'}, "a nearest needs its name after the field id and 2 bytes remain"},
		{[]byte{0x01, 0x05, 0x01, 'V', 0x00}, "a nearest needs a 2-byte vector length after the field id and 1 bytes remain"},
		{[]byte{0x01, 0x05, 0x01, 'V', 0x00, 0x08, 1, 2, 3}, "a nearest needs its vector after the field id and 3 bytes remain"},
	}
	for _, tc := range decoderCases {
		_, err := DecodeRingKeyed(append(bytes.Clone(head), tc.tail...))
		u65AssertRefused(t, err, tc.want)
	}
	// A canonical request has a valid form after the checks: a vector of 3 bytes is refused on decode too.
	_, err = DecodeRingKeyed(append(bytes.Clone(head), 0x01, 0x05, 0x01, 'V', 0x00, 0x03, 1, 2, 3))
	u65AssertRefused(t, err, "a nearest vector is 3 bytes")
}

func TestNearestLyingLengthsAllocateNothing(t *testing.T) {
	good := nearestRead()
	full, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &good})
	if err != nil {
		t.Fatal(err)
	}
	head := full[:RingPrologueLen+1+ringHeaderLen+len(u65EncID)+2]
	lie := append(bytes.Clone(head), 0x01, 0x05, 0x01, 'V', 0xff, 0xff, 0, 0, 0, 0)
	_, err = DecodeRingKeyed(lie)
	u65AssertRefused(t, err, "a nearest needs its vector")
	bytesPerRun := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingKeyed(lie)
		}
	}).AllocedBytesPerOp()
	if bytesPerRun > 1<<12 {
		t.Fatalf("a request claiming 65535 vector bytes with 4 after it allocated %d bytes", bytesPerRun)
	}
}

func TestRingClassRefusalNamesTheNewRange(t *testing.T) {
	read := u65EveryRead()[0]
	valid, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
	if err != nil {
		t.Fatal(err)
	}
	for _, class := range []byte{0x8C, 0x90} {
		raw := bytes.Clone(valid)
		raw[RingPrologueLen] = class
		_, err := DecodeRingKeyed(raw)
		u65AssertRefused(t, err, "0x81 to 0x84, 0x87 to 0x8B")
	}
}
