package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func u32cDoc(elements ...[]byte) []byte {
	body := bytes.Join(elements, nil)
	out := binary.LittleEndian.AppendUint32(nil, uint32(len(body)+5))
	out = append(out, body...)
	return append(out, 0)
}

func u32cInt64(name string, v int64) []byte {
	return binary.LittleEndian.AppendUint64(append([]byte{0x12}, name+"\x00"...), uint64(v))
}

func u32cInt32(name string, v int32) []byte {
	return binary.LittleEndian.AppendUint32(append([]byte{0x10}, name+"\x00"...), uint32(v))
}

func u32cBin(name string, b []byte) []byte {
	out := binary.LittleEndian.AppendUint32(append([]byte{0x05}, name+"\x00"...), uint32(len(b)))
	return append(append(out, 0), b...)
}

func u32cBool(name string, v bool) []byte {
	out := append([]byte{0x08}, name+"\x00"...)
	if v {
		return append(out, 1)
	}
	return append(out, 0)
}

func u32cSub(name string, doc []byte) []byte {
	return append(append([]byte{0x03}, name+"\x00"...), doc...)
}

func u32cMask(slots ...int) []byte {
	mask := make([]byte, 1024)
	for _, s := range slots {
		mask[s/8] |= 1 << (s % 8)
	}
	return mask
}

func u32cRequire(t *testing.T, err error, want ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a refusal, got none")
	}
	var ce *cluster.Error
	if !errors.As(err, &ce) || ce.Code != cluster.CodeInvalidArgument {
		t.Fatalf("refusal %v is not an INVALID_ARGUMENT cluster.Error", err)
	}
	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Fatalf("refusal %q does not contain %q", err.Error(), w)
		}
	}
}

func u32cList() SlotListRequest {
	return SlotListRequest{DBID: 7, BucketID: 9, Mask: u32cMask(0, 8191, 4000), ResumeSlot: 12, ResumeID: []byte{1, 2, 3}, Want: 50, MaxBytes: 1 << 20}
}

func TestSlotListLayoutAndRoundTrip(t *testing.T) {
	filter := u32cDoc(u32cInt32("age", 3))
	req := u32cList()
	req.Filter = filter
	got, err := EncodeSlotList(req)
	if err != nil {
		t.Fatal(err)
	}
	want := u32cDoc(u32cInt64("db_id", 7), u32cInt64("bucket_id", 9), u32cSub("filter", filter), u32cBin("mask", req.Mask),
		u32cInt32("resume_slot", 12), u32cBin("resume_id", []byte{1, 2, 3}), u32cInt32("want", 50), u32cInt64("max_bytes", 1<<20))
	if !bytes.Equal(got, want) {
		t.Fatalf("SLOT_LIST bytes\n got %x\nwant %x", got, want)
	}
	back, err := DecodeSlotList(got)
	if err != nil {
		t.Fatal(err)
	}
	if back.DBID != 7 || back.BucketID != 9 || !bytes.Equal(back.Filter, filter) || !bytes.Equal(back.Mask, req.Mask) ||
		back.ResumeSlot != 12 || !bytes.Equal(back.ResumeID, req.ResumeID) || back.Want != 50 || back.MaxBytes != 1<<20 {
		t.Fatalf("round trip lost a field: %+v", back)
	}
}

func TestSlotListFirstPageWritesEmptyResumeAndNoFilter(t *testing.T) {
	req := SlotListRequest{DBID: 1, BucketID: 2, Mask: u32cMask(5), Want: 10, MaxBytes: 100}
	got, err := EncodeSlotList(req)
	if err != nil {
		t.Fatal(err)
	}
	want := u32cDoc(u32cInt64("db_id", 1), u32cInt64("bucket_id", 2), u32cBin("mask", req.Mask),
		u32cInt32("resume_slot", 0), u32cBin("resume_id", nil), u32cInt32("want", 10), u32cInt64("max_bytes", 100))
	if !bytes.Equal(got, want) {
		t.Fatalf("first page bytes\n got %x\nwant %x", got, want)
	}
	back, err := DecodeSlotList(got)
	if err != nil || back.Filter != nil || len(back.ResumeID) != 0 {
		t.Fatalf("first page decoded as %+v, %v", back, err)
	}
}

func TestSlotListRefusals(t *testing.T) {
	tooLong := make([]byte, maxEncodedID+1)
	cases := []struct {
		name   string
		mutate func(*SlotListRequest)
		want   string
	}{
		{"short mask", func(r *SlotListRequest) { r.Mask = make([]byte, 1023) }, "1024"},
		{"long mask", func(r *SlotListRequest) { r.Mask = make([]byte, 1025) }, "1024"},
		{"no mask", func(r *SlotListRequest) { r.Mask = nil }, "1024"},
		{"slot past the table", func(r *SlotListRequest) { r.ResumeSlot = 8192 }, "8191"},
		{"zero want", func(r *SlotListRequest) { r.Want = 0 }, "want"},
		{"negative want", func(r *SlotListRequest) { r.Want = -1 }, "want"},
		{"zero byte share", func(r *SlotListRequest) { r.MaxBytes = 0 }, "max_bytes"},
		{"resume id too long", func(r *SlotListRequest) { r.ResumeID = tooLong }, "resume_id"},
		{"filter not a document", func(r *SlotListRequest) { r.Filter = []byte{1, 2, 3} }, "filter"},
		{"projection with a field", func(r *SlotListRequest) { r.Fields = []uint16{2} }, "SPEC-GAP U87-5"},
		{"projection with no field", func(r *SlotListRequest) { r.Fields = []uint16{} }, "SPEC-GAP U87-5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := u32cList()
			tc.mutate(&req)
			_, err := EncodeSlotList(req)
			u32cRequire(t, err, tc.want)
		})
	}
}

func TestSlotListDecodeRefusals(t *testing.T) {
	good := u32cDoc(u32cInt64("db_id", 7), u32cInt64("bucket_id", 9), u32cBin("mask", u32cMask(1)),
		u32cInt32("resume_slot", 0), u32cBin("resume_id", nil), u32cInt32("want", 5), u32cInt64("max_bytes", 5))
	if _, err := DecodeSlotList(good); err != nil {
		t.Fatalf("the control body is refused: %v", err)
	}
	bad := map[string][]byte{
		"empty":            nil,
		"truncated":        good[:len(good)-3],
		"trailing byte":    append(append([]byte(nil), good...), 0),
		"missing want":     u32cDoc(u32cInt64("db_id", 7), u32cInt64("bucket_id", 9), u32cBin("mask", u32cMask(1)), u32cInt32("resume_slot", 0), u32cBin("resume_id", nil), u32cInt64("max_bytes", 5)),
		"short mask":       u32cDoc(u32cInt64("db_id", 7), u32cInt64("bucket_id", 9), u32cBin("mask", make([]byte, 1023)), u32cInt32("resume_slot", 0), u32cBin("resume_id", nil), u32cInt32("want", 5), u32cInt64("max_bytes", 5)),
		"slot 8192":        u32cDoc(u32cInt64("db_id", 7), u32cInt64("bucket_id", 9), u32cBin("mask", u32cMask(1)), u32cInt32("resume_slot", 8192), u32cBin("resume_id", nil), u32cInt32("want", 5), u32cInt64("max_bytes", 5)),
		"want as int64":    u32cDoc(u32cInt64("db_id", 7), u32cInt64("bucket_id", 9), u32cBin("mask", u32cMask(1)), u32cInt32("resume_slot", 0), u32cBin("resume_id", nil), u32cInt64("want", 5), u32cInt64("max_bytes", 5)),
		"unknown field":    u32cDoc(u32cInt64("db_id", 7), u32cInt64("bucket_id", 9), u32cBin("mask", u32cMask(1)), u32cInt32("resume_slot", 0), u32cBin("resume_id", nil), u32cInt32("want", 5), u32cInt64("max_bytes", 5), u32cInt32("extra", 1)),
		"fields reordered": u32cDoc(u32cInt64("bucket_id", 9), u32cInt64("db_id", 7), u32cBin("mask", u32cMask(1)), u32cInt32("resume_slot", 0), u32cBin("resume_id", nil), u32cInt32("want", 5), u32cInt64("max_bytes", 5)),
		"oversized":        make([]byte, maxSubScanBody+1),
	}
	for name, raw := range bad {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeSlotList(raw)
			u32cRequire(t, err)
		})
	}
}

func TestSlotCountLayoutAndRoundTrip(t *testing.T) {
	filter := u32cDoc(u32cBool("on", true))
	req := SlotCountRequest{DBID: 3, BucketID: 4, Filter: filter, Mask: u32cMask(0, 1, 2)}
	got, err := EncodeSlotCount(req)
	if err != nil {
		t.Fatal(err)
	}
	want := u32cDoc(u32cInt64("db_id", 3), u32cInt64("bucket_id", 4), u32cSub("filter", filter), u32cBin("mask", req.Mask))
	if !bytes.Equal(got, want) {
		t.Fatalf("SLOT_COUNT bytes\n got %x\nwant %x", got, want)
	}
	back, err := DecodeSlotCount(got)
	if err != nil || back.DBID != 3 || back.BucketID != 4 || !bytes.Equal(back.Filter, filter) || !bytes.Equal(back.Mask, req.Mask) {
		t.Fatalf("round trip: %+v, %v", back, err)
	}
	for _, size := range []int{0, 1023, 1025} {
		_, err := EncodeSlotCount(SlotCountRequest{DBID: 1, BucketID: 1, Mask: make([]byte, size)})
		u32cRequire(t, err, "1024")
		_, err = DecodeSlotCount(u32cDoc(u32cInt64("db_id", 1), u32cInt64("bucket_id", 1), u32cBin("mask", make([]byte, size))))
		u32cRequire(t, err, "1024")
	}
	_, err = DecodeSlotCount(append(append([]byte(nil), got...), 7))
	u32cRequire(t, err)
}

func TestSlotCountReplyIsOneInteger(t *testing.T) {
	got, err := EncodeSlotCountReply(SlotCountReply{Count: 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	if want := u32cDoc(u32cInt64("count", 1<<40)); !bytes.Equal(got, want) {
		t.Fatalf("SLOT_COUNT_REPLY bytes\n got %x\nwant %x", got, want)
	}
	back, err := DecodeSlotCountReply(got)
	if err != nil || back.Count != 1<<40 {
		t.Fatalf("round trip: %+v, %v", back, err)
	}
	_, err = EncodeSlotCountReply(SlotCountReply{Count: -1})
	u32cRequire(t, err, "count")
	_, err = DecodeSlotCountReply(u32cDoc(u32cInt64("count", -5)))
	u32cRequire(t, err, "count")
	_, err = DecodeSlotCountReply(u32cDoc(u32cInt32("count", 5)))
	u32cRequire(t, err)
}

func TestSlotListReplyLayoutAndRecords(t *testing.T) {
	var entries []byte
	var err error
	recs := []SlotRecord{{Slot: 3, EncID: []byte{1}, Body: []byte("abc")}, {Slot: 8191, EncID: []byte{2, 3}, Body: nil}}
	for _, r := range recs {
		if entries, err = AppendSlotRecord(entries, r); err != nil {
			t.Fatal(err)
		}
	}
	if want := []byte{0, 3, 1, 1, 3, 'a', 'b', 'c', 0x1F, 0xFF, 2, 2, 3, 0}; !bytes.Equal(entries, want) {
		t.Fatalf("record bytes %x want %x", entries, want)
	}
	raw, err := EncodeSlotListReply(SlotListReply{Entries: entries, More: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := u32cDoc(u32cBin("entries", entries), u32cBool("more", true)); !bytes.Equal(raw, want) {
		t.Fatalf("SLOT_LIST_REPLY bytes\n got %x\nwant %x", raw, want)
	}
	back, err := DecodeSlotListReply(raw)
	if err != nil || !back.More || !bytes.Equal(back.Entries, entries) {
		t.Fatalf("round trip: %+v, %v", back, err)
	}
	parsed, err := ParseSlotRecords(back.Entries)
	if err != nil || len(parsed) != 2 || parsed[0].Slot != 3 || string(parsed[0].Body) != "abc" || parsed[1].Slot != 8191 || len(parsed[1].Body) != 0 {
		t.Fatalf("parsed %+v, %v", parsed, err)
	}
	empty, err := EncodeSlotListReply(SlotListReply{})
	if err != nil {
		t.Fatal(err)
	}
	if want := u32cDoc(u32cBin("entries", nil), u32cBool("more", false)); !bytes.Equal(empty, want) {
		t.Fatalf("empty reply bytes %x want %x", empty, want)
	}
}

func TestSlotRecordFramingRefusals(t *testing.T) {
	bad := map[string][]byte{
		"one byte":                  {0},
		"slot past table":           {0x20, 0x00, 1, 1, 0},
		"no id":                     {0, 1, 0, 0},
		"id past the end":           {0, 1, 5, 1},
		"no body length":            {0, 1, 1, 1},
		"body past the end":         {0, 1, 1, 1, 9, 1},
		"id length varint overflow": {0, 1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
		"id over the maximum":       append([]byte{0, 1, 0x81, 0x08}, make([]byte, 1025)...),
	}
	for name, entries := range bad {
		t.Run(name, func(t *testing.T) {
			_, err := ParseSlotRecords(entries)
			u32cRequire(t, err)
			_, err = DecodeSlotListReply(u32cDoc(u32cBin("entries", entries), u32cBool("more", false)))
			u32cRequire(t, err)
			_, err = EncodeSlotListReply(SlotListReply{Entries: entries})
			u32cRequire(t, err)
		})
	}
	_, err := AppendSlotRecord(nil, SlotRecord{Slot: 8192, EncID: []byte{1}})
	u32cRequire(t, err, "8191")
	_, err = AppendSlotRecord(nil, SlotRecord{Slot: 1})
	u32cRequire(t, err, "id")
	_, err = AppendSlotRecord(nil, SlotRecord{Slot: 1, EncID: make([]byte, maxEncodedID+1)})
	u32cRequire(t, err, "id")
}

func TestSlotScanDecodeRefusesABodyPastTheFrame(t *testing.T) {
	pad := u32cDoc(u32cInt64("count", 1), u32cBin("pad", make([]byte, maxSubScanBody)))
	for name, decode := range map[string]func([]byte) error{
		"SLOT_LIST":        func(b []byte) error { _, err := DecodeSlotList(b); return err },
		"SLOT_COUNT":       func(b []byte) error { _, err := DecodeSlotCount(b); return err },
		"SLOT_LIST_REPLY":  func(b []byte) error { _, err := DecodeSlotListReply(b); return err },
		"SLOT_COUNT_REPLY": func(b []byte) error { _, err := DecodeSlotCountReply(b); return err },
	} {
		u32cRequire(t, decode(pad), name, "frame holds at most")
	}
}

func TestSlotListNegativeAndWrappingResumeSlotIsRefused(t *testing.T) {
	for _, slot := range []int32{-1, 65536, 65536 + 5} {
		raw := u32cDoc(u32cInt64("db_id", 7), u32cInt64("bucket_id", 9), u32cBin("mask", u32cMask(1)),
			u32cInt32("resume_slot", slot), u32cBin("resume_id", nil), u32cInt32("want", 5), u32cInt64("max_bytes", 5))
		_, err := DecodeSlotList(raw)
		u32cRequire(t, err, "resume_slot")
	}
}

func TestSlotListReplyEntriesAreBoundedByTheFrame(t *testing.T) {
	big, err := AppendSlotRecord(nil, SlotRecord{Slot: 1, EncID: []byte{1}, Body: make([]byte, maxSubScanBody-16)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = EncodeSlotListReply(SlotListReply{Entries: big})
	u32cRequire(t, err, "SLOT_LIST_REPLY", "frame")
	_, err = EncodeSlotListReply(SlotListReply{Entries: make([]byte, maxSubScanBody+1)})
	u32cRequire(t, err, "entries")
}

func u32cMutate(rng *rand.Rand, raw []byte) []byte {
	out := append([]byte(nil), raw...)
	switch rng.IntN(4) {
	case 0:
		return out[:rng.IntN(len(out)+1)]
	case 1:
		return append(out, byte(rng.IntN(256)))
	default:
		for range 1 + rng.IntN(3) {
			out[rng.IntN(len(out))] = byte(rng.IntN(256))
		}
	}
	return out
}

func TestSlotScanDecodersSurviveMutation(t *testing.T) {
	filter := u32cDoc(u32cInt32("age", 3))
	listRaw, _ := EncodeSlotList(SlotListRequest{DBID: 7, BucketID: 9, Filter: filter, Mask: u32cMask(1, 99), ResumeSlot: 4, ResumeID: []byte{9}, Want: 5, MaxBytes: 99})
	countRaw, _ := EncodeSlotCount(SlotCountRequest{DBID: 7, BucketID: 9, Filter: filter, Mask: u32cMask(1, 99)})
	entries, _ := AppendSlotRecord(nil, SlotRecord{Slot: 2, EncID: []byte{1, 2}, Body: []byte("xy")})
	replyRaw, _ := EncodeSlotListReply(SlotListReply{Entries: entries, More: true})
	countReplyRaw, _ := EncodeSlotCountReply(SlotCountReply{Count: 12})
	rng := rand.New(rand.NewPCG(32, 32))
	accepted := 0
	for range 100_000 {
		switch rng.IntN(4) {
		case 0:
			raw := u32cMutate(rng, listRaw)
			if msg, err := DecodeSlotList(raw); err == nil {
				accepted++
				if again, err := EncodeSlotList(msg); err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("SLOT_LIST %x re-encodes as %x, %v", raw, again, err)
				}
			}
		case 1:
			raw := u32cMutate(rng, countRaw)
			if msg, err := DecodeSlotCount(raw); err == nil {
				accepted++
				if again, err := EncodeSlotCount(msg); err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("SLOT_COUNT %x re-encodes as %x, %v", raw, again, err)
				}
			}
		case 2:
			raw := u32cMutate(rng, replyRaw)
			if msg, err := DecodeSlotListReply(raw); err == nil {
				accepted++
				if again, err := EncodeSlotListReply(msg); err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("SLOT_LIST_REPLY %x re-encodes as %x, %v", raw, again, err)
				}
			}
		default:
			raw := u32cMutate(rng, countReplyRaw)
			if msg, err := DecodeSlotCountReply(raw); err == nil {
				accepted++
				if again, err := EncodeSlotCountReply(msg); err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("SLOT_COUNT_REPLY %x re-encodes as %x, %v", raw, again, err)
				}
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated body was accepted, so the re-encode check never ran")
	}
}

func TestSlotScanDecodeAllocationIsBoundedByTheInput(t *testing.T) {
	// A header that claims a huge document must not allocate what it claims.
	lie := binary.LittleEndian.AppendUint32(nil, 0x7fffffff)
	lie = append(lie, 0x05, 'm', 0, 0xff, 0xff, 0xff, 0x7f, 0)
	allocs := testing.AllocsPerRun(20, func() {
		_, _ = DecodeSlotList(lie)
		_, _ = DecodeSlotCount(lie)
		_, _ = DecodeSlotListReply(lie)
		_, _ = DecodeSlotCountReply(lie)
	})
	if allocs > 200 {
		t.Fatalf("%v allocations decoding four lying bodies", allocs)
	}
	var sink uint64
	before := testing.Benchmark(func(b *testing.B) {
		for range b.N {
			_, _ = DecodeSlotList(lie)
		}
	})
	sink += uint64(before.AllocedBytesPerOp())
	if sink > 1<<16 {
		t.Fatalf("a %d byte lying body allocated %d bytes", len(lie), sink)
	}
}

func TestSlotScanFilterIsValidatedOnBothKindsAndBothDirections(t *testing.T) {
	badFilters := map[string][]byte{
		"not a document": {1, 2, 3},
		"bad element":    u32cDoc([]byte{0x7f, 'a', 0, 1, 2}),
		"too deep":       u32cNested(slotFilterDepth + 1),
	}
	for name, filter := range badFilters {
		t.Run(name, func(t *testing.T) {
			_, err := EncodeSlotCount(SlotCountRequest{DBID: 1, BucketID: 1, Filter: filter, Mask: u32cMask(1)})
			u32cRequire(t, err, "filter")
			_, err = EncodeSlotList(SlotListRequest{DBID: 1, BucketID: 1, Filter: filter, Mask: u32cMask(1), Want: 1, MaxBytes: 1})
			u32cRequire(t, err, "filter")
			rawCount := u32cDoc(u32cInt64("db_id", 1), u32cInt64("bucket_id", 1), u32cSub("filter", filter), u32cBin("mask", u32cMask(1)))
			_, err = DecodeSlotCount(rawCount)
			u32cRequire(t, err)
			rawList := u32cDoc(u32cInt64("db_id", 1), u32cInt64("bucket_id", 1), u32cSub("filter", filter), u32cBin("mask", u32cMask(1)),
				u32cInt32("resume_slot", 0), u32cBin("resume_id", nil), u32cInt32("want", 1), u32cInt64("max_bytes", 1))
			_, err = DecodeSlotList(rawList)
			u32cRequire(t, err)
		})
	}
}

func u32cNested(levels int) []byte {
	doc := u32cDoc(u32cInt32("leaf", 1))
	for range levels - 1 {
		doc = u32cDoc(u32cSub("n", doc))
	}
	return doc
}
