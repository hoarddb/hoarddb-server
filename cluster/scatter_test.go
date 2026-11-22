package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"math/rand"
	"sort"
	"testing"
)

// u48 vector: conventions Formats 4, app.Users, _id_ "ada", database id 7, bucket id 66.
const u48CursorHex = "01000000000000000700000000000000421669065061646100000efdef57"

func u48Cursor() ListCursor {
	return ListCursor{DatabaseID: 7, BucketID: 66, Slot: 5737, EncID: []byte{0x50, 'a', 'd', 'a', 0x00, 0x00}}
}

func TestU48CursorMatchesTheSpecVector(t *testing.T) {
	cur := u48Cursor()
	if got := hex.EncodeToString(EncodeCursor(cur)); got != u48CursorHex {
		t.Fatalf("cursor hex = %s, want %s", got, u48CursorHex)
	}
	raw, _ := hex.DecodeString(u48CursorHex)
	back, err := DecodeCursor(raw, 7, 66)
	if err != nil {
		t.Fatalf("DecodeCursor: %v", err)
	}
	if back.Slot != 5737 || !bytes.Equal(back.EncID, cur.EncID) {
		t.Fatalf("decoded %+v", back)
	}
}

func TestU48CursorSizesAreTheSpecBounds(t *testing.T) {
	if n := len(EncodeCursor(ListCursor{EncID: []byte{0x00}})); n != 25 {
		t.Fatalf("null id cursor is %d bytes, want 25", n)
	}
	if n := len(EncodeCursor(ListCursor{EncID: make([]byte, 1024)})); n != 1049 {
		t.Fatalf("1024-byte id cursor is %d bytes, want 1049", n)
	}
}

func TestU48CursorRefusalsNameTheirReason(t *testing.T) {
	good := EncodeCursor(u48Cursor())
	flip := bytes.Clone(good)
	flip[len(flip)/2] ^= 0x01
	bump := bytes.Clone(good)
	bump[0] = 2
	cases := []struct {
		name   string
		raw    []byte
		db, bk uint64
		reason string
	}{
		{"flipped byte", flip, 7, 66, "the checksum does not match"},
		{"version bump", bump, 7, 66, "its format version is not 1"},
		{"other bucket", good, 7, 67, "it was issued for bucket id 66"},
		{"other database", good, 8, 66, "its database id is 7"},
		{"truncated", good[:10], 7, 66, "it is truncated"},
	}
	for _, tc := range cases {
		_, err := DecodeCursor(tc.raw, tc.db, tc.bk)
		var bad *CursorError
		if !errors.As(err, &bad) {
			t.Fatalf("%s: err = %v, want a CursorError", tc.name, err)
		}
		if bad.Reason != tc.reason {
			t.Fatalf("%s: reason %q, want %q", tc.name, bad.Reason, tc.reason)
		}
	}
}

// TestU48CursorDecodeMutations: 100k mutations never panic and an accepted input
// re-encodes to the same bytes.
func TestU48CursorDecodeMutations(t *testing.T) {
	rng := rand.New(rand.NewSource(48))
	seeds := [][]byte{EncodeCursor(u48Cursor()), EncodeCursor(ListCursor{DatabaseID: 7, BucketID: 66, EncID: make([]byte, 300)})}
	accepted := 0
	for i := 0; i < 100000; i++ {
		raw := bytes.Clone(seeds[i%len(seeds)])
		for flips := rng.Intn(4); flips >= 0 && len(raw) > 0; flips-- {
			switch rng.Intn(3) {
			case 0:
				raw[rng.Intn(len(raw))] = byte(rng.Intn(256))
			case 1:
				raw = raw[:rng.Intn(len(raw)+1)]
			default:
				raw = append(raw, byte(rng.Intn(256)))
			}
		}
		if i%2 == 0 && len(raw) >= cursorMin {
			// Recompute the checksum so the structure checks, not the CRC, decide.
			binary.BigEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(raw[:len(raw)-4], castagnoli))
		}
		cur, err := DecodeCursor(raw, 7, 66)
		if err != nil {
			continue
		}
		accepted++
		if !bytes.Equal(EncodeCursor(cur), raw) {
			t.Fatalf("accepted %x re-encodes to %x", raw, EncodeCursor(cur))
		}
	}
	if accepted < 1000 {
		t.Fatalf("only %d of 100000 mutations were accepted: the test no longer exercises the accept path", accepted)
	}
}

func TestU48EntriesRoundTripAndMutations(t *testing.T) {
	entries := []ListEntry{
		{Slot: 5737, EncID: []byte{0x50, 0x61, 0x64, 0x61, 0x00, 0x00}, Body: []byte{5, 0, 0, 0, 0}},
		{Slot: 8191, EncID: []byte{0x00}, Body: bytes.Repeat([]byte{7}, 300)},
	}
	var blob []byte
	for _, entry := range entries {
		blob = AppendEntry(blob, entry)
	}
	back, err := DecodeEntries(blob)
	if err != nil {
		t.Fatalf("DecodeEntries: %v", err)
	}
	if len(back) != 2 || back[0].Slot != 5737 || !bytes.Equal(back[1].Body, entries[1].Body) {
		t.Fatalf("round trip: %+v", back)
	}
	rng := rand.New(rand.NewSource(49))
	accepted := 0
	for i := 0; i < 100000; i++ {
		raw := bytes.Clone(blob)
		for flips := rng.Intn(4); flips >= 0 && len(raw) > 0; flips-- {
			switch rng.Intn(3) {
			case 0:
				raw[rng.Intn(len(raw))] = byte(rng.Intn(256))
			case 1:
				raw = raw[:rng.Intn(len(raw)+1)]
			default:
				raw = append(raw, byte(rng.Intn(256)))
			}
		}
		got, err := DecodeEntries(raw)
		if err != nil {
			continue
		}
		accepted++
		var again []byte
		for _, entry := range got {
			again = AppendEntry(again, entry)
		}
		if !bytes.Equal(again, raw) {
			t.Fatalf("accepted %x re-encodes to %x", raw, again)
		}
	}
	if accepted < 1000 {
		t.Fatalf("only %d of 100000 mutations were accepted", accepted)
	}
}

func TestU48SlotMask(t *testing.T) {
	var mask SlotMask
	for _, slot := range []uint16{0, 7, 8, 8191} {
		mask.Set(slot)
	}
	if mask.Count() != 4 || !mask.Has(0) || !mask.Has(8191) || mask.Has(1) {
		t.Fatalf("mask wrong: count %d", mask.Count())
	}
	if mask[0] != 0x81 || mask[1] != 0x01 {
		t.Fatalf("bit order: %x %x, want least significant bit of byte 0 = slot 0", mask[0], mask[1])
	}
}

// u48Owner is a fake owner that holds a fixed sorted set of documents and stops at a fixed
// count per reply, as a real owner stops at its byte share.
type u48Owner struct {
	docs []ListEntry
	cap  int
}

func (o *u48Owner) list(req SlotListRequest) SlotListReply {
	var reply SlotListReply
	for _, doc := range o.docs {
		after := doc.Slot > req.ResumeSlot || (doc.Slot == req.ResumeSlot && bytes.Compare(doc.EncID, req.ResumeID) > 0)
		if !after {
			continue
		}
		if len(reply.Entries) == o.cap || len(reply.Entries) == req.Want {
			reply.More = true
			break
		}
		reply.Entries = append(reply.Entries, doc)
	}
	return reply
}

type u48FakeScanner struct{ owners map[string]*u48Owner }

func (f u48FakeScanner) ScanList(_ context.Context, owner Member, req SlotListRequest) (SlotListReply, error) {
	return f.owners[owner.ID].list(req), nil
}

func (f u48FakeScanner) ScanCount(context.Context, Member, SlotCountRequest) (int64, error) {
	return 0, nil
}

type u48FakeTopo struct{ table *SlotTable }

func (t u48FakeTopo) Epoch() uint64                 { return 1 }
func (t u48FakeTopo) Table() *SlotTable             { return t.table }
func (t u48FakeTopo) Refresh(context.Context) error { return nil }

type u48FakeResolver struct{}

func (u48FakeResolver) Resolve(context.Context, string, string) (BucketTarget, error) {
	return BucketTarget{DatabaseID: 1, BucketID: 2, RF: 1}, nil
}

// TestU48AnOwnerThatStoppedAtItsShareCannotBeSkipped: n1 stops after one document with
// more=true while n2 returns a longer list. Merging n2's later documents past n1's last
// one would skip n1's unseen documents; paging must still return each document once, in order.
func TestU48AnOwnerThatStoppedAtItsShareCannotBeSkipped(t *testing.T) {
	doc := func(slot uint16) ListEntry {
		return ListEntry{Slot: slot, EncID: []byte{0x50, byte(slot), 0, 0}, Body: []byte{5, 0, 0, 0, 0}}
	}
	table := BuildTable(ClusterState{Epoch: 1, Members: []Member{
		{ID: "n1", State: MemberActive}, {ID: "n2", State: MemberActive},
	}})
	// Which node owns a slot is the table's decision, so give each node the documents of
	// slots it owns.
	owners := map[string]*u48Owner{"n1": {cap: 1}, "n2": {cap: 100}}
	var want []ListEntry
	for slot := uint16(0); len(want) < 12; slot++ {
		owner, ok := table.Owner(slot, 1)
		if !ok {
			t.Fatal("slot without an owner")
		}
		entry := doc(slot)
		owners[owner.ID].docs = append(owners[owner.ID].docs, entry)
		want = append(want, entry)
	}
	if len(owners["n1"].docs) < 3 || len(owners["n2"].docs) < 3 {
		t.Skipf("the table gave n1 %d and n2 %d of the first slots", len(owners["n1"].docs), len(owners["n2"].docs))
	}
	coord := NewCoordinator(CoordinatorConfig{
		Topology: u48FakeTopo{table}, Scanner: u48FakeScanner{owners}, Buckets: u48FakeResolver{},
		Limits: ScanLimits{DefaultLimit: 100, MaxLimit: 10000, MaxResultBytes: 1 << 20},
	})
	var got []ListEntry
	var cursor []byte
	for pages := 0; ; pages++ {
		if pages > 40 {
			t.Fatal("paging does not end")
		}
		page, err := coord.List(context.Background(), ListRequest{Database: "d", Bucket: "b", Limit: 10, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		got = append(got, page.Entries...)
		if !page.More {
			break
		}
		cursor = page.Cursor
	}
	sort.Slice(want, func(i, j int) bool { return want[i].Slot < want[j].Slot })
	if len(got) != len(want) {
		t.Fatalf("paged %d documents, want %d: a gap or a repeat", len(got), len(want))
	}
	for i := range want {
		if got[i].Slot != want[i].Slot {
			t.Fatalf("document %d is slot %d, want slot %d", i, got[i].Slot, want[i].Slot)
		}
	}
}
