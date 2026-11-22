package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
)

func tableMember(id string, state MemberState) Member {
	return Member{ID: id, DataAddr: id + ":4433", State: state}
}

func tableState(members ...Member) ClusterState {
	return ClusterState{Epoch: 7, StateSeq: 9, Members: members}
}

func tableIDs(ms []Member) []string {
	ids := make([]string, len(ms))
	for i, m := range ms {
		ids[i] = m.ID
	}
	return ids
}

func tableRankedStates(s MemberState) bool {
	return s == MemberActive || s == MemberDown || s == MemberLeaving
}

// tableScoreOf is the spec's rank hash written out as bytes, independent of
// the implementation: id bytes, then the slot as two big-endian bytes.
func tableScoreOf(id string, slot uint16) uint64 {
	raw := append([]byte(id), byte(slot>>8), byte(slot))
	return xxhash.Sum64(raw)
}

// The hand-computed golden scores below come from an independent Python
// xxhash64 (checked against the reference vectors for "", "a", "abc") over
// the bytes utf8(id) || slot as 2 bytes big-endian.
var tableGoldenScores = []struct {
	id    string
	slot  uint16
	score uint64
}{
	{"node-a", 0, 0xb418fdbad275f47e},
	{"node-b", 0, 0x5dbb6dad574444db},
	{"node-c", 0, 0x2dd16f4f1c28f53b},
	{"node-a", 1, 0xd7815091a264cce6},
	{"node-b", 1, 0xa271569bd9d9496f},
	{"node-c", 1, 0xfcbfc3368f8c0940},
	{"node-a", 2, 0x7740549f22b49b2c},
	{"node-b", 2, 0xd944a14d639a2dc0},
	{"node-c", 2, 0x6a6edc1f55dd9b0f},
}

var tableGoldenRanking = map[uint16][]string{
	0: {"node-a", "node-b", "node-c"},
	1: {"node-c", "node-a", "node-b"},
	2: {"node-b", "node-a", "node-c"},
}

func tableThree() ClusterState {
	return tableState(tableMember("node-a", MemberActive), tableMember("node-b", MemberActive), tableMember("node-c", MemberActive))
}

func TestTableGoldenScoresAndRanking(t *testing.T) {
	for _, g := range tableGoldenScores {
		if got := tableScoreOf(g.id, g.slot); got != g.score {
			t.Errorf("score(%s,%d) = %#x, golden %#x", g.id, g.slot, got, g.score)
		}
	}
	tb := BuildTable(tableThree())
	for slot, want := range tableGoldenRanking {
		if got := tableIDs(tb.Ranking(slot)); !slices.Equal(got, want) {
			t.Errorf("slot %d ranking = %v, want %v", slot, got, want)
		}
	}
	raw := tb.Encode()
	if !bytes.HasPrefix(raw, []byte{3, 0, 1, 2, 3, 2, 0, 1, 3, 1, 0, 2}) {
		t.Errorf("first three slots on the wire = %x, want 030001020302000103010002", raw)
	}
}

func TestTableRankIsStableAcrossStateChanges(t *testing.T) {
	calls := 0
	score := func(id string, slot uint16) uint64 { calls++; return tableScoreOf(id, slot) }
	base := tableState(tableMember("a", MemberActive), tableMember("b", MemberActive), tableMember("c", MemberActive), tableMember("d", MemberActive))
	before := buildTable(base, score)
	if calls != 4*tableSlots {
		t.Fatalf("build hashed %d times, want one hash per member per slot = %d", calls, 4*tableSlots)
	}
	changed := tableState(tableMember("a", MemberActive), tableMember("b", MemberDown), tableMember("c", MemberLeaving), tableMember("d", MemberActive))
	changed.Epoch, changed.StateSeq = 99, 1234
	after := BuildTable(changed)
	for slot := uint16(0); slot < tableSlots; slot++ {
		if !slices.Equal(tableIDs(before.Ranking(slot)), tableIDs(after.Ranking(slot))) {
			t.Fatalf("slot %d: ranking moved on a down/leaving change: %v -> %v", slot, tableIDs(before.Ranking(slot)), tableIDs(after.Ranking(slot)))
		}
	}
	// A returning node lands on its old rank: down then active again.
	back := tableState(tableMember("a", MemberActive), tableMember("b", MemberActive), tableMember("c", MemberActive), tableMember("d", MemberActive))
	back.Epoch = 100
	if !bytes.Equal(before.Encode(), BuildTable(back).Encode()) {
		t.Fatal("a node that returns to active did not land on its old rank")
	}
	// state_seq and epoch never enter the table.
	other := base
	other.StateSeq, other.Epoch = 55, 55
	if !bytes.Equal(before.Encode(), BuildTable(other).Encode()) {
		t.Fatal("the table changed with epoch or state_seq alone")
	}
}

func TestTableTiesBreakByNodeIDBytes(t *testing.T) {
	constant := func(string, uint16) uint64 { return 42 }
	ids := []string{"b", "a", "B", "ab", "a\xff", "aa", "z"}
	// More than 12 names, because the sort switches from insertion sort to
	// an unstable algorithm there and only an explicit tie-break survives it.
	for i := 0; i < 60; i++ {
		ids = append(ids, fmt.Sprintf("node-%02d", (i*37)%60))
	}
	for seed := int64(0); seed < 20; seed++ {
		r := rand.New(rand.NewSource(seed))
		var members []Member
		for _, i := range r.Perm(len(ids)) {
			members = append(members, tableMember(ids[i], MemberActive))
		}
		tb := buildTable(tableState(members...), constant)
		want := slices.Clone(ids)
		slices.Sort(want)
		for _, slot := range []uint16{0, 1, 4095, 8191} {
			if got := tableIDs(tb.Ranking(slot)); !slices.Equal(got, want) {
				t.Fatalf("seed %d slot %d: tied ranking = %q, want byte order %q", seed, slot, got, want)
			}
		}
	}
	// Three score classes interleaved with the id order leave many ties among
	// unsorted rows, which a missing tie-break leaves in sort-dependent order.
	classes := func(id string, _ uint16) uint64 { return uint64(xxhash.Sum64String(id) % 3) }
	var members []Member
	for _, id := range ids {
		members = append(members, tableMember(id, MemberActive))
	}
	want := slices.Clone(ids)
	slices.SortFunc(want, func(a, b string) int {
		if ca, cb := classes(a, 0), classes(b, 0); ca != cb {
			return int(cb) - int(ca)
		}
		return strings.Compare(a, b)
	})
	if got := tableIDs(buildTable(tableState(members...), classes).Ranking(5)); !slices.Equal(got, want) {
		t.Fatalf("interleaved ties ranked %q, want score descending then id bytes %q", got, want)
	}
	// A higher score still wins over a lower id.
	prefer := func(id string, _ uint16) uint64 {
		if id == "z" {
			return 100
		}
		return 1
	}
	tb := buildTable(tableState(tableMember("a", MemberActive), tableMember("z", MemberActive)), prefer)
	if got := tableIDs(tb.Ranking(0)); got[0] != "z" {
		t.Fatalf("score must dominate the tie-break: %v", got)
	}
}

func TestTableOwnerSkipsDown(t *testing.T) {
	// Golden slot 0 ranking is a, b, c.
	owner := func(a, b, c MemberState) (string, bool) {
		tb := BuildTable(tableState(tableMember("node-a", a), tableMember("node-b", b), tableMember("node-c", c)))
		m, ok := tb.Owner(0, 3)
		return m.ID, ok
	}
	cases := []struct {
		a, b, c MemberState
		want    string
		ok      bool
	}{
		{MemberActive, MemberActive, MemberActive, "node-a", true},
		{MemberDown, MemberActive, MemberActive, "node-b", true},
		{MemberDown, MemberDown, MemberActive, "node-c", true},
		{MemberDown, MemberDown, MemberDown, "", false},
		{MemberLeaving, MemberActive, MemberActive, "node-a", true},
		{MemberLeaving, MemberDown, MemberDown, "node-a", true},
		{MemberDown, MemberLeaving, MemberDown, "node-b", true},
	}
	for _, c := range cases {
		got, ok := owner(c.a, c.b, c.c)
		if got != c.want || ok != c.ok {
			t.Errorf("states %v %v %v: owner = %q,%v want %q,%v", c.a, c.b, c.c, got, ok, c.want, c.ok)
		}
	}
	// A down member keeps its position in the copy set.
	tb := BuildTable(tableState(tableMember("node-a", MemberDown), tableMember("node-b", MemberActive), tableMember("node-c", MemberActive)))
	if got := tableIDs(tb.Copies(0, 2)); !slices.Equal(got, []string{"node-a", "node-b"}) {
		t.Errorf("copies with a down rank 1 = %v, want the down name kept in place", got)
	}
}

func TestTableOwnerIsNeverAJoiningMember(t *testing.T) {
	// The joining identity would rank first on slot 0 if it were ranked.
	tb := BuildTable(tableState(tableMember("node-a", MemberJoining), tableMember("node-b", MemberActive), tableMember("node-c", MemberActive)))
	if m, ok := tb.Owner(0, 3); !ok || m.ID != "node-b" {
		t.Fatalf("owner with a joining rank-1 identity = %v,%v want node-b", m.ID, ok)
	}
	for slot := uint16(0); slot < tableSlots; slot++ {
		for _, m := range tb.Ranking(slot) {
			if m.ID == "node-a" {
				t.Fatalf("slot %d ranks a joining member", slot)
			}
		}
	}
	only := BuildTable(tableState(tableMember("node-a", MemberJoining)))
	if m, ok := only.Owner(0, 3); ok {
		t.Fatalf("a lone joining member owns slot 0: %v", m.ID)
	}
	// dead and witnesses are never ranked either.
	w := tableMember("node-w", MemberActive)
	w.Witness = true
	tb = BuildTable(tableState(tableMember("node-a", MemberDead), w, tableMember("node-b", MemberActive)))
	if got := tableIDs(tb.Members()); !slices.Equal(got, []string{"node-b"}) {
		t.Fatalf("ranked members = %v, want only node-b", got)
	}
}

func TestTableCopiesAreTruncatedByRF(t *testing.T) {
	tb := BuildTable(tableThree())
	for slot, rank := range tableGoldenRanking {
		for rf := 1; rf <= 3; rf++ {
			if got := tableIDs(tb.Copies(slot, rf)); !slices.Equal(got, rank[:rf]) {
				t.Errorf("slot %d rf %d: copies = %v, want %v", slot, rf, got, rank[:rf])
			}
		}
		if got := tb.Copies(slot, 5); len(got) != 3 {
			t.Errorf("slot %d rf 5 over 3 ranked members = %d copies, want 3", slot, len(got))
		}
		if got := tb.Copies(slot, 0); len(got) != 0 {
			t.Errorf("slot %d rf 0 = %v, want none", slot, got)
		}
	}
	if tb.Copies(tableSlots, 2) != nil {
		t.Error("an out-of-range slot has copies")
	}
	if _, ok := tb.Owner(tableSlots, 3); ok {
		t.Error("an out-of-range slot has an owner")
	}
}

func TestTableWireTableIsTheFullRanking(t *testing.T) {
	st := tableState(tableMember("n1", MemberActive), tableMember("n2", MemberDown), tableMember("n3", MemberLeaving),
		tableMember("n4", MemberActive), tableMember("n5", MemberJoining))
	tb := BuildTable(st)
	raw := tb.Encode()
	// 4 ranked members: each slot is 1 count byte + 4 one-byte indices.
	if want := tableSlots * (1 + 4); len(raw) != want {
		t.Fatalf("wire table is %d bytes, want 8192 x (1 + |R|) = %d", len(raw), want)
	}
	for slot := 0; slot < tableSlots; slot++ {
		if raw[slot*5] != 4 {
			t.Fatalf("slot %d count byte = %d, want the full ranking 4 (down and leaving names stay)", slot, raw[slot*5])
		}
	}
	back, err := DecodeTable(raw, tb.Members())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	for slot := uint16(0); slot < tableSlots; slot++ {
		if !slices.Equal(tableIDs(back.Ranking(slot)), tableIDs(tb.Ranking(slot))) {
			t.Fatalf("slot %d differs after a round trip", slot)
		}
		a, aok := back.Owner(slot, 2)
		b, bok := tb.Owner(slot, 2)
		if a.ID != b.ID || aok != bok {
			t.Fatalf("slot %d owner differs after a round trip", slot)
		}
	}
	if !bytes.Equal(back.Encode(), raw) {
		t.Fatal("re-encode differs")
	}
	// More than 128 members need two-byte indices; the size follows.
	var many []Member
	for i := 0; i < 130; i++ {
		many = append(many, tableMember(fmt.Sprintf("m%03d", i), MemberActive))
	}
	big := BuildTable(tableState(many...))
	again, err := DecodeTable(big.Encode(), big.Members())
	if err != nil || !bytes.Equal(again.Encode(), big.Encode()) {
		t.Fatalf("130-member round trip failed: %v", err)
	}
}

func tableRefusal(t *testing.T, raw []byte, members []Member, what string) {
	t.Helper()
	tb, err := DecodeTable(raw, members)
	if err == nil {
		t.Errorf("%s: decode accepted it (%v)", what, tb)
		return
	}
	var ce *Error
	if !errors.As(err, &ce) || ce.Code != CodeInvalidArgument {
		t.Errorf("%s: error %v is not INVALID_ARGUMENT", what, err)
	}
}

func TestTableDecodeRefusesBadInput(t *testing.T) {
	tb := BuildTable(tableThree())
	members := tb.Members()
	good := tb.Encode()
	tableRefusal(t, nil, members, "empty")
	tableRefusal(t, good[:len(good)-1], members, "one byte short")
	tableRefusal(t, good[:len(good)/2], members, "half")
	tableRefusal(t, append(slices.Clone(good), 0), members, "one byte over")
	tableRefusal(t, append(slices.Clone(good), good...), members, "twice as long")

	bad := slices.Clone(good)
	bad[1] = 3 // index 3 with 3 members
	tableRefusal(t, bad, members, "index out of range")
	bad = slices.Clone(good)
	bad[0] = 2 // truncated ranking (count below |R|)
	tableRefusal(t, bad, members, "count below the full ranking")
	bad = slices.Clone(good)
	bad[0] = 4 // count above |R|
	tableRefusal(t, bad, members, "count above the full ranking")
	bad = slices.Clone(good)
	bad[2] = bad[1] // duplicate index in one ranking
	tableRefusal(t, bad, members, "duplicate index")
	bad = slices.Clone(good)
	bad[1] = 0x80 // overlong or unterminated varint
	tableRefusal(t, bad, members, "non-minimal varint")

	tableRefusal(t, good, members[:2], "fewer members than the table names")
	tableRefusal(t, good, []Member{tableMember("x", MemberJoining), members[1], members[2]}, "joining member in the members list")
	tableRefusal(t, good, []Member{members[0], members[0], members[2]}, "duplicate member")
	w := members[1]
	w.Witness = true
	tableRefusal(t, good, []Member{members[0], w, members[2]}, "witness in the members list")

	if _, err := DecodeTable(BuildTable(ClusterState{}).Encode(), nil); err != nil {
		t.Errorf("an empty ranking must round trip: %v", err)
	}
}

func TestTableMutationsNeverPanicAndRoundTrip(t *testing.T) {
	st := tableState(tableMember("a", MemberActive), tableMember("b", MemberDown), tableMember("c", MemberLeaving))
	tb := BuildTable(st)
	golden, members := tb.Encode(), tb.Members()
	r := rand.New(rand.NewSource(20261101))
	accepted := 0
	for i := 0; i < 20000; i++ {
		raw := slices.Clone(golden)
		for k := r.Intn(3) + 1; k > 0 && len(raw) > 0; k-- {
			switch r.Intn(6) {
			case 0:
				raw[r.Intn(len(raw))] = byte(r.Intn(256))
			case 1:
				raw[r.Intn(len(raw))] ^= 1 << uint(r.Intn(8))
			case 2:
				raw = raw[:r.Intn(len(raw)+1)]
			case 3:
				raw = append(raw, byte(r.Intn(256)))
			case 4:
				at := r.Intn(len(raw))
				raw = slices.Insert(raw, at, byte(r.Intn(256)))
			case 5:
				at := r.Intn(len(raw))
				raw = slices.Delete(raw, at, at+1)
			}
		}
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("mutation %d panicked: %v", i, p)
				}
			}()
			got, err := DecodeTable(raw, members)
			if err != nil {
				return
			}
			accepted++
			if !bytes.Equal(got.Encode(), raw) {
				t.Fatalf("mutation %d decoded but re-encodes differently", i)
			}
		}()
	}
	t.Logf("accepted %d of 20000 mutations (all canonical)", accepted)
}

func tableRandomState(r *rand.Rand) ClusterState {
	n := r.Intn(9)
	seen := map[string]bool{}
	var st ClusterState
	for len(st.Members) < n {
		id := fmt.Sprintf("n%d-%x", r.Intn(1000), r.Intn(256))
		if seen[id] {
			continue
		}
		seen[id] = true
		m := tableMember(id, MemberState(1+r.Intn(5)))
		m.Witness = r.Intn(5) == 0
		st.Members = append(st.Members, m)
		if r.Intn(2) == 0 {
			st.Voters = append(st.Voters, id)
		}
	}
	st.Epoch, st.StateSeq = r.Int63n(1000), r.Int63n(1000)
	return st
}

func TestTableProperties(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 200; iter++ {
		st := tableRandomState(r)
		tb := BuildTable(st)
		ranked := map[string]Member{}
		for _, m := range st.Members {
			if !m.Witness && tableRankedStates(m.State) {
				ranked[m.ID] = m
			}
		}
		for slot := uint16(0); slot < tableSlots; slot++ {
			full := tb.Ranking(slot)
			if len(full) != len(ranked) {
				t.Fatalf("state %d slot %d: ranking has %d names, %d are ranked", iter, slot, len(full), len(ranked))
			}
			for i, m := range full {
				if _, ok := ranked[m.ID]; !ok {
					t.Fatalf("state %d slot %d: %s (%v, witness %v) is ranked", iter, slot, m.ID, m.State, m.Witness)
				}
				if i > 0 {
					prev := full[i-1]
					ps, cs := tableScoreOf(prev.ID, slot), tableScoreOf(m.ID, slot)
					if ps < cs || (ps == cs && prev.ID > m.ID) {
						t.Fatalf("state %d slot %d: %s before %s breaks the order", iter, slot, prev.ID, m.ID)
					}
				}
			}
			for rf := 1; rf <= 4; rf++ {
				c := tb.Copies(slot, rf)
				want := full[:min(rf, len(full))]
				if !slices.Equal(tableIDs(c), tableIDs(want)) {
					t.Fatalf("state %d slot %d rf %d: copies %v want prefix %v", iter, slot, rf, tableIDs(c), tableIDs(want))
				}
				tableCheckOwner(t, tb, slot, rf, c)
				if rf > 1 && !slices.Equal(tableIDs(tb.Copies(slot, rf-1)), tableIDs(c)[:min(rf-1, len(c))]) {
					t.Fatalf("state %d slot %d: copies are not nested between rf %d and %d", iter, slot, rf-1, rf)
				}
			}
		}
	}
}

func TestTableIsDeterministicInInputOrder(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	st := tableRandomState(r)
	for len(st.Members) < 6 {
		st = tableRandomState(r)
	}
	want := BuildTable(st).Encode()
	for i := 0; i < 100; i++ {
		shuffled := st
		shuffled.Members = slices.Clone(st.Members)
		r.Shuffle(len(shuffled.Members), func(a, b int) { shuffled.Members[a], shuffled.Members[b] = shuffled.Members[b], shuffled.Members[a] })
		if !bytes.Equal(BuildTable(shuffled).Encode(), want) {
			t.Fatalf("input order %d produced different table bytes", i)
		}
	}
}

func TestTableRemovingANodeMovesOnlyItsOwnSlots(t *testing.T) {
	var members []Member
	for i := 0; i < 5; i++ {
		members = append(members, tableMember(fmt.Sprintf("node-%d", i), MemberActive))
	}
	full := BuildTable(tableState(members...))
	for gone := range members {
		rest := slices.Delete(slices.Clone(members), gone, gone+1)
		after := BuildTable(tableState(rest...))
		changed := 0
		for slot := uint16(0); slot < tableSlots; slot++ {
			before, _ := full.Owner(slot, 3)
			now, _ := after.Owner(slot, 3)
			if before.ID != now.ID {
				changed++
				if before.ID != members[gone].ID {
					t.Fatalf("removing %s moved slot %d from %s to %s", members[gone].ID, slot, before.ID, now.ID)
				}
			}
		}
		share := float64(changed) / tableSlots
		t.Logf("removing %s changes %d of 8192 slots = %.4f", members[gone].ID, changed, share)
		if share < 0.15 || share > 0.25 {
			t.Errorf("removing 1 of 5 nodes changed %.3f of the slots, want about 0.2", share)
		}
	}
}

// tableCheckOwner asserts the owner is the first non-down name of the copy
// set, and that there is none exactly when every copy is down.
func tableCheckOwner(t *testing.T, tb *SlotTable, slot uint16, rf int, copies []Member) {
	t.Helper()
	owner, ok := tb.Owner(slot, rf)
	for _, c := range copies {
		if c.State == MemberDown {
			continue
		}
		if !ok || owner.ID != c.ID {
			t.Fatalf("slot %d rf %d: owner %q,%v want first non-down copy %q", slot, rf, owner.ID, ok, c.ID)
		}
		if owner.State == MemberDown || owner.State == MemberJoining || owner.State == MemberDead || owner.Witness {
			t.Fatalf("slot %d rf %d: owner %s is %v witness %v", slot, rf, owner.ID, owner.State, owner.Witness)
		}
		return
	}
	if ok {
		t.Fatalf("slot %d rf %d: owner %q although every copy is down", slot, rf, owner.ID)
	}
}

func TestTableOwnerIsAlwaysACopy(t *testing.T) {
	r := rand.New(rand.NewSource(22))
	for iter := 0; iter < 200; iter++ {
		tb := BuildTable(tableRandomState(r))
		for slot := uint16(0); slot < tableSlots; slot++ {
			for rf := 1; rf <= 5; rf++ {
				tableCheckOwner(t, tb, slot, rf, tb.Copies(slot, rf))
				if owner, ok := tb.Owner(slot, rf); ok && !slices.Contains(tableIDs(tb.Copies(slot, rf)), owner.ID) {
					t.Fatalf("state %d slot %d rf %d: owner %s holds no copy", iter, slot, rf, owner.ID)
				}
			}
			if _, ok := tb.Owner(slot, 0); ok {
				t.Fatalf("slot %d: an owner for rf 0", slot)
			}
		}
	}
}

func TestTableNoOwnerWhenEveryCopyIsDown(t *testing.T) {
	// Slot 0 ranks a, b, c.
	tb := BuildTable(tableState(tableMember("node-a", MemberDown), tableMember("node-b", MemberDown), tableMember("node-c", MemberActive)))
	if m, ok := tb.Owner(0, 2); ok {
		t.Fatalf("rf 2 with both copies down: owner %s holds no copy", m.ID)
	}
	if m, ok := tb.Owner(0, 3); !ok || m.ID != "node-c" {
		t.Fatalf("rf 3: owner %v,%v want node-c", m.ID, ok)
	}
}

func TestTableOwnerChangesWithRF(t *testing.T) {
	tb := BuildTable(tableState(tableMember("node-a", MemberDown), tableMember("node-b", MemberActive), tableMember("node-c", MemberActive)))
	if m, ok := tb.Owner(0, 1); ok {
		t.Fatalf("rf 1 with the only copy down: owner %s", m.ID)
	}
	if m, ok := tb.Owner(0, 3); !ok || m.ID != "node-b" {
		t.Fatalf("rf 3: owner %v,%v want node-b", m.ID, ok)
	}
}
