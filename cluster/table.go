package cluster

import (
	"cmp"
	"encoding/binary"
	"math"
	"slices"

	"github.com/cespare/xxhash/v2"
)

// tableSlots is the slot count (conventions section 3). It repeats
// storage.SlotCount because the cluster package does not import storage.
const tableSlots = 8192

// SlotTable is the full ranking of every slot at one membership. It is a pure
// function of the ranked members and their states (conventions section 4): the
// epoch and state_seq of the ClusterState never enter it, so equal membership
// gives equal bytes.
//
// Cost: tableSlots x n 2-byte indices (n = ranked members), held once per
// cluster and never per bucket, so a bucket adds nothing; a membership change
// rebuilds it in O(slots x n log n), not O(buckets).
type SlotTable struct {
	// members are the ranked members, sorted by id bytes; ranks index into it.
	members []Member
	// ranks is tableSlots rows of len(members) indices, best rank first.
	ranks []uint16
}

// rankScore is rank(node, slot) of conventions section 4:
// xxhash64(utf8(node id) || slot as 2 bytes, big-endian).
func rankScore(id string, slot uint16) uint64 {
	key := make([]byte, 0, len(id)+2)
	key = append(key, id...)
	key = append(key, byte(slot>>8), byte(slot))
	return xxhash.Sum64(key)
}

func rankedState(s MemberState) bool {
	return s == MemberActive || s == MemberDown || s == MemberLeaving
}

// BuildTable ranks every slot from the ranked set of s: non-witness members
// that are active, down or leaving. joining and dead members and witnesses
// are never ranked; a down member keeps its rank. The result does not depend
// on the order of s.Members.
func BuildTable(s ClusterState) *SlotTable {
	return buildTable(s, rankScore)
}

// buildTable takes the score function so a test can force ties, which a real
// xxhash64 cannot be made to produce.
func buildTable(s ClusterState, score func(id string, slot uint16) uint64) *SlotTable {
	var members []Member
	for _, m := range s.Members {
		if !m.Witness && rankedState(m.State) {
			members = append(members, m)
		}
	}
	slices.SortFunc(members, func(a, b Member) int { return cmp.Compare(a.ID, b.ID) })
	n := len(members)
	t := &SlotTable{members: members, ranks: make([]uint16, tableSlots*n)}
	scores := make([]uint64, n)
	for slot := 0; slot < tableSlots; slot++ {
		row := t.ranks[slot*n : (slot+1)*n]
		for i := range members {
			row[i] = uint16(i)
			scores[i] = score(members[i].ID, uint16(slot))
		}
		// Members are sorted by id, so the lower index is the lower id: a
		// score tie falls back to id bytes ascending.
		slices.SortFunc(row, func(a, b uint16) int {
			if c := cmp.Compare(scores[b], scores[a]); c != 0 {
				return c
			}
			return cmp.Compare(a, b)
		})
	}
	return t
}

func (t *SlotTable) row(slot uint16) []uint16 {
	n := len(t.members)
	if int(slot) >= tableSlots || n == 0 {
		return nil
	}
	return t.ranks[int(slot)*n : (int(slot)+1)*n]
}

// Members returns the ranked members in the order the wire table indexes them
// (sorted by id bytes).
func (t *SlotTable) Members() []Member {
	return slices.Clone(t.members)
}

// Ranking returns every ranked member of the slot, best first, down members
// included. It is nil for a slot outside 0..8191.
func (t *SlotTable) Ranking(slot uint16) []Member {
	row := t.row(slot)
	if row == nil {
		return nil
	}
	out := make([]Member, len(row))
	for i, idx := range row {
		out[i] = t.members[idx]
	}
	return out
}

// Owner is the first name of the copy set (the first rf names of the ranking)
// whose state is not down (conventions section 4). A joining, dead or witness
// member is not in the ranking, so it is never an owner, and a ranked node
// outside the copy set is never one either, because it holds no copy. It
// reports false when rf < 1, the slot is out of range, or every copy is down:
// the slot is then unavailable.
func (t *SlotTable) Owner(slot uint16, rf int) (Member, bool) {
	for _, m := range t.Copies(slot, rf) {
		if m.State != MemberDown {
			return m, true
		}
	}
	return Member{}, false
}

// Copies is the copy set: the first rf names of the ranking, down names
// included, in rank order. Lists for different rf are nested prefixes.
func (t *SlotTable) Copies(slot uint16, rf int) []Member {
	row := t.row(slot)
	if row == nil || rf <= 0 {
		return nil
	}
	out := make([]Member, 0, min(rf, len(row)))
	for _, idx := range row[:min(rf, len(row))] {
		out = append(out, t.members[idx])
	}
	return out
}

// Encode writes the wire table of queries Formats: for each of the 8192 slots,
// count:uvarint then count rank indices:uvarint into Members(), the full
// ranking (a driver must see a down name to skip it), never truncated by RF.
func (t *SlotTable) Encode() []byte {
	n := len(t.members)
	out := make([]byte, 0, tableSlots*(1+n))
	for slot := 0; slot < tableSlots; slot++ {
		out = binary.AppendUvarint(out, uint64(n))
		for _, idx := range t.ranks[slot*n : (slot+1)*n] {
			out = binary.AppendUvarint(out, uint64(idx))
		}
	}
	return out
}

// readCanonicalUvarint reads a uvarint and refuses one that is truncated,
// overflows or is not the shortest encoding, so one table is one byte string.
func readCanonicalUvarint(raw []byte) (uint64, int, bool) {
	v, n := binary.Uvarint(raw)
	if n <= 0 || n != len(binary.AppendUvarint(nil, v)) {
		return 0, 0, false
	}
	return v, n, true
}

// DecodeTable reads a wire table whose indices refer to members, the ranked
// members as the same response's members blob lists them. It refuses, with
// INVALID_ARGUMENT, a members list that holds a witness, a joining or dead
// member or a repeated id; a table that is truncated or has trailing bytes;
// a slot whose count is not the full ranking; and an index that is out of
// range or repeated in a slot. It never panics.
func DecodeTable(raw []byte, members []Member) (*SlotTable, error) {
	n := len(members)
	if n > math.MaxUint16 {
		return nil, stateRefusal("the members list has %d names: a slot table ranks at most %d", n, math.MaxUint16)
	}
	seen := make(map[string]bool, n)
	for _, m := range members {
		if m.Witness || !rankedState(m.State) {
			return nil, stateRefusal("member %q is %v (witness %v): a slot table lists only active, down and leaving data members", m.ID, m.State, m.Witness)
		}
		if seen[m.ID] {
			return nil, stateRefusal("member %q appears twice in the members list: list each ranked member once", m.ID)
		}
		seen[m.ID] = true
	}
	t := &SlotTable{members: slices.Clone(members), ranks: make([]uint16, tableSlots*n)}
	pos := 0
	listed := make([]bool, n)
	for slot := 0; slot < tableSlots; slot++ {
		count, used, ok := readCanonicalUvarint(raw[pos:])
		if !ok {
			return nil, stateRefusal("slot table is cut short or malformed at slot %d (byte %d): fetch the table again with TOPOLOGY epoch 0", slot, pos)
		}
		pos += used
		if count != uint64(n) {
			return nil, stateRefusal("slot %d lists %d names, but the full ranking has %d: the wire table is never truncated by the replication factor", slot, count, n)
		}
		row := t.ranks[slot*n : (slot+1)*n]
		clear(listed)
		for i := range row {
			idx, used, ok := readCanonicalUvarint(raw[pos:])
			if !ok {
				return nil, stateRefusal("slot table is cut short or malformed at slot %d (byte %d): fetch the table again with TOPOLOGY epoch 0", slot, pos)
			}
			pos += used
			if idx >= uint64(n) || listed[idx] {
				return nil, stateRefusal("slot %d names rank index %d, which is out of range or repeated for %d members", slot, idx, n)
			}
			listed[idx] = true
			row[i] = uint16(idx)
		}
	}
	if pos != len(raw) {
		return nil, stateRefusal("slot table has %d trailing bytes after slot 8191: fetch the table again with TOPOLOGY epoch 0", len(raw)-pos)
	}
	return t, nil
}
