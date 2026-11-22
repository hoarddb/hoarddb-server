package wire

import "github.com/hoarddb/hoarddb-server/cluster"

// catchUpEligible is the CATCHUP_REQ role check: id may pull the retained log of
// (slot) only when it is a copy of the pair at the committed epoch, which includes the
// new owner while a handoff barrier is open, or was one at the epoch before, so a copy
// that has not yet applied an epoch change is still served. now and before are the slot
// tables of those epochs and rf and beforeRF the pair's replication factors in them;
// before may be nil. A joining or dead member is never in a table, so a gainer being
// seeded is not served here: it pulls over the seed channel.
//
// Cost: two copy-set lookups of at most rf names; nothing is per bucket.
func catchUpEligible(now, before *cluster.SlotTable, rf, beforeRF int, slot uint16, id string) bool {
	return isCopy(now, rf, slot, id) || (before != nil && isCopy(before, beforeRF, slot, id))
}

func isCopy(table *cluster.SlotTable, rf int, slot uint16, id string) bool {
	if table == nil || rf <= 0 {
		return false
	}
	for _, member := range table.Copies(slot, rf) {
		if member.ID == id {
			return true
		}
	}
	return false
}
