package wire

import (
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// seedRowKey names one committed SEEDED row.
type seedRowKey struct {
	node string
	db   int64
}

// seedBitRows indexes ClusterState.SeedBitmaps once, so a caller that reads many slots does
// not rescan the rows for each.
type seedBitRows map[seedRowKey][]byte

// newSeedBitRows indexes the committed SEEDED rows of state. The bitmaps are shared with
// state and must not be written.
func newSeedBitRows(state cluster.ClusterState) seedBitRows {
	rows := make(seedBitRows, len(state.SeedBitmaps))
	for _, row := range state.SeedBitmaps {
		rows[seedRowKey{node: row.Node, db: row.DBID}] = row.Bits
	}
	return rows
}

// Holds says whether node has the committed SEEDED bit for slot of database db. An absent
// row, a short row and a row the machine cannot read all answer false: a bit the gate cannot
// read is a pair not yet reported, so the joiner waits rather than activating on it.
func (rows seedBitRows) Holds(node string, db int64, slot uint16) bool {
	bits, ok := rows[seedRowKey{node: node, db: db}]
	if !ok {
		return false
	}
	on, err := machine.Bit(bits, slot)
	return err == nil && on
}

// joinerGap is what a joining member has not yet reported ready.
type joinerGap struct {
	Missing, Total int
	Database       string // the first database with a missing pair, "" when none
}

// activationGap counts the pairs id will hold at its activation and how many of them have no
// committed SEEDED bit for id. It reads only state: no record, no round trip to the joiner.
func activationGap(state cluster.ClusterState, id string) joinerGap {
	table := reconcilePlacingTable(state, id)
	rows := newSeedBitRows(state)
	var gap joinerGap
	for _, database := range state.Databases {
		for slot := range uint16(storage.SlotCount) {
			for _, copyOf := range table.Copies(slot, int(database.RF)) {
				if copyOf.ID != id {
					continue
				}
				gap.Total++
				if !rows.Holds(id, database.ID, slot) {
					gap.Missing++
					if gap.Database == "" {
						gap.Database = database.Name
					}
				}
				break
			}
		}
	}
	return gap
}
