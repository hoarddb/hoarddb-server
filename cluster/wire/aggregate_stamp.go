package wire

import (
	"slices"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// aggregateStampFor is the stamp of op: the ready aggregates of its store field in the declaration the
// write was admitted with, in id order. It is read from that one declaration, once: the dry run at check
// 6a, the Explain text and the record's stamp all use the list this returns, so they cannot differ
// whatever the declaration cache does in between. Nil for a class that touches no store element and for
// a field with none; a retired or undeclared field stamps its set too, and the apply of such a record
// writes nothing before it reads the stamp.
func aggregateStampFor(decl machine.Declaration, op cluster.Op) []cluster.AggregateStamp {
	switch op.Class {
	case cluster.OpStoreVerb, cluster.OpClaim, cluster.OpAck, cluster.OpRelease, cluster.OpExtend:
	default:
		return nil
	}
	if len(op.Fields) != 1 {
		return nil
	}
	set, ok := decl.Aggregates.Set(op.Fields[0].FieldID)
	if !ok {
		return nil
	}
	stamp := make([]cluster.AggregateStamp, 0, len(set.Entries))
	for _, entry := range set.Entries {
		if entry.State == machine.AggStateReady {
			stamp = append(stamp, cluster.AggregateStamp{AggID: entry.ID, Kind: entry.Kind, Field: entry.Field, By: entry.By})
		}
	}
	slices.SortFunc(stamp, func(a, b cluster.AggregateStamp) int { return int(a.AggID) - int(b.AggID) })
	if len(stamp) == 0 {
		return nil
	}
	return stamp
}
