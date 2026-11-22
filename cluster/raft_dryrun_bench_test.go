package cluster

import (
	"fmt"
	"testing"
)

// BenchmarkDryRunAgainstTheState measures what the dry run adds to one cluster command: the one clone
// ApplyCommand makes of a state of three members and N databases (each carries seed bitmaps per
// member). It runs once per cluster command (DDL, membership, settings, seed bits), never per
// bucket or per write (spec check 12).
func BenchmarkDryRunAgainstTheState(b *testing.B) {
	for _, databases := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("%d databases", databases), func(b *testing.B) {
			st := fsmCluster(b)
			for i := range databases {
				id := st.NextDatabaseID
				fsmApply(b, &st, CommandCreateDatabase, fsmCreateDatabaseBody(fmt.Sprintf("db%d", i), id, 1))
				for _, node := range []string{"n1", "n2", "n3"} {
					fsmApply(b, &st, CommandSeeded, fsmNodeBitsBody(node, id, fsmBitsAt(5, 6)))
				}
			}
			cmd := Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(st.StateSeq), Body: fsmCreateDatabaseBody("probe", st.NextDatabaseID, 1)}
			b.ReportAllocs()
			for b.Loop() {
				copyOfState := st // shallow, as dryRunOnce does: ApplyCommand clones before it mutates
				if _, err := copyOfState.ApplyCommand(cmd); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
