package cli

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
)

// The REPLICATION column of `show buckets` (bucket-replication-rev8-spec
// §12.1's rendering table; the G4 to G7 build doc step 9).

// TestTheRenderingTableRendersEveryOwnerRow is the table: one assertion per
// row, in the words the spec gives.
func TestTheRenderingTableRendersEveryOwnerRow(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	ago := func(age time.Duration) int64 { return now.Add(-age).Unix() }
	cases := []struct {
		row  string
		info server.BucketInfo
		want string
	}{
		{"owner, healthy", server.BucketInfo{ReplicationFactor: 3, InSync: 3, State: server.StateOK}, "rf 3 (3/3)"},
		{"owner, a copy pending", server.BucketInfo{
			ReplicationFactor: 3, InSync: 1, State: server.StateCopying,
			PendingCopies: []server.PendingCopy{{Node: "node-b", Since: ago(4 * time.Hour)}, {Node: "node-c", Since: ago(12 * time.Minute)}},
		}, "rf 3 (1/3, copying to node-b (4h), node-c (12m))"},
		{"owner, a stalled addressee", server.BucketInfo{
			ReplicationFactor: 3, InSync: 2, State: server.StateUnderReplicated, NotInSync: []string{"node-c"},
		}, "rf 3 (2/3, node-c not in sync)"},
		{"owner, fewer members than the factor", server.BucketInfo{
			ReplicationFactor: 3, InSync: 2, State: server.StateUnderReplicated,
		}, "rf 3 (2/3, under-replicated: 2 members)"},
		{"standalone", server.BucketInfo{ReplicationFactor: 3, InSync: 1, State: server.StateStandalone}, "rf 3 (standalone: 1 copy)"},
		{"non-owner holder", server.BucketInfo{ReplicationFactor: 3, State: server.StateOwnerElsewhere}, "rf 3 (owner elsewhere)"},
		{"non-owner, its copy outside", server.BucketInfo{ReplicationFactor: 3, State: server.StateOwnerElsewhere, Outside: true}, "rf 3 (leaving)"},
	}
	for _, tc := range cases {
		t.Run(tc.row, func(t *testing.T) {
			if got := replicationColumn(tc.info, now); got != tc.want {
				t.Errorf("the %s row prints %q, want %q", tc.row, got, tc.want)
			}
		})
	}
}

// TestAnInheritingBucketKeepsTheInheritedWord: a bucket on the node's own
// factor says so inside the parenthesis, with a pending copy and without.
func TestAnInheritingBucketKeepsTheInheritedWord(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		info server.BucketInfo
		want string
	}{
		{"healthy", server.BucketInfo{ReplicationFactor: 1, Inherited: true, InSync: 1, State: server.StateOK}, "rf 1 (inherited, 1/1)"},
		{"copying", server.BucketInfo{
			ReplicationFactor: 3, Inherited: true, InSync: 1, State: server.StateCopying,
			PendingCopies: []server.PendingCopy{{Node: "node-b", Since: now.Add(-4 * time.Hour).Unix()}},
		}, "rf 3 (inherited, 1/3, copying to node-b (4h))"},
		{"non-owner", server.BucketInfo{ReplicationFactor: 3, Inherited: true, State: server.StateOwnerElsewhere}, "rf 3 (inherited, owner elsewhere)"},
		{"standalone at one", server.BucketInfo{ReplicationFactor: 1, Inherited: true, InSync: 1, State: server.StateStandalone}, "rf 1 (inherited)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := replicationColumn(tc.info, now); got != tc.want {
				t.Errorf("the %s row prints %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

// TestACopysAgeIsRoundedToTheLargestWholeUnit: 45s, 12m, 4h, 3d, and never a
// negative age when the owner's clock is ahead of this one.
func TestACopysAgeIsRoundedToTheLargestWholeUnit(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	cases := map[time.Duration]string{
		45 * time.Second:                "45s",
		12*time.Minute + 40*time.Second: "12m",
		4*time.Hour + 59*time.Minute:    "4h",
		3*24*time.Hour + 23*time.Hour:   "3d",
		-30 * time.Second:               "0s",
	}
	for age, want := range cases {
		if got := formatCopyAge(now.Add(-age).Unix(), now); got != want {
			t.Errorf("a copy placed %v ago prints %q, want %q", age, got, want)
		}
	}
}

// TestAServerThatSendsNoCountsKeepsTheOldColumn: a server that predates the
// fields sends no State, and its rows say what they said before.
func TestAServerThatSendsNoCountsKeepsTheOldColumn(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	cases := map[string]server.BucketInfo{
		"rf 3":                      {ReplicationFactor: 3},
		"rf 1 (inherited)":          {ReplicationFactor: 1, Inherited: true},
		"-":                         {},
		"rf 3, leaving":             {ReplicationFactor: 3, Outside: true},
		"rf 1 (inherited), leaving": {ReplicationFactor: 1, Inherited: true, Outside: true},
	}
	for want, info := range cases {
		if got := replicationColumn(info, now); got != want {
			t.Errorf("%+v prints %q, want %q", info, got, want)
		}
	}
}
