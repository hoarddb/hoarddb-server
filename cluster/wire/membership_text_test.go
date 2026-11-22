package wire

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestTheJoinRefusalNamesTheReleaseWhenKnownAndTheFallbackWhenNot is the join half of mixed-build
// spec 9.3 item 4 and test 21: the text names the first release that carries the cluster's version
// when the release table records one, and says how to find the build when it does not.
func TestTheJoinRefusalNamesTheReleaseWhenKnownAndTheFallbackWhenNot(t *testing.T) {
	const id, addr = "n2", "n2-host:7000"
	below := cluster.StateRange{Min: 1, Max: 1, Writes: 1}
	leader := cluster.StateRange{Min: 2, Max: 2, Writes: 2}

	known := joinRangeText(joinRefusal{ID: id, Addr: addr, Build: "devel", Joiner: below, Leader: leader, Release: "1.2.0"})
	wantKnown := "cannot join n2@n2-host:7000: node n2 runs build devel, which supports cluster versions 1 to 1, " +
		"and this cluster runs version 2 (first released in 1.2.0). Upgrade n2 to 1.2.0 or later, " +
		"then run 'join n2@n2-host:7000' again."
	if got := knownMessage(t, known); got != wantKnown {
		t.Fatalf("the text is\n%s\nwant\n%s", got, wantKnown)
	}

	unknown := joinRangeText(joinRefusal{ID: id, Addr: addr, Build: "devel", Joiner: below, Leader: leader})
	wantUnknown := "cannot join n2@n2-host:7000: node n2 runs build devel, which supports cluster versions 1 to 1, " +
		"and this cluster runs version 2. This build records no release for version 2: run hoarddb-server -v " +
		"on a cluster member and start n2 on that build or later, then run 'join n2@n2-host:7000' again."
	unknownText := knownMessage(t, unknown)
	if unknownText != wantUnknown {
		t.Fatalf("the text is\n%s\nwant\n%s", unknownText, wantUnknown)
	}
	if strings.Contains(unknownText, "first released") {
		t.Fatal("a release the table does not record was named")
	}

	newer := cluster.StateRange{Min: 2, Max: 2, Writes: 2}
	ahead := joinRangeText(joinRefusal{ID: id, Addr: addr, Build: "devel", Joiner: newer, Leader: cluster.StateRange{Min: 1, Max: 1, Writes: 1}, Release: "1.2.0"})
	if got := knownMessage(t, ahead); !strings.Contains(got, "Start n2 on 1.2.0, the release that carries version 1, or upgrade every member") {
		t.Fatalf("a newer joiner with a known release is not told which release to start: %s", got)
	}

	// Spec 9.3 case 2: with no release recorded, a newer joiner is told to start on the cluster's build.
	aheadUnknown := joinRangeText(joinRefusal{ID: id, Addr: addr, Build: "devel", Joiner: newer, Leader: cluster.StateRange{Min: 1, Max: 1, Writes: 1}})
	wantAhead := "start n2 on the same build as the cluster, or upgrade every member to n2's build first, then run 'join n2@n2-host:7000' again."
	if got := knownMessage(t, aheadUnknown); !strings.Contains(got, wantAhead) {
		t.Fatalf("the newer joiner's fallback is\n%s\nwant it to contain\n%s", got, wantAhead)
	}
}

// knownMessage returns the refusal's message, failing the test when the join refused nothing.
func knownMessage(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		t.Fatal("the join was not refused")
	}
	return err.Error()
}
