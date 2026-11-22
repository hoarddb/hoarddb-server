package server

import (
	"fmt"
	"net"
	"strconv"
	"testing"
)

// A test node's ring identity is its address, 127.0.0.1 and an ephemeral port
// the kernel chose (reserveNodePorts), and the ring places each of its
// ring.PartitionCount partitions by the node identities alone. A test whose
// setup needs a placement between named nodes (a partition node 1 owns before
// a join and the joiner owns after it) can therefore find none on some port
// draws, however many bucket names it tries: the names already reach every
// partition (BUG-0487 class A).

// placementDraws bounds drawPlaced. Draws are independent: each is fresh
// ports, since a rejected draw keeps its listeners. The setups that use it
// miss their placement on at most about 1.8 percent of draws (the stray-term
// cluster, measured over 20000 random port sets; a joiner taking a partition
// from one named founder misses on about 0.27 percent), so all 16 miss with
// probability below 0.018^16, about 1e-28.
const placementDraws = 16

// drawPlaced returns the first draw whose ring addresses fit, the ring inputs
// of a test's setup chosen before any node starts, so that the placement the
// test needs exists. A draw that does not fit is not reused: its listeners stay
// bound until the test ends, so the next draw cannot be given its ports again.
func drawPlaced[D any](draw func() D, ringAddrs func(D) []string, fits func(addrs []string) bool) (D, error) {
	var tried [][]string
	for attempt := 0; attempt < placementDraws; attempt++ {
		drawn := draw()
		if fits(ringAddrs(drawn)) {
			return drawn, nil
		}
		tried = append(tried, ringAddrs(drawn))
	}
	var none D
	return none, fmt.Errorf("test setup: the placement this test needs exists on no partition of the ring for any of %d draws of "+
		"ring addresses: %v", placementDraws, tried)
}

// newTestClusterPlaced is newTestCluster on ring addresses for which fits
// holds: a test whose setup needs a placement between its founders.
func newTestClusterPlaced(t *testing.T, count, rf int, fits func(addrs []string) bool) *testCluster {
	t.Helper()
	ports, err := drawPlaced(func() []*testNodePorts { return reserveClusterPorts(t, count) }, ringAddrsOf, fits)
	if err != nil {
		t.Fatal(err)
	}
	tc := startTestClusterOn(t, ports, rf, nil, nil)
	tc.waitReady()
	return tc
}

// reserveJoinerPlaced reserves a joiner's ports at a ring address for which
// fits holds: a test whose setup needs a placement between the founders and
// the joiner.
func reserveJoinerPlaced(t *testing.T, fits func(joiner string) bool) *testNodePorts {
	t.Helper()
	ports, err := drawPlaced(func() []*testNodePorts { return []*testNodePorts{reserveNodePorts(t)} }, ringAddrsOf,
		func(addrs []string) bool { return fits(addrs[0]) })
	if err != nil {
		t.Fatal(err)
	}
	return ports[0]
}

// ringAddrsOf is the ring address of each node's ports.
func ringAddrsOf(ports []*testNodePorts) []string {
	addrs := make([]string, 0, len(ports))
	for _, nodePorts := range ports {
		addrs = append(addrs, nodePorts.ringAddr)
	}
	return addrs
}

// joinerFrom is the placement test of
// TestACoordinatorThatHasNotHeardOfTheJoinNeverLandsASchemaChangeAtTheJoiner
// and of the orphan tests: a bucket name founders[from] owns on the founders'
// ring that joiner owns once it joins.
func joinerFrom(t *testing.T, prefix string, founders []string, joiner string, from int) bool {
	t.Helper()
	before := ringWith(t, 3, founders...)
	joined := ringWith(t, 3, append(append([]string(nil), founders...), joiner)...)
	_, found := findBucketName(prefix, func(name string) bool {
		return ownerOn(joined, name) == joiner && ownerOn(before, name) == founders[from]
	})
	return found
}

// fixedDraws returns the address sets in order, one per call, for a
// reproducer that forces the draws a setup is given.
func fixedDraws(t *testing.T, sets ...[]string) func() []string {
	t.Helper()
	next := 0
	return func() []string {
		if next == len(sets) {
			t.Fatalf("the setup asked for draw %d; the reproducer has %d", next+1, len(sets))
		}
		next++
		return sets[next-1]
	}
}

// TestAPlacementDrawRedrawsAnUnsatisfiablePlacement (BUG-0487 class A): the
// first draw of ring addresses has no partition the setup needs, the second
// has. Red where the setup takes the first draw it is given and searches
// names on it: it reports no placement however many names it tries.
func TestAPlacementDrawRedrawsAnUnsatisfiablePlacement(t *testing.T) {
	identity := func(addrs []string) []string { return addrs }
	t.Run("the stray-term cluster", func(t *testing.T) {
		// Five nodes on which no partition is owned by node 5 on the first and
		// committed rings and by node 4 on node 3's set (found by a scratch
		// search over random port sets, about 1.6 percent of draws).
		unlucky := []string{"127.0.0.1:46545", "127.0.0.1:46402", "127.0.0.1:53850", "127.0.0.1:42144", "127.0.0.1:42954"}
		lucky := []string{"127.0.0.1:40001", "127.0.0.1:40011", "127.0.0.1:40021", "127.0.0.1:40031", "127.0.0.1:40041"}
		fabricated := 21000
		fits := func(addrs []string) bool {
			_, found := strayLossPlacement(t, addrs, 3, func() string {
				fabricated++
				return net.JoinHostPort("127.0.0.1", strconv.Itoa(fabricated))
			})
			return found
		}
		if fits(unlucky) || !fits(lucky) {
			t.Fatalf("the reproducer's draws are not what it needs: the unlucky one fits %v, the lucky one %v", fits(unlucky), fits(lucky))
		}
		drawn, err := drawPlaced(fixedDraws(t, unlucky, lucky), identity, fits)
		if err != nil {
			t.Fatalf("the stray-term setup was given an unsatisfiable draw and did not draw again: %v", err)
		}
		if fmt.Sprint(drawn) != fmt.Sprint(lucky) {
			t.Errorf("drew %v, want the second draw %v", drawn, lucky)
		}
	})
	t.Run("a join that node 1 gives the joiner nothing", func(t *testing.T) {
		// Three founders and a joiner where the joiner takes no partition
		// from node 1 (about 0.27 percent of draws).
		founders := []string{"127.0.0.1:48939", "127.0.0.1:44172", "127.0.0.1:60263"}
		unlucky, lucky := "127.0.0.1:52990", "127.0.0.1:40001"
		fits := func(addrs []string) bool { return joinerFrom(t, "across", founders, addrs[0], 0) }
		if fits([]string{unlucky}) || !fits([]string{lucky}) {
			t.Fatalf("the reproducer's draws are not what it needs: the unlucky one fits %v, the lucky one %v",
				fits([]string{unlucky}), fits([]string{lucky}))
		}
		drawn, err := drawPlaced(fixedDraws(t, []string{unlucky}, []string{lucky}), identity, fits)
		if err != nil {
			t.Fatalf("the join setup was given an unsatisfiable joiner and did not draw again: %v", err)
		}
		if drawn[0] != lucky {
			t.Errorf("drew joiner %s, want the second draw %s", drawn[0], lucky)
		}
	})
}
