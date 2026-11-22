package wire

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Seeding slice S4, step B1: the node's seed byte rate paces the pull that receives the bytes
// (the entry ring seeding completion spec §4). The pull runs on a virtual clock, so a wait
// costs no real time.

// s4PaceRate is the byte rate of the pacing tests: slow enough that a chunk of three entries
// costs a couple of seconds on the virtual clock, far under the idle period.
const s4PaceRate = 40

// s4PacedPull pulls ten keys in chunks of three from one source, over a pacer on clock. It
// returns the entries the source held and the clock time of every SEED_ACK, in order.
func s4PacedPull(t *testing.T, clock *s1VirtualClock) ([]transport.SeedEntry, []time.Time) {
	t.Helper()
	pacer := newSeedPacer(seedPacerConfig{BytesPerSecond: s4PaceRate, Now: clock.Now, Sleep: clock.Sleep})
	rig := newT3Rig(t, t3Options{Pace: pacer})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 10, t3Cut(t, rig), 0)
	var acks []time.Time
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if msg.Env.Kind == transport.KindSeedAck {
			acks = append(acks, clock.Now())
		}
		return reply, err
	}
	rig.pullUntil("the pair to be ready", rig.isReady)
	return owner.dump(t, rig.slot, reconDB), acks
}

// s4ChunkCosts is what each chunk of three entries costs at the pacing rate, in the order the
// source sends them.
func s4ChunkCosts(entries []transport.SeedEntry) []time.Duration {
	var costs []time.Duration
	for start := 0; start < len(entries); start += 3 {
		bytes := 0
		for _, entry := range entries[start:min(start+3, len(entries))] {
			bytes += entryBytes(entry.Key, entry.Value)
		}
		costs = append(costs, time.Duration(float64(bytes)/float64(s4PaceRate)*float64(time.Second)))
	}
	return costs
}

// TestAPullWaitsForItsShareBeforeTheNextAck: the SEED_ACK after a chunk leaves only once that
// chunk's share of the rate has passed, and the last chunk, after which nothing is asked, is
// the one that is not waited for. [BREAK: skip the Wait in Puller.stream]
func TestAPullWaitsForItsShareBeforeTheNextAck(t *testing.T) {
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	entries, acks := s4PacedPull(t, clock)
	costs := s4ChunkCosts(entries)
	if len(acks) < 3 {
		t.Fatalf("%d SEED_ACKs for ten keys in chunks of three, want at least three", len(acks))
	}
	if gap := acks[1].Sub(acks[0]); gap+time.Microsecond < costs[0] {
		t.Errorf("the second SEED_ACK followed the first by %v, want at least the first chunk's share %v", gap, costs[0])
	}
	var full time.Duration
	for _, cost := range costs {
		full += cost
	}
	if slept, want := clock.total(), full-costs[len(costs)-1]; slept+time.Microsecond < want {
		t.Errorf("the pull waited %v in all, want at least %v: the range's share less the last chunk's", slept, want)
	}
}

// TestAPullChargesTheChunkItApplied: the waits of a pull add up to the range's bytes at the
// rate, within one chunk. [BREAK: charge nothing, by dropping the Spent call in Puller.takeChunk]
func TestAPullChargesTheChunkItApplied(t *testing.T) {
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	entries, _ := s4PacedPull(t, clock)
	costs := s4ChunkCosts(entries)
	var full, largest time.Duration
	for _, cost := range costs {
		full += cost
		largest = max(largest, cost)
	}
	if slept := clock.total(); slept < full-largest || slept > full {
		t.Errorf("the pull waited %v, want the range's share %v within one chunk (%v)", slept, full, largest)
	}
}
