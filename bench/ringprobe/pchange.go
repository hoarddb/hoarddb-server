package ringprobe

import "testing"

// totalSlots is Part B's fixed slot count S = 4096 (entity-placement-proposal.md
// Q7, "P = S = 4,096, fixed"). This probe holds S fixed and grows the ring's
// partition count P to measure the churn Q7's arithmetic warns about: a slot's
// governing partition is s >> (log2(S) - log2(P)) (G-05/G-07's shift relation),
// so growing P changes which partition — and therefore which ring position,
// and therefore which node — governs a slot.
const totalSlots = 4096

// pChangeSteps is the P sequence the review measured: 256 -> 512 -> 1024 -> 4096.
// Every value must be a power of two dividing totalSlots for slotShift to be
// exact; 271 (the server's actual PartitionCount) is deliberately excluded
// here because it cannot express the slot-to-partition shift the proposal's
// P = S design relies on.
var pChangeSteps = []int{256, 512, 1024, 4096}

// log2 returns the base-2 logarithm of x, which must be an exact power of two.
func log2(x int) int {
	n := 0
	for x > 1 {
		x >>= 1
		n++
	}
	return n
}

// slotShift returns the right-shift that maps a slot id in [0, totalSlots) to
// a partition id in [0, p), per the proposal's p = s >> (log2(S) - log2(P)).
func slotShift(p int) int {
	return log2(totalSlots) - log2(p)
}

// runPChangeStep measures, for an n-member ring holding membership fixed,
// the proportion of stored copies that must move when the ring's partition
// count grows from "from" to "to". A copy is one (slot, replica-holder) pair;
// it counts as moved when the holder is present in the new replicationFactor-wide
// replica set for that slot's new partition but was not present in the old
// one.
func runPChangeStep(tb testing.TB, n, from, to int, style nodeIDStyle) float64 {
	tb.Helper()
	members := buildMembers(n, style)
	ringFrom := newConsistent(members, from)
	ringTo := newConsistent(members, to)
	shiftFrom := slotShift(from)
	shiftTo := slotShift(to)

	movedCopies, totalCopies := 0, 0
	for slot := 0; slot < totalSlots; slot++ {
		holdersFrom, err := ringFrom.GetClosestNForPartition(slot>>shiftFrom, replicationFactor)
		if err != nil {
			tb.Fatalf("ringprobe: slot %d partition %d (P=%d, n=%d): %v", slot, slot>>shiftFrom, from, n, err)
		}
		holdersTo, err := ringTo.GetClosestNForPartition(slot>>shiftTo, replicationFactor)
		if err != nil {
			tb.Fatalf("ringprobe: slot %d partition %d (P=%d, n=%d): %v", slot, slot>>shiftTo, to, n, err)
		}
		before := holderSet(holdersFrom)
		for _, h := range holdersTo {
			totalCopies++
			if _, kept := before[h.String()]; !kept {
				movedCopies++
			}
		}
	}
	return float64(movedCopies) / float64(totalCopies)
}
