package ringprobe

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/ring"
)

// leaveSample is one (N, P, id style, leaver) observation: the proportion of
// partitions whose post-removal owner held none of the replicationFactor
// copies the partition held immediately before that member left.
type leaveSample struct {
	n          int
	p          int
	style      nodeIDStyle
	leaverID   string
	proportion float64
}

// runLeaveEveryMember reproduces the review's TestLeaveEveryMember for one
// (n, p, style) combination: it builds an n-member ring at partition count p,
// records who holds each partition's replicationFactor copies, then removes
// every member in turn and measures, for each removal, the proportion of the
// p partitions whose new owner is not among the copies the partition held
// before that member left. It returns one leaveSample per removal (n samples).
//
// tb reports a fatal test failure if the ring cannot answer
// GetClosestNForPartition or GetPartitionOwner for a valid partition id at
// this n and p — both are guaranteed by the consistent library's contract
// once n >= replicationFactor, which every (n, p) pair this package tests
// satisfies (n starts at 4).
func runLeaveEveryMember(tb testing.TB, n, p int, style nodeIDStyle) []leaveSample {
	tb.Helper()
	members := buildMembers(n, style)
	before := newConsistent(members, p)

	ownerBefore := make([]string, p)
	copiesBefore := make([]map[string]struct{}, p)
	for part := 0; part < p; part++ {
		owner := before.GetPartitionOwner(part)
		if owner == nil {
			tb.Fatalf("ringprobe: partition %d has no owner at n=%d p=%d", part, n, p)
		}
		ownerBefore[part] = owner.String()

		holders, err := before.GetClosestNForPartition(part, replicationFactor)
		if err != nil {
			tb.Fatalf("ringprobe: partition %d replica lookup at n=%d p=%d: %v", part, n, p, err)
		}
		copiesBefore[part] = holderSet(holders)
	}

	samples := make([]leaveSample, 0, n)
	for leaverIdx, leaver := range members {
		remaining := make([]ring.Node, 0, n-1)
		for i, m := range members {
			if i != leaverIdx {
				remaining = append(remaining, m)
			}
		}
		after := newConsistent(remaining, p)

		noCopy := 0
		for part := 0; part < p; part++ {
			newOwner := after.GetPartitionOwner(part)
			if newOwner == nil {
				tb.Fatalf("ringprobe: partition %d has no owner after removing %s (n=%d p=%d)", part, leaver.ID, n, p)
			}
			newOwnerID := newOwner.String()
			if newOwnerID == ownerBefore[part] {
				continue // owner unchanged: it already held a copy.
			}
			if _, held := copiesBefore[part][newOwnerID]; !held {
				noCopy++
			}
		}
		samples = append(samples, leaveSample{
			n:          n,
			p:          p,
			style:      style,
			leaverID:   leaver.ID,
			proportion: float64(noCopy) / float64(p),
		})
	}
	return samples
}
