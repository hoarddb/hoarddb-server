// Package ringprobe reproduces, as committed and reproducible Go tests, the
// two consistent-hash placement measurements that the entity-placement-rev3
// design review found asserted from an uncommitted scratch script rather than
// backed by a test in this repository:
//
//   - TestLeaveEveryMember (leave_test.go) reproduces the review's
//     `ringprobe-rev2/rev2_test.go:TestLeaveEveryMember`: for every member of
//     a ring, remove it and measure the proportion of partitions whose new
//     owner holds none of the copies the partition held immediately before.
//     Cited in the entity-placement-proposal record §B.4 (the "Node
//     leaves" row) as "median ~6-7%, p90 ~9%, up to ~16.6%", and in
//     the entity-placement-rev3-gemini record finding 2.1 as
//     "a median of 5.8%-7.0%, a p90 of ~9%, and a maximum of 16.6%".
//
//   - TestChangingP (pchange_test.go) reproduces the review's
//     `ringprobe-rev2/pchange_test.go:TestChangingP`: holding membership
//     fixed, grow the ring's partition count P and measure the proportion of
//     stored copies that land on a different node than before. Cited in
//     §B.4 and Q7 as "40-88% of stored copies" (N = 5-25), and in the
//     Gemini review finding 2.2 as "~39.4%-40.2%" at N=5 and "87.0%-88.1%" at
//     N=25.
//
// Both dispositions are in the entity-placement-rev3-dispositions record,
// G-05 and G-06: both accepted, both remedied by committing the probes here.
//
// # Reusing the production ring construction
//
// Both probes build the ring the same way the server does: github.com/buraksezer/consistent,
// hashed with ring.XXHasher, at the production virtual-node count and load
// factor (ring.Config()). ring.Ring itself cannot be driven directly here,
// because it hardcodes PartitionCount at the unexported package constant
// ring.PartitionCount inside ring.Config(), with no constructor argument to
// vary it — and the dispatch that produced this package forbids touching
// ring/ring.go to add one. newConsistent (helpers.go) calls ring.Config() and
// overrides only PartitionCount, so every other knob (hasher, virtual nodes,
// load factor) is identical to what Ring.rebuildLocked builds. This is a
// finding worth recording rather than working around: a proposal or a future
// probe that needs to vary P should either get a ring.NewWithPartitions
// constructor, or continue building consistent.Consistent directly as this
// package does.
//
// # Replication factor is an assumption this package makes, not a recorded one
//
// Neither the review nor the accepted dispositions record what replication
// factor (RF) the original scratch probes used when deciding whether a node
// "holds a copy" of a partition. This package uses replicationFactor = 3,
// matching the proposal's own worked example (§B.4's "Replica set (RF=3)"
// row). A different RF changes both measurements' figures, because the
// "previous copies" set (TestLeaveEveryMember) and the "stored copies" being
// counted (TestChangingP) are both RF-wide. See the commit message and the
// dispatch report for the measured figures at RF=3 and how they compare to
// the numbers the review recorded.
package ringprobe
