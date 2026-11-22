package server

import (
	"fmt"
	"slices"
	"strings"
)

// Step 10b.8: what "the hand-off has settled" reads in the tests that prove
// it (spec §20.16, T-133; T-97 widened): among others, the node's
// hoarddb_handoffs_in_progress gauge, as an operator reads it.

// unsettledOn is how many copies a node still holds a hand-off record for,
// as its hoarddb_handoffs_in_progress gauge serves it.
func unsettledOn(s *Server) (int, error) {
	families, err := metricsGatherer(s).Gather()
	if err != nil {
		return 0, fmt.Errorf("gathering %s's metrics: %w", s.cluster.self.ID, err)
	}
	for _, family := range families {
		if family.GetName() == "hoarddb_handoffs_in_progress" && len(family.GetMetric()) == 1 {
			return int(family.GetMetric()[0].GetGauge().GetValue()), nil
		}
	}
	return 0, fmt.Errorf("%s serves no hoarddb_handoffs_in_progress gauge", s.cluster.self.ID)
}

// handOffUnsettled says what is not settled yet across members, by node ID,
// for buckets of database: a member with a placement mark left; a bucket whose
// owner has not taken it over; a member of a bucket's set that does not hold
// it or does not follow its owner's term. Every member and every bucket is
// asserted — a copy that never settles is not filtered out. The empty string
// is a settled cluster.
func handOffUnsettled(members map[string]*Server, database string, buckets []string) string {
	var behind []string
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		marked, err := unsettledOn(members[id])
		switch {
		case err != nil:
			behind = append(behind, err.Error())
		case marked != 0:
			behind = append(behind, fmt.Sprintf("%s holds %d unsettled copies", id, marked))
		}
	}
	placer := members[ids[0]].cluster
	for _, bucket := range buckets {
		key := placementOf(database, bucket)
		set, err := placer.Replicas(key)
		if err != nil {
			behind = append(behind, fmt.Sprintf("placing %s: %v", bucket, err))
			continue
		}
		owner, known := members[set[0].ID]
		if !known {
			behind = append(behind, fmt.Sprintf("the owner of %s, %s, is not a running member", bucket, set[0].ID))
			continue
		}
		ownerTerm, opened, err := owner.bucketLineage(database, bucket)
		if err != nil || !opened || owner.cluster.placementMarksOf(key).Moved != 0 {
			behind = append(behind, fmt.Sprintf("%s has not taken %s over (opened %v, %v)", set[0].ID, bucket, opened, err))
			continue
		}
		for _, member := range set[1:] {
			follower, known := members[member.ID]
			if !known {
				behind = append(behind, fmt.Sprintf("%s, a member of %s's set, is not a running member", member.ID, bucket))
				continue
			}
			if !follower.bucketStoreOpen(database, bucket) {
				behind = append(behind, fmt.Sprintf("%s does not hold %s", member.ID, bucket))
				continue
			}
			term, _, err := follower.bucketLineage(database, bucket)
			marks := follower.cluster.placementMarksOf(key)
			if err != nil || term != ownerTerm || marks != (placementMarks{}) {
				behind = append(behind, fmt.Sprintf("%s's copy of %s is in term %d with marks %+v, its owner %s in term %d (%v)",
					member.ID, bucket, term, marks, set[0].ID, ownerTerm, err))
			}
		}
	}
	return strings.Join(behind, "; ")
}
