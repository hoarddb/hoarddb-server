package wire

// expandRef is one id to read: the far bucket, the id's encoding and its slot.
type expandRef struct {
	ref   bucketRef
	encID []byte
	slot  uint16
	// project is the far fields this read returns (nil: the whole body); the same document
	// read with two projections is two reads.
	project []uint16
	// local lets a ready copy on this node answer the read.
	local bool
	// at is the ref's index in the reads readMany is given, where its result goes.
	at int
	// targets are the (spec, id) cells of the page this read answers.
	targets []expandTarget
}

// expandGroup is the distinct refs one owner serves; owner is "" when no member holds the slot.
type expandGroup struct {
	owner string
	refs  []expandRef
}

// groupByOwner splits refs by the owner ownerOf names for each slot. A ref that repeats
// (same bucket, same id, same projection and local) is kept once, so a page of 100 messages by 7 authors is 7 reads.
// Group order is the first appearance of the owner.
func groupByOwner(refs []expandRef, ownerOf func(ref bucketRef, slot uint16) string) []expandGroup {
	type refKey struct {
		bucketID uint64
		id       string
		project  string
		local    bool
	}
	seen := make(map[refKey]bool, len(refs))
	index := map[string]int{}
	var groups []expandGroup
	for _, ref := range refs {
		key := refKey{ref.ref.entry.Decl.BucketID, string(ref.encID), getManyProjectionKey(ref.project), ref.local}
		if seen[key] {
			continue
		}
		seen[key] = true
		owner := ownerOf(ref.ref, ref.slot)
		at, known := index[owner]
		if !known {
			at = len(groups)
			index[owner] = at
			groups = append(groups, expandGroup{owner: owner})
		}
		groups[at].refs = append(groups[at].refs, ref)
	}
	return groups
}

// ownerOfSlot is the member that owns slot at the committed epoch, from the table the node
// already holds: no lookup per id. It is "" when every copy is down.
func (n *Node) ownerOfSlot(ref bucketRef, slot uint16) string {
	row, ok := n.topology.Database(ref.dbID)
	if !ok {
		return ""
	}
	owner, held := n.topology.Table().Owner(slot, row.RF)
	if !held {
		return ""
	}
	return owner.ID
}
