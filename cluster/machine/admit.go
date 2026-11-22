package machine

import "github.com/hoarddb/hoarddb-server/cluster"

// Admission is everything the owner's ordering node knows when it admits one
// operation (conventions §8). The caller reads the world and fills it in; Check
// reads nothing else.
type Admission struct {
	FromNode              bool
	FrameEpoch, Committed uint64
	ClientEpoch           uint64 // 0 = route it for me
	Hold                  bool   // M4 says this pair is held
	Rank1                 bool
	LeaseValid            bool
	// Lease is the lease decision LeaseValid was read from. Check does not read it: a refusal
	// that names the lapse prints this decision's figures, never a second serve check's.
	Lease LeaseDecision
	// QuorumLost is true when this node has not confirmed a voter majority within the
	// quorum bound (membership §8); it is ignored for a one-member cluster.
	QuorumLost        bool
	Members           int
	Present           bool        // an admission entry exists (a cache entry, or the synthetic one)
	Missing           bool        // the declaration fetch found no such bucket
	Decl              Declaration // the entry this operation was admitted with, pending table included
	Seen              int64       // the entry's schema_version_seen (conv §20)
	Published         int64       // this owner's PUBLISHED schema_version, read at this instant (§20 step 6)
	ReAdmitted        bool        // the one transparent re-admission already happened
	PayloadBucketID   uint64
	AdmittedVersion   int64 // the effective declaration version the operation carries (Formats 1)
	WrittenFieldIDs   []uint16
	Deduped           bool
	Hop               uint8
	PreconditionFails bool // 6a: duplicate _id_, absent document, no such lease (the caller's own state)
}

// AdmissionResult is the decision of Check. Code 0 with no flag set is "admitted".
type AdmissionResult struct {
	Code       cluster.Code // 0 = no refusal
	Hold       bool         // join the hold buffer, no position assigned
	Forward    bool         // this node is not the owner
	MarkBehind bool
	Deduped    bool
	ReAdmit    bool // leave the section (no position taken), refetch, re-enter once with the new seen
	// PreconditionRefused is check 6a: no code and no position, the caller returns its own
	// refusal. It is a flag because the caller has to tell it from "admitted".
	PreconditionRefused bool
	EffectiveVersion    int64
}

// Admitted reports that every check passed, so the caller takes a position.
func (r AdmissionResult) Admitted() bool {
	return r == AdmissionResult{EffectiveVersion: r.EffectiveVersion}
}

// FenceResult is the decision of the schema fence (check 4). Code 0 with ReAdmit false
// means the declaration is current.
type FenceResult struct {
	Code    cluster.Code
	ReAdmit bool
}

// Check is conventions §8's ordered checks 1, 2, 3, 3a, 4, 5, 6 and 6a. The first failing
// check is the one reported, and a refusal changes no state.
func Check(a Admission) AdmissionResult {
	if r, stop := checkEpoch(a); stop {
		return r
	}
	if a.Hold {
		return AdmissionResult{Hold: true}
	}
	if !a.Rank1 {
		if a.Hop >= 1 {
			return AdmissionResult{Code: cluster.CodeNotOwner}
		}
		if a.Members > 1 && !a.LeaseValid {
			return AdmissionResult{Code: cluster.CodeLeaseLapsed}
		}
		return AdmissionResult{Forward: true}
	}

	fence := Fence(a)
	decision := AdmissionResult{Code: fence.Code, ReAdmit: fence.ReAdmit}
	if a.Present && !a.Missing {
		_, decision.EffectiveVersion = EffectiveTable(a.Decl, a.Published)
	}
	if fence.Code != 0 || fence.ReAdmit {
		return decision
	}

	switch {
	case a.Members > 1 && !a.LeaseValid:
		decision.Code = cluster.CodeLeaseLapsed
	case a.Members > 1 && a.QuorumLost:
		decision.Code = cluster.CodeNoQuorum
	case a.Deduped:
		decision.Deduped = true
	case a.PreconditionFails:
		decision.PreconditionRefused = true
	}
	return decision
}

// checkEpoch is check 1. Node rows apply to a node frame, client rows to a client
// prologue; the other side's epoch is ignored.
func checkEpoch(a Admission) (AdmissionResult, bool) {
	if a.FromNode {
		switch {
		case a.FrameEpoch < a.Committed:
			return AdmissionResult{Code: cluster.CodeEpochFenced}, true
		case a.FrameEpoch > a.Committed:
			return AdmissionResult{Code: cluster.CodeEpochFenced, MarkBehind: true}, true
		}
		return AdmissionResult{}, false
	}
	switch {
	case a.ClientEpoch != 0 && a.ClientEpoch < a.Committed:
		return AdmissionResult{Code: cluster.CodeStaleEpoch}, true
	case a.ClientEpoch > a.Committed:
		return AdmissionResult{Code: cluster.CodeStaleEpoch, MarkBehind: true}, true
	}
	return AdmissionResult{}, false
}

// Fence is check 4 whole: the OWNER's schema fence, evaluated at admission and once more
// under the pair lock immediately before the append, with Published re-read at that
// instant. Nothing but the owner's ordering path may call it: a copy applying a record, a
// replay, a catch-up and a seed drain evaluate no fence (conv §20), and there is no
// apply-side variant in this package.
func Fence(a Admission) FenceResult {
	if a.Missing {
		return FenceResult{Code: cluster.CodeNotFound}
	}
	if a.Decl.State == StateDropping {
		return FenceResult{Code: cluster.CodeBucketDropping}
	}
	if !a.Present {
		// A cold owner fetches rather than refuses (conv §8 check 4); the fetch is the re-admit.
		if !a.ReAdmitted {
			return FenceResult{ReAdmit: true}
		}
		return FenceResult{Code: cluster.CodeSchemaNotCurrent}
	}

	table, version := EffectiveTable(a.Decl, a.Published)
	if a.Seen < a.Published && !a.ReAdmitted &&
		a.Decl.BucketID == a.PayloadBucketID && version == a.AdmittedVersion {
		// The database moved and this bucket's own table did not: no position is taken yet,
		// so leaving the section leaves nothing to undo.
		return FenceResult{ReAdmit: true}
	}
	if a.Seen != a.Published || version > a.Published ||
		a.Decl.State != StateActive || a.Decl.BucketID != a.PayloadBucketID {
		return FenceResult{Code: cluster.CodeSchemaNotCurrent}
	}
	for _, id := range a.WrittenFieldIDs {
		if !writable(table, id) && !systemOnlyField(a.Decl.BucketID, id) && !systemWritable(a.Decl.BucketID, id) {
			return FenceResult{Code: cluster.CodeFieldType}
		}
	}
	return FenceResult{}
}

// SystemExpireField is the id of the expiry table in a _buckets document (declaration field 11). The
// compiled system table does not list it, so the field-id check admits it for the system bucket by
// name instead.
const SystemExpireField uint16 = 11

// systemOnlyField reports whether id is a _buckets field the compiled system table leaves out.
func systemOnlyField(bucketID uint64, id uint16) bool {
	return bucketID == SystemBucketID && id == SystemExpireField
}

// writable reports whether id is declared in the effective table and not retired.
func writable(table FieldTable, id uint16) bool {
	for _, retired := range table.Retired {
		if retired.ID == id {
			return false
		}
	}
	for _, entry := range table.Entries {
		if entry.ID == id {
			return true
		}
	}
	return false
}

// SystemView is the admission entry of the _buckets system bucket (conv §19): the compiled-in
// declaration with seen = published, never fetched and never cached, so the seen and bucket id
// clauses hold by construction and only the field-id check can refuse.
func SystemView(published int64) (Declaration, int64) {
	return SystemDeclaration(), published
}
