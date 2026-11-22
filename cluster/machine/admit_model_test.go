package machine

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// admitEntryClass is one class of admission entry the decision table distinguishes.
type admitEntryClass int

const (
	admitEntMissing admitEntryClass = iota
	admitEntCold
	admitEntDropping
	admitEntUnknownState
	admitEntSeenAhead
	admitEntBehindEligible // seen < s, own bucket id and version unchanged
	admitEntBehindOtherBucket
	admitEntBehindVersionMoved
	admitEntBehindVersionAhead // seen < s and the table's version is above s
	admitEntVersionAhead       // seen == s and the table's version is above s
	admitEntBucketMismatch
	admitEntFieldBad
	admitEntOK
	admitEntClassCount
)

func (e admitEntryClass) String() string {
	return [...]string{"missing", "cold", "dropping", "unknown-state", "seen-ahead", "behind-eligible",
		"behind-other-bucket", "behind-version-moved", "behind-version-ahead", "version-ahead",
		"bucket-mismatch", "field-bad", "ok"}[e]
}

// admitClass is one point of the cross product of the input classes of Check.
type admitClass struct {
	fromNode   bool
	frame      int // 0 behind, 1 equal, 2 ahead of the committed epoch
	client     int // 0 zero, 1 behind, 2 equal, 3 ahead
	hold       bool
	rank1      bool
	hop        uint8
	members    int
	lease      bool
	entry      admitEntryClass
	reAdmitted bool
	deduped    bool
	precond    bool
}

func (c admitClass) String() string {
	return fmt.Sprintf("%+v", struct {
		FromNode             bool
		Frame, Client        int
		Hold, Rank1          bool
		Hop                  uint8
		Members              int
		Lease                bool
		Entry                string
		ReAdmitted, Dd, Prec bool
	}{c.fromNode, c.frame, c.client, c.hold, c.rank1, c.hop, c.members, c.lease, c.entry.String(), c.reAdmitted, c.deduped, c.precond})
}

func (c admitClass) admission() Admission {
	a := admitGood()
	a.FromNode = c.fromNode
	a.FrameEpoch = [...]uint64{admitCommitted - 1, admitCommitted, admitCommitted + 1}[c.frame]
	a.ClientEpoch = [...]uint64{0, admitCommitted - 1, admitCommitted, admitCommitted + 1}[c.client]
	a.Hold, a.Rank1, a.Hop, a.Members, a.LeaseValid = c.hold, c.rank1, c.hop, c.members, c.lease
	a.ReAdmitted, a.Deduped, a.PreconditionFails = c.reAdmitted, c.deduped, c.precond
	switch c.entry {
	case admitEntMissing:
		a.Present, a.Missing = false, true
	case admitEntCold:
		a.Present = false
	case admitEntDropping:
		a.Decl.State = StateDropping
	case admitEntUnknownState:
		a.Decl.State = "unknown"
	case admitEntSeenAhead:
		a.Seen = admitPublished + 1
	case admitEntBehindEligible:
		a.Seen = admitPublished - 1
	case admitEntBehindOtherBucket:
		a.Seen, a.PayloadBucketID = admitPublished-1, admitBucket+1
	case admitEntBehindVersionMoved:
		a.Seen, a.AdmittedVersion = admitPublished-1, admitVersion+1
	case admitEntBehindVersionAhead:
		a.Seen, a.Decl.Version, a.AdmittedVersion = admitPublished-1, admitPublished+1, admitPublished+1
	case admitEntVersionAhead:
		a.Decl.Version, a.AdmittedVersion = admitPublished+1, admitPublished+1
	case admitEntBucketMismatch:
		a.PayloadBucketID = admitBucket + 1
	case admitEntFieldBad:
		a.WrittenFieldIDs = []uint16{1, 9}
	}
	return a
}

// effectiveVersion is the version Check reports once it has read a declaration.
func (c admitClass) effectiveVersion() int64 {
	switch c.entry {
	case admitEntMissing, admitEntCold:
		return 0
	case admitEntBehindVersionAhead, admitEntVersionAhead:
		return admitPublished + 1
	}
	return admitVersion
}

// admitRow is one row of the decision table: the exact class it covers, written without
// reference to Check, and the decision it requires.
type admitRow struct {
	name  string
	match func(admitClass) bool
	want  func(admitClass) AdmissionResult
}

func admitFixed(r AdmissionResult) func(admitClass) AdmissionResult {
	return func(admitClass) AdmissionResult { return r }
}

func admitAtEntry(r AdmissionResult) func(admitClass) AdmissionResult {
	return func(c admitClass) AdmissionResult {
		r.EffectiveVersion = c.effectiveVersion()
		return r
	}
}

func admitEpochPasses(c admitClass) bool {
	if c.fromNode {
		return c.frame == 1
	}
	return c.client == 0 || c.client == 2
}

func admitNotHeld(c admitClass) bool { return admitEpochPasses(c) && !c.hold }
func admitIsOwner(c admitClass) bool { return admitNotHeld(c) && c.rank1 }
func admitFencePasses(c admitClass) bool {
	return admitIsOwner(c) && c.entry == admitEntOK
}
func admitLeasePasses(c admitClass) bool { return admitFencePasses(c) && (c.members == 1 || c.lease) }

// admitEntryRow builds a row for owner-side check 4 with one entry class and a ReAdmitted
// constraint (-1 any, 0 not yet, 1 already).
func admitEntryRow(entry admitEntryClass, reAdmitted int, want func(admitClass) AdmissionResult) admitRow {
	return admitRow{
		name: fmt.Sprintf("4 %s readmitted=%d", entry, reAdmitted),
		match: func(c admitClass) bool {
			if !admitIsOwner(c) || c.entry != entry {
				return false
			}
			return reAdmitted < 0 || c.reAdmitted == (reAdmitted == 1)
		},
		want: want,
	}
}

func admitDecisionRows() []admitRow {
	snc := admitAtEntry(AdmissionResult{Code: cluster.CodeSchemaNotCurrent})
	readmit := admitAtEntry(AdmissionResult{ReAdmit: true})
	return []admitRow{
		{"1 node frame behind", func(c admitClass) bool { return c.fromNode && c.frame == 0 },
			admitFixed(AdmissionResult{Code: cluster.CodeEpochFenced})},
		{"1 node frame ahead", func(c admitClass) bool { return c.fromNode && c.frame == 2 },
			admitFixed(AdmissionResult{Code: cluster.CodeEpochFenced, MarkBehind: true})},
		{"1 client behind", func(c admitClass) bool { return !c.fromNode && c.client == 1 },
			admitFixed(AdmissionResult{Code: cluster.CodeStaleEpoch})},
		{"1 client ahead", func(c admitClass) bool { return !c.fromNode && c.client == 3 },
			admitFixed(AdmissionResult{Code: cluster.CodeStaleEpoch, MarkBehind: true})},
		{"2 hold", func(c admitClass) bool { return admitEpochPasses(c) && c.hold }, admitFixed(AdmissionResult{Hold: true})},
		{"3 forward", func(c admitClass) bool {
			return admitNotHeld(c) && !c.rank1 && c.hop == 0 && (c.members == 1 || c.lease)
		}, admitFixed(AdmissionResult{Forward: true})},
		{"3a forwarder without a lease", func(c admitClass) bool {
			return admitNotHeld(c) && !c.rank1 && c.hop == 0 && c.members > 1 && !c.lease
		}, admitFixed(AdmissionResult{Code: cluster.CodeLeaseLapsed})},
		{"3 not owner", func(c admitClass) bool { return admitNotHeld(c) && !c.rank1 && c.hop >= 1 },
			admitFixed(AdmissionResult{Code: cluster.CodeNotOwner})},
		admitEntryRow(admitEntMissing, -1, admitFixed(AdmissionResult{Code: cluster.CodeNotFound})),
		admitEntryRow(admitEntCold, 0, readmit),
		admitEntryRow(admitEntCold, 1, snc),
		admitEntryRow(admitEntDropping, -1, admitAtEntry(AdmissionResult{Code: cluster.CodeBucketDropping})),
		admitEntryRow(admitEntBehindEligible, 0, readmit),
		admitEntryRow(admitEntBehindEligible, 1, snc),
		admitEntryRow(admitEntBehindVersionAhead, 0, readmit),
		admitEntryRow(admitEntBehindVersionAhead, 1, snc),
		admitEntryRow(admitEntUnknownState, -1, snc),
		admitEntryRow(admitEntSeenAhead, -1, snc),
		admitEntryRow(admitEntBehindOtherBucket, -1, snc),
		admitEntryRow(admitEntBehindVersionMoved, -1, snc),
		admitEntryRow(admitEntVersionAhead, -1, snc),
		admitEntryRow(admitEntBucketMismatch, -1, snc),
		admitEntryRow(admitEntFieldBad, -1, admitAtEntry(AdmissionResult{Code: cluster.CodeFieldType})),
		{"5 lease", func(c admitClass) bool { return admitFencePasses(c) && c.members > 1 && !c.lease },
			admitAtEntry(AdmissionResult{Code: cluster.CodeLeaseLapsed})},
		{"6 dedupe", func(c admitClass) bool { return admitLeasePasses(c) && c.deduped },
			admitAtEntry(AdmissionResult{Deduped: true})},
		{"6a precondition", func(c admitClass) bool { return admitLeasePasses(c) && !c.deduped && c.precond },
			admitAtEntry(AdmissionResult{PreconditionRefused: true})},
		{"admitted", func(c admitClass) bool { return admitLeasePasses(c) && !c.deduped && !c.precond },
			admitAtEntry(AdmissionResult{})},
	}
}

func admitAllClasses(visit func(admitClass)) {
	bools := []bool{false, true}
	for _, fromNode := range bools {
		for frame := 0; frame < 3; frame++ {
			for client := 0; client < 4; client++ {
				for _, hold := range bools {
					for _, rank1 := range bools {
						for hop := uint8(0); hop < 3; hop++ {
							for _, members := range []int{1, 3} {
								for _, lease := range bools {
									for entry := admitEntryClass(0); entry < admitEntClassCount; entry++ {
										for _, reAdmitted := range bools {
											for _, deduped := range bools {
												for _, precond := range bools {
													visit(admitClass{fromNode, frame, client, hold, rank1, hop, members, lease,
														entry, reAdmitted, deduped, precond})
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

// TestEveryAdmissionClassHasExactlyOneRow enumerates the cross product of the input classes
// and fails if any class matches zero rows or two, or if Check disagrees with its row.
// Guard: leave a gap (drop the 3a row) or an overlap (let the lease row ignore the fence).
func TestEveryAdmissionClassHasExactlyOneRow(t *testing.T) {
	rows := admitDecisionRows()
	seen := make(map[string]int)
	classes, failures := 0, 0
	admitAllClasses(func(c admitClass) {
		classes++
		var matched []admitRow
		for _, row := range rows {
			if row.match(c) {
				matched = append(matched, row)
			}
		}
		if len(matched) != 1 {
			if failures++; failures <= 5 {
				names := make([]string, len(matched))
				for i, row := range matched {
					names[i] = row.name
				}
				t.Errorf("class %s matches %d rows %v, want exactly one", c, len(matched), names)
			}
			return
		}
		seen[matched[0].name]++
		want := matched[0].want(c)
		if got := Check(c.admission()); got != want {
			if failures++; failures <= 5 {
				t.Errorf("class %s (row %q): Check = %+v, want %+v", c, matched[0].name, got, want)
			}
		}
	})
	for _, row := range rows {
		if seen[row.name] == 0 {
			t.Errorf("row %q matched no class: the table has a row nothing reaches", row.name)
		}
	}
	t.Logf("%d classes, %d rows", classes, len(rows))
}

// ---- the model test -------------------------------------------------------------------

// A reference implementation written as a list of checks in conv §8 order; the first check
// that fails supplies the decision.

type admitRefCheck struct {
	name     string
	fails    bool
	decision AdmissionResult
}

func admitRefFence(a Admission) (name string, refDecision FenceResult) {
	table, version := EffectiveTable(a.Decl, a.Published)
	declared := func(id uint16) bool {
		in := false
		for _, entry := range table.Entries {
			in = in || entry.ID == id
		}
		for _, retired := range table.Retired {
			in = in && retired.ID != id
		}
		return in
	}
	badField := false
	for _, id := range a.WrittenFieldIDs {
		badField = badField || !declared(id)
	}
	conjunction := a.Seen == a.Published && version <= a.Published && a.Decl.State == StateActive &&
		a.Decl.BucketID == a.PayloadBucketID
	switch {
	case a.Missing:
		return "4 not found", FenceResult{Code: cluster.CodeNotFound}
	case a.Decl.State == StateDropping:
		return "4 dropping", FenceResult{Code: cluster.CodeBucketDropping}
	case !a.Present && !a.ReAdmitted:
		return "4 cold", FenceResult{ReAdmit: true}
	case !a.Present:
		return "4 cold again", FenceResult{Code: cluster.CodeSchemaNotCurrent}
	case a.Seen < a.Published && !a.ReAdmitted && a.Decl.BucketID == a.PayloadBucketID && version == a.AdmittedVersion:
		return "4 re-admit", FenceResult{ReAdmit: true}
	case !conjunction:
		return "4 not current", FenceResult{Code: cluster.CodeSchemaNotCurrent}
	case badField:
		return "4 field type", FenceResult{Code: cluster.CodeFieldType}
	}
	return "", FenceResult{}
}

func admitRefChecks(a Admission) []admitRefCheck {
	fenceName, fence := admitRefFence(a)
	entryVersion := int64(0)
	if a.Present && !a.Missing {
		_, entryVersion = EffectiveTable(a.Decl, a.Published)
	}
	fenceResult := AdmissionResult{Code: fence.Code, ReAdmit: fence.ReAdmit, EffectiveVersion: entryVersion}
	lapsed := a.Members > 1 && !a.LeaseValid
	return []admitRefCheck{
		{"1 node behind", a.FromNode && a.FrameEpoch < a.Committed, AdmissionResult{Code: cluster.CodeEpochFenced}},
		{"1 node ahead", a.FromNode && a.FrameEpoch > a.Committed, AdmissionResult{Code: cluster.CodeEpochFenced, MarkBehind: true}},
		{"1 client behind", !a.FromNode && a.ClientEpoch != 0 && a.ClientEpoch < a.Committed, AdmissionResult{Code: cluster.CodeStaleEpoch}},
		{"1 client ahead", !a.FromNode && a.ClientEpoch > a.Committed, AdmissionResult{Code: cluster.CodeStaleEpoch, MarkBehind: true}},
		{"2 hold", a.Hold, AdmissionResult{Hold: true}},
		{"3 not owner", !a.Rank1 && a.Hop >= 1, AdmissionResult{Code: cluster.CodeNotOwner}},
		{"3a lease", !a.Rank1 && lapsed, AdmissionResult{Code: cluster.CodeLeaseLapsed}},
		{"3 forward", !a.Rank1, AdmissionResult{Forward: true}},
		{fenceName, fenceName != "", fenceResult},
		{"5 lease", lapsed, AdmissionResult{Code: cluster.CodeLeaseLapsed, EffectiveVersion: entryVersion}},
		{"6 dedupe", a.Deduped, AdmissionResult{Deduped: true, EffectiveVersion: entryVersion}},
		{"6a precondition", a.PreconditionFails, AdmissionResult{PreconditionRefused: true, EffectiveVersion: entryVersion}},
		{"admitted", true, AdmissionResult{EffectiveVersion: entryVersion}},
	}
}

func admitFirstFailing(a Admission) (string, AdmissionResult) {
	for _, check := range admitRefChecks(a) {
		if check.fails {
			return check.name, check.decision
		}
	}
	panic("the admitted row always matches") // test-only invariant
}

func admitPick[T any](rng *rand.Rand, choices ...T) T { return choices[rng.Intn(len(choices))] }

func admitRandomTable(rng *rand.Rand) FieldTable {
	table := FieldTable{NextFieldID: 7}
	for id := uint16(1); id <= 4; id++ {
		if rng.Intn(4) != 0 {
			table.Entries = append(table.Entries, FieldEntry{ID: id, Type: "string", Name: fmt.Sprintf("f%d", id)})
		}
	}
	if rng.Intn(4) == 0 {
		table.Retired = []RetiredID{{ID: uint16(1 + rng.Intn(4)), Epoch: 3}}
	}
	return table
}

func admitRandomEntry(rng *rand.Rand, a *Admission) {
	a.Missing = rng.Intn(15) == 0
	a.Present = !a.Missing && rng.Intn(12) != 0
	a.Published = int64(3 + rng.Intn(4))
	a.Seen = a.Published + admitPick(rng, int64(0), 0, 0, 0, -1, -1, 1, -2)
	a.Decl = Declaration{
		BucketID: uint64(1 + rng.Intn(2)),
		Version:  int64(rng.Intn(9)),
		State:    admitPick(rng, StateActive, StateActive, StateActive, StateActive, StateDropping, "unknown"),
		Fields:   admitRandomTable(rng),
	}
	if rng.Intn(3) == 0 {
		a.Decl.Pending = &Pending{Version: int64(2 + rng.Intn(7)), Table: admitRandomTable(rng)}
	}
	a.PayloadBucketID = a.Decl.BucketID
	if rng.Intn(5) == 0 {
		a.PayloadBucketID = uint64(1 + rng.Intn(2))
	}
	_, version := EffectiveTable(a.Decl, a.Published)
	a.AdmittedVersion = version
	if rng.Intn(5) == 0 {
		a.AdmittedVersion = int64(rng.Intn(9))
	}
	a.ReAdmitted = rng.Intn(4) == 0
	a.WrittenFieldIDs = nil
	for id := uint16(1); id <= 5; id++ {
		if rng.Intn(3) == 0 {
			a.WrittenFieldIDs = append(a.WrittenFieldIDs, id)
		}
	}
}

func admitRandomAdmission(rng *rand.Rand) Admission {
	a := Admission{Committed: uint64(1 + rng.Intn(4))}
	a.FromNode = rng.Intn(2) == 0
	a.FrameEpoch = uint64(rng.Intn(6))
	a.ClientEpoch = admitPick(rng, 0, 0, uint64(rng.Intn(6)))
	if rng.Intn(3) != 0 {
		a.FrameEpoch, a.ClientEpoch = a.Committed, admitPick(rng, uint64(0), a.Committed)
	}
	a.Hold = rng.Intn(8) == 0
	a.Rank1 = rng.Intn(4) != 0
	a.LeaseValid = rng.Intn(5) != 0
	a.Members = admitPick(rng, 1, 2, 3, 5)
	a.Hop = uint8(rng.Intn(3))
	a.Deduped = rng.Intn(6) == 0
	a.PreconditionFails = rng.Intn(6) == 0
	admitRandomEntry(rng, &a)
	return a
}

// admitScrambleAfter re-randomises every input that only checks AFTER the reported one read.
// The result must not change: the later checks, and the fence among them, were not consulted.
func admitScrambleAfter(rng *rand.Rand, name string, a Admission) Admission {
	b := a
	other := admitRandomAdmission(rng)
	entry := func() {
		b.Present, b.Missing, b.Decl, b.Seen, b.Published = other.Present, other.Missing, other.Decl, other.Seen, other.Published
		b.ReAdmitted, b.PayloadBucketID, b.AdmittedVersion, b.WrittenFieldIDs = other.ReAdmitted, other.PayloadBucketID, other.AdmittedVersion, other.WrittenFieldIDs
	}
	tail := func() { b.Deduped, b.PreconditionFails = other.Deduped, other.PreconditionFails }
	switch name {
	case "1 node behind", "1 node ahead", "1 client behind", "1 client ahead":
		b.Hold, b.Rank1, b.LeaseValid, b.Members, b.Hop = other.Hold, other.Rank1, other.LeaseValid, other.Members, other.Hop
		entry()
		tail()
	case "2 hold":
		b.Rank1, b.LeaseValid, b.Members, b.Hop = other.Rank1, other.LeaseValid, other.Members, other.Hop
		entry()
		tail()
	case "3 not owner":
		b.LeaseValid, b.Members = other.LeaseValid, other.Members
		entry()
		tail()
	case "3a lease", "3 forward":
		entry()
		tail()
	case "5 lease":
		tail()
	case "6 dedupe":
		b.PreconditionFails = other.PreconditionFails
	case "6a precondition", "admitted":
	default: // a check 4 outcome: the lease, dedupe and precondition inputs are later
		if name == "4 not found" || name == "4 cold" || name == "4 cold again" || name == "4 dropping" ||
			name == "4 re-admit" || name == "4 not current" || name == "4 field type" {
			b.LeaseValid, b.Members = other.LeaseValid, other.Members
			tail()
		}
	}
	return b
}

// admitScrambleDead re-randomises the inputs that cannot matter to the reported outcome: the
// epoch of the other kind of sender, the hop count of an owner, and, once the lease check has
// passed, which of "one member" or "a valid lease" made it pass. It is the only scramble that
// the last two outcomes ("6a precondition" and "admitted") have, because no check runs after them.
func admitScrambleDead(rng *rand.Rand, name string, a Admission) Admission {
	b := a
	if b.FromNode {
		b.ClientEpoch = uint64(rng.Intn(8))
	} else {
		b.FrameEpoch = uint64(rng.Intn(8))
	}
	if b.Rank1 {
		b.Hop = uint8(rng.Intn(3))
	}
	switch name {
	case "6 dedupe", "6a precondition", "admitted":
		b.Members, b.LeaseValid = 1, rng.Intn(2) == 0
	}
	return b
}

func admitClone(a Admission) Admission {
	a.WrittenFieldIDs = append([]uint16(nil), a.WrittenFieldIDs...)
	return a
}

// TestAdmissionModel runs 20 admitFixed seeds of random requests against the reference and
// checks the four invariants of the brief.
// Guard: swap two checks, evaluate the fence on a replicated record, skip the re-admit limit.
func TestAdmissionModel(t *testing.T) {
	fired := map[string]int{}
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		for i := 0; i < 4000; i++ {
			a := admitRandomAdmission(rng)
			before := admitClone(a)
			name, want := admitFirstFailing(a)
			fired[name]++

			got := Check(a)
			if got != want {
				t.Fatalf("seed %d case %d: Check = %+v, reference (first failing check %q) = %+v\ninput %+v", seed, i, got, name, want, a)
			}
			if !reflect.DeepEqual(a, before) {
				t.Fatalf("seed %d case %d: Check changed its input", seed, i)
			}

			// invariant 1: the first failing check in the documented order is the one
			// reported; inputs only later checks read cannot change the answer, so the
			// fence is not consulted before checks 1, 2 and 3 have passed.
			if scrambled := admitScrambleAfter(rng, name, a); Check(scrambled) != got {
				t.Fatalf("seed %d case %d: %q reported, but changing later checks' inputs changed the result\nbefore %+v\nafter  %+v", seed, i, name, a, scrambled)
			}

			// the same holds for inputs the outcome does not read, which is all the scramble
			// the last two outcomes ("6a precondition", "admitted") can have.
			if scrambled := admitScrambleDead(rng, name, a); Check(scrambled) != got {
				t.Fatalf("seed %d case %d: %q reported, but changing inputs it does not read changed the result\nbefore %+v\nafter  %+v", seed, i, name, a, scrambled)
			}

			// invariant 4: a re-admission is offered at most once per request.
			if a.ReAdmitted && (got.ReAdmit || Fence(a).ReAdmit) {
				t.Fatalf("seed %d case %d: re-admit offered after the re-admission: %+v", seed, i, a)
			}
			readmits := 0
			request := admitClone(a)
			for step := 0; step < 4; step++ {
				stepDecision := Check(request)
				if !stepDecision.ReAdmit {
					break
				}
				if request.ReAdmitted {
					t.Fatalf("seed %d case %d: second re-admit for one request", seed, i)
				}
				readmits++
				request = admitClone(request)
				request.ReAdmitted = true // the owner refetched and re-enters with the new entry
				admitRandomEntry(rng, &request)
				request.ReAdmitted = true
			}
			if readmits > 1 {
				t.Fatalf("seed %d case %d: %d re-admits for one request", seed, i, readmits)
			}

			// invariant 3: the synthetic _buckets declaration always passes the fence.
			decl, seen := SystemView(a.Published)
			system := Admission{
				Committed: a.Committed, Rank1: true, LeaseValid: true, Members: a.Members,
				Present: true, Decl: decl, Seen: seen, Published: a.Published, ReAdmitted: a.ReAdmitted,
				PayloadBucketID: SystemBucketID, AdmittedVersion: 0,
				WrittenFieldIDs: []uint16{uint16(1 + rng.Intn(8)), uint16(1 + rng.Intn(8))},
			}
			if fence := Fence(system); fence != (FenceResult{}) {
				t.Fatalf("seed %d case %d: the system bucket was refused: %+v for %+v", seed, i, fence, system)
			}
		}
	}
	for _, check := range admitRefChecks(Admission{}) {
		if check.name != "" && fired[check.name] == 0 {
			t.Errorf("the generator never produced the first failure %q", check.name)
		}
	}
	for _, name := range []string{"4 not found", "4 cold", "4 cold again", "4 dropping", "4 re-admit", "4 not current", "4 field type"} {
		if fired[name] == 0 {
			t.Errorf("the generator never produced the first failure %q", name)
		}
	}
	t.Logf("first failures over 20 seeds x 4000 cases: %v", fired)
}
