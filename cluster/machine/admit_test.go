package machine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

const (
	admitPublished int64  = 5
	admitBucket    uint64 = 77
	admitVersion   int64  = 3
	admitCommitted uint64 = 10
)

// admitGood is a write that passes every check: an owner with a lease, an entry at the
// published version and fields 1 and 2 declared.
func admitGood() Admission {
	return Admission{
		Committed:  admitCommitted,
		Rank1:      true,
		LeaseValid: true,
		Members:    3,
		Present:    true,
		Decl: Declaration{
			BucketID: admitBucket,
			Version:  admitVersion,
			State:    StateActive,
			Fields: FieldTable{
				NextFieldID: 3,
				Entries:     []FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "name"}},
			},
		},
		Seen:            admitPublished,
		Published:       admitPublished,
		PayloadBucketID: admitBucket,
		AdmittedVersion: admitVersion,
		WrittenFieldIDs: []uint16{1, 2},
	}
}

// admitBreaker makes one check of a good admission fail. group is the number of the check in
// conv §8 order (3a shares group 3), want the result when it is the only failure.
type admitBreaker struct {
	name  string
	group int
	apply func(*Admission)
	want  AdmissionResult
}

func admitBreakers() []admitBreaker {
	fenceEV := func(r AdmissionResult) AdmissionResult {
		r.EffectiveVersion = admitVersion
		return r
	}
	return []admitBreaker{
		{"1 node frame behind", 1, func(a *Admission) { a.FromNode = true; a.FrameEpoch = admitCommitted - 1 },
			AdmissionResult{Code: cluster.CodeEpochFenced}},
		{"1 node frame ahead", 1, func(a *Admission) { a.FromNode = true; a.FrameEpoch = admitCommitted + 1 },
			AdmissionResult{Code: cluster.CodeEpochFenced, MarkBehind: true}},
		{"1 client behind", 1, func(a *Admission) { a.ClientEpoch = admitCommitted - 1 },
			AdmissionResult{Code: cluster.CodeStaleEpoch}},
		{"1 client ahead", 1, func(a *Admission) { a.ClientEpoch = admitCommitted + 1 },
			AdmissionResult{Code: cluster.CodeStaleEpoch, MarkBehind: true}},
		{"2 hold", 2, func(a *Admission) { a.Hold = true }, AdmissionResult{Hold: true}},
		{"3 forward", 3, func(a *Admission) { a.Rank1 = false }, AdmissionResult{Forward: true}},
		{"3 not owner after one hop", 3, func(a *Admission) { a.Rank1 = false; a.Hop = 1 },
			AdmissionResult{Code: cluster.CodeNotOwner}},
		{"3a forwarder without a lease", 3, func(a *Admission) { a.Rank1 = false; a.LeaseValid = false },
			AdmissionResult{Code: cluster.CodeLeaseLapsed}},
		{"4 bucket missing", 4, func(a *Admission) { a.Present = false; a.Missing = true },
			AdmissionResult{Code: cluster.CodeNotFound}},
		{"4 cold owner fetches", 4, func(a *Admission) { a.Present = false },
			AdmissionResult{ReAdmit: true}},
		{"4 dropping", 4, func(a *Admission) { a.Decl.State = StateDropping },
			fenceEV(AdmissionResult{Code: cluster.CodeBucketDropping})},
		{"4 unknown state", 4, func(a *Admission) { a.Decl.State = "unknown" },
			fenceEV(AdmissionResult{Code: cluster.CodeSchemaNotCurrent})},
		{"4 seen ahead", 4, func(a *Admission) { a.Seen = admitPublished + 1 },
			fenceEV(AdmissionResult{Code: cluster.CodeSchemaNotCurrent})},
		{"4 seen behind, re-admit", 4, func(a *Admission) { a.Seen = admitPublished - 1 },
			fenceEV(AdmissionResult{ReAdmit: true})},
		{"4 seen behind, second miss", 4, func(a *Admission) { a.Seen = admitPublished - 1; a.ReAdmitted = true },
			fenceEV(AdmissionResult{Code: cluster.CodeSchemaNotCurrent})},
		{"4 seen behind, other bucket id", 4, func(a *Admission) { a.Seen = admitPublished - 1; a.PayloadBucketID = admitBucket + 1 },
			fenceEV(AdmissionResult{Code: cluster.CodeSchemaNotCurrent})},
		{"4 seen behind, version moved", 4, func(a *Admission) { a.Seen = admitPublished - 1; a.AdmittedVersion = admitVersion + 1 },
			fenceEV(AdmissionResult{Code: cluster.CodeSchemaNotCurrent})},
		{"4 version above published", 4, func(a *Admission) { a.Decl.Version = admitPublished + 1; a.AdmittedVersion = admitPublished + 1 },
			AdmissionResult{Code: cluster.CodeSchemaNotCurrent, EffectiveVersion: admitPublished + 1}},
		{"4 bucket id differs", 4, func(a *Admission) { a.PayloadBucketID = admitBucket + 1 },
			fenceEV(AdmissionResult{Code: cluster.CodeSchemaNotCurrent})},
		{"4 undeclared field", 4, func(a *Admission) { a.WrittenFieldIDs = []uint16{1, 9} },
			fenceEV(AdmissionResult{Code: cluster.CodeFieldType})},
		{"4 retired field", 4, func(a *Admission) { a.Decl.Fields.Retired = []RetiredID{{ID: 2, Epoch: 4}} },
			fenceEV(AdmissionResult{Code: cluster.CodeFieldType})},
		{"5 lease", 5, func(a *Admission) { a.LeaseValid = false },
			fenceEV(AdmissionResult{Code: cluster.CodeLeaseLapsed})},
		{"6 dedupe", 6, func(a *Admission) { a.Deduped = true }, fenceEV(AdmissionResult{Deduped: true})},
		{"6a precondition", 7, func(a *Admission) { a.PreconditionFails = true },
			fenceEV(AdmissionResult{PreconditionRefused: true})},
	}
}

func TestGoodAdmissionIsAdmitted(t *testing.T) {
	got := Check(admitGood())
	want := AdmissionResult{EffectiveVersion: admitVersion}
	if got != want || !got.Admitted() {
		t.Fatalf("Check(good) = %+v, want %+v admitted", got, want)
	}
}

// TestWritePathCheckOrder shows every row of conv §8 fires alone, and that each check fires
// only when every earlier check passes: a request failing checks j and k (j before k) is
// reported as j, for every pair.
// Guard: swap checks 4 and 5, then 1 and 4.
func TestWritePathCheckOrder(t *testing.T) {
	all := admitBreakers()
	for _, b := range all {
		a := admitGood()
		b.apply(&a)
		if got := Check(a); got != b.want {
			t.Errorf("%s alone: Check = %+v, want %+v", b.name, got, b.want)
		}
	}
	for _, first := range all {
		for _, later := range all {
			if first.group >= later.group {
				continue
			}
			a := admitGood()
			first.apply(&a)
			later.apply(&a)
			want := first.want
			if strings.HasPrefix(first.name, "3 forward") && later.group == 5 {
				// check 3 forwards before check 5 is reached, so the lease is judged as 3a.
				want = AdmissionResult{Code: cluster.CodeLeaseLapsed}
			}
			if got := Check(a); got != want {
				t.Errorf("%s then %s: Check = %+v, want the earlier check's %+v", first.name, later.name, got, want)
			}
		}
	}
}

// TestForwarderMustHoldALease: check 3a refuses instead of forwarding.
// Guard: delete check 3a.
func TestForwarderMustHoldALease(t *testing.T) {
	a := admitGood()
	a.Rank1 = false
	a.LeaseValid = false
	if got := Check(a); got.Code != cluster.CodeLeaseLapsed || got.Forward {
		t.Fatalf("forwarder with no lease: %+v, want LEASE_LAPSED and no forward", got)
	}
	a.Members = 1
	if got := Check(a); !got.Forward || got.Code != 0 {
		t.Fatalf("one-member cluster holds no lease and may forward: %+v", got)
	}
	a.Members = 3
	a.LeaseValid = true
	if got := Check(a); !got.Forward || got.Code != 0 {
		t.Fatalf("forwarder with a lease: %+v, want Forward", got)
	}
}

// TestVersionBelowCommittedIsAccepted: create bucket does not bump, so an untouched bucket
// carries version < s and must stay writable.
// Guard: compare the declaration version with == instead of <=.
func TestVersionBelowCommittedIsAccepted(t *testing.T) {
	a := admitGood()
	a.Decl.Version = 1
	a.AdmittedVersion = 1
	got := Check(a)
	if !got.Admitted() || got.EffectiveVersion != 1 {
		t.Fatalf("version 1 at published %d: %+v, want admitted at effective version 1", admitPublished, got)
	}
}

// TestHigherClientEpochMarksBehind: a client epoch above the committed one marks the node
// behind as well as refusing, while a lower one only refuses.
// Guard: test only client < committed.
func TestHigherClientEpochMarksBehind(t *testing.T) {
	a := admitGood()
	a.ClientEpoch = admitCommitted + 1
	if got := Check(a); !got.MarkBehind || got.Code != cluster.CodeStaleEpoch {
		t.Fatalf("higher client epoch: %+v, want MarkBehind and STALE_EPOCH", got)
	}
	a.ClientEpoch = admitCommitted - 1
	if got := Check(a); got.MarkBehind || got.Code != cluster.CodeStaleEpoch {
		t.Fatalf("lower client epoch: %+v, want STALE_EPOCH without MarkBehind", got)
	}
	a.ClientEpoch = admitCommitted
	if got := Check(a); !got.Admitted() {
		t.Fatalf("equal client epoch: %+v, want admitted", got)
	}
	a.ClientEpoch = 0
	if got := Check(a); !got.Admitted() {
		t.Fatalf("client epoch 0 means route it for me: %+v, want admitted", got)
	}
}

// TestNodeFrameAndClientEpochsAreSeparate: client rows apply only to a client prologue and
// node rows only to a node frame.
func TestNodeFrameAndClientEpochsAreSeparate(t *testing.T) {
	a := admitGood()
	a.FromNode = true
	a.FrameEpoch = admitCommitted
	a.ClientEpoch = admitCommitted + 5
	if got := Check(a); !got.Admitted() {
		t.Fatalf("a node frame ignores ClientEpoch: %+v", got)
	}
	a = admitGood()
	a.FrameEpoch = admitCommitted + 5
	if got := Check(a); !got.Admitted() {
		t.Fatalf("a client prologue ignores FrameEpoch: %+v", got)
	}
	a.FromNode = true
	if got := Check(a); got.Code != cluster.CodeEpochFenced {
		t.Fatalf("the same epochs on a node frame: %+v, want EPOCH_FENCED", got)
	}
}

// TestSchemaMissIsReAdmittedOnce: a bump committed for another bucket moves the database
// version only; the owner refetches and re-enters, and the client sees nothing.
// Guard: refuse whenever seen < s.
func TestSchemaMissIsReAdmittedOnce(t *testing.T) {
	a := admitGood()
	a.Seen = admitPublished - 1
	got := Check(a)
	if !got.ReAdmit || got.Code != 0 || got.Admitted() {
		t.Fatalf("seen behind, own table unchanged: %+v, want ReAdmit with no code and no position", got)
	}
	if fence := Fence(a); fence != (FenceResult{ReAdmit: true}) {
		t.Fatalf("Fence = %+v, want ReAdmit", fence)
	}
}

// TestTheSecondMissIsRefused: after the one re-admission, a version miss is the refusal.
// Guard: re-admit twice (ignore ReAdmitted).
func TestTheSecondMissIsRefused(t *testing.T) {
	a := admitGood()
	a.Seen = admitPublished - 1
	a.ReAdmitted = true
	got := Check(a)
	if got.ReAdmit || got.Code != cluster.CodeSchemaNotCurrent {
		t.Fatalf("second miss: %+v, want SCHEMA_NOT_CURRENT and no re-admit", got)
	}
	if fence := Fence(a); fence != (FenceResult{Code: cluster.CodeSchemaNotCurrent}) {
		t.Fatalf("Fence on a second miss = %+v, want SCHEMA_NOT_CURRENT", fence)
	}
	cold := admitGood()
	cold.Present = false
	cold.ReAdmitted = true
	if fence := Fence(cold); fence != (FenceResult{Code: cluster.CodeSchemaNotCurrent}) {
		t.Fatalf("a cold entry after the re-admission = %+v, want SCHEMA_NOT_CURRENT", fence)
	}
}

// TestVersionAbovePublishedRowComesAfterReAdmit pins the position of the
// "version > published" refusal: it is part of the conjunction after the re-admit row, so a
// miss that is only seen < s is re-admitted even when the table's version is above s, and
// the refusal happens once the bucket is re-read.
// Guard: move the version > published row before the re-admit row.
func TestVersionAbovePublishedRowComesAfterReAdmit(t *testing.T) {
	ahead := func(a *Admission) { a.Decl.Version = admitPublished + 1; a.AdmittedVersion = admitPublished + 1 }

	a := admitGood()
	ahead(&a)
	if got := Check(a); got.Code != cluster.CodeSchemaNotCurrent || got.ReAdmit {
		t.Fatalf("seen == published, version above published: %+v, want SCHEMA_NOT_CURRENT", got)
	}

	a = admitGood()
	ahead(&a)
	a.Seen = admitPublished - 1
	if got := Check(a); !got.ReAdmit || got.Code != 0 {
		t.Fatalf("seen behind AND version above published, first time: %+v, want the re-admit (row 4 before row 5)", got)
	}

	a.ReAdmitted = true
	if got := Check(a); got.Code != cluster.CodeSchemaNotCurrent || got.ReAdmit {
		t.Fatalf("the same after the re-admission: %+v, want SCHEMA_NOT_CURRENT", got)
	}
}

// TestPendingTableIsInertUntilPublished: an alter's table is in force only once the published
// version reaches it, so a crash between its write and its bump cannot make a bucket unwritable.
// Guard: use a pending table whenever it is present.
func TestPendingTableIsInertUntilPublished(t *testing.T) {
	pending := &Pending{Version: admitPublished + 1, Table: FieldTable{
		NextFieldID: 4,
		Entries:     []FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 3, Type: "int", Name: "age"}},
		Retired:     []RetiredID{{ID: 2, Epoch: 9}},
	}}

	a := admitGood()
	a.Decl.Pending = pending
	if got := Check(a); !got.Admitted() || got.EffectiveVersion != admitVersion {
		t.Fatalf("pending above published: %+v, want field 5 in force at version %d", got, admitVersion)
	}
	a.WrittenFieldIDs = []uint16{1, 3}
	if got := Check(a); got.Code != cluster.CodeFieldType {
		t.Fatalf("a pending-only field before its bump: %+v, want FIELD_TYPE", got)
	}

	published := admitGood()
	published.Decl.Pending = pending
	published.Published = admitPublished + 1
	published.Seen = admitPublished + 1
	published.AdmittedVersion = admitPublished + 1
	published.WrittenFieldIDs = []uint16{1, 3}
	if got := Check(published); !got.Admitted() || got.EffectiveVersion != admitPublished+1 {
		t.Fatalf("pending at published: %+v, want the pending table in force at version %d", got, admitPublished+1)
	}
	published.WrittenFieldIDs = []uint16{1, 2}
	if got := Check(published); got.Code != cluster.CodeFieldType {
		t.Fatalf("a field the published pending table retired: %+v, want FIELD_TYPE", got)
	}
}

// TestSystemBucketPassesTheFence: _buckets has no document to fetch; its synthetic entry is
// current by construction, so the first create bucket in a fresh database is admitted.
// Guard: admit _buckets only when a cache entry exists.
func TestSystemBucketPassesTheFence(t *testing.T) {
	for _, published := range []int64{1, 2, 7, 1 << 40} {
		decl, seen := SystemView(published)
		if seen != published {
			t.Fatalf("SystemView(%d) seen = %d, want the published version", published, seen)
		}
		if decl.BucketID != SystemBucketID || decl.Version != 0 || decl.State != StateActive || len(decl.Fields.Entries) != 8 {
			t.Fatalf("SystemView(%d) = %+v, want the built-in _buckets declaration", published, decl)
		}
		for _, ids := range [][]uint16{{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5, 6, 7, 8}, nil} {
			a := Admission{
				Committed: admitCommitted, Rank1: true, LeaseValid: true, Members: 3,
				Present: true, Decl: decl, Seen: seen, Published: published,
				PayloadBucketID: SystemBucketID, AdmittedVersion: 0, WrittenFieldIDs: ids,
			}
			if got := Check(a); !got.Admitted() {
				t.Fatalf("system bucket at published %d writing %v: %+v, want admitted", published, ids, got)
			}
			if got := Fence(a); got != (FenceResult{}) {
				t.Fatalf("Fence(system bucket, published %d) = %+v", published, got)
			}
		}
		a := Admission{
			Committed: admitCommitted, Rank1: true, LeaseValid: true, Members: 3,
			Present: true, Decl: decl, Seen: seen, Published: published,
			PayloadBucketID: SystemBucketID, WrittenFieldIDs: []uint16{9},
		}
		if got := Fence(a); got.Code != cluster.CodeFieldType {
			t.Fatalf("system bucket writing an undeclared id: %+v, want FIELD_TYPE: only the field check runs", got)
		}
		// the optional declaration fields (indexes 10, expiry 11, aggregates 12, references 13) are admitted by id, and
		// only on _buckets: 9 is not, and a user bucket never writes them
		for id, want := range map[uint16]bool{9: false, 10: true, 11: true, 12: true, 13: true} {
			a.WrittenFieldIDs = []uint16{id}
			if got := Fence(a); (got == FenceResult{}) != want {
				t.Fatalf("system bucket writing id %d: %+v, admitted should be %v", id, got, want)
			}
		}
	}
	first, _ := SystemView(1)
	first.Fields.Entries[0].Name = "changed"
	again, _ := SystemView(1)
	if again.Fields.Entries[0].Name != "_id_" {
		t.Fatalf("SystemView returned a shared declaration: a caller changed the next caller's table")
	}
}

// TestBucketIDMismatchIsRefused: a drop-and-recreate racing a forwarded write must not be
// written under the dead id, whose keys nothing sweeps.
// Guard: drop the bucket_id comparison.
func TestBucketIDMismatchIsRefused(t *testing.T) {
	a := admitGood()
	a.PayloadBucketID = admitBucket + 1
	if got := Check(a); got.Code != cluster.CodeSchemaNotCurrent {
		t.Fatalf("payload bucket id differs: %+v, want SCHEMA_NOT_CURRENT", got)
	}
	a.Seen = admitPublished - 1
	if got := Check(a); got.Code != cluster.CodeSchemaNotCurrent || got.ReAdmit {
		t.Fatalf("payload bucket id differs and seen is behind: %+v, want the refusal, not a re-admit", got)
	}
}

// TestPreconditionRefusalConsumesNoPosition: check 6a carries no code and no position; the
// caller returns its own refusal.
// Guard: give 6a a code or a position.
func TestPreconditionRefusalConsumesNoPosition(t *testing.T) {
	a := admitGood()
	a.PreconditionFails = true
	got := Check(a)
	if got.Code != 0 || !got.PreconditionRefused || got.Admitted() {
		t.Fatalf("precondition: %+v, want no code, PreconditionRefused, and not admitted", got)
	}
	a.Deduped = true
	if got := Check(a); !got.Deduped || got.PreconditionRefused {
		t.Fatalf("deduped retry with a failing precondition: %+v, want the dedupe answer (6 before 6a)", got)
	}
}

// TestColdAdmissionFetchesOnce: an owner that has not cached the declaration fetches and
// re-enters once; it refuses only if that fetch also leaves it without an entry.
func TestColdAdmissionFetchesOnce(t *testing.T) {
	a := admitGood()
	a.Present = false
	if got := Check(a); !got.ReAdmit || got.Code != 0 {
		t.Fatalf("cold: %+v, want ReAdmit", got)
	}
	a.Missing = true
	if got := Check(a); got.Code != cluster.CodeNotFound || got.ReAdmit {
		t.Fatalf("missing: %+v, want NOT_FOUND", got)
	}
}

// TestDroppingIsItsOwnRefusal: dropping is BUCKET_DROPPING and nothing else, even when the
// versions also disagree.
func TestDroppingIsItsOwnRefusal(t *testing.T) {
	a := admitGood()
	a.Decl.State = StateDropping
	a.Seen = admitPublished + 3
	if got := Check(a); got.Code != cluster.CodeBucketDropping {
		t.Fatalf("dropping with seen ahead: %+v, want BUCKET_DROPPING", got)
	}
}

// TestDroppingBeatsColdInCheck4 pins the order inside check 4 (ruling U13-6): missing, then
// dropping, then cold. A declaration that reads dropping is BUCKET_DROPPING even when the
// admission entry is otherwise absent, so a cold owner never turns it into a re-admit.
// Guard: evaluate the cold row before the dropping row.
func TestDroppingBeatsColdInCheck4(t *testing.T) {
	a := admitGood()
	a.Present = false
	a.Decl.State = StateDropping
	want := FenceResult{Code: cluster.CodeBucketDropping}
	if got := Fence(a); got != want {
		t.Fatalf("Fence(cold, dropping) = %+v, want %+v", got, want)
	}
	if got := Check(a); got.Code != cluster.CodeBucketDropping || got.ReAdmit {
		t.Fatalf("Check(cold, dropping) = %+v, want BUCKET_DROPPING and no re-admit", got)
	}
	a.ReAdmitted = true
	if got := Fence(a); got != want {
		t.Fatalf("Fence(cold, dropping, re-admitted) = %+v, want %+v", got, want)
	}
	a.Missing = true
	if got := Fence(a); got.Code != cluster.CodeNotFound {
		t.Fatalf("Fence(missing, dropping) = %+v, want NOT_FOUND: missing is first", got)
	}
}

// TestRetiredFieldIsNotWritable: a retired id keeps FIELD_TYPE even though it is still in
// the entries.
func TestRetiredFieldIsNotWritable(t *testing.T) {
	a := admitGood()
	a.Decl.Fields.Retired = []RetiredID{{ID: 2, Epoch: 1}}
	if got := Fence(a); got.Code != cluster.CodeFieldType {
		t.Fatalf("retired id: %+v, want FIELD_TYPE", got)
	}
}

// TestFenceIsCheckFourAlone: Fence reads only the entry and the published version, so it
// answers the same whatever the epoch, hold, ownership or lease say.
func TestFenceIsCheckFourAlone(t *testing.T) {
	a := admitGood()
	a.FromNode = true
	a.FrameEpoch = 1
	a.Hold = true
	a.Rank1 = false
	a.LeaseValid = false
	a.Deduped = true
	a.PreconditionFails = true
	if got := Fence(a); got != (FenceResult{}) {
		t.Fatalf("Fence = %+v, want admitted: check 4 alone", got)
	}
	a.Seen = admitPublished + 1
	if got := Fence(a); got.Code != cluster.CodeSchemaNotCurrent {
		t.Fatalf("Fence with seen ahead = %+v, want SCHEMA_NOT_CURRENT whatever the other checks say", got)
	}
}

var admitApplySideName = regexp.MustCompile(`(?i)fence.*(apply|replay|catch|seed|drain)|(apply|replay|catch|seed|drain).*fence`)

// TestSchemaFenceIsTheOwnersAlone is the call-graph property of conv §20: the fence is
// evaluated by the owner's ordering path and by no copy, replay, catch-up or seed drain.
// Inside this package that means Check is the only function that calls Fence, and no
// apply-shaped fence function exists.
// Guard: call Fence from an applier or a replay function.
func TestSchemaFenceIsTheOwnersAlone(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the package: %v", err)
	}
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		files = append(files, file)
	}
	callers := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if admitApplySideName.MatchString(fn.Name.Name) && fn.Name.Name != "Fence" {
				t.Errorf("function %s looks like an apply-side fence: the fence is the owner's alone", fn.Name.Name)
			}
			if fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "Fence" {
					callers[fn.Name.Name] = true
				}
				return true
			})
		}
	}
	if !callers["Check"] {
		t.Errorf("Check does not call Fence: the owner's admission is where the fence is evaluated")
	}
	delete(callers, "Check")
	for name := range callers {
		t.Errorf("%s calls Fence: only Check (the owner's admission) may", name)
	}
}
