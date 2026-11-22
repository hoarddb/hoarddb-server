package wire

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// s4vPair is one (slot, database) key of the fakes below.
type s4vPair struct {
	slot uint16
	db   uint64
}

// s4vFake holds what the view reads from its node: records, pulls and gate waits by pair.
type s4vFake struct {
	databases    []cluster.Database
	records      map[s4vPair]SeedRecord
	pulls        map[s4vPair]PullStatus
	waits        map[s4vPair][]string
	attested     map[s4vPair]int // other members that attest a copy of the pair (what the engine counts)
	reported     []error
	feedRetry    func(slot uint16, db uint64) (machine.SlotAction, error)
	requeued     []s4vPair
	fed          int
	forgets      []s4vPair                       // every Forget the view asked the engine for
	onForget     func(key s4vPair) (bool, error) // the engine's removal; nil means not removed
	beforeEngine func(key s4vPair)               // runs as the engine takes the pair lock, before it reads the record
}

func newS4vFake() *s4vFake {
	return &s4vFake{
		databases: []cluster.Database{{Name: "app", ID: 1, RF: 2}},
		records:   map[s4vPair]SeedRecord{},
		pulls:     map[s4vPair]PullStatus{},
		waits:     map[s4vPair][]string{},
		attested:  map[s4vPair]int{},
	}
}

func (f *s4vFake) hold(slot uint16, db uint64, state uint8, source string) {
	f.records[s4vPair{slot, db}] = SeedRecord{SeedRecord: machine.SeedRecord{State: state, Source: source}}
}

func (f *s4vFake) view() *nodeSeedingView {
	return newSeedingView(seedViewDeps{
		State: func() cluster.ClusterState { return cluster.ClusterState{Databases: f.databases} },
		Records: func(visit func(slot uint16, db uint64, state uint8)) {
			for key, record := range f.records {
				visit(key.slot, key.db, record.State)
			}
		},
		Record: func(slot uint16, db uint64) (SeedRecord, bool) {
			record, held := f.records[s4vPair{slot, db}]
			return record, held
		},
		Pull: func(slot uint16, db uint64) (PullStatus, bool) {
			pull, pulled := f.pulls[s4vPair{slot, db}]
			return pull, pulled
		},
		Waiting: func(slot uint16, db uint64) []string { return f.waits[s4vPair{slot, db}] },
		FeedRetry: func(slot uint16, db uint64) (machine.SlotAction, error) {
			f.fed++
			return f.feedRetry(slot, db)
		},
		Requeue: func(slot uint16, db uint64) { f.requeued = append(f.requeued, s4vPair{slot, db}) },
		// Forget stands in for Reconciler.Forget: it reads the record once, as the engine does
		// under the pair lock, and judges the state and the only-copy check on that reading.
		Forget: func(slot uint16, db uint64, anyway bool) (ForgetOutcome, error) {
			key := s4vPair{slot, db}
			f.forgets = append(f.forgets, key)
			if f.beforeEngine != nil {
				f.beforeEngine(key)
			}
			record, held := f.records[key]
			switch {
			case !held:
				return ForgetNoRecord, nil
			case record.State != machine.SeedRetired:
				return ForgetHeld, nil
			case !anyway && f.attested[key] == 0:
				return ForgetOnlyCopy, nil
			case f.onForget == nil:
				return ForgetHeld, nil
			}
			removed, err := f.onForget(key)
			if removed {
				return ForgetRemoved, err
			}
			return ForgetHeld, err
		},
		Report: func(err error) { f.reported = append(f.reported, err) },
	})
}

// s4vRows returns the status rows by slot.
func s4vRows(t *testing.T, f *s4vFake) map[int]SeedRow {
	t.Helper()
	rows := map[int]SeedRow{}
	for _, row := range f.view().Status() {
		if _, dup := rows[row.Slot]; dup {
			t.Fatalf("slot %d appears twice", row.Slot)
		}
		rows[row.Slot] = row
	}
	return rows
}

// TestStatusRowsIncludeRetiredAndRetiring: a ready record is no row, and a retired or
// retiring one is, with no pull behind it. [guard: read the puller only]
func TestStatusRowsIncludeRetiredAndRetiring(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, machine.SeedReady, "")
	f.hold(2, 1, machine.SeedSeeding, "node-b")
	f.hold(3, 1, machine.SeedRetired, "")
	f.hold(4, 1, machine.SeedRetiring, "")
	f.pulls[s4vPair{2, 1}] = PullStatus{State: PullPulling, Source: "node-b"}
	rows := s4vRows(t, f)
	if len(rows) != 3 {
		t.Fatalf("got %d rows %+v, want slots 2, 3 and 4", len(rows), rows)
	}
	want := map[int]SeedState{2: SeedSeeding, 3: SeedRetired, 4: SeedRetiring}
	for slot, state := range want {
		if rows[slot].State != state || rows[slot].Database != "app" {
			t.Errorf("slot %d: %+v, want %s of app", slot, rows[slot], state)
		}
	}
	if _, ready := rows[1]; ready {
		t.Errorf("a ready record is a row: %+v", rows[1])
	}
	if rows[4].Refusal != "" {
		t.Errorf("a retiring row carries %q, want no refusal", rows[4].Refusal)
	}
}

// TestStatusNamesTheMemberTheGateWaitsFor: the retired row says which member has not
// confirmed. [guard: print the state word without the wait]
func TestStatusNamesTheMemberTheGateWaitsFor(t *testing.T) {
	f := newS4vFake()
	f.hold(3, 1, machine.SeedRetired, "")
	f.hold(4, 1, machine.SeedRetired, "")
	f.hold(5, 1, machine.SeedRetired, "")
	f.waits[s4vPair{3, 1}] = []string{"node-c"}
	f.waits[s4vPair{4, 1}] = []string{"node-b", "node-c"}
	rows := s4vRows(t, f)
	for slot, want := range map[int]string{
		3: "waiting for node-c to confirm it holds this slot",
		4: "waiting for node-b, node-c to confirm they hold this slot",
		5: "",
	} {
		if rows[slot].Refusal != want {
			t.Errorf("slot %d refusal = %q, want %q", slot, rows[slot].Refusal, want)
		}
	}
	if seedWaitText(nil) != "" {
		t.Errorf("seedWaitText(nil) = %q, want empty", seedWaitText(nil))
	}
}

// TestAStatusRowNeverCarriesAnUnknownStateWord: a record in a state no word names is skipped
// and reported, never printed. [guard: pass the machine's raw state]
func TestAStatusRowNeverCarriesAnUnknownStateWord(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, 0, "")
	f.hold(2, 1, 9, "")
	f.hold(3, 1, machine.SeedRetired, "")
	rows := s4vRows(t, f)
	if len(rows) != 1 || rows[3].State != SeedRetired {
		t.Fatalf("rows = %+v, want slot 3 only", rows)
	}
	for _, row := range rows {
		if !slices.Contains(seedStates, row.State) {
			t.Errorf("row %+v carries %q, which is not one of the six words", row, row.State)
		}
	}
	if len(f.reported) != 2 {
		t.Fatalf("reported %d errors %v, want 2", len(f.reported), f.reported)
	}
	for i, slotAndState := range [][2]string{{"slot 1", "state 0"}, {"slot 2", "state 9"}} {
		var found bool
		for _, err := range f.reported {
			text := err.Error()
			found = found || (strings.Contains(text, slotAndState[0]) && strings.Contains(text, slotAndState[1]) && strings.Contains(text, "app"))
		}
		if !found {
			t.Errorf("case %d: no report names %v and the database: %v", i, slotAndState, f.reported)
		}
	}
}

// TestAPairNeverPickedUpIsPlacingAndAPullingOneIsSeeding: queued with nothing done is
// placing; a queued retry that began is seeding. [guard: map queued to seeding]
func TestAPairNeverPickedUpIsPlacingAndAPullingOneIsSeeding(t *testing.T) {
	f := newS4vFake()
	for slot := uint16(1); slot <= 6; slot++ {
		f.hold(slot, 1, machine.SeedSeeding, "")
	}
	f.pulls[s4vPair{2, 1}] = PullStatus{State: PullQueued}
	f.pulls[s4vPair{3, 1}] = PullStatus{State: PullQueued, Source: "node-b"}
	f.pulls[s4vPair{4, 1}] = PullStatus{State: PullQueued, Entries: 7}
	f.pulls[s4vPair{5, 1}] = PullStatus{State: PullPulling}
	f.pulls[s4vPair{6, 1}] = PullStatus{State: PullStalled, Source: "node-b"}
	rows := s4vRows(t, f)
	want := map[int]SeedState{1: SeedPlacing, 2: SeedPlacing, 3: SeedSeeding, 4: SeedSeeding, 5: SeedSeeding, 6: SeedStalled}
	for slot, state := range want {
		if rows[slot].State != state {
			t.Errorf("slot %d is %q, want %q", slot, rows[slot].State, state)
		}
	}
}

// TestStatusSkipsARecordWhoseDatabaseIsGone: a record of a dropped database has no name to
// print, so it is no row. [guard: print the id]
func TestStatusSkipsARecordWhoseDatabaseIsGone(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, machine.SeedRetired, "")
	f.hold(2, 77, machine.SeedRetired, "")
	rows := s4vRows(t, f)
	if len(rows) != 1 || rows[1].Database != "app" {
		t.Fatalf("rows = %+v, want slot 1 of app only", rows)
	}
	if len(f.reported) != 0 {
		t.Errorf("a dropped database was reported: %v", f.reported)
	}
}

// TestAStatusRowCarriesTheCountsTheIdleAndTheSource: the row reads the pull's source, bytes,
// entries, idle and detail, and falls back to the record's source when no pull exists.
// [guard: leave Entries out]
func TestAStatusRowCarriesTheCountsTheIdleAndTheSource(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, machine.SeedSeeding, "node-z")
	f.hold(2, 1, machine.SeedSeeding, "node-z")
	f.pulls[s4vPair{1, 1}] = PullStatus{State: PullStalled, Source: "node-b", Bytes: 4096, Entries: 42, Idle: 3 * time.Second, Detail: "node-b is full; free its disk"}
	rows := s4vRows(t, f)
	got := rows[1]
	want := SeedRow{Database: "app", Slot: 1, State: SeedStalled, Source: "node-b", BytesDone: 4096, Entries: 42, Idle: 3 * time.Second, Refusal: "node-b is full; free its disk"}
	if got != want {
		t.Errorf("row = %+v, want %+v", got, want)
	}
	if rows[2].Source != "node-z" || rows[2].State != SeedPlacing {
		t.Errorf("a pair with no pull = %+v, want the record's source and placing", rows[2])
	}
}

// s4vMachineRetry answers a retry the way the slot machine's SeedRetryAsked rows do for the
// record the fake holds: a restart for a seeding record, SEED_NOT_HELD for every other.
func s4vMachineRetry(f *s4vFake) func(uint16, uint64) (machine.SlotAction, error) {
	return func(slot uint16, db uint64) (machine.SlotAction, error) {
		if record, held := f.records[s4vPair{slot, db}]; held && record.State == machine.SeedSeeding {
			return machine.SlotAction{Do: machine.SlotRestartSeed, Restart: 4}, nil
		}
		return machine.SlotAction{Refusal: cluster.CodeSeedNotHeld}, nil
	}
}

// [guard: requeue for any record]
func TestRetryOfANonSeedingSlotIsRefusedWithSeedNotHeld(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, machine.SeedReady, "")
	f.hold(2, 1, machine.SeedRetired, "node-b")
	f.hold(3, 1, machine.SeedRetiring, "node-b")
	f.feedRetry = s4vMachineRetry(f)
	for slot, word := range map[int]string{1: "ready", 2: "retired", 3: "retiring", 4: "absent"} {
		err := f.view().Retry(context.Background(), "app", slot)
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeSeedNotHeld {
			t.Fatalf("slot %d: Retry = %v, want a SEED_NOT_HELD refusal", slot, err)
		}
		if want := "is " + word + " on this node"; !strings.Contains(refusal.Message, want) {
			t.Errorf("slot %d: message %q does not say %q", slot, refusal.Message, want)
		}
	}
	if len(f.requeued) != 0 {
		t.Errorf("a refused retry requeued %v", f.requeued)
	}
}

// [guard: do not requeue]
func TestRetryOfASeedingSlotRestartsAndRequeuesOnce(t *testing.T) {
	f := newS4vFake()
	f.hold(5, 1, machine.SeedSeeding, "node-b")
	f.feedRetry = s4vMachineRetry(f)
	if err := f.view().Retry(context.Background(), "app", 5); err != nil {
		t.Fatalf("Retry = %v, want nil", err)
	}
	if want := []s4vPair{{5, 1}}; !slices.Equal(f.requeued, want) {
		t.Errorf("requeued %v, want %v", f.requeued, want)
	}
}

func TestRetryOfAnUnknownDatabaseIsRefusedAsAbsent(t *testing.T) {
	f := newS4vFake()
	f.feedRetry = s4vMachineRetry(f)
	err := f.view().Retry(context.Background(), "nope", 5)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeSeedNotHeld || !strings.Contains(refusal.Message, "is absent on this node") {
		t.Fatalf("Retry = %v, want SEED_NOT_HELD naming absent", err)
	}
	if f.fed != 0 || len(f.requeued) != 0 {
		t.Errorf("an unknown database fed %d retries and requeued %v", f.fed, f.requeued)
	}
}

// [guard: swallow it]
func TestRetryReturnsAFeedErrorWrappedAndRequeuesNothing(t *testing.T) {
	f := newS4vFake()
	f.hold(5, 1, machine.SeedSeeding, "node-b")
	busy := errors.New("pair busy")
	f.feedRetry = func(uint16, uint64) (machine.SlotAction, error) {
		return machine.SlotAction{Do: machine.SlotRestartSeed}, busy
	}
	err := f.view().Retry(context.Background(), "app", 5)
	if !errors.Is(err, busy) {
		t.Fatalf("Retry = %v, want the feed error wrapped", err)
	}
	if want := `restarting slot 5 of database "app": `; !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Retry = %q, want prefix %q", err, want)
	}
	if len(f.requeued) != 0 {
		t.Errorf("a failed feed requeued %v", f.requeued)
	}
}

// s4vForgetCode is the cluster code of err, failing the test when err is not a cluster error.
func s4vForgetCode(t *testing.T, err error) cluster.Code {
	t.Helper()
	var clusterErr *cluster.Error
	if !errors.As(err, &clusterErr) {
		t.Fatalf("err = %v, want a *cluster.Error", err)
	}
	return clusterErr.Code
}

// TestForgetOfARetiredRecordSucceeds: the engine removes a retired record and Forget returns
// nil after one engine call for that pair.
func TestForgetOfARetiredRecordSucceeds(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, machine.SeedRetired, "")
	f.attested[s4vPair{1, 1}] = 1
	f.onForget = func(key s4vPair) (bool, error) {
		delete(f.records, key)
		return true, nil
	}
	if err := f.view().Forget(context.Background(), "app", 1, false); err != nil {
		t.Fatalf("Forget of a retired record = %v, want nil", err)
	}
	if len(f.forgets) != 1 || f.forgets[0] != (s4vPair{1, 1}) {
		t.Errorf("engine calls = %v, want one for slot 1", f.forgets)
	}
	if _, held := f.records[s4vPair{1, 1}]; held {
		t.Errorf("the record is still held after Forget")
	}
}

// TestForgetOfARetiredRecordWithNoOtherCopyIsRefused: a retired record that no other member
// attests a copy of may be the only copy of the slot's data, so Forget refuses with the named
// text and the fix and nothing is removed (BUG-0795). [BREAK: skip
// the attested-copy check]
func TestForgetOfARetiredRecordWithNoOtherCopyIsRefused(t *testing.T) {
	f := newS4vFake()
	f.hold(7, 1, machine.SeedRetired, "")
	f.onForget = func(key s4vPair) (bool, error) {
		delete(f.records, key)
		return true, nil
	}
	err := f.view().Forget(context.Background(), "app", 7, false)
	if s4vForgetCode(t, err) != cluster.CodeSeedHeld {
		t.Fatalf("err = %v, want SEED_HELD", err)
	}
	for _, want := range []string{`slot 7 of database "app" is retired on this node`, "no other member has confirmed a copy covering the fence (yet)", "may hold the only copy", "Nothing was changed", "'seeding forget app 7 anyway'"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message = %q, want it to contain %q", err.Error(), want)
		}
	}
	if _, held := f.records[s4vPair{7, 1}]; !held {
		t.Errorf("the record was removed by a refused forget")
	}
}

// TestForgetOfARecordThatRetiresBetweenTheReadsIsRefused: the record is retiring at the view's
// first read and retired when the engine reads it under the pair lock, with no confirmed copy.
// The check lives in the engine, so the plain forget is refused (BUG-0795 TOCTOU).
func TestForgetOfARecordThatRetiresBetweenTheReadsIsRefused(t *testing.T) {
	f := newS4vFake()
	f.hold(7, 1, machine.SeedRetiring, "")
	f.beforeEngine = func(key s4vPair) { f.hold(key.slot, key.db, machine.SeedRetired, "") }
	f.onForget = func(key s4vPair) (bool, error) {
		delete(f.records, key)
		return true, nil
	}
	err := f.view().Forget(context.Background(), "app", 7, false)
	if s4vForgetCode(t, err) != cluster.CodeSeedHeld || !strings.Contains(err.Error(), "may hold the only copy") {
		t.Fatalf("err = %v, want the only-copy SEED_HELD refusal", err)
	}
	if _, held := f.records[s4vPair{7, 1}]; !held {
		t.Errorf("a record that retired mid-forget was removed with no confirmed copy and no anyway")
	}
}

// TestForgetAnywayRemovesARetiredRecordWithNoOtherCopy: the anyway form goes ahead where the
// plain form refuses. [BREAK: ignore the anyway flag]
func TestForgetAnywayRemovesARetiredRecordWithNoOtherCopy(t *testing.T) {
	f := newS4vFake()
	f.hold(7, 1, machine.SeedRetired, "")
	f.onForget = func(key s4vPair) (bool, error) {
		delete(f.records, key)
		return true, nil
	}
	if err := f.view().Forget(context.Background(), "app", 7, true); err != nil {
		t.Fatalf("Forget anyway = %v, want nil", err)
	}
	if len(f.forgets) != 1 {
		t.Errorf("engine calls = %v, want one", f.forgets)
	}
	if _, held := f.records[s4vPair{7, 1}]; held {
		t.Errorf("the record is still held after Forget anyway")
	}
}

// TestForgetWithAnAttestingCopyNeedsNoAnyway: on RF 2 or more with a member that has confirmed a copy the
// forget proceeds as it always did. [BREAK: refuse every retired record]
func TestForgetWithAnAttestingCopyNeedsNoAnyway(t *testing.T) {
	f := newS4vFake()
	f.hold(7, 1, machine.SeedRetired, "")
	f.attested[s4vPair{7, 1}] = 1
	f.onForget = func(key s4vPair) (bool, error) {
		delete(f.records, key)
		return true, nil
	}
	if err := f.view().Forget(context.Background(), "app", 7, false); err != nil {
		t.Fatalf("Forget with an attesting copy = %v, want nil", err)
	}
	if _, held := f.records[s4vPair{7, 1}]; held {
		t.Errorf("the record is still held")
	}
}

// TestTheOnlyCopyRefusalDoesNotFireForAHeldOrAbsentRecord: a record that is not retired keeps
// the SEED_HELD state text and an absent one the nothing-to-forget text, even with no copy
// attested and without anyway. [BREAK: test the copy before the state]
func TestTheOnlyCopyRefusalDoesNotFireForAHeldOrAbsentRecord(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, machine.SeedReady, "")
	err := f.view().Forget(context.Background(), "app", 1, false)
	if s4vForgetCode(t, err) != cluster.CodeSeedHeld || !strings.Contains(err.Error(), "is ready on this node") || strings.Contains(err.Error(), "only copy") {
		t.Errorf("held: err = %v, want the ready state text", err)
	}
	err = f.view().Forget(context.Background(), "app", 2, false)
	if s4vForgetCode(t, err) != cluster.CodeSeedNotHeld || !strings.Contains(err.Error(), "nothing to forget") {
		t.Errorf("absent: err = %v, want the nothing-to-forget text", err)
	}
}

// TestForgetOfAHeldRecordIsRefusedWithItsStateWord: a record that is not retired is refused
// with the word of its state as the operator sees it, not the machine's number. [guard: use
// the record's raw state]
func TestForgetOfAHeldRecordIsRefusedWithItsStateWord(t *testing.T) {
	cases := []struct {
		name  string
		state uint8
		pull  *PullStatus
		word  string
	}{
		{"stalled", machine.SeedSeeding, &PullStatus{State: PullStalled}, "stalled"},
		{"placing", machine.SeedSeeding, nil, "placing"},
		{"ready", machine.SeedReady, nil, "ready"},
		{"retiring", machine.SeedRetiring, nil, "retiring"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newS4vFake()
			f.hold(1, 1, tc.state, "")
			if tc.pull != nil {
				f.pulls[s4vPair{1, 1}] = *tc.pull
			}
			err := f.view().Forget(context.Background(), "app", 1, false)
			if s4vForgetCode(t, err) != cluster.CodeSeedHeld {
				t.Fatalf("code = %v, want SEED_HELD: %v", err, err)
			}
			if !strings.Contains(err.Error(), "is "+tc.word+" on this node") {
				t.Errorf("message = %q, want it to say %q", err.Error(), tc.word)
			}
		})
	}
}

// TestForgetOfNoRecordIsNothingToForget: an unknown database, a slot with no record and a slot
// number no uint16 holds are all SEED_NOT_HELD with the forget text, and the engine is never
// asked. The slot number guard is what keeps 65537 from acting on slot 1. [guard: forget the
// slot check and let 65537 reach the engine]
func TestForgetOfNoRecordIsNothingToForget(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, machine.SeedRetired, "")
	view := f.view()
	for _, tc := range []struct {
		name     string
		database string
		slot     int
	}{
		{"no record", "app", 2},
		{"unknown database", "other", 1},
		{"slot beyond uint16", "app", 65537},
		{"negative slot", "app", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := view.Forget(context.Background(), tc.database, tc.slot, false)
			if s4vForgetCode(t, err) != cluster.CodeSeedNotHeld {
				t.Fatalf("err = %v, want SEED_NOT_HELD", err)
			}
			if !strings.Contains(err.Error(), "nothing to forget") {
				t.Errorf("message = %q, want the forget text", err.Error())
			}
		})
	}
	if len(f.forgets) != 0 {
		t.Errorf("the engine was asked %v, want no call", f.forgets)
	}
	if _, held := f.records[s4vPair{1, 1}]; !held {
		t.Errorf("slot 1 lost its record to a Forget of another slot")
	}
}

// TestAForgetThatLostTheRaceIsJudgedByWhatTheRecordIsNow: the engine's answer is judged by the
// record after it, not by the record the view read first. [guard: answer SeedHeld from the
// first read]
func TestAForgetThatLostTheRaceIsJudgedByWhatTheRecordIsNow(t *testing.T) {
	t.Run("the record was removed meanwhile", func(t *testing.T) {
		f := newS4vFake()
		f.hold(1, 1, machine.SeedSeeding, "")
		f.pulls[s4vPair{1, 1}] = PullStatus{State: PullStalled}
		f.beforeEngine = func(key s4vPair) { delete(f.records, key) }
		err := f.view().Forget(context.Background(), "app", 1, false)
		if s4vForgetCode(t, err) != cluster.CodeSeedNotHeld {
			t.Fatalf("err = %v, want SEED_NOT_HELD, not the stalled word read before the lock", err)
		}
	})
	t.Run("the record became ready meanwhile", func(t *testing.T) {
		f := newS4vFake()
		f.hold(1, 1, machine.SeedRetired, "")
		f.attested[s4vPair{1, 1}] = 1
		f.beforeEngine = func(key s4vPair) { f.hold(key.slot, key.db, machine.SeedReady, "") }
		err := f.view().Forget(context.Background(), "app", 1, false)
		if s4vForgetCode(t, err) != cluster.CodeSeedHeld {
			t.Fatalf("err = %v, want SEED_HELD", err)
		}
		if !strings.Contains(err.Error(), "is ready on this node") || strings.Contains(err.Error(), "is retired on this node") {
			t.Errorf("message = %q, want the ready word and not the retired one read first", err.Error())
		}
	})
}

// TestForgetReturnsAnEngineErrorWrapped: an engine error is returned with the slot and the
// database named, and the error stays matchable. [guard: swallow it]
func TestForgetReturnsAnEngineErrorWrapped(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, machine.SeedRetired, "")
	f.attested[s4vPair{1, 1}] = 1
	engineErr := errors.New("sync the range delete: disk on fire")
	f.onForget = func(s4vPair) (bool, error) { return false, engineErr }
	err := f.view().Forget(context.Background(), "app", 1, false)
	if !errors.Is(err, engineErr) {
		t.Fatalf("err = %v, want it to wrap the engine error", err)
	}
	if !strings.Contains(err.Error(), `forgetting slot 1 of database "app"`) {
		t.Errorf("message = %q, want the slot and the database", err.Error())
	}
}

// TestForgetOfAnUnnamedStateIsAnErrorNotAWord: a record in a state no word names is an error
// that gives the state number, because the refusal has no word to print. [guard: print the
// refusal with an empty word]
func TestForgetOfAnUnnamedStateIsAnErrorNotAWord(t *testing.T) {
	f := newS4vFake()
	f.hold(1, 1, 9, "")
	err := f.view().Forget(context.Background(), "app", 1, false)
	if err == nil || strings.Contains(err.Error(), "is  on this node") {
		t.Fatalf("err = %v, want an error naming state 9", err)
	}
	if !strings.Contains(err.Error(), "state 9") || !strings.Contains(err.Error(), `database "app"`) {
		t.Errorf("message = %q, want the state number, the slot and the database", err.Error())
	}
}

// TestTheNodeInstallsItsSeedingView: a node that formed a cluster answers with the view over its
// own seeding engine, not the NoSeeding value a node without one keeps. [guard: leave it unset]
func TestTheNodeInstallsItsSeedingView(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	if _, bare := node.SeedingView().(noSeeding); bare {
		t.Fatalf("a cluster node answers with %T, want the view over its seeding engine", node.SeedingView())
	}
}

// TestTheSeedingViewIsInstalledBeforeTheNodeServes: from the moment a node begins serving, its
// seeding view answers with the engine's view, so a status call never reads the empty NoSeeding
// value while copies run. The window between serving becoming true and the view install is not
// observable from outside startCluster, so this test reads the view through the afterServingStarts
// seam at that point: the substitute for observing the window directly. [guard: install the view
// after serving.Store(true) in startCluster]
func TestTheSeedingViewIsInstalledBeforeTheNodeServes(t *testing.T) {
	var atServing SeedingView
	var reached bool
	afterServingStarts = func(n *Node) {
		reached = true
		atServing = n.SeedingView()
	}
	t.Cleanup(func() { afterServingStarts = nil })
	s0OpenClusterNode(t, t.TempDir(), nil)
	if !reached {
		t.Fatal("startCluster never reached the serving seam, so the test observed nothing")
	}
	if _, bare := atServing.(noSeeding); bare {
		t.Errorf("the node began serving with %T as its seeding view, want the view over its seeding engine", atServing)
	}
}
