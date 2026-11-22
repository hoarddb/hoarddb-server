package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// The tests of cluster-version spec 11.1 (step 3a-1) that live in this package. Each names, in
// its comment, the implementation it is shown against.

// cvRange is a stub range: a build that runs versions lo to hi and writes lo.
func cvRange(lo, hi uint8) *StateRange { return &StateRange{Min: lo, Max: hi, Writes: lo} }

// cvState is a state at cluster version v with members n1, n2 and n3 active, built by the
// commands themselves under levels.
func cvState(t *testing.T, v uint8, levels StateRange) ClusterState {
	t.Helper()
	var st ClusterState
	cvApply(t, &st, levels, CommandBootstrap, BootstrapBody(stateClusterID(), Member{ID: "n1", Incarnation: 1, DataAddr: "n1:1"}, v))
	for _, id := range []string{"n2", "n3"} {
		cvApply(t, &st, levels, CommandRegisterMember, RegisterMemberBody(Member{ID: id, Incarnation: 1, DataAddr: id + ":1"}, fsmBitsAt(1), levels))
		cvApply(t, &st, levels, CommandSetMemberState, fsmSetMemberStateBody(id, "active"))
	}
	return st
}

func cvTry(st *ClusterState, levels StateRange, kind byte, body []byte) (Result, error) {
	return st.applyCommandWith(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(st.StateSeq), Body: body}, nil, levels)
}

func cvApply(t *testing.T, st *ClusterState, levels StateRange, kind byte, body []byte) Result {
	t.Helper()
	res, err := cvTry(st, levels, kind, body)
	if err != nil {
		t.Fatalf("kind 0x%02x refused: %v", kind, err)
	}
	return res
}

func cvWantRefusal(t *testing.T, err error, code Code, words string) {
	t.Helper()
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
	if !strings.Contains(err.Error(), words) {
		t.Fatalf("the refusal %q does not say %q", err, words)
	}
}

// TestTheClusterVersionIsTheSnapshotsSecondField (A1): the canonical document carries
// cluster_version as an int32 directly after v, the decoder reads it back and refuses 0 and a value
// above 255, and the open check reads it out of a snapshot whose later fields this build does not
// know. Against: the field written last, where a later build's unknown field comes first.
func TestTheClusterVersionIsTheSnapshotsSecondField(t *testing.T) {
	st := stateFull()
	st.ClusterVersion = 7
	raw, err := EncodeClusterState(st)
	if err != nil {
		t.Fatal(err)
	}
	elems, err := bsoncore.Document(raw).Elements()
	if err != nil {
		t.Fatal(err)
	}
	if elems[0].Key() != "v" || elems[1].Key() != "cluster_version" {
		t.Fatalf("the first two fields are %q and %q", elems[0].Key(), elems[1].Key())
	}
	if got, ok := elems[1].Value().Int32OK(); !ok || got != 7 {
		t.Fatalf("cluster_version holds %v (int32 %v)", elems[1].Value(), ok)
	}
	back, err := DecodeClusterState(raw)
	if err != nil || back.ClusterVersion != 7 {
		t.Fatalf("decoded version %d, %v", back.ClusterVersion, err)
	}
	for _, bad := range []int32{0, 256, -1} {
		forged := bytes.Replace(raw, []byte("cluster_version\x00\x07\x00\x00\x00"), binary.LittleEndian.AppendUint32([]byte("cluster_version\x00"), uint32(bad)), 1)
		if _, err := DecodeClusterState(forged); err == nil || !strings.Contains(err.Error(), "cluster_version") {
			t.Errorf("cluster_version %d was accepted: %v", bad, err)
		}
	}
	// A later build's snapshot: an unknown field after the known ones still names its version.
	withField := append([]byte{}, raw[:len(raw)-1]...)
	withField = bsoncore.AppendInt32Element(withField, "zz_later_field", 1)
	withField = append(withField, 0)
	binary.LittleEndian.PutUint32(withField, uint32(len(withField)))
	if version, ok := snapshotClusterVersion(forgedSnapshot(t, st, snapshotVersion, snapshotFlagBSON, withField)); !ok || version != 7 {
		t.Fatalf("the version of a later build's snapshot read as %d (%v)", version, ok)
	}
}

// TestTheEncoderRefusesAStateWithNoClusterVersion: a state whose cluster version is 0 (one that
// no BOOTSTRAP has set) is refused by the encoder, by name, so no node writes a snapshot the
// decoder of every build refuses (the decoder reads 0 as damage). Against: an encoder that writes
// whatever the field holds.
func TestTheEncoderRefusesAStateWithNoClusterVersion(t *testing.T) {
	st := stateFull()
	st.ClusterVersion = 0
	if _, err := EncodeClusterState(st); err == nil || !strings.Contains(err.Error(), "cluster_version") {
		t.Fatalf("EncodeClusterState of a state at version 0 gave %v", err)
	}
	if _, err := EncodeSnapshot(st); err == nil || !strings.Contains(err.Error(), "cluster_version") {
		t.Fatalf("EncodeSnapshot of a state at version 0 gave %v", err)
	}
	fsm := &clusterFSM{}
	if _, err := fsm.Snapshot(); err == nil {
		t.Fatal("the FSM of a node with no state wrote a snapshot")
	}
}

// TestBootstrapSetsTheClusterVersionToTheBootstrappersMax (A2): a node that runs versions 1 to 3
// bootstraps its cluster at 3, from the body, and a replica that applies the same BOOTSTRAP takes
// the body's version, never its own. Against: the bootstrap at Min.
func TestBootstrapSetsTheClusterVersionToTheBootstrappersMax(t *testing.T) {
	cfg := quickRaft(t, t.TempDir(), "n1")
	cfg.StateRange = cvRange(1, 3)
	r := openOrFail(t, cfg)
	if got := r.State().ClusterVersion; got != 3 {
		t.Fatalf("the cluster bootstrapped at version %d, want 3 (the bootstrapper's Max)", got)
	}
	if got := r.ClusterVersion(); got != 3 {
		t.Fatalf("Raft.ClusterVersion is %d, want 3", got)
	}
	var replica ClusterState
	cvApply(t, &replica, StateRange{Min: 1, Max: 9, Writes: 1}, CommandBootstrap, BootstrapBody(stateClusterID(), Member{ID: "n1", Incarnation: 1, DataAddr: "n1:1"}, 2))
	if replica.ClusterVersion != 2 {
		t.Fatalf("a replica that runs 1 to 9 took version %d from a body that says 2", replica.ClusterVersion)
	}
}

// TestSetClusterVersionRefusals (A3): refusals 1, 2, 4 and 5 of 4.3, each leaving the state as it
// was, and an accepted command that moves the version and state_seq but not the epoch. Against:
// no from check.
func TestSetClusterVersionRefusals(t *testing.T) {
	levels := StateRange{Min: 1, Max: 5, Writes: 1}
	base := cvState(t, 1, levels)
	cases := []struct {
		name  string
		edit  func(st *ClusterState)
		body  func(st ClusterState) []byte
		code  Code
		words string
	}{
		{"from is not the cluster's version", nil, func(st ClusterState) []byte { return SetClusterVersionBody(2, 3, st.Members) },
			CodeConflict, "the cluster runs version 1, not 2"},
		{"to is not above from", nil, func(st ClusterState) []byte { return SetClusterVersionBody(1, 1, st.Members) },
			CodeInvalidArgument, "finalize moves the cluster to a higher version, and 1 is not above 1"},
		{"a member is joining", func(st *ClusterState) {
			cvApply(t, st, levels, CommandRegisterMember, RegisterMemberBody(Member{ID: "n4", Incarnation: 1, DataAddr: "n4:1"}, fsmBitsAt(1), levels))
		}, func(st ClusterState) []byte { return SetClusterVersionBody(1, 2, st.Members) }, CodeConflict, "node n4 is joining"},
		{"a member is leaving", func(st *ClusterState) {
			cvApply(t, st, levels, CommandSetMemberState, fsmSetMemberStateBody("n3", "leaving"))
		}, func(st ClusterState) []byte { return SetClusterVersionBody(1, 2, st.Members) }, CodeConflict, "node n3 is leaving"},
		{"a member was not checked", nil, func(st ClusterState) []byte { return SetClusterVersionBody(1, 2, st.Members[:2]) },
			CodeConflict, "the members changed since finalize checked them (node n3 is a member and was not checked)"},
		{"a checked node is not a member", nil, func(st ClusterState) []byte {
			return SetClusterVersionBody(1, 2, append(st.Members, Member{ID: "n9", Incarnation: 1}))
		}, CodeConflict, "node n9 was checked and is not a member"},
		{"a member restarted", nil, func(st ClusterState) []byte {
			listed := append([]Member(nil), st.Members...)
			listed[1].Incarnation = 2
			return SetClusterVersionBody(1, 2, listed)
		}, CodeConflict, "node n2 is at incarnation 1, not the 2 that was checked"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := base.clone()
			if c.edit != nil {
				c.edit(&st)
			}
			before := fsmEncoded(t, st)
			_, err := cvTry(&st, levels, CommandSetClusterVersion, c.body(st))
			cvWantRefusal(t, err, c.code, c.words)
			if !bytes.Equal(before, fsmEncoded(t, st)) {
				t.Fatal("a refused SET_CLUSTER_VERSION changed the state")
			}
		})
	}
	st := base.clone()
	res := cvApply(t, &st, levels, CommandSetClusterVersion, SetClusterVersionBody(1, 2, st.Members))
	if st.ClusterVersion != 2 || st.StateSeq != base.StateSeq+1 || st.Epoch != base.Epoch || res.EpochMoved {
		t.Fatalf("accepted: version %d state_seq %d epoch %d (moved %v), want 2, %d, %d", st.ClusterVersion, st.StateSeq, st.Epoch, res.EpochMoved, base.StateSeq+1, base.Epoch)
	}
}

// TestFinalizeMayJumpSeveralLevels (A4): from 1 to 5 is accepted when the build's Max is 5 or
// more, and refused (refusal 3) when it is 4. Against: to == from + 1.
func TestFinalizeMayJumpSeveralLevels(t *testing.T) {
	wide := StateRange{Min: 1, Max: 5, Writes: 1}
	st := cvState(t, 1, wide)
	cvApply(t, &st, wide, CommandSetClusterVersion, SetClusterVersionBody(1, 5, st.Members))
	if st.ClusterVersion != 5 {
		t.Fatalf("version %d after finalize from 1 to 5", st.ClusterVersion)
	}
	narrow := StateRange{Min: 1, Max: 4, Writes: 1}
	short := cvState(t, 1, narrow)
	_, err := cvTry(&short, narrow, CommandSetClusterVersion, SetClusterVersionBody(1, 5, short.Members))
	cvWantRefusal(t, err, CodeClusterMismatch, "the cluster version is 5 and this build runs versions 1 to 4: the cluster was finalized past this build")
	var unrunnable *ClusterVersionError
	if !errors.As(err, &unrunnable) || unrunnable.Version != 5 {
		t.Fatalf("refusal 3 is %T, want a *ClusterVersionError at version 5", err)
	}
}

// cvReopened is a node bootstrapped by a build that runs version 1 only, closed and opened again
// on a build that runs 1 to hi: a cluster at version 1 whose node could finalize to hi.
func cvReopened(t *testing.T, dir string, hi uint8, edit func(*RaftConfig)) *Raft {
	t.Helper()
	cfg := quickRaft(t, dir, "n1")
	cfg.StateRange = cvRange(1, 1)
	first, err := OpenRaft(cfg)
	if err != nil {
		t.Fatalf("OpenRaft: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	cfg = quickRaft(t, dir, "n1")
	cfg.StateRange = cvRange(1, hi)
	if edit != nil {
		edit(&cfg)
	}
	r := openOrFail(t, cfg)
	waitFor(t, func() bool { return r.IsLeader() && r.State().ClusterVersion == 1 })
	return r
}

func waitFor(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal("the condition never held")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func cvFinalize(r *Raft, from, to uint8) error {
	_, err := r.propose(CommandSetClusterVersion, SetClusterVersionBody(from, to, r.State().Members))
	return err
}

// TestANodeThatCannotRunTheNewVersionStopsOnTheEntry (A5): a follower whose build runs version 1
// only applies a committed finalize to 2: refusal 3 at apply records a divergence whose reason, and
// stop line, is the 4.4 text (finalized past this build, keep the data directory, exit 70), not the
// generic text that says to run the proposer's build, and the leader, which runs 1 to 5, moves on.
// Against: no build-max check (the follower takes version 2 and runs on); and refusal 3 as a plain
// refusal (the stop line tells a release-1 operator to start the proposer's build).
func TestANodeThatCannotRunTheNewVersionStopsOnTheEntry(t *testing.T) {
	leader := cvReopened(t, t.TempDir(), 5, nil)
	cfg := quickRaft(t, t.TempDir(), "n2")
	cfg.Bootstrap = false
	cfg.StateRange = cvRange(1, 1)
	follower := openOrFail(t, cfg)
	if _, err := leader.Join(JoinRequest{Member: Member{ID: "n2", Incarnation: 1, DataAddr: cfg.Listener.Addr().String()},
		RaftAddr: cfg.Listener.Addr().String(), PlacingSet: make([]byte, 1024), Range: *cvRange(1, 1)}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if _, err := leader.propose(CommandSetMemberState, fsmSetMemberStateBody("n2", "active")); err != nil {
		t.Fatal(err)
	}
	if err := cvFinalize(leader, 1, 2); err != nil {
		t.Fatalf("finalize on the leader: %v", err)
	}
	record := waitDiverged(t, follower)
	// Prefix and suffix, not the whole text: a release row for level 2 would change the middle
	// ("Start this node on <release> or later").
	want := "the cluster version is 2 and this build runs versions 1 to 1: the cluster was finalized past this build. "
	if record.Kind != CommandSetClusterVersion || !strings.HasPrefix(record.Reason, want) || !strings.HasSuffix(record.Reason, "Keep the data directory. Exit code 70.") {
		t.Fatalf("the follower recorded %+v, want a reason starting %q", record, want)
	}
	if follower.fsm.unrunnable.Load() == nil {
		t.Fatal("the refusal was not kept as the cluster version refusal that stopped the node")
	}
	if follower.ClusterVersion() != 1 {
		t.Fatalf("the follower took version %d", follower.ClusterVersion())
	}
	if leader.IsDiverged() || leader.ClusterVersion() != 2 {
		t.Fatalf("the leader diverged (%v) or is at version %d", leader.IsDiverged(), leader.ClusterVersion())
	}
}

// cvLogOutcomes replays every command entry r's log holds onto an empty state, in order, under
// levels, and returns the outcome of each: what every node did with the entries.
func cvLogOutcomes(t *testing.T, r *Raft, levels StateRange) []error {
	t.Helper()
	first, err := r.store.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	last, err := r.store.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	var st ClusterState
	var outcomes []error
	for index := first; index <= last && index > 0; index++ {
		var entry hraft.Log
		if err := r.store.GetLog(index, &entry); err != nil {
			t.Fatalf("log index %d: %v", index, err)
		}
		if entry.Type != hraft.LogCommand {
			continue
		}
		cmd, err := DecodeCommand(entry.Data)
		if err != nil {
			t.Fatalf("log index %d does not decode: %v", index, err)
		}
		_, err = st.applyCommandWith(cmd, nil, levels)
		outcomes = append(outcomes, err)
	}
	return outcomes
}

// TestASecondConcurrentFinalizeNeverReachesTheLogAsARefusal (A10, the M4 invariant): two
// finalizes stamped at one state_seq both pass their dry runs (the FSM is held so the first has
// not applied when the second is appended); the second meets STATE_MOVED at apply, is stamped
// again, and its second dry run refuses it (refusal 1), so nothing is appended for it. Every
// committed entry's outcome is accepted or STATE_MOVED and the node records no divergence.
// Against: a refusal-1 check moved before the base_seq comparison (the second entry is then
// refused CONFLICT at apply, which stops the node).
func TestASecondConcurrentFinalizeNeverReachesTheLogAsARefusal(t *testing.T) {
	var hold sync.Mutex
	held := make(chan struct{}, 1)
	release := make(chan struct{})
	armed := false
	gate := func(kind byte) {
		hold.Lock()
		stop := armed && kind == CommandSetClusterVersion
		armed = armed && !stop
		hold.Unlock()
		if stop {
			held <- struct{}{}
			<-release
		}
	}
	levels := StateRange{Min: 1, Max: 5, Writes: 1}
	r := cvReopened(t, t.TempDir(), 5, func(cfg *RaftConfig) { cfg.ApplyGate = gate })
	hold.Lock()
	armed = true
	hold.Unlock()
	body := SetClusterVersionBody(1, 2, r.State().Members)
	stamp := uint64(r.State().StateSeq)
	results := make(chan error, 2)
	go func() {
		_, err := r.Propose(Command{Version: CommandVersion, Kind: CommandSetClusterVersion, BaseSeq: stamp, Body: body})
		results <- err
	}()
	<-held // the first finalize is committed and held before it applies
	appended, err := r.store.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, err := r.propose(CommandSetClusterVersion, body)
		results <- err
	}()
	waitFor(t, func() bool { last, _ := r.store.LastIndex(); return last > appended })
	close(release)
	var accepted, refused int
	for range 2 {
		if err := <-results; err != nil {
			cvWantRefusal(t, err, CodeConflict, "the cluster runs version 2, not 1")
			refused++
		} else {
			accepted++
		}
	}
	if accepted != 1 || refused != 1 {
		t.Fatalf("%d accepted and %d refused, want one each", accepted, refused)
	}
	moved := 0
	for i, outcome := range cvLogOutcomes(t, r, levels) {
		switch {
		case outcome == nil:
		case isStateMoved(outcome):
			moved++
		default:
			t.Fatalf("committed command %d was refused at apply: %v", i, outcome)
		}
	}
	if moved != 1 {
		t.Fatalf("%d committed entries were STATE_MOVED, want the second finalize's first stamp", moved)
	}
	if r.IsDiverged() || r.ClusterVersion() != 2 {
		t.Fatalf("diverged %v, version %d", r.IsDiverged(), r.ClusterVersion())
	}
}

// TestRaftClusterVersionMovesOnlyWhenTheEntryApplies (A11): ClusterVersion is the applied
// version: unchanged while a committed finalize is held before the FSM applies it, moved once it
// applies, and set by a Restore. Against: reading the version from the proposal.
func TestRaftClusterVersionMovesOnlyWhenTheEntryApplies(t *testing.T) {
	held := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	r := cvReopened(t, t.TempDir(), 5, func(cfg *RaftConfig) {
		cfg.ApplyGate = func(kind byte) {
			if kind == CommandSetClusterVersion {
				once.Do(func() { close(held); <-release })
			}
		}
	})
	done := make(chan error, 1)
	go func() { done <- cvFinalize(r, 1, 3) }()
	<-held
	if got := r.ClusterVersion(); got != 1 {
		t.Fatalf("the version is %d before the entry applied", got)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := r.ClusterVersion(); got != 3 {
		t.Fatalf("the version is %d after the entry applied, want 3", got)
	}
	fsm := &clusterFSM{levels: StateRange{Min: 1, Max: 5, Writes: 1}}
	st := cvState(t, 4, fsm.levels)
	raw, err := EncodeSnapshot(st)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(nopCloser{bytes.NewReader(raw)}); err != nil {
		t.Fatal(err)
	}
	if got := fsm.version.Load(); got != 4 {
		t.Fatalf("the version is %d after a Restore of a state at 4", got)
	}
}

type nopCloser struct{ *bytes.Reader }

func (nopCloser) Close() error { return nil }

// TestMinStateLevelIsThePreviousReleasesLevel (A9): with no release recorded every build is
// [L, L]; once a release is recorded the build runs from that release's level, whatever number of
// development levels lie between; the release build itself runs from the release before it.
// Against: StateLevel - 1.
func TestMinStateLevelIsThePreviousReleasesLevel(t *testing.T) {
	cases := []struct {
		name  string
		rows  []StateVersionRow
		level uint8
		want  uint8
	}{
		{"no release yet", []StateVersionRow{{1, ""}, {2, ""}}, 2, 2},
		{"the first release itself", []StateVersionRow{{1, "1.1"}}, 1, 1},
		{"one development level after it", []StateVersionRow{{1, "1.1"}, {2, ""}}, 2, 1},
		{"several development levels", []StateVersionRow{{1, "1.1"}, {2, ""}, {3, ""}, {4, ""}}, 4, 1},
		{"the second release itself", []StateVersionRow{{1, "1.1"}, {2, ""}, {3, "1.2"}}, 3, 1},
		{"after the second release", []StateVersionRow{{1, "1.1"}, {2, ""}, {3, "1.2"}, {4, ""}, {5, ""}}, 5, 3},
	}
	for _, c := range cases {
		if got := minStateLevelOf(c.rows, c.level); got != c.want {
			t.Errorf("%s: MinStateLevel %d, want %d", c.name, got, c.want)
		}
	}
	if MinStateLevel() != minStateLevelOf(stateVersions, StateLevel) || BuildStateRange().Min != MinStateLevel() {
		t.Fatal("the build's range does not start at MinStateLevel")
	}
}

func cvMarker(t *testing.T, dir string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, clusterVersionFile))
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestTheMarkerFollowsTheAppliedVersion (A17): the CLUSTER_VERSION marker is written after the
// BOOTSTRAP, after a SET_CLUSTER_VERSION and after a Restore that moves the version, survives the
// node's close, and is never written for another kind; a marker one change behind still lets the
// Apply check stop a build that cannot run the log. Against: no marker write on Restore.
func TestTheMarkerFollowsTheAppliedVersion(t *testing.T) {
	dir := t.TempDir()
	r := cvReopened(t, dir, 5, nil)
	if got := cvMarker(t, dir); got != "hoarddb cluster version 1\n" {
		t.Fatalf("after the bootstrap the marker says %q", got)
	}
	if err := cvFinalize(r, 1, 3); err != nil {
		t.Fatal(err)
	}
	if got := cvMarker(t, dir); got != "hoarddb cluster version 3\n" {
		t.Fatalf("after the finalize the marker says %q", got)
	}
	if err := os.Remove(filepath.Join(dir, clusterVersionFile)); err != nil {
		t.Fatal(err)
	}
	createDatabase(t, r, "app", 1)
	if got := cvMarker(t, dir); got != "" {
		t.Fatalf("CREATE_DATABASE wrote the marker: %q", got)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// One change behind: the marker says 1 and the log holds the finalize to 3. A build that runs
	// version 1 only passes the open check and is stopped by the entry.
	if err := writeClusterVersionMarker(dir, 1); err != nil {
		t.Fatal(err)
	}
	cfg := quickRaft(t, dir, "n1")
	cfg.StateRange = cvRange(1, 1)
	behind, err := OpenRaft(cfg)
	if err == nil {
		t.Cleanup(func() { _ = behind.Close() })
		record := waitDiverged(t, behind)
		if record.Kind != CommandSetClusterVersion {
			t.Fatalf("the behind marker's node recorded %+v", record)
		}
	} else if !strings.Contains(err.Error(), "the cluster version is 3 and this build runs versions 1 to 1") {
		t.Fatalf("the open failed without the entry's refusal: %v", err)
	}

	restored := t.TempDir()
	fsm := &clusterFSM{levels: StateRange{Min: 1, Max: 5, Writes: 1}, markerDir: restored, logger: hclog.NewNullLogger()}
	raw, err := EncodeSnapshot(cvState(t, 4, fsm.levels))
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(nopCloser{bytes.NewReader(raw)}); err != nil {
		t.Fatal(err)
	}
	if got := cvMarker(t, restored); got != "hoarddb cluster version 4\n" {
		t.Fatalf("after a Restore the marker says %q", got)
	}
}

// TestTheMarkerNeverMovesDown (A17, the replay half): the open restores its own older snapshot,
// or replays a BOOTSTRAP below the version the marker records, on its way to the version the log
// ends at; the marker keeps the higher version, since it is a lower bound of the final one and
// moving it down would let a crash in the replay lose what it proved. Against: a rewrite whenever
// the version differs from the marker's.
func TestTheMarkerNeverMovesDown(t *testing.T) {
	dir := t.TempDir()
	if err := writeClusterVersionMarker(dir, 2); err != nil {
		t.Fatal(err)
	}
	fsm := &clusterFSM{levels: StateRange{Min: 1, Max: 3, Writes: 1}, markerDir: dir, markerAt: 2, logger: hclog.NewNullLogger()}
	raw, err := EncodeSnapshot(cvState(t, 1, fsm.levels))
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(nopCloser{bytes.NewReader(raw)}); err != nil {
		t.Fatal(err)
	}
	if got := cvMarker(t, dir); got != "hoarddb cluster version 2\n" {
		t.Fatalf("a replay through version 1 left the marker saying %q", got)
	}
	fsm.followVersion(3)
	if got := cvMarker(t, dir); got != "hoarddb cluster version 3\n" {
		t.Fatalf("a rise to 3 left the marker saying %q", got)
	}
}

// TestADirectoryAboveTheBuildsMaxIsRefusedInRestore (A6, the Restore half): a leader-sent snapshot
// at a version above this build's Max stops the node with the 4.4 text in its no-release form and
// leaves the state as it was. Against: the snapshot loads silently.
func TestADirectoryAboveTheBuildsMaxIsRefusedInRestore(t *testing.T) {
	fsm := &clusterFSM{levels: StateRange{Min: 1, Max: 1, Writes: 1}}
	raw, err := EncodeSnapshot(cvState(t, 2, StateRange{Min: 1, Max: 2, Writes: 1}))
	if err != nil {
		t.Fatal(err)
	}
	err = fsm.Restore(nopCloser{bytes.NewReader(raw)})
	var unrunnable *ClusterVersionError
	if !errors.As(err, &unrunnable) {
		t.Fatalf("the Restore answered %v, want a *ClusterVersionError", err)
	}
	record := fsm.diverged.Load()
	want := "the cluster version is 2 and this build runs versions 1 to 1: the cluster was finalized past this build. "
	if record == nil || !strings.HasPrefix(record.Reason, want) || !strings.HasSuffix(record.Reason, "Keep the data directory. Exit code 70.") {
		t.Fatalf("recorded %+v, want a reason starting %q", record, want)
	}
	if fsm.state.StateSeq != 0 || fsm.version.Load() != 0 {
		t.Fatalf("the state was replaced (state_seq %d, version %d)", fsm.state.StateSeq, fsm.version.Load())
	}
}

// TestALaterBuildsSnapshotAboveMaxIsRefusedWithTheVersionText (4.4 check 2): a leader-sent
// snapshot from a later build, at a version above this build's Max and carrying a field this build
// does not know, is refused with the 4.4 text that names the build to run, not the foreign-snapshot
// text, because its version is read before the decode that fails on the unknown field. Against:
// DecodeSnapshot first (the operator reads "written by another build" and no version).
func TestALaterBuildsSnapshotAboveMaxIsRefusedWithTheVersionText(t *testing.T) {
	st := cvState(t, 2, StateRange{Min: 1, Max: 2, Writes: 1})
	raw, err := EncodeClusterState(st)
	if err != nil {
		t.Fatal(err)
	}
	withField := append([]byte{}, raw[:len(raw)-1]...)
	withField = bsoncore.AppendInt32Element(withField, "zz_later_field", 1)
	withField = append(withField, 0)
	binary.LittleEndian.PutUint32(withField, uint32(len(withField)))
	fsm := &clusterFSM{levels: StateRange{Min: 1, Max: 1, Writes: 1}}
	err = fsm.Restore(nopCloser{bytes.NewReader(forgedSnapshot(t, st, snapshotVersion, snapshotFlagBSON, withField))})
	var unrunnable *ClusterVersionError
	if !errors.As(err, &unrunnable) || unrunnable.Version != 2 {
		t.Fatalf("the Restore answered %v, want a *ClusterVersionError at version 2", err)
	}
	want := "the cluster version is 2 and this build runs versions 1 to 1: the cluster was finalized past this build."
	if record := fsm.diverged.Load(); record == nil || !strings.HasPrefix(record.Reason, want) {
		t.Fatalf("recorded %+v, want a reason starting %q", record, want)
	}
	if fsm.unrunnable.Load() == nil {
		t.Fatal("the refusal was not kept for OpenRaft to return typed")
	}
}

// TestADirectoryBelowTheBuildsMinStartsFromTheLog (A7, the log-only variant, inverted by the owner
// ruling of 2026-11-09): a directory whose only record of its version is the BOOTSTRAP at 1 in its
// log, opened by a build whose range starts at 2, opens and runs. Every version a node holds locally
// (its log, its snapshot, its marker) is at most the cluster's committed version, so a local value
// below Min never proves the node is below the cluster: the cluster may have been finalized past it
// while this node was down, and the leader's entries bring it up. The lower bound is judged only by
// a holder of the committed version (step 3c's leader check). The range is a stub (this build's Min
// is StateLevel). Against: the lower bound judged at the open's last local command.
func TestADirectoryBelowTheBuildsMinStartsFromTheLog(t *testing.T) {
	dir := t.TempDir()
	cfg := quickRaft(t, dir, "n1")
	cfg.StateRange = cvRange(1, 1)
	first, err := OpenRaft(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, clusterVersionFile)); err != nil {
		t.Fatal(err)
	}
	cfg = quickRaft(t, dir, "n1")
	cfg.StateRange = cvRange(2, 2)
	r := openOrFail(t, cfg)
	waitFor(t, func() bool { return r.IsDiverged() || (r.LogApplied() && r.ClusterVersion() == 1) })
	if record, diverged := r.Divergence(); diverged {
		t.Fatalf("the node stopped on a local version below Min: %+v", record)
	}
}

// cvFinalizedOneToTwo is a node directory whose cluster bootstrapped at version 1 and was finalized
// to 2, closed. snapshotFirst takes a snapshot at version 1 before the finalize, so the newest
// snapshot is older than the version the log ends at.
func cvFinalizedOneToTwo(t *testing.T, snapshotFirst bool) string {
	t.Helper()
	dir := t.TempDir()
	r := cvReopened(t, dir, 2, nil)
	if snapshotFirst {
		if err := r.TakeSnapshot(); err != nil {
			t.Fatal(err)
		}
	}
	if err := cvFinalize(r, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestAHistoricalVersionBelowMinDoesNotRefuseTheOpen (4.4 checks 2 and 3, the lower bound on the
// final state only): a directory finalized from 1 to 2, opened by a build that runs 2 to 3, opens
// and ends at version 2, whether the replay starts from the BOOTSTRAP at version 1 or from an older
// snapshot at version 1. The version only rises, so a version the replay passes through says
// nothing about the one it ends at. Against: the lower bound applied to BOOTSTRAP and to a local
// Restore (the node stops on history it has already moved past).
func TestAHistoricalVersionBelowMinDoesNotRefuseTheOpen(t *testing.T) {
	for name, snapshotFirst := range map[string]bool{"from the bootstrap": false, "from an older snapshot": true} {
		t.Run(name, func(t *testing.T) {
			dir := cvFinalizedOneToTwo(t, snapshotFirst)
			cfg := quickRaft(t, dir, "n1")
			cfg.StateRange = cvRange(2, 3)
			r := openOrFail(t, cfg)
			waitFor(t, func() bool { return r.IsDiverged() || (r.LogApplied() && r.ClusterVersion() == 2) })
			if record, diverged := r.Divergence(); diverged {
				t.Fatalf("the node stopped on its history: %+v", record)
			}
		})
	}
}

// TestALeaderSentSnapshotBelowMinRestores (4.4 check 2, inverted by the owner ruling of
// 2026-11-09): a snapshot below this build's Min restores, whether the node's own during the open or
// one the leader sent. The leader's InstallSnapshot sends its newest stored snapshot, not its state
// now, and that snapshot can predate a finalize indefinitely; the entries after it bring the node to
// the cluster's version (TestAJoinerAfterAFinalizeInstallsTheLeadersOlderSnapshot). Against: the
// leader-sent snapshot taken for the cluster's state now and refused below Min.
func TestALeaderSentSnapshotBelowMinRestores(t *testing.T) {
	raw, err := EncodeSnapshot(cvState(t, 1, StateRange{Min: 1, Max: 3, Writes: 1}))
	if err != nil {
		t.Fatal(err)
	}
	fsm := &clusterFSM{levels: StateRange{Min: 2, Max: 3, Writes: 2}}
	if err := fsm.Restore(nopCloser{bytes.NewReader(raw)}); err != nil {
		t.Fatalf("a snapshot below Min was refused: %v", err)
	}
	if record := fsm.diverged.Load(); record != nil || fsm.version.Load() != 1 {
		t.Fatalf("recorded %+v, version %d after the Restore, want no record and version 1", record, fsm.version.Load())
	}
}

// cvUpgradeLeader is a leader whose build runs versions 1 to 3, its cluster at version 1. Its stub
// Writes is 2, the version it writes once a finalize to 2 applies (step 3c makes Writes the applied
// version), so the acceptor of a member reopened on a build that runs 2 to 3 admits its dial.
func cvUpgradeLeader(t *testing.T) *Raft {
	t.Helper()
	return cvReopened(t, t.TempDir(), 3, func(cfg *RaftConfig) { cfg.StateRange = &StateRange{Min: 1, Max: 3, Writes: 2} })
}

// cvJoin registers the node cfg opens as a member of leader's cluster and activates it.
func cvJoin(t *testing.T, leader *Raft, cfg RaftConfig) {
	t.Helper()
	if _, err := leader.Join(JoinRequest{Member: Member{ID: cfg.NodeID, Incarnation: 1, DataAddr: cfg.Listener.Addr().String()},
		RaftAddr: cfg.Listener.Addr().String(), PlacingSet: make([]byte, 1024), Range: *cfg.StateRange}); err != nil {
		t.Fatalf("join %s: %v", cfg.NodeID, err)
	}
	if _, err := leader.propose(CommandSetMemberState, fsmSetMemberStateBody(cfg.NodeID, "active")); err != nil {
		t.Fatalf("activate %s: %v", cfg.NodeID, err)
	}
}

// cvCaughtUp waits until r has applied cluster version want, and fails if it stopped instead.
func cvCaughtUp(t *testing.T, r *Raft, want uint8) {
	t.Helper()
	waitFor(t, func() bool { return r.IsDiverged() || r.ClusterVersion() == want })
	if record, diverged := r.Divergence(); diverged {
		t.Fatalf("the node stopped instead of reaching cluster version %d: %+v", want, record)
	}
}

// TestALaggingMemberUpgradedAfterAFinalizeCatchesUp (owner ruling 2026-11-09, the lower bound is
// judged only by a holder of the committed V): a member on a build that runs 1 to 2 joins a
// cluster at version 1 and is closed; the cluster is finalized to 2 while it is down; the member is
// started again on the same address on a build that runs 2 to 3. Its own log ends at version 1, and
// the library applies that log (in appendEntries) before the leader's finalize behind it, so a
// node that judged Min on what it holds locally stops on a state the cluster has left. It must run
// on and reach version 2. Against: the lower bound judged at the open's last local command.
func TestALaggingMemberUpgradedAfterAFinalizeCatchesUp(t *testing.T) {
	leader := cvUpgradeLeader(t)
	dir := t.TempDir()
	cfg := quickRaft(t, dir, "n2")
	cfg.Bootstrap = false
	cfg.StateRange = cvRange(1, 2)
	addr := cfg.Listener.Addr().String()
	member, err := OpenRaft(cfg)
	if err != nil {
		t.Fatalf("OpenRaft n2: %v", err)
	}
	member.Admit() // cvJoin goes through Raft.Join, which asks no identity (see openOrFail)
	cvJoin(t, leader, cfg)
	waitFor(t, func() bool { row, ok := fsmMember(member.State(), "n2"); return ok && row.State == MemberActive })
	if err := member.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cvFinalize(leader, 1, 2); err != nil {
		t.Fatalf("finalize while n2 is down: %v", err)
	}
	// The port was ephemeral and just released: another test's socket can hold it for a moment, so
	// the bind is retried before the test gives up.
	var listener net.Listener
	for deadline := time.Now().Add(5 * time.Second); ; {
		listener, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("listening again on n2's address %s: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cfg = quickRaft(t, dir, "n2")
	if err := cfg.Listener.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Listener, cfg.Bootstrap, cfg.StateRange = listener, false, cvRange(2, 3)
	cvCaughtUp(t, openOrFail(t, cfg), 2)
}

// TestAJoinerAfterAFinalizeInstallsTheLeadersOlderSnapshot (owner ruling 2026-11-09): the library's
// InstallSnapshot sends the leader's newest snapshot, not its state now, and with a snapshot
// threshold of 128 a snapshot taken before a finalize stays the newest indefinitely. Here the leader
// holds a snapshot at version 1, taken after enough commands that the log no longer starts at 1,
// then the cluster is finalized to 2 and a fresh node on a build that runs 2 to 3 joins: it installs
// the version 1 snapshot, applies the finalize after it and reaches version 2. Against: Restore
// refusing a leader-sent snapshot below Min (the joiner stops on a snapshot older than the cluster).
func TestAJoinerAfterAFinalizeInstallsTheLeadersOlderSnapshot(t *testing.T) {
	leader := cvUpgradeLeader(t)
	for i := range 80 {
		if _, err := leader.propose(CommandSetSetting, fsmDoc(fsmStr("key", "dead_ms"), fsmI64("value", int64(10_000+i)))); err != nil {
			t.Fatalf("SET_SETTING %d: %v", i, err)
		}
	}
	if err := leader.TakeSnapshot(); err != nil {
		t.Fatal(err)
	}
	if first, err := leader.store.FirstIndex(); err != nil || first <= 1 {
		t.Fatalf("the leader's log starts at %d (%v): a joiner would replay it instead of installing the snapshot", first, err)
	}
	if err := cvFinalize(leader, 1, 2); err != nil {
		t.Fatal(err)
	}
	cfg := quickRaft(t, t.TempDir(), "n3")
	cfg.Bootstrap = false
	cfg.StateRange = cvRange(2, 3)
	joiner := openOrFail(t, cfg)
	cvJoin(t, leader, cfg)
	cvCaughtUp(t, joiner, 2)
}

// TestAJoinCheckedBeforeAFinalizeAndCommittedAfterIsRefused (A8): a joiner judged when the
// cluster ran version 1, whose build runs version 1 only, registered after a finalize to 2: its
// REGISTER_MEMBER is refused at the leader's dry run with text F8, no member row exists and no
// registration reaches the log. Against: REGISTER_MEMBER without the range (the joiner becomes a
// joining member that can never run the cluster's version).
func TestAJoinCheckedBeforeAFinalizeAndCommittedAfterIsRefused(t *testing.T) {
	leader := cvReopened(t, t.TempDir(), 5, nil)
	if err := cvFinalize(leader, 1, 2); err != nil {
		t.Fatal(err)
	}
	before := len(cvLogOutcomes(t, leader, StateRange{Min: 1, Max: 5, Writes: 1}))
	_, err := leader.Join(JoinRequest{Member: Member{ID: "n2", Incarnation: 1, DataAddr: "n2:1"}, RaftAddr: "127.0.0.1:1",
		PlacingSet: make([]byte, 1024), Range: *cvRange(1, 1)})
	cvWantRefusal(t, err, CodeClusterMismatch, "cannot join n2@n2:1: the cluster moved to version 2 while the join was checked, and n2 runs versions 1 to 1. "+
		"Upgrade n2 to a build whose state_range includes 2, then run 'join n2@n2:1' again. Nothing was registered.")
	if _, registered := fsmMember(leader.State(), "n2"); registered {
		t.Fatal("the refused joiner has a member row")
	}
	if after := len(cvLogOutcomes(t, leader, StateRange{Min: 1, Max: 5, Writes: 1})); after != before {
		t.Fatalf("the log grew from %d to %d commands: the registration was appended", before, after)
	}
}

// TestTheOpenCheckReadsTheMarkerAndTheNewestSnapshot is the unit half of check 1: the larger of
// the two is judged, a directory with neither is not, a refusal on the marker's version says it came
// from the marker file and what a stale marker means (review finding 5), and a marker this build
// did not write is refused naming the file.
func TestTheOpenCheckReadsTheMarkerAndTheNewestSnapshot(t *testing.T) {
	levels := StateRange{Min: 2, Max: 3, Writes: 2}
	empty := t.TempDir()
	if err := CheckDataDirClusterVersion(empty, levels); err != nil {
		t.Fatalf("an empty directory was judged: %v", err)
	}
	marked := t.TempDir()
	if err := writeClusterVersionMarker(marked, 4); err != nil {
		t.Fatal(err)
	}
	var unrunnable *ClusterVersionError
	err := CheckDataDirClusterVersion(marked, levels)
	if !errors.As(err, &unrunnable) || unrunnable.Version != 4 {
		t.Fatalf("a marker at 4 against 2 to 3 gave %v", err)
	}
	// The version came from the marker alone, so the text says so, and what a stale marker means.
	markerSays := "The version was read from the marker file " + filepath.Join(marked, clusterVersionFile) +
		"; if this node's raft state is at an older version, the marker is stale: the raft directory was lost or restored from an older backup." +
		" If it is, move " + filepath.Join(marked, clusterVersionFile) + " aside and start this build again."
	if !strings.Contains(err.Error(), markerSays) || !strings.HasSuffix(err.Error(), "Exit code 70.") {
		t.Fatalf("the marker refusal says %q, want it to contain %q and end with the exit code", err, markerSays)
	}
	// A marker below Min is not judged: the marker and the snapshot are lower bounds of the
	// cluster's committed version, so only a holder of that version can say this node is below Min.
	behind := t.TempDir()
	if err := writeClusterVersionMarker(behind, 1); err != nil {
		t.Fatal(err)
	}
	if err := CheckDataDirClusterVersion(behind, levels); err != nil {
		t.Fatalf("a marker at 1 against 2 to 3 was refused from the files: %v", err)
	}
	snapshotted := t.TempDir()
	raw, err := EncodeSnapshot(cvState(t, 5, StateRange{Min: 1, Max: 5, Writes: 1}))
	if err != nil {
		t.Fatal(err)
	}
	writeSnapshot(t, snapshotted, raw)
	if err := writeClusterVersionMarker(snapshotted, 2); err != nil {
		t.Fatal(err)
	}
	if err := CheckDataDirClusterVersion(snapshotted, levels); !errors.As(err, &unrunnable) || unrunnable.Version != 5 {
		t.Fatalf("a snapshot at 5 behind a marker at 2 gave %v", err)
	} else if strings.Contains(err.Error(), "marker") {
		t.Fatalf("a refusal on the snapshot's version names the marker: %q", err)
	}
	broken := t.TempDir()
	if err := os.WriteFile(filepath.Join(broken, clusterVersionFile), []byte("hoarddb cluster version 07\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = CheckDataDirClusterVersion(broken, levels)
	if err == nil || !strings.Contains(err.Error(), "Move the file aside") {
		t.Fatalf("a marker this build did not write gave %v", err)
	}
	// Typed, so the server exits 70 on it (review finding 20): restarting cannot fix the file.
	var unreadable *ClusterVersionMarkerError
	if !errors.As(err, &unreadable) || unreadable.Path != filepath.Join(broken, clusterVersionFile) || !strings.HasSuffix(err.Error(), "Exit code 70.") {
		t.Fatalf("a marker this build did not write gave %T %v, want a *ClusterVersionMarkerError naming the file and ending with the exit code", err, err)
	}
}

// TestAnOpenFailureKeepsBothErrorsMatchable (review finding 13): when a cluster version refusal
// stopped the open, OpenRaft's error wraps both the refusal and the library's own error, so a
// caller can match either with errors.Is or errors.As. Against: the second error formatted with
// %v, which drops it from the chain.
func TestAnOpenFailureKeepsBothErrorsMatchable(t *testing.T) {
	fsm := &clusterFSM{}
	refusal := &ClusterVersionError{Version: 3, Range: StateRange{Min: 1, Max: 2, Writes: 1}}
	fsm.unrunnable.Store(refusal)
	library := errors.New("start raft: the library failed")
	err := fsm.openFailure(library)
	var unrunnable *ClusterVersionError
	if !errors.As(err, &unrunnable) || unrunnable != refusal {
		t.Fatalf("the open failure %v does not carry the cluster version refusal", err)
	}
	if !errors.Is(err, library) {
		t.Fatalf("the open failure %v does not carry the library's error", err)
	}
}

// TestABootstrapAboveMaxSaysTheClusterWasBootstrappedThere (review S4-1): a BOOTSTRAP entry at a
// version above this build's Max is refused with a text that does not claim a finalize happened,
// and keeps the rest of 4.4's text.
func TestABootstrapAboveMaxSaysTheClusterWasBootstrappedThere(t *testing.T) {
	levels := StateRange{Min: 1, Max: 1, Writes: 1}
	var st ClusterState
	_, err := cvTry(&st, levels, CommandBootstrap, BootstrapBody(stateClusterID(), Member{ID: "n1", Incarnation: 1, DataAddr: "n1:1"}, 2))
	var unrunnable *ClusterVersionError
	if !errors.As(err, &unrunnable) || unrunnable.Version != 2 {
		t.Fatalf("a BOOTSTRAP at 2 against 1 to 1 gave %v, want a *ClusterVersionError at version 2", err)
	}
	if text := err.Error(); strings.Contains(text, "finalized") ||
		!strings.HasPrefix(text, "the cluster version is 2 and this build runs versions 1 to 1: the cluster was bootstrapped at a version past this build. ") ||
		!strings.HasSuffix(text, "Keep the data directory. Exit code 70.") {
		t.Fatalf("the BOOTSTRAP refusal says %q", text)
	}
}
