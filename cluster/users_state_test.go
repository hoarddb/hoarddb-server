package cluster

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/authrules"
)

const userTestKID = "0a1b2c3d"

// userTestRow is a valid row whose salt and hash are unique to name and seq.
func userTestRow(name string, seq int64, grants ...authrules.Grant) UserRow {
	salt := bytes.Repeat([]byte{byte(seq)}, authrules.MinSaltLen)
	copy(salt, name)
	hash := bytes.Repeat([]byte{0xA5}, authrules.Argon2HashLen)
	copy(hash, name)
	return UserRow{
		Name: name, Roles: authrules.NormalizeGrants(grants), Algo: UserAlgo, Salt: salt, Hash: hash, PepperKID: userTestKID,
		Memory: authrules.MinArgon2MemoryKiB, Iterations: authrules.MinArgon2Iterations, Parallelism: 4, Enabled: true,
		CreatedMs: 1_700_000_000_000, PasswordChangedMs: 1_700_000_000_000, CreatedSeq: seq, ChangedSeq: seq,
	}
}

func userTestRequest(b byte, seq int64) UserRequest {
	var id [UserRequestIDLen]byte
	for i := range id {
		id[i] = b
	}
	return UserRequest{ID: id, StateSeq: seq}
}

// userTestState is stateFull with a user table: root and ops, the pepper id and two requests.
func userTestState() ClusterState {
	st := stateFull()
	st.Users = []UserRow{
		userTestRow("ops", 30, authrules.Grant{Role: authrules.RoleUserManager, DB: "*"}, authrules.Grant{Role: authrules.RoleReader, DB: "app"}),
		userTestRow("root", 20, authrules.Grant{Role: authrules.RoleRoot, DB: "*"}),
	}
	st.PepperKID = userTestKID
	st.UserRequests = []UserRequest{userTestRequest(1, 20), userTestRequest(2, 30)}
	return st
}

// snapshotOfBody wraps a state document in the snapshot file layout with a valid checksum.
func snapshotOfBody(body []byte, epoch, stateSeq int64) []byte {
	raw := make([]byte, snapshotHeaderBytes, snapshotHeaderBytes+len(body)+snapshotCRCBytes)
	copy(raw, snapshotMagic)
	raw[4], raw[5] = snapshotVersion, snapshotFlagBSON
	binary.BigEndian.PutUint64(raw[8:], uint64(epoch))
	binary.BigEndian.PutUint64(raw[16:], uint64(stateSeq))
	binary.BigEndian.PutUint32(raw[24:], uint32(len(body)))
	raw = append(raw, body...)
	return binary.BigEndian.AppendUint32(raw, crc32.Checksum(raw, snapshotCRCTable))
}

// TestSnapshotCarriesUsersAndRestoresThem: the user table, the pepper key id and the request list
// travel in a snapshot and come back byte for byte, through the decoder and through the FSM's
// Restore, which is how a joiner and a restarted node get them (spec 6.6).
func TestSnapshotCarriesUsersAndRestoresThem(t *testing.T) {
	st := userTestState()
	raw, err := EncodeSnapshot(st)
	if err != nil {
		t.Fatalf("EncodeSnapshot: %v", err)
	}
	got, err := DecodeSnapshot(raw)
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if !reflect.DeepEqual(got.Users, st.Users) || got.PepperKID != st.PepperKID || !reflect.DeepEqual(got.UserRequests, st.UserRequests) {
		t.Fatalf("snapshot round trip changed the user table:\n got %+v %q %+v\nwant %+v %q %+v", got.Users, got.PepperKID, got.UserRequests, st.Users, st.PepperKID, st.UserRequests)
	}
	again, err := EncodeSnapshot(got)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("the decoded snapshot does not encode to the same bytes (%v)", err)
	}
	f := &clusterFSM{}
	if err := f.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	view := f.userView()
	if !reflect.DeepEqual(view.Users, st.Users) || view.PepperKID != userTestKID || !reflect.DeepEqual(view.Requests, st.UserRequests) || view.StateSeq != st.StateSeq {
		t.Fatalf("the restored FSM reads another user table: %+v", view)
	}
}

// TestASnapshotWithoutTheUserTableIsRefusedAsABuildDifference: a snapshot with no user fields
// (as a build without the user table wrote it) is refused as written by another build, not as
// damage, so the node stops with text 2 instead of falling back to an older snapshot. The level
// numbering restarts at 1 at the first release, so no earlier level exists to stamp it with; the
// snapshot keeps this build's version and only the fields are missing.
func TestASnapshotWithoutTheUserTableIsRefusedAsABuildDifference(t *testing.T) {
	st := stateFull()
	current, err := EncodeClusterState(st)
	if err != nil {
		t.Fatalf("EncodeClusterState: %v", err)
	}
	elems, err := bsoncore.Document(current).Elements()
	if err != nil {
		t.Fatal(err)
	}
	var older []byte
	for _, e := range elems {
		switch e.Key() {
		case "users", "pepper_kid", "user_requests":
		default:
			older = append(older, e...)
		}
	}
	raw := snapshotOfBody(bsoncore.BuildDocument(nil, older), st.Epoch, st.StateSeq)
	_, err = DecodeSnapshot(raw)
	if err == nil {
		t.Fatal("a snapshot without the user table decoded")
	}
	phrase, foreign := SnapshotBuildDifference(err)
	if !foreign {
		t.Fatalf("refusal %v is not a build difference", err)
	}
	if !strings.Contains(phrase+err.Error(), "users") {
		t.Fatalf("refusal %v does not name the missing field users", err)
	}
}

// TestNonUserCommandsDoNotCopyTheUserTable: every non-user command leaves the user table and the
// request list as the very slices it was given (spec 4.1), so a command that is not about users
// costs nothing per user. Break: make clone copy Users; red.
func TestNonUserCommandsDoNotCopyTheUserTable(t *testing.T) {
	base := fsmCluster(t)
	users := userTestState()
	base.Users, base.PepperKID, base.UserRequests = users.Users, users.PepperKID, users.UserRequests
	for kind, body := range fsmKindCases(base) {
		st := base
		fsmApply(t, &st, kind, body)
		if &st.Users[0] != &base.Users[0] || &st.UserRequests[0] != &base.UserRequests[0] {
			t.Errorf("kind 0x%02x copied the user table or the request list", kind)
		}
	}
}

// TestTheUserAccessorSharesTheTableWithoutCloningTheState: the follower's read allocates nothing
// and hands back the committed slices, however many members, databases and seed bitmaps the state
// holds (spec 6.3, B16).
func TestTheUserAccessorSharesTheTableWithoutCloningTheState(t *testing.T) {
	f := &clusterFSM{state: userTestState()}
	if allocs := testing.AllocsPerRun(100, func() { _ = f.userView() }); allocs != 0 {
		t.Fatalf("userView allocated %v times per read; it must share the committed table", allocs)
	}
	view := f.userView()
	if &view.Users[0] != &f.state.Users[0] || view.StateSeq != f.state.StateSeq || view.PepperKID != userTestKID {
		t.Fatal("userView does not return the committed table")
	}
	if seq, ok := view.RequestApplied(userTestRequest(2, 0).ID); !ok || seq != 30 {
		t.Fatalf("RequestApplied = %d, %v; want 30, true", seq, ok)
	}
	if _, ok := view.RequestApplied(userTestRequest(9, 0).ID); ok {
		t.Fatal("RequestApplied found a request the list does not hold")
	}
}

// TestTheUserTableDecodesStrictlyAndCanonically: one byte string per table. Every row is sorted by
// name and appears once, roles are sorted and known, lengths are in range, and the request list is
// bounded and ascending; anything else is refused.
func TestTheUserTableDecodesStrictlyAndCanonically(t *testing.T) {
	for name, edit := range map[string]func(*ClusterState){
		"duplicate name":          func(st *ClusterState) { st.Users[1].Name = st.Users[0].Name },
		"unsorted names":          func(st *ClusterState) { st.Users[0], st.Users[1] = st.Users[1], st.Users[0] },
		"unsorted roles":          func(st *ClusterState) { r := st.Users[0].Roles; r[0], r[1] = r[1], r[0] },
		"duplicate role":          func(st *ClusterState) { st.Users[0].Roles[1] = st.Users[0].Roles[0] },
		"unknown role":            func(st *ClusterState) { st.Users[1].Roles[0].Role = "superuser" },
		"empty database":          func(st *ClusterState) { st.Users[1].Roles[0].DB = "" },
		"short salt":              func(st *ClusterState) { st.Users[0].Salt = st.Users[0].Salt[:authrules.MinSaltLen-1] },
		"long salt":               func(st *ClusterState) { st.Users[0].Salt = make([]byte, authrules.MaxSaltLen+1) },
		"short hash":              func(st *ClusterState) { st.Users[0].Hash = st.Users[0].Hash[:31] },
		"row kid not hex":         func(st *ClusterState) { st.Users[0].PepperKID = "ZZZZZZZZ" },
		"state kid":               func(st *ClusterState) { st.PepperKID = "abc" },
		"other algo":              func(st *ClusterState) { st.Users[0].Algo = "scrypt" },
		"epoch over uint32":       func(st *ClusterState) { st.Users[0].Epoch = 1 << 32 },
		"no created_seq":          func(st *ClusterState) { st.Users[0].CreatedSeq, st.Users[0].ChangedSeq = 0, 0 },
		"requests unsorted":       func(st *ClusterState) { r := st.UserRequests; r[0], r[1] = r[1], r[0] },
		"row kid not the state's": func(st *ClusterState) { st.Users[0].PepperKID = "ffffffff" },
		"drop seq above state":    func(st *ClusterState) { st.UserDropSeq = st.StateSeq + 1 },
		"negative drop seq":       func(st *ClusterState) { st.UserDropSeq = -1 },
		"rows and no state kid":   func(st *ClusterState) { st.PepperKID = "" },
		"over the user ceiling": func(st *ClusterState) {
			st.Users = nil
			for i := 0; i <= authrules.MaxUsers; i++ {
				st.Users = append(st.Users, userTestRow(fmt.Sprintf("u%05d", i), 2))
			}
		},
		"too many requests": func(st *ClusterState) {
			st.UserRequests = nil
			for i := 0; i <= authrules.RequestListSize; i++ {
				st.UserRequests = append(st.UserRequests, userTestRequest(byte(i), int64(i+1)))
			}
		},
	} {
		st := userTestState()
		st.Users = []UserRow{userTestState().Users[0], userTestState().Users[1]}
		edit(&st)
		if _, err := DecodeClusterState(encodeClusterStateInGivenOrder(st)); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
	raw := fsmEncoded(t, userTestState())
	if _, err := DecodeClusterState(raw); err != nil {
		t.Fatalf("the canonical table does not decode: %v", err)
	}
}
