package cluster

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	vendorbson "go.mongodb.org/mongo-driver/bson"
	vendorbsoncore "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/authrules"
)

var updateStateGoldens = flag.Bool("update", false, "rewrite cluster/testdata/state-vocabulary.txt and state-after-corpus.txt")
var createStateGoldens = flag.Bool("create", false, "with -update, create a golden that does not exist (a new level or a new file)")

const (
	vocabularyGolden  = "testdata/state-vocabulary.txt"
	stateAfterGolden  = "testdata/state-after-corpus.txt"
	releasesGolden    = "testdata/state-releases.txt"
	bumpInstructionFm = "the replicated-state vocabulary changed (%s): bump cluster.StateLevel, add its row to cluster/stateversions.go and regenerate cluster/testdata/state-vocabulary.txt and cluster/testdata/state-after-corpus.txt with 'go test ./cluster -run TestStateLevel -update'"
)

// goldenCommand is one step of the corpus: a command every kind of which appears, with each
// optional field it can carry, applied to the state the earlier steps built.
type goldenCommand struct {
	name string
	kind byte
	body func(st ClusterState) []byte
}

func goldenBody(b []byte) func(ClusterState) []byte { return func(ClusterState) []byte { return b } }

func goldenCorpus() []goldenCommand {
	member := func(id string, withClient bool) []byte {
		elems := [][]byte{fsmStr("id", id), fsmI64("incarnation", 1), fsmStr("data_addr", id+":1")}
		if withClient {
			elems = append(elems, fsmStr("client_addr", id+":2"))
		}
		return fsmDoc(elems...)
	}
	clusterID := stateClusterID()
	register := func(id string, witness bool) []byte {
		return fsmDoc(fsmStr("id", id), fsmI64("incarnation", 1), fsmStr("data_addr", id+":1"), fsmStr("client_addr", id+":2"),
			fsmBool("witness", witness), fsmBin("placing_set", fsmBitsAt(1, 2, 3)),
			fsmI32("min_level", int32(goldenLevels.Min)), fsmI32("max_level", int32(goldenLevels.Max)))
	}
	retired := vendorbsoncore.AppendBinaryElement(nil, "retired", 0, []byte{0, 2, 0, 3})
	return []goldenCommand{
		{"bootstrap", CommandBootstrap, goldenBody(fsmDoc(fsmBin("cluster_id", clusterID[:]), fsmVersion(), vendorbsoncore.AppendDocumentElement(nil, "self", member("n1", true))))},
		{"register n2", CommandRegisterMember, goldenBody(register("n2", false))},
		{"activate n2", CommandSetMemberState, goldenBody(fsmDoc(fsmStr("id", "n2"), fsmStr("state", "active"), fsmStr("expect_state", "joining")))},
		{"register n3", CommandRegisterMember, goldenBody(register("n3", false))},
		{"activate n3", CommandSetMemberState, goldenBody(fsmDoc(fsmStr("id", "n3"), fsmStr("state", "active")))},
		{"finalize", CommandSetClusterVersion, func(st ClusterState) []byte {
			return SetClusterVersionBody(st.ClusterVersion, st.ClusterVersion+1, st.Members)
		}},
		{"register n4", CommandRegisterMember, goldenBody(register("n4", true))},
		{"voter n2", CommandSetVoters, goldenBody(fsmVotersBody("n2", true))},
		{"voter n3", CommandSetVoters, goldenBody(fsmVotersBody("n3", true))},
		{"create app", CommandCreateDatabase, func(st ClusterState) []byte {
			return fsmDoc(fsmStr("name", "app"), fsmI64("id", st.NextDatabaseID), fsmI32("rf", 2), fsmI64("schema_version", 1), fsmStr("storage", "disk"))
		}},
		{"create cache", CommandCreateDatabase, func(st ClusterState) []byte {
			return fsmDoc(fsmStr("name", "cache"), fsmI64("id", st.NextDatabaseID), fsmI32("rf", 1), fsmI64("schema_version", 1), fsmStr("storage", "memory"))
		}},
		{"alter app", CommandAlterDatabase, goldenBody(fsmDoc(fsmI64("id", 1), fsmI32("rf", 3)))},
		{"seed n2", CommandSeeded, goldenBody(fsmNodeBitsBody("n2", 1, fsmBitsAt(5, 6)))},
		{"retire n2", CommandRetire, goldenBody(fsmNodeBitsBody("n2", 1, fsmBitsAt(5)))},
		{"bump app", CommandBumpSchema, goldenBody(fsmDoc(fsmI64("db_id", 1), fsmI64("from", 1), fsmI64("to", 2)))},
		{"bump app with note", CommandBumpSchema, goldenBody(fsmDoc(fsmI64("db_id", 1), fsmI64("from", 2), fsmI64("to", 3), fsmI64("bucket_id", 7), retired, fsmI64("epoch", 3)))},
		{"bump app drop note", CommandBumpSchema, goldenBody(fsmDoc(fsmI64("db_id", 1), fsmI64("from", 3), fsmI64("to", 4), fsmI64("bucket_id", 7), fsmBool("drop", true)))},
		{"setting stale_reads", CommandSetSetting, goldenBody(fsmDoc(fsmStr("key", "stale_reads"), fsmBool("value", true), fsmBool("expect_value", false)))},
		{"setting probe_failures", CommandSetSetting, goldenBody(fsmDoc(fsmStr("key", "probe_failures"), fsmI32("value", 4)))},
		{"setting heartbeat_ms", CommandSetSetting, goldenBody(fsmDoc(fsmStr("key", "heartbeat_ms"), fsmI64("value", 700)))},
		{"down n3", CommandSetMemberState, goldenBody(fsmDoc(fsmStr("id", "n3"), fsmStr("state", "down")))},
		{"replace n3", CommandSetMemberState, goldenBody(fsmReplaceBody("n3", 2, true))},
		{"remove n4", CommandRemoveMember, goldenBody(fsmDoc(fsmStr("id", "n4")))},
		{"drop cache", CommandDropDatabase, goldenBody(fsmDoc(fsmI64("id", 2), fsmI64("schema_version", 1)))},
		{"user bootstrap", CommandUserBootstrap, goldenBody(UserBootstrapBody{Request: goldenRequest(1), Name: "root", Credential: userCred(1), CreatedMs: 1}.Encode())},
		{"user create", CommandUserCreate, func(st ClusterState) []byte {
			return UserCreateBody{Request: goldenRequest(2), By: "root", BySeq: goldenRow(st, "root").CreatedSeq, Name: "ada", ExpectDropSeq: st.UserDropSeq,
				Roles: []authrules.Grant{readerApp}, Credential: userCred(2), Enabled: true, CreatedMs: 2}.Encode()
		}},
		{"user alter", CommandUserAlter, func(st ClusterState) []byte {
			cred, on := userCred(3), true
			roles := []authrules.Grant{readerApp, writerApp}
			return UserAlterBody{Request: goldenRequest(3), By: "root", BySeq: goldenRow(st, "root").CreatedSeq, Name: "ada", ExpectSeq: goldenRow(st, "ada").ChangedSeq,
				Credential: &cred, PasswordChangedMs: 3, Enabled: &on, Roles: &roles}.Encode()
		}},
		{"user grant", CommandUserGrant, func(st ClusterState) []byte {
			return UserRoleBody{Request: goldenRequest(4), By: "root", BySeq: goldenRow(st, "root").CreatedSeq, Name: "ada", ExpectSeq: goldenRow(st, "ada").ChangedSeq, Grant: managerAll}.Encode()
		}},
		{"user revoke", CommandUserRevoke, func(st ClusterState) []byte {
			return UserRoleBody{Request: goldenRequest(5), By: "root", BySeq: goldenRow(st, "root").CreatedSeq, Name: "ada", ExpectSeq: goldenRow(st, "ada").ChangedSeq, Grant: writerApp}.Encode()
		}},
		{"user create bob", CommandUserCreate, func(st ClusterState) []byte {
			return UserCreateBody{Request: goldenRequest(6), By: "ada", BySeq: goldenRow(st, "ada").CreatedSeq, Name: "bob", ExpectDropSeq: st.UserDropSeq,
				Roles: []authrules.Grant{readerApp}, Credential: userCred(6), Enabled: false, CreatedMs: 6}.Encode()
		}},
		{"user drop", CommandUserDrop, func(st ClusterState) []byte {
			return UserDropBody{Request: goldenRequest(7), By: "root", BySeq: goldenRow(st, "root").CreatedSeq, Name: "bob", ExpectSeq: goldenRow(st, "bob").ChangedSeq}.Encode()
		}},
		// The branches the steps above do not take: a whole role replacement (no epoch move), a
		// disable (epoch and login generation move), and a grant already held (nothing on the row
		// moves, the request list does).
		{"user create cy", CommandUserCreate, func(st ClusterState) []byte {
			return UserCreateBody{Request: goldenRequest(8), By: "root", BySeq: goldenRow(st, "root").CreatedSeq, Name: "cy", ExpectDropSeq: st.UserDropSeq,
				Roles: []authrules.Grant{readerApp}, Credential: userCred(8), Enabled: true, CreatedMs: 8}.Encode()
		}},
		{"user replace roles cy", CommandUserAlter, func(st ClusterState) []byte {
			roles := []authrules.Grant{writerApp}
			return UserAlterBody{Request: goldenRequest(9), By: "root", BySeq: goldenRow(st, "root").CreatedSeq, Name: "cy", ExpectSeq: goldenRow(st, "cy").ChangedSeq, Roles: &roles}.Encode()
		}},
		{"user disable cy", CommandUserAlter, func(st ClusterState) []byte {
			off := false
			return UserAlterBody{Request: goldenRequest(10), By: "root", BySeq: goldenRow(st, "root").CreatedSeq, Name: "cy", ExpectSeq: goldenRow(st, "cy").ChangedSeq, Enabled: &off}.Encode()
		}},
		{"user no-op grant cy", CommandUserGrant, func(st ClusterState) []byte {
			return UserRoleBody{Request: goldenRequest(11), By: "root", BySeq: goldenRow(st, "root").CreatedSeq, Name: "cy", ExpectSeq: goldenRow(st, "cy").ChangedSeq, Grant: writerApp}.Encode()
		}},
	}
}

// goldenProbes are commands the corpus's final state must refuse, recorded by code in the
// state-after golden: a refusal is also what a command does, and an accepted probe would move the
// state. The ceiling probe: ada, a user-manager on every database who is not root, drops root.
func goldenProbes(t *testing.T, final ClusterState) []string {
	t.Helper()
	probes := []struct {
		name string
		kind byte
		body []byte
	}{
		{"ceiling drop root by ada", CommandUserDrop, UserDropBody{Request: goldenRequest(100), By: "ada", BySeq: goldenRow(final, "ada").CreatedSeq, Name: "root", ExpectSeq: goldenRow(final, "root").ChangedSeq}.Encode()},
	}
	var lines []string
	for _, p := range probes {
		st := final
		_, err := st.applyCommandWith(Command{Version: CommandVersion, Kind: p.kind, BaseSeq: uint64(st.StateSeq), Body: p.body}, nil, goldenLevels)
		outcome := "accepted"
		var refusal *Error
		if errors.As(err, &refusal) {
			outcome = "refused " + refusal.Code.String()
		} else if err != nil {
			outcome = "error " + err.Error()
		}
		lines = append(lines, fmt.Sprintf("probe %s %s", p.name, outcome))
	}
	return lines
}

func goldenRequest(n byte) [UserRequestIDLen]byte {
	var id [UserRequestIDLen]byte
	id[0], id[15] = 0x60, n
	return id
}

// goldenRow is the committed row a corpus step builds its fence and grantor from; the corpus is
// fixed, so a missing row is a broken corpus.
func goldenRow(st ClusterState, name string) UserRow {
	row, ok := UserView{Users: st.Users}.User(name)
	if !ok {
		panic("corpus: no user " + name) // invariant: the corpus creates every user it names first
	}
	return row
}

// userVocabularyLines are the value domains of the user table: the role names and what each
// includes, the one last-administrator predicate, the credential window, the request-list size,
// the user ceiling, the reserved node name and the algorithm. Each is a value an applier accepts or
// a rule it applies, so a change to authrules changes this golden and needs a level bump.
func userVocabularyLines() []string {
	roles := authrules.RoleNames()
	lines := []string{"domain role names: " + strings.Join(roles, " ")}
	for _, held := range roles {
		var includes []string
		for _, granted := range roles {
			if authrules.RoleIncludes(held, granted) {
				includes = append(includes, granted)
			}
		}
		lines = append(lines, fmt.Sprintf("domain role %s includes: %s", held, strings.Join(includes, " ")))
	}
	var admins []string
	for _, role := range roles {
		for _, db := range []string{authrules.AllDatabases, "app"} {
			if authrules.CanAdministerUsers(true, []authrules.Grant{{Role: role, DB: db}}) {
				admins = append(admins, role+"@"+db)
			}
		}
	}
	return append(lines, "domain user administrators: "+strings.Join(admins, " "),
		credentialWindowLine(),
		fmt.Sprintf("domain user request list: %d", authrules.RequestListSize),
		fmt.Sprintf("domain user ceiling: %d", authrules.MaxUsers),
		fmt.Sprintf("domain reserved user name: %s", authrules.NodeUser),
		fmt.Sprintf("domain credential algorithm: %s", UserAlgo),
	)
}

// credentialWindowLine is the credential window as the golden records it, from authrules's literal
// bounds.
func credentialWindowLine() string {
	return fmt.Sprintf("domain credential window: memory_kib %d..%d iterations %d..%d parallelism %d..%d salt %d..%d hash %d",
		authrules.MinArgon2MemoryKiB, authrules.MaxArgon2MemoryKiB, authrules.MinArgon2Iterations, authrules.MaxArgon2Iterations,
		authrules.MinArgon2Threads, authrules.MaxArgon2Threads, authrules.MinSaltLen, authrules.MaxSaltLen, authrules.Argon2HashLen)
}

// TestTheCredentialWindowIsInTheStateVocabularyGolden (B11): the window apply checks is recorded in
// the vocabulary golden by its literal bounds, so moving a bound changes the golden and therefore
// needs a state level bump. Break: change one bound in authrules without regenerating; red.
func TestTheCredentialWindowIsInTheStateVocabularyGolden(t *testing.T) {
	raw, err := os.ReadFile(vocabularyGolden)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(strings.Split(string(raw), "\n"), credentialWindowLine()) {
		t.Fatalf("%s does not record the credential window %q: "+bumpInstructionFm, vocabularyGolden, credentialWindowLine(), "the credential window")
	}
}

// goldenLevels is the range the corpus runs at: this build's level and the next, so the corpus can
// move the cluster version once (SET_CLUSTER_VERSION's refusal 3 reads the range).
var goldenLevels = StateRange{Min: StateLevel, Max: StateLevel + 1, Writes: StateLevel}

// runGoldenCorpus applies the corpus, reporting every field a reader asked for and the hash of
// the encoded state after each command.
func runGoldenCorpus(t *testing.T) (vocabulary, states []string, final ClusterState) {
	t.Helper()
	seen := map[string]bool{}
	var st ClusterState
	for _, step := range goldenCorpus() {
		observe := func(where, key string, want bsontype.Type) {
			path := strings.TrimPrefix(strings.TrimPrefix(where, "command body"), " ")
			if path != "" {
				path += "."
			}
			seen[fmt.Sprintf("kind 0x%02x field %s%s %s", step.kind, path, key, want)] = true
		}
		res, err := st.applyCommandObserving(Command{Version: CommandVersion, Kind: step.kind, BaseSeq: uint64(st.StateSeq), Body: step.body(st)}, nil, observe, goldenLevels)
		if err != nil {
			t.Fatalf("corpus step %q refused: %v", step.name, err)
		}
		_ = res
		encoded, err := EncodeClusterState(st)
		if err != nil {
			t.Fatalf("corpus step %q left a state that does not encode: %v", step.name, err)
		}
		sum := sha256.Sum256(encoded)
		states = append(states, fmt.Sprintf("%02d %s %s", st.StateSeq, step.name, hex.EncodeToString(sum[:])))
	}
	for line := range seen {
		vocabulary = append(vocabulary, line)
	}
	sort.Strings(vocabulary)
	return vocabulary, states, st
}

// vocabularyLines is the golden's text: the fields read, the value domains the build accepts
// (as the build's own refusals word them), and the snapshot's names in canonical order.
func vocabularyLines(t *testing.T) []string {
	t.Helper()
	fields, _, final := runGoldenCorpus(t)
	lines := []string{
		fmt.Sprintf("command_version %d", CommandVersion),
		fmt.Sprintf("snapshot_version %d", snapshotVersion),
	}
	// The kinds are what the decoder accepts, probed over every byte, not a list kept here: a new
	// kind in knownCommandKind changes this golden. Scratch proof: add 0x10 to knownCommandKind and
	// this test is red.
	for kind := 0; kind <= 0xFF; kind++ {
		if knownCommandKind(byte(kind)) {
			lines = append(lines, fmt.Sprintf("kind 0x%02x", kind))
		}
	}
	lines = append(lines, fields...)
	// Storage words are the table ParseStorageWord reads. The canaries prove the table is the whole
	// accepted set. Scratch proof: add {"ssd", StorageDisk} to storageWordTable and this is red.
	for _, entry := range storageWordTable() {
		lines = append(lines, fmt.Sprintf("domain storage word %q -> %d", entry.word, entry.storage))
	}
	for _, canary := range []string{"", "ssd", "ram", "tiered", "Disk", "disk ", "mem"} {
		if _, err := ParseStorageWord(canary); err == nil {
			t.Fatalf("ParseStorageWord accepts %q, which storageWordTable does not list", canary)
		}
	}
	_, storageErr := ParseStorageWord("\x00probe")
	lines = append(lines, "domain storage: "+strings.SplitN(storageErr.Error(), ". ", 2)[1])
	for b := 0; b <= 0xFF; b++ {
		if state, err := DecodeMemberState(byte(b)); err == nil {
			lines = append(lines, fmt.Sprintf("domain member state byte %d -> %s", b, state))
		}
	}
	_, memberErr := DecodeMemberState(0)
	lines = append(lines, "domain member state byte: "+strings.SplitN(memberErr.Error(), ": ", 2)[1])
	_, wordErr := decodeStateWord("\x00probe")
	lines = append(lines, "domain member state word: "+wordErr.Error())
	settings := final
	_, settingErr := settings.ApplyCommand(Command{Version: CommandVersion, Kind: CommandSetSetting, BaseSeq: uint64(final.StateSeq), Body: fsmDoc(fsmStr("key", "zz_probe"), fsmI64("value", 1))})
	lines = append(lines, "domain setting keys: "+strings.SplitN(settingErr.Error(), ": use ", 2)[1])
	lines = append(lines, userVocabularyLines()...)
	encoded, err := EncodeClusterState(final)
	if err != nil {
		t.Fatalf("final corpus state does not encode: %v", err)
	}
	lines = append(lines, snapshotNames("snapshot", vendorbsoncore.Document(encoded))...)
	return lines
}

// snapshotNames lists the document's keys and BSON types in stored order, descending into
// embedded documents and into the first element of an array of them.
func snapshotNames(prefix string, doc vendorbsoncore.Document) []string {
	var lines []string
	elems, err := doc.Elements()
	if err != nil {
		return []string{prefix + " unreadable"}
	}
	for _, e := range elems {
		name := prefix + "." + e.Key()
		lines = append(lines, fmt.Sprintf("%s %s", name, e.Value().Type))
		switch e.Value().Type {
		case vendorbson.TypeEmbeddedDocument:
			lines = append(lines, snapshotNames(name, e.Value().Document())...)
		case vendorbson.TypeArray:
			if first, err := e.Value().Array().IndexErr(0); err == nil && first.Type == vendorbson.TypeEmbeddedDocument {
				lines = append(lines, snapshotNames(name+"[]", first.Document())...)
			}
		}
	}
	return lines
}

// goldenLevelLine heads every golden: the StateLevel it was recorded at. -update refuses to
// rewrite a golden that already carries the current level with other contents, so a change that
// regenerates the file without bumping StateLevel cannot get past the build.
func goldenLevelLine() string { return fmt.Sprintf("level %d", StateLevel) }

// goldenUpdateRefusal says why -update may not write path, or "" when it may. A golden at the
// build's own level is never rewritten with other contents, and a missing one is created only with
// -create, so deleting a file does not get around the bump.
func goldenUpdateRefusal(path string, old []byte, readErr error, text string, create bool) string {
	if readErr != nil {
		if create {
			return ""
		}
		return fmt.Sprintf("%s does not exist (%v): -update does not create a golden unless -create is given, because a deleted golden would otherwise be regenerated at the same level", path, readErr)
	}
	if bytes.HasPrefix(old, []byte(goldenLevelLine()+"\n")) && !bytes.Equal(old, []byte(text)) {
		return fmt.Sprintf("%s was recorded at level %d and the build still declares level %d but its contents changed: bump cluster.StateLevel and add its row to cluster/stateversions.go first, then regenerate", path, StateLevel, StateLevel)
	}
	return ""
}

func TestUpdateRefusesAGoldenItWouldRecreateOrRewriteAtTheSameLevel(t *testing.T) {
	same := []byte(goldenLevelLine() + "\nold\n")
	if goldenUpdateRefusal("g", nil, os.ErrNotExist, "x", false) == "" {
		t.Error("a missing golden was created without -create")
	}
	if goldenUpdateRefusal("g", nil, os.ErrNotExist, "x", true) != "" {
		t.Error("-create did not allow creating a golden")
	}
	if goldenUpdateRefusal("g", same, nil, goldenLevelLine()+"\nnew\n", false) == "" {
		t.Error("a golden at this level was rewritten with other contents")
	}
	if goldenUpdateRefusal("g", []byte("level 0\nold\n"), nil, goldenLevelLine()+"\nnew\n", false) != "" {
		t.Error("a golden from an older level could not be regenerated")
	}
}

func checkGolden(t *testing.T, path string, got []string, what string) {
	t.Helper()
	got = append([]string{goldenLevelLine()}, got...)
	text := strings.Join(got, "\n") + "\n"
	if *updateStateGoldens {
		old, err := os.ReadFile(path)
		if refusal := goldenUpdateRefusal(path, old, err, text, *createStateGoldens); refusal != "" {
			t.Fatal(refusal)
		}
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s is missing (%v): "+bumpInstructionFm, path, err, what)
	}
	if bytes.Equal(want, []byte(text)) {
		return
	}
	wantLines := strings.Split(strings.TrimSuffix(string(want), "\n"), "\n")
	for i := 0; i < len(got) || i < len(wantLines); i++ {
		var g, w string
		if i < len(got) {
			g = got[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if g != w {
			t.Fatalf(bumpInstructionFm+"\nfirst difference at line %d:\n  golden: %q\n  build:  %q", what, i+1, w, g)
		}
	}
}

func TestStateLevelMovesWithTheVocabulary(t *testing.T) {
	checkGolden(t, vocabularyGolden, vocabularyLines(t), "a field, kind, value domain or snapshot name")
}

func TestStateLevelMovesWhenTheStateAfterACommandMoves(t *testing.T) {
	_, states, final := runGoldenCorpus(t)
	checkGolden(t, stateAfterGolden, append(states, goldenProbes(t, final)...), "what a command does to the state")
}

func TestEveryStateLevelHasARowAndRowsOnlyGrow(t *testing.T) {
	rows := StateVersions()
	if len(rows) == 0 || rows[len(rows)-1].Level != StateLevel {
		t.Fatalf("the newest row of stateVersions must be StateLevel %d; add its row to cluster/stateversions.go in the commit that bumps it", StateLevel)
	}
	for i, row := range rows {
		if row.Level != uint8(i+1) {
			t.Fatalf("stateVersions row %d is level %d: levels run from 1 without a gap", i, row.Level)
		}
	}
	// A row whose release is set is never edited: an operator was told to upgrade to it.
	raw, err := os.ReadFile(filepath.Clean(releasesGolden))
	if err != nil {
		t.Fatalf("%s: %v", releasesGolden, err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var level uint8
		var release string
		if _, err := fmt.Sscanf(line, "%d %s", &level, &release); err != nil {
			t.Fatalf("%s line %q: %v", releasesGolden, line, err)
		}
		if ReleaseOf(level) != release {
			t.Fatalf("level %d was released in %q and the table now says %q: a released row is never edited", level, release, ReleaseOf(level))
		}
	}
	for _, row := range rows {
		if row.FirstRelease == "" {
			continue
		}
		if !bytes.Contains(raw, []byte(fmt.Sprintf("%d %s\n", row.Level, row.FirstRelease))) {
			t.Fatalf("level %d names release %q, which %s does not record: add the line there when a release ships", row.Level, row.FirstRelease, releasesGolden)
		}
	}
}

func TestBuildStateRangeIsExactBeforeTheFirstRelease(t *testing.T) {
	r := BuildStateRange()
	if r.Min != StateLevel || r.Max != StateLevel || r.Writes != StateLevel {
		t.Fatalf("range %+v: before the first release a build declares Min == Max == Writes == StateLevel", r)
	}
	other := StateRange{Min: StateLevel + 1, Max: StateLevel + 1, Writes: StateLevel + 1}
	if r.Overlaps(other) || other.Overlaps(r) {
		t.Fatal("two exact ranges at different levels must not overlap")
	}
	if !r.Overlaps(r) {
		t.Fatal("a range overlaps itself")
	}
	widened := StateRange{Min: StateLevel, Max: StateLevel + 1, Writes: StateLevel}
	if !widened.Overlaps(r) || !r.Overlaps(widened) {
		t.Fatal("a widened range containing the other's level and writes overlaps it")
	}
	if (StateRange{}).Overlaps(r) {
		t.Fatal("level 0 (a build from before this check) must not overlap a current build")
	}
}

func TestAMalformedRangeOverlapsNothing(t *testing.T) {
	build := BuildStateRange()
	for name, r := range map[string]StateRange{"min above max": {Min: 2, Max: 1, Writes: 1}, "zero min under a real max": {Min: 0, Max: 1, Writes: 1}, "writes above max": {Min: 1, Max: 1, Writes: 2}} {
		if build.Overlaps(r) || r.Overlaps(build) {
			t.Errorf("%s: overlaps the build", name)
		}
	}
	if (StateRange{}).Overlaps(StateRange{}) {
		t.Error("two ranges that declare nothing overlap")
	}
}
