package wire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// storageElement reports whether the BSON document holds a top-level storage member and its value.
func storageElement(t *testing.T, raw []byte) (string, bool) {
	t.Helper()
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	value, ok := doc["storage"]
	if !ok {
		return "", false
	}
	word, isString := value.(string)
	if !isString {
		t.Fatalf("storage is %T, want string", value)
	}
	return word, true
}

// TestCreateDatabaseOverTheWireCarriesStorageBothWays: [GUARD decode storage in the request and
// drop it in the reply, or the reverse].
func TestCreateDatabaseOverTheWireCarriesStorageBothWays(t *testing.T) {
	asked, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminCreateDatabase, Name: "scratch", Storage: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	if word, ok := storageElement(t, asked); !ok || word != "memory" {
		t.Fatalf("request storage member = %q, %v; want \"memory\"", word, ok)
	}
	decoded, err := DecodeRingAdmin(asked)
	if err != nil || decoded.Storage != "memory" {
		t.Fatalf("decoded request %+v, %v", decoded, err)
	}
	plain, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminCreateDatabase, Name: "plain"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := storageElement(t, plain); ok {
		t.Error("a create that names no storage still carries a storage member")
	}

	answered, err := EncodeRingAdminReply(RingAdminReply{Cmd: AdminCreateDatabase, DatabaseID: 3, RF: 1, Storage: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	if word, ok := storageElement(t, answered); !ok || word != "memory" {
		t.Fatalf("reply storage member = %q, %v; want \"memory\"", word, ok)
	}
	back, err := DecodeRingAdminReply(answered)
	if err != nil || back.Storage != "memory" {
		t.Fatalf("decoded reply %+v, %v", back, err)
	}

	env := u66Start(t)
	c := env.login(t)
	refused := u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "scratch", Storage: "memory"}), cluster.CodeInvalidArgument)
	if refused.Message != memoryStorageRefusalFor("scratch") {
		t.Fatalf("create memory refusal %q", refused.Message)
	}
	if _, ok := env.node.topology.DatabaseID("scratch"); ok {
		t.Error("a refused memory create committed the database")
	}
	disk := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "plain"}))
	if disk.Storage != "disk" || env.node.topology.MemoryDatabase(uint64(disk.DatabaseID)) {
		t.Fatalf("create with no storage: reply %+v, want a disk database", disk)
	}
	explicit := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "plain2", Storage: "disk"}))
	if explicit.Storage != "disk" {
		t.Fatalf("create with storage disk: reply %+v", explicit)
	}
}

// TestAdminRefusesStorageOnOtherCommands: [GUARD adminStray not updated: describe_database
// accepts a storage member; a reply carries storage where it must not].
func TestAdminRefusesStorageOnOtherCommands(t *testing.T) {
	others := []RingAdminRequest{
		{Cmd: AdminCreateBucket, Database: "app", Name: "User", Fields: []RingAdminField{}},
		{Cmd: AdminDescribeBucket, Database: "app", Bucket: "User"},
		{Cmd: AdminAlterBucket, Database: "app", Bucket: "User", Add: []RingAdminField{{Name: "A", Type: "string"}}, Drop: []string{}},
		{Cmd: AdminShowBuckets, Database: "app"},
		{Cmd: AdminDescribeDatabase, Database: "app"},
		{Cmd: AdminShowDatabases},
		{Cmd: AdminDropBucket, Database: "app", Bucket: "User"},
		{Cmd: AdminDescribeNode},
		{Cmd: AdminLeave, NodeID: "node-a"},
	}
	for _, req := range others {
		if _, err := EncodeRingAdmin(req); err != nil {
			t.Fatalf("%s without storage: %v", req.Cmd, err)
		}
		req.Storage = "memory"
		_, err := EncodeRingAdmin(req)
		if err == nil || !strings.Contains(err.Error(), "storage") {
			t.Errorf("%s with storage: err = %v, want a refusal that names storage", req.Cmd, err)
		}
	}

	_, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminCreateDatabase, Name: "app", Storage: "ram"})
	if err == nil || !strings.Contains(err.Error(), `storage is "ram"`) || !strings.Contains(err.Error(), "disk or memory") {
		t.Errorf("an unknown storage word: err = %v", err)
	}

	listed := []RingAdminReply{
		{Cmd: AdminCreateBucket, BucketID: 7, State: "active", Fields: []RingAdminReplyField{}, Storage: "disk"},
		{Cmd: AdminShowBuckets, Buckets: []RingAdminListed{}, Storage: "disk"},
		{Cmd: AdminShowDatabases, Databases: []RingAdminDatabase{}, Storage: "disk"},
		{Cmd: AdminShowBuckets, Buckets: []RingAdminListed{}, MemoryBytes: 1},
	}
	for _, reply := range listed {
		if _, err := EncodeRingAdminReply(reply); err == nil {
			t.Errorf("a %s reply with storage or memory figures was accepted: %+v", reply.Cmd, reply)
		}
	}

	bad := []RingAdminReply{
		{Cmd: AdminCreateDatabase, DatabaseID: 1, RF: 1},
		{Cmd: AdminCreateDatabase, DatabaseID: 1, RF: 1, Storage: "ram"},
		{Cmd: AdminCreateDatabase, DatabaseID: 1, RF: 1, Storage: "disk", MemoryBytes: 5},
		{Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Buckets: []RingAdminListed{}},
		{Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Buckets: []RingAdminListed{}, Storage: "ram"},
		{Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Buckets: []RingAdminListed{}, Storage: "disk", MemoryBytes: 5},
		{Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Buckets: []RingAdminListed{}, Storage: "disk", MemoryCapBytes: 5},
	}
	for _, reply := range bad {
		if _, err := EncodeRingAdminReply(reply); err == nil {
			t.Errorf("the strict reply check accepted %+v", reply)
		}
	}
	good := RingAdminReply{Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Buckets: []RingAdminListed{}, Storage: "memory", MemoryBytes: 5, MemoryCapBytes: 10}
	raw, err := EncodeRingAdminReply(good)
	if err != nil {
		t.Fatalf("a memory describe reply with figures was refused: %v", err)
	}
	back, err := DecodeRingAdminReply(raw)
	if err != nil || back.Storage != "memory" || back.MemoryBytes != 5 || back.MemoryCapBytes != 10 {
		t.Fatalf("describe reply round trip %+v, %v", back, err)
	}
}

// TestDescribeDatabaseOfADiskDatabaseAnswersDisk: [GUARD fill Storage from the Memory bool as a
// string: a disk database answers "" and the strict reply check refuses every describe database].
func TestDescribeDatabaseOfADiskDatabaseAnswersDisk(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "plain"}))
	if _, err := env.node.CreateDatabaseWithStorage(context.Background(), "scratch", 1, "memory"); err != nil {
		t.Fatal(err)
	}
	disk := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminDescribeDatabase, Database: "plain"}))
	if disk.Storage != "disk" || disk.MemoryBytes != 0 || disk.MemoryCapBytes != 0 {
		t.Errorf("describe of a disk database: %+v", disk)
	}
	memory := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminDescribeDatabase, Database: "scratch"}))
	if memory.Storage != "memory" {
		t.Errorf("describe of a memory database: %+v", memory)
	}
}

// memoryStorageRefusalFor is the sentence a create database with storage memory is refused with.
// It is written out in full so a reworded production text fails these tests.
func memoryStorageRefusalFor(name string) string {
	return "storage: memory is not available yet: this build keeps every database on disk, so a memory database would not be lost on restart as its name says. Create it with storage: disk (the default): create database " + name
}

// TestCreateMemoryDatabaseIsRefusedOnTheWireWhateverTheCopies: a create with storage memory is
// refused with the rule 3 sentence for one copy and for two, and commits nothing.
func TestCreateMemoryDatabaseIsRefusedOnTheWireWhateverTheCopies(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	one, two := int32(1), int32(2)
	for _, rf := range []*int32{nil, &one, &two} {
		refusal := u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "scratch", RF: rf, Storage: "memory"}), cluster.CodeInvalidArgument)
		if refusal.Message != memoryStorageRefusalFor("scratch") {
			t.Errorf("message = %q, want exactly %q", refusal.Message, memoryStorageRefusalFor("scratch"))
		}
	}
	if _, ok := env.node.topology.DatabaseID("scratch"); ok {
		t.Error("a refused create_database committed the database")
	}
}

// TestCreateDatabaseWithStorageMemoryIsRefusedByTheServerHandler: the handler is the authority, so
// it refuses before it takes the admin lock or reads the node: the server here has no node, and any
// step past the refusal would block on the held lock or fail on the nil node.
// [BREAK: delete the storage branch of createDatabase]
func TestCreateDatabaseWithStorageMemoryIsRefusedByTheServerHandler(t *testing.T) {
	server := &ringServer{}
	server.adminMu.Lock()
	defer server.adminMu.Unlock()
	done := make(chan error, 1)
	go func() {
		_, err := server.createDatabase(context.Background(), RingAdminRequest{Cmd: AdminCreateDatabase, Name: "scratch", Storage: AdminStorageMemory})
		done <- err
	}()
	select {
	case err := <-done:
		var coded *cluster.Error
		if !errors.As(err, &coded) || coded.Code != cluster.CodeInvalidArgument || coded.Message != memoryStorageRefusalFor("scratch") {
			t.Fatalf("refusal = %v, want INVALID_ARGUMENT with exactly %q", err, memoryStorageRefusalFor("scratch"))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a memory create waited for the admin lock; the early refusal is gone")
	}
}

// TestTheReplyStorageRefusalsNameTheFix: a rule 3 text names the thing and the fix.
func TestTheReplyStorageRefusalsNameTheFix(t *testing.T) {
	_, err := EncodeRingAdminReply(RingAdminReply{Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Buckets: []RingAdminListed{}, Storage: "ram"})
	const unknownWord = `a reply names storage "ram", and a database is on "disk" or in "memory" only: this server and client are different builds, so run the same build on both (upgrade the older one)`
	if err == nil || err.Error() != unknownWord {
		t.Errorf("an unknown storage word: err = %v, want exactly %q", err, unknownWord)
	}
	_, err = EncodeRingAdminReply(RingAdminReply{Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Buckets: []RingAdminListed{}, Storage: "disk", MemoryBytes: 5})
	const figures = "a describe_database reply carries memory_bytes and memory_cap_bytes only for a memory database, never a negative figure, and this one is disk with 5 and 0: send both as 0 for a disk database"
	if err == nil || err.Error() != figures {
		t.Errorf("memory figures on a disk database: err = %v, want exactly %q", err, figures)
	}
}

// TestACreateReplyIsTheCommittedRowOrARefusal: the create reply carries what the committed state
// holds for the name. A row with other copies or storage than the request is refused with the fix,
// never answered with the requested options.
// [BREAK: build the reply from the request again]
func TestACreateReplyIsTheCommittedRowOrARefusal(t *testing.T) {
	state := cluster.ClusterState{Databases: []cluster.Database{
		{Name: "app", ID: 4, RF: 1, Storage: cluster.StorageDisk},
		{Name: "cache", ID: 5, RF: 1, Storage: cluster.StorageMemory},
	}}
	two := int32(2)
	cases := []struct {
		name string
		req  RingAdminRequest
		rf   int32
	}{
		{"other storage", RingAdminRequest{Cmd: AdminCreateDatabase, Name: "app", Storage: AdminStorageMemory}, 1},
		{"other copies", RingAdminRequest{Cmd: AdminCreateDatabase, Name: "app", RF: &two}, 2},
		{"disk asked of a memory row", RingAdminRequest{Cmd: AdminCreateDatabase, Name: "cache"}, 1},
	}
	for _, c := range cases {
		reply, err := committedCreateReply(state, c.req, 4, c.rf)
		var coded *cluster.Error
		if !errors.As(err, &coded) || coded.Code != cluster.CodeConflict {
			t.Errorf("%s: reply %+v, err %v; want a CONFLICT refusal", c.name, reply, err)
		}
	}
	reply, err := committedCreateReply(state, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "cache", Storage: AdminStorageMemory}, 5, 1)
	if err != nil || reply.DatabaseID != 5 || reply.RF != 1 || reply.Storage != "memory" {
		t.Errorf("a create that matches its row: reply %+v, err %v", reply, err)
	}
	if _, err := committedCreateReply(state, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "gone"}, 9, 1); err == nil {
		t.Error("a create whose database is not in the committed state was answered")
	}
}

// TestACreateReplyDoesNotDenyACommittedCreate: a row with this create's own id was written by
// this create, so options that differ now were changed after it committed, and the text must not
// say nothing was changed. A row with another id was written by another create, and for this
// request nothing was changed.
// [BREAK: use the one text for both rows]
func TestACreateReplyDoesNotDenyACommittedCreate(t *testing.T) {
	state := cluster.ClusterState{Databases: []cluster.Database{{Name: "app", ID: 4, RF: 2, Storage: cluster.StorageDisk}}}
	req := RingAdminRequest{Cmd: AdminCreateDatabase, Name: "app"}
	_, err := committedCreateReply(state, req, 4, 1)
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeConflict {
		t.Fatalf("own id, other copies: err %v, want CONFLICT", err)
	}
	if strings.Contains(coded.Message, "Nothing was changed") || !strings.Contains(coded.Message, "was created as id 4") {
		t.Errorf("own id: %q must say the create committed and must not say nothing was changed", coded.Message)
	}
	_, err = committedCreateReply(state, req, 7, 1)
	if !errors.As(err, &coded) || !strings.Contains(coded.Message, "Nothing was changed") {
		t.Errorf("other id: %v, want the text that says nothing was changed", err)
	}
}
