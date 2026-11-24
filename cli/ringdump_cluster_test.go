package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dump and restore on a cluster of three data members (BUG-0903). A member answers list,
// count and show buckets for the slots it owns, so a dump that reads through one member holds
// that member's share. These tests seed databases at one, two and three copies through every
// member, then require that a dump through any member holds every database, bucket, entity and
// store element exactly once, that a member it cannot read refuses the dump, and that restore's
// own checks see the whole cluster.

const dumpClusterPassword = "live-test-password"

// dumpClusterOptions shapes one test cluster: advertise publishes each member's client
// address (tcp.advertise); without it a loopback member publishes none, as a cluster formed on
// one machine does.
type dumpClusterOptions struct {
	advertise bool
}

// startDumpCluster starts three data members, node-a, node-b and node-c, joins b and c through
// a, and returns once every member reports the same epoch with all three active.
func startDumpCluster(t *testing.T, options dumpClusterOptions) []*ringJoinNode {
	t.Helper()
	nodes := []*ringJoinNode{{id: "node-a"}, {id: "node-b"}, {id: "node-c"}}
	dirs := map[string]string{}
	for _, node := range nodes {
		dirs[node.id] = t.TempDir()
		_, fingerprint, err := transport.MakeServerTLSConfig(filepath.Join(dirs[node.id], "keys"))
		if err != nil {
			t.Fatal(err)
		}
		node.fingerprint, node.listen = fingerprint, ringJoinPorts(t)
	}
	for _, node := range nodes {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		node.driver = listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		cfg := config.DefaultConfig()
		cfg.Storage.DataDir = dirs[node.id]
		cfg.Metrics.Enabled, cfg.Audit.Enabled = false, false
		cfg.Server.NodeID, cfg.Server.Listen, cfg.TCP.Listen = node.id, node.listen, node.driver
		if options.advertise {
			cfg.TCP.Advertise = node.driver
		}
		cfg.Cluster.Enabled = true
		// Every member pins every other member's key, as an operator forming a cluster does:
		// members replicate to and read declarations from each other, not only from node-a.
		for _, other := range nodes {
			if other.id != node.id {
				cfg.HashRing.SeedFingerprints = append(cfg.HashRing.SeedFingerprints, other.id+"="+other.fingerprint)
			}
		}
		if node.id != "node-a" {
			cfg.HashRing.SeedNodes = []string{"node-a@" + nodes[0].listen}
			if _, _, err := (server.CredentialKeys{}).InitClusterToken(cfg.Storage.DataDir); err != nil {
				t.Fatal(err)
			}
		}
		logTo := io.Discard
		if os.Getenv("DUMP_CLUSTER_LOG") != "" {
			logTo = os.Stderr
		}
		service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(logTo, &slog.HandlerOptions{Level: dumpClusterLogLevel()})).With("node", node.id))
		if err != nil {
			t.Fatalf("%s: %v", node.id, err)
		}
		if node.id == "node-a" {
			if err := service.BootstrapUsersFrom("root", dumpClusterPassword, true, "HOARDDB_ROOT_PASSWORD"); err != nil {
				t.Fatal(err)
			}
		}
		if err := service.Start(); err != nil {
			t.Fatalf("starting %s: %v", node.id, err)
		}
		t.Cleanup(func() { _ = service.Stop() })
		node.service = service
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rootA := ringUsersDial(t, nodes[0].driver, "root", dumpClusterPassword, 30*time.Second)
	for _, node := range nodes[1:] {
		if _, err := rootA.Join(ctx, node.id, node.listen, false); err != nil {
			t.Fatalf("join %s: %v", node.id, err)
		}
	}
	waitDumpClusterActive(t, nodes, 3)
	return nodes
}

// waitDumpClusterActive waits until every member's own topology names want active data members
// at one epoch, so a database created next is placed on all of them.
func waitDumpClusterActive(t *testing.T, nodes []*ringJoinNode, want int) {
	t.Helper()
	clients := make([]*ringclient.Client, len(nodes))
	for index, node := range nodes {
		clients[index] = ringUsersDial(t, node.driver, "root", dumpClusterPassword, 60*time.Second)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		settled, last := true, ""
		epochs := map[int64]bool{}
		for index, client := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			view, err := client.Topology(ctx, nil)
			cancel()
			if err != nil {
				settled, last = false, fmt.Sprintf("%s: %v", nodes[index].id, err)
				continue
			}
			active := 0
			for _, member := range view.Members {
				if member.State == "active" {
					active++
				}
			}
			epochs[view.Epoch] = true
			if active != want {
				settled, last = false, fmt.Sprintf("%s sees %+v", nodes[index].id, view.Members)
			}
		}
		if settled && len(epochs) == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster did not settle with %d active members at one epoch: %s (epochs %v)", want, last, epochs)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// dumpClusterExpect is what a seeded cluster holds, as the keys a dump's records carry: a
// database name, "db/bucket", "db/bucket/id", and the element count of "db/bucket/id/field".
type dumpClusterExpect struct {
	databases, buckets, entities map[string]bool
	elements                     map[string]int
}

// seedDumpCluster creates databases one, two and three (one, two and three copies), six buckets
// in each and 30 documents in every bucket, writing each bucket through a different member so
// no member saw every write. Every fifth document gets two queue elements.
func seedDumpCluster(t *testing.T, nodes []*ringJoinNode) dumpClusterExpect {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clients := make([]*ringclient.Client, len(nodes))
	for index, node := range nodes {
		clients[index] = ringUsersDial(t, node.driver, "root", dumpClusterPassword, 30*time.Second)
	}
	expect := dumpClusterExpect{databases: map[string]bool{}, buckets: map[string]bool{}, entities: map[string]bool{}, elements: map[string]int{}}
	type queued struct {
		handle *ringclient.Bucket
		id     string
	}
	var queues []queued
	for copies, database := range []string{"one", "two", "three"} {
		if _, err := clients[copies].CreateDatabase(ctx, database, int32(copies+1)); err != nil {
			t.Fatalf("creating database %s with %d copies: %v", database, copies+1, err)
		}
		expect.databases[database] = true
		for bucketIndex := range 6 {
			bucket := fmt.Sprintf("B%d", bucketIndex)
			writer := clients[bucketIndex%len(clients)]
			if err := retrySeedWrite(func() error {
				_, err := writer.CreateBucket(ctx, database, bucket, []ringclient.FieldDecl{
					{Name: "Name", Type: "string"}, {Name: "Log", Type: "fifoStore"},
				})
				return err
			}); err != nil {
				t.Fatalf("creating bucket %s.%s: %v", database, bucket, err)
			}
			expect.buckets[database+"/"+bucket] = true
			handle := writer.Bucket(database, bucket)
			for docIndex := range 30 {
				id := fmt.Sprintf("d%03d", docIndex)
				if err := retrySeedWrite(func() error {
					_, err := handle.Upsert(ctx, id, ringclient.Doc{"Name": database + "-" + bucket + "-" + id})
					return err
				}); err != nil {
					t.Fatalf("writing %s.%s %s: %v", database, bucket, id, err)
				}
				expect.entities[database+"/"+bucket+"/"+id] = true
				if docIndex%5 == 0 {
					queues = append(queues, queued{handle: handle, id: id})
					expect.elements[database+"/"+bucket+"/"+id+"/Log"] = 2
				}
			}
		}
	}
	// A push is not idempotent, so each one is sent only after a count shows it is still
	// missing: a push whose outcome was unknown is then never sent twice.
	for _, queue := range queues {
		if err := retrySeedWrite(func() error {
			count, err := queue.handle.StoreCount(ctx, queue.id, "Log")
			if err != nil {
				return err
			}
			for element := count; element < 2; element++ {
				if _, err := queue.handle.Push(ctx, queue.id, "Log", bson.D{{Key: "n", Value: int32(element)}}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("pushing to %s: %v", queue.id, err)
		}
	}
	waitDumpClusterServing(t, clients, expect)
	return expect
}

// waitDumpClusterServing reads one document of every bucket through every member, retrying
// the refusals a member answers while it has no valid lease, so the first dump does not meet
// a member that is still renewing. The owners of the buckets are all asked: a read through a
// member is answered by the bucket's owner.
func waitDumpClusterServing(t *testing.T, clients []*ringclient.Client, expect dumpClusterExpect) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for bucketPath := range expect.buckets {
		database, bucket, _ := strings.Cut(bucketPath, "/")
		for _, client := range clients {
			if err := retrySeedWrite(func() error {
				_, err := client.Bucket(database, bucket).Get(ctx, "d000")
				return err
			}); err != nil {
				t.Fatalf("reading %s through a member before the dump: %v", bucketPath, err)
			}
		}
	}
}

// retrySeedWrite retries an idempotent seeding step the cluster could not complete yet: right
// after a join a member can answer NO_QUORUM, DECL_UNAVAILABLE or UNAVAILABLE (a peer link in
// backoff) for a few seconds. A bucket created through one member can also answer NOT_FOUND
// ("database not found") through another that has not yet read the database's declaration.
// It backs off, because a retry at once meets the same backoff.
func retrySeedWrite(write func() error) error {
	deadline := time.Now().Add(2 * time.Minute)
	wait := 100 * time.Millisecond
	for {
		err := write()
		if err == nil {
			return nil
		}
		var refusal *ringclient.Error
		transient := errors.As(err, &refusal) && (refusal.Code == cluster.CodeNoQuorum || refusal.Code == cluster.CodeDeclUnavailable ||
			refusal.Code == cluster.CodeLeaseLapsed || refusal.Code == cluster.CodeUnavailable || refusal.Code == cluster.CodeNotFound)
		if !transient || time.Now().After(deadline) {
			return err
		}
		time.Sleep(wait)
		wait = min(2*wait, 2*time.Second)
	}
}

// dumpClusterContents counts what a dump file holds under the keys of dumpClusterExpect, so a
// record written twice shows as a count of two.
type dumpClusterContents struct {
	databases, buckets, entities map[string]int
	elements                     map[string]int
	// sequence is every db, bucket, doc and elems record in file order, encoded, so two files
	// can be compared record for record without their header and footer.
	sequence []string
}

func readDumpClusterContents(t *testing.T, path string) dumpClusterContents {
	t.Helper()
	contents := dumpClusterContents{databases: map[string]int{}, buckets: map[string]int{}, entities: map[string]int{}, elements: map[string]int{}}
	for _, record := range readDumpRecords(t, path) {
		switch record := record.(type) {
		case *dumpDB:
			contents.databases[record.Name]++
		case *dumpBucket:
			contents.buckets[record.DB+"/"+record.Name]++
		case *dumpDoc:
			contents.entities[record.DB+"/"+record.B+"/"+record.ID]++
		case *dumpElems:
			contents.elements[record.DB+"/"+record.B+"/"+record.ID+"/"+record.Field] += len(record.Els)
		default:
			continue
		}
		encoded, err := bson.Marshal(record)
		if err != nil {
			t.Fatalf("encoding a %s record of %s: %v", record.kindWord(), path, err)
		}
		contents.sequence = append(contents.sequence, string(encoded))
	}
	return contents
}

// requireExactlyOnce fails unless got holds every key of want once and nothing else.
func requireExactlyOnce(t *testing.T, label, what string, want map[string]bool, got map[string]int) {
	t.Helper()
	var missing, twice, extra []string
	for key := range want {
		switch got[key] {
		case 0:
			missing = append(missing, key)
		case 1:
		default:
			twice = append(twice, key)
		}
	}
	for key := range got {
		if !want[key] {
			extra = append(extra, key)
		}
	}
	if len(missing)+len(twice)+len(extra) == 0 {
		return
	}
	slices.Sort(missing)
	slices.Sort(twice)
	t.Errorf("%s holds %d of %d %s: %d missing (first %v), %d more than once (first %v), %d not seeded (first %v)",
		label, len(want)-len(missing), len(want), what, len(missing), firstFew(missing), len(twice), firstFew(twice), len(extra), firstFew(extra))
}

func firstFew(keys []string) []string {
	return keys[:min(len(keys), 5)]
}

// requireDumpHoldsCluster checks one file against what was seeded.
func requireDumpHoldsCluster(t *testing.T, label string, expect dumpClusterExpect, contents dumpClusterContents) {
	t.Helper()
	requireExactlyOnce(t, label, "databases", expect.databases, contents.databases)
	requireExactlyOnce(t, label, "buckets", expect.buckets, contents.buckets)
	requireExactlyOnce(t, label, "entities", expect.entities, contents.entities)
	if !reflect.DeepEqual(contents.elements, expect.elements) {
		missing := 0
		for key, count := range expect.elements {
			if contents.elements[key] != count {
				missing++
			}
		}
		t.Errorf("%s holds the queue elements of %d of %d seeded fields, and %d fields in all", label, len(expect.elements)-missing, len(expect.elements), len(contents.elements))
	}
}

// dumpClusterLogin is the connection flags of root on addr, trusting it with -insecure.
func dumpClusterLogin(addr string) []string {
	return []string{"-address", addr, "-user", "root", "-password=" + dumpClusterPassword, "-insecure"}
}

func dumpThroughMember(t *testing.T, addr string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	return runRingDumpForTest(t, append(append([]string{}, args...), dumpClusterLogin(addr)...)...)
}

func restoreThroughMember(t *testing.T, addr string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	return runRingRestoreForTest(t, append(append([]string{}, args...), dumpClusterLogin(addr)...)...)
}

// TestDumpOfAThreeMemberClusterHoldsEverythingOnce is the defect itself: on v1.1.0 a dump
// through each member held that member's share and exited 0. Every file must hold every
// database, bucket, entity and element exactly once, and the three files must be the same
// record for record, because the order is the members' id order whichever member was dialled.
func TestDumpOfAThreeMemberClusterHoldsEverythingOnce(t *testing.T) {
	nodes := startDumpCluster(t, dumpClusterOptions{advertise: true})
	expect := seedDumpCluster(t, nodes)
	outDir := t.TempDir()
	var first dumpClusterContents
	for index, node := range nodes {
		outPath := filepath.Join(outDir, node.id+".hoarddump")
		code, stdout, stderr := dumpThroughMember(t, node.driver, "--out", outPath)
		if code != ExitOK {
			t.Fatalf("dump through %s: exit %d\nstdout: %s\nstderr: %s", node.id, code, stdout, stderr)
		}
		contents := readDumpClusterContents(t, outPath)
		requireDumpHoldsCluster(t, "the dump through "+node.id, expect, contents)
		if index == 0 {
			first = contents
			continue
		}
		if !slices.Equal(first.sequence, contents.sequence) {
			t.Errorf("the dump through %s differs from the dump through %s: %d and %d records", node.id, nodes[0].id, len(contents.sequence), len(first.sequence))
		}
	}

	// A dump scoped to one database or one bucket reads every member for its scope too.
	scoped := filepath.Join(outDir, "one-B2.hoarddump")
	code, stdout, stderr := dumpThroughMember(t, nodes[1].driver, "--db", "one", "--bucket", "B2", "--out", scoped)
	if code != ExitOK {
		t.Fatalf("dump --db one --bucket B2 through node-b: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	contents := readDumpClusterContents(t, scoped)
	wantEntities := map[string]bool{}
	for key := range expect.entities {
		if strings.HasPrefix(key, "one/B2/") {
			wantEntities[key] = true
		}
	}
	requireExactlyOnce(t, "the dump of one.B2", "entities", wantEntities, contents.entities)
}

// TestDumpTrustsEachMemberOnlyByItsOwnFingerprint: an operator who pinned the entry member gave
// no fingerprint for the others. The dump must not fall back to unverified TLS for them: it
// refuses, naming the member and the flag, and it succeeds once every member is pinned.
func TestDumpTrustsEachMemberOnlyByItsOwnFingerprint(t *testing.T) {
	nodes := startDumpCluster(t, dumpClusterOptions{advertise: true})
	expect := seedDumpCluster(t, nodes)
	outDir := t.TempDir()
	login := []string{"-address", nodes[0].driver, "-user", "root", "-password=" + dumpClusterPassword}

	refused := filepath.Join(outDir, "pinned-one.hoarddump")
	code, stdout, stderr := runRingDumpForTest(t, append([]string{"--out", refused, "-fingerprint", nodes[0].fingerprint}, login...)...)
	if code == ExitOK {
		t.Fatalf("a dump that pinned node-a alone exited 0; it cannot verify node-b and node-c\nstderr: %s", stderr)
	}
	if !strings.Contains(stderr, "node-b") || !strings.Contains(stderr, "-fingerprint") {
		t.Errorf("the refusal does not name the member it could not verify and the flag that fixes it:\nstdout: %s\nstderr: %s", stdout, stderr)
	}
	requireNoDumpFile(t, refused)

	pinned := filepath.Join(outDir, "pinned-all.hoarddump")
	args := []string{"--out", pinned}
	for _, node := range nodes {
		args = append(args, "-fingerprint", node.fingerprint)
	}
	code, stdout, stderr = runRingDumpForTest(t, append(args, login...)...)
	if code != ExitOK {
		t.Fatalf("a dump that pinned all three members: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	requireDumpHoldsCluster(t, "the pinned dump", expect, readDumpClusterContents(t, pinned))
}

// TestDumpRefusesWhenAMemberIsDown: with node-c stopped, a dump of everything cannot read the
// one-copy database's slots on node-c through anyone, so it refuses, names node-c and writes no
// file, before and after the cluster marks node-c down. Once node-c is down, a dump of the
// three-copy database reads node-c's slots from their new owners and holds every entity.
func TestDumpRefusesWhenAMemberIsDown(t *testing.T) {
	nodes := startDumpCluster(t, dumpClusterOptions{advertise: true})
	expect := seedDumpCluster(t, nodes)
	outDir := t.TempDir()
	if err := nodes[2].service.Stop(); err != nil {
		t.Fatalf("stopping node-c: %v", err)
	}

	whole := filepath.Join(outDir, "whole-unreachable.hoarddump")
	code, stdout, stderr := dumpThroughMember(t, nodes[0].driver, "--out", whole)
	if code == ExitOK || !strings.Contains(stderr, "node-c") {
		t.Errorf("a dump with node-c stopped: exit %d, want a refusal that names node-c\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	requireNoDumpFile(t, whole)

	waitMemberDown(t, nodes[0], "node-c")
	downWhole := filepath.Join(outDir, "whole-down.hoarddump")
	code, stdout, stderr = dumpThroughMember(t, nodes[0].driver, "--out", downWhole)
	if code == ExitOK || !strings.Contains(stderr, "node-c") || !strings.Contains(stderr, `"one"`) {
		t.Errorf("a dump with node-c down: exit %d, want a refusal that names node-c and database \"one\"\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	requireNoDumpFile(t, downWhole)

	// The new owners of node-c's slots refuse a scan (exit 8, nothing written) until their
	// handoff closes, then answer for those slots.
	three := filepath.Join(outDir, "three.hoarddump")
	deadline := time.Now().Add(2 * time.Minute)
	for {
		code, stdout, stderr = dumpThroughMember(t, nodes[1].driver, "--db", "three", "--out", three)
		if code == ExitOK {
			break
		}
		requireNoDumpFile(t, three)
		if code != ExitUnavailable || time.Now().After(deadline) {
			t.Fatalf("dump --db three with node-c down: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
		}
		time.Sleep(time.Second)
	}
	contents := readDumpClusterContents(t, three)
	wantEntities := map[string]bool{}
	for key := range expect.entities {
		if strings.HasPrefix(key, "three/") {
			wantEntities[key] = true
		}
	}
	requireExactlyOnce(t, "the dump of three with node-c down", "entities", wantEntities, contents.entities)
}

// waitMemberDown waits until the member at via reports id down in its topology.
func waitMemberDown(t *testing.T, via *ringJoinNode, id string) {
	t.Helper()
	client := ringUsersDial(t, via.driver, "root", dumpClusterPassword, 30*time.Second)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		view, err := client.Topology(ctx, nil)
		cancel()
		if err == nil {
			for _, member := range view.Members {
				if member.ID == id && member.State == "down" {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not marked down within two minutes (last view %+v, %v)", id, view, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestDumpAndRestoreOnAClusterWithoutPublishedAddresses: members whose client listener is on a
// loopback address publish no client address. The dump refuses and names --member; given the
// addresses it dumps the cluster whole. The file restores into a three-member cluster and into
// one node, and both read back equal; restore's emptiness check sees buckets whose declaration
// another member owns.
func TestDumpAndRestoreOnAClusterWithoutPublishedAddresses(t *testing.T) {
	source := startDumpCluster(t, dumpClusterOptions{})
	expect := seedDumpCluster(t, source)
	outDir := t.TempDir()

	refused := filepath.Join(outDir, "no-address.hoarddump")
	code, stdout, stderr := dumpThroughMember(t, source[0].driver, "--out", refused)
	if code == ExitOK || !strings.Contains(stderr, "--member") {
		t.Errorf("a dump of members that publish no client address: exit %d, want a refusal that names --member\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	requireNoDumpFile(t, refused)

	file := filepath.Join(outDir, "source.hoarddump")
	code, stdout, stderr = dumpThroughMember(t, source[0].driver, append([]string{"--out", file}, memberFlags(source)...)...)
	if code != ExitOK {
		t.Fatalf("dump with --member for every member: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	sourceContents := readDumpClusterContents(t, file)
	requireDumpHoldsCluster(t, "the dump with --member", expect, sourceContents)

	target := startDumpCluster(t, dumpClusterOptions{})
	code, stdout, stderr = restoreThroughMember(t, target[1].driver, append([]string{file}, memberFlags(target)...)...)
	if code != ExitOK {
		t.Fatalf("restore into three members through node-b: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	readBack := filepath.Join(outDir, "target.hoarddump")
	code, stdout, stderr = dumpThroughMember(t, target[2].driver, append([]string{"--out", readBack}, memberFlags(target)...)...)
	if code != ExitOK {
		t.Fatalf("dump of the restored cluster: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	targetContents := readDumpClusterContents(t, readBack)
	requireDumpHoldsCluster(t, "the restored three-member cluster", expect, targetContents)
	if !slices.Equal(sourceContents.sequence, targetContents.sequence) {
		t.Errorf("the restored cluster dumps %d records and the source %d, or in another order", len(targetContents.sequence), len(sourceContents.sequence))
	}

	// Restore again through every member: each one must see that the databases hold buckets.
	for _, node := range target {
		code, stdout, stderr = restoreThroughMember(t, node.driver, append([]string{file}, memberFlags(target)...)...)
		if code != ExitRefused || !strings.Contains(stderr, "already holds") {
			t.Errorf("a second restore through %s: exit %d, want %d and the already-holds refusal\nstdout: %s\nstderr: %s", node.id, code, ExitRefused, stdout, stderr)
		}
	}

	// One node cannot hold "three" or "two": the restore is refused before it creates "one",
	// which comes first in the file, so the run leaves nothing behind (review F9).
	addr, singleClient := newRingTestHost(t)
	code, stdout, stderr = restoreOnHost(t, addr, file)
	if code != ExitRefused || !strings.Contains(stderr, `Database "three" in the dump has 3 copies, and this cluster cannot hold them: it has 1 voter and 1 data member,`) {
		t.Errorf("restore of databases at two and three copies into one node: exit %d, want %d and the copies refusal\nstdout: %s\nstderr: %s", code, ExitRefused, stdout, stderr)
	}
	if listed, err := singleClient.ShowDatabases(context.Background(), ringclient.PageRequest{}); err != nil || len(listed.Databases) != 0 {
		t.Errorf("the refused restore left databases behind: %+v, %v", listed.Databases, err)
	}
	code, stdout, stderr = restoreOnHost(t, addr, file, "--db", "one")
	if code != ExitOK {
		t.Fatalf("restore --db one into a single node: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	single := filepath.Join(outDir, "single.hoarddump")
	code, stdout, stderr = dumpOnHost(t, addr, "--out", single)
	if code != ExitOK {
		t.Fatalf("dump of the single node: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	singleContents := readDumpClusterContents(t, single)
	wantEntities := map[string]bool{}
	for key := range expect.entities {
		if strings.HasPrefix(key, "one/") {
			wantEntities[key] = true
		}
	}
	requireExactlyOnce(t, "the single node after restore --db one", "entities", wantEntities, singleContents.entities)
}

// TestRestoreRefusesANonEmptyDatabaseThroughAMemberThatOwnsNoneOfItsBuckets: a database whose
// one bucket is declared on another member looks empty to a member that lists only its own
// declarations, and v1.1.0's restore merged into it. It must be refused through every member.
func TestRestoreRefusesANonEmptyDatabaseThroughAMemberThatOwnsNoneOfItsBuckets(t *testing.T) {
	nodes := startDumpCluster(t, dumpClusterOptions{advertise: true})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	root := ringUsersDial(t, nodes[0].driver, "root", dumpClusterPassword, 30*time.Second)
	if _, err := root.CreateDatabase(ctx, "solo", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := root.CreateBucket(ctx, "solo", "Only", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	// The file comes from a single node that holds "solo" with another bucket, "Other".
	hostAddr, host := newRingTestHost(t)
	if _, err := host.CreateDatabase(ctx, "solo", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateBucket(ctx, "solo", "Other", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "solo-other.hoarddump")
	code, stdout, stderr := dumpOnHost(t, hostAddr, "--out", file)
	if code != ExitOK {
		t.Fatalf("dump of the single node's solo: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	blind := ""
	for _, node := range nodes {
		client := ringUsersDial(t, node.driver, "root", dumpClusterPassword, 30*time.Second)
		page, err := client.ShowBuckets(ctx, "solo", ringclient.PageRequest{})
		if err != nil {
			t.Fatalf("show buckets in solo through %s: %v", node.id, err)
		}
		if len(page.Buckets) == 0 {
			blind = node.driver
			break
		}
	}
	if blind == "" {
		t.Fatal("every member lists solo.Only; the test needs one that does not")
	}
	code, stdout, stderr = restoreThroughMember(t, blind, file)
	if code != ExitRefused || !strings.Contains(stderr, `already holds 1 bucket (Only)`) {
		t.Errorf("restore into solo, which holds Only, through a member that does not list it: exit %d, want %d and the already-holds refusal\nstdout: %s\nstderr: %s", code, ExitRefused, stdout, stderr)
	}
}

// memberFlags names every member's client address with --member, for a cluster whose members
// publish none.
func memberFlags(nodes []*ringJoinNode) []string {
	var flags []string
	for _, node := range nodes {
		flags = append(flags, "--member", node.id+"="+node.driver)
	}
	return flags
}

// dumpClusterLogLevel is the level DUMP_CLUSTER_LOG names (debug, info), warn by default.
func dumpClusterLogLevel() slog.Level {
	switch os.Getenv("DUMP_CLUSTER_LOG") {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	}
	return slog.LevelWarn
}
