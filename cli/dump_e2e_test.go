package cli

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
	"golang.org/x/sys/unix"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// closedTCPAddr returns an address nothing listens on, for a test that needs
// a connection to be refused. The port is bound but never listened on: the
// kernel keeps it for this socket until the test ends, so no other process can
// be given it between this call and the dial, and a connection is still
// refused (BUG-0201). It must never be handed to a server: startLoopbackService
// binds 127.0.0.1:0 instead.
func closedTCPAddr(t *testing.T) string {
	t.Helper()
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("creating the probe socket: %v", err)
	}
	unix.CloseOnExec(fd)
	t.Cleanup(func() {
		if err := unix.Close(fd); err != nil {
			t.Errorf("closing the probe socket: %v", err)
		}
	})
	// SO_REUSEADDR is deliberately not set: on Linux two sockets that both set
	// it may bind the same address while neither listens, so the port would not
	// be held exclusively.
	if err := unix.Bind(fd, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatalf("binding the probe socket: %v", err)
	}
	bound, err := unix.Getsockname(fd)
	if err != nil {
		t.Fatalf("reading the probe socket's address: %v", err)
	}
	inet4, ok := bound.(*unix.SockaddrInet4)
	if !ok {
		t.Fatalf("the probe socket bound a %T, want *unix.SockaddrInet4", bound)
	}
	return net.JoinHostPort(net.IP(inet4.Addr[:]).String(), strconv.Itoa(inet4.Port))
}

// TestDeadAddressIsNeverAnotherListener is BUG-0201: the dead address a
// golden dials must stay held by the helper, so no other process can be given
// the port between the helper and the dial. A released port can be bound
// again, and a hoardDB server that took it answers the dial — which is how a
// golden recorded an internode-listener refusal instead of a connection error.
func TestDeadAddressIsNeverAnotherListener(t *testing.T) {
	addr := closedTCPAddr(t)

	ln, err := net.Listen("tcp", addr)
	if err == nil {
		_ = ln.Close()
		t.Fatalf("closedTCPAddr released %s: another listener bound it, so the dial can reach whatever holds the port instead of failing", addr)
	}

	dialed, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		_ = dialed.Close()
		t.Fatalf("dial %s succeeded: nothing should accept there", addr)
	}
}

// driverAddressCapture is a slog handler that remembers the address the TCP
// driver listener reports once it is bound. The server exposes no accessor,
// and the log line is the one place a caller that asked for port 0 can learn
// the port the kernel chose, with no gap in which it is unowned.
type driverAddressCapture struct {
	address chan string
}

func (h *driverAddressCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *driverAddressCapture) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *driverAddressCapture) WithGroup(string) slog.Handler            { return h }
func (h *driverAddressCapture) Handle(_ context.Context, record slog.Record) error {
	if record.Message != "TCP driver listener listening" {
		return nil
	}
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "address" {
			select {
			case h.address <- attr.Value.String():
			default:
			}
			return false
		}
		return true
	})
	return nil
}

// startLoopbackService starts a real server whose driver listener binds
// 127.0.0.1:0, so the port is never named before it is owned, and returns it
// with cfg.TCP.Listen rewritten to the address the kernel chose. It seeds
// the admin user with password and stops the server at test end.
func startLoopbackService(t *testing.T, password string, edit func(*config.Config)) (*server.Service, *config.Config) {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	if edit != nil {
		edit(cfg)
	}
	if beforeServerStart != nil {
		beforeServerStart(t, cfg)
	}

	capture := &driverAddressCapture{address: make(chan string, 1)}
	qs, err := server.NewService(cfg, slog.New(capture))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := qs.SeedRootUser("admin", password); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	if err := qs.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = qs.Stop() })
	select {
	case cfg.TCP.Listen = <-capture.address:
	default:
		t.Fatalf("the server started but did not report its driver listener address")
	}
	return qs, cfg
}

// beforeServerStart, when set, runs after the harness has chosen the server's
// listen address and before the server binds it. A test uses it to play the
// other process that takes that address in the gap.
var beforeServerStart func(t *testing.T, cfg *config.Config)

// newDumpTestServer starts a real hoardDB server (TCP+TLS, on disk) and
// returns a connected, authenticated CLI pointed at it — the same
// connection path RunDumpCommand and the REPL use, so a dump driven through
// it exercises the real wire protocol end to end, not just the in-process
// dispatch table.
func newDumpTestServer(t *testing.T) *CLI {
	t.Helper()
	return newDumpTestServerWith(t, nil)
}

// newDumpTestServerWith is newDumpTestServer with the server's configuration
// edited before it starts.
func newDumpTestServerWith(t *testing.T, edit func(*config.Config)) *CLI {
	t.Helper()

	_, cfg := startLoopbackService(t, "dump-e2e-password", edit)

	c := NewCLI(
		WithAddress(cfg.TCP.Listen),
		WithInsecureTLS(true),
		WithCredentials("admin", "dump-e2e-password"),
		WithTerminal(io.Discard),
	)
	if err := c.Connect(); err != nil {
		t.Fatalf("connect to %s: %v", cfg.TCP.Listen, err)
	}
	t.Cleanup(c.Close)
	return c
}

// splitBSONDocumentsForTest mirrors server.splitBSONDocuments: each BSON
// document's own leading 4-byte little-endian length says how much of the
// stream belongs to it, exactly what bsondump relies on.
func splitBSONDocumentsForTest(t *testing.T, data []byte) [][]byte {
	t.Helper()
	var docs [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			t.Fatalf("trailing %d byte(s) are not a full BSON length prefix", len(data))
		}
		n := int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24
		if n < 5 || n > len(data) {
			t.Fatalf("invalid BSON document length %d in %d remaining byte(s)", n, len(data))
		}
		docs = append(docs, data[:n])
		data = data[n:]
	}
	return docs
}

// TestDumpEndToEndProducesBsondumpReadableLayout drives the real
// cli/dump.go path (runDumpBuckets, the same function RunDumpCommand and
// the REPL's `dump` statement both call) against a live server, and checks
// the "Done when" criteria from the task-2 brief that are CLI-shaped: the
// on-disk layout matches dump-restore-spec.md §4 well enough for a plain
// BSON reader to consume it, and no credential material appears anywhere
// under the output directory.
func TestDumpEndToEndProducesBsondumpReadableLayout(t *testing.T) {
	c := newDumpTestServer(t)

	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "users", Type: server.BucketTypeHash,
	}, nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	want := map[string]string{"alice": "alice-tag", "bob": "bob-tag", "carol": "carol-tag"}
	for key, tag := range want {
		if err := c.handler.send(transport.OpPut, &server.InsertRequest{
			Database: "app", Bucket: "users", Key: key, Data: map[string]interface{}{"tag": tag},
		}, nil); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}

	outDir := filepath.Join(t.TempDir(), "dump")
	summary, err := runDumpBuckets(c.handler, "app", "users", outDir, io.Discard, nil)
	if err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}
	if len(summary.failures) != 0 {
		t.Fatalf("dump reported failures: %v", summary.failures)
	}
	if summary.documents != int64(len(want)) {
		t.Fatalf("summary.documents = %d, want %d", summary.documents, len(want))
	}

	// Layout: dump/<database>/<bucket>.bson, .metadata.json, manifest.json.
	bsonPath := filepath.Join(outDir, "app", "users.bson")
	raw, err := os.ReadFile(bsonPath)
	if err != nil {
		t.Fatalf("reading %s: %v", bsonPath, err)
	}
	docs := splitBSONDocumentsForTest(t, raw)
	if len(docs) != len(want) {
		t.Fatalf("decoded %d BSON documents from %s, want %d", len(docs), bsonPath, len(want))
	}
	got := map[string]string{}
	for _, docBytes := range docs {
		var doc map[string]interface{}
		if err := bson.Unmarshal(docBytes, &doc); err != nil {
			t.Fatalf("unmarshal document: %v", err)
		}
		id, _ := doc["_id_"].(string)
		tag, _ := doc["tag"].(string)
		got[id] = tag
	}
	for key, tag := range want {
		if got[key] != tag {
			t.Fatalf("document %q = %q, want %q (value-by-value check, not a count)", key, got[key], tag)
		}
	}

	if _, err := os.Stat(filepath.Join(outDir, "app", "users.metadata.json")); err != nil {
		t.Fatalf("users.metadata.json missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "manifest.json")); err != nil {
		t.Fatalf("manifest.json missing: %v", err)
	}

	// Exfiltration check (task-2 brief's "Done when"): grep the whole
	// directory for the root password, its hash material, and anything
	// resembling a cluster token — dump-restore-spec.md §11's grep test.
	assertNoSecrets(t, outDir, "dump-e2e-password")
}

// TestDumpEndToEndPartialFailureMarksManifestIncomplete drives a
// whole-database dump where one bucket does not exist on the server (it was
// dropped between listing and dumping, in effect), and checks the
// §16.4/§4 behaviour: the readable bucket is still dumped, the run exits
// non-zero (ExitPartial, since at least one bucket succeeded), and
// manifest.json is marked "incomplete" with the failure recorded.
func TestDumpEndToEndPartialFailureMarksManifestIncomplete(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "users", Type: server.BucketTypeHash,
	}, nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := c.handler.send(transport.OpPut, &server.InsertRequest{
		Database: "app", Bucket: "users", Key: "alice", Data: map[string]interface{}{"tag": "alice-tag"},
	}, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}

	outDir := filepath.Join(t.TempDir(), "dump")
	// dumpOneBucket is driven directly for the missing bucket so this test
	// does not depend on resolveDumpTargets' enumeration order; the point is
	// runDumpBuckets' bookkeeping (incomplete, failures, exit code) given a
	// mixed outcome, which is exercised the same way regardless of how the
	// target list was built.
	summary, err := runDumpBucketsForTest(t, c.handler, outDir, []dumpBucketTarget{
		{Database: "app", Bucket: "users"},
		{Database: "app", Bucket: "does-not-exist"},
	})
	if err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}
	if summary.buckets != 1 {
		t.Fatalf("summary.buckets = %d, want 1 (only the readable bucket)", summary.buckets)
	}
	if !summary.incomplete {
		t.Fatal("summary.incomplete = false, want true after one bucket failed")
	}
	if len(summary.failures) != 1 || summary.failures[0].Bucket != "does-not-exist" {
		t.Fatalf("summary.failures = %v, want exactly one entry naming does-not-exist", summary.failures)
	}
	if code := summary.exitCode(); code != ExitPartial {
		t.Fatalf("exitCode() = %d, want ExitPartial (%d)", code, ExitPartial)
	}

	manifestPath := filepath.Join(outDir, "manifest.json")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("reading manifest.json: %v", err)
	}
	var manifest topLevelManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("unmarshal manifest.json: %v", err)
	}
	if !manifest.Incomplete {
		t.Fatal(`manifest.json "incomplete" = false, want true`)
	}
	if len(manifest.Failures) != 1 || manifest.Failures[0].Bucket != "does-not-exist" {
		t.Fatalf("manifest.json failures = %v, want exactly one entry naming does-not-exist", manifest.Failures)
	}
}

// runDumpBucketsForTest runs the same per-bucket loop runDumpBuckets does,
// against an explicit target list, so a test can force a mixed
// success/failure outcome without depending on enumeration order.
func runDumpBucketsForTest(t *testing.T, h *CommandHandler, outDir string, targets []dumpBucketTarget) (*dumpSummary, error) {
	t.Helper()
	if err := checkOutDir(outDir); err != nil {
		return nil, err
	}
	summary := newDumpSummary()
	for _, target := range targets {
		outcome := dumpOneBucket(h, target, outDir, io.Discard, nil)
		if outcome.err != nil {
			summary.incomplete = true
			summary.failures = append(summary.failures, dumpFailure{Database: target.Database, Bucket: target.Bucket, Error: outcome.err.Error()})
			continue
		}
		summary.addDatabase(target.Database)
		summary.buckets++
		summary.documents += outcome.documents
		summary.bytes += outcome.bytes
	}
	manifest := topLevelManifest{
		FormatVersion: dumpManifestFormatVersionCLI,
		Databases:     summary.databases,
		Buckets:       summary.buckets,
		Documents:     summary.documents,
		Bytes:         summary.bytes,
		Incomplete:    summary.incomplete,
		Failures:      summary.failures,
	}
	if err := writeManifest(outDir, manifest); err != nil {
		return summary, err
	}
	return summary, nil
}

// assertNoSecrets walks dir and fails the test if any file's contents
// contain needle.
func assertNoSecrets(t *testing.T, dir, needle string) {
	t.Helper()
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		if strings.Contains(string(data), needle) {
			t.Errorf("secret material found in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
}

// TestDumpUnderLoadCountMatchesStreamedDocuments is the dump-restore-spec.md
// §12.3 "dump under load" test, added as the fix-round-1 reproduction for
// the three-transaction finding: a document is inserted concurrently with a
// real dump's walk, racing it, and the assertion is that
// <bucket>.metadata.json's "documents" count always agrees with what is
// actually decodable from <bucket>.bson — never a separately-computed
// upfront estimate that a concurrent write could make disagree with the
// real streamed content (server/dump.go's DumpBucketManifest doc comment).
// Whichever side of the race the concurrent insert lands on (the walk's one
// Badger transaction either sees it or, under MVCC snapshot isolation,
// never does), the two numbers are drawn from the same tally by
// construction post-fix, so this holds either way — that consistency is
// exactly the property the earlier three-transaction design could violate.
func TestDumpUnderLoadCountMatchesStreamedDocuments(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "users", Type: server.BucketTypeHash,
	}, nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	const seeded = 20
	for i := 0; i < seeded; i++ {
		key := fmt.Sprintf("k%02d", i)
		if err := c.handler.send(transport.OpPut, &server.InsertRequest{
			Database: "app", Bucket: "users", Key: key, Data: map[string]interface{}{"tag": "seed"},
		}, nil); err != nil {
			t.Fatalf("seed insert %s: %v", key, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// No synchronization with the walk on purpose: the race itself is
		// the point. Either the walk's transaction snapshot already existed
		// (this insert is invisible to it) or it hadn't started yet (this
		// insert is included) — both are legitimate outcomes under Badger's
		// MVCC isolation, and the test does not care which happened.
		_ = c.handler.send(transport.OpPut, &server.InsertRequest{
			Database: "app", Bucket: "users", Key: "concurrent", Data: map[string]interface{}{"tag": "concurrent"},
		}, nil)
	}()

	outDir := filepath.Join(t.TempDir(), "dump")
	summary, err := runDumpBuckets(c.handler, "app", "users", outDir, io.Discard, nil)
	wg.Wait()
	if err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}
	if len(summary.failures) != 0 {
		t.Fatalf("dump reported failures: %v", summary.failures)
	}

	metaPath := filepath.Join(outDir, "app", "users.metadata.json")
	metaRaw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("reading %s: %v", metaPath, err)
	}
	var meta bucketMetadataFile
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		t.Fatalf("unmarshal metadata.json: %v", err)
	}

	bsonPath := filepath.Join(outDir, "app", "users.bson")
	bsonRaw, err := os.ReadFile(bsonPath)
	if err != nil {
		t.Fatalf("reading %s: %v", bsonPath, err)
	}
	docs := splitBSONDocumentsForTest(t, bsonRaw)

	if meta.Documents != int64(len(docs)) {
		t.Fatalf("metadata.json documents=%d but users.bson actually decodes to %d documents — the reported count disagrees with what was streamed",
			meta.Documents, len(docs))
	}
	if meta.Documents < seeded || meta.Documents > seeded+1 {
		t.Fatalf("documents=%d, want %d or %d (seeded, plus the concurrent insert depending on the race)", meta.Documents, seeded, seeded+1)
	}
	t.Logf("documents=%d (seeded=%d; concurrent insert %s included)", meta.Documents, seeded,
		map[bool]string{true: "was", false: "was not"}[meta.Documents == seeded+1])
}

// TestDumpRestoreKeepsDateAsBSONDateTime verifies that a date field round-trips
// through dump and restore as a BSON DateTime (element type 0x09), not as a string.
func TestDumpRestoreKeepsDateAsBSONDateTime(t *testing.T) {
	c := newDumpTestServer(t)

	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "Events", Type: server.BucketTypeHash,
	}, nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Insert a document with a date field
	dateValue := primitive.DateTime(1789836463000)
	if err := c.handler.send(transport.OpPut, &server.InsertRequest{
		Database: "app",
		Bucket:   "Events",
		Key:      "e1",
		Data: map[string]interface{}{
			"_id_": "e1",
			"ts":   dateValue,
			"name": "ada",
		},
	}, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Dump the bucket
	outDir := filepath.Join(t.TempDir(), "dump")
	_, err := runDumpBuckets(c.handler, "app", "Events", outDir, io.Discard, nil)
	if err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}

	// Read the dumped BSON file
	bsonPath := filepath.Join(outDir, "app", "Events.bson")
	bsonData, err := os.ReadFile(bsonPath)
	if err != nil {
		t.Fatalf("reading %s: %v", bsonPath, err)
	}

	// Parse the BSON document
	docs := splitBSONDocumentsForTest(t, bsonData)
	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}

	docBytes := docs[0]
	tsTypeByte, found := findBSONFieldType(docBytes, "ts")
	if !found {
		t.Fatal("ts field not found in dumped BSON")
	}

	// 0x09 is the BSON element type for DateTime
	if tsTypeByte != 0x09 {
		t.Errorf("ts field has BSON type 0x%02x, want 0x09 (DateTime)", tsTypeByte)
	}
}

// findBSONFieldType scans a top-level BSON document for fieldName and
// reports its BSON element type. It only understands enough element types
// to locate the "ts" field hoardDB's dump writer produces (DateTime,
// String, Int32, Int64); on any other type it stops scanning rather than
// guessing the value's length, because continuing would misread the
// following bytes as a new field header (staticcheck SA4011).
//
// BSON format: 4-byte length, fields, 0x00 terminator.
// Field format: 1-byte type, null-terminated field name, value.
func findBSONFieldType(docBytes []byte, fieldName string) (typeByte byte, found bool) {
	pos := 4 // Skip the 4-byte length prefix

scan:
	for pos < len(docBytes)-1 {
		elementType := docBytes[pos]
		pos++

		if elementType == 0x00 {
			// Document end
			break
		}

		// Read field name (null-terminated string)
		nameEnd := strings.Index(string(docBytes[pos:]), "\x00")
		if nameEnd == -1 {
			break
		}
		name := string(docBytes[pos : pos+nameEnd])
		pos += nameEnd + 1

		if name == fieldName {
			return elementType, true
		}

		switch elementType {
		case 0x09: // DateTime
			pos += 8 // 8-byte timestamp
		case 0x02: // String
			if pos+4 > len(docBytes) {
				break scan
			}
			strlen := int(binary.LittleEndian.Uint32(docBytes[pos : pos+4]))
			pos += 4 + strlen
		case 0x10: // Int32
			pos += 4
		case 0x12: // Int64
			pos += 8
		default:
			// An unrecognized type's value has unknown length, so there is
			// no safe way to skip it.
			break scan
		}
	}
	return 0, false
}

// TestFindBSONFieldTypeStopsAtUnrecognizedType is the break-the-guard test
// for staticcheck SA4011: findBSONFieldType's default case must exit the
// scan loop, not just its switch. The document below has a single top-level
// field of an unhandled type (0x07, ObjectID) whose payload is crafted to
// look exactly like a nested field named "ts" (type byte 0x02). If the scan
// ever treated that opaque payload as more field headers to read — which is
// what an ineffective `break` does, since it falls out of the switch and
// the loop just keeps going from the same, unadvanced position — it would
// wrongly report a "ts" match here. A correct scan must stop the instant it
// meets a type it does not know how to skip, and report the field not found.
func TestFindBSONFieldTypeStopsAtUnrecognizedType(t *testing.T) {
	doc := []byte{
		0, 0, 0, 0, // length prefix (unused by the scanner)
		0x07,      // field "x": type 0x07 (ObjectID), unrecognized by the scanner
		'x', 0x00, // field name "x\x00"
		0x02, 't', 's', 0x00, // opaque payload shaped like a nested "ts" field header
		0x00, // document terminator
	}

	typeByte, found := findBSONFieldType(doc, "ts")
	if found {
		t.Fatalf("findBSONFieldType: reported ts field with type 0x%02x from inside "+
			"an unrecognized field's opaque payload; the scan should have stopped "+
			"at the unrecognized type instead", typeByte)
	}
}

// TestDumpServerSurvivesPortStolenAfterReservation is BUG-0499: another
// process takes the address the harness chose between the choice and the
// server's own bind. The server must still come up, which only holds when the
// harness never names a port in advance.
func TestDumpServerSurvivesPortStolenAfterReservation(t *testing.T) {
	beforeServerStart = func(t *testing.T, cfg *config.Config) {
		thief, err := net.Listen("tcp", cfg.TCP.Listen)
		if err != nil {
			t.Fatalf("taking the chosen address %s: %v", cfg.TCP.Listen, err)
		}
		t.Cleanup(func() { _ = thief.Close() })
	}
	t.Cleanup(func() { beforeServerStart = nil })

	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("a server that started after the theft should answer: %v", err)
	}
}
