package server

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dump format 3: the "dead" section and DEAD_RESTORE as the restore's own write
// (claim/ack build step 17).

// dumpedBucket is one bucket's dump as the CLI would hold it: each section's
// records, concatenated as they arrived, and the server's closing tally.
type dumpedBucket struct {
	manifest *DumpBucketManifest
	sections map[string][]byte
	done     *DumpDoneResponse
}

func digestOf(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// dumpSections drives start, chunk and done on s the way the CLI does, keeping
// each section's bytes apart and checking each against the server's digest.
func dumpSections(t *testing.T, s *Server, bucket string, request DumpStartRequest) dumpedBucket {
	t.Helper()
	request.Database, request.Bucket = "app", bucket
	started, err := s.startDump(rootContext(), request)
	if err != nil {
		t.Fatalf("starting the dump of %s: %v", bucket, err)
	}
	sections := map[string][]byte{}
	for {
		chunk, err := s.dumpChunk(rootContext(), DumpChunkRequest{Database: "app", Bucket: bucket})
		if err != nil {
			t.Fatalf("pulling a chunk of %s: %v", bucket, err)
		}
		sections[chunk.Section] = append(sections[chunk.Section], chunk.Data...)
		if chunk.EOF {
			break
		}
	}
	// A documents digest is always sent, as the CLI sends it; the others only
	// when their section exists.
	sum := sha256.Sum256(sections[""])
	done, err := s.dumpDone(rootContext(), DumpDoneRequest{
		Database: "app", Bucket: bucket, Digest: "sha256:" + hex.EncodeToString(sum[:]),
		StoresDigest: digestOf(sections[dumpSectionStores]), DeadDigest: digestOf(sections[dumpSectionDead]),
	})
	if err != nil {
		t.Fatalf("finishing the dump of %s: %v", bucket, err)
	}
	return dumpedBucket{manifest: started.Manifest, sections: sections, done: done}
}

func recordsOf(t *testing.T, data []byte) []bson.Raw {
	t.Helper()
	parts, err := splitBSONDocuments(data)
	if err != nil {
		t.Fatalf("splitting a section: %v", err)
	}
	records := make([]bson.Raw, len(parts))
	for i, part := range parts {
		records[i] = part
	}
	return records
}

// documentNumbers are the n of each dumped document, in dump order.
func (d dumpedBucket) documentNumbers(t *testing.T) []int {
	t.Helper()
	var numbers []int
	for _, document := range recordsOf(t, d.sections[""]) {
		if value, ok := document.Lookup("value").DocumentOK(); ok {
			document = value
		}
		n, ok := document.Lookup("n").AsInt64OK()
		if !ok {
			t.Fatalf("dumped document %v has no integer n", document)
		}
		numbers = append(numbers, int(n))
	}
	return numbers
}

// restoreSections restores d into dest as the CLI would: documents, then the
// dead section, then done with the counts d's dump recorded. Anything amiss in
// the chunks fails the test; a count that disagrees comes back in the answer.
func restoreSections(t *testing.T, dest *Server, d dumpedBucket) *RestoreDoneResponse {
	t.Helper()
	started, err := dest.startRestore(rootContext(), RestoreStartRequest{
		Database: "app", Bucket: d.manifest.Bucket, FormatVersion: d.manifest.FormatVersion, StoreType: d.manifest.StoreType,
		Bytes: 1, Dead: d.done.Dead,
	})
	if err != nil {
		t.Fatalf("starting the restore of %s: %v", d.manifest.Bucket, err)
	}
	for _, section := range []string{"", dumpSectionDead} {
		if len(d.sections[section]) == 0 {
			continue
		}
		if _, err := dest.restoreChunk(rootContext(), RestoreChunkRequest{
			Database: "app", Bucket: d.manifest.Bucket, SessionID: started.SessionID, Data: d.sections[section], Section: section,
		}); err != nil {
			t.Fatalf("restoring the %q section of %s: %v", section, d.manifest.Bucket, err)
		}
	}
	done, err := dest.restoreDone(rootContext(), RestoreDoneRequest{
		Database: "app", Bucket: d.manifest.Bucket, SessionID: started.SessionID, Documents: d.done.Documents, Dead: d.done.Dead,
	})
	if err != nil {
		t.Fatalf("finishing the restore of %s: %v", d.manifest.Bucket, err)
	}
	return done
}

func deadListOf(t *testing.T, s *Server, bucket string) []DeadItem {
	t.Helper()
	answer, err := s.DeadList(DeadListRequest{Database: "app", Bucket: bucket, Limit: 1000})
	if err != nil {
		t.Fatalf("listing the dead entries of %s: %v", bucket, err)
	}
	return answer.Items
}

// TestDumpIncludesPendingAsEntriesInPopOrder: a bucket with claimed entries
// dumps them as ordinary entries, in the order they would pop, and the count
// matches. A dump that walked 0x04 only would drop them with its count intact.
func TestDumpIncludesPendingAsEntriesInPopOrder(t *testing.T) {
	for _, bucketType := range leaseBucketTypes {
		t.Run(string(bucketType), func(t *testing.T) {
			s, _ := leaseServer(t)
			bucket := string(bucketType)
			before := dumpSections(t, s, bucket, DumpStartRequest{AcceptDead: true})
			if got := before.documentNumbers(t); len(got) != 5 || before.done.Documents != 5 {
				t.Fatalf("an idle bucket dumped %v with a count of %d, want 5 entries", got, before.done.Documents)
			}
			claimOf(t, s, bucket, 2, 60_000)
			after := dumpSections(t, s, bucket, DumpStartRequest{AcceptDead: true})
			if got, want := after.documentNumbers(t), before.documentNumbers(t); !slices.Equal(got, want) || after.done.Documents != 5 {
				t.Fatalf("with two claimed the dump is %v (count %d), want %v (count 5): claimed entries are entries, in pop order", got, after.done.Documents, want)
			}
		})
	}
}

// TestDumpAndRestoreKeepDeadEntriesDead: dead entries land in the dead
// section with their own digest and count, come back as dead entries with
// their deliveries and reason, and are never documents of the restored bucket.
func TestDumpAndRestoreKeepDeadEntriesDead(t *testing.T) {
	for _, bucketType := range leaseBucketTypes {
		t.Run(string(bucketType), func(t *testing.T) {
			source, _ := leaseServer(t)
			bucket := string(bucketType)
			restoredDead(t, source, bucket, bucketType, 3, "")
			claimOf(t, source, bucket, 1, 60_000)
			dumped := dumpSections(t, source, bucket, DumpStartRequest{AcceptDead: true})
			if dumped.manifest.FormatVersion != 3 || dumped.done.Dead != 3 || dumped.done.DeadDigest != digestOf(dumped.sections[dumpSectionDead]) ||
				len(recordsOf(t, dumped.sections[dumpSectionDead])) != 3 || dumped.done.Documents != 5 {
				t.Fatalf("the dump is format %d with %d dead (digest %q) and %d documents; want format 3, 3 dead in the dead section and 5 documents",
					dumped.manifest.FormatVersion, dumped.done.Dead, dumped.done.DeadDigest, dumped.done.Documents)
			}

			dest := newTestServer(t)
			answer := restoreSections(t, dest, dumped)
			if !answer.Success || answer.DeadApplied != 3 || answer.Applied != 5 {
				t.Fatalf("the restore answered %+v, want success with 5 documents and 3 dead entries", answer)
			}
			want, got := deadListOf(t, source, bucket), deadListOf(t, dest, bucket)
			if len(got) != 3 {
				t.Fatalf("the restored bucket lists %d dead entries, want 3", len(got))
			}
			for i := range want {
				if !slices.Equal(want[i].Data, got[i].Data) || want[i].Deliveries != got[i].Deliveries ||
					want[i].Reason != got[i].Reason || !want[i].DeadAt.Equal(got[i].DeadAt) {
					t.Errorf("dead entry %d came back as %+v, want %+v (the id is new)", i, got[i], want[i])
				}
			}
			again := dumpSections(t, dest, bucket, DumpStartRequest{AcceptDead: true})
			if !slices.Equal(again.documentNumbers(t), dumped.documentNumbers(t)) || again.done.Documents != 5 {
				t.Fatalf("the restored bucket holds %v live entries, want the source's %v: a dead entry must not come back alive",
					again.documentNumbers(t), dumped.documentNumbers(t))
			}
		})
	}
}

// TestDumpOfAHeapTwinRoundTripsAsBothEntries (A-8): a claimed heap element and
// a live one with the same id and priority. The live one is the document, the
// claimed one a collided dead entry, and the restore keeps both.
func TestDumpOfAHeapTwinRoundTripsAsBothEntries(t *testing.T) {
	source := newTestServer(t)
	withLeaseClock(source)
	createTestBucket(t, source, "app", "twin", BucketTypeHeap)
	for n := int32(1); n <= 2; n++ {
		if resp, err := source.Push(PushRequest{Database: "app", Bucket: "twin", Priority: 10, Data: map[string]interface{}{"_id_": "a", "n": n}}); err != nil || !resp.Success {
			t.Fatalf("pushing twin %d: %+v, %v", n, resp, err)
		}
		if n == 1 {
			claimOf(t, source, "twin", 1, 60_000)
		}
	}
	dumped := dumpSections(t, source, "twin", DumpStartRequest{AcceptDead: true})
	if got := dumped.documentNumbers(t); !slices.Equal(got, []int{2}) || dumped.done.Dead != 1 {
		t.Fatalf("the dump holds documents %v and %d dead; want the live twin 2 as the document and the claimed one as the dead entry", got, dumped.done.Dead)
	}
	dest := newTestServer(t)
	if answer := restoreSections(t, dest, dumped); !answer.Success || answer.Applied != 1 || answer.DeadApplied != 1 {
		t.Fatalf("the restore answered %+v, want 1 document and 1 dead entry", answer)
	}
	dead := deadListOf(t, dest, "twin")
	if len(dead) != 1 || dead[0].Reason != store.DeadReasonCollided {
		t.Fatalf("the restored dead list is %+v, want the claimed twin, collided", dead)
	}
	if value, ok := dead[0].Data.Lookup("value").DocumentOK(); !ok || value.Lookup("n").Int32() != 1 {
		t.Fatalf("the dead twin is %v, want entry n=1", dead[0].Data)
	}
}

// TestRestoreRefusesADeadCountMismatch: the count metadata.json recorded is
// compared with the entries applied, whether it is higher or zero.
func TestRestoreRefusesADeadCountMismatch(t *testing.T) {
	source, _ := leaseServer(t)
	restoredDead(t, source, "fifo", BucketTypeFIFO, 3, "")
	dumped := dumpSections(t, source, "fifo", DumpStartRequest{AcceptDead: true})
	records := recordsOf(t, dumped.sections[dumpSectionDead])

	rows := []struct {
		name     string
		recorded int64
		streamed []bson.Raw
		want     string
	}{
		{"fewer streamed than recorded", 3, records[:2], "recorded 3 dead entries, but restore applied 2"},
		{"some streamed with none recorded", 0, records[:1], "recorded 0 dead entries, but restore applied 1"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			dest := newTestServer(t)
			started, err := dest.startRestore(rootContext(), RestoreStartRequest{Database: "app", Bucket: "fifo", FormatVersion: 3, StoreType: BucketTypeFIFO, Bytes: 1, Dead: row.recorded})
			if err != nil {
				t.Fatalf("starting: %v", err)
			}
			var chunk []byte
			for _, record := range row.streamed {
				chunk = append(chunk, record...)
			}
			if _, err := dest.restoreChunk(rootContext(), RestoreChunkRequest{Database: "app", Bucket: "fifo", SessionID: started.SessionID, Data: chunk, Section: dumpSectionDead}); err != nil {
				t.Fatalf("restoring the dead chunk: %v", err)
			}
			done, err := dest.restoreDone(rootContext(), RestoreDoneRequest{Database: "app", Bucket: "fifo", SessionID: started.SessionID, Dead: row.recorded})
			if err != nil || done.Success || !strings.Contains(done.Message, row.want) {
				t.Fatalf("the restore answered %+v, %v; want a failure saying %q", done, err, row.want)
			}
		})
	}
}

// TestRestoreResetsDeliveryCounts: a pending entry comes back as an ordinary
// entry. Claimed twice at the source, it delivers with deliveries == 1 here.
func TestRestoreResetsDeliveryCounts(t *testing.T) {
	for _, bucketType := range leaseBucketTypes {
		t.Run(string(bucketType), func(t *testing.T) {
			source, clock := leaseServer(t)
			bucket := string(bucketType)
			claimOf(t, source, bucket, 1, 1000)
			clock.advance(5 * time.Second)
			if again := claimOf(t, source, bucket, 1, 60_000); !slices.Equal(again.Deliveries, []int32{2}) {
				t.Fatalf("setup: the second claim of the returned entry reports deliveries %v, want [2]", again.Deliveries)
			}
			dest := newTestServer(t)
			if answer := restoreSections(t, dest, dumpSections(t, source, bucket, DumpStartRequest{AcceptDead: true})); !answer.Success {
				t.Fatalf("the restore answered %+v", answer)
			}
			if claimed := claimOf(t, dest, bucket, 1, 60_000); len(claimed.Deliveries) != 0 {
				t.Fatalf("the restored entry delivers with deliveries %v, want absent (every value 1)", claimed.Deliveries)
			}
		})
	}
}

// TestRestoreRefusesASectionItDoesNotKnow: an older server pushed every section
// that was not "stores" as documents, which is how a dead record came back
// alive. A section this server does not name is refused and writes nothing.
func TestRestoreRefusesASectionItDoesNotKnow(t *testing.T) {
	source, _ := leaseServer(t)
	restoredDead(t, source, "fifo", BucketTypeFIFO, 1, "")
	dumped := dumpSections(t, source, "fifo", DumpStartRequest{AcceptDead: true})
	dest := newTestServer(t)
	started, err := dest.startRestore(rootContext(), RestoreStartRequest{Database: "app", Bucket: "fifo", FormatVersion: 3, StoreType: BucketTypeFIFO, Bytes: 1})
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	_, err = dest.restoreChunk(rootContext(), RestoreChunkRequest{Database: "app", Bucket: "fifo", SessionID: started.SessionID, Data: dumped.sections[dumpSectionDead], Section: "deads"})
	if err == nil || !strings.Contains(err.Error(), `section "deads"`) {
		t.Fatalf("a chunk of an unknown section = %v; want a refusal naming the section", err)
	}
	done, err := dest.restoreDone(rootContext(), RestoreDoneRequest{Database: "app", Bucket: "fifo", SessionID: started.SessionID})
	if err != nil || !done.Success || done.Applied != 0 || done.DeadApplied != 0 {
		t.Fatalf("after the refused chunk the restore reports %+v, %v; want nothing applied", done, err)
	}
	if live := dumpSections(t, dest, "fifo", DumpStartRequest{AcceptDead: true}); live.done.Documents != 0 {
		t.Fatalf("the refused dead record became %d live documents", live.done.Documents)
	}
}

// TestDumpFormat2StillRestores: this server writes format 3 and restores 2 and
// 3 and nothing else; a restorer that knows only 2 refuses what this one writes.
func TestDumpFormat2StillRestores(t *testing.T) {
	older := func(version int) bool { return version == 2 }
	if older(dumpManifestFormatVersion) {
		t.Fatalf("this server writes format %d, which a restorer that knows only 2 accepts: it would push the dead section as documents", dumpManifestFormatVersion)
	}
	dest := newTestServer(t)
	for version, accepted := range map[int]bool{0: false, 1: false, 2: true, 3: true, 4: false} {
		_, err := dest.startRestore(rootContext(), RestoreStartRequest{Database: "app", Bucket: "Plain", FormatVersion: version, StoreType: BucketTypeHash, DryRun: true})
		if accepted && err != nil {
			t.Errorf("format_version %d was refused: %v", version, err)
		}
		if !accepted && !errors.Is(err, ErrRestoreFormatVersion) {
			t.Errorf("format_version %d = %v; want ErrRestoreFormatVersion", version, err)
		}
	}

	source, _ := leaseServer(t)
	dumped := dumpSections(t, source, "fifo", DumpStartRequest{})
	dumped.manifest.FormatVersion = 2
	if answer := restoreSections(t, dest, dumped); !answer.Success || answer.Applied != 5 {
		t.Fatalf("a format-2 dump restored as %+v, want 5 documents", answer)
	}
}

// TestDumpOfABucketWithDeadEntriesNeedsANewerClient (B-14): a client that does
// not say it writes the dead section is refused a bucket that has dead entries,
// naming the bucket and the fix, and the refusal does not hold the dump slot.
func TestDumpOfABucketWithDeadEntriesNeedsANewerClient(t *testing.T) {
	s, _ := leaseServer(t)
	restoredDead(t, s, "fifo", BucketTypeFIFO, 2, "")

	_, err := s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "fifo"})
	if !errors.Is(err, ErrDumpNeedsNewerClient) || codeFor(err) != transport.ErrInvalidArgument {
		t.Fatalf("an old client's dump of a bucket with dead entries = %v; want ErrDumpNeedsNewerClient", err)
	}
	for _, part := range []string{"app/fifo", "2 dead entries", "Upgrade hoarddb"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("the refusal %q does not say %q", err, part)
		}
	}
	if old := dumpSections(t, s, "lifo", DumpStartRequest{}); old.done.Documents != 5 {
		t.Fatalf("an old client's dump of a bucket with no dead entry = %+v, want served as before", old.done)
	}
	if current := dumpSections(t, s, "fifo", DumpStartRequest{AcceptDead: true}); current.done.Dead != 2 {
		t.Fatalf("a current client's dump reports %d dead, want 2", current.done.Dead)
	}
}

// TestDumpOfAHeapTwinNeedsANewerClient: a twin is a dead entry the start-time
// count cannot see, so the walk refuses it for a client that would put it among
// the documents, and the refused dump releases the slot.
func TestDumpOfAHeapTwinNeedsANewerClient(t *testing.T) {
	s := newTestServer(t)
	withLeaseClock(s)
	createTestBucket(t, s, "app", "twin", BucketTypeHeap)
	for n := int32(1); n <= 2; n++ {
		if _, err := s.Push(PushRequest{Database: "app", Bucket: "twin", Priority: 10, Data: map[string]interface{}{"_id_": "a", "n": n}}); err != nil {
			t.Fatalf("pushing twin %d: %v", n, err)
		}
		if n == 1 {
			claimOf(t, s, "twin", 1, 60_000)
		}
	}
	if _, err := s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "twin"}); err != nil {
		t.Fatalf("starting the dump: %v", err)
	}
	var pullErr error
	for pullErr == nil {
		var chunk *DumpChunkResponse
		if chunk, pullErr = s.dumpChunk(rootContext(), DumpChunkRequest{Database: "app", Bucket: "twin"}); chunk != nil && chunk.EOF {
			t.Fatalf("an old client's dump of a heap twin reached the end without a refusal")
		}
	}
	if !errors.Is(pullErr, ErrDumpNeedsNewerClient) {
		t.Fatalf("the walk ended with %v; want ErrDumpNeedsNewerClient", pullErr)
	}
	if after := dumpSections(t, s, "twin", DumpStartRequest{AcceptDead: true}); after.done.Dead != 1 {
		t.Fatalf("the dump after the refusal reports %d dead, want 1: the refused dump must not hold the slot", after.done.Dead)
	}
}

// TestRestoreOfDeadEntriesIntoAnotherTypeIsRefused: a dump that records dead
// entries for a bucket type that has none is corrupt and is refused at the
// start, before the bucket exists.
func TestRestoreOfDeadEntriesIntoAnotherTypeIsRefused(t *testing.T) {
	dest := newTestServer(t)
	_, err := dest.startRestore(rootContext(), RestoreStartRequest{Database: "app", Bucket: "Plain", FormatVersion: 3, StoreType: BucketTypeHash, Bytes: 1, Dead: 2})
	if err == nil || !strings.Contains(err.Error(), "a hash bucket has none") {
		t.Fatalf("a hash bucket's dump recording dead entries = %v; want a refusal", err)
	}
	if _, lookupErr := dest.resolveCommittedBucket("app", "Plain"); lookupErr == nil {
		t.Fatalf("the refused restore created the bucket")
	}
}

// TestClientDeadRestoreIsRefusedEvenForRoot (pin of step 11): root's own
// DEAD_RESTORE frame is refused outside a restore, with the restore-only text,
// and writes nothing.
func TestClientDeadRestoreIsRefusedEvenForRoot(t *testing.T) {
	s, _ := leaseServer(t)
	answer, err := NewHandler(s)(rootContext(), transport.NewFrame(transport.OpDeadRestore, mustMarshalBSON(t, DeadRestoreRequest{
		Database: "app", Bucket: "fifo", Data: mustMarshalBSON(t, bson.M{"n": 1}), Deliveries: 1, Reason: store.DeadReasonCollided,
	})))
	if err != nil {
		t.Fatal(err)
	}
	if answer.Opcode != transport.OpError || !strings.Contains(decodeErrorMessage(t, answer.Payload), "restore-only: DEAD_RESTORE is sent by a restore, never by a client") {
		t.Fatalf("root's DEAD_RESTORE was answered %s: %s", answer.Opcode, answer.Payload)
	}
	if dead := deadListOf(t, s, "fifo"); len(dead) != 0 {
		t.Fatalf("the refused DEAD_RESTORE wrote %d dead entries", len(dead))
	}
}

// TestDeadRestoreIsGatedByQueueClaim: while a cluster member lacks queue_claim,
// a restore whose dump holds dead entries is refused at its start, before the
// bucket or any document exists, and a DEAD_RESTORE a restore routes is refused
// before the append.
func TestDeadRestoreIsGatedByQueueClaim(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "gated")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
	stale := stopThirdNodeAndEditWhatIsKnown(t, tc, func(c *Cluster, stale string) {
		c.memberFeatures[stale] = map[string]bool{featureBucketRF: true, featureMemtableMB: true, featureTypedFields: true}
	})
	node := tc.nodes[0].server.core

	for _, dryRun := range []bool{false, true} {
		fresh := bucketOwnedBy(t, tc, 0, "fresh")
		logged := node.wal.LastSequence()
		_, err := node.startRestore(rootContext(), RestoreStartRequest{
			Database: testDatabase, Bucket: fresh, FormatVersion: 3, StoreType: BucketTypeFIFO, Bytes: 1, Dead: 3, DryRun: dryRun,
		})
		if !errors.Is(err, errMemberLacksFeature) {
			t.Fatalf("a restore with dead entries (dry run %v) = %v; want a refusal for the member that lacks queue_claim", dryRun, err)
		}
		for _, part := range []string{"3 dead entries", `supports "queue_claim"`, `member "` + stale + `"`, "Nothing was restored."} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("the refusal %q does not say %q", err, part)
			}
		}
		if _, lookupErr := node.resolveCommittedBucket(testDatabase, fresh); lookupErr == nil {
			t.Errorf("the refused restore created bucket %q", fresh)
		}
		if now := node.wal.LastSequence(); now != logged {
			t.Errorf("the refused restore moved the log from %d to %d", logged, now)
		}
	}

	payload := mustMarshalBSON(t, DeadRestoreRequest{
		Database: testDatabase, Bucket: bucket, Data: mustMarshalBSON(t, bson.M{"n": 1}), Deliveries: 1, DeadAt: time.UnixMilli(1_800_000_000_000), Reason: store.DeadReasonCollided,
	})
	logged := node.wal.LastSequence()
	answer, err := node.routeWrite(rootContext(), transport.OpDeadRestore, payload)
	if err != nil {
		t.Fatal(err)
	}
	if refusal := frameError(answer); refusal == nil || !strings.Contains(refusal.Error(), `supports "queue_claim"`) {
		t.Fatalf("a restore's DEAD_RESTORE while a member lacks queue_claim was answered %s: %s", answer.Opcode, answer.Payload)
	}
	if now := node.wal.LastSequence(); now != logged {
		t.Errorf("the refused DEAD_RESTORE moved the log from %d to %d", logged, now)
	}
}

// TestDumpDoneChecksTheDeadDigest: the digest of the dead file the client wrote
// is compared with the bytes the server sent, as the stores digest is.
func TestDumpDoneChecksTheDeadDigest(t *testing.T) {
	s, _ := leaseServer(t)
	restoredDead(t, s, "fifo", BucketTypeFIFO, 2, "")
	if _, err := s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "fifo", AcceptDead: true}); err != nil {
		t.Fatalf("starting the dump: %v", err)
	}
	for {
		chunk, err := s.dumpChunk(rootContext(), DumpChunkRequest{Database: "app", Bucket: "fifo"})
		if err != nil {
			t.Fatalf("pulling a chunk: %v", err)
		}
		if chunk.EOF {
			break
		}
	}
	_, err := s.dumpDone(rootContext(), DumpDoneRequest{Database: "app", Bucket: "fifo", DeadDigest: "sha256:" + strings.Repeat("0", 64)})
	if !errors.Is(err, ErrDumpDigestMismatch) || !strings.Contains(err.Error(), ".dead.bson") {
		t.Fatalf("a wrong dead digest = %v; want ErrDumpDigestMismatch naming the dead file", err)
	}
}
