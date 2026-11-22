package server

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

const damagedReadDocuments = 2000

// damagedReadPad is 256 bytes derived from the document's index, so the
// table's blocks do not compress to a few bytes and a flipped bit lands in a
// document rather than in the block layout.
func damagedReadPad(index int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("pad-%d", index)))
	return strings.Repeat(hex.EncodeToString(digest[:]), 4)
}

// TestADamagedStoreFileReadIsAnErrorNamingTheBucket defends the damaged-store error at the
// surface a developer meets: with one bit flipped in a table of the node store,
// a find of a document in the damaged block answers that the bucket could not
// be read because a store file is damaged, and how to repair it. Before the
// fix this plaintext node verified no block checksum and answered the damaged
// document with a wrong value; an encrypted one answered "document not found"
// (the storage test covers both). A full-scan search of the bucket fails
// rather than answer without those documents, and the node counts the reads.
func TestADamagedStoreFileReadIsAnErrorNamingTheBucket(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if resp, err := first.core.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp, err := first.core.CreateBucket(CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	for index := 0; index < damagedReadDocuments; index++ {
		_, err := first.core.Insert(InsertRequest{
			Database: "app", Bucket: "users", Key: fmt.Sprintf("user-%05d", index),
			Data: map[string]interface{}{"index": index, "pad": damagedReadPad(index)},
		})
		if err != nil {
			t.Fatalf("Insert user-%05d: %v", index, err)
		}
	}
	first.stop(t)

	storeDir := filepath.Join(dataDir, nodeStoreDirName)
	tablePath := largestTableIn(t, storeDir)
	tableBytes, err := os.ReadFile(tablePath)
	if err != nil {
		t.Fatalf("reading %s: %v", tablePath, err)
	}
	tableBytes[len(tableBytes)/2] ^= 0x10
	if err := os.WriteFile(tablePath, tableBytes, 0o600); err != nil {
		t.Fatalf("writing %s: %v", tablePath, err)
	}

	next := startCatalogServerAt(t, dataDir)
	if err := next.qs.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog: %v (the flipped bit was meant to land in a document, not the catalog)", err)
	}
	countedBefore := storage.DamagedReads()

	var found, wrongValues, silentMisses, damaged int
	var damagedMessage string
	for index := 0; index < damagedReadDocuments; index++ {
		resp, err := next.core.Find(FindRequest{Database: "app", Bucket: "users", Key: fmt.Sprintf("user-%05d", index)})
		switch {
		case err != nil:
			t.Fatalf("Find user-%05d returned a Go error instead of an answer: %v", index, err)
		case resp.Found:
			found++
			if pad, _ := resp.Data.Lookup("pad").StringValueOK(); pad != damagedReadPad(index) {
				wrongValues++
				t.Errorf("user-%05d answered a document whose pad is not the one written: the damage read back as data", index)
			}
		case strings.Contains(resp.Message, "is damaged"):
			damaged++
			damagedMessage = resp.Message
		default:
			silentMisses++
			t.Errorf("user-%05d answered %q: a document in the damaged block read as never written", index, resp.Message)
		}
	}
	t.Logf("found=%d (wrong value=%d) damaged=%d silently missing=%d", found, wrongValues, damaged, silentMisses)
	if damaged == 0 {
		t.Fatalf("no find reported the damage")
	}
	for _, wanted := range []string{`bucket "users" in database "app" could not be read`, filepath.Base(tablePath), "backup", "remove-node"} {
		if !strings.Contains(damagedMessage, wanted) {
			t.Errorf("the damaged read's message does not say %q: %s", wanted, damagedMessage)
		}
	}
	if counted := storage.DamagedReads() - countedBefore; counted < int64(damaged) {
		t.Errorf("hoarddb_store_damaged_reads_total rose by %d for %d damaged reads", counted, damaged)
	}

	// No document holds this value, so the search walks the whole bucket:
	// before the fix it answered "no results" without the damaged block.
	resp, err := next.core.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"index": -1}})
	if err == nil {
		t.Fatalf("a full-scan search across the damaged block answered %+v and no error", resp)
	}
	if !strings.Contains(err.Error(), "is damaged") {
		t.Errorf("the search's error does not say the store is damaged: %v", err)
	}
}

// largestTableIn returns the biggest table file in a stopped node store.
func largestTableIn(t *testing.T, storeDir string) string {
	t.Helper()
	tablePaths, err := filepath.Glob(filepath.Join(storeDir, "*.sst"))
	if err != nil {
		t.Fatalf("listing the tables in %s: %v", storeDir, err)
	}
	var largest string
	var largestSize int64
	for _, tablePath := range tablePaths {
		info, err := os.Stat(tablePath)
		if err != nil {
			t.Fatalf("stat %s: %v", tablePath, err)
		}
		if info.Size() > largestSize {
			largest, largestSize = tablePath, info.Size()
		}
	}
	if largest == "" {
		t.Fatalf("the stopped store at %s holds no table file to damage", storeDir)
	}
	return largest
}
