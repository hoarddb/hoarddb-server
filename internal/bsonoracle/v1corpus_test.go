package bsonoracle

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/oracle/compare"
)

const (
	maxCorpusRecords   = 50000
	maxCorpusGzipBytes = 8 << 20
)

func readCorpusTestdata(t *testing.T, name string) []byte {
	t.Helper()
	fileBytes, err := os.ReadFile(filepath.Join(v1CorpusDir(), name))
	if err != nil {
		t.Fatalf("reading %s: %v. The capture is internal/bsonoracle/testdata/v1corpus/capture.sh", name, err)
	}
	return fileBytes
}

func TestV1CorpusRecordsItsSourceAndCap(t *testing.T) {
	records, err := V1Corpus()
	if err != nil {
		t.Fatalf("loading the v1.0 corpus: %v", err)
	}
	gzipBytes := readCorpusTestdata(t, "records.jsonl.gz")

	// The cap: at most 8 MiB compressed and 50,000 records, each distinct by
	// the SHA-256 of its value.
	if len(gzipBytes) > maxCorpusGzipBytes {
		t.Errorf("records.jsonl.gz is %d bytes, the cap is %d", len(gzipBytes), maxCorpusGzipBytes)
	}
	if len(records) == 0 || len(records) > maxCorpusRecords {
		t.Errorf("the corpus holds %d records, want 1 to %d", len(records), maxCorpusRecords)
	}
	seen := map[[sha256.Size]byte]bool{}
	perClass := map[string]int{}
	for position, record := range records {
		valueHash := sha256.Sum256(record.Value)
		if seen[valueHash] {
			t.Fatalf("record %d repeats the value of an earlier record (SHA-256 %x)", position, valueHash)
		}
		seen[valueHash] = true
		perClass[record.Class]++
		if len(record.Value) < 5 || int(binary.LittleEndian.Uint32(record.Value)) != len(record.Value) || record.Value[len(record.Value)-1] != 0 {
			t.Fatalf("record %d (class %s) does not frame as a BSON document", position, record.Class)
		}
		if record.Class == "" || record.DepthLimit < 100 {
			t.Fatalf("record %d has class %q and depth limit %d", position, record.Class, record.DepthLimit)
		}
	}

	// SOURCE.md names where the corpus came from, and what it holds.
	source := string(readCorpusTestdata(t, "SOURCE.md"))
	field := func(name string) string {
		match := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `: (\S+)$`).FindStringSubmatch(source)
		if match == nil {
			t.Fatalf("SOURCE.md has no line %q", name+": <value>")
		}
		return match[1]
	}
	for _, commitField := range []string{"main merge-base", "next", "capture tree"} {
		commit := field(commitField)
		if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(commit) {
			t.Errorf("SOURCE.md %s = %q, want a 40-digit commit hash", commitField, commit)
			continue
		}
		// The published tree has no history: the hash is then only recorded.
		if _, lookErr := exec.LookPath("git"); lookErr == nil {
			if exec.Command("git", "cat-file", "-e", commit+"^{commit}").Run() != nil {
				t.Logf("SOURCE.md %s %s is not in this checkout's history (a published tree or a shallow clone)", commitField, commit)
			}
		}
	}
	if got := field("records"); got != strconv.Itoa(len(records)) {
		t.Errorf("SOURCE.md says %s records, the corpus holds %d", got, len(records))
	}
	if got := field("gzip bytes"); got != strconv.Itoa(len(gzipBytes)) {
		t.Errorf("SOURCE.md says %s gzip bytes, the file is %d", got, len(gzipBytes))
	}
	// Every command the capture ran is written down: the test commands of
	// capture.sh appear in SOURCE.md exactly as the script runs them.
	captureScript := string(readCorpusTestdata(t, "capture.sh"))
	commands := regexp.MustCompile(`(?m)^\s*runTests (\S+) (.+)$`).FindAllStringSubmatch(captureScript, -1)
	if len(commands) < 4 {
		t.Fatalf("capture.sh runs %d tapped test commands, want at least 4", len(commands))
	}
	for _, command := range commands {
		want := "go test -tags bsoncorpus -count=1 " + command[2]
		if !strings.Contains(source, want) {
			t.Errorf("SOURCE.md does not record the command %q that capture.sh runs", want)
		}
	}
	for _, command := range []string{"capture.sh", "go build -tags bsoncorpus", "bench/social", "bench/propertyshape", "hoarddb ingest", "corpuscapture"} {
		if !strings.Contains(source, command) {
			t.Errorf("SOURCE.md does not mention %q", command)
		}
	}

	// stats.json is the merge's own count; it must say what the corpus holds.
	var stats struct {
		Kept   int            `json:"kept"`
		KeptBy map[string]int `json:"keptPerClass"`
	}
	if err := json.Unmarshal(readCorpusTestdata(t, "stats.json"), &stats); err != nil {
		t.Fatalf("parsing stats.json: %v", err)
	}
	if stats.Kept != len(records) {
		t.Errorf("stats.json kept %d, the corpus holds %d", stats.Kept, len(records))
	}
	for class, count := range perClass {
		if stats.KeptBy[class] != count {
			t.Errorf("stats.json keeps %d of class %s, the corpus holds %d", stats.KeptBy[class], class, count)
		}
	}
}

// blobMetaSiteDepthLimit is the depth the server accepts a blob's metadata
// record at: server/migrate.go:storedDocumentOf reads the NSBlobMeta namespace
// at requestDepthLimit(OpBlobWrite), maxDocumentDepth plus the request
// envelope, 101. The corpus's depthLimit column says 100 for the class
// (corpuscapture/classify.go gives every namespace without a row the bare
// bound), which understates the site: three records the v1.0 server wrote are
// 101 deep. The server test TestBlobMetaSiteLimitIsTheOneThisTestUses holds
// the 101 to requestDepthLimit.
const blobMetaSiteDepthLimit = 101

// TestV1CorpusPassesValidateAtItsSiteLimit holds the library's Validate to the
// bytes the v1.0 server wrote: every record is a document the server accepted
// at its site's depth limit, so the library, which takes the scan's place at
// that site, must accept it at the same limit. A record it refuses is a stored
// value the swapped server could no longer read.
func TestV1CorpusPassesValidateAtItsSiteLimit(t *testing.T) {
	records, err := V1Corpus()
	if err != nil {
		t.Fatalf("loading the v1.0 corpus: %v", err)
	}
	perLimit := map[int]int{}
	for position, record := range records {
		siteLimit := record.DepthLimit
		if record.Class == "blobmeta" {
			siteLimit = blobMetaSiteDepthLimit
		}
		if _, err := bson.Validate(record.Value, bson.Limits{Depth: siteLimit}); err != nil {
			t.Errorf("record %d (class %s, key prefix %q, site depth limit %d) is refused: %v", position, record.Class, record.KeyPrefix, siteLimit, err)
		}
		perLimit[siteLimit]++
	}
	if len(perLimit) == 0 {
		t.Fatal("the v1.0 corpus holds no records, so nothing was validated")
	}
	t.Logf("%d records validated at their site limits: %v", len(records), perLimit)
}

// decodeRecordWithBoth decodes record twice into fresh targets of one type,
// with the vendor (through the shim, then ConvertTarget so its interface
// slots hold the library's types) and with the library, and returns what each
// produced. A record one refuses and the other accepts is a divergence.
func decodeRecordWithBoth[T any](t *testing.T, position int, record V1Record) (vendorResult, libraryResult T, decoded bool) {
	t.Helper()
	vendorErr := Unmarshal(record.Value, &vendorResult)
	libraryErr := bson.Unmarshal(record.Value, &libraryResult)
	switch {
	case vendorErr != nil && libraryErr != nil:
		return vendorResult, libraryResult, false
	case vendorErr != nil || libraryErr != nil:
		t.Errorf("record %d (class %s, key prefix %q) decoded into %T: vendor error %v, library error %v", position, record.Class, record.KeyPrefix, vendorResult, vendorErr, libraryErr)
		return vendorResult, libraryResult, false
	}
	ConvertTarget(&vendorResult)
	return vendorResult, libraryResult, true
}

// TestV1CorpusDecodesIdentically is the other half of spec 9.1 row 6: the
// documents the v1.0 server wrote must decode to the same values with the
// library as with the vendor, as a D and as a map, which are the two shapes
// the server reads stored documents into. Validate passing is not enough: a
// codec that reads a stored byte differently would hand the swapped server
// other data than the v1.0 server stored.
func TestV1CorpusDecodesIdentically(t *testing.T) {
	records, err := V1Corpus()
	if err != nil {
		t.Fatalf("loading the v1.0 corpus: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("the v1.0 corpus holds no records, so nothing was decoded")
	}
	decodedAsD, decodedAsMap := 0, 0
	for position, record := range records {
		if vendorD, libraryD, ok := decodeRecordWithBoth[bson.D](t, position, record); ok {
			decodedAsD++
			compare.Decoded(t, vendorD, libraryD)
		}
		if vendorMap, libraryMap, ok := decodeRecordWithBoth[map[string]any](t, position, record); ok {
			decodedAsMap++
			compare.Decoded(t, vendorMap, libraryMap)
		}
	}
	if decodedAsD == 0 || decodedAsMap == 0 {
		t.Fatalf("no record decoded (as D: %d, as a map: %d); the comparison would pass over nothing", decodedAsD, decodedAsMap)
	}
	t.Logf("%d records: %d decoded identically as a D, %d as a map", len(records), decodedAsD, decodedAsMap)
}
