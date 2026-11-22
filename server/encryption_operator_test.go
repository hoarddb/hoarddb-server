package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
)

// operatorKEK is the key-encryption key the sealed test nodes below use. Its
// bytes are recognisable so a test can look for them in what a node reports.
var operatorKEK = bytes.Repeat([]byte{0xA7}, 32)

// sealEdit configures a node to seal under a key file in mode.
func sealEdit(t testing.TB, mode string) func(*config.Config) {
	t.Helper()
	// The keystore refuses a key file inside the data directory.
	keyPath := filepath.Join(t.TempDir(), "kek")
	if err := os.WriteFile(keyPath, operatorKEK, 0o600); err != nil {
		t.Fatalf("writing the test key file: %v", err)
	}
	return func(cfg *config.Config) {
		cfg.Encryption.Keystore = "keyfile"
		cfg.Encryption.KeyFile = keyPath
		cfg.Encryption.Mode = mode
	}
}

// stopService stops a test service when the test ends.
func stopService(t testing.TB, qs *Service) {
	t.Helper()
	t.Cleanup(func() {
		if err := qs.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
}

func newSealedService(t testing.TB) *Service {
	t.Helper()
	qs := newCapacityService(t, sealEdit(t, "require"), nil)
	stopService(t, qs)
	return qs
}

func newPlaintextService(t testing.TB) *Service {
	t.Helper()
	qs := newCapacityService(t, nil, nil)
	stopService(t, qs)
	return qs
}

// encryptionSeries is every series in the encryption families.
func encryptionSeries(t testing.TB, qs *Service) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	for family, series := range gatherSampler(t, qs.core.capacity) {
		if strings.HasPrefix(family, "hoarddb_node_encryption") {
			counts[family] = len(series)
		}
	}
	return counts
}

// TestMetricsReportASealedNode: /metrics says whether the node's store is
// sealed, as one 0/1 gauge, and carries the mode and keystore as bounded info
// labels (inventory D4).
func TestMetricsReportASealedNode(t *testing.T) {
	sealed := gatherSampler(t, newSealedService(t).core.capacity)
	if value, ok := seriesValue(sealed, "hoarddb_node_encryption_sealed", "", ""); !ok || value != 1 {
		t.Errorf("hoarddb_node_encryption_sealed on a sealed node = %v (present %v), want 1", value, ok)
	}
	info := sealed["hoarddb_node_encryption_info"]
	if len(info) != 1 || labelValue(info[0], "mode") != "require" || labelValue(info[0], "keystore") != "keyfile" || metricValue(info[0]) != 1 {
		t.Errorf("hoarddb_node_encryption_info = %v, want one series mode=require keystore=keyfile at 1", info)
	}

	plaintext := gatherSampler(t, newPlaintextService(t).core.capacity)
	if value, ok := seriesValue(plaintext, "hoarddb_node_encryption_sealed", "", ""); !ok || value != 0 {
		t.Errorf("hoarddb_node_encryption_sealed on a plaintext node = %v (present %v), want 0", value, ok)
	}
}

// TestMetricsCarryNoPerBucketEncryptionSeries: the encryption series are one
// node-level fact; a thousand buckets add none (small-bucket principle).
func TestMetricsCarryNoPerBucketEncryptionSeries(t *testing.T) {
	qs := newSealedService(t)
	before := encryptionSeries(t, qs)
	if len(before) == 0 {
		t.Fatal("the node reports no encryption series, so this test proves nothing")
	}
	if resp, err := qs.core.CreateDatabase("many"); err != nil || !resp.Success {
		t.Fatalf("creating the database: %+v, %v", resp, err)
	}
	for i := 0; i < 1000; i++ {
		req := CreateBucketRequest{Database: "many", Name: fmt.Sprintf("b%04d", i), Type: BucketTypeHash}
		if resp, err := qs.core.createBucketLocked(context.Background(), req); err != nil || !resp.Success {
			t.Fatalf("creating bucket %d: %+v, %v", i, resp, err)
		}
	}
	after := encryptionSeries(t, qs)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Errorf("encryption series changed with 1,000 buckets: %v before, %v after", before, after)
	}
	for _, series := range gatherSampler(t, qs.core.capacity)["hoarddb_node_encryption_info"] {
		for _, label := range series.GetLabel() {
			if label.GetName() != "mode" && label.GetName() != "keystore" {
				t.Errorf("hoarddb_node_encryption_info carries label %q; only mode and keystore are bounded", label.GetName())
			}
		}
	}
}

// TestStatusReportsTheStructuralCheckNotTheConfiguredMode: the answer is what
// the store's files show, computed once when the store opened. A store opened
// plaintext under a config that says "on" must not say yes, and a sealed store
// under a config that says "off" must not say no (spec §5.3: reporting the
// configured mode is how a mixed state stays invisible).
func TestStatusReportsTheStructuralCheckNotTheConfiguredMode(t *testing.T) {
	plaintext := newPlaintextService(t)
	plaintext.core.config.Encryption.Mode = "on"
	if plaintext.EncryptionStatus().StoreSealed {
		t.Error("EncryptionStatus.StoreSealed is true for a plaintext store because the config says on")
	}
	health, err := plaintext.core.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.Encryption == nil || health.Encryption.Sealed {
		t.Errorf("HealthResponse.Encryption = %+v, want present and not sealed", health.Encryption)
	}
	if value, _ := seriesValue(gatherSampler(t, plaintext.core.capacity), "hoarddb_node_encryption_sealed", "", ""); value != 0 {
		t.Errorf("hoarddb_node_encryption_sealed = %v for a plaintext store, want 0", value)
	}
	listing := mustListBuckets(t, plaintext)
	if listing.Encrypted != "no" {
		t.Errorf("ListBucketsResponse.Encrypted = %q for a plaintext store, want no", listing.Encrypted)
	}

	sealed := newSealedService(t)
	sealed.core.config.Encryption.Mode = "off"
	if !sealed.EncryptionStatus().StoreSealed {
		t.Error("EncryptionStatus.StoreSealed is false for a sealed store because the config says off")
	}
}

// TestStatusIsSealedAfterARestartWithTables: the check must also pass on a
// store that has tables on disk, which is what every real restart opens.
func TestStatusIsSealedAfterARestartWithTables(t *testing.T) {
	dataDir := t.TempDir()
	edit := sealEdit(t, "require")
	withDataDir := func(cfg *config.Config) {
		edit(cfg)
		cfg.Storage.DataDir = dataDir
	}
	first := newCapacityServiceAt(t, dataDir, withDataDir)
	fillEncryptionTestNode(t, first.core)
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if tables, _ := filepath.Glob(filepath.Join(dataDir, nodeStoreDirName, "*.sst")); len(tables) == 0 {
		t.Fatal("the first run left no table on disk, so the restart would check an empty MANIFEST")
	}
	second := newCapacityServiceAt(t, dataDir, withDataDir)
	stopService(t, second)
	status := second.EncryptionStatus()
	if !status.StoreSealed {
		t.Errorf("a restarted sealed store reports not sealed: %q", status.StoreProblem)
	}
}

// sealedDataDirWithDamagedMarker starts a node in firstMode over a fresh data
// directory, writes to it, stops it, and zeroes the key id at the head of its
// value log: the on-disk state the structural check exists to catch
// (BUG-0321). It returns the config edit that points a later start at the same
// directory with the given mode, and the damaged log's name.
func sealedDataDirWithDamagedMarker(t *testing.T, firstMode string) (withDataDir func(mode string) func(*config.Config), damagedLog string, damagedPath string) {
	t.Helper()
	dataDir := t.TempDir()
	keyEdit := sealEdit(t, firstMode)
	withDataDir = func(mode string) func(*config.Config) {
		return func(cfg *config.Config) {
			keyEdit(cfg)
			cfg.Encryption.Mode = mode
			cfg.Storage.DataDir = dataDir
		}
	}
	first := newCapacityServiceAt(t, dataDir, withDataDir(firstMode))
	fillEncryptionTestNode(t, first.core)
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	vlogs, err := filepath.Glob(filepath.Join(dataDir, nodeStoreDirName, "*.vlog"))
	if err != nil || len(vlogs) == 0 {
		t.Fatalf("the first run left no value log (%v), so there is no key id to corrupt", err)
	}
	vlog, err := os.OpenFile(vlogs[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("opening the value log: %v", err)
	}
	if _, err := vlog.WriteAt(make([]byte, 8), 0); err != nil {
		_ = vlog.Close()
		t.Fatalf("zeroing the value log key id: %v", err)
	}
	if err := vlog.Close(); err != nil {
		t.Fatalf("closing the value log: %v", err)
	}
	return withDataDir, filepath.Base(vlogs[0]), vlogs[0]
}

// TestStoreSealedFalseWhenVlogKeyIDCorruptedBeforeStart: a store whose value
// log was altered before start must not be reported as sealed by a node that
// is configured to seal. The key id is the first 8 bytes of the log; zeroing
// them is the on-disk state the structural check exists to catch (BUG-0321).
// The node runs in mode on, which warns and keeps serving; require refuses
// (TestRequireModeRefusesToStartWhenTheStoreIsNotSealed).
func TestStoreSealedFalseWhenVlogKeyIDCorruptedBeforeStart(t *testing.T) {
	withDataDir, damagedLog, _ := sealedDataDirWithDamagedMarker(t, "on")

	var logLines syncBuffer
	logger := slog.New(slog.NewTextHandler(&logLines, nil))
	second := newCapacityService(t, withDataDir("on"), logger)
	stopService(t, second)
	status := second.EncryptionStatus()
	if status.StoreSealed {
		t.Error("EncryptionStatus.StoreSealed is true for a store whose value log key id is 0")
	}
	wantProblem := damagedLog + " has key id 0"
	if !strings.Contains(status.StoreProblem, wantProblem) {
		t.Errorf("EncryptionStatus.StoreProblem = %q, want it to name the log: %q", status.StoreProblem, wantProblem)
	}
	if !strings.Contains(logLines.String(), "level=WARN") || !strings.Contains(logLines.String(), damagedLog) {
		t.Errorf("an on node over a damaged marker logged %q, want a WARN naming %s", logLines.String(), damagedLog)
	}
}

// newCapacityServiceAt is newCapacityService over an existing data directory.
func newCapacityServiceAt(t testing.TB, dataDir string, edit func(*config.Config)) *Service {
	t.Helper()
	return newCapacityService(t, func(cfg *config.Config) {
		if edit != nil {
			edit(cfg)
		}
		cfg.Storage.DataDir = dataDir
	}, nil)
}

// TestHealthReportsTheNodesEncryptionWithoutKeyMaterial: the verb's source
// carries mode, keystore and key ids and the not-sealed list, and neither the
// key-encryption key's bytes nor its hex appear in what travels.
func TestHealthReportsTheNodesEncryptionWithoutKeyMaterial(t *testing.T) {
	qs := newSealedService(t)
	health, err := qs.core.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	report := health.Encryption
	if report == nil {
		t.Fatal("HealthResponse.Encryption is nil")
	}
	if !report.Sealed || report.Mode != "require" || report.Keystore != "keyfile" || report.KEKKid == "" || report.NodeMasterKid == "" {
		t.Errorf("HealthResponse.Encryption = %+v, want sealed, require, keyfile, with both key ids", report)
	}
	if len(report.NotSealed) == 0 {
		t.Error("HealthResponse.Encryption.NotSealed is empty; the verb would overstate what sealed means")
	}
	wireBytes, err := bson.Marshal(health)
	if err != nil {
		t.Fatalf("marshalling the health response: %v", err)
	}
	jsonBytes, err := json.Marshal(health)
	if err != nil {
		t.Fatalf("marshalling the health response as JSON: %v", err)
	}
	for _, rendering := range [][]byte{wireBytes, jsonBytes} {
		if bytes.Contains(rendering, operatorKEK) || bytes.Contains(bytes.ToLower(rendering), []byte(hex.EncodeToString(operatorKEK))) {
			t.Error("the health response carries the key-encryption key")
		}
	}
	plaintext, err := newPlaintextService(t).core.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck on a plaintext node: %v", err)
	}
	if plaintext.Encryption == nil || plaintext.Encryption.Sealed || plaintext.Encryption.Mode != "off" {
		t.Errorf("a plaintext node reports %+v, want present, off, not sealed", plaintext.Encryption)
	}
}

func mustListBuckets(t testing.TB, qs *Service) *ListBucketsResponse {
	t.Helper()
	if resp, err := qs.core.CreateDatabase("shown"); err != nil || !resp.Success {
		t.Fatalf("creating the database: %+v, %v", resp, err)
	}
	listing, err := qs.core.ListBuckets(ListBucketsRequest{Database: "shown"})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	return listing
}

// TestDescribeBucketNamesTheNodesEncryption: describe bucket, stats() and show
// buckets read the node's state (spec §5.3).
func TestDescribeBucketNamesTheNodesEncryption(t *testing.T) {
	for _, tc := range []struct {
		name string
		qs   *Service
		want string
	}{
		{"sealed", newSealedService(t), "yes"},
		{"plaintext", newPlaintextService(t), "no"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listing := mustListBuckets(t, tc.qs)
			if listing.Encrypted != tc.want {
				t.Errorf("ListBucketsResponse.Encrypted = %q, want %q", listing.Encrypted, tc.want)
			}
			req := CreateBucketRequest{Database: "shown", Name: "users", Type: BucketTypeHash}
			if resp, err := tc.qs.core.createBucketLocked(context.Background(), req); err != nil || !resp.Success {
				t.Fatalf("creating the bucket: %+v, %v", resp, err)
			}
			stats, err := tc.qs.core.BucketStats(BucketStatsRequest{Database: "shown", Bucket: "users"})
			if err != nil {
				t.Fatalf("BucketStats: %v", err)
			}
			if stats.Encrypted != tc.want {
				t.Errorf("BucketStatsResponse.Encrypted = %q, want %q", stats.Encrypted, tc.want)
			}
		})
	}
}

// TestAuditRecordsAKeyUnwrap: a sealed node's start writes a key.unwrap entry
// naming the node master's key id and no key bytes. (A failed unwrap refuses
// the start before an audit log exists; that ordering is an open question and
// is not built here.)
func TestAuditRecordsAKeyUnwrap(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	edit := sealEdit(t, "require")
	qs, _ := newTCPEnabledServer(t, func(cfg *config.Config) {
		edit(cfg)
		cfg.Audit.Enabled = true
		cfg.Audit.Output = auditPath
	})
	wantTarget := "node-master " + qs.core.nodeMaster.Kid()

	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, _ := os.ReadFile(auditPath)
		for _, line := range strings.Split(string(raw), "\n") {
			if !strings.Contains(line, `"`+EventKeyUnwrap+`"`) {
				continue
			}
			var entry AuditEntry
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Fatalf("parsing audit line %q: %v", line, err)
			}
			if entry.Result != "ok" || entry.Target != wantTarget {
				t.Errorf("key.unwrap entry = %+v, want result ok, target %q", entry, wantTarget)
			}
			if strings.Contains(line, hex.EncodeToString(operatorKEK)) {
				t.Error("the audit entry carries key material")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no key.unwrap entry in the audit log after 10s:\n%s", raw)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestAuditWritesNoKeyUnwrapWithoutAKeystore: a plaintext node unwraps nothing.
func TestAuditWritesNoKeyUnwrapWithoutAKeystore(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	newTCPEnabledServer(t, func(cfg *config.Config) {
		cfg.Audit.Enabled = true
		cfg.Audit.Output = auditPath
	})
	time.Sleep(300 * time.Millisecond)
	raw, _ := os.ReadFile(auditPath)
	if strings.Contains(string(raw), EventKeyUnwrap) {
		t.Errorf("a node with no keystore wrote a key.unwrap entry:\n%s", raw)
	}
}
