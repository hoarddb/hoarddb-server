package server

import (
	"encoding/binary"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4/pb"
	"google.golang.org/protobuf/proto"

	"github.com/hoarddb/hoarddb-server/config"
)

// ringStartLogged is ringStartAt with the node's log written to logs.
func ringStartLogged(t *testing.T, dataDir string, edit func(*config.Config), logs *ringClusterBuffer) (*Service, error) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	edit(cfg)
	qs, err := NewService(cfg, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		return nil, err
	}
	if err := qs.Start(); err != nil {
		return nil, err
	}
	return qs, nil
}

// zeroEarlierStoreLogKeyID zeroes the key id at the head of the earlier store's value log: its
// structural check then fails while the ring store stays sealed, so a report that reads the
// earlier store says something different from one that reads the ring.
func zeroEarlierStoreLogKeyID(t *testing.T, dataDir string) {
	t.Helper()
	logs, err := filepath.Glob(filepath.Join(dataDir, nodeStoreDirName, "*.vlog"))
	if err != nil || len(logs) == 0 {
		t.Fatalf("the earlier store has no value log (%v)", err)
	}
	valueLog, err := os.OpenFile(logs[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := valueLog.WriteAt(make([]byte, 8), 0); err != nil {
		t.Fatal(errorsJoinClose(err, valueLog))
	}
	if err := valueLog.Close(); err != nil {
		t.Fatal(err)
	}
}

func errorsJoinClose(err error, file *os.File) error {
	if closeErr := file.Close(); closeErr != nil {
		return closeErr
	}
	return err
}

// TestHealthReportsTheRingStoreAsSealed: [GUARD /health's encryption field reports the ring
// store's structural check, with an earlier-store finding appended as "node store: "; red when
// checkStoreSeal still reads the earlier store's directory]. Ring encryption at rest spec §6,
// §8, §9.1 step 6.
func TestHealthReportsTheRingStoreAsSealed(t *testing.T) {
	dataDir := t.TempDir()
	edit := ringKEKEdit(t, ringFirstKEK, "on")
	ringRunOnce(t, dataDir, edit)
	zeroEarlierStoreLogKeyID(t, dataDir)
	qs, err := ringStartLogged(t, dataDir, edit, &ringClusterBuffer{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopService(t, qs)
	health, err := qs.core.HealthCheck()
	if err != nil {
		t.Fatal(err)
	}
	report := health.Encryption
	if report == nil || report.Mode != "on" {
		t.Fatalf("HealthResponse.Encryption = %+v, want mode on", report)
	}
	if !report.Sealed {
		t.Errorf("the ring store is sealed but /health says sealed=false (problem %q): it reads the earlier store", report.Problem)
	}
	if !strings.HasPrefix(report.Problem, "node store: ") {
		t.Errorf("/health's problem is %q, want the earlier store's finding prefixed \"node store: \"", report.Problem)
	}
}

// TestTheStartLineSaysTheRingIsEncrypted: [GUARD the start-up line says the ring store and log
// are encrypted, naming the node master kid, from the ring's own keys; red when the line reads
// the earlier store's state, which this fixture damages]. Spec §6, §9.1 step 6.
func TestTheStartLineSaysTheRingIsEncrypted(t *testing.T) {
	dataDir := t.TempDir()
	edit := ringKEKEdit(t, ringFirstKEK, "on")
	ringRunOnce(t, dataDir, edit)
	zeroEarlierStoreLogKeyID(t, dataDir)
	logs := &ringClusterBuffer{}
	qs, err := ringStartLogged(t, dataDir, edit, logs)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopService(t, qs)
	want := "ring store and log encrypted at rest (node master kid " + qs.core.nodeMaster.Kid() + ")"
	if !strings.Contains(logs.String(), want) {
		t.Fatalf("the start-up log does not say %q:\n%s", want, logs.String())
	}

	plainLogs := &ringClusterBuffer{}
	plain, err := ringStartLogged(t, t.TempDir(), func(*config.Config) {}, plainLogs)
	if err != nil {
		t.Fatalf("Start of the plaintext node: %v", err)
	}
	stopService(t, plain)
	if !strings.Contains(plainLogs.String(), "ring store and log are not encrypted (encryption.mode off)") {
		t.Fatalf("the plaintext node's start-up log does not say it is not encrypted:\n%s", plainLogs.String())
	}
}

// appendManifestTableWithKeyIDZero appends to storeDir's MANIFEST, in Badger's record encoding
// (length, CRC-32C, the change set), the creation of a level-6 table with key id 0: the
// hand-made mixed store only require's structural check refuses.
func appendManifestTableWithKeyIDZero(t *testing.T, storeDir string) {
	t.Helper()
	changes := &pb.ManifestChangeSet{Changes: []*pb.ManifestChange{{
		Id: 999999, Op: pb.ManifestChange_CREATE, Level: 6, KeyId: 0, EncryptionAlgo: pb.EncryptionAlgo_aes,
	}}}
	body, err := proto.Marshal(changes)
	if err != nil {
		t.Fatal(err)
	}
	record := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	record = binary.BigEndian.AppendUint32(record, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	record = append(record, body...)
	manifest, err := os.OpenFile(filepath.Join(storeDir, "MANIFEST"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manifest.Write(record); err != nil {
		t.Fatal(errorsJoinClose(err, manifest))
	}
	if err := manifest.Close(); err != nil {
		t.Fatal(err)
	}
}

// ringStoreListing is ring/badger's file names and sizes, sorted.
func ringStoreListing(t *testing.T, storeDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	var listing []string
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		listing = append(listing, entry.Name()+" "+itoa(int(info.Size())))
	}
	sort.Strings(listing)
	return listing
}

// TestRequireRefusesAHandMadeMixedRingStoreBeforeItOpens: [GUARD require runs the structural
// check of ring/badger in the load, read-only, before Badger opens; red when the require call is
// removed from the load, and separately when it moves after the store open: the open then
// rotates the logs and the listing changes]. Spec §5.7 (second review R2-8), §9.1 step 6.
func TestRequireRefusesAHandMadeMixedRingStoreBeforeItOpens(t *testing.T) {
	dataDir := t.TempDir()
	keyPath := ringKEKFile(t, ringFirstKEK)
	withMode := func(mode string) func(*config.Config) {
		return func(cfg *config.Config) {
			cfg.Encryption.Keystore = "keyfile"
			cfg.Encryption.KeyFile = keyPath
			cfg.Encryption.Mode = mode
		}
	}
	ringRunOnce(t, dataDir, withMode("require"))
	storeDir := filepath.Join(dataDir, ringDirName, "badger")
	if _, err := os.Stat(filepath.Join(dataDir, ringDirName, ringKeysDirName, ringStoreKeyFileName)); err != nil {
		t.Fatalf("the fixture has no ring store key: %v", err)
	}
	// Control: the check passes a sealed ring store a require node wrote and closed.
	ringRunOnce(t, dataDir, withMode("require"))
	appendManifestTableWithKeyIDZero(t, storeDir)
	before := ringStoreListing(t, storeDir)
	_, err := ringStartAt(t, dataDir, withMode("require"))
	requireRefusalSays(t, err, "encryption.mode is require but the ring store at "+storeDir+" is not sealed: table 999999 has KeyID == 0")
	after := ringStoreListing(t, storeDir)
	if strings.Join(before, ",") != strings.Join(after, ",") {
		t.Fatalf("the refused start changed ring/badger:\nbefore %v\nafter  %v", before, after)
	}
}
