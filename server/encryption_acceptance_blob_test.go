package server

import (
	"os"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/store"
)

// Environment of TestAcceptanceWriteBlob, set by scripts/encryption-acceptance.sh
// for its blob leg.
const (
	acceptanceBlobDirEnv    = "HOARDDB_ENCRYPTION_ACCEPTANCE_BLOB_DIR"
	acceptanceBlobModeEnv   = "HOARDDB_ENCRYPTION_ACCEPTANCE_BLOB_MODE"
	acceptanceBlobKeyEnv    = "HOARDDB_ENCRYPTION_ACCEPTANCE_BLOB_KEY"
	acceptanceBlobMarkerEnv = "HOARDDB_ENCRYPTION_ACCEPTANCE_BLOB_MARKER"
)

// TestAcceptanceWriteBlob writes blobs into a fresh data directory through the
// real start path (NewService at the given mode with a key file) and stops it,
// for the acceptance script to inspect and to start its real server on. It
// exists because the CLI cannot send a payload of 64 KiB or more: its statement
// reader is bounded at 64 KiB a line, and a payload below that is inline in the
// store, which is not what the blob leg is about. It skips unless the script
// has set the environment, so a plain `go test` never runs it.
//
// It writes one blob bucket, "uploads" in "accdb", holding a file-backed blob
// "report" (the marker repeated to a bit over three chunks) and an inline blob
// "note", and nothing else.
func TestAcceptanceWriteBlob(t *testing.T) {
	dataDir := os.Getenv(acceptanceBlobDirEnv)
	if dataDir == "" {
		t.Skipf("%s is not set: this is the acceptance script's blob-leg writer", acceptanceBlobDirEnv)
	}
	mode, keyPath, marker := os.Getenv(acceptanceBlobModeEnv), os.Getenv(acceptanceBlobKeyEnv), os.Getenv(acceptanceBlobMarkerEnv)
	if marker == "" || (mode != "off" && mode != "require") {
		t.Fatalf("%s must be off or require and %s must be set (mode %q, marker %q)", acceptanceBlobModeEnv, acceptanceBlobMarkerEnv, mode, marker)
	}
	cfg := sealedServiceConfig(dataDir, keyPath, mode)
	cfg.Server.ReplicationFactor = 1
	if mode == "off" {
		cfg.Encryption.Keystore, cfg.Encryption.KeyFile = "off", ""
	}
	service, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("starting the node at mode %s: %v", mode, err)
	}
	if resp, err := service.core.CreateDatabase("accdb"); err != nil || !resp.Success {
		t.Fatalf("create database: %+v, %v", resp, err)
	}
	if resp, err := service.core.CreateBucket(CreateBucketRequest{Database: "accdb", Name: "uploads", Type: BucketTypeBlob}); err != nil || !resp.Success {
		t.Fatalf("create bucket at mode %s: %+v, %v", mode, resp, err)
	}
	report := strings.Repeat(marker, (3*store.InlineThreshold+17)/len(marker)+1)
	for key, payload := range map[string]string{"report": report, "note": marker} {
		resp, err := service.core.BlobWrite(BlobWriteRequest{Database: "accdb", Bucket: "uploads", Key: key, Data: []byte(payload)})
		if err != nil || !resp.Success {
			t.Fatalf("blob write %s: %+v, %v", key, resp, err)
		}
	}
	if err := service.Stop(); err != nil {
		t.Fatalf("stopping the node: %v", err)
	}
}
