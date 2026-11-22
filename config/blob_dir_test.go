package config

import "testing"

// TestBlobDirDefaultsToEmpty pins zero-config: an unset blob_dir is empty, and
// the server resolves empty to <data dir>/blobs, the path it always used.
func TestBlobDirDefaultsToEmpty(t *testing.T) {
	t.Setenv("HOARDDB_BLOB_DIR", "")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.BlobDir != "" {
		t.Errorf("blob_dir default = %q, want empty (the server reads empty as <data dir>/blobs)", cfg.Storage.BlobDir)
	}
}

func TestBlobDirFromYAMLAndEnv(t *testing.T) {
	t.Setenv("HOARDDB_BLOB_DIR", "")
	path := writeYAML(t, "storage:\n  blob_dir: /mnt/yaml-blobs\n")
	fromYAML, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if fromYAML.Storage.BlobDir != "/mnt/yaml-blobs" {
		t.Errorf("storage.blob_dir from YAML = %q, want /mnt/yaml-blobs", fromYAML.Storage.BlobDir)
	}

	t.Setenv("HOARDDB_BLOB_DIR", "/mnt/env-blobs")
	fromEnv, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if fromEnv.Storage.BlobDir != "/mnt/env-blobs" {
		t.Errorf("HOARDDB_BLOB_DIR over YAML = %q, want /mnt/env-blobs (the environment overrides the file)", fromEnv.Storage.BlobDir)
	}
}
