package config

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// GAP-35: a blob has an explicit size cap. It is 16 MiB by default, an
// operator may raise it, and the ceiling is what one frame can carry.

func TestBlobCapDefaultsTo16MiB(t *testing.T) {
	t.Setenv("HOARDDB_MAX_BLOB_BYTES", "")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.MaxBlobBytes != 16<<20 {
		t.Errorf("storage.max_blob_bytes default = %d, want %d (16 MiB)", cfg.Storage.MaxBlobBytes, 16<<20)
	}
}

// The ceiling is below the ruled 32 MiB because the frame is 32 MiB in total.
func TestBlobCeilingLeavesRoomInTheFrame(t *testing.T) {
	if MaxBlobBytesCeiling != transport.MaxFrameLength-BlobFrameReserve {
		t.Fatalf("MaxBlobBytesCeiling = %d, want MaxFrameLength - BlobFrameReserve = %d",
			MaxBlobBytesCeiling, transport.MaxFrameLength-BlobFrameReserve)
	}
	if MaxBlobBytesCeiling >= transport.MaxFrameLength {
		t.Fatalf("a blob of MaxBlobBytesCeiling (%d) cannot fit a frame of %d with its envelope",
			MaxBlobBytesCeiling, transport.MaxFrameLength)
	}
}

func TestLoadRefusesBlobCapOutsideItsRange(t *testing.T) {
	tests := []struct {
		name  string
		value int
	}{
		{"zero", 0},
		{"negative", -1},
		{"one over the ceiling", MaxBlobBytesCeiling + 1},
		{"the ruled 32 MiB, which does not fit a frame", 32 << 20},
	}
	for _, tt := range tests {
		t.Run("yaml "+tt.name, func(t *testing.T) {
			t.Setenv("HOARDDB_MAX_BLOB_BYTES", "")
			path := writeYAML(t, fmt.Sprintf("storage:\n  max_blob_bytes: %d\n", tt.value))
			_, err := Load(path)
			assertBlobCapRefusal(t, err)
		})
		t.Run("env "+tt.name, func(t *testing.T) {
			t.Setenv("HOARDDB_MAX_BLOB_BYTES", fmt.Sprint(tt.value))
			_, err := Load("")
			assertBlobCapRefusal(t, err)
		})
	}
}

// assertBlobCapRefusal checks Rule 3: the error names the key, the allowed
// range and the fix.
func assertBlobCapRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Load accepted a blob cap outside 1..MaxBlobBytesCeiling; want a refusal")
	}
	for _, want := range []string{
		"storage.max_blob_bytes",
		"HOARDDB_MAX_BLOB_BYTES",
		fmt.Sprint(MaxBlobBytesCeiling),
		"between 1 and",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
}

func TestLoadAcceptsARaisedBlobCapUpToTheCeiling(t *testing.T) {
	for _, value := range []int{1, 20 << 20, MaxBlobBytesCeiling} {
		t.Setenv("HOARDDB_MAX_BLOB_BYTES", fmt.Sprint(value))
		cfg, err := Load("")
		if err != nil {
			t.Fatalf("Load with max_blob_bytes %d: %v", value, err)
		}
		if cfg.Storage.MaxBlobBytes != value {
			t.Errorf("max_blob_bytes = %d, want %d", cfg.Storage.MaxBlobBytes, value)
		}
	}
}
