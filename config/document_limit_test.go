package config

import (
	"fmt"
	"strings"
	"testing"
)

// GAP-36: one document has an explicit size limit. It is 24 MiB by default, an
// operator may lower it to 4 KiB or raise it to 30 MiB, and nothing outside
// that starts the server.

func TestDocumentLimitDefaultsTo24MiB(t *testing.T) {
	t.Setenv("HOARDDB_MAX_DOCUMENT_BYTES", "")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.MaxDocumentBytes != 24<<20 {
		t.Errorf("storage.max_document_bytes default = %d, want %d (24 MiB)", cfg.Storage.MaxDocumentBytes, 24<<20)
	}
}

func TestLoadRefusesDocumentLimitOutsideItsRange(t *testing.T) {
	tests := []struct {
		name  string
		value int
	}{
		{"zero", 0},
		{"negative", -1},
		{"below the minimum", MinDocumentBytes - 1},
		{"31 MiB", 31 << 20},
		{"one over the ceiling", MaxDocumentBytesCeiling + 1},
	}
	for _, tt := range tests {
		t.Run("yaml "+tt.name, func(t *testing.T) {
			t.Setenv("HOARDDB_MAX_DOCUMENT_BYTES", "")
			_, err := Load(writeYAML(t, fmt.Sprintf("storage:\n  max_document_bytes: %d\n", tt.value)))
			assertDocumentLimitRefusal(t, err)
		})
		t.Run("env "+tt.name, func(t *testing.T) {
			t.Setenv("HOARDDB_MAX_DOCUMENT_BYTES", fmt.Sprint(tt.value))
			_, err := Load("")
			assertDocumentLimitRefusal(t, err)
		})
	}
}

// assertDocumentLimitRefusal checks Rule 3: the error names the key, the
// allowed range and the fix.
func assertDocumentLimitRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Load accepted a document limit outside its range; want a refusal")
	}
	for _, want := range []string{
		"storage.max_document_bytes",
		"HOARDDB_MAX_DOCUMENT_BYTES",
		fmt.Sprint(MinDocumentBytes),
		fmt.Sprint(MaxDocumentBytesCeiling),
		"between",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
}

func TestLoadAcceptsADocumentLimitFromMinimumToCeiling(t *testing.T) {
	for _, value := range []int{MinDocumentBytes, 1 << 20, MaxDocumentBytesCeiling} {
		t.Setenv("HOARDDB_MAX_DOCUMENT_BYTES", fmt.Sprint(value))
		cfg, err := Load("")
		if err != nil {
			t.Fatalf("Load with max_document_bytes %d: %v", value, err)
		}
		if cfg.Storage.MaxDocumentBytes != value {
			t.Errorf("max_document_bytes = %d, want %d", cfg.Storage.MaxDocumentBytes, value)
		}
	}
}
