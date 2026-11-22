package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAuditEntryCarriesTheVersion stamps the build version into every audit
// entry, so a trail can say which build wrote what (GAP-4).
func TestAuditEntryCarriesTheVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	logger, err := NewAuditLogger(path)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	logger.Log(AuditEntry{Event: "auth.login", User: "root", Result: "ok"})

	logger.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), `"version":"`+Version+`"`) {
		t.Fatalf("audit entry lacks the server version %q: %s", Version, data)
	}
}
