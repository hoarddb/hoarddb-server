package transport

import "testing"

// TestDocumentExistsCodeIsPinned: DOCUMENT_EXISTS is 0x12, the next code after
// BATCH_TOO_LARGE, and has a name a driver can print (build doc BD-1).
func TestDocumentExistsCodeIsPinned(t *testing.T) {
	if ErrDocumentExists != 0x12 {
		t.Fatalf("ErrDocumentExists = %#x, want 0x12", uint8(ErrDocumentExists))
	}
	if got := ErrorCode(0x12).String(); got != "DOCUMENT_EXISTS" {
		t.Fatalf("ErrorCode(0x12).String() = %q, want DOCUMENT_EXISTS", got)
	}
}
