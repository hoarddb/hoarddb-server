package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// GAP-35: the CLI refuses a blob no server could accept before it is sent.
// The CLI does not know a server's configured cap (it is node configuration
// and the protocol does not carry it), so it checks the hard ceiling every
// server shares and leaves the configured cap to the server, whose refusal
// names it.

func TestBlobPutRefusesAPayloadNoServerCouldAccept(t *testing.T) {
	var out strings.Builder
	h := &CommandHandler{terminal: &out} // no connection: a payload that reached send would say so
	oversize := strings.Repeat("x", config.MaxBlobBytesCeiling+1)

	err := h.handleBlobPut("app", "files", []string{`{_id_: "videos/intro.mp4", data: "` + oversize + `"}`})

	if err == nil || errors.Is(err, ErrNotConnected) {
		t.Fatalf("put of an oversized payload = %v, want a refusal before anything is sent", err)
	}
	for _, want := range []string{`"videos/intro.mp4"`, "31.9 MiB", "pointer document", `"Large file stores"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
}

func TestBlobPutSendsAPayloadWithinTheCeiling(t *testing.T) {
	h := &CommandHandler{terminal: &strings.Builder{}}

	err := h.handleBlobPut("app", "files", []string{`{_id_: "small", data: "hello"}`})

	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("put of a small payload = %v, want it to reach send (ErrNotConnected)", err)
	}
}

// TestBlobPutRefusesAnEmptyID is BUG-0153 for the blob put: an explicit empty
// _id_ is refused where the difference between "empty" and "omitted" is still
// visible, because the wire carries only the key and an omission renders the
// same way. Omitting _id_ still asks for a generated key.
func TestBlobPutRefusesAnEmptyID(t *testing.T) {
	h := &CommandHandler{terminal: &strings.Builder{}}

	err := h.handleBlobPut("app", "files", []string{`{_id_: "", data: "hello"}`})

	if err == nil || errors.Is(err, ErrNotConnected) {
		t.Fatalf("put with an empty _id_ = %v, want a refusal before anything is sent", err)
	}
	if !strings.Contains(err.Error(), "non-empty") {
		t.Errorf("refusal %q does not say an _id_ must be non-empty", err)
	}

	err = h.handleBlobPut("app", "files", []string{`{data: "hello"}`})
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("put with _id_ omitted = %v, want it to reach send (ErrNotConnected)", err)
	}
}
