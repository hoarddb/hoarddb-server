package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0283: a store field may carry its own braces. The CLI parsed the inner
// declaration but kept only the inner store fields, so an inner scalar and an
// inner unique were dropped with no note. A unique inside a store field's
// braces read as a unique index and created none.

// TestCreateBucketNestedUniqueIsRefused: a unique modifier inside a store
// field's braces is refused before anything is sent, and the refusal names the
// store field and the inner field.
func TestCreateBucketNestedUniqueIsRefused(t *testing.T) {
	cmd, err := ParseCommand("create bucket Post { Purchases fifoStore { Amount double, Code string unique } }")
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	_, err = parseBucketSchema(cmd, "app")
	if err == nil {
		t.Fatal("a unique modifier inside a store field's braces was accepted; it must be refused, because no index is created for it")
	}
	for _, want := range []string{"Purchases", "Code", "unique", "bucket"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not name %q", err, want)
		}
	}
}

// TestCreateBucketNestedScalarIsReported: an inner scalar without unique is not
// silent. It carries the same not-enforced note as a top-level scalar, and the
// note names the store field the scalar sits in.
func TestCreateBucketNestedScalarIsReported(t *testing.T) {
	c := newDumpTestServer(t)
	var out bytes.Buffer
	c.handler.terminal = &out
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	if err := createFromStatement(t, c, "create bucket Post { Purchases fifoStore { Code string } }"); err != nil {
		t.Fatalf("create: %v", err)
	}
	printed := out.String()
	for _, want := range []string{"not enforced", "Purchases.Code string"} {
		if !strings.Contains(printed, want) {
			t.Errorf("the create output\n%s\ndoes not say %q", printed, want)
		}
	}
}
