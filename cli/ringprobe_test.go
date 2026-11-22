package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringFieldNameAnswer is the ring's verdict on one field name: whether a declaration may use
// it, and when it may not, who refused it and in what words (the text is matched as a
// substring, so the probe pins the reason rather than the offsets inside it).
type ringFieldNameAnswer struct {
	accept bool
	// by is "server" when the ring refused the declaration, and "client" when ringclient
	// refused it while encoding, so the name never reached the server at all.
	by      string
	refusal string
}

// TestRingFieldNameLegality records whether the ring accepts a declaration whose field name is
// one a BSON file may carry: a plain name, the reserved _id, Mongoose's __v, a dotted path, a
// dollar name, a non-ASCII name, and both ends of the 255-byte name limit.
//
// Build plan 4.3 leaves this unverified, and I1a's refusal text (and whether it needs a
// --skip-field flag) depends on the answer, so this probe records the ring's own verdict
// instead of asserting a rule it would be inventing. The limit the server enforces is on
// bytes, not characters (cluster/wire/ringadmin.go:checkAdminFieldList compares
// len(field.Name) with ringMaxFieldWord), which is why the two long names here are ASCII.
func TestRingFieldNameLegality(t *testing.T) {
	names := []string{
		"Name",
		"_id",
		"__v",
		"a.b",
		"$x",
		"Ünï",
		strings.Repeat("n", 255),
		strings.Repeat("n", 256),
		"",
		"a\tb",
		"\xff\xfe",
		"a b",
	}
	// want is the answer the ring gave, copied from this probe's own run: it is a record of
	// the server's behaviour, not a specification of it. A name with no row fails, so a name
	// added above cannot pass unnoticed. Every accepted name is legal today, so a file may
	// declare any of them and I1a needs no --skip-field flag for them (plan 4.3); both ends
	// of the byte limit are refused by ringclient while encoding, before a byte is sent.
	want := map[string]ringFieldNameAnswer{
		"Name": {accept: true},
		// _id is the document id, which a declaration normally leaves out; naming it as a
		// field is accepted and nothing else is probed about it here.
		"_id": {accept: true},
		// Mongoose writes __v into every document it saves, so an ingest of a mongodump
		// file meets it; the ring takes it as an ordinary field.
		"__v": {accept: true},
		// A dotted name is one flat field here, never a nested path: the ring accepts it.
		"a.b": {accept: true},
		"$x":  {accept: true},
		// The limit is bytes, not characters: this name is 4 characters and 5 bytes.
		"Ünï": {accept: true},
		// 255 bytes is the ceiling (cluster/wire/ringadmin.go:ringMaxFieldWord).
		strings.Repeat("n", 255): {accept: true},
		strings.Repeat("n", 256): {
			by:      "client",
			refusal: "a name of 256 bytes and a field name is 1 to 255. Send a name in that range",
		},
		"": {
			by:      "client",
			refusal: "a name of 0 bytes and a field name is 1 to 255. Send a name in that range",
		},
		// A control character and bytes that are not UTF-8 are refused by ringclient while
		// encoding, in the same words; a space is an ordinary character and is accepted.
		"a\tb": {
			by:      "client",
			refusal: "not printable UTF-8. Send a name of printable characters",
		},
		"\xff\xfe": {
			by:      "client",
			refusal: "not printable UTF-8. Send a name of printable characters",
		},
		"a b": {accept: true},
	}

	_, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "probe", 0); err != nil {
		t.Fatalf("create database probe: %v", err)
	}
	for index, name := range names {
		// One bucket per name: a refused create must not be confused with a bucket that a
		// previous row already declared.
		bucket := fmt.Sprintf("Probe%d", index)
		_, err := client.CreateBucket(ctx, "probe", bucket, []ringclient.FieldDecl{{Name: name, Type: "string"}})
		got := ringFieldNameAnswer{accept: err == nil}
		if err != nil {
			var refusal *ringclient.Error
			if errors.As(err, &refusal) {
				got.by, got.refusal = "server", refusal.Message
			} else {
				got.by, got.refusal = "client", err.Error()
			}
		}
		t.Logf("field name %s (%d bytes): %s", probeLabel(name), len(name), got.describe())

		expected, recorded := want[name]
		if !recorded {
			t.Errorf("field name %s (%d bytes) has no recorded answer; the ring answered: %s", probeLabel(name), len(name), got.describe())
			continue
		}
		if got.accept != expected.accept {
			t.Errorf("field name %s (%d bytes): accept = %v, want %v (the ring answered: %s)", probeLabel(name), len(name), got.accept, expected.accept, got.describe())
			continue
		}
		if got.by != expected.by {
			t.Errorf("field name %s (%d bytes): refused by the %s, want the %s (the ring answered: %s)", probeLabel(name), len(name), got.by, expected.by, got.describe())
			continue
		}
		if !strings.Contains(got.refusal, expected.refusal) {
			t.Errorf("field name %s (%d bytes): refusal = %q, want it to contain %q", probeLabel(name), len(name), got.refusal, expected.refusal)
		}
	}
}

// describe renders a verdict for the probe's log and its failure texts.
func (a ringFieldNameAnswer) describe() string {
	if a.accept {
		return "accepted"
	}
	return fmt.Sprintf("refused by the %s: %s", a.by, a.refusal)
}

// probeLabel names a field in a log line without printing a 256-byte run of one letter.
func probeLabel(name string) string {
	const shown = 24
	if len(name) <= shown {
		return fmt.Sprintf("%q", name)
	}
	return fmt.Sprintf("%q...", name[:shown])
}
