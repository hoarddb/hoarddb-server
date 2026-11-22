package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestFoundFalseClassification is T15: one sub-test per absence literal of spec
// 6.4, each driving a real server into the case, so a renamed server message
// fails here instead of silently reclassifying failures as misses. The two
// refused cases the real server can produce (a verb the bucket type does not
// have) and the two it cannot (a storage failure, an empty reply) close the
// table.
func TestFoundFalseClassification(t *testing.T) {
	absent := []struct {
		name      string
		statement []string
		want      string
	}{
		{"hash miss", []string{`db.Users.find("zz")`}, "NOT FOUND\n"},
		{"btree miss", []string{`db.Events.find("zz")`}, "NOT FOUND\n"},
		{"empty fifo", []string{`db.Jobs.pop()`, `db.Jobs.pop()`}, "EMPTY  queue is empty\n"},
		{"empty heap", []string{`db.Tasks.pop()`, `db.Tasks.pop()`}, "EMPTY  heap is empty\n"},
		{"blob miss on get", []string{`db.Files.get("nope")`}, "NOT FOUND\n"},
		{"blob miss on info", []string{`db.Files.info("nope")`}, "NOT FOUND\n"},
		{"blob miss on find", []string{`db.Files.find("nope")`}, "NOT FOUND\n"},
	}
	for _, tc := range absent {
		t.Run("absent/"+tc.name, func(t *testing.T) {
			session := newFixtureSession(t)
			var err error
			for _, statement := range tc.statement {
				session.out.Reset()
				err = session.cli.handle(statement)
			}
			if err != nil {
				t.Fatalf("%s returned %v; an absent key is a clean miss", tc.name, err)
			}
			if got := session.out.String(); got != tc.want {
				t.Errorf("%s printed %q, want %q", tc.name, got, tc.want)
			}
		})
	}

	t.Run("refused/find on a fifo bucket", func(t *testing.T) {
		session := newFixtureSession(t)
		requireWrongTypeRefused(t, session.cli.handle(`db.Jobs.find("k")`), "get is not supported for bucket type fifo")
	})
	t.Run("refused/pop on a hash bucket", func(t *testing.T) {
		session := newFixtureSession(t)
		requireWrongTypeRefused(t, session.cli.handle(`db.Users.pop()`), `pop is not defined for hash buckets: a hash has no order to take from, so there is no "next" to remove. Remove one with delete("<key>"), or read them with get("<key>") or find().`)
	})
	t.Run("refused/a stubbed storage failure", func(t *testing.T) {
		var out bytes.Buffer
		_, c := newStubServer(t, map[transport.Opcode]interface{}{
			transport.OpGet: server.FindResponse{Found: false, Message: "hash get failed: disk I/O error"},
		}, &out)
		c.currentDB = "app"
		requireRefused(t, c.handle(`db.Users.find("k")`), "hash get failed: disk I/O error")
	})
	t.Run("refused/a stubbed empty reply is the fixed message", func(t *testing.T) {
		var out bytes.Buffer
		_, c := newStubServer(t, map[transport.Opcode]interface{}{
			transport.OpGet:     server.FindResponse{Found: false},
			transport.OpFIFOPop: server.PopResponse{Found: false},
		}, &out)
		c.currentDB = "app"
		requireRefused(t, c.handle(`db.Users.find("k")`), emptyFoundFalseMessage)
		requireRefused(t, c.handle(`db.Jobs.pop()`), emptyFoundFalseMessage)
	})
}

// requireWrongTypeRefused is requireRefused for a verb the bucket's store type
// does not answer: since BUG-0469 the server refuses it as an ERROR frame
// INVALID_ARGUMENT, not in band, so the CLI reports a ServerError, exit refused.
func requireWrongTypeRefused(t *testing.T, err error, fragment string) {
	t.Helper()
	var serverErr *ServerError
	if !errors.As(err, &serverErr) || serverErr.Code != transport.ErrInvalidArgument {
		t.Fatalf("returned %v (%T), want a ServerError INVALID_ARGUMENT", err, err)
	}
	if !strings.Contains(serverErr.Message, fragment) {
		t.Errorf("ServerError.Message = %q, want it to hold %q", serverErr.Message, fragment)
	}
	if info := classify(err); info.Code != "INVALID_ARGUMENT" || info.Exit != ExitRefused {
		t.Errorf("classify = %s exit %d, want INVALID_ARGUMENT exit %d", info.Code, info.Exit, ExitRefused)
	}
}

// requireRefused fails unless err is a *RefusedError with exactly message.
func requireRefused(t *testing.T, err error, message string) {
	t.Helper()
	var refused *RefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("returned %v (%T), want a *RefusedError %q", err, err, message)
	}
	if refused.Message != message {
		t.Errorf("RefusedError.Message = %q, want %q", refused.Message, message)
	}
	if info := classify(err); info.Code != "REFUSED" || info.Exit != ExitRefused {
		t.Errorf("classify = %s exit %d, want REFUSED exit %d", info.Code, info.Exit, ExitRefused)
	}
}

// TestFoundFalseAllowList pins the classifier's table itself, including the
// shapes that must NOT be taken for an absence.
func TestFoundFalseAllowList(t *testing.T) {
	tests := []struct {
		message string
		want    foundKind
	}{
		{"document not found", foundAbsent},
		{"queue is empty", foundAbsent},
		{"heap is empty", foundAbsent},
		{`blob "k" not found in app.Files — keys are exact; db.Files.count() shows how many the bucket holds`, foundAbsent},
		{`blob "k" not found in app.Files`, foundAbsent},
		{"", foundEmpty},
		{"hash get failed: disk I/O error", foundRefused},
		{"queue pop failed: disk I/O error", foundRefused},
		{"find not supported for bucket type fifo", foundRefused},
		{"key required", foundRefused},
		{"document not found in the catalog", foundRefused},
		{"blob read failed: not found in cache", foundRefused},
	}
	for _, tc := range tests {
		if got := foundFalse(tc.message); got != tc.want {
			t.Errorf("foundFalse(%q) = %v, want %v", tc.message, got, tc.want)
		}
	}
}
