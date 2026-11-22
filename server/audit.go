// Package server — Append-only audit log for security-relevant operations.
//
// Audit entries are queued in a bounded channel and drained to disk by a
// background goroutine. If the queue fills up, entries are dropped with a
// warning — the data path is never blocked.
package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// AuditEvent constants for the event field.
const (
	EventAuthLogin    = "auth.login"
	EventAuthFailure  = "auth.failure"
	EventUserCreate   = "user.create"
	EventUserAlter    = "user.alter"
	EventUserDrop     = "user.drop"
	EventSchemaCreate = "schema.create"
	EventSchemaDrop   = "schema.drop"
	// EventSchemaAlter is a bucket's declaration changed: store fields added,
	// or its replication factor changed, with the old and the new factor in
	// the entry's result.
	EventSchemaAlter = "schema.alter"
	// EventAuthzDenied is a privilege refusal (spec 4 rule 5). Repeats are
	// batched so one misconfigured client cannot flood the log.
	EventAuthzDenied = "authz.denied"

	EventDumpStart = "dump.start"
	EventDumpDone  = "dump.done"
	// EventDumpAborted and EventRestoreAborted are the terminal audit events
	// for a dump or restore that ended on connection loss or a pin-ceiling
	// force-release rather than an ordinary Done call (dump-restore-spec.md
	// §11, §19 A-14): without them the log shows a Start with no matching
	// Done, indistinguishable from "still running" until an operator checks
	// the directory or bucket by hand.
	EventDumpAborted    = "dump.aborted"
	EventRestoreStart   = "restore.start"
	EventRestoreDone    = "restore.done"
	EventRestoreDrop    = "restore.drop"
	EventRestoreAborted = "restore.aborted"
	// EventKeyUnwrap is the node master unwrapped under the keystore's key at
	// start. Its Target names the key id, never key bytes.
	EventKeyUnwrap = "key.unwrap"
)

// AuditEntry is a single audit log record.
type AuditEntry struct {
	Time   time.Time `json:"time"`
	Event  string    `json:"event"`
	User   string    `json:"user,omitempty"`
	Result string    `json:"result,omitempty"`
	// AuthMethod is "password" or "token" on auth events; it is what makes
	// Identity.Resumed a read field rather than decoration (spec 2.5).
	AuthMethod    string `json:"auth_method,omitempty"`
	SourceIP      string `json:"source_ip,omitempty"`
	Target        string `json:"target,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	// Version is the server build, stamped into every entry so an audit trail
	// can say which build wrote what (GAP-4; additive to a JSON line, so no
	// reader breaks).
	Version string `json:"version,omitempty"`
}

// KeyUnwrapFailure classifies a startup failure to resolve the keystore or to
// unwrap the node master under it. It is raised while building the service,
// before the service and its audit logger exist, so a refusal can be written
// by the caller as the one key.unwrap result=failed entry the audit log would
// otherwise miss (BUG-0315). Class is "keystore" or "node-master"; the
// wrapped error keeps its own message, so wrapping changes nothing an operator
// reads.
type KeyUnwrapFailure struct {
	Class string
	err   error
}

// Error is the wrapped error's message: the classification adds a matchable
// type without changing what the operator reads.
func (e *KeyUnwrapFailure) Error() string { return e.err.Error() }

// Unwrap returns the wrapped error so errors.Is and errors.As see through it.
func (e *KeyUnwrapFailure) Unwrap() error { return e.err }

// AuditLogger writes append-only JSON audit entries to a file.
type AuditLogger struct {
	file  *os.File
	mu    sync.Mutex
	queue chan AuditEntry
	done  chan struct{}
}

const auditQueueSize = 100_000

// NewAuditLogger opens (or creates) the audit log file at the given path
// and starts a background goroutine that drains the queue to disk.
func NewAuditLogger(output string) (*AuditLogger, error) {
	// 0600: audit entries carry usernames, source addresses and targets.
	f, err := os.OpenFile(output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open audit log %s: %w", output, err)
	}

	al := &AuditLogger{
		file:  f,
		queue: make(chan AuditEntry, auditQueueSize),
		done:  make(chan struct{}),
	}

	go al.drain()

	return al, nil
}

// Log queues an audit entry for asynchronous disk write. It is non-blocking;
// if the internal queue is full the entry is dropped and a warning is logged.
//
// A nil logger (audit disabled) is a no-op, so call sites on the request path
// do not each need a nil check.
func (al *AuditLogger) Log(entry AuditEntry) {
	if al == nil {
		return
	}
	if entry.Time.IsZero() {
		entry.Time = time.Now().UTC()
	}
	if entry.Version == "" {
		// Every entry names the build that wrote it, so a trail can say which
		// server produced what. One source, stamped at the single write point.
		entry.Version = Version
	}

	select {
	case al.queue <- entry:
	default:
		slog.Warn("audit queue full, dropping entry",
			"event", entry.Event,
			"user", entry.User,
		)
	}
}

// Close drains the remaining queue entries and closes the underlying file.
func (al *AuditLogger) Close() {
	if al == nil {
		return
	}
	close(al.queue) // signals the drain goroutine to finish
	<-al.done       // wait for it
	if err := al.file.Close(); err != nil {
		slog.Warn("closing audit log", "error", err)
	}
}

// drain reads entries from the queue channel and writes them as JSON lines
// to the audit log file. It exits when the channel is closed and drained.
func (al *AuditLogger) drain() {
	defer close(al.done)

	for entry := range al.queue {
		line, err := json.Marshal(entry)
		if err != nil {
			slog.Warn("failed to marshal audit entry", "error", err)
			continue
		}
		line = append(line, '\n')

		al.mu.Lock()
		_, err = al.file.Write(line)
		al.mu.Unlock()

		if err != nil {
			slog.Error("failed to write audit entry", "error", err)
		}
	}
}
