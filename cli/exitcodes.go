package cli

// The exit codes of the hoarddb binary and its standalone verbs (dump,
// restore, ingest) are one table, defined here and nowhere else
// (the gap24-one-shot-spec record §6.2). A number is never reused for a new
// meaning, and a script that branches on one can rely on it across verbs.
//
// Which numbers each verb returns today is a subset of this table. Codes 6, 7
// and 8 are the retry policies a wrapper needs to tell apart: 6 means nothing
// was sent, 7 means never auto-retry, and 8 means the statement was sent and
// its outcome is unknown.
const (
	// ExitOK: every statement (or every bucket, or every document) succeeded.
	ExitOK = 0

	// ExitUsage: a bad flag or flag combination, an unreadable input, a
	// pre-flight refusal, or client-side validation. No write was sent.
	ExitUsage = 1

	// ExitRefused: the server answered and did not apply the request: a missing
	// database or bucket, a permission or argument refusal, a duplicate key,
	// INTERNAL, or a container the CLI found absent. Do not retry unchanged.
	ExitRefused = 2

	// ExitPartial: dump and restore, at least one bucket succeeded and at least
	// one failed, or, once the ring dump can see them, queue messages it had to
	// leave out (a damaged or cut-off dump file is ExitUsage, not this code, and
	// today dump and restore return 3 only through the legacy engine); ingest, the run completed with
	// documents skipped; batch, some statement was not a success. Read the
	// report's errors.
	ExitPartial = 3

	// ExitInterrupted: SIGINT or SIGTERM stopped the run early. Every result
	// already emitted is true.
	ExitInterrupted = 4

	// ExitAccountsAwaiting: restore recreated accounts that still need a
	// password. No code path returns it yet: restore does not recreate users in
	// this release.
	ExitAccountsAwaiting = 5

	// ExitConnect: no session could be established (dial, TLS, untrusted
	// certificate, pin mismatch, or an error during AUTH such as OVERLOADED).
	// Nothing was sent, so retrying with backoff is safe.
	ExitConnect = 6

	// ExitAuth: the server refused the credentials, for any reason. Never
	// auto-retry: repeated failures lock the account out.
	ExitAuth = 7

	// ExitUnavailable: the request was sent and its outcome is unknown or
	// transient (UNAVAILABLE, TIMEOUT, a lost reply). Retry only if the request
	// is idempotent, because a write may have applied.
	ExitUnavailable = 8
)

// exitRow is one line of the exit table.
type exitRow struct {
	Code int
	// Class is the machine-readable name of the code, or "" for the two codes
	// (0 and 5) that are not failure classes.
	Class   string
	Meaning string
}

// exitTable is the single source of the constants' meanings: the docs generator
// (DOC-18) and ExitClass both read it, so a number cannot mean one thing here
// and another in a page. It lists every code exactly once, in ascending order.
var exitTable = [...]exitRow{
	{ExitOK, "", "every statement succeeded"},
	{ExitUsage, "usage", "bad flag or flag combination, unreadable input, pre-flight refusal, or client-side validation; no write was sent"},
	{ExitRefused, "refused", "the server answered and did not apply the request"},
	{ExitPartial, "partial", "dump/restore: some buckets succeeded and some failed; ingest: completed with documents skipped; batch: some statements were not a success"},
	{ExitInterrupted, "interrupted", "SIGINT or SIGTERM stopped the run early"},
	{ExitAccountsAwaiting, "", "restore: accounts awaiting passwords"},
	{ExitConnect, "connect", "no session could be established; nothing was sent"},
	{ExitAuth, "auth", "the server refused the credentials; never auto-retry"},
	{ExitUnavailable, "unavailable", "the request was sent and its outcome is unknown or transient"},
}

// ExitClass returns the machine-readable class of an exit code: "usage",
// "refused", "partial", "interrupted", "connect", "auth" or "unavailable". Codes
// 0 and 5 are in the table but are not failure classes, so they return "" and
// true. A number the table does not define returns "" and false.
func ExitClass(code int) (class string, defined bool) {
	for _, row := range exitTable {
		if row.Code == code {
			return row.Class, true
		}
	}
	return "", false
}
