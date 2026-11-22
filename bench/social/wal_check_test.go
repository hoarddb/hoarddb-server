package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// appendPostEntry logs one post the way the server logs a field write: the
// request frame as the payload, under the author's bucket.
func appendPostEntry(t *testing.T, wal *storage.WAL, bucket string, author int, seq int64) {
	t.Helper()
	post := postRecord{Author: userID(author), Seq: seq, Text: "wal check", Created: time.Now().UTC().Truncate(time.Millisecond), Tags: []string{"t"}}
	frame, err := bson.Marshal(server.FieldRequest{Database: "social", Bucket: bucket, Key: userID(author), Field: "Posts", Data: post.document()})
	if err != nil {
		t.Fatalf("encoding the request: %v", err)
	}
	if _, err := wal.Append(bucket, uint8(transport.OpFieldPut), []byte(userID(author)), frame); err != nil {
		t.Fatalf("appending: %v", err)
	}
}

func walCheckTestLedger() ackLedgerFile {
	return ackLedgerFile{Schema: ackLedgerSchema, Authors: []ackEntry{
		{User: 4, Author: userID(4), Bucket: "User_4", Owner: "node-a", Acked: int64Ptr(12), InFlight: int64Ptr(13)},
		{User: 5, Author: userID(5), Bucket: "User_5", Owner: "node-b", Acked: int64Ptr(12)},
	}}
}

// TestWALCheckFindsEveryAcknowledgedPostInTheLog: with each acknowledged post
// in the log under its author's bucket, the check passes, and the in-flight
// post is reported either way.
func TestWALCheckFindsEveryAcknowledgedPostInTheLog(t *testing.T) {
	dir := t.TempDir()
	wal, err := storage.OpenWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendPostEntry(t, wal, "User_4", 4, 12)
	appendPostEntry(t, wal, "User_5", 5, 12)
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runWALCheckOn(context.Background(), &config{WALDir: dir}, walCheckTestLedger(), &out); err != nil {
		t.Fatalf("a log holding every acknowledged post failed: %v", err)
	}
	var result ledgerCheckResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || !result.Pass || result.Found != 2 || result.InFlightAbsent != 1 {
		t.Errorf("result %+v (%v); want 2 found, the in-flight post absent, pass", result, err)
	}
}

// TestWALCheckFailsOnAnAcknowledgedPostMissingFromTheLog: a post in the wrong
// bucket, or the same sequence of another author, does not count.
func TestWALCheckFailsOnAnAcknowledgedPostMissingFromTheLog(t *testing.T) {
	dir := t.TempDir()
	wal, err := storage.OpenWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendPostEntry(t, wal, "User_5", 4, 12) // author 4's post filed under author 5's bucket
	appendPostEntry(t, wal, "User_5", 5, 11) // author 5's neighbour, not the acknowledged one
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = runWALCheckOn(context.Background(), &config{WALDir: dir}, walCheckTestLedger(), &out)
	if err == nil || !strings.Contains(err.Error(), "author "+userID(5)+" seq 12") || !strings.Contains(err.Error(), "author "+userID(4)+" seq 12") {
		t.Fatalf("error = %v; want both authors' seq 12 named", err)
	}
}

// TestWALCheckCanBeScopedToOneOwner: -ledger-owner limits the check to the
// authors one node owns.
func TestWALCheckCanBeScopedToOneOwner(t *testing.T) {
	dir := t.TempDir()
	wal, err := storage.OpenWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendPostEntry(t, wal, "User_4", 4, 12)
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runWALCheckOn(context.Background(), &config{WALDir: dir, LedgerOwner: "node-a"}, walCheckTestLedger(), &out); err != nil {
		t.Fatalf("author 5 belongs to node-b and must not be checked for node-a: %v", err)
	}
}

// TestWALCheckNeedsTheLogDirectory: -mode walcheck without -wal-dir is refused.
func TestWALCheckNeedsTheLogDirectory(t *testing.T) {
	if _, err := parseFlags([]string{"-mode", "walcheck", "-ledger", "l.json"}); err == nil || !strings.Contains(err.Error(), "-wal-dir") {
		t.Errorf("-mode walcheck without -wal-dir: %v; want a refusal naming -wal-dir", err)
	}
	cfg, err := parseFlags([]string{"-mode", "walcheck", "-ledger", "l.json", "-wal-dir", "wal"})
	if err != nil || cfg.WALDir != "wal" {
		t.Errorf("-mode walcheck: %+v, %v", cfg, err)
	}
}
