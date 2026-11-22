package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The write-ahead-log level of the power-loss check (spec 7.2): after the
// reset, every acknowledged post must be in the reset node's own log, not only
// readable through the store. A read through the store could be answered from
// data the log never held; the log is what a peer would be caught up from.
//
// The check runs on a copy of the node's wal directory, taken while the node is
// stopped, because opening a log repairs its tail. It finds a post by the shape
// the server logs it in: an entry under the author's bucket whose payload is the
// request frame, whose data document carries "author" and "seq". It does not
// decode the frame; it looks for those two BSON elements, which a frame that
// holds the post must contain verbatim.

// postInPayload says whether payload holds a BSON document with the string
// element author=<author> and the integer element seq=<seq>.
func postInPayload(payload []byte, author string, seq int64) bool {
	name := append([]byte{0x02}, "author\x00"...)
	name = binary.LittleEndian.AppendUint32(name, uint32(len(author)+1))
	authorElement := append(append(name, author...), 0)
	if !bytes.Contains(payload, authorElement) {
		return false
	}
	seq64 := binary.LittleEndian.AppendUint64(append([]byte{0x12}, "seq\x00"...), uint64(seq))
	seq32 := binary.LittleEndian.AppendUint32(append([]byte{0x10}, "seq\x00"...), uint32(seq))
	return bytes.Contains(payload, seq64) || (seq >= 0 && seq <= 1<<31-1 && bytes.Contains(payload, seq32))
}

// runWALCheckOn reads the log in cfg.WALDir and checks the ledger against it.
func runWALCheckOn(_ context.Context, cfg *config, ledger ackLedgerFile, out io.Writer) error {
	if ledger.Schema != ackLedgerSchema {
		return fmt.Errorf("-ledger has schema %q, want %q", ledger.Schema, ackLedgerSchema)
	}
	wal, err := storage.OpenWAL(cfg.WALDir, nil)
	if err != nil {
		return fmt.Errorf("opening the copy of the write-ahead log in -wal-dir %s: %w (a sealed log needs its keys; this check reads plain logs)", cfg.WALDir, err)
	}
	entries, readErr := wal.ReadFrom(1)
	if closeErr := wal.Close(); readErr == nil && closeErr != nil {
		readErr = fmt.Errorf("closing the log copy: %w", closeErr)
	}
	if readErr != nil {
		return fmt.Errorf("reading the write-ahead log in -wal-dir %s: %w", cfg.WALDir, readErr)
	}
	byBucket := map[string][][]byte{}
	for _, entry := range entries {
		byBucket[entry.Bucket] = append(byBucket[entry.Bucket], entry.Data)
	}
	held := func(entry ackEntry, seq int64) bool {
		for _, payload := range byBucket[entry.Bucket] {
			if postInPayload(payload, entry.Author, seq) {
				return true
			}
		}
		return false
	}
	result := ledgerCheckResult{Mode: modeWALCheck, Authors: len(ledger.Authors), Missing: []missingPost{}}
	for _, entry := range ledger.Authors {
		if cfg.LedgerOwner != "" && entry.Owner != cfg.LedgerOwner {
			result.Skipped++
			continue
		}
		result.Checked++
		if entry.Acked != nil {
			if held(entry, *entry.Acked) {
				result.Found++
			} else {
				result.Missing = append(result.Missing, missingPost{Author: entry.Author, Seq: *entry.Acked, Owner: entry.Owner})
			}
		}
		if entry.InFlight != nil {
			if held(entry, *entry.InFlight) {
				result.InFlightPresent++
			} else {
				result.InFlightAbsent++
			}
		}
	}
	result.Pass = len(result.Missing) == 0
	line, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encoding the log check: %w", err)
	}
	if _, err := fmt.Fprintln(out, string(line)); err != nil {
		return fmt.Errorf("writing the log check: %w", err)
	}
	if !result.Pass {
		var lost []string
		for _, post := range result.Missing {
			lost = append(lost, fmt.Sprintf("author %s seq %d", post.Author, post.Seq))
		}
		return fmt.Errorf("log check failed: %d acknowledged posts are not in the write-ahead log: %v", len(lost), lost)
	}
	return nil
}

// runWALCheck is -mode walcheck.
func runWALCheck(ctx context.Context, cfg *config, out io.Writer) error {
	encoded, err := os.ReadFile(cfg.LedgerFile)
	if err != nil {
		return fmt.Errorf("reading -ledger: %w", err)
	}
	var ledger ackLedgerFile
	if err := json.Unmarshal(encoded, &ledger); err != nil {
		return fmt.Errorf("-ledger %s is not a ledger file: %w", cfg.LedgerFile, err)
	}
	return runWALCheckOn(ctx, cfg, ledger, out)
}
