package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// ackLedger is the client-side record of acknowledged inserts: one line per
// ack, fsynced before the next operation, so a server crash can never lose
// an id the server had already acknowledged and the ledger had not. A nil
// *ackLedger records nothing.
type ackLedger struct {
	mu   sync.Mutex
	file *os.File
}

// ledgerHeaderPrefix starts the one header line that carries what a later
// -verify-ledger run needs to address the same bucket without creating it.
const ledgerHeaderPrefix = "# hoardledger "

// openAckLedger creates the ledger and writes its header.
func openAckLedger(path, header string) (*ackLedger, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("creating the ledger %s (it must not exist yet): %w", path, err)
	}
	l := &ackLedger{file: file}
	if _, err := io.WriteString(file, ledgerHeaderPrefix+header+"\n"); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("writing the ledger header: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("syncing the ledger header: %w", err)
	}
	return l, nil
}

// record appends id and syncs. It is called after the server's ack.
func (l *ackLedger) record(id string) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := io.WriteString(l.file, id+"\n"); err != nil {
		return fmt.Errorf("appending id %s to the ledger: %w", id, err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("syncing the ledger: %w", err)
	}
	return nil
}

func (l *ackLedger) close() error {
	if l == nil {
		return nil
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("closing the ledger: %w", err)
	}
	return nil
}

// readLedger returns the header and the ids. A final line without a newline is
// a torn write of the client itself, not an acknowledged id, and is dropped.
func readLedger(path string) (string, []string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", nil, fmt.Errorf("reading the ledger %s: %w", path, err)
	}
	text := string(raw)
	if i := strings.LastIndexByte(text, '\n'); i >= 0 {
		text = text[:i]
	} else {
		text = ""
	}
	scanner := bufio.NewScanner(strings.NewReader(text))
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), ledgerHeaderPrefix) {
		return "", nil, fmt.Errorf("%s is not a wiremicro ledger: the first line is not the header", path)
	}
	header := strings.TrimPrefix(scanner.Text(), ledgerHeaderPrefix)
	var ids []string
	for scanner.Scan() {
		ids = append(ids, scanner.Text())
	}
	return header, ids, scanner.Err()
}

// errNotFound is a get that reached the server and found no document.
var errNotFound = errors.New("document not found")

// verifyIDs gets every id and returns the ones the server does not have. Any
// other error (connection, timeout) stops the verification: it is not a
// missing document.
func verifyIDs(ctx context.Context, w worker, ids []string) ([]string, error) {
	var missing []string
	for _, id := range ids {
		err := w.get(ctx, id)
		switch {
		case err == nil:
		case errors.Is(err, errNotFound):
			missing = append(missing, id)
		default:
			return missing, err
		}
	}
	return missing, nil
}
