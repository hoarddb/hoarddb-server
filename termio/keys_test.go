package termio

import (
	"testing"
	"time"
)

// TestKeysReadsAnArrowAsOneKey is the guard against the defect that made the
// pager quit on an arrow key: a terminal sends Down as ESC, '[', 'B', and a
// reader that hands back one byte at a time turns the ESC into a quit key and
// leaves "[B" in the terminal, where it leaks into the next prompt. The whole
// sequence must come back as one key, and nothing may be left behind.
func TestKeysReadsAnArrowAsOneKey(t *testing.T) {
	master, slave := openTestPty(t)
	keys, err := NewKeys(slave)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	defer func() { _ = keys.Close() }()

	if _, err := master.WriteString("\x1b[B"); err != nil {
		t.Fatalf("writing the arrow key: %v", err)
	}
	key, err := keys.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if key != "\x1b[B" {
		t.Fatalf("Read returned %q, want the whole Down sequence %q", key, "\x1b[B")
	}

	// The next key a reader sees must be the next key the user pressed. If the
	// sequence's tail were still queued, the next read would return "[".
	if _, err := master.WriteString("q"); err != nil {
		t.Fatalf("writing the next key: %v", err)
	}
	next, err := keys.Read()
	if err != nil {
		t.Fatalf("Read after the arrow: %v", err)
	}
	if next != "q" {
		t.Fatalf("the key after the arrow was %q, want %q: the sequence's tail leaked", next, "q")
	}
}

// TestKeysReadsAPageKeyAsOneKey covers the longer sequences: Page Up is ESC,
// '[', '5', '~', whose parameters must be consumed too.
func TestKeysReadsAPageKeyAsOneKey(t *testing.T) {
	master, slave := openTestPty(t)
	keys, err := NewKeys(slave)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	defer func() { _ = keys.Close() }()

	if _, err := master.WriteString("\x1b[5~"); err != nil {
		t.Fatalf("writing Page Up: %v", err)
	}
	key, err := keys.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if key != "\x1b[5~" {
		t.Fatalf("Read returned %q, want the whole Page Up sequence %q", key, "\x1b[5~")
	}
}

// TestKeysReadsALoneEscapeAsEscape is the other half of the same rule: a real
// Escape key sends nothing after the ESC, so the read must give up waiting and
// return it — a reader that always waited for more bytes would hang the pager.
func TestKeysReadsALoneEscapeAsEscape(t *testing.T) {
	master, slave := openTestPty(t)
	keys, err := NewKeys(slave)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	defer func() { _ = keys.Close() }()

	if _, err := master.WriteString("\x1b"); err != nil {
		t.Fatalf("writing Escape: %v", err)
	}
	done := make(chan string, 1)
	go func() {
		key, err := keys.Read()
		if err != nil {
			done <- "error: " + err.Error()
			return
		}
		done <- key
	}()
	select {
	case key := <-done:
		if key != "\x1b" {
			t.Fatalf("Read returned %q, want the lone %q", key, "\x1b")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a lone Escape did not come back: the read waited for bytes that never arrive")
	}
}

// TestKeysReadsAPlainKeyAsItself pins that the sequence handling did not change
// the ordinary case: one printable byte is one key.
func TestKeysReadsAPlainKeyAsItself(t *testing.T) {
	master, slave := openTestPty(t)
	keys, err := NewKeys(slave)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	defer func() { _ = keys.Close() }()

	if _, err := master.WriteString("j"); err != nil {
		t.Fatalf("writing the key: %v", err)
	}
	key, err := keys.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if key != "j" {
		t.Fatalf("Read returned %q, want %q", key, "j")
	}
}
