package termio

import (
	"io"
	"os"
	"testing"
	"time"
)

// TestUpRecallsWhatThisSessionTyped is the within-session half of the history
// the owner asked for: Up and Down walk the lines typed in the running session.
// The editor's own list is the only store, so recall has to work with nothing
// on disk.
func TestUpRecallsWhatThisSessionTyped(t *testing.T) {
	master, slave := openTestPty(t)
	previous := os.Stdin
	os.Stdin = slave
	t.Cleanup(func() { os.Stdin = previous })

	editor := NewLineEditor(io.Discard, nil)
	editor.AddHistory("use app")
	editor.AddHistory("show databases")

	recalled := make(chan string, 1)
	failed := make(chan error, 1)
	go func() {
		line, err := editor.ReadLine("> ")
		if err != nil {
			failed <- err
			return
		}
		recalled <- line
	}()

	// Up, then Enter.
	if _, err := master.WriteString("\x1b[A\n"); err != nil {
		t.Fatalf("typing Up: %v", err)
	}
	select {
	case line := <-recalled:
		if line != "show databases" {
			t.Fatalf("Up recalled %q, want the last line typed, %q", line, "show databases")
		}
	case err := <-failed:
		t.Fatalf("ReadLine: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ReadLine did not return after Up and Enter")
	}
}

// TestDownWalksBackToTheLineBeingTyped: Up then Down comes back to what the user
// was typing, which is what makes Up safe to press.
func TestDownWalksBackToTheLineBeingTyped(t *testing.T) {
	master, slave := openTestPty(t)
	previous := os.Stdin
	os.Stdin = slave
	t.Cleanup(func() { os.Stdin = previous })

	editor := NewLineEditor(io.Discard, nil)
	editor.AddHistory("use app")

	recalled := make(chan string, 1)
	go func() {
		line, err := editor.ReadLine("> ")
		if err != nil {
			recalled <- "error: " + err.Error()
			return
		}
		recalled <- line
	}()

	// "show dat", Up (recalls "use app"), Down (back to what was typed), Enter.
	if _, err := master.WriteString("show dat\x1b[A\x1b[B\n"); err != nil {
		t.Fatalf("typing: %v", err)
	}
	select {
	case line := <-recalled:
		if line != "show dat" {
			t.Fatalf("Up then Down left %q, want the half-typed line %q", line, "show dat")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadLine did not return")
	}
}
