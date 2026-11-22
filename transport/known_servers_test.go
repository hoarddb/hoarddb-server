package transport

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// The tests in this file use only the KnownServers API that existed before
// the known_servers fix (NewKnownServers, IsKnown,
// IsKnownFingerprint, Trust, ClientVerifyPeerCertFunc), so each can be run
// against the pre-fix code and seen to fail. The tests of the API the fix
// added are in known_servers_rules_test.go.

var (
	knownKeyA = mustKeyHex(strings.Repeat("a1", ed25519.PublicKeySize))
	knownKeyB = mustKeyHex(strings.Repeat("b2", ed25519.PublicKeySize))
)

func mustKeyHex(hexKey string) ed25519.PublicKey {
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		panic(fmt.Sprintf("test key %q is not hex: %v", hexKey, err)) // a package-level test constant
	}
	return ed25519.PublicKey(keyBytes)
}

// knownServersTestPath is a known_servers path in a fresh 0700 directory.
// t.TempDir's own directories are 0777 less the umask (0775 under the
// common 0002), which the safety rule refuses (BUG-0532).
func knownServersTestPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(ownerOnlyDir(t), "known_servers")
}

func ownerOnlyDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "hoarddb")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	return dir
}

// writeKnownServers writes text as a known_servers file in a fresh 0700
// directory and returns its path.
func writeKnownServers(t *testing.T, text string) string {
	t.Helper()
	path := knownServersTestPath(t)
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatalf("write known_servers: %v", err)
	}
	return path
}

func loadKnownServers(t *testing.T, path string) *KnownServers {
	t.Helper()
	known, err := NewKnownServers(path)
	if err != nil {
		t.Fatalf("NewKnownServers(%s): %v", path, err)
	}
	return known
}

func readText(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

// TestKnownServersCanonicalHost: every spelling of one host finds
// the key recorded under another, and 127.0.0.1 and localhost stay apart.
func TestKnownServersCanonicalHost(t *testing.T) {
	tests := []struct {
		name     string
		recorded string
		dialled  []string
	}{
		{"upper-case name", "db.internal:7433", []string{"DB.internal:7433", "DB.INTERNAL:7433"}},
		{"trailing dot", "db.internal:7433", []string{"db.internal.:7433", "DB.Internal.:7433"}},
		{"recorded with a trailing dot", "db.internal.:7433", []string{"db.internal:7433"}},
		{"IPv6 spellings", "[::1]:7433", []string{"[0:0:0:0:0:0:0:1]:7433", "[::0001]:7433"}},
		{"IPv6 recorded long", "[0:0:0:0:0:0:0:1]:7433", []string{"[::1]:7433"}},
		{"leading zeros in the port", "db:7433", []string{"db:07433"}},
		{"default port", "db.internal:7433", []string{"db.internal"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			known := loadKnownServers(t, writeKnownServers(t, tc.recorded+" "+fmt.Sprintf("%x", knownKeyA)+"\n"))
			for _, spelling := range tc.dialled {
				if !known.IsKnown(spelling, knownKeyA) {
					t.Errorf("IsKnown(%q) = false for a key recorded as %q", spelling, tc.recorded)
				}
				if recorded, _ := known.IsKnownFingerprint(spelling); !recorded {
					t.Errorf("IsKnownFingerprint(%q) = false for a key recorded as %q", spelling, tc.recorded)
				}
			}
		})
	}

	t.Run("127.0.0.1 and localhost stay different names", func(t *testing.T) {
		known := loadKnownServers(t, writeKnownServers(t, "127.0.0.1:7433 "+fmt.Sprintf("%x", knownKeyA)+"\n"))
		if recorded, _ := known.IsKnownFingerprint("localhost:7433"); recorded {
			t.Fatal("localhost:7433 found the key recorded for 127.0.0.1:7433; the two must stay different names")
		}
	})

	t.Run("Trust under one spelling is found under another", func(t *testing.T) {
		path := knownServersTestPath(t)
		known := loadKnownServers(t, path)
		if err := known.Trust("DB.Internal.:7433", knownKeyA); err != nil {
			t.Fatalf("Trust: %v", err)
		}
		reloaded := loadKnownServers(t, path)
		if !reloaded.IsKnown("db.internal:7433", knownKeyA) {
			t.Fatalf("a key trusted as DB.Internal.:7433 is not found as db.internal:7433; file:\n%s", readText(t, path))
		}
	})
}

// TestKnownServersChangedKeyUnderAnotherSpelling: a server that
// shows a different key under another spelling of a recorded host is a
// changed key, refused naming both fingerprints, and the prompt is never
// asked.
func TestKnownServersChangedKeyUnderAnotherSpelling(t *testing.T) {
	recorded := generatedCertDER(t)
	presented := generatedCertDER(t)
	path := writeKnownServers(t, fmt.Sprintf("db.internal:7433 %x\n", recorded.pub))
	known := loadKnownServers(t, path)

	prompted := false
	verify := ClientVerifyPeerCertFunc(known, "DB.Internal.:7433", func(string, string) bool {
		prompted = true
		return true
	})
	refusal := verify([][]byte{presented.der}, nil)
	if refusal == nil {
		t.Fatal("a different key under another spelling of a recorded host was accepted")
	}
	if prompted {
		t.Fatal("a changed key under another spelling was offered as a first sight")
	}
	for _, want := range []string{Fingerprint(recorded.pub), Fingerprint(presented.pub), path} {
		if !strings.Contains(refusal.Error(), want) {
			t.Errorf("refusal %q does not name %q", refusal, want)
		}
	}
	if text := readText(t, path); strings.Contains(text, fmt.Sprintf("%x", presented.pub)) {
		t.Fatalf("the presented key was recorded:\n%s", text)
	}
}

// TestKnownServersDuplicateUnderTwoSpellings: two lines
// for one canonical host with different keys are an error naming both, and
// the same key under two spellings is harmless.
func TestKnownServersDuplicateUnderTwoSpellings(t *testing.T) {
	path := writeKnownServers(t, fmt.Sprintf("DB:7433 %x\ndb.:07433 %x\n", knownKeyA, knownKeyB))
	_, err := NewKnownServers(path)
	if err == nil {
		t.Fatal("two keys for one host under two spellings loaded without an error")
	}
	for _, want := range []string{"db:7433", Fingerprint(knownKeyA), Fingerprint(knownKeyB), path} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
	same := writeKnownServers(t, fmt.Sprintf("DB:7433 %x\ndb:7433 %x\n", knownKeyA, knownKeyA))
	if _, err := NewKnownServers(same); err != nil {
		t.Fatalf("the same key under two spellings was refused: %v", err)
	}
}

// TestKnownServersLoadSeparatorGrammar (BUG-0533): lines end at LF with one
// trailing CR stripped; only space and tab separate fields. A Unicode or
// control separator makes the line malformed, so it loads nothing.
func TestKnownServersLoadSeparatorGrammar(t *testing.T) {
	keyA := fmt.Sprintf("%x", knownKeyA)
	keyB := fmt.Sprintf("%x", knownKeyB)
	tests := []struct {
		name      string
		text      string
		wantKnown map[string]ed25519.PublicKey
		wantNot   []string
	}{
		{"CRLF line ending", "db:7433 " + keyA + "\r\n", map[string]ed25519.PublicKey{"db:7433": knownKeyA}, nil},
		{"tab separator", "db:7433\t" + keyA + "\tadded by hand\n", map[string]ed25519.PublicKey{"db:7433": knownKeyA}, nil},
		{"leading spaces", "   db:7433 " + keyA + "\n", map[string]ed25519.PublicKey{"db:7433": knownKeyA}, nil},
		{"no-break space is not a separator", "db:7433 " + keyA + "\n", nil, []string{"db:7433"}},
		{"NEL is not a separator", "db:7433\u0085" + keyA + "\n", nil, []string{"db:7433"}},
		{"form feed is not a separator", "db:7433\f" + keyA + "\n", nil, []string{"db:7433"}},
		{"vertical tab is not a separator", "db:7433\v" + keyA + "\n", nil, []string{"db:7433"}},
		{"line separator does not end a line", "a:7433 " + keyA + " b:7433 " + keyB + "\n", nil, []string{"a:7433", "b:7433"}},
		{"two CRs: only one is stripped", "db:7433 " + keyA + "\r\r\n", nil, []string{"db:7433"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			known := loadKnownServers(t, writeKnownServers(t, tc.text))
			for host, key := range tc.wantKnown {
				if !known.IsKnown(host, key) {
					t.Errorf("%q: %s not loaded", tc.text, host)
				}
			}
			for _, host := range tc.wantNot {
				if recorded, _ := known.IsKnownFingerprint(host); recorded {
					t.Errorf("%q: %s loaded from a malformed line", tc.text, host)
				}
			}
		})
	}
}

// TestKnownServersSaveKeepsOtherLines (BUG-0531): recording a first-seen
// server appends one line and keeps comments, blank lines, text after a key,
// malformed lines and their order exactly.
func TestKnownServersSaveKeepsOtherLines(t *testing.T) {
	original := fmt.Sprintf("# work servers\n\ndb1:7433 %x  added by hand\nnot a valid line\n", knownKeyA)
	path := writeKnownServers(t, original)
	known := loadKnownServers(t, path)
	if err := known.Trust("db2:7433", knownKeyB); err != nil {
		t.Fatalf("Trust: %v", err)
	}
	want := original + fmt.Sprintf("db2:7433 %x\n", knownKeyB)
	if got := readText(t, path); got != want {
		t.Fatalf("file after Trust:\n%q\nwant:\n%q", got, want)
	}
}

// TestKnownServersTwoProcessesKeepBothEntries (BUG-0531): two stores loaded
// from one file before either saves each record a different server; both
// lines survive.
func TestKnownServersTwoProcessesKeepBothEntries(t *testing.T) {
	path := knownServersTestPath(t)
	first := loadKnownServers(t, path)
	second := loadKnownServers(t, path)
	if err := first.Trust("a:7433", knownKeyA); err != nil {
		t.Fatalf("first Trust: %v", err)
	}
	if err := second.Trust("b:7433", knownKeyB); err != nil {
		t.Fatalf("second Trust: %v", err)
	}
	reloaded := loadKnownServers(t, path)
	if !reloaded.IsKnown("a:7433", knownKeyA) || !reloaded.IsKnown("b:7433", knownKeyB) {
		t.Fatalf("one process's entry was lost; file:\n%s", readText(t, path))
	}
}

// TestKnownServersSecondProcessComparesBeforeAppending (BUG-0531): a key
// another process recorded after this one loaded is compared before writing,
// so a different key is a refusal, not a second line.
func TestKnownServersSecondProcessComparesBeforeAppending(t *testing.T) {
	path := knownServersTestPath(t)
	first := loadKnownServers(t, path)
	second := loadKnownServers(t, path)
	if err := first.Trust("db:7433", knownKeyA); err != nil {
		t.Fatalf("first Trust: %v", err)
	}
	err := second.Trust("db:7433", knownKeyB)
	if err == nil {
		t.Fatalf("a second key for a host recorded meanwhile was written; file:\n%s", readText(t, path))
	}
	for _, want := range []string{Fingerprint(knownKeyA), Fingerprint(knownKeyB)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
	if !loadKnownServers(t, path).IsKnown("db:7433", knownKeyA) {
		t.Fatalf("the first key was overwritten; file:\n%s", readText(t, path))
	}
}

// TestKnownServersSaveIsAtomicUnderConcurrentRead (BUG-0531): what a reader
// is guaranteed while another process saves. The line being appended may be
// read cut short (a single write(2) is not atomic against a concurrent read),
// and the grammar makes any cut-short line malformed, so it is skipped. So a
// reader sees every entry saved before it started reading, and never a wrong
// key: every host it does load carries the key that host was given. Pre-fix,
// the truncate-and-rewrite save loses earlier entries. Run with -race.
func TestKnownServersSaveIsAtomicUnderConcurrentRead(t *testing.T) {
	path := knownServersTestPath(t)
	writer := loadKnownServers(t, path)
	const saves = 200
	keyOf := func(index int) ed25519.PublicKey {
		if index%2 == 0 {
			return knownKeyA
		}
		return knownKeyB
	}
	var saved atomic.Int64
	var group sync.WaitGroup
	group.Add(1)
	writerErrors := make(chan error, 1)
	go func() {
		defer group.Done()
		for index := range saves {
			if err := writer.Trust(fmt.Sprintf("host-%d:7433", index), keyOf(index)); err != nil {
				writerErrors <- err
				return
			}
			saved.Add(1)
		}
	}()
	lostReads, wrongKeys := 0, 0
	for saved.Load() < saves && len(writerErrors) == 0 {
		before := int(saved.Load())
		reader, err := NewKnownServers(path)
		if err != nil {
			t.Fatalf("NewKnownServers during a save: %v", err)
		}
		for index := range saves {
			host := fmt.Sprintf("host-%d:7433", index)
			recorded, fingerprint := reader.IsKnownFingerprint(host)
			switch {
			case recorded && fingerprint != Fingerprint(keyOf(index)):
				wrongKeys++
			case !recorded && index < before:
				lostReads++
			}
		}
	}
	group.Wait()
	close(writerErrors)
	if err := <-writerErrors; err != nil {
		t.Fatalf("Trust: %v", err)
	}
	if wrongKeys > 0 {
		t.Fatalf("%d entries were read with a key their host was never given: a cut-short line was read as an entry", wrongKeys)
	}
	if lostReads > 0 {
		t.Fatalf("%d entries saved before a read began were missing from it: an earlier entry was lost while the file was saved", lostReads)
	}
}

// TestKnownServersRefusesWritableFileOrDirectory (BUG-0532): a file or
// directory other users can write is refused, naming the path and the chmod
// that fixes it.
func TestKnownServersRefusesWritableFileOrDirectory(t *testing.T) {
	tests := []struct {
		name     string
		dirMode  os.FileMode
		fileMode os.FileMode
		wantFix  string
	}{
		{"world-writable directory", 0o777, 0o600, "chmod 700"},
		{"group-writable directory", 0o770, 0o600, "chmod 700"},
		{"world-writable file", 0o700, 0o666, "chmod 600"},
		{"group-writable file", 0o700, 0o620, "chmod 600"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := ownerOnlyDir(t)
			path := filepath.Join(dir, "known_servers")
			if err := os.WriteFile(path, fmt.Appendf(nil, "db:7433 %x\n", knownKeyA), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, tc.fileMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(dir, tc.dirMode); err != nil {
				t.Fatal(err)
			}
			_, err := NewKnownServers(path)
			if err == nil {
				t.Fatalf("dir %04o, file %04o: loaded without an error", tc.dirMode, tc.fileMode)
			}
			if !strings.Contains(err.Error(), tc.wantFix) {
				t.Errorf("error %q does not give the fix %q", err, tc.wantFix)
			}
			if !strings.Contains(err.Error(), dir) {
				t.Errorf("error %q does not name the path", err)
			}
		})
	}

	t.Run("owner-only modes still load and save", func(t *testing.T) {
		path := writeKnownServers(t, fmt.Sprintf("db:7433 %x\n", knownKeyA))
		known := loadKnownServers(t, path)
		if err := known.Trust("db2:7433", knownKeyB); err != nil {
			t.Fatalf("Trust with 0700/0600: %v", err)
		}
	})
	t.Run("a file others can read is accepted", func(t *testing.T) {
		path := writeKnownServers(t, fmt.Sprintf("db:7433 %x\n", knownKeyA))
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		loadKnownServers(t, path)
	})
}

// TestKnownServersRefusesSymlinkedFile (BUG-0532): the file itself must not
// be a link, so nothing is read through one and a save never writes to its
// target. A linked directory is accepted (container mounts, dotfile
// managers) and its target is what is checked.
func TestKnownServersRefusesSymlinkedFile(t *testing.T) {
	elsewhere := filepath.Join(ownerOnlyDir(t), "elsewhere")
	original := fmt.Sprintf("db:7433 %x\n", knownKeyA)
	if err := os.WriteFile(elsewhere, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	path := knownServersTestPath(t)
	if err := os.Symlink(elsewhere, path); err != nil {
		t.Fatal(err)
	}
	known, err := NewKnownServers(path)
	if err == nil {
		saveErr := known.Trust("db2:7433", knownKeyB)
		t.Fatalf("a symlinked known_servers loaded (Trust through it: %v); target now:\n%s", saveErr, readText(t, elsewhere))
	}
	if !strings.Contains(err.Error(), "symbolic link") || !strings.Contains(err.Error(), path) {
		t.Errorf("error %q does not say the path is a symbolic link", err)
	}
	if readText(t, elsewhere) != original {
		t.Fatal("the link's target was changed")
	}

	t.Run("a linked directory is accepted", func(t *testing.T) {
		realDir := ownerOnlyDir(t)
		linkDir := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(realDir, linkDir); err != nil {
			t.Fatal(err)
		}
		known := loadKnownServers(t, filepath.Join(linkDir, "known_servers"))
		if err := known.Trust("db:7433", knownKeyA); err != nil {
			t.Fatalf("Trust through a linked directory: %v", err)
		}
		if !loadKnownServers(t, filepath.Join(realDir, "known_servers")).IsKnown("db:7433", knownKeyA) {
			t.Fatal("the entry did not reach the linked directory's target")
		}
	})
	t.Run("a linked directory resolving to a writable one is refused", func(t *testing.T) {
		realDir := filepath.Join(t.TempDir(), "real")
		if err := os.Mkdir(realDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(realDir, 0o777); err != nil {
			t.Fatal(err)
		}
		linkDir := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(realDir, linkDir); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(realDir, "known_servers"), []byte(original), 0o600); err != nil {
			t.Fatal(err)
		}
		resolved, err := filepath.EvalSymlinks(realDir)
		if err != nil {
			t.Fatal(err)
		}
		_, err = NewKnownServers(filepath.Join(linkDir, "known_servers"))
		if err == nil || !strings.Contains(err.Error(), "chmod 700 "+resolved) {
			t.Fatalf("err = %v, want a refusal giving chmod 700 for the resolved directory %s", err, resolved)
		}
	})
}

// TestKnownServersRefusesForeignOwner (BUG-0532): a file owned by another
// user who is not root is refused with the chown that fixes it. Changing a
// file's owner needs root; the decision itself is covered without root by
// TestTrustPathRefusalTable.
func TestKnownServersRefusesForeignOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to give a file another owner; TestTrustPathRefusalTable covers the decision without a real uid")
	}
	path := writeKnownServers(t, fmt.Sprintf("db:7433 %x\n", knownKeyA))
	const otherUID = 54321
	if err := os.Lchown(path, otherUID, otherUID); err != nil {
		t.Fatalf("chown: %v", err)
	}
	_, err := NewKnownServers(path)
	if err == nil || !strings.Contains(err.Error(), "chown") {
		t.Fatalf("err = %v, want a refusal giving the chown", err)
	}
}
