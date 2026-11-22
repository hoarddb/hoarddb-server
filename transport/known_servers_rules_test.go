package transport

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
)

// Tests of the API the known_servers fix added:
// CanonicalServerAddress, TrustPathRefusal, Replace, Lookup and the lock.
// They do not compile against the pre-fix code; known_servers_test.go holds
// the tests that do, and fail there.

// TestCanonicalServerAddress: the canonical host form of
// clients/handbook/trust.md section 4.1.
func TestCanonicalServerAddress(t *testing.T) {
	tests := []struct {
		input, want, wantError string
	}{
		{input: "db.internal:7433", want: "db.internal:7433"},
		{input: "DB.Internal:7433", want: "db.internal:7433"},
		{input: "db.internal.:7433", want: "db.internal:7433"},
		{input: "db.internal", want: "db.internal:7433"},
		{input: "db.internal.", want: "db.internal:7433"},
		{input: "db:07433", want: "db:7433"},
		{input: "db:4434", want: "db:4434"},
		{input: "under_score-host:1", want: "under_score-host:1"},
		{input: "127.0.0.1:7433", want: "127.0.0.1:7433"},
		{input: "127.0.0.1", want: "127.0.0.1:7433"},
		{input: "localhost:7433", want: "localhost:7433"},
		{input: "[::1]:7433", want: "[::1]:7433"},
		{input: "[0:0:0:0:0:0:0:1]:7433", want: "[::1]:7433"},
		{input: "[::0001]", want: "[::1]:7433"},
		{input: "[2001:DB8:0:0:1:0:0:1]:7433", want: "[2001:db8::1:0:0:1]:7433"},
		{input: "[2001:db8:0:1:1:1:1:1]:7433", want: "[2001:db8:0:1:1:1:1:1]:7433"},
		{input: "[FE80::1%eth0]:7433", want: "[fe80::1%eth0]:7433"},
		{input: "[::ffff:127.0.0.1]:7433", want: "[::ffff:127.0.0.1]:7433"},
		{input: "", wantError: "empty"},
		{input: "::1:7433", wantError: "brackets"},
		{input: "[127.0.0.1]:7433", wantError: "IPv6"},
		{input: "[::1", wantError: "never closes"},
		{input: "[::1]7433", wantError: "after the bracketed address"},
		{input: "[fe80::1%a b]:7433", wantError: "zone"},
		{input: "db:0", wantError: "1 to 65535"},
		{input: "db:65536", wantError: "1 to 65535"},
		{input: "db:+7433", wantError: "1 to 65535"},
		{input: "db:007433", wantError: "1 to 65535"},
		{input: "db:", wantError: "no port"},
		{input: ":7433", wantError: "no host"},
		{input: "127.000.000.001:7433", wantError: "not an IPv4 address"},
		{input: "1.2.3:7433", wantError: "not an IPv4 address"},
		{input: "db..internal:7433", wantError: "empty label"},
		{input: ".db:7433", wantError: "empty label"},
		{input: "db.internal..:7433", wantError: "empty label"},
		{input: "dé.internal:7433", wantError: "may only hold"},
		{input: "\u212aey.example:7433", wantError: "may only hold"},      // U+212A KELVIN SIGN lowercases to ASCII k
		{input: "\u0130nternal.example:7433", wantError: "may only hold"}, // U+0130 lowercases to ASCII i
		{input: "[::1]:", wantError: "no port"},
		{input: "[fe80::1%eth0]:", wantError: "no port"},
		{input: "db x:7433", wantError: "may only hold"},
	}
	for _, tc := range tests {
		got, err := CanonicalServerAddress(tc.input)
		if tc.wantError != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Errorf("CanonicalServerAddress(%q) = %q, %v; want an error containing %q", tc.input, got, err, tc.wantError)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("CanonicalServerAddress(%q) = %q, %v; want %q", tc.input, got, err, tc.want)
		}
	}
}

// TestTrustPathRefusalTable (BUG-0532): the decision, with no real uid, so
// the foreign-owner rows run without root.
func TestTrustPathRefusalTable(t *testing.T) {
	const self, other = 1000, 2000
	tests := []struct {
		name      string
		facts     TrustPathFacts
		wantFixes []string // empty: accepted
	}{
		{"file 0600 own", TrustPathFacts{Perm: 0o600, OwnerUID: self}, nil},
		{"file 0644 own", TrustPathFacts{Perm: 0o644, OwnerUID: self}, nil},
		{"file 0400 root", TrustPathFacts{Perm: 0o400, OwnerUID: 0}, nil},
		{"file 0620", TrustPathFacts{Perm: 0o620, OwnerUID: self}, []string{"chmod 600 F"}},
		{"file 0666", TrustPathFacts{Perm: 0o666, OwnerUID: self}, []string{"chmod 600 F"}},
		{"file 0602", TrustPathFacts{Perm: 0o602, OwnerUID: self}, []string{"chmod 600 F"}},
		{"file other owner", TrustPathFacts{Perm: 0o600, OwnerUID: other}, []string{"chown 1000 F"}},
		{"file other owner and 0666", TrustPathFacts{Perm: 0o666, OwnerUID: other}, []string{"chown 1000 F", "chmod 600 F"}},
		{"file symlink", TrustPathFacts{Symlink: true, Perm: 0o777, OwnerUID: self}, []string{"symbolic link", "readlink"}},
		{"file fifo", TrustPathFacts{Type: fs.ModeNamedPipe, Perm: 0o600, OwnerUID: self}, []string{"not a regular file", "mv F F.bak"}},
		{"dir 0700 own", TrustPathFacts{Directory: true, Type: fs.ModeDir, Perm: 0o700, OwnerUID: self}, nil},
		{"dir 0755 root", TrustPathFacts{Directory: true, Type: fs.ModeDir, Perm: 0o755, OwnerUID: 0}, nil},
		{"dir reached through a link", TrustPathFacts{Directory: true, Symlink: true, Type: fs.ModeDir, Perm: 0o700, OwnerUID: self}, nil},
		{"dir 0770", TrustPathFacts{Directory: true, Type: fs.ModeDir, Perm: 0o770, OwnerUID: self}, []string{"chmod 700 F"}},
		{"dir 0777", TrustPathFacts{Directory: true, Type: fs.ModeDir, Perm: 0o777, OwnerUID: self}, []string{"chmod 700 F"}},
		{"dir 1777 sticky", TrustPathFacts{Directory: true, Type: fs.ModeDir, Perm: 0o777, OwnerUID: 0}, []string{"chmod 700 F"}},
		{"dir other owner", TrustPathFacts{Directory: true, Type: fs.ModeDir, Perm: 0o700, OwnerUID: other}, []string{"chown 1000 F"}},
		{"dir is a file", TrustPathFacts{Directory: true, Type: 0, Perm: 0o700, OwnerUID: self}, []string{"not a directory"}},
	}
	for _, tc := range tests {
		tc.facts.Path = "F"
		tc.facts.CurrentUID = self
		err := TrustPathRefusal(tc.facts)
		if len(tc.wantFixes) == 0 {
			if err != nil {
				t.Errorf("%s: refused: %v", tc.name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		for _, fix := range tc.wantFixes {
			if !strings.Contains(err.Error(), fix) {
				t.Errorf("%s: %q does not contain %q", tc.name, err, fix)
			}
		}
	}
}

// TestKnownServersReplaceKeepsOtherLinesVerbatim (BUG-0531): replacing a key
// rewrites only that host's lines and keeps every other line, malformed ones
// and their CRs included, in order.
func TestKnownServersReplaceKeepsOtherLinesVerbatim(t *testing.T) {
	original := fmt.Sprintf("# work\r\nDB:7433 %x old\nnot a valid line\n\ndb.:7433 %x\nother:7433 %x", knownKeyA, knownKeyA, knownKeyB)
	path := writeKnownServers(t, original)
	known := loadKnownServers(t, path)
	if err := known.Replace("db:7433", knownKeyB); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	want := fmt.Sprintf("# work\r\ndb:7433 %x\nnot a valid line\n\nother:7433 %x\n", knownKeyB, knownKeyB)
	if got := readText(t, path); got != want {
		t.Fatalf("file after Replace:\n%q\nwant:\n%q", got, want)
	}
	if !known.IsKnown("DB:7433", knownKeyB) {
		t.Fatal("the store does not know the replaced key")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("rewritten file is %04o, want 0600", info.Mode().Perm())
	}
	leftovers, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range leftovers {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Errorf("temporary file %s left behind", entry.Name())
		}
	}
}

// TestKnownServersAppendAfterUnterminatedLine (BUG-0531): a last line with no
// LF is completed before the new line, never joined to it.
func TestKnownServersAppendAfterUnterminatedLine(t *testing.T) {
	original := fmt.Sprintf("a:7433 %x", knownKeyA)
	path := writeKnownServers(t, original)
	if err := loadKnownServers(t, path).Trust("b:7433", knownKeyB); err != nil {
		t.Fatalf("Trust: %v", err)
	}
	want := original + fmt.Sprintf("\nb:7433 %x\n", knownKeyB)
	if got := readText(t, path); got != want {
		t.Fatalf("file:\n%q\nwant:\n%q", got, want)
	}
}

// TestKnownServersLockWaitIsBounded (BUG-0531): a save waits for another
// holder of the lock only so long, then names the lock file.
func TestKnownServersLockWaitIsBounded(t *testing.T) {
	previous := knownServersLockWait
	knownServersLockWait = 100 * time.Millisecond
	t.Cleanup(func() { knownServersLockWait = previous })

	path := knownServersTestPath(t)
	holder, err := lockKnownServers(path)
	if err != nil {
		t.Fatalf("taking the lock: %v", err)
	}
	t.Cleanup(func() { _ = holder.Close() })
	err = loadKnownServers(t, path).Trust("db:7433", knownKeyA)
	if err == nil || !strings.Contains(err.Error(), path+".lock") {
		t.Fatalf("Trust while another holds the lock: %v; want a refusal naming %s.lock", err, path)
	}
}

// TestKnownServersLockDoesNotStarveWaiters (BUG-0537): sixteen savers each
// recording twenty new servers against one file, with the wait bound set well
// above the time one save holds the lock. Every save must return nil. The save
// lock is queued in the kernel (a blocking flock), so a saver that starts
// waiting is served when the holder releases; a lock retried on a 20 ms timer
// instead lets a saver that just released its own save re-take the lock first
// and starve the others until the bound expires.
func TestKnownServersLockDoesNotStarveWaiters(t *testing.T) {
	previous := knownServersLockWait
	knownServersLockWait = 500 * time.Millisecond
	t.Cleanup(func() { knownServersLockWait = previous })

	const savers, saves = 16, 20
	path := knownServersTestPath(t)
	// Each saver has its own KnownServers, so nothing but the file lock
	// serialises them: flock locks per open file description.
	knowns := make([]*KnownServers, savers)
	for i := range knowns {
		knowns[i] = loadKnownServers(t, path)
	}
	starved := make([]error, savers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < savers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < saves; j++ {
				host := fmt.Sprintf("saver%d-%d:7433", i, j)
				if err := knowns[i].Trust(host, knownKeyA); err != nil {
					starved[i] = err
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range starved {
		if err != nil {
			t.Errorf("saver %d timed out with saves still to record: %v", i, err)
		}
	}
}

// TestKnownServersLockGrantsInArrivalOrder (BUG-0537): with the lock held,
// three saves start waiting in a known order, staggered far apart against any
// poll interval; on each release the first to wait must be granted first, then
// the second. A lock retried on a timer grants whichever save polls first after
// a release, so over fifty rounds of three waiters it cannot keep the order by
// chance (1 in 6 per round). No seam: the stagger is the only way to fix the
// order in which calls enqueue without one.
func TestKnownServersLockGrantsInArrivalOrder(t *testing.T) {
	previous := knownServersLockWait
	knownServersLockWait = 10 * time.Second
	t.Cleanup(func() { knownServersLockWait = previous })

	const rounds, waiters, stagger = 50, 3, 30 * time.Millisecond
	path := knownServersTestPath(t)
	for round := 0; round < rounds; round++ {
		holder, err := lockKnownServers(path)
		if err != nil {
			t.Fatalf("round %d: taking the lock: %v", round, err)
		}
		var (
			orderMu sync.Mutex
			granted []int
			wg      sync.WaitGroup
		)
		for waiter := 0; waiter < waiters; waiter++ {
			wg.Add(1)
			go func(waiter int) {
				defer wg.Done()
				lockFile, err := lockKnownServers(path)
				if err != nil {
					t.Errorf("round %d: waiter %d: %v", round, waiter, err)
					return
				}
				orderMu.Lock()
				granted = append(granted, waiter)
				orderMu.Unlock()
				if err := lockFile.Close(); err != nil {
					t.Errorf("round %d: waiter %d releasing: %v", round, waiter, err)
				}
			}(waiter)
			time.Sleep(stagger)
		}
		if err := holder.Close(); err != nil {
			t.Fatalf("round %d: releasing the lock: %v", round, err)
		}
		wg.Wait()
		for position, waiter := range granted {
			if waiter != position {
				t.Fatalf("round %d: the lock was granted in the order %v, want [0 1 2]: a save that waited longer was passed over", round, granted)
			}
		}
	}
}

// TestKnownServersTimedOutWaiterLeavesNoLock (BUG-0537): a save that gives up
// waiting for the lock must leave it free. The fix waits in the kernel, so on
// timeout its waiter may still be queued; when it is later granted the lock it
// must release it at once.
func TestKnownServersTimedOutWaiterLeavesNoLock(t *testing.T) {
	previous := knownServersLockWait
	knownServersLockWait = 100 * time.Millisecond
	t.Cleanup(func() { knownServersLockWait = previous })

	path := knownServersTestPath(t)
	holder, err := lockKnownServers(path)
	if err != nil {
		t.Fatalf("taking the lock: %v", err)
	}
	if err := loadKnownServers(t, path).Trust("db:7433", knownKeyA); err == nil || !strings.Contains(err.Error(), path+".lock") {
		t.Fatalf("Trust while another holds the lock: %v; want a refusal naming %s.lock", err, path)
	}
	if err := holder.Close(); err != nil {
		t.Fatalf("releasing the lock: %v", err)
	}
	knownServersLockWait = 5 * time.Second
	if err := loadKnownServers(t, path).Trust("db:7433", knownKeyA); err != nil {
		t.Fatalf("a save after the timed-out waiter released: %v", err)
	}
}

// TestKnownServersTimedOutSavesDoNotLeak (BUG-0537): fifty saves that all give
// up on a holder that never releases must not each leave a goroutine, a file
// descriptor and an OS thread blocked in flock behind them. The waiters of one
// lock file are shared, so the count stays at the baseline plus a small
// constant, and drains once the holder lets go.
func TestKnownServersTimedOutSavesDoNotLeak(t *testing.T) {
	if _, err := os.ReadDir("/proc/self/fd"); err != nil {
		t.Skipf("counting open files needs /proc/self/fd: %v", err)
	}
	previous := knownServersLockWait
	knownServersLockWait = 20 * time.Millisecond
	t.Cleanup(func() { knownServersLockWait = previous })

	path := knownServersTestPath(t)
	holder, err := lockKnownServers(path)
	if err != nil {
		t.Fatalf("taking the lock: %v", err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			_ = holder.Close()
		}
	})
	baselineGoroutines, baselineFiles := runtime.NumGoroutine(), openFileCount(t)

	const timedOutSaves, slack = 50, 3
	known := loadKnownServers(t, path)
	for i := 0; i < timedOutSaves; i++ {
		err := known.Trust(fmt.Sprintf("db%d:7433", i), knownKeyA)
		if err == nil || !strings.Contains(err.Error(), "fuser "+path+".lock") {
			t.Fatalf("save %d while another holds the lock: %v; want a refusal naming the lock and fuser", i, err)
		}
	}
	if got := runtime.NumGoroutine(); got > baselineGoroutines+slack {
		t.Errorf("%d goroutines after %d timed-out saves, baseline %d: each save left a waiter", got, timedOutSaves, baselineGoroutines)
	}
	if got := openFileCount(t); got > baselineFiles+slack {
		t.Errorf("%d open files after %d timed-out saves, baseline %d: each save left a descriptor", got, timedOutSaves, baselineFiles)
	}

	if err := holder.Close(); err != nil {
		t.Fatalf("releasing the lock: %v", err)
	}
	released = true
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baselineGoroutines || openFileCount(t) > baselineFiles-1 {
		if time.Now().After(deadline) {
			t.Fatalf("after the holder released: %d goroutines (baseline %d), %d open files (baseline %d, less the holder's); the waiter did not exit",
				runtime.NumGoroutine(), baselineGoroutines, openFileCount(t), baselineFiles)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// openFileCount is the number of descriptors this process has open.
func openFileCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("listing /proc/self/fd: %v", err)
	}
	return len(entries)
}

// TestKnownServersLookup: the three answers, under any spelling.
func TestKnownServersLookup(t *testing.T) {
	known := loadKnownServers(t, writeKnownServers(t, fmt.Sprintf("[::1]:7433 %x\n", knownKeyA)))
	tests := []struct {
		host string
		key  []byte
		want KnownServerMatch
	}{
		{"[0::1]", knownKeyA, KnownServerKnown},
		{"[::1]:7433", knownKeyB, KnownServerChanged},
		{"[::2]:7433", knownKeyA, KnownServerUnknown},
	}
	for _, tc := range tests {
		got, _, err := known.Lookup(tc.host, tc.key)
		if err != nil || got != tc.want {
			t.Errorf("Lookup(%s) = %v, %v; want %v", tc.host, got, err, tc.want)
		}
	}
	if _, _, err := known.Lookup("::1:7433", knownKeyA); err == nil {
		t.Error("Lookup accepted an unbracketed IPv6 address")
	}
}

// TestPeerLearnReplacesRotatedKey: a peer key verified by the configured
// seed_fingerprints replaces a stale learned one, which is how an operator
// rotates a peer's certificate (the "configuration" user-docs page). Trust alone
// would refuse it as a changed key.
func TestPeerLearnReplacesRotatedKey(t *testing.T) {
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	t.Cleanup(func() { slog.SetDefault(previous) })

	rotated := generatedCertDER(t)
	host := "127.0.0.1:4434"
	path := writeKnownServers(t, fmt.Sprintf("%s %x\n", host, knownKeyA))
	learned := loadKnownServers(t, path)
	pinned, err := NewFingerprintSet([]string{Fingerprint(rotated.pub)}, learned)
	if err != nil {
		t.Fatalf("NewFingerprintSet: %v", err)
	}
	if err := pinned.Verify(host)([][]byte{rotated.der}, nil); err != nil {
		t.Fatalf("the configured fingerprint did not verify: %v", err)
	}
	if !loadKnownServers(t, path).IsKnown(host, rotated.pub) {
		t.Fatalf("the rotated key was not learned; file:\n%s", readText(t, path))
	}
}

// TestChangedServerKeyErrorIsTyped: the CLI and a driver can tell a changed
// key from any other failure without reading the text.
func TestChangedServerKeyErrorIsTyped(t *testing.T) {
	path := writeKnownServers(t, fmt.Sprintf("db:7433 %x\n", knownKeyA))
	err := loadKnownServers(t, path).Trust("DB.:7433", knownKeyB)
	var changed *ChangedServerKeyError
	if !errors.As(err, &changed) {
		t.Fatalf("Trust of a second key = %v, want a *ChangedServerKeyError", err)
	}
	if changed.Host != "db:7433" || !strings.Contains(err.Error(), "recorded as db:7433") {
		t.Errorf("changed-key error %q does not name the recorded host", err)
	}
}

// TestCanonicalServerAddressRefusesNonASCIIAndEmptyPorts is a differential
// sweep over the two families the first review missed. Every rune outside
// ASCII whose Unicode lower case is ASCII (U+0130, U+212A) would canonicalise
// to a different, ASCII, host if case folding ran before the ASCII check; each
// must be refused, in any position of a name. And an empty port after "]:"
// must be refused exactly as "db:" is, for every bracketed form.
func TestCanonicalServerAddressRefusesNonASCIIAndEmptyPorts(t *testing.T) {
	folding := 0
	for r := rune(0x80); r <= unicode.MaxRune; r++ {
		if unicode.ToLower(r) >= 0x80 {
			continue
		}
		for _, host := range []string{string(r) + "db.example", "db" + string(r) + ".example", "db.example" + string(r)} {
			if canonical, err := CanonicalServerAddress(host + ":7433"); err == nil {
				t.Errorf("CanonicalServerAddress(%q, U+%04X) = %q; a name with a non-ASCII rune must be refused", host, r, canonical)
			}
		}
		folding++
	}
	if folding == 0 {
		t.Fatal("the sweep found no case-folding rune; the test is not testing anything")
	}
	if _, err := CanonicalServerAddress("db:"); err == nil {
		t.Fatal("db: was accepted; the baseline of the empty-port family is a refusal")
	}
	for _, bracketed := range []string{"[::1]:", "[0:0:0:0:0:0:0:1]:", "[fe80::1%eth0]:", "[2001:db8::1]:", "[::ffff:127.0.0.1]:"} {
		canonical, err := CanonicalServerAddress(bracketed)
		if err == nil {
			t.Errorf("CanonicalServerAddress(%q) = %q; an empty port after ']:' must be refused like 'db:'", bracketed, canonical)
			continue
		}
		if !strings.Contains(err.Error(), "no port") {
			t.Errorf("CanonicalServerAddress(%q): %v; want the 'no port' explanation", bracketed, err)
		}
	}
}

// TestKnownServersLineWithCaseFoldingRuneIsMalformed: a file line whose host
// holds U+212A or U+0130 loads nothing, rather than an entry for the ASCII
// host it would fold to.
func TestKnownServersLineWithCaseFoldingRuneIsMalformed(t *testing.T) {
	text := fmt.Sprintf("\u212aey.example:7433 %x\n\u0130nternal.example:7433 %x\n", knownKeyA, knownKeyB)
	known := loadKnownServers(t, writeKnownServers(t, text))
	if entries := known.Entries(); len(entries) != 0 {
		t.Fatalf("lines with case-folding runes loaded %v; they must be malformed", entries)
	}
}

// TestKnownServersRefusesSymlinkedLockFile: a link at known_servers.lock is
// refused with a message that names the lock file, not the trust file, and
// says how to fix it.
func TestKnownServersRefusesSymlinkedLockFile(t *testing.T) {
	path := knownServersTestPath(t)
	target := filepath.Join(ownerOnlyDir(t), "elsewhere")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path+".lock"); err != nil {
		t.Fatal(err)
	}
	err := loadKnownServers(t, path).Trust("db:7433", knownKeyA)
	if err == nil {
		t.Fatal("a symlinked lock file was used")
	}
	for _, want := range []string{"lock file " + path + ".lock", "symbolic link", "rm " + path + ".lock"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "known_servers file") {
		t.Errorf("error %q calls the lock file the known_servers file", err)
	}
}

// TestKnownServersSaveSweepsStaleTemporaryFiles: a Replace killed between
// creating its temporary file and the rename leaves a .known_servers-*.tmp
// behind. The next save, holding the lock, removes those older than a
// minute; a fresh one (a live process could own it) and any other file stay.
func TestKnownServersSaveSweepsStaleTemporaryFiles(t *testing.T) {
	path := knownServersTestPath(t)
	dir := filepath.Dir(path)
	stale := time.Now().Add(-2 * time.Minute)
	tests := []struct {
		name     string
		old      bool
		survives bool
	}{
		{".known_servers-123.tmp", true, false},
		{".known_servers-456.tmp", false, true},
		{"notes.tmp", true, true},
		{".known_servers-789.other", true, true},
	}
	for _, tc := range tests {
		target := filepath.Join(dir, tc.name)
		if err := os.WriteFile(target, []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
		if tc.old {
			if err := os.Chtimes(target, stale, stale); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := loadKnownServers(t, path).Trust("db:7433", knownKeyA); err != nil {
		t.Fatalf("Trust: %v", err)
	}
	for _, tc := range tests {
		_, err := os.Lstat(filepath.Join(dir, tc.name))
		if exists := err == nil; exists != tc.survives {
			t.Errorf("%s: exists=%v after a save, want %v", tc.name, exists, tc.survives)
		}
	}
}

// TestKnownServersEveryPrefixOfAnAppendIsOldOrNew (BUG-0531): a reader racing
// an append can read any prefix of the appended line. For every prefix, the
// file must load to the state before the append or to that state plus the
// new entry, never to a different entry. This is the guarantee the line
// grammar gives: a prefix of "host:port key" lacks the key or has a key
// shorter than 64 hex characters, so it is malformed.
func TestKnownServersEveryPrefixOfAnAppendIsOldOrNew(t *testing.T) {
	tests := []struct {
		name, before, host string
	}{
		{"after a terminated line", fmt.Sprintf("# servers\na:7433 %x\n", knownKeyA), "b:7433"},
		{"after a line with no LF", fmt.Sprintf("a:7433 %x", knownKeyA), "b:7433"},
		{"an IPv6 host", fmt.Sprintf("a:7433 %x\n", knownKeyA), "[2001:db8::1]:7433"},
		{"into an empty file", "", "b:7433"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeKnownServers(t, tc.before)
			if err := loadKnownServers(t, path).Trust(tc.host, knownKeyB); err != nil {
				t.Fatalf("Trust: %v", err)
			}
			after := readText(t, path)
			if !strings.HasPrefix(after, tc.before) {
				t.Fatalf("the save changed the existing text:\n%q\nto\n%q", tc.before, after)
			}
			oldEntries := loadKnownServers(t, writeKnownServers(t, tc.before)).Entries()
			for cut := len(tc.before); cut <= len(after); cut++ {
				entries := loadKnownServers(t, writeKnownServers(t, after[:cut])).Entries()
				newEntry, hasNew := entries[tc.host]
				delete(entries, tc.host)
				if len(entries) != len(oldEntries) {
					t.Fatalf("prefix %q loads %v; want the old entries %v plus at most %s", after[:cut], entries, oldEntries, tc.host)
				}
				for host, key := range oldEntries {
					if !entries[host].Equal(key) {
						t.Fatalf("prefix %q changed the entry for %s", after[:cut], host)
					}
				}
				if hasNew && !newEntry.Equal(knownKeyB) {
					t.Fatalf("prefix %q loads %s with a key it was never given", after[:cut], tc.host)
				}
			}
		})
	}
}
