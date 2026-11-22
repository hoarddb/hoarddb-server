package transport

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The known_servers trust store. Its rules are a contract every driver
// follows (clients/handbook/trust.md section 4) and are pinned as data in
// clients/testdata/trust-vectors/v1.json, generated from this file:
//
//   - every host is looked up and recorded in one canonical form
//     (CanonicalServerAddress);
//   - lines end at LF, one trailing CR is stripped, and fields are separated by
//     runs of space and tab only (parseKnownServersText);
//   - the file and its directory must belong to the user or root and must not
//     be group- or world-writable, and the file must not be a link
//     (TrustPathRefusal);
//   - a first-seen server is appended under a file lock after re-reading the
//     file (Trust), and a key is replaced through a temporary file and a
//     rename (Replace), so no writer loses another's line. A reader may see
//     the line being appended cut short; the line grammar makes it malformed,
//     so it is skipped, and the reader sees every entry saved before that save
//     and never a wrong key. A Replace is never seen partly written.

// DefaultDriverPort is the port a server address without one means: the
// TCP driver port (trust.md section 6). The canonical form writes it out.
const DefaultDriverPort = 7433

// knownServersLockWait bounds how long a save waits for another process's
// save. A save holds the lock for one read and one write, so a wait this long
// means a process is stuck, not busy. A variable only so a test can shorten it.
var knownServersLockWait = 5 * time.Second

// errSymbolicLink is what openNoFollow wraps when the last component of the
// path is a link; each caller says which of the store's files it was.
var errSymbolicLink = errors.New("is a symbolic link")

// KnownServers manages the set of trusted server public keys. Lookups use
// the entries read when the store was opened and updated by its own saves;
// every save re-reads the file under the lock first, so another process's
// line is never lost.
type KnownServers struct {
	entries map[string]ed25519.PublicKey // canonical host:port -> pubkey
	path    string
	mu      sync.RWMutex
}

// NewKnownServers opens the store backed by the file at path. A missing file
// is an empty store. The file and its directory are checked first
// (TrustPathRefusal), and a file recording two keys for one host is refused.
func NewKnownServers(path string) (*KnownServers, error) {
	content, _, err := readKnownServersFile(path)
	if err != nil {
		return nil, err
	}
	entries, err := knownServersEntries(parseKnownServersText(string(content)), path)
	if err != nil {
		return nil, err
	}
	return &KnownServers{entries: entries, path: path}, nil
}

// Path returns the file the store reads and writes, so a refusal can name the
// file the developer has to edit.
func (ks *KnownServers) Path() string {
	return ks.path
}

// Entries returns a copy of the recorded keys by canonical host:port.
func (ks *KnownServers) Entries() map[string]ed25519.PublicKey {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	entries := make(map[string]ed25519.PublicKey, len(ks.entries))
	for host, key := range ks.entries {
		entries[host] = key
	}
	return entries
}

// KnownServerMatch is what the store knows about a host and a presented key.
type KnownServerMatch int

const (
	// KnownServerUnknown: no key is recorded for the host.
	KnownServerUnknown KnownServerMatch = iota
	// KnownServerKnown: the presented key is the recorded one.
	KnownServerKnown
	// KnownServerChanged: a different key is recorded for the host.
	KnownServerChanged
)

// Lookup canonicalises host and compares pub with the key recorded for it.
// The recorded key is returned for KnownServerKnown and KnownServerChanged.
func (ks *KnownServers) Lookup(host string, pub ed25519.PublicKey) (KnownServerMatch, ed25519.PublicKey, error) {
	canonical, err := CanonicalServerAddress(host)
	if err != nil {
		return KnownServerUnknown, nil, err
	}
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	recorded, ok := ks.entries[canonical]
	switch {
	case !ok:
		return KnownServerUnknown, nil, nil
	case recorded.Equal(pub):
		return KnownServerKnown, recorded, nil
	default:
		return KnownServerChanged, recorded, nil
	}
}

// IsKnown returns true if the host has a recorded key and it matches. An
// address that does not canonicalise is never known.
func (ks *KnownServers) IsKnown(host string, pub ed25519.PublicKey) bool {
	match, _, err := ks.Lookup(host, pub)
	return err == nil && match == KnownServerKnown
}

// IsKnownFingerprint returns true if the host has any recorded key, and
// returns its fingerprint for display.
func (ks *KnownServers) IsKnownFingerprint(host string) (bool, string) {
	canonical, err := CanonicalServerAddress(host)
	if err != nil {
		return false, ""
	}
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	existing, ok := ks.entries[canonical]
	if !ok {
		return false, ""
	}
	return true, Fingerprint(existing)
}

// ChangedServerKeyError is the refusal for a server whose key differs from
// the one recorded for its canonical address. It is never re-prompted.
type ChangedServerKeyError struct {
	// Dialled is the address as the caller wrote it; Host is its canonical
	// form, the one written in the file.
	Dialled, Host       string
	Path                string
	Recorded, Presented ed25519.PublicKey
}

func (e *ChangedServerKeyError) Error() string {
	recordedAs := ""
	if e.Dialled != e.Host {
		recordedAs = fmt.Sprintf(" (recorded as %s)", e.Host)
	}
	return fmt.Sprintf(
		"WARNING: server key for %s%s has changed!\n"+
			"  old fingerprint: %s\n"+
			"  new fingerprint: %s\n"+
			"  This could indicate a man-in-the-middle attack.\n"+
			"  If you replaced the server on purpose, delete the line for %s from %s and connect again",
		e.Dialled, recordedAs, Fingerprint(e.Recorded), Fingerprint(e.Presented), e.Host, e.Path)
}

// Trust records a first-seen server: under the file lock it re-reads the
// file, and appends one line unless the host is already recorded. A key
// another process recorded for the host meanwhile is compared first, and a
// different one is a *ChangedServerKeyError, never a second line.
func (ks *KnownServers) Trust(host string, pub ed25519.PublicKey) (err error) {
	canonical, err := CanonicalServerAddress(host)
	if err != nil {
		return fmt.Errorf("recording server %s in %s: %w", host, ks.path, err)
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()
	unlock, content, existed, err := ks.lockAndRead()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, unlock()) }()

	entries, err := knownServersEntries(parseKnownServersText(string(content)), ks.path)
	if err != nil {
		return err
	}
	ks.entries = entries
	if recorded, ok := entries[canonical]; ok {
		if recorded.Equal(pub) {
			return nil
		}
		return &ChangedServerKeyError{Dialled: host, Host: canonical, Path: ks.path, Recorded: recorded, Presented: pub}
	}
	var line []byte
	if len(content) > 0 && content[len(content)-1] != '\n' {
		line = append(line, '\n')
	}
	line = fmt.Appendf(line, "%s %x\n", canonical, []byte(pub))
	if err := appendKnownServersLine(ks.path, line, existed); err != nil {
		return err
	}
	ks.entries[canonical] = pub
	return nil
}

// Replace records pub as the key for host in place of any recorded one: the
// explicit "accept the new key" action. Under the file lock it re-reads the
// file and writes a new one through a temporary file and a rename, keeping
// every other line verbatim.
func (ks *KnownServers) Replace(host string, pub ed25519.PublicKey) (err error) {
	canonical, err := CanonicalServerAddress(host)
	if err != nil {
		return fmt.Errorf("replacing the key of server %s in %s: %w", host, ks.path, err)
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()
	unlock, content, _, err := ks.lockAndRead()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, unlock()) }()

	lines := parseKnownServersText(string(content))
	replacement := fmt.Sprintf("%s %x", canonical, []byte(pub))
	var rewritten bytes.Buffer
	replaced := false
	for _, line := range lines {
		if line.host == canonical {
			if !replaced {
				rewritten.WriteString(replacement + "\n")
				replaced = true
			}
			continue
		}
		rewritten.WriteString(line.raw + "\n")
	}
	if !replaced {
		rewritten.WriteString(replacement + "\n")
	}
	// The rewritten text must load: a duplicate for another host is the
	// file's own conflict, and saving it would hide that from no one, but a
	// refusal now names it while the person is here to fix it.
	entries, err := knownServersEntries(parseKnownServersText(rewritten.String()), ks.path)
	if err != nil {
		return err
	}
	if err := replaceKnownServersFile(ks.path, rewritten.Bytes()); err != nil {
		return err
	}
	ks.entries = entries
	return nil
}

// lockAndRead makes sure the directory exists and is safe, takes the
// exclusive lock, and reads the file under it. The returned function
// releases the lock.
func (ks *KnownServers) lockAndRead() (func() error, []byte, bool, error) {
	if err := ensureKnownServersDir(ks.path); err != nil {
		return nil, nil, false, err
	}
	lockFile, err := lockKnownServers(ks.path)
	if err != nil {
		return nil, nil, false, err
	}
	unlock := func() error {
		if err := lockFile.Close(); err != nil {
			return fmt.Errorf("releasing the lock %s: %w", lockFile.Name(), err)
		}
		return nil
	}
	content, existed, err := readKnownServersFile(ks.path)
	if err == nil {
		err = sweepStaleTemporaryFiles(ks.path)
	}
	if err != nil {
		return nil, nil, false, errors.Join(err, unlock())
	}
	return unlock, content, existed, nil
}

// staleTemporaryAge is how old a leftover temporary file must be before a
// save removes it. Only a Replace creates one, and it holds the lock until
// the rename, so under the lock no live process can own one; the age is a
// second guard, not the safety argument.
const staleTemporaryAge = time.Minute

// sweepStaleTemporaryFiles removes the .known_servers-*.tmp files a Replace
// killed before its rename left behind: regular files, owned by this user,
// older than staleTemporaryAge. It runs only with the lock held.
func sweepStaleTemporaryFiles(path string) error {
	dir := filepath.Dir(path)
	leftovers, err := filepath.Glob(filepath.Join(dir, ".known_servers-*.tmp"))
	if err != nil {
		return fmt.Errorf("listing leftover temporary files in %s: %w", dir, err)
	}
	for _, leftover := range leftovers {
		info, err := os.Lstat(leftover)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("checking leftover temporary file %s: %w", leftover, err)
		}
		if !info.Mode().IsRegular() || time.Since(info.ModTime()) < staleTemporaryAge {
			continue
		}
		facts, err := trustPathFacts(leftover, info)
		if err != nil {
			return err
		}
		if facts.OwnerUID != facts.CurrentUID {
			continue
		}
		if err := os.Remove(leftover); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("removing leftover temporary file %s: %w", leftover, err)
		}
	}
	return nil
}

// ---- the canonical host form (trust.md section 4.1) ------------------------

// CanonicalServerAddress returns the one form of a server address that
// known_servers records and looks up: a registered name lowercased with one
// trailing dot stripped, an IPv4 address in dotted decimal, an IPv6 address
// in net/netip's compressed form in brackets, and the port always written
// (DefaultDriverPort when the address has none) without leading zeros. No
// name is resolved, so 127.0.0.1 and localhost stay different names.
func CanonicalServerAddress(address string) (string, error) {
	host, portText, err := splitServerAddress(address)
	if err != nil {
		return "", err
	}
	port, err := canonicalPort(address, portText)
	if err != nil {
		return "", err
	}
	canonicalHost, err := canonicalServerHost(address, host)
	if err != nil {
		return "", err
	}
	return canonicalHost + ":" + port, nil
}

// splitServerAddress separates host and port. portText is empty when the
// address has no port.
func splitServerAddress(address string) (host, portText string, err error) {
	if address == "" {
		return "", "", errors.New("the server address is empty; write it as host:port, for example db.internal:7433")
	}
	if address[0] == '[' {
		end := strings.IndexByte(address, ']')
		if end < 0 {
			return "", "", fmt.Errorf("server address %q opens a bracket but never closes it; write an IPv6 address as [::1]:7433", address)
		}
		rest := address[end+1:]
		switch {
		case rest == "":
			return address[:end+1], "", nil
		case rest == ":":
			return "", "", fmt.Errorf("server address %q ends with ':' but has no port; write [address]:port, for example [::1]:7433, or leave out the ':' for the default port", address)
		case rest[0] == ':':
			return address[:end+1], rest[1:], nil
		default:
			return "", "", fmt.Errorf("server address %q has %q after the bracketed address; write it as [address]:port", address, rest)
		}
	}
	switch strings.Count(address, ":") {
	case 0:
		return address, "", nil
	case 1:
		host, portText, _ = strings.Cut(address, ":")
		if portText == "" {
			return "", "", fmt.Errorf("server address %q ends with ':' but has no port; write host:port, for example db.internal:7433", address)
		}
		return host, portText, nil
	default:
		return "", "", fmt.Errorf("server address %q has more than one ':'; an IPv6 address must be written in brackets, for example [::1]:7433", address)
	}
}

// canonicalPort writes the port in decimal without leading zeros, or the
// default port when there is none.
func canonicalPort(address, portText string) (string, error) {
	if portText == "" {
		return strconv.Itoa(DefaultDriverPort), nil
	}
	refusal := fmt.Errorf("the port of server address %q is %q, which is not a number from 1 to 65535 (at most five digits)", address, portText)
	if len(portText) > 5 || strings.Trim(portText, "0123456789") != "" {
		return "", refusal
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", refusal
	}
	return strconv.Itoa(port), nil
}

// canonicalServerHost canonicalises the host part (brackets included for
// IPv6).
func canonicalServerHost(address, host string) (string, error) {
	if strings.HasPrefix(host, "[") {
		literal := host[1 : len(host)-1]
		ip, err := netip.ParseAddr(literal)
		if err != nil || !ip.Is6() {
			return "", fmt.Errorf("server address %q holds %q in brackets, which is not an IPv6 address; brackets are for IPv6 only (write an IPv4 address as 127.0.0.1:7433)", address, literal)
		}
		if zone := ip.Zone(); zone != "" && !isHostNameText(zone) {
			return "", fmt.Errorf("server address %q has the zone %q, which may only hold letters, digits, '-', '_' and '.'", address, zone)
		}
		return "[" + ip.String() + "]", nil
	}
	if host == "" {
		return "", fmt.Errorf("server address %q has no host; write host:port, for example db.internal:7433", address)
	}
	if strings.Trim(host, "0123456789.") == "" {
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is4() {
			return "", fmt.Errorf("server address %q has the host %q, which is made of digits and dots but is not an IPv4 address (four numbers from 0 to 255 without leading zeros, like 127.0.0.1)", address, host)
		}
		return ip.String(), nil
	}
	// The character check runs on the host as written, before any case
	// folding: Unicode lower-casing maps U+0130 to "i" and U+212A (Kelvin) to
	// "k", which would make a non-ASCII name canonicalise to an ASCII one.
	if !isHostNameText(host) {
		return "", fmt.Errorf("server address %q has the host %q; a host name may only hold ASCII letters, digits, '-', '_' and '.'", address, host)
	}
	name := asciiLower(strings.TrimSuffix(host, "."))
	for label := range strings.SplitSeq(name, ".") {
		if label == "" {
			return "", fmt.Errorf("server address %q has the host %q, which has an empty label (two dots together, or a leading dot)", address, host)
		}
	}
	return name, nil
}

// asciiLower lowercases ASCII letters only; text has passed isHostNameText.
func asciiLower(text string) string {
	lowered := []byte(text)
	for index, character := range lowered {
		if character >= 'A' && character <= 'Z' {
			lowered[index] = character + ('a' - 'A')
		}
	}
	return string(lowered)
}

// isHostNameText reports whether text holds only ASCII letters, digits, '-',
// '_' and '.', so a canonical address can never hold a field separator.
func isHostNameText(text string) bool {
	for index := range len(text) {
		character := text[index]
		switch {
		case character >= 'a' && character <= 'z', character >= 'A' && character <= 'Z',
			character >= '0' && character <= '9', character == '-', character == '_', character == '.':
		default:
			return false
		}
	}
	return true
}

// ---- the line grammar (trust.md section 4.2) -------------------------------

// knownServersLine is one line of the file. host is the canonical address
// of an entry line and empty for a comment, a blank line or a malformed one.
type knownServersLine struct {
	raw  string // the line as written, without its LF
	host string
	key  ed25519.PublicKey
}

// parseKnownServersText splits text into lines at LF only, and parses each.
// A last line without LF is still a line.
func parseKnownServersText(text string) []knownServersLine {
	if text == "" {
		return nil
	}
	rawLines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	lines := make([]knownServersLine, 0, len(rawLines))
	for _, raw := range rawLines {
		lines = append(lines, parseKnownServersLine(raw))
	}
	return lines
}

// parseKnownServersLine parses one line. Only space and tab separate fields;
// any other byte, a Unicode separator included, is part of a field, so a
// line that holds one where a separator belongs is malformed.
func parseKnownServersLine(raw string) knownServersLine {
	line := knownServersLine{raw: raw}
	body := strings.TrimSuffix(raw, "\r")
	if body == "" || body[0] == '#' {
		return line
	}
	fields := strings.FieldsFunc(body, func(r rune) bool { return r == ' ' || r == '\t' })
	if len(fields) < 2 || len(fields[1]) != 2*ed25519.PublicKeySize {
		return line
	}
	host, err := CanonicalServerAddress(fields[0])
	if err != nil {
		return line
	}
	keyBytes, err := hex.DecodeString(fields[1])
	if err != nil {
		return line
	}
	line.host = host
	line.key = ed25519.PublicKey(keyBytes)
	return line
}

// knownServersEntries maps each canonical host to its key, refusing two
// different keys for one host: choosing either line would be a guess about
// which key is the real server's, and an appended line must not outrank the
// one the person verified.
func knownServersEntries(lines []knownServersLine, path string) (map[string]ed25519.PublicKey, error) {
	entries := make(map[string]ed25519.PublicKey)
	firstLine := make(map[string]int)
	for index, line := range lines {
		if line.host == "" {
			continue
		}
		earlier, seen := entries[line.host]
		if !seen {
			entries[line.host] = line.key
			firstLine[line.host] = index + 1
			continue
		}
		if !earlier.Equal(line.key) {
			return nil, fmt.Errorf("%s records two different keys for %s (line %d: %s, line %d: %s); "+
				"delete the line for the key you do not trust, then connect again",
				path, line.host, firstLine[line.host], Fingerprint(earlier), index+1, Fingerprint(line.key))
		}
	}
	return entries, nil
}

// ---- file safety (trust.md section 4.3) ------------------------------------

// TrustPathFacts describes the known_servers file or the directory holding
// it, as TrustPathRefusal judges them. For the directory, the facts are those
// of the directory a link resolves to.
type TrustPathFacts struct {
	// Path is the path the refusal names.
	Path string
	// Directory is true for the directory holding the file.
	Directory bool
	// Symlink is true when the file itself is a symbolic link.
	Symlink bool
	// Type is the type bits of the mode (0 for a regular file).
	Type fs.FileMode
	// Perm is the permission bits.
	Perm fs.FileMode
	// OwnerUID and CurrentUID are the owner and the process's effective uid.
	OwnerUID, CurrentUID uint32
}

// TrustPathRefusal returns nil when the facts are acceptable, and otherwise
// an error naming the path and the command that fixes it. The file must be
// a regular file and not a link (a save through a link would write to its
// target); the file and the directory must be owned by the current user or
// root and must not be group- or world-writable.
func TrustPathRefusal(facts TrustPathFacts) error {
	what := "known_servers file"
	if facts.Directory {
		what = "directory holding known_servers"
	}
	switch {
	case !facts.Directory && facts.Symlink:
		return fmt.Errorf("the known_servers file %[1]s is a symbolic link, and hoardDB does not read or write the trust store through a link: a save would write to the link's target. "+
			"Replace the link with a copy of the file it points to: cp \"$(readlink -f %[1]s)\" %[1]s.new && mv %[1]s.new %[1]s", facts.Path)
	case !facts.Directory && facts.Type != 0:
		return fmt.Errorf("the known_servers file %[1]s is not a regular file (%[2]s); move it aside and connect again: mv %[1]s %[1]s.bak", facts.Path, facts.Type)
	case facts.Directory && facts.Type != fs.ModeDir:
		return fmt.Errorf("the directory holding known_servers, %[1]s, is not a directory (%[2]s); move it aside and connect again: mv %[1]s %[1]s.bak", facts.Path, facts.Type)
	}
	var problems, fixes []string
	if facts.OwnerUID != facts.CurrentUID && facts.OwnerUID != 0 {
		problems = append(problems, fmt.Sprintf("is owned by uid %d, which is neither you (uid %d) nor root", facts.OwnerUID, facts.CurrentUID))
		fixes = append(fixes, fmt.Sprintf("chown %d %s", facts.CurrentUID, facts.Path))
	}
	if facts.Perm&0o022 != 0 {
		problems = append(problems, fmt.Sprintf("is writable by its group or by other users (mode %04o)", facts.Perm))
		mode := "600"
		if facts.Directory {
			mode = "700"
		}
		fixes = append(fixes, fmt.Sprintf("chmod %s %s", mode, facts.Path))
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("the %s %s %s, so another user could change which servers you trust. Fix it with: %s",
		what, facts.Path, strings.Join(problems, " and "), strings.Join(fixes, " && "))
}

// checkKnownServersDir checks the directory holding path, after resolving
// any link, and reports whether it exists.
func checkKnownServersDir(path string) (bool, error) {
	dir := filepath.Dir(path)
	resolved, err := filepath.EvalSymlinks(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("resolving the directory holding known_servers, %s: %w", dir, err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return false, fmt.Errorf("checking the directory holding known_servers, %s: %w", resolved, err)
	}
	facts, err := trustPathFacts(resolved, info)
	if err != nil {
		return false, err
	}
	facts.Directory = true
	return true, TrustPathRefusal(facts)
}

// ensureKnownServersDir creates the directory 0700 when it is missing, and
// checks it.
func ensureKnownServersDir(path string) error {
	exists, err := checkKnownServersDir(path)
	if err != nil || exists {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating the directory for known_servers, %s: %w", dir, err)
	}
	// MkdirAll's mode passes through the umask; the directory must be
	// exactly owner-only whatever the umask is.
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("setting %s to mode 0700: %w", dir, err)
	}
	_, err = checkKnownServersDir(path)
	return err
}

// readKnownServersFile reads the file without following a link, after
// checking the directory and the file. A missing file reads as empty.
func readKnownServersFile(path string) ([]byte, bool, error) {
	if _, err := checkKnownServersDir(path); err != nil {
		return nil, false, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("checking known_servers %s: %w", path, err)
	}
	if err := checkKnownServersFileInfo(path, info); err != nil {
		return nil, false, err
	}
	file, err := openNoFollow(path, os.O_RDONLY, 0)
	if errors.Is(err, errSymbolicLink) {
		return nil, false, TrustPathRefusal(TrustPathFacts{Path: path, Symlink: true})
	}
	if err != nil {
		return nil, false, fmt.Errorf("opening known_servers %s: %w", path, err)
	}
	content, readErr := readCheckedFile(path, file)
	if closeErr := file.Close(); closeErr != nil && readErr == nil {
		readErr = fmt.Errorf("closing known_servers %s: %w", path, closeErr)
	}
	return content, true, readErr
}

// readCheckedFile checks the open file (the path could have been swapped
// between the Lstat and the open) and reads it.
func readCheckedFile(path string, file *os.File) ([]byte, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("checking known_servers %s: %w", path, err)
	}
	if err := checkKnownServersFileInfo(path, info); err != nil {
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("reading known_servers %s: %w", path, err)
	}
	return content, nil
}

func checkKnownServersFileInfo(path string, info fs.FileInfo) error {
	facts, err := trustPathFacts(path, info)
	if err != nil {
		return err
	}
	facts.Symlink = info.Mode()&fs.ModeSymlink != 0
	if facts.Symlink {
		facts.Type = 0
	}
	return TrustPathRefusal(facts)
}

// ---- saving (trust.md section 4.4) -----------------------------------------

// lockKnownServers takes the exclusive lock on path's sibling lock file,
// waiting at most knownServersLockWait. The wait is a blocking flock queued in
// the kernel, so a save is served when the holder releases. Retrying a
// non-blocking flock on a timer instead decides the winner by poll phase, so
// a waiting save can lose every round until the bound expires.
//
// A blocked flock cannot be cancelled, so the saves of one process that wait
// for one lock file share a single waiter goroutine (knownServersLockQueues)
// instead of starting one each: a save that gives up leaves the queue, never
// a goroutine, a descriptor and a thread behind.
func lockKnownServers(path string) (*os.File, error) {
	lockPath := path + ".lock"
	request := &knownServersLockRequest{granted: make(chan knownServersLockGrant, 1)}
	enqueueKnownServersLock(lockPath, request)
	timer := time.NewTimer(knownServersLockWait)
	defer timer.Stop()
	select {
	case grant := <-request.granted:
		return grant.file, grant.err
	case <-timer.C:
		if dequeueKnownServersLock(lockPath, request) {
			return nil, fmt.Errorf("another process has held the lock on %s for more than %s while saving known_servers; "+
				"if no other hoardDB client is running, find the holder with: fuser %s", lockPath, knownServersLockWait, lockPath)
		}
		// The waiter handed this save the lock as the bound expired; the grant
		// is already in the buffered channel.
		grant := <-request.granted
		return grant.file, grant.err
	}
}

// knownServersLockRequest is one save waiting for a lock file. granted has room
// for the one grant, so the waiter never blocks handing it over.
type knownServersLockRequest struct {
	granted chan knownServersLockGrant
}

type knownServersLockGrant struct {
	file *os.File
	err  error
}

// knownServersLockQueues holds, per lock file, the saves of this process that
// are waiting, in arrival order. An entry exists exactly while its waiter
// goroutine runs; the waiter deletes it under the mutex as it exits, so a save
// that enqueues afterwards starts a new one and is never left unserved.
var knownServersLockQueues = struct {
	sync.Mutex
	waiting map[string][]*knownServersLockRequest
}{waiting: make(map[string][]*knownServersLockRequest)}

func enqueueKnownServersLock(lockPath string, request *knownServersLockRequest) {
	knownServersLockQueues.Lock()
	defer knownServersLockQueues.Unlock()
	queue, waiterRuns := knownServersLockQueues.waiting[lockPath]
	knownServersLockQueues.waiting[lockPath] = append(queue, request)
	if !waiterRuns {
		go serveKnownServersLock(lockPath)
	}
}

// dequeueKnownServersLock removes request and reports whether it was still
// waiting; false means the waiter has already granted it the lock.
func dequeueKnownServersLock(lockPath string, request *knownServersLockRequest) bool {
	knownServersLockQueues.Lock()
	defer knownServersLockQueues.Unlock()
	queue := knownServersLockQueues.waiting[lockPath]
	for position, queued := range queue {
		if queued == request {
			knownServersLockQueues.waiting[lockPath] = append(queue[:position:position], queue[position+1:]...)
			return true
		}
	}
	return false
}

// serveKnownServersLock is the one waiter of lockPath in this process. It
// blocks in flock on its own descriptor; a save that gets the lock owns that
// descriptor and releases by closing it. The waiter then blocks on a fresh
// descriptor if more saves wait. It exits, closing what it holds, when the
// lock is granted and nobody is left waiting (every timed-out save has left
// the queue) or when the lock cannot be opened or taken, which every waiting
// save is told. Under a holder that never lets go it stays blocked, one
// goroutine and one descriptor however many saves time out.
func serveKnownServersLock(lockPath string) {
	for {
		lockFile, err := openKnownServersLock(lockPath)
		if err == nil {
			if err = lockExclusive(lockFile); err != nil {
				err = errors.Join(fmt.Errorf("locking %s: %w", lockPath, err), lockFile.Close())
			}
		}
		knownServersLockQueues.Lock()
		queue := knownServersLockQueues.waiting[lockPath]
		if err != nil {
			delete(knownServersLockQueues.waiting, lockPath)
			knownServersLockQueues.Unlock()
			for _, request := range queue {
				request.granted <- knownServersLockGrant{err: err}
			}
			return
		}
		if len(queue) == 0 {
			delete(knownServersLockQueues.waiting, lockPath)
			if err := lockFile.Close(); err != nil {
				slog.Default().Warn("closing the known_servers lock a timed-out save left queued failed", "lock", lockPath, "error", err)
			}
			knownServersLockQueues.Unlock()
			return
		}
		queue[0].granted <- knownServersLockGrant{file: lockFile}
		if len(queue) == 1 {
			delete(knownServersLockQueues.waiting, lockPath)
			knownServersLockQueues.Unlock()
			return
		}
		knownServersLockQueues.waiting[lockPath] = queue[1:]
		knownServersLockQueues.Unlock()
	}
}

// openKnownServersLock opens, creating it, the lock file beside the store.
func openKnownServersLock(lockPath string) (*os.File, error) {
	lockFile, err := openNoFollow(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if errors.Is(err, errSymbolicLink) {
		return nil, fmt.Errorf("the known_servers lock file %[1]s is a symbolic link, and hoardDB does not lock the trust store through a link: "+
			"the lock would be taken on the link's target, which another process may not use. Remove the link; the next save creates the lock file: rm %[1]s", lockPath)
	}
	if err != nil {
		return nil, fmt.Errorf("opening the known_servers lock %s: %w", lockPath, err)
	}
	return lockFile, nil
}

// appendKnownServersLine appends line in one write and syncs it. A new file
// is created 0600 whatever the umask, and its directory is synced so the
// file's name survives a crash too.
func appendKnownServersLine(path string, line []byte, existed bool) error {
	file, err := openNoFollow(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if errors.Is(err, errSymbolicLink) {
		return TrustPathRefusal(TrustPathFacts{Path: path, Symlink: true})
	}
	if err != nil {
		return fmt.Errorf("opening known_servers %s to add a server: %w", path, err)
	}
	writeErr := writeAndSync(file, line, existed)
	if closeErr := file.Close(); closeErr != nil && writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return fmt.Errorf("adding a server to known_servers %s: %w", path, writeErr)
	}
	if !existed {
		return syncDir(filepath.Dir(path))
	}
	return nil
}

func writeAndSync(file *os.File, content []byte, existed bool) error {
	if !existed {
		if err := file.Chmod(0o600); err != nil {
			return err
		}
	}
	if _, err := file.Write(content); err != nil {
		return err
	}
	return file.Sync()
}

// replaceKnownServersFile writes content to a temporary file in the same
// directory, syncs it and renames it over path, so a reader sees the old
// file or the new one and never a partial one.
func replaceKnownServersFile(path string, content []byte) error {
	dir := filepath.Dir(path)
	temporary, err := os.CreateTemp(dir, ".known_servers-*.tmp")
	if err != nil {
		return fmt.Errorf("creating a temporary file in %s to rewrite known_servers: %w", dir, err)
	}
	writeErr := writeAndSync(temporary, content, false)
	if closeErr := temporary.Close(); closeErr != nil && writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(temporary.Name(), path)
	}
	if writeErr != nil {
		if removeErr := os.Remove(temporary.Name()); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			writeErr = errors.Join(writeErr, removeErr)
		}
		return fmt.Errorf("rewriting known_servers %s: %w", path, writeErr)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening %s to sync it: %w", dir, err)
	}
	syncErr := handle.Sync()
	if closeErr := handle.Close(); closeErr != nil && syncErr == nil {
		syncErr = closeErr
	}
	if syncErr != nil {
		return fmt.Errorf("syncing %s: %w", dir, syncErr)
	}
	return nil
}
