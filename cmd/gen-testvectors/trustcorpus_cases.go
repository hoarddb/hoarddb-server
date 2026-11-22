package main

import (
	"fmt"
	"io/fs"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The trust corpus inputs. {a} and {b} in a text are the two fixed keys' hex
// ({A} is key a in upper case), {fa} and {fb} their fingerprints. Every
// expected value below is checked against the real code by the generator.

type trustCanonicalCase struct {
	name, input, want, notes string
	refused                  bool
}

var trustCanonicalCases = []trustCanonicalCase{
	{name: "a lowercase name", input: "db.internal:7433", want: "db.internal:7433"},
	{name: "an upper-case name", input: "DB.Internal:7433", want: "db.internal:7433"},
	{name: "one trailing dot", input: "db.internal.:7433", want: "db.internal:7433"},
	{name: "a trailing dot and no port", input: "db.internal.", want: "db.internal:7433"},
	{name: "no port is the default port", input: "db.internal", want: "db.internal:7433"},
	{name: "leading zeros in the port", input: "db:07433", want: "db:7433"},
	{name: "another port", input: "db:4434", want: "db:4434"},
	{name: "an IPv4 address", input: "127.0.0.1:7433", want: "127.0.0.1:7433"},
	{name: "an IPv4 address without a port", input: "127.0.0.1", want: "127.0.0.1:7433"},
	{name: "localhost stays localhost", input: "localhost:7433", want: "localhost:7433", notes: "never resolved: localhost and 127.0.0.1 are different names"},
	{name: "LOCALHOST", input: "LOCALHOST", want: "localhost:7433"},
	{name: "IPv6 loopback", input: "[::1]:7433", want: "[::1]:7433"},
	{name: "IPv6 loopback written out", input: "[0:0:0:0:0:0:0:1]:7433", want: "[::1]:7433"},
	{name: "IPv6 loopback with leading zeros and no port", input: "[::0001]", want: "[::1]:7433"},
	{name: "IPv6 tie: the leftmost zero run is compressed", input: "[2001:DB8:0:0:1:0:0:1]:7433", want: "[2001:db8::1:0:0:1]:7433"},
	{name: "IPv6: the longest zero run is compressed", input: "[2001:db8:0:0:1:0:0:0]:7433", want: "[2001:db8:0:0:1::]:7433"},
	{name: "IPv6: a single zero group is not compressed", input: "[2001:db8:0:1:1:1:1:1]", want: "[2001:db8:0:1:1:1:1:1]:7433"},
	{name: "IPv6 with a zone", input: "[FE80::1%eth0]:7433", want: "[fe80::1%eth0]:7433", notes: "the zone is kept as written"},
	{name: "IPv4-mapped IPv6 stays IPv6", input: "[::FFFF:127.0.0.1]:7433", want: "[::ffff:127.0.0.1]:7433"},
	{name: "underscore and hyphen", input: "Under_Score-host:1", want: "under_score-host:1"},
	{name: "empty", input: "", refused: true},
	{name: "IPv6 without brackets", input: "::1:7433", refused: true},
	{name: "IPv4 in brackets", input: "[127.0.0.1]:7433", refused: true},
	{name: "unclosed bracket", input: "[::1", refused: true},
	{name: "text after the bracket", input: "[::1]7433", refused: true},
	{name: "a zone with a space", input: "[fe80::1%a b]:7433", refused: true},
	{name: "port 0", input: "db:0", refused: true},
	{name: "port 65536", input: "db:65536", refused: true},
	{name: "a signed port", input: "db:+7433", refused: true},
	{name: "a six-digit port", input: "db:007433", refused: true},
	{name: "an empty port", input: "db:", refused: true},
	{name: "no host", input: ":7433", refused: true},
	{name: "IPv4 with leading zeros", input: "127.000.000.001:7433", refused: true, notes: "digits and dots that are not an IPv4 address are refused, not read as a name"},
	{name: "three-part IPv4", input: "1.2.3:7433", refused: true},
	{name: "an empty label", input: "db..internal:7433", refused: true},
	{name: "a leading dot", input: ".db:7433", refused: true},
	{name: "two trailing dots", input: "db.internal..:7433", refused: true, notes: "only one trailing dot is stripped"},
	{name: "a non-ASCII name", input: "dé.internal:7433", refused: true},
	{name: "a no-break space in the name", input: "db x:7433", refused: true},
	{name: "U+212A KELVIN SIGN in a name", input: "\u212aey.example:7433", refused: true, notes: "Unicode lower-casing maps U+212A to ASCII k; check for ASCII before folding case"},
	{name: "U+0130 in a name", input: "\u0130nternal.example:7433", refused: true, notes: "Unicode lower-casing maps U+0130 to ASCII i; check for ASCII before folding case"},
	{name: "an empty port after a bracketed IPv6 address", input: "[::1]:", refused: true, notes: "a written port is 1 to 65535; leave out the ':' for the default"},
	{name: "an empty port after a bracketed address with a zone", input: "[fe80::1%eth0]:", refused: true},
}

type trustParseCase struct {
	name, text, notes string
	entries           map[string]string // host -> key name ("a" or "b")
	errorContains     []string
}

var trustParseCases = []trustParseCase{
	{name: "one entry", text: "db:7433 {a}\n", entries: map[string]string{"db:7433": "a"}},
	{name: "CRLF", text: "db:7433 {a}\r\nweb:7433 {b}\r\n", entries: map[string]string{"db:7433": "a", "web:7433": "b"}},
	{name: "no final LF", text: "db:7433 {a}", entries: map[string]string{"db:7433": "a"}},
	{name: "a tab separator", text: "db:7433\t{a}\n", entries: map[string]string{"db:7433": "a"}},
	{name: "runs of spaces and tabs", text: "db:7433 \t  \t{a}\n", entries: map[string]string{"db:7433": "a"}},
	{name: "leading spaces and tabs", text: "  \tdb:7433 {a}\n", entries: map[string]string{"db:7433": "a"}},
	{name: "text after the key", text: "db:7433 {a} added 2026-10-05\n", entries: map[string]string{"db:7433": "a"}},
	{name: "text after the key, tab", text: "db:7433\t{a}\tadded by hand\n", entries: map[string]string{"db:7433": "a"}},
	{name: "comments, blank and whitespace-only lines", text: "# trusted servers\n\ndb:7433 {a}\n \t \n#web:7433 {b}\n", entries: map[string]string{"db:7433": "a"}},
	{name: "an indented # is not a comment", text: " # db:7433 {a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}, notes: "the line is malformed: its first field, #, is not a host"},
	{name: "no-break space is not a separator", text: "db:7433 {a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}},
	{name: "NEL is not a separator", text: "db:7433\u0085{a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}},
	{name: "line separator does not end a line", text: "db:7433 {a} web:7433 {b}\n", entries: map[string]string{}, notes: "one line, whose key field runs into U+2028: malformed"},
	{name: "form feed is not a separator", text: "db:7433\f{a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}},
	{name: "vertical tab is not a separator", text: "db:7433\v{a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}},
	{name: "only one CR is stripped", text: "db:7433 {a}\r\r\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}},
	{name: "U+212A in a host is malformed", text: "\u212aey.example:7433 {a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}, notes: "not read as key.example"},
	{name: "U+0130 in a host is malformed", text: "\u0130nternal.example:7433 {a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}, notes: "not read as internal.example"},
	{name: "an empty bracketed port is malformed", text: "[::1]: {a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}},
	{name: "a CR inside the line", text: "db:7433\r{a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}},
	{name: "an upper-case key", text: "db:7433 {A}\n", entries: map[string]string{"db:7433": "a"}, notes: "read in either case; entries are lowercase"},
	{name: "malformed lines are skipped", text: "db:7433\nbad:7433 not-hex\nshort:7433 0123\n::1:7433 {a}\nweb:7433 {b}\n", entries: map[string]string{"web:7433": "b"}, notes: "one field; a key that is not hex; a short key; a host that does not canonicalise"},
	{name: "hosts are canonicalised", text: "DB.Internal.:07433 {a}\n[0:0:0:0:0:0:0:1] {b}\n", entries: map[string]string{"db.internal:7433": "a", "[::1]:7433": "b"}},
	{name: "the same key twice", text: "db:7433 {a}\ndb:7433 {a}\n", entries: map[string]string{"db:7433": "a"}},
	{name: "the same key under two spellings", text: "db:7433 {a}\nDB.:07433 {a}\n", entries: map[string]string{"db:7433": "a"}},
	{name: "two keys for one host", text: "db:7433 {a}\ndb:7433 {b}\n", errorContains: []string{"db:7433", "{fa}", "{fb}"}},
	{name: "two keys under two spellings", text: "db.internal:7433 {a}\nDB.Internal.:7433 {b}\n", errorContains: []string{"db.internal:7433", "{fa}", "{fb}"}},
	{name: "an empty file", text: "", entries: map[string]string{}},
	{name: "only comments", text: "# nothing yet\n", entries: map[string]string{}},
}

// trustLookupFile is the file every lookup case reads.
const trustLookupFile = "# lookup vectors\ndb.internal:7433 {a}\n[::1]:7433 {a}\n127.0.0.1:7433 {b}\n"

type trustLookupCase struct {
	name, host, key, expect, notes string
}

var trustLookupCases = []trustLookupCase{
	{name: "the recorded spelling", host: "db.internal:7433", key: "a", expect: "known"},
	{name: "upper case and a trailing dot", host: "DB.INTERNAL.:7433", key: "a", expect: "known"},
	{name: "no port", host: "db.internal", key: "a", expect: "known"},
	{name: "another port", host: "db.internal:7434", key: "a", expect: "unknown"},
	{name: "a changed key under another spelling", host: "DB.Internal.:7433", key: "b", expect: "changed", notes: "never a first sight"},
	{name: "IPv6 written out", host: "[0:0:0:0:0:0:0:1]:7433", key: "a", expect: "known"},
	{name: "a changed IPv6 key", host: "[::0001]:7433", key: "b", expect: "changed"},
	{name: "localhost is not 127.0.0.1", host: "localhost:7433", key: "b", expect: "unknown"},
	{name: "127.0.0.1", host: "127.0.0.1", key: "b", expect: "known"},
}

type trustSaveCase struct {
	name, action, host, key, result, notes string
	text                                   *string
	errorContains                          []string
}

func fileText(text string) *string { return &text }

var trustSaveCases = []trustSaveCase{
	{name: "append to a missing file", action: "append", host: "db:7433", key: "a", result: "db:7433 {a}\n"},
	{name: "append to an empty file", action: "append", text: fileText(""), host: "db:7433", key: "a", result: "db:7433 {a}\n"},
	{
		name: "append keeps every other line", action: "append", host: "web:7433", key: "b",
		text:   fileText("# work servers\r\n\ndb:7433 {A}  added by hand\nnot a valid line\n \n"),
		result: "# work servers\r\n\ndb:7433 {A}  added by hand\nnot a valid line\n \nweb:7433 {b}\n",
	},
	{name: "append after a line with no LF", action: "append", text: fileText("db:7433 {a}"), host: "web:7433", key: "b", result: "db:7433 {a}\nweb:7433 {b}\n"},
	{name: "append writes the canonical host", action: "append", host: "DB.Internal.:07433", key: "a", result: "db.internal:7433 {a}\n"},
	{name: "append of a recorded key writes nothing", action: "append", text: fileText("db.internal:7433 {a}\n"), host: "DB.INTERNAL", key: "a", result: "db.internal:7433 {a}\n"},
	{
		name: "append of a different key is refused", action: "append", text: fileText("db.internal:7433 {a}\n"), host: "DB.Internal.:7433", key: "b",
		result: "db.internal:7433 {a}\n", errorContains: []string{"{fa}", "{fb}", "db.internal:7433"}, notes: "a changed key: the file is unchanged",
	},
	{
		name: "replace keeps every other line verbatim", action: "replace", host: "db:7433", key: "b",
		text:   fileText("# work\r\ndb:7433 {a} old note\nnot a valid line\n\nweb:7433 {a}\r\n"),
		result: "# work\r\ndb:7433 {b}\nnot a valid line\n\nweb:7433 {a}\r\n",
	},
	{
		name: "replace removes the host's later lines", action: "replace", host: "DB:7433", key: "b",
		text:   fileText("db:7433 {a}\nweb:7433 {a}\nDB.:07433 {a}\n"),
		result: "db:7433 {b}\nweb:7433 {a}\n",
	},
	{name: "replace of an absent host appends", action: "replace", text: fileText("# servers\nweb:7433 {a}\n"), host: "db:7433", key: "b", result: "# servers\nweb:7433 {a}\ndb:7433 {b}\n"},
	{name: "replace ends every line with LF", action: "replace", text: fileText("web:7433 {a}\ndb:7433 {a}"), host: "db:7433", key: "b", result: "web:7433 {a}\ndb:7433 {b}\n"},
}

type trustSafetyCase struct {
	name, target, kind, mode, owner, notes string
	symlink                                bool
	errorContains                          []string
}

var trustSafetyCases = []trustSafetyCase{
	{name: "file 0600", target: "file", kind: "regular", mode: "0600", owner: "self"},
	{name: "file 0644", target: "file", kind: "regular", mode: "0644", owner: "self", notes: "readable by others is accepted; writable is not"},
	{name: "file 0640", target: "file", kind: "regular", mode: "0640", owner: "self"},
	{name: "file 0400 owned by root", target: "file", kind: "regular", mode: "0400", owner: "root"},
	{name: "file 0620", target: "file", kind: "regular", mode: "0620", owner: "self", errorContains: []string{"chmod 600"}},
	{name: "file 0602", target: "file", kind: "regular", mode: "0602", owner: "self", errorContains: []string{"chmod 600"}},
	{name: "file 0666", target: "file", kind: "regular", mode: "0666", owner: "self", errorContains: []string{"chmod 600"}},
	{name: "file owned by another user", target: "file", kind: "regular", mode: "0600", owner: "other", errorContains: []string{"chown"}},
	{name: "file owned by another user and 0666", target: "file", kind: "regular", mode: "0666", owner: "other", errorContains: []string{"chown", "chmod 600"}},
	{name: "file is a symlink", target: "file", kind: "regular", symlink: true, mode: "0777", owner: "self", errorContains: []string{"symbolic link"}, notes: "refused whatever it points to: a save would write to the target"},
	{name: "file is a FIFO", target: "file", kind: "fifo", mode: "0600", owner: "self", errorContains: []string{"not a regular file"}},
	{name: "directory 0700", target: "directory", kind: "directory", mode: "0700", owner: "self"},
	{name: "directory 0755", target: "directory", kind: "directory", mode: "0755", owner: "self"},
	{name: "directory 0755 owned by root", target: "directory", kind: "directory", mode: "0755", owner: "root"},
	{name: "directory 0770", target: "directory", kind: "directory", mode: "0770", owner: "self", errorContains: []string{"chmod 700"}},
	{name: "directory 0777", target: "directory", kind: "directory", mode: "0777", owner: "self", errorContains: []string{"chmod 700"}},
	{name: "directory 0777 owned by root", target: "directory", kind: "directory", mode: "0777", owner: "root", errorContains: []string{"chmod 700"}},
	{name: "directory owned by another user", target: "directory", kind: "directory", mode: "0700", owner: "other", errorContains: []string{"chown"}},
	{name: "directory reached through a symlink", target: "directory", kind: "directory", symlink: true, mode: "0700", owner: "self", notes: "a linked directory is accepted; the directory it resolves to is checked"},
	{name: "directory through a symlink resolving to 0777", target: "directory", kind: "directory", symlink: true, mode: "0777", owner: "self", errorContains: []string{"chmod 700"}},
	{name: "directory is not a directory", target: "directory", kind: "not-a-directory", mode: "0700", owner: "self", errorContains: []string{"not a directory"}},
}

// facts turns a row into what transport.TrustPathRefusal judges.
func (tc trustSafetyCase) facts() (transport.TrustPathFacts, error) {
	mode, err := parseOctalMode(tc.mode)
	if err != nil {
		return transport.TrustPathFacts{}, err
	}
	facts := transport.TrustPathFacts{
		Path: "known_servers", Directory: tc.target == "directory", Symlink: tc.symlink,
		Perm: mode.Perm(), CurrentUID: trustSafetySelf,
	}
	switch tc.kind {
	case "regular", "not-a-directory":
		facts.Type = 0
	case "directory":
		facts.Type = fs.ModeDir
	case "fifo":
		facts.Type = fs.ModeNamedPipe
	default:
		return transport.TrustPathFacts{}, fmt.Errorf("safety case %q: unknown kind %q", tc.name, tc.kind)
	}
	switch tc.owner {
	case "self":
		facts.OwnerUID = trustSafetySelf
	case "root":
		facts.OwnerUID = 0
	case "other":
		facts.OwnerUID = trustSafetyOther
	default:
		return transport.TrustPathFacts{}, fmt.Errorf("safety case %q: unknown owner %q", tc.name, tc.owner)
	}
	return facts, nil
}
