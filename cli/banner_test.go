package cli

import (
	"bytes"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

var ansi = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// TestBannerLinesAreAllTheSameWidth is the guard for a box whose frame and
// contents disagreed: the top rule was three columns short of every other line
// (73 against 76), so the right edge stepped in at the top — in the first thing
// a user sees. The width is counted in runes and measured after stripping
// colour, so the geometry the user sees is what is asserted, whatever the theme
// paints.
func TestBannerLinesAreAllTheSameWidth(t *testing.T) {
	ratio := 0.001
	cases := []struct {
		name  string
		facts bannerFacts
	}{
		{
			name: "a plain local session",
			facts: bannerFacts{
				Address: "127.0.0.1:28533", NodeID: "node-a", Version: "0.1.0",
				Uptime: "1h53m37s", Healthy: true, Buckets: 1, Standalone: true, HasNodes: true,
				RF: 1, RFKnown: true, MemRatio: &ratio, MemLimit: 30786125824, User: "admin",
				Now: time.Date(2026, 10, 11, 9, 42, 30, 0, time.UTC),
			},
		},
		{
			name: "a long node id and address are clipped, not spilled",
			facts: bannerFacts{
				Address: "hoarddb-node-07.internal.example.com:7433",
				NodeID:  "hoarddb-node-07.internal.example.com",
				Version: "1.0.0-rc1+sha.abcdef123456", Uptime: "8760h0m1s", Healthy: true,
				Buckets: 1000000, Members: 7, HasNodes: true, RF: 3, RFKnown: true,
				User: "someone-with-a-long-name",
				Now:  time.Date(2026, 10, 11, 9, 42, 30, 0, time.UTC),
			},
		},
		{
			name: "no factor is known, the node is unhealthy and the reasons are long",
			facts: bannerFacts{
				Address: "127.0.0.1:7433", NodeID: "n1", Version: "0.1.0", Uptime: "5s",
				Healthy: false, Buckets: 400000, Members: 0, HasNodes: false,
				RFKnown: false, RFReason: "not read: >256 buckets",
				User: "admin", Database: "app",
				Reasons: []string{
					"the node store refuses writes: no space left on device, and it cannot persist them",
					"capacity: memory ratio 1.20 is past its 0.80 threshold",
				},
				Now: time.Date(2026, 10, 11, 9, 42, 30, 0, time.UTC),
			},
		},
		{
			name: "a session with a database selected",
			facts: bannerFacts{
				Address: "127.0.0.1:7433", NodeID: "n1", Version: "0.1.0", Uptime: "2m",
				Healthy: true, Buckets: 3, Members: 2, HasNodes: true, RF: 2, RFKnown: true,
				User: "ada", Database: "analytics",
				Now: time.Date(2026, 10, 11, 9, 42, 30, 0, time.UTC),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := renderBanner(t, tc.facts)
			if len(lines) < 3 {
				t.Fatalf("the box rendered %d lines, want a frame and content", len(lines))
			}
			want := len([]rune(lines[1]))
			for i, line := range lines {
				if got := len([]rune(line)); got != want {
					t.Errorf("line %d is %d columns, want %d (the first content line's width):\n%s",
						i, got, want, line)
				}
			}
			for _, i := range []int{0, len(lines) - 1} {
				if !strings.HasPrefix(lines[i], "┌") && !strings.HasPrefix(lines[i], "└") {
					t.Errorf("line %d is not a frame line: %q", i, lines[i])
				}
			}
		})
	}
}

// TestBannerShowsNoStoreDetail pins the shape the owner asked for: the greeting
// is about the session, not the store. Bucket, memtable and encryption figures
// live in `status`; repeating them here made the box a dashboard, so an edit
// that puts them back should be deliberate and fail this first.
func TestBannerShowsNoStoreDetail(t *testing.T) {
	lines := renderBanner(t, bannerFacts{
		Address: "127.0.0.1:7433", NodeID: "n1", Version: "0.1.0", Uptime: "2m",
		Healthy: true, Buckets: 4213, Members: 1, HasNodes: true, RF: 1, RFKnown: true,
		User: "admin", Now: time.Date(2026, 10, 11, 9, 42, 30, 0, time.UTC),
	})
	box := strings.Join(lines, "\n")
	for _, unwanted := range []string{"buckets", "memtable", "encryption", "healthy"} {
		if strings.Contains(box, unwanted) {
			t.Errorf("the banner mentions %q; store detail belongs to `status`:\n%s", unwanted, box)
		}
	}
	for _, wanted := range []string{"node", "address", "cluster", "RF", "process mem", "session"} {
		if !strings.Contains(box, wanted) {
			t.Errorf("the banner does not show %q:\n%s", wanted, box)
		}
	}
}

// TestBannerWarnsWhenTheNodeIsUnhealthy is the one store fact that must not be
// dropped with the others: an unhealthy node says so. The box is 72 columns, so
// the reason is clipped and the line names `status`, which carries the whole of
// it — a truncated warning that does not say where the rest is would be worse
// than none.
func TestBannerWarnsWhenTheNodeIsUnhealthy(t *testing.T) {
	lines := renderBanner(t, bannerFacts{
		Address: "127.0.0.1:7433", NodeID: "n1", Version: "0.1.0", Uptime: "2m",
		Healthy: false, Buckets: 1, Members: 1, HasNodes: true, RFKnown: false,
		RFReason: "no buckets yet", User: "admin",
		Reasons: []string{"the node store refuses writes: no space left on device"},
		Now:     time.Date(2026, 10, 11, 9, 42, 30, 0, time.UTC),
	})
	box := strings.Join(lines, "\n")
	for _, want := range []string{"warning", "unhealthy", "the node store refuses", "status"} {
		if !strings.Contains(box, want) {
			t.Errorf("an unhealthy node's banner does not carry %q:\n%s", want, box)
		}
	}
}

// renderBanner draws the box for one fact set and returns its lines, with any
// colour stripped: the assertions are about geometry and wording, not the
// escapes this build may or may not emit.
func renderBanner(t *testing.T, f bannerFacts) []string {
	t.Helper()
	var out bytes.Buffer
	c := NewCLI(WithTerminal(&out))
	c.drawBanner(f)
	text := ansi.ReplaceAllString(out.String(), "")
	return strings.Split(strings.TrimRight(text, "\n"), "\n")
}

// TestShowBannerDrawsTheBox is the half the routing test cannot reach: the
// greeting prints once and scrolls away, so `show banner` exists to draw the
// box again. Routing that stops at the error type stays green while the command
// draws nothing at all.
func TestShowBannerDrawsTheBox(t *testing.T) {
	var out bytes.Buffer
	_, c := newStubServer(t, bannerScript(), &out)

	if err := c.handle("show banner"); err != nil {
		t.Fatalf("handle(\"show banner\"): %v", err)
	}
	text := ansi.ReplaceAllString(out.String(), "")
	for _, want := range []string{"┌", "└", "node", "n1", "address", "cluster", "RF", "session"} {
		if !strings.Contains(text, want) {
			t.Fatalf("`show banner` drew no %q:\n%s", want, text)
		}
	}
}

// TestShowBannerIsACommand pins the on-demand half: the greeting prints once and
// scrolls away, so `show banner` brings it back. It is routed by the session
// (the box is built from the theme and the session's own fields), which means
// the routing, the parser's acceptance of the target and completion are three
// separate things that can each regress on their own.
func TestShowBannerIsACommand(t *testing.T) {
	cmd, err := ParseCommand("show banner")
	if err != nil {
		t.Fatalf("ParseCommand(\"show banner\"): %v", err)
	}
	if cmd.Type != "global" || cmd.Method != "show" || len(cmd.Args) != 1 || cmd.Args[0] != "banner" {
		t.Fatalf("ParseCommand(\"show banner\") = %+v, want the show target \"banner\"", cmd)
	}

	// A session with no connection refuses it the way every other show target
	// does. The point of the check is which error comes back: an unrouted
	// target answers "unknown show target".
	c := NewCLI(WithTerminal(&bytes.Buffer{}))
	err = c.handle("show banner")
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("handle(\"show banner\") = %v, want ErrNotConnected (an unrouted target says \"unknown show target\")", err)
	}

	// And it is completed, so `show ban<Tab>` finds it.
	line := "show ban"
	if _, got := c.completer()(line, len(line)); len(got) != 1 || got[0] != "banner" {
		t.Fatalf("completing %q = %v, want [banner]", line, got)
	}

	// The refusal for a target that does not exist still names the real ones,
	// including this one.
	if _, err := ParseCommand("show nonsense"); err == nil || !strings.Contains(err.Error(), "banner") {
		t.Fatalf("ParseCommand(\"show nonsense\") = %v, want an error naming the valid targets", err)
	}
}

// bannerScript is what the banner's reads are answered with: a healthy node, so
// the box has something to draw.
func bannerScript() map[transport.Opcode]interface{} {
	return map[transport.Opcode]interface{}{
		transport.OpHealthCheck: server.HealthResponse{NodeID: "n1", Uptime: "2m", Version: "0.1.0", Healthy: true},
		transport.OpListNodes:   server.NodeListResponse{},
	}
}

// swapStdin points the process's standard input at f for one test. The line
// editor reads os.Stdin directly, so a session driven from a pseudo-terminal
// needs it there; the previous file is put back when the test ends.
func swapStdin(t *testing.T, f *os.File) {
	t.Helper()
	previous := os.Stdin
	os.Stdin = f
	t.Cleanup(func() { os.Stdin = previous })
}

// TestBannerGreetsAnInteractiveSession is the plain case: with a terminal at
// both ends the box is drawn.
func TestBannerGreetsAnInteractiveSession(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	outMaster, outSlave := openTestPty(t)
	setPtySize(t, outSlave, 24, 80)
	inMaster, inSlave := openTestPty(t)
	setPtySize(t, inSlave, 24, 80)

	_, c := newStubServer(t, bannerScript(), outSlave)
	c.reader = inSlave
	swapStdin(t, inSlave)
	feedPty(t, inMaster, "exit\n")

	if err := c.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out := ptyOutput(t, outMaster); !strings.Contains(out, "┌") {
		t.Fatalf("an interactive session was not greeted with the banner box:\n%q", out)
	}
}

// TestBannerDoesNotGoIntoARedirectedStdout is the guard for defect (d): the
// greeting was gated on the *input* being a terminal, so a session typing at a
// terminal but writing to a file or a pipe drew the box into the file. The
// banner needs a terminal at both ends; with a redirected stdout the session
// keeps the two plain lines it always printed.
func TestBannerDoesNotGoIntoARedirectedStdout(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	inMaster, inSlave := openTestPty(t)
	setPtySize(t, inSlave, 24, 80)

	var out bytes.Buffer
	_, c := newStubServer(t, bannerScript(), &out)
	c.reader = inSlave
	swapStdin(t, inSlave)
	feedPty(t, inMaster, "exit\n")

	if err := c.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	text := out.String()
	if strings.Contains(text, "┌") {
		t.Fatalf("the banner box was written to a redirected stdout:\n%q", text)
	}
	for _, want := range []string{"hoardDB CLI ", "Type 'help' for help"} {
		if !strings.Contains(text, want) {
			t.Fatalf("a session with a redirected stdout did not print %q:\n%q", want, text)
		}
	}
}

// TestBannerPaintsTheLabelsNotTheValues is the guard for the accent: the labels
// — the words that name a field — wear it, and every value stays in the
// terminal's own foreground. A value is what a reader has to trust; a label is
// what they look for.
//
// A test's terminal is a buffer, so colour is off unless it is turned on
// deliberately — which is also why this is the one test that turns it on.
func TestBannerPaintsTheLabelsNotTheValues(t *testing.T) {
	ratio := 0.001
	facts := bannerFacts{
		Address: "127.0.0.1:28533", NodeID: "node-a", Version: "0.1.0",
		Uptime: "1h53m37s", Healthy: true, Buckets: 1, Standalone: true, HasNodes: true,
		RF: 1, RFKnown: true, MemRatio: &ratio, MemLimit: 30786125824, User: "admin",
		Now: time.Date(2026, 10, 11, 9, 42, 30, 0, time.UTC),
	}

	var out bytes.Buffer
	c := NewCLI(WithTerminal(&out))
	c.theme.Colour = true
	c.drawBanner(facts)
	text := out.String()

	accent := c.theme.Styles["accent"]
	painted := func(value string) string { return coloured(value, accent, true) }
	escape := fgEscape(accent.FG)

	// The frame is painted as one run rather than cell by cell, so its title
	// comes with it: check the rules directly.
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if !strings.HasPrefix(lines[0], escape) || !strings.Contains(lines[0], bannerTitle) {
		t.Errorf("the top rule is not painted: %q", lines[0])
	}
	if !strings.HasPrefix(lines[len(lines)-1], escape) {
		t.Errorf("the bottom rule is not painted: %q", lines[len(lines)-1])
	}

	for _, want := range []string{
		"node", "address", "cluster", "process mem", "session", // the left column's words
		"version", "uptime", "RF", // and the right column's
	} {
		if !strings.Contains(text, painted(want)) {
			t.Errorf("the label %q is not painted", want)
		}
	}
	for _, want := range []string{
		"node-a", "127.0.0.1:28533", "standalone", // what the left column holds
		"0.1.0", "1h53m37s", itoa(facts.RF), // and the right
		facts.memory(), "2026-10-11 09:42:30",
	} {
		if strings.Contains(text, painted(want)) {
			t.Errorf("%q is painted, and it is a value", want)
		}
	}

	// Colour must not move anything: the box is the same box with escapes in it.
	for i, line := range lines {
		if width := len([]rune(ansi.ReplaceAllString(line, ""))); width != bannerOuter {
			t.Errorf("line %d is %d columns wide with colour on, want %d:\n%q", i, width, bannerOuter, line)
		}
	}
}

// bannerWithHealth draws `show banner` against a stub whose HEALTH reply is
// health, and returns the plain text the session printed. The node lists nothing,
// so the only reads are the ones the banner makes for every node.
func bannerWithHealth(t *testing.T, health server.HealthResponse) string {
	t.Helper()
	var out bytes.Buffer
	_, c := newStubServer(t, map[transport.Opcode]interface{}{
		transport.OpHealthCheck: health,
		transport.OpListNodes:   server.NodeListResponse{},
	}, &out)
	if err := c.handle("show banner"); err != nil {
		t.Fatalf("handle(\"show banner\"): %v", err)
	}
	return ansi.ReplaceAllString(out.String(), "")
}

// TestBannerHostRowsFollowTheReply is the table over the reply shapes an older,
// newer, containerised or non-Linux server can send. Each row must show what the
// reply carries and omit what it does not: no "0 B", no "n/a", no panic on a
// reply with no host.
func TestBannerHostRowsFollowTheReply(t *testing.T) {
	load := &server.HealthLoad{OneMinute: 0.42, FiveMinutes: 0.38, FifteenMinutes: 0.35}
	cases := []struct {
		name    string
		host    *server.HealthHost
		want    []string
		notWant []string
	}{
		{
			name: "a full host reply on a host with no container limit",
			host: &server.HealthHost{CPUs: 4, MemoryTotalBytes: 16 << 30, MemoryUsedBytes: 3 << 30,
				MemorySource: server.MemorySourceHost, Load: load},
			want:    []string{"cpus", "server ram", "3.0 GiB of 16.0 GiB", "load", "0.42 0.38 0.35"},
			notWant: []string{"on the server", "limit", "n/a", "0 B"},
		},
		{
			name: "a cgroup-limited node says how many CPUs the host has and where the memory limit came from",
			host: &server.HealthHost{CPUs: 2, HostCPUs: 64, MemoryTotalBytes: 4 << 30, MemoryUsedBytes: 1 << 30,
				MemorySource: server.MemorySourceCgroup, Load: load},
			want:    []string{"2 of 64 on the server", "server ram", "1.0 GiB of 4.0 GiB", "limit", "cgroup", "load"},
			notWant: []string{"n/a", "0 B"},
		},
		{
			name:    "a reply with only the CPU count, as a non-Linux server sends",
			host:    &server.HealthHost{CPUs: 4},
			want:    []string{"cpus"},
			notWant: []string{"server ram", "load", "limit", "on the server", "0 B", "n/a"},
		},
		{
			name:    "a reply with no host at all, as an older server sends",
			host:    nil,
			notWant: []string{"cpus", "server ram", "load", "limit", "n/a", "0 B"},
		},
		{
			name: "a host reply without load shows no load and no blank load",
			host: &server.HealthHost{CPUs: 4, MemoryTotalBytes: 16 << 30, MemoryUsedBytes: 3 << 30,
				MemorySource: server.MemorySourceHost},
			want:    []string{"cpus", "server ram", "3.0 GiB of 16.0 GiB"},
			notWant: []string{"load", "n/a", "0.00"},
		},
		{
			name:    "zero memory is left out rather than shown as 0 B",
			host:    &server.HealthHost{CPUs: 4, Load: load},
			want:    []string{"cpus", "load"},
			notWant: []string{"server ram", "0 B", "GiB", "n/a"},
		},
		{
			name:    "a total with no used figure shows the total alone",
			host:    &server.HealthHost{CPUs: 4, MemoryTotalBytes: 16 << 30, MemorySource: server.MemorySourceHost},
			want:    []string{"16.0 GiB total"},
			notWant: []string{"0 B", "n/a"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text := bannerWithHealth(t, server.HealthResponse{
				NodeID: "node-a", Uptime: "2m", Version: "0.1.0", Healthy: true, Host: tc.host,
			})
			if !strings.Contains(text, "┌") {
				t.Fatalf("the box was not drawn:\n%s", text)
			}
			for _, want := range tc.want {
				if !strings.Contains(text, want) {
					t.Errorf("the banner does not show %q:\n%s", want, text)
				}
			}
			for _, unwanted := range tc.notWant {
				if strings.Contains(text, unwanted) {
					t.Errorf("the banner shows %q, which this reply does not carry:\n%s", unwanted, text)
				}
			}
		})
	}
}

// TestBannerPrintsPlainLinesWhenHealthFails is the greeting's fallback: a node
// that cannot answer HEALTH gets the two plain lines the banner always printed,
// with no box and no host rows.
func TestBannerPrintsPlainLinesWhenHealthFails(t *testing.T) {
	var out bytes.Buffer
	_, c := newStubServer(t, map[transport.Opcode]interface{}{
		transport.OpListNodes: server.NodeListResponse{},
	}, &out)
	c.printBanner()
	text := ansi.ReplaceAllString(out.String(), "")
	if strings.Contains(text, "┌") || !strings.Contains(text, "hoardDB CLI") {
		t.Fatalf("a failed HEALTH read did not fall back to the plain greeting:\n%s", text)
	}
}

// TestBannerNamesTheProcessMemoryRow pins the row's words: the figure is the
// hoardDB process's own resident memory, so the label says process, and a
// reader does not mistake it for the machine's RAM in the server row below it.
func TestBannerNamesTheProcessMemoryRow(t *testing.T) {
	text := bannerWithHealth(t, server.HealthResponse{NodeID: "node-a", Uptime: "2m", Version: "0.1.0", Healthy: true})
	if !strings.Contains(text, "process mem") {
		t.Fatalf("the process memory row is not labelled process mem:\n%s", text)
	}
	if strings.Contains(text, "│ memory ") {
		t.Fatalf("the process memory row still says memory:\n%s", text)
	}
}

// bannerWithScript is bannerWithHealth with the node's list replies chosen by
// the caller, for the reads the replication factor makes.
func bannerWithScript(t *testing.T, health server.HealthResponse, extra map[transport.Opcode]interface{}) string {
	t.Helper()
	script := map[transport.Opcode]interface{}{
		transport.OpHealthCheck: health,
		transport.OpListNodes:   server.NodeListResponse{},
	}
	for op, reply := range extra {
		script[op] = reply
	}
	var out bytes.Buffer
	_, c := newStubServer(t, script, &out)
	if err := c.handle("show banner"); err != nil {
		t.Fatalf("handle(\"show banner\"): %v", err)
	}
	return ansi.ReplaceAllString(out.String(), "")
}

// TestBannerReplicationFactorStatesAreNeverClipped renders every state the
// cluster row's right-hand cell can take and checks that each one prints whole.
// The cell is 30 columns, the label "RF" takes three of them, so a value over 27
// columns is cut to "…" — which is how "— no buckets yet" used to print as
// "— no …". The states are the ones bannerReplicationFactor can return.
func TestBannerReplicationFactorStatesAreNeverClipped(t *testing.T) {
	health := func(buckets int) server.HealthResponse {
		return server.HealthResponse{NodeID: "node-a", Uptime: "2m", Version: "0.1.0", Healthy: true, BucketCount: buckets}
	}
	oneDB := map[transport.Opcode]interface{}{
		transport.OpListDBs: server.ListDatabasesResponse{Databases: []server.DatabaseInfo{{Name: "app", BucketCount: 2}}},
	}
	withBuckets := func(buckets ...server.BucketInfo) map[transport.Opcode]interface{} {
		script := map[transport.Opcode]interface{}{
			transport.OpListDBs:     oneDB[transport.OpListDBs],
			transport.OpListBuckets: server.ListBucketsResponse{Found: true, Database: "app", Buckets: buckets},
		}
		return script
	}
	cases := []struct {
		name    string
		buckets int
		extra   map[transport.Opcode]interface{}
		want    string
	}{
		{name: "no buckets yet", buckets: 0, want: "RF — no buckets yet"},
		{name: "too many buckets to read", buckets: 300, want: "RF — not read: >256 buckets"},
		{name: "a bucket inherits the node's factor", buckets: 2,
			extra: withBuckets(server.BucketInfo{Name: "users", Inherited: true, ReplicationFactor: 3}), want: "RF 3"},
		{name: "every bucket declares its own", buckets: 2,
			extra: withBuckets(server.BucketInfo{Name: "users", Inherited: false, ReplicationFactor: 2}), want: "RF — each bucket has its own"},
		{name: "the factor cannot be read", buckets: 2, want: "RF — unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text := bannerWithScript(t, health(tc.buckets), tc.extra)
			if !strings.Contains(text, tc.want) {
				t.Errorf("the cluster row does not show %q whole:\n%s", tc.want, text)
			}
			if strings.Contains(text, "…") {
				t.Errorf("the banner clipped a value:\n%s", text)
			}
		})
	}
}

// bannerWithNodes draws `show banner` against a stub whose node list is list,
// and returns the plain text the session printed.
func bannerWithNodes(t *testing.T, list server.NodeListResponse) string {
	t.Helper()
	var out bytes.Buffer
	_, c := newStubServer(t, map[transport.Opcode]interface{}{
		transport.OpHealthCheck: server.HealthResponse{NodeID: "n1", Uptime: "2m", Version: "0.1.0", Healthy: true},
		transport.OpListNodes:   list,
	}, &out)
	if err := c.handle("show banner"); err != nil {
		t.Fatalf("handle(\"show banner\"): %v", err)
	}
	return ansi.ReplaceAllString(out.String(), "")
}

// TestBannerMemberCountComesFromTheMemberSet is BUG-0708: the cluster line
// printed the length of the node list, which is the known-peer table, so a node
// that had heard from one peer of two members said "members 1".
func TestBannerMemberCountComesFromTheMemberSet(t *testing.T) {
	cases := []struct {
		name string
		list server.NodeListResponse
		want string
		not  string
	}{
		{
			name: "one known peer of two members",
			list: server.NodeListResponse{Nodes: []*server.NodeInfo{{NodeID: "n2"}}, MemberCount: 2, Epoch: 2},
			want: "members 2",
		},
		{
			name: "a one-member cluster is not standalone",
			list: server.NodeListResponse{MemberCount: 1, Epoch: 1},
			want: "members 1",
			not:  "standalone",
		},
		{
			name: "no cluster at all",
			list: server.NodeListResponse{Standalone: true},
			want: "standalone",
		},
		{
			name: "a server that predates the count is unknown, not zero",
			list: server.NodeListResponse{Nodes: []*server.NodeInfo{{NodeID: "n2"}, {NodeID: "n3"}}},
			want: "members unknown",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := bannerWithNodes(t, tc.list)
			if !strings.Contains(got, tc.want) {
				t.Errorf("the cluster line does not read %q:\n%s", tc.want, got)
			}
			if tc.not != "" && strings.Contains(got, tc.not) {
				t.Errorf("the banner says %q, which it must not:\n%s", tc.not, got)
			}
		})
	}
}
