package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/version"
)

// u84Now is the clock of every banner golden.
var u84Now = time.Date(2026, 10, 31, 16, 38, 25, 0, time.UTC)

// u84Host is the machine part of the documents' screens: 16 CPUs, 7.5 of 28.7 GiB, a server using 61.2 MiB.
func u84Host() *ringclient.HostDescription {
	return &ringclient.HostDescription{CPUs: 16, Load: &ringclient.Load{One: 0.42, Five: 0.51, Fifteen: 0.60},
		MemUsed: 8053063680, MemTotal: 30815000000, MemSource: "host",
		Process: &ringclient.ProcessMemory{Used: 64175309, Limit: 30815000000}}
}

func u84OneNode() ringclient.NodeDescription {
	return ringclient.NodeDescription{NodeID: "node-a", Version: "1.1.0-dev", UptimeMS: 312000, MemberState: "active", Members: 1, Voters: 1,
		Epoch: 1, Leader: "node-a", DefaultRF: 1, Serving: "ok", Host: u84Host()}
}

func u84ClusterNode() ringclient.NodeDescription {
	node := u84OneNode()
	node.UptimeMS = (76*3600 + 12*60 + 5) * 1000
	node.Members, node.Voters, node.Epoch, node.Leader, node.DefaultRF = 4, 3, 4, "node-b", 2
	node.Host = &ringclient.HostDescription{CPUs: 4, Load: &ringclient.Load{One: 1.02, Five: 0.98, Fifteen: 0.91},
		MemUsed: 6552000000, MemTotal: 16744000000, MemSource: "host", Process: &ringclient.ProcessMemory{Used: 1288490189, Limit: 16744000000}}
	return node
}

func u84NotServing(serving string) ringclient.NodeDescription {
	node := u84ClusterNode()
	node.NodeID, node.UptimeMS, node.Leader, node.Serving = "node-c", (2*3600+3*60+10)*1000, "", serving
	node.Host.Load = &ringclient.Load{One: 0.20, Five: 0.31, Fifteen: 0.40}
	node.Host.MemUsed, node.Host.Process = 6341000000, &ringclient.ProcessMemory{Used: 1170000000, Limit: 16744000000}
	return node
}

// u84Session is a session on a fake whose node is node, with the clock fixed and the identity of the documents.
func u84Session(node ringclient.NodeDescription, databases ...string) (*RingSession, *u52Fake) {
	session, fake := u52Session()
	fake.node = node
	session.clock = func() time.Time { return u84Now }
	WithRingIdentity("192.0.2.10:7533", "admin")(session)
	for _, name := range databases {
		if _, err := session.Execute(context.Background(), "create database "+name); err != nil {
			panic(err)
		}
	}
	return session, fake
}

// u84AtNodeC is a session on the third node of the document's cluster, which listens on its own address.
func u84AtNodeC(serving string) *RingSession {
	session, _ := u84Session(u84NotServing(serving))
	WithRingIdentity("192.0.2.12:7533", "admin")(session)
	return session
}

func u84Greeting(session *RingSession, boxed bool) string {
	var out bytes.Buffer
	session.greetInteractive(context.Background(), &out, boxed)
	// The fallback line carries the CLI's own version, which a build stamps; the goldens are written for 1.1.0-dev.
	return strings.ReplaceAll(out.String(), "hoardDB CLI "+version.Version, "hoardDB CLI 1.1.0-dev")
}

// TestBannerGoldens pins the screens of the review document: one node, a cluster, a node that is not serving
// (no quorum, lease lapsed), a reply without host, and the older server. [GUARD every row, its wording and
// the order of the lines under the box.]
func TestBannerGoldens(t *testing.T) {
	older, olderFake := u84Session(ringclient.NodeDescription{})
	olderFake.failures["DescribeNode"] = &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: `cmd "describe_node" is not an admin command`}
	noHost := u84OneNode()
	noHost.Host = nil
	clusterSession, _ := u84Session(u84ClusterNode(), "app")
	if _, err := clusterSession.Execute(context.Background(), "use app"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		slug    string
		session func() *RingSession
	}{
		{"banner-one-node", func() *RingSession { s, _ := u84Session(u84OneNode()); return s }},
		{"banner-cluster", func() *RingSession { return clusterSession }},
		{"banner-not-serving", func() *RingSession { return u84AtNodeC("no_quorum") }},
		{"banner-lease-lapsed", func() *RingSession { return u84AtNodeC("lease_lapsed") }},
		{"banner-no-host", func() *RingSession { s, _ := u84Session(noHost); return s }},
		{"banner-older-server", func() *RingSession { return older }},
	}
	for _, c := range cases {
		t.Run(c.slug, func(t *testing.T) {
			assertRingGolden(t, c.slug, u84Greeting(c.session(), true))
		})
	}
}

// TestBannerLeaseLapsedWarning: the warning row names the lapsed lease and no hint line follows, because
// show_databases would be refused. [GUARD the serving value picks the warning text.]
func TestBannerLeaseLapsedWarning(t *testing.T) {
	session, fake := u84Session(u84NotServing("lease_lapsed"))
	got := u84Greeting(session, true)
	if !strings.Contains(got, "│ warning     lease lapsed: reads and writes are refused until it renews   │\n") {
		t.Errorf("lease lapsed box:\n%s", got)
	}
	for _, call := range fake.calls {
		if call == "ShowDatabases" {
			t.Error("a node that is not serving was asked for its databases")
		}
	}
}

// TestBannerLinesAreAllTheSameWidthForRingFacts: whatever the node reports, every line of the box is as wide
// as the frame, and a long value is cut with an ellipsis (v1.0's narrow rules). [GUARD the ring rows go through
// the shared cells.]
func TestBannerLinesAreAllTheSameWidthForRingFacts(t *testing.T) {
	long := strings.Repeat("n", 90)
	nodes := map[string]ringclient.NodeDescription{"one": u84OneNode(), "cluster": u84ClusterNode(), "no quorum": u84NotServing("no_quorum")}
	huge := u84NotServing("no_quorum")
	huge.NodeID, huge.Leader, huge.Version, huge.MemberState, huge.Voters = long, long, long, long, 1234567
	nodes["long values"] = huge
	for name, node := range nodes {
		session, _ := u84Session(node)
		facts := ringBannerFacts(node, ringBannerWho{Address: long, User: long, Database: long, Now: u84Now})
		box := renderBannerBox(session.theme, facts)
		for _, line := range strings.Split(strings.TrimRight(box, "\n"), "\n") {
			if got := len([]rune(line)); got != bannerOuter {
				t.Errorf("%s: line is %d columns, want %d: %q", name, got, bannerOuter, line)
			}
		}
		if name == "long values" && !strings.Contains(box, "…") {
			t.Errorf("long values were not cut with an ellipsis:\n%s", box)
		}
	}
}

// TestBannerPipedIsUnchanged: a script's stdout keeps the two plain lines, byte for byte, and the server is
// asked nothing for the greeting. [GUARD RunPiped never draws or asks.]
func TestBannerPipedIsUnchanged(t *testing.T) {
	session, fake := u84Session(u84OneNode())
	var out, errOut bytes.Buffer
	if status := session.RunPiped(context.Background(), strings.NewReader(""), &out, &errOut); status != 0 {
		t.Fatalf("status %d: %s", status, errOut.String())
	}
	if want := "hoardDB CLI " + version.Version + "\nType 'help' for help, 'exit' to quit.\n\n"; out.String() != want {
		t.Errorf("piped greeting: %q, want %q", out.String(), want)
	}
	if strings.Contains(out.String(), "┌") || len(fake.calls) != 0 {
		t.Errorf("a piped run drew a box or asked the server: %q %v", out.String(), fake.calls)
	}
}

// TestBannerInteractiveWithRedirectedOutputKeepsPlainLines: a terminal on stdin and a file on stdout is v1.0's
// plain-lines case; no box and no describe_node. [GUARD plain gates the box.]
func TestBannerInteractiveWithRedirectedOutputKeepsPlainLines(t *testing.T) {
	session, fake := u84Session(u84OneNode())
	var out bytes.Buffer
	session.RunInteractive(context.Background(), &out, true)
	if strings.Contains(out.String(), "┌") {
		t.Errorf("a plain session drew the box:\n%s", out.String())
	}
	for _, call := range fake.calls {
		if call == "DescribeNode" {
			t.Error("a plain session asked for describe_node")
		}
	}
}

// TestBannerRunInteractiveDrawsTheBox goes through the interactive code path: not plain, so the box is the
// greeting and the first prompt follows it.
func TestBannerRunInteractiveDrawsTheBox(t *testing.T) {
	session, _ := u84Session(u84OneNode())
	var out bytes.Buffer
	session.RunInteractive(context.Background(), &out, false)
	text := out.String()
	if !strings.HasPrefix(text, "┌─ hoardDB ") || !strings.Contains(text, "│ cluster     single node") || !strings.Contains(text, "Type 'help' to get started") {
		t.Errorf("interactive greeting:\n%s", text)
	}
}

// TestShowBanner: the statement draws the box again, and on a server that cannot answer it refuses with the
// document's two lines. [GUARD show banner is in the grammar and reports the failure.]
func TestShowBanner(t *testing.T) {
	session, fake := u84Session(u84ClusterNode())
	output, err := session.Execute(context.Background(), "show banner")
	if err != nil {
		t.Fatal(err)
	}
	box := strings.Join(output.Lines, "\n")
	if !strings.HasPrefix(box, "┌─ hoardDB ") || !strings.Contains(box, "│ cluster     4 members, 3 voters           leader node-b") || !strings.HasSuffix(box, "┘") {
		t.Errorf("show banner:\n%s", box)
	}
	fake.failures["DescribeNode"] = &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: `cmd "describe_node" is not an admin command`}
	_, err = session.Execute(context.Background(), "show banner")
	var failed *RingError
	if !errors.As(err, &failed) {
		t.Fatalf("show banner on an older server gave %v", err)
	}
	lines := strings.Join(ringErrorLines(err), "\n")
	want := "Error: this server does not answer describe_node, so there is no banner to show.\n       Fix:   Run 'show databases' to see what the server holds."
	if !strings.Contains(lines, "this server does not answer describe_node, so there is no banner to show.") || !strings.Contains(lines, "Run 'show databases' to see what the server holds") {
		t.Errorf("refusal:\n%s\nwant like\n%s", lines, want)
	}
}

// TestBannerIsListedInHelp: 'help all' lists show banner under Session. [GUARD the form is in the table.]
func TestBannerIsListedInHelp(t *testing.T) {
	if !strings.Contains(HelpAll(), "show banner") {
		t.Errorf("help all does not list show banner")
	}
}

// TestLiveBannerOnARealNode: the real describe_node reply of a fresh in-process server draws a sane
// one-node box: single node, RF default 1, a version and an uptime, the machine rows, no warning, and the
// greeting follows it with the hint for an empty server.
func TestLiveBannerOnARealNode(t *testing.T) {
	session, ctx := u52LiveSession(t)
	WithRingIdentity("192.0.2.10:7533", "root")(session)
	var out bytes.Buffer
	session.greetInteractive(ctx, &out, true)
	text := out.String()
	t.Logf("\n%s", text)
	for _, want := range []string{"┌─ hoardDB ", "│ node        ", "│ cluster     single node", "RF default 1", "│ session     root — no database selected",
		"No databases yet. Start with: create database app\n", "Type 'help' to get started, 'exit' to quit.\n\n"} {
		if !strings.Contains(text, want) {
			t.Errorf("greeting lacks %q", want)
		}
	}
	for _, unwanted := range []string{"warning", "leader", "epoch", "0 B", "n/a", "unknown"} {
		if strings.Contains(text, unwanted) {
			t.Errorf("greeting has %q:\n%s", unwanted, text)
		}
	}
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "│") || strings.HasPrefix(line, "┌") || strings.HasPrefix(line, "└") {
			if got := len([]rune(line)); got != bannerOuter {
				t.Errorf("line is %d columns, want %d: %q", got, bannerOuter, line)
			}
		}
	}
	output, err := session.Execute(ctx, "show banner")
	if err != nil || len(output.Lines) < 5 {
		t.Fatalf("show banner on a real node: %v %v", output.Lines, err)
	}
}

// TestBannerOnAPty runs the interactive loop on a pseudo-terminal: the box is the greeting, the typed
// `exit` ends the session, and nothing reaches the output before the box.
func TestBannerOnAPty(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	inMaster, inSlave := openTestPty(t)
	setPtySize(t, inSlave, 24, 80)
	swapStdin(t, inSlave)
	feedPty(t, inMaster, "exit\n")
	session, _ := u84Session(u84ClusterNode(), "app")
	var out bytes.Buffer
	if status := session.RunInteractive(context.Background(), &out, false); status != 0 {
		t.Fatalf("status %d: %q", status, out.String())
	}
	text := ansi.ReplaceAllString(out.String(), "")
	if !strings.HasPrefix(text, "┌─ hoardDB ") || !strings.Contains(text, "leader node-b") || !strings.Contains(text, "Goodbye.") {
		t.Errorf("pty session:\n%s", text)
	}
}
