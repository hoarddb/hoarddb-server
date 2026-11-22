package cli

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
	"github.com/hoarddb/hoarddb-server/version"
)

// The banner is the session's greeting: a boxed summary of the node the CLI is
// connected to, plus the line that says how to get help. It prints only for an
// interactive session — a piped run keeps the two plain lines it always had, so
// a script's stdout does not grow a frame it has to skip.
//
// Everything in it is measured. The node lines and the server rows come from
// the health response; the server rows describe the machine the node runs on,
// which a CLI on another machine can be talking to. Nothing is estimated and
// nothing is invented: a fact the wire does not carry is left out rather than
// guessed, and an older server's reply shows fewer rows.
const (
	bannerLabel  = 12 // the label column; wide enough for "process mem" and "server ram" with a gap
	bannerValue  = 30 // the left value column
	bannerRight  = 30 // the right value column
	bannerInside = bannerLabel + bannerValue + bannerRight
	// bannerOuter is what one content row occupies: the two border characters
	// and the two spaces holding them off the text. The top and bottom rules
	// are built from it, so the frame cannot disagree with the box — it did,
	// by three columns, until TestBannerLinesAreAllTheSameWidth caught it.
	bannerOuter = bannerInside + 4
	bannerTitle = "hoardDB"
)

// bannerRFBucketBudget bounds the one non-O(1) read the banner makes. The node's
// replication factor travels only per bucket, so reading it costs a LIST_DBS and
// a LIST_BUCKETS over the first database. That is fine for the handful of
// buckets a development node holds and wrong for the many small buckets this
// database is built for, so above this many buckets the line says so instead.
// The real fix is a node-level field on the health response, which is a server
// change and not this dispatch's file.
const bannerRFBucketBudget = 256

// bannerFacts is what the box shows, all of it read, none of it assumed.
type bannerFacts struct {
	Address string
	NodeID  string
	Version string
	Uptime  string
	Healthy bool
	Buckets int
	Members int
	// Standalone is a node with no cluster at all, which is not a cluster of
	// one member. HasNodes is false when the server did not say which it is.
	Standalone bool
	HasNodes   bool
	RF         int
	RFKnown    bool
	RFReason   string
	MemRatio   *float64
	MemLimit   int64
	Host       *server.HealthHost
	User       string
	Database   string
	Reasons    []string
	Now        time.Time
	// Ring, when set, replaces the legacy cluster, RF, memory and warning rows with the ring node's.
	Ring *ringBannerRows
}

// printBanner greets an interactive session. A banner is a greeting, not a
// statement: if the node cannot be asked, the session prints the plain lines it
// always printed rather than an error the user did not earn. `show banner` is
// the other half of that — see showBanner.
func (c *CLI) printBanner() {
	facts, err := c.gatherBannerFacts()
	if err != nil {
		c.printf("hoardDB CLI %s\n", version.Version)
		c.printf("Type 'help' for help, 'exit' to quit.\n\n")
		return
	}
	c.drawBanner(facts)
	c.printf("Type 'help' for help, 'exit' to quit.\n\n")
}

// showBanner prints the box on demand (`show banner`). The greeting prints once
// and scrolls away; this is how a session that has grown a screen of output, or
// a script that wants the node's state as one block, gets it back.
//
// It reports a failed read instead of falling back quietly the way the greeting
// does: a command that printed two unrelated lines would hide why.
func (c *CLI) showBanner() error {
	if c.handler == nil {
		return ErrNotConnected
	}
	facts, err := c.gatherBannerFacts()
	if err != nil {
		return fmt.Errorf("reading the node for the banner: %w", err)
	}
	c.drawBanner(facts)
	return nil
}

// gatherBannerFacts reads the node. A non-nil error is the caller's cue: the
// greeting falls back to the plain lines, the command reports it.
func (c *CLI) gatherBannerFacts() (bannerFacts, error) {
	facts := bannerFacts{
		Address:  c.addr,
		User:     c.username,
		Database: c.currentDB,
		Version:  version.Version,
		Now:      time.Now(),
	}
	if c.handler == nil {
		return facts, errors.New("no server connection")
	}

	var health server.HealthResponse
	if err := c.handler.send(transport.OpHealthCheck, &server.HealthRequest{}, &health); err != nil {
		return facts, err
	}
	facts.NodeID = health.NodeID
	facts.Uptime = health.Uptime
	if health.Version != "" {
		facts.Version = health.Version
	}
	facts.Healthy = health.Healthy
	facts.Buckets = health.BucketCount
	facts.Reasons = health.Reasons
	facts.Host = health.Host
	if health.Memtable != nil {
		facts.MemLimit = health.Memtable.LimitBytes
	}
	if health.Capacity != nil {
		for _, row := range health.Capacity.Rows {
			if row.Resource == "memory" {
				facts.MemRatio = row.Ratio
				break
			}
		}
	}

	// The member count is the server's, read from its epoch record: the rows of
	// the list are the peers it has heard from, not the members. A reply with
	// neither a count nor the standalone mark is from a server that predates
	// them, and says nothing about membership.
	var nodes server.NodeListResponse
	if err := c.handler.send(transport.OpListNodes, &server.NodeListRequest{}, &nodes); err == nil {
		facts.Standalone = nodes.Standalone
		facts.Members = nodes.MemberCount
		facts.HasNodes = nodes.Standalone || nodes.MemberCount > 0
	}

	facts.RF, facts.RFKnown, facts.RFReason = c.bannerReplicationFactor(facts.Buckets)
	return facts, nil
}

// bannerReplicationFactor reads the node's own factor off a bucket that
// inherited it. Refusing to read it costs one line of the box; reading it at
// every session start on a node with 10^6 buckets would cost the session.
func (c *CLI) bannerReplicationFactor(bucketCount int) (int, bool, string) {
	if bucketCount == 0 {
		return 0, false, "no buckets yet"
	}
	if bucketCount > bannerRFBucketBudget {
		return 0, false, fmt.Sprintf("not read: >%d buckets", bannerRFBucketBudget)
	}
	var dbs server.ListDatabasesResponse
	if err := c.handler.send(transport.OpListDBs, &server.ListDatabasesRequest{}, &dbs); err != nil || len(dbs.Databases) == 0 {
		return 0, false, "unknown"
	}
	for _, db := range dbs.Databases {
		var buckets server.ListBucketsResponse
		if err := c.handler.send(transport.OpListBuckets, &server.ListBucketsRequest{Database: db.Name}, &buckets); err != nil {
			continue
		}
		for _, bucket := range buckets.Buckets {
			if bucket.Inherited && bucket.ReplicationFactor > 0 {
				return bucket.ReplicationFactor, true, ""
			}
		}
	}
	return 0, false, "each bucket has its own"
}

// drawBanner renders the box: the frame, the labels and the four values a user
// reads rather than copies — the version, the uptime, the replication factor and
// the memory — in the theme's accent, the purple the prompt's own mark carries.
// The rest keep the terminal's own foreground, which is what stays legible on a
// colour scheme this CLI has never seen.
func (c *CLI) drawBanner(f bannerFacts) {
	c.printf("%s", renderBannerBox(c.theme, f))
}

// renderBannerBox is the one drawer legacy and ring mode share: the frame and the rows of one fact
// set, in the theme's accent when the theme has colour. It touches no session, so a mode only has to
// fill bannerFacts. Every line ends with a newline.
func renderBannerBox(theme *Theme, f bannerFacts) string {
	accent, hasAccent := theme.Styles["accent"]
	paint := func(text string) string {
		if !hasAccent {
			return text
		}
		return coloured(text, accent, theme.Colour)
	}

	var box strings.Builder
	lead := "┌─ " + bannerTitle + " "
	top := lead + strings.Repeat("─", bannerOuter-len([]rune(lead))-1) + "┐"
	box.WriteString(paint(top) + "\n")
	for _, line := range f.boxLines() {
		box.WriteString(paint("│") + " " + line.render(paint) + " " + paint("│") + "\n")
	}
	box.WriteString(paint("└"+strings.Repeat("─", bannerOuter-2)+"┘") + "\n")
	return box.String()
}

// bannerCell is one field of a row: an optional label word and a value, padded
// to its column.
//
// The label wears the accent so a field can be found at a glance; the value is
// left in the terminal's own foreground, because the value is what a reader has
// to trust on a colour scheme this CLI has never seen.
type bannerCell struct {
	label string // the word naming the field; empty for a value-only cell
	value string // what the field holds; empty for a label-only cell
	width int    // the column's width, in character cells
}

// render lays the field out at its column width, painting the label and nothing
// else: neither the value nor the padding after it wears the accent, so the
// colour marks the words a reader looks for rather than a band of columns.
func (c bannerCell) render(paint func(string) string) string {
	if c.label == "" {
		return padTo(clip(c.value, c.width), c.width)
	}
	label := clip(c.label, c.width)
	room := c.width - len([]rune(label)) - 1
	if room <= 0 {
		// No room for a separator and a value: the label is the whole field.
		return paint(label) + padTo("", c.width-len([]rune(label)))
	}
	plain := padTo(label+" "+clip(c.value, room), c.width)
	return paint(label) + plain[len(label):]
}

// bannerLine is one row of the box. A row is the label column followed by the two
// value columns; the warning row is a label and one field that spans the rest of
// the box.
type bannerLine struct {
	cells []bannerCell
}

// bannerRow builds a three-column row. The constructor owns the column widths, so
// a caller cannot lay a row out wrong.
func bannerRow(label, value, rightLabel, rightValue string) bannerLine {
	return bannerLine{cells: []bannerCell{
		{label: label, width: bannerLabel},
		{value: value, width: bannerValue},
		{label: rightLabel, value: rightValue, width: bannerRight},
	}}
}

// render lays the row out: every cell at its column, so the row is exactly as
// wide as the box.
func (l bannerLine) render(paint func(string) string) string {
	var b strings.Builder
	for _, cell := range l.cells {
		b.WriteString(cell.render(paint))
	}
	return b.String()
}

// boxLines is the content, one row per line, each already laid out in the three
// columns.
func (f bannerFacts) boxLines() []bannerLine {
	if f.Ring != nil {
		return f.ringLines()
	}
	// The right column names its field, so the factor's own words stay with it:
	// the value is the number, or the reason there is none.
	rfValue := itoa(f.RF)
	if !f.RFKnown {
		rfValue = "— " + f.RFReason
	}
	cluster := "members " + itoa(f.Members)
	if !f.HasNodes {
		cluster = "members unknown"
	} else if f.Standalone {
		// A node with no cluster has no member set at all, which is not the
		// same as a cluster of one: the server says which this is.
		cluster = "standalone"
	}

	// Every word that names a field is accent; every value is the terminal's
	// own foreground, so nothing a reader has to compare or copy is coloured
	// by this CLI's taste.
	lines := []bannerLine{
		bannerRow("node", f.NodeID, "version", f.Version),
		bannerRow("address", f.Address, "uptime", f.Uptime),
		bannerRow("cluster", cluster, "RF", rfValue),
		bannerRow("process mem", f.memory(), "", ""),
	}
	lines = append(lines, f.serverLines()...)
	lines = append(lines, bannerRow("session", f.session(), "", f.Now.Format("2006-01-02 15:04:05")))
	if !f.Healthy {
		// The box shows no bucket, memtable or encryption detail — `status`
		// does, and repeating it here made the greeting a dashboard. A node
		// that is not healthy is the one thing a session must not hide, so it
		// survives as a line of its own. It spans the box rather than the value
		// column, which is 32 columns wide and would clip the reason away, and
		// it names where the whole of it lives.
		// The line is 62 columns, so the reason gets a fixed share and the
		// pointer to the whole of it survives whatever the reason's length.
		const reasonBudget = 26
		detail := "unhealthy — run 'status' for the reasons"
		if len(f.Reasons) > 0 {
			detail = "unhealthy — " + clip(f.Reasons[0], reasonBudget) + " (see 'status')"
		}
		lines = append(lines, bannerLine{cells: []bannerCell{
			{label: "warning", width: bannerLabel},
			{value: detail, width: bannerInside - bannerLabel},
		}})
	}
	return lines
}

// serverLines is the machine the node runs on: its CPUs with the load, and its
// memory. Each row is left out when the reply does not carry what it shows, so
// an older server, a non-Linux one and a reply with zero memory each print the
// rows they have and nothing that reads as a measurement they did not make.
func (f bannerFacts) serverLines() []bannerLine {
	h := f.Host
	if h == nil {
		return nil
	}
	var lines []bannerLine
	load := ""
	if h.Load != nil {
		load = fmt.Sprintf("%.2f %.2f %.2f", h.Load.OneMinute, h.Load.FiveMinutes, h.Load.FifteenMinutes)
	}
	if h.CPUs > 0 {
		cpus := itoa(h.CPUs)
		if h.HostCPUs > 0 {
			cpus = fmt.Sprintf("%d of %d on the server", h.CPUs, h.HostCPUs)
		}
		loadLabel := ""
		if load != "" {
			loadLabel = "load"
		}
		lines = append(lines, bannerRow("cpus", cpus, loadLabel, load))
	} else if load != "" {
		lines = append(lines, bannerRow("load", load, "", ""))
	}
	if h.MemoryTotalBytes > 0 {
		value := humanBytes(h.MemoryTotalBytes) + " total"
		if h.MemoryUsedBytes > 0 {
			value = humanBytes(h.MemoryUsedBytes) + " of " + humanBytes(h.MemoryTotalBytes)
		}
		sourceLabel, sourceValue := "", ""
		if h.MemorySource == server.MemorySourceCgroup {
			sourceLabel, sourceValue = "limit", "cgroup"
		}
		lines = append(lines, bannerRow("server ram", value, sourceLabel, sourceValue))
	}
	return lines
}

// memory is the node's own measurement of what it is using: the capacity row's
// ratio is its resident anonymous memory plus swap, over the limit it chose.
func (f bannerFacts) memory() string {
	if f.MemRatio == nil || f.MemLimit <= 0 {
		return "not sampled yet"
	}
	used := int64(*f.MemRatio * float64(f.MemLimit))
	return fmt.Sprintf("%s of %s (%.1f%%)", humanBytes(used), humanBytes(f.MemLimit), *f.MemRatio*100)
}

func (f bannerFacts) session() string {
	user := f.User
	if user == "" {
		user = "unknown user"
	}
	if f.Database == "" {
		return user + " — no database selected"
	}
	return user + " @ " + f.Database
}

// padTo pads s with spaces to width, and clips it when it is longer, so one
// long value cannot push the right border out of line.
func padTo(s string, width int) string {
	s = clip(s, width)
	if pad := width - len([]rune(s)); pad > 0 {
		return s + strings.Repeat(" ", pad)
	}
	return s
}

// clip shortens s to width runes, marking the cut so a shortened value is not
// mistaken for a complete one.
func clip(s string, width int) string {
	runes := []rune(s)
	if len(runes) <= width {
		return s
	}
	if width <= 1 {
		return string(runes[:width])
	}
	return string(runes[:width-1]) + "…"
}

// itoa is the integer-to-string the box needs, without importing strconv into
// a file that has no other use for it.
func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
