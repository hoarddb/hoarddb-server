package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/server"
)

// ringBannerWho is what the banner knows about the session itself: the client's own facts, not the node's.
type ringBannerWho struct {
	Address, User, Database string
	Now                     time.Time
}

// ringBannerRows is the ring mode's part of the box. Its presence on bannerFacts switches boxLines to the
// ring rows: the cluster shape, RF as the node default, a process row measured by the node and a warning
// that never points at a statement ring mode cannot run.
type ringBannerRows struct {
	// Node is the node id, with its member state in brackets when the cluster has more than one member.
	Node string
	// Single is a cluster of at most one member: the box shows neither leader, epoch nor state.
	Single  bool
	Cluster string
	Leader  string
	Epoch   string
	RF      string
	// Process is empty when the reply carries no host part, and then the row is left out.
	Process string
	// Warning is empty when the node is serving.
	Warning string
}

// ringBannerFacts maps a describe_node reply into the facts the shared box draws. Every string the server
// chose is made safe for a terminal here, so the drawer only sees printable text.
func ringBannerFacts(node ringclient.NodeDescription, who ringBannerWho) bannerFacts {
	rows := &ringBannerRows{
		Node:   terminalSafe(node.NodeID),
		Single: node.Members <= 1,
		RF:     "default unknown",
	}
	if node.DefaultRF > 0 {
		rows.RF = fmt.Sprintf("default %d", node.DefaultRF)
	}
	if rows.Single {
		rows.Cluster = "single node"
	} else {
		rows.Cluster = fmt.Sprintf("%d members, %d voters", node.Members, node.Voters)
		if node.MemberState != "" {
			rows.Node += " (" + terminalSafe(node.MemberState) + ")"
		}
		rows.Leader = "none"
		if node.Leader != "" {
			rows.Leader = terminalSafe(node.Leader)
		}
		rows.Epoch = fmt.Sprintf("%d", node.Epoch)
	}
	switch node.Serving {
	case "", "ok":
	case "no_quorum":
		rows.Warning = fmt.Sprintf("no quorum: this node cannot reach a majority of the %d voters", node.Voters)
	case "lease_lapsed":
		rows.Warning = "lease lapsed: reads and writes are refused until it renews"
	case "epoch_behind":
		rows.Warning = "behind the cluster: this node learned a later epoch and refuses requests until it has applied it"
	case "replaying":
		rows.Warning = "replaying its log: this node serves nothing until it has applied its saved cluster state"
	case "diverged":
		rows.Warning = "diverged: this node refused a committed entry its leader cannot have appended and is stopping"
	default:
		rows.Warning = "not serving: " + terminalSafe(node.Serving)
	}
	facts := bannerFacts{
		Address:  who.Address,
		User:     who.User,
		Database: who.Database,
		NodeID:   terminalSafe(node.NodeID),
		Version:  terminalSafe(node.Version),
		Uptime:   ringUptime(node.UptimeMS),
		Healthy:  rows.Warning == "",
		Now:      who.Now,
		Ring:     rows,
	}
	if host := node.Host; host != nil {
		facts.Host = ringBannerHost(host)
		if host.Process != nil && host.Process.Limit > 0 {
			rows.Process = fmt.Sprintf("%s of %s (%.1f%%)", humanBytes(host.Process.Used), humanBytes(host.Process.Limit),
				float64(host.Process.Used)/float64(host.Process.Limit)*100)
		}
	}
	return facts
}

// ringBannerHost reuses the machine rows of v1.0, which read a server.HealthHost.
func ringBannerHost(host *ringclient.HostDescription) *server.HealthHost {
	out := &server.HealthHost{
		CPUs:             int(host.CPUs),
		HostCPUs:         int(host.HostCPUs),
		MemoryTotalBytes: host.MemTotal,
		MemoryUsedBytes:  host.MemUsed,
		MemorySource:     host.MemSource,
	}
	if host.Load != nil {
		out.Load = &server.HealthLoad{OneMinute: host.Load.One, FiveMinutes: host.Load.Five, FifteenMinutes: host.Load.Fifteen}
	}
	return out
}

// ringUptime prints a duration the way v1.0 prints its uptime: whole seconds, hours without days.
func ringUptime(milliseconds int64) string {
	if milliseconds < 0 {
		milliseconds = 0
	}
	return (time.Duration(milliseconds) * time.Millisecond).Truncate(time.Second).String()
}

// ringLines is the box of a ring node: the rows of v1.0 with the cluster rows the document specifies.
func (f bannerFacts) ringLines() []bannerLine {
	r := f.Ring
	lines := []bannerLine{
		bannerRow("node", r.Node, "version", f.Version),
		bannerRow("address", f.Address, "uptime", f.Uptime),
	}
	if r.Single {
		lines = append(lines, bannerRow("cluster", r.Cluster, "RF", r.RF))
	} else {
		lines = append(lines, bannerRow("cluster", r.Cluster, "leader", r.Leader), bannerRow("epoch", r.Epoch, "RF", r.RF))
	}
	if r.Process != "" {
		lines = append(lines, bannerRow("process mem", r.Process, "", ""))
	}
	lines = append(lines, f.serverLines()...)
	lines = append(lines, bannerRow("session", f.session(), "", f.Now.Format("2006-01-02 15:04:05")))
	if r.Warning != "" {
		lines = append(lines, bannerLine{cells: []bannerCell{
			{label: "warning", width: bannerLabel},
			{value: r.Warning, width: bannerInside - bannerLabel},
		}})
	}
	return lines
}

// WithRingIdentity tells the session the address it dialled and the user it logged in as, for the banner.
func WithRingIdentity(address, user string) RingOption {
	return func(s *RingSession) { s.address, s.user = address, user }
}

func (s *RingSession) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

// bannerBox asks the node about itself and draws the box. The node is returned so the greeting can tell
// whether it is serving. Any error means the node cannot be asked.
func (s *RingSession) bannerBox(ctx context.Context, theme *Theme) (string, ringclient.NodeDescription, error) {
	ctx, cancel := context.WithTimeout(ctx, ringNameCall)
	defer cancel()
	node, err := s.api.DescribeNode(ctx)
	if err != nil {
		return "", node, fmt.Errorf("describing the node for the banner: %w", err)
	}
	facts := ringBannerFacts(node, ringBannerWho{Address: s.address, User: s.user, Database: s.selected, Now: s.now()})
	return neutralizeControls(renderBannerBox(theme, facts)), node, nil
}

// showBanner is `show banner`: the box again, without colour because statement output is escaped text.
func (s *RingSession) showBanner(ctx context.Context) (RingOutput, error) {
	plain := *s.theme
	plain.Colour = false
	box, _, err := s.bannerBox(ctx, &plain)
	if err != nil {
		return RingOutput{}, &RingError{Message: "this server does not answer describe_node, so there is no banner to show. " +
			"Run 'show databases' to see what the server holds", Exit: ExitRefused, Err: err}
	}
	return ringLines(strings.Split(strings.TrimRight(box, "\n"), "\n")...), nil
}
