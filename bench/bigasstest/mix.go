package main

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// profile is a named set of per-layout read shares: the share of a layout's requests that read,
// the rest write. Write weights and the weights within reads do not depend on it.
type profile struct {
	// Name is "R" or "S" and goes into every record.
	Name string
	// ReadShare is the share of each layout's requests that read.
	ReadShare map[model.Layout]float64
}

// The two profiles. R is the headline mix (owner ruling 2026-11-07): application-shape
// judgements of how read-heavy each layout is, 67.5% reads when every layout gets a quarter of
// the requests. S is the layouts spec's own shares, which are write-heavy in every layout; it
// runs as a secondary leg.
var (
	profileR = profile{Name: "R", ReadShare: map[model.Layout]float64{model.Social: 0.90, model.Chat: 0.60, model.Ledger: 0.70, model.Agent: 0.50}}
	profileS = profile{Name: "S", ReadShare: map[model.Layout]float64{model.Social: 0.20, model.Chat: 0.30, model.Ledger: 0.30, model.Agent: 0.10}}
)

// profileByName returns profile "R" or "S".
func profileByName(name string) (profile, error) {
	switch name {
	case "R":
		return profileR, nil
	case "S":
		return profileS, nil
	}
	return profile{}, fmt.Errorf("-profile %q is not a profile. Use R (per-layout read shares 90/60/70/50, the headline) or S (the layouts spec's 20/30/30/10)", name)
}

// pooledReads is the share of all requests that read when every layout in layouts gets an
// equal share of the requests.
func (p profile) pooledReads(layouts []model.Layout) float64 {
	var total float64
	for _, layout := range layouts {
		total += p.ReadShare[layout]
	}
	return total / float64(len(layouts))
}

// weighted is one operation and its weight within the reads or the writes of its layout.
type weighted struct {
	kind   string
	weight float64
}

// opTable holds the weights within the reads and within the writes of each layout. The write
// weights are the layouts spec's shares renormalised within writes; the read weights are the
// owner's ruling of 2026-11-07 (social feed 10, inbox 5, followers 5; chat room page 70, user
// rooms 30; ledger account page 70, invoices page 30; agent memories 50, session page 50).
var opTable = map[model.Layout]struct{ reads, writes []weighted }{
	model.Social: {
		reads:  []weighted{{string(ReadSocialFeed), 10}, {string(ReadSocialInbox), 5}, {string(ReadSocialFollowers), 5}},
		writes: []weighted{{string(WriteSocialPost), 25}, {string(WriteSocialComment), 15}, {string(WriteSocialLike), 15}, {string(WriteSocialFollow), 10}, {string(WriteSocialGroupPost), 5}, {string(WriteSocialSettings), 5}},
	},
	model.Chat: {
		reads:  []weighted{{string(ReadChatRoomPage), 70}, {string(ReadChatUserRooms), 30}},
		writes: []weighted{{string(WriteChatMessage), 50}, {string(WriteChatJoin), 20}},
	},
	model.Ledger: {
		reads:  []weighted{{string(ReadLedgerPage), 70}, {string(ReadLedgerInvoices), 30}},
		writes: []weighted{{string(WriteLedgerExpense), 50}, {string(WriteLedgerInvoice), 20}},
	},
	model.Agent: {
		reads:  []weighted{{string(ReadAgentMemories), 50}, {string(ReadAgentSession), 50}},
		writes: []weighted{{string(WriteAgentMessage), 40}, {string(WriteAgentTask), 20}, {string(WriteAgentScratch), 20}, {string(WriteAgentMemory), 10}},
	},
}

// recordOnlyReads are operations that exist but are not in the mix; -op selects one.
var recordOnlyReads = []weighted{{string(ReadSocialFollowScan), 1}, {string(ReadLedgerAll), 1}, {string(ReadLedgerTotals), 1}}

// mixEntry is one operation a request can be.
type mixEntry struct {
	Layout model.Layout
	Write  bool
	Kind   string
}

// Mix draws what a request does.
type Mix struct {
	profile profile
	layouts []model.Layout
	reads   map[model.Layout][]weightedCum
	writes  map[model.Layout][]weightedCum
	only    *mixEntry
}

type weightedCum struct {
	kind string
	cum  float64
}

func cumulate(list []weighted) []weightedCum {
	var total float64
	for _, item := range list {
		total += item.weight
	}
	out := make([]weightedCum, len(list))
	var running float64
	for i, item := range list {
		running += item.weight / total
		out[i] = weightedCum{item.kind, running}
	}
	return out
}

// newMix builds the mix of the headline profile R, see newMixProfile.
func newMix(layouts []model.Layout, only string) (*Mix, error) {
	return newMixProfile(layouts, only, profileR)
}

// Profile is the read shares the mix draws with.
func (m *Mix) Profile() profile { return m.profile }

// newMixProfile builds the mix over layouts. only, when set, names one operation that every
// request then is (the record-only legs); it must be a kind of the tables.
func newMixProfile(layouts []model.Layout, only string, prof profile) (*Mix, error) {
	for _, layout := range layouts {
		if share, ok := prof.ReadShare[layout]; !ok || share < 0 || share > 1 {
			return nil, fmt.Errorf("profile %q has no read share between 0 and 1 for layout %s", prof.Name, layout)
		}
	}
	m := &Mix{profile: prof, layouts: layouts, reads: map[model.Layout][]weightedCum{}, writes: map[model.Layout][]weightedCum{}}
	var known []string
	for _, layout := range layouts {
		table := opTable[layout]
		m.reads[layout], m.writes[layout] = cumulate(table.reads), cumulate(table.writes)
		for _, item := range table.reads {
			known = append(known, item.kind)
			if item.kind == only {
				m.only = &mixEntry{Layout: layout, Kind: only}
			}
		}
		for _, item := range table.writes {
			known = append(known, item.kind)
			if item.kind == only {
				m.only = &mixEntry{Layout: layout, Write: true, Kind: only}
			}
		}
	}
	for _, item := range recordOnlyReads {
		known = append(known, item.kind)
		if item.kind == only {
			layout := model.Layout(strings.SplitN(only, ".", 2)[0])
			m.only = &mixEntry{Layout: layout, Kind: only}
		}
	}
	if only != "" && m.only == nil {
		sort.Strings(known)
		return nil, fmt.Errorf("-op %q is not an operation. Use one of %s", only, strings.Join(known, ", "))
	}
	return m, nil
}

// draw picks the layout (uniformly), then read or write by the layout's read share, then the
// operation. That is exactly three draws from rng, in this order; the planner's entity draw is
// the fourth, so a stream is the same on every engine.
func (m *Mix) draw(rng *rand.Rand) mixEntry {
	if m.only != nil {
		return *m.only
	}
	layout := m.layouts[rng.IntN(len(m.layouts))]
	write := rng.Float64() >= m.profile.ReadShare[layout]
	table := m.reads[layout]
	if write {
		table = m.writes[layout]
	}
	pick := rng.Float64()
	for _, item := range table {
		if pick < item.cum {
			return mixEntry{Layout: layout, Write: write, Kind: item.kind}
		}
	}
	return mixEntry{Layout: layout, Write: write, Kind: table[len(table)-1].kind}
}

// kinds lists every operation the mix can draw, sorted: the operations a record must account
// for, even one that was never drawn.
func (m *Mix) kinds() []string {
	if m.only != nil {
		return []string{m.only.Kind}
	}
	var out []string
	for _, layout := range m.layouts {
		for _, table := range [][]weightedCum{m.reads[layout], m.writes[layout]} {
			for _, item := range table {
				out = append(out, item.kind)
			}
		}
	}
	sort.Strings(out)
	return out
}

// shares returns the expected share of every operation among all requests.
func (m *Mix) shares() map[string]float64 {
	if m.only != nil {
		return map[string]float64{m.only.Kind: 1}
	}
	out := map[string]float64{}
	perLayout := 1 / float64(len(m.layouts))
	for _, layout := range m.layouts {
		for _, side := range []struct {
			table []weightedCum
			share float64
		}{{m.reads[layout], m.profile.ReadShare[layout]}, {m.writes[layout], 1 - m.profile.ReadShare[layout]}} {
			var previous float64
			for _, item := range side.table {
				out[item.kind] += perLayout * side.share * (item.cum - previous)
				previous = item.cum
			}
		}
	}
	return out
}
