package harness

// The seeded schedule runner (fault-plan slice H4).
//
// Determinism statement: a re-run with the same seed reproduces the schedule and every
// client's operation stream; it does not reproduce the interleaving, which real TLS, timers
// and Raft decide. The schedule's String() in a failing log is what a bug report pastes, and
// ScheduleFromLog rebuilds the schedule from the log's first line alone. One rand.Rand seeded
// with the seed builds the steps, with no map iteration anywhere; client i draws from its own
// rand.Rand seeded with seed*1000+i. Neither the wall clock nor the global source is read
// while a schedule or a client stream is drawn.
//
// The mix keeps a majority of the voters out of harm at every instant: the nodes that are
// killed, stalled or cut off never exceed (n-1)/2. A run that took a majority away would make
// every verdict trivially "lapsed"; that scenario belongs to the failure tables' rows B4 and
// A4, not to a random walk.

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// StepKind names one fault a schedule applies.
type StepKind uint8

// The kinds of schedule step. StepAddNode is reserved: a node added during writes (fault
// tables C3 and C4) needs the seeding-completion slices, so no schedule draws it yet and Run
// refuses a schedule that holds one.
const (
	StepKill StepKind = iota + 1
	StepRestart
	StepPartition
	StepHeal
	StepStall
	StepUnstall
	StepFailDisk
	StepSlowDisk
	StepDelayChannel
	StepDropReplies
	StepBlockOneWay
	StepAddNode
)

// Step is one scheduled fault. At is the offset from the start of the run. Nodes names the
// subject: the node of a kill, restart, stall, unstall or disk fault; the minority side of a
// partition; or the (from, to) pair of a channel fault or a one-way cut. D is the added latency
// of a delay or a slow disk; it is zero for the other kinds.
type Step struct {
	At    time.Duration
	Kind  StepKind
	Nodes []string
	Ch    transport.Channel
	D     time.Duration
}

// Schedule is a reproducible list of faults and the shape of the clients that run beside it.
// Profile is the timing profile the cluster runs at: it sets the spacing of the steps and how
// long a timed fault stays on.
type Schedule struct {
	Seed     int64
	N        int
	Clients  int
	Duration time.Duration
	Profile  Profile
	Steps    []Step
}

// Mix weights, out of 100: kill 25, restart 20, partition and heal 25 (15 two-way, 10
// one-way), stall and unstall 10, delay and drop-replies 10, fail-disk and slow-disk 10.
const (
	weightKill       = 25
	weightRestart    = 20
	weightPartition  = 15
	weightOneWay     = 10
	weightStall      = 5
	weightUnstall    = 5
	weightDelay      = 5
	weightDropReply  = 5
	weightFailDisk   = 5
	weightSlowDisk   = 5
	idsPerClient     = 6
	operationTimeout = 10 * time.Second
)

// Hold is how long a timed fault (a delay, dropped replies, a failed or slow disk) stays on,
// and the longest a partition may last: twice the profile's detection time.
func (s Schedule) Hold() time.Duration { return 2 * s.Profile.Detection() }

func nodeName(i int) string { return "n" + strconv.Itoa(i+1) }

// timedChannels are the channels a delay or a dropped-reply fault may sit on.
var timedChannels = []transport.Channel{transport.ChannelData, transport.ChannelControl, transport.ChannelRaft}
var replyChannels = []transport.Channel{transport.ChannelData, transport.ChannelControl}

// maxDown is how many nodes may be out of service at once: a minority.
func maxDown(n int) int { return (n - 1) / 2 }

// generator is the model of the cluster the schedule is drawn against, advanced step by step.
type generator struct {
	n         int
	killed    []bool
	stalled   []bool
	cutOff    []bool
	healAt    time.Duration
	cutActive bool
	diskUntil []time.Duration
	hold      time.Duration
	// since is when a node went out of service by kill or stall (-1 while it is in service);
	// limit is how long it may stay out: a third of dead_ms, because a member that stays down
	// past dead_ms is declared dead and needs replace-node, which a random walk must not reach.
	since []time.Duration
	limit time.Duration
	// stallID is the node whose unstall is due at unstallAt, or -1: one stall at a time, and
	// never longer than half a detection time, because a node stalled for longer is committed
	// down and is not returned to active when it resumes (a finding, in the build status).
	stallID   int
	unstallAt time.Duration
}

func newGenerator(n int, hold, limit time.Duration) *generator {
	g := &generator{n: n, killed: make([]bool, n), stalled: make([]bool, n), cutOff: make([]bool, n), diskUntil: make([]time.Duration, n), hold: hold, since: make([]time.Duration, n), limit: limit, stallID: -1}
	for i := range g.since {
		g.since[i] = -1
	}
	return g
}

func (g *generator) down() int {
	count := 0
	for i := 0; i < g.n; i++ {
		if g.killed[i] || g.stalled[i] || g.cutOff[i] {
			count++
		}
	}
	return count
}

func (g *generator) anyStalled() bool { return slices.Contains(g.stalled, true) }

// pick returns the nodes for which ok holds, in index order.
func (g *generator) pick(ok func(i int) bool) []int {
	var out []int
	for i := 0; i < g.n; i++ {
		if ok(i) {
			out = append(out, i)
		}
	}
	return out
}

// room says one more node can go out of service without the minority being exceeded.
func (g *generator) room(i int) bool {
	if g.killed[i] || g.stalled[i] || g.cutOff[i] {
		return true
	}
	return g.down()+1 <= maxDown(g.n)
}

type option struct {
	weight int
	build  func(rng *rand.Rand, at time.Duration) Step
}

// options lists the steps that are possible now, in a fixed order, each with its weight.
// remaining is how many steps are still to be drawn, so a cut always has room for its heal.
func (g *generator) options(remaining int, now time.Duration) []option {
	var out []option
	if forced := g.overdue(now); forced != nil {
		return forced
	}
	notKilled := g.pick(func(i int) bool { return !g.killed[i] })
	killable := g.pick(func(i int) bool { return !g.killed[i] && g.room(i) })
	// A stalled node is never killed: Node.Close waits for goroutines the stall holds, so the
	// kill would hang the run (a finding about the harness, recorded in the build status).
	killNow := g.pick(func(i int) bool { return !g.killed[i] && !g.stalled[i] && g.room(i) })
	if len(killNow) > 0 {
		out = append(out, option{weightKill, func(rng *rand.Rand, at time.Duration) Step {
			i := killNow[rng.Intn(len(killNow))]
			g.killed[i] = true
			g.since[i] = at
			return Step{At: at, Kind: StepKill, Nodes: []string{nodeName(i)}}
		}})
	}
	if !g.cutActive && !g.anyStalled() {
		restartable := g.pick(func(i int) bool { return g.killed[i] && g.diskUntil[i] <= 0 })
		if len(restartable) > 0 {
			out = append(out, option{weightRestart, func(rng *rand.Rand, at time.Duration) Step {
				i := restartable[rng.Intn(len(restartable))]
				g.killed[i] = false
				g.since[i] = -1
				return Step{At: at, Kind: StepRestart, Nodes: []string{nodeName(i)}}
			}})
		}
	}
	if !g.cutActive && remaining >= 2 {
		isolable := g.pick(func(i int) bool { return !g.killed[i] && !g.stalled[i] && g.room(i) })
		if len(isolable) > 0 {
			out = append(out, option{weightPartition, func(rng *rand.Rand, at time.Duration) Step {
				size := 1 + rng.Intn(max(1, maxDown(g.n)-g.down()))
				var minority []string
				for len(minority) < size && len(isolable) > 0 {
					j := rng.Intn(len(isolable))
					i := isolable[j]
					isolable = slices.Delete(isolable, j, j+1)
					if !g.room(i) {
						continue
					}
					g.cutOff[i] = true
					minority = append(minority, nodeName(i))
				}
				g.arm(at, rng)
				return Step{At: at, Kind: StepPartition, Nodes: minority}
			}})
		}
		observers := g.pick(func(i int) bool { return !g.killed[i] && !g.stalled[i] })
		if len(observers) >= 2 && len(killable) > 0 {
			out = append(out, option{weightOneWay, func(rng *rand.Rand, at time.Duration) Step {
				to := killable[rng.Intn(len(killable))]
				from := observers[rng.Intn(len(observers))]
				if from == to {
					from = observers[(slices.Index(observers, from)+1)%len(observers)]
				}
				g.cutOff[to] = true
				g.arm(at, rng)
				return Step{At: at, Kind: StepBlockOneWay, Nodes: []string{nodeName(from), nodeName(to)}}
			}})
		}
	}
	stallable := g.pick(func(i int) bool { return !g.killed[i] && !g.stalled[i] && g.room(i) })
	if len(stallable) > 0 && g.stallID < 0 && remaining >= 2 {
		out = append(out, option{weightStall + weightUnstall, func(rng *rand.Rand, at time.Duration) Step {
			i := stallable[rng.Intn(len(stallable))]
			g.stalled[i] = true
			g.stallID = i
			g.unstallAt = at + g.hold/4
			return Step{At: at, Kind: StepStall, Nodes: []string{nodeName(i)}}
		}})
	}
	if len(notKilled) >= 2 {
		pair := func(rng *rand.Rand) (string, string) {
			from := rng.Intn(len(notKilled))
			to := rng.Intn(len(notKilled) - 1)
			if to >= from {
				to++
			}
			return nodeName(notKilled[from]), nodeName(notKilled[to])
		}
		out = append(out, option{weightDelay, func(rng *rand.Rand, at time.Duration) Step {
			from, to := pair(rng)
			ch := timedChannels[rng.Intn(len(timedChannels))]
			return Step{At: at, Kind: StepDelayChannel, Nodes: []string{from, to}, Ch: ch, D: time.Duration(2+rng.Intn(19)) * 10 * time.Millisecond}
		}})
		out = append(out, option{weightDropReply, func(rng *rand.Rand, at time.Duration) Step {
			from, to := pair(rng)
			ch := replyChannels[rng.Intn(len(replyChannels))]
			return Step{At: at, Kind: StepDropReplies, Nodes: []string{from, to}, Ch: ch}
		}})
	}
	if len(notKilled) > 0 {
		out = append(out, option{weightFailDisk, func(rng *rand.Rand, at time.Duration) Step {
			i := notKilled[rng.Intn(len(notKilled))]
			g.diskUntil[i] = at + g.hold
			return Step{At: at, Kind: StepFailDisk, Nodes: []string{nodeName(i)}}
		}})
		out = append(out, option{weightSlowDisk, func(rng *rand.Rand, at time.Duration) Step {
			i := notKilled[rng.Intn(len(notKilled))]
			g.diskUntil[i] = at + g.hold
			return Step{At: at, Kind: StepSlowDisk, Nodes: []string{nodeName(i)}, D: time.Duration(2+rng.Intn(29)) * 10 * time.Millisecond}
		}})
	}
	return out
}

// overdue is the only step possible when a node has been killed for longer than
// limit: restart it. It is nil when nothing is overdue or nothing can be done yet (a cut or a
// stall is waiting for its end).
func (g *generator) overdue(now time.Duration) []option {
	for i := 0; i < g.n; i++ {
		if g.since[i] < 0 || now-g.since[i] < g.limit {
			continue
		}
		switch {
		case g.killed[i] && !g.cutActive && !g.anyStalled() && g.diskUntil[i] <= 0:
			return []option{{weightRestart, func(_ *rand.Rand, at time.Duration) Step {
				g.killed[i] = false
				g.since[i] = -1
				return Step{At: at, Kind: StepRestart, Nodes: []string{nodeName(i)}}
			}}}
		}
	}
	return nil
}

// arm schedules the heal of a cut that starts at at: between one and two detection times
// later, never more.
func (g *generator) arm(at time.Duration, rng *rand.Rand) {
	g.cutActive = true
	third := g.hold / 4
	g.healAt = at + g.hold/2 + time.Duration(rng.Int63n(int64(third*2/time.Millisecond)+1))*time.Millisecond
}

func (g *generator) heal() {
	g.cutActive = false
	clear(g.cutOff)
}

// expire forgets disk faults that are over by at.
func (g *generator) expire(at time.Duration) {
	for i := range g.diskUntil {
		if g.diskUntil[i] > 0 && g.diskUntil[i] <= at {
			g.diskUntil[i] = 0
		}
	}
}

// NewSchedule draws a schedule of the given number of steps (heals included) for n nodes and
// the stated number of clients. The same arguments always give the same schedule.
func NewSchedule(seed int64, n, clients, steps int, profile Profile) Schedule {
	rng := rand.New(rand.NewSource(seed))
	s := Schedule{Seed: seed, N: n, Clients: clients, Profile: profile}
	hold := s.Hold()
	limit := time.Duration(profile["dead_ms"]) * time.Millisecond / 3
	g := newGenerator(n, hold, limit)
	detection := profile.Detection()
	var cursor time.Duration
	for len(s.Steps) < steps {
		gap := detection/2 + time.Duration(rng.Int63n(int64(detection*3/2/time.Millisecond)+1))*time.Millisecond
		next := cursor + gap
		pending := 0
		if g.cutActive {
			pending++
		}
		if g.stallID >= 0 {
			pending++
		}
		// The end of a cut or a stall comes before whatever else is drawn once it is due, and
		// always has room: a new fault is drawn only while two steps remain beyond the pending ones.
		endsCut := g.cutActive && (g.stallID < 0 || g.healAt <= g.unstallAt)
		due := g.healAt
		if !endsCut {
			due = g.unstallAt
		}
		if pending > 0 && (due <= next || steps-len(s.Steps) == pending) {
			cursor = max(due, cursor)
			if endsCut {
				s.Steps = append(s.Steps, Step{At: cursor, Kind: StepHeal})
				g.heal()
			} else {
				s.Steps = append(s.Steps, Step{At: cursor, Kind: StepUnstall, Nodes: []string{nodeName(g.stallID)}})
				g.stalled[g.stallID] = false
				g.stallID = -1
			}
			continue
		}
		cursor = next
		g.expire(cursor)
		options := g.options(steps-len(s.Steps)-pending, cursor)
		if len(options) == 0 {
			// Only possible below three nodes, where nothing may go out of service: a slow
			// disk on the first node keeps the schedule moving.
			s.Steps = append(s.Steps, Step{At: cursor, Kind: StepSlowDisk, Nodes: []string{nodeName(0)}, D: 20 * time.Millisecond})
			g.diskUntil[0] = cursor + hold
			continue
		}
		total := 0
		for _, o := range options {
			total += o.weight
		}
		roll := rng.Intn(total)
		for _, o := range options {
			if roll < o.weight {
				s.Steps = append(s.Steps, o.build(rng, cursor))
				break
			}
			roll -= o.weight
		}
	}
	s.Duration = cursor + 2*hold
	return s
}

var kindWords = map[StepKind]string{
	StepKill: "kill", StepRestart: "restart", StepPartition: "partition", StepHeal: "heal", StepStall: "stall",
	StepUnstall: "unstall", StepFailDisk: "fail-disk", StepSlowDisk: "slow-disk", StepDelayChannel: "delay",
	StepDropReplies: "drop-replies", StepBlockOneWay: "block-oneway", StepAddNode: "add-node",
}

// String is the step as one line, the form the ledger records as a fault.
func (st Step) String() string {
	word := kindWords[st.Kind]
	switch st.Kind {
	case StepDelayChannel:
		return fmt.Sprintf("%s %s->%s %s +%s", word, st.Nodes[0], st.Nodes[1], st.Ch, st.D)
	case StepDropReplies:
		return fmt.Sprintf("%s %s->%s %s", word, st.Nodes[0], st.Nodes[1], st.Ch)
	case StepBlockOneWay:
		return fmt.Sprintf("%s %s->%s", word, st.Nodes[0], st.Nodes[1])
	case StepSlowDisk:
		return fmt.Sprintf("%s %s +%s", word, st.Nodes[0], st.D)
	case StepHeal:
		return word
	default:
		return word + " " + strings.Join(st.Nodes, ",")
	}
}

// Header is the first line of the schedule's log: everything NewSchedule needs besides the
// steps, so ScheduleFromLog can rebuild the schedule from a pasted log.
func (s Schedule) Header() string {
	keys := make([]string, 0, len(s.Profile))
	for key := range s.Profile {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, len(keys))
	for i, key := range keys {
		pairs[i] = key + ":" + strconv.FormatInt(s.Profile[key], 10)
	}
	return fmt.Sprintf("fault schedule seed=%d nodes=%d clients=%d steps=%d profile=%s", s.Seed, s.N, s.Clients, len(s.Steps), strings.Join(pairs, ","))
}

// String is one line per step, in the order they run.
func (s Schedule) String() string {
	var out strings.Builder
	for i, st := range s.Steps {
		fmt.Fprintf(&out, "step %02d at %-7s %s\n", i+1, st.At, st)
	}
	return out.String()
}

var headerPattern = regexp.MustCompile(`fault schedule seed=(-?\d+) nodes=(\d+) clients=(\d+) steps=(\d+) profile=(\S+)`)

// ScheduleFromLog rebuilds the schedule a failing log printed, from its header line alone.
func ScheduleFromLog(log string) (Schedule, error) {
	m := headerPattern.FindStringSubmatch(log)
	if m == nil {
		return Schedule{}, fmt.Errorf("no line like %q in the log; paste the line that starts 'fault schedule seed=' from the failing run", "fault schedule seed=7 nodes=3 clients=2 steps=14 profile=...")
	}
	seed, _ := strconv.ParseInt(m[1], 10, 64)
	n, _ := strconv.Atoi(m[2])
	clients, _ := strconv.Atoi(m[3])
	steps, _ := strconv.Atoi(m[4])
	profile := Profile{}
	for _, pair := range strings.Split(m[5], ",") {
		key, text, found := strings.Cut(pair, ":")
		millis, err := strconv.ParseInt(text, 10, 64)
		if !found || err != nil {
			return Schedule{}, fmt.Errorf("the profile %q in the log header is not key:number pairs: %w", m[5], err)
		}
		profile[key] = millis
	}
	return NewSchedule(seed, n, clients, steps, profile), nil
}

// ClientOp is one operation of a client's stream. Owner and Slot pick the document id (a write
// always uses the client's own ids); Via is reduced modulo the nodes that are eligible when
// the operation runs.
type ClientOp struct {
	Write       bool
	Owner, Slot int
	Via         int
	Pause       time.Duration
}

// ClientStream is the operation stream of one client.
type ClientStream struct {
	rng     *rand.Rand
	clients int
}

// ClientStream returns the stream of client i (0-based), seeded with seed*1000+i.
func (s Schedule) ClientStream(i int) *ClientStream {
	return &ClientStream{rng: rand.New(rand.NewSource(s.Seed*1000 + int64(i))), clients: s.Clients}
}

// Next draws the next operation.
func (cs *ClientStream) Next() ClientOp {
	op := ClientOp{
		Write: cs.rng.Intn(10) < 7,
		Owner: cs.rng.Intn(cs.clients),
		Slot:  cs.rng.Intn(idsPerClient),
		Via:   cs.rng.Intn(1 << 16),
		Pause: time.Duration(5+cs.rng.Intn(26)) * time.Millisecond,
	}
	return op
}

func docID(owner, slot int) string { return fmt.Sprintf("c%d-%d", owner+1, slot) }

// span is a window in which a partition separated the minority from the rest.
type span struct {
	start, end time.Time
	minority   []string
}

type killRecord struct {
	node string
	at   time.Time
}

type timedUndo struct {
	due  time.Duration
	what string
	do   func()
}

// runner is the state of one Run.
type runner struct {
	t *testing.T
	c *Cluster
	l *Ledger
	s Schedule

	mu         sync.Mutex
	unavail    map[string]bool
	spans      []span
	kills      []killRecord
	undos      []timedUndo
	killedNow  map[string]bool
	stalledNow map[string]bool
}

// eligible lists the nodes a client may use as its entry: running and not stalled.
func (r *runner) eligible() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for i := 0; i < r.s.N; i++ {
		if id := nodeName(i); !r.unavail[id] {
			out = append(out, id)
		}
	}
	return out
}

func (r *runner) setAvailable(id string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unavail[id] = !ok
}

// client runs one client's stream until stop is set. One goroutine, one client name, and the
// client writes only ids it owns, which is what the ledger's record-order checks need.
func (r *runner) client(i int, stop *atomic.Bool, wg *sync.WaitGroup) {
	defer wg.Done()
	name := fmt.Sprintf("c%d", i+1)
	stream := r.s.ClientStream(i)
	for !stop.Load() {
		op := stream.Next()
		time.Sleep(op.Pause)
		entries := r.eligible()
		if len(entries) == 0 {
			continue
		}
		via := entries[op.Via%len(entries)]
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		if op.Write {
			// A refusal is recorded by the ledger; the next write of the id carries a higher seq.
			_, _ = r.l.Write(ctx, r.c, via, name, docID(i, op.Slot))
		} else {
			_, _ = r.l.Read(ctx, r.c, via, name, docID(op.Owner, op.Slot))
		}
		cancel()
	}
}

func (r *runner) later(st Step, what string, do func()) {
	r.undos = append(r.undos, timedUndo{due: st.At + r.s.Hold(), what: what, do: do})
	sort.SliceStable(r.undos, func(a, b int) bool { return r.undos[a].due < r.undos[b].due })
}

// apply performs one step against the cluster and records it as a fault.
func (r *runner) apply(st Step) {
	r.t.Helper()
	r.l.Fault(st.String())
	r.t.Logf("apply %s", st)
	switch st.Kind {
	case StepKill:
		id := st.Nodes[0]
		r.setAvailable(id, false)
		r.mu.Lock()
		r.kills = append(r.kills, killRecord{node: id, at: time.Now()})
		r.killedNow[id] = true
		r.mu.Unlock()
		r.c.Kill(id)
	case StepRestart:
		id := st.Nodes[0]
		r.c.Restart(id)
		r.mu.Lock()
		delete(r.killedNow, id)
		r.mu.Unlock()
		r.setAvailable(id, true)
		r.awaitActive(st.String())
		r.l.Note(r.c, "after "+st.String())
	case StepPartition:
		var rest []string
		for i := 0; i < r.s.N; i++ {
			if id := nodeName(i); !slices.Contains(st.Nodes, id) {
				rest = append(rest, id)
			}
		}
		r.mu.Lock()
		r.spans = append(r.spans, span{start: time.Now(), minority: slices.Clone(st.Nodes)})
		r.mu.Unlock()
		r.c.Partition(slices.Clone(st.Nodes), rest)
	case StepHeal:
		r.closeSpans()
		r.c.Heal()
		r.awaitActive("heal")
		r.l.Note(r.c, "after heal")
	case StepStall:
		id := st.Nodes[0]
		r.setAvailable(id, false)
		r.mu.Lock()
		r.stalledNow[id] = true
		r.mu.Unlock()
		r.c.Stall(id, true)
	case StepUnstall:
		id := st.Nodes[0]
		r.c.Stall(id, false)
		r.mu.Lock()
		delete(r.stalledNow, id)
		r.mu.Unlock()
		r.setAvailable(id, true)
	case StepFailDisk:
		id := st.Nodes[0]
		r.c.FailDisk(id, true)
		r.later(st, "undo fail-disk "+id, func() { r.c.FailDisk(id, false) })
	case StepSlowDisk:
		id := st.Nodes[0]
		r.c.SlowDisk(id, st.D)
		r.later(st, "undo slow-disk "+id, func() { r.c.SlowDisk(id, 0) })
	case StepDelayChannel:
		from, to := st.Nodes[0], st.Nodes[1]
		r.c.DelayChannel(from, to, st.Ch, st.D)
		r.later(st, "undo delay "+from+"->"+to, func() { r.c.DelayChannel(from, to, st.Ch, 0) })
	case StepDropReplies:
		from, to := st.Nodes[0], st.Nodes[1]
		r.c.DropReplies(from, to, st.Ch, true)
		r.later(st, "undo drop-replies "+from+"->"+to, func() { r.c.DropReplies(from, to, st.Ch, false) })
	case StepBlockOneWay:
		r.c.BlockOneWay(st.Nodes[0], st.Nodes[1], true)
	default:
		r.t.Fatalf("fault schedule: step kind %d (%s) is not implemented; StepAddNode waits for the seeding-completion slices", st.Kind, kindWords[st.Kind])
	}
}

func (r *runner) closeSpans() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for i := range r.spans {
		if r.spans[i].end.IsZero() {
			r.spans[i].end = now
		}
	}
}

// settle lifts every fault still on so the cluster can reach quiescence: heal, unstall, undo
// the timed faults, restart what is killed.
func (r *runner) settle() {
	r.t.Helper()
	r.l.Fault("settle: heal, unstall, clear timed faults, restart the killed")
	r.closeSpans()
	r.c.Heal()
	for _, undo := range r.undos {
		undo.do()
	}
	r.undos = nil
	for i := 0; i < r.s.N; i++ {
		id := nodeName(i)
		r.mu.Lock()
		stalled := r.stalledNow[id]
		killed := r.killedNow[id]
		delete(r.stalledNow, id)
		delete(r.killedNow, id)
		r.mu.Unlock()
		if stalled {
			r.c.Stall(id, false)
		}
		if killed {
			r.c.Restart(id)
		}
		r.setAvailable(id, true)
	}
	r.c.AwaitConverged(30 * time.Second)
	r.c.AwaitLeases(30 * time.Second)
	r.awaitActive("settling")
}

// awaitActive waits up to 30 s for every node that should be in service to be committed active
// again, and activates the ones that are down the way an operator does (Node.Activate on the
// leader): nothing in the harness returns a member to active by itself, and the tables that
// bring a node back do the same (bug0746_tail_test.go). The next fault must not land on a
// cluster that has not finished recovering from the last. A member that stays down is a
// finding, reported as a test error with the members' states and the last refusal.
func (r *runner) awaitActive(after string) {
	r.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var refusal error
	for {
		report, down, ok := r.memberReport()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			r.l.Fault("finding: members not active 30s after " + after)
			r.t.Errorf("finding: 30s after %s the members are %s and activating %v was refused: %v (seed %d)", after, report, down, refusal, r.s.Seed)
			return
		}
		for _, id := range r.c.Running() {
			if !r.c.Node(id).IsLeader() {
				continue
			}
			for _, member := range down {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := r.c.Node(id).Activate(ctx, member); err != nil {
					refusal = err
				}
				cancel()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// memberReport is every member's committed state as the first running node sees it, the
// members that should be in service and are down, and whether none is.
func (r *runner) memberReport() (string, []string, bool) {
	running := r.c.Running()
	if len(running) == 0 {
		return "no node is running", nil, false
	}
	members := r.c.Node(running[0]).State().Members
	r.mu.Lock()
	defer r.mu.Unlock()
	words := make([]string, len(members))
	var down []string
	for i, member := range members {
		words[i] = member.ID + "=" + member.State.String()
		if member.State != cluster.MemberActive && !r.killedNow[member.ID] && !r.stalledNow[member.ID] {
			down = append(down, member.ID)
		}
	}
	return strings.Join(words, " "), down, len(down) == 0
}

// lossSet computes the declared loss set from what the clients were acknowledged and the
// faults that were in force: the latest ack of an id is excused when it came through a node
// on the minority side of a partition in force at the ack, or when the owner that ordered it
// was killed afterwards with fewer than a majority of the copies confirmed. Everything else
// must be readable.
func (r *runner) lossSet(acks []Ack) map[string]string {
	latest := map[string]Ack{}
	for _, ack := range acks {
		latest[ack.ID] = ack
	}
	lost := map[string]string{}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, ack := range latest {
		for _, p := range r.spans {
			if ack.At.Before(p.start) || (!p.end.IsZero() && ack.At.After(p.end)) {
				continue
			}
			if slices.Contains(p.minority, ack.Via) || slices.Contains(p.minority, ack.ServedBy) {
				lost[id] = fmt.Sprintf("acked via %s (served by %s) while %v was cut off from the majority", ack.Via, ack.ServedBy, p.minority)
				break
			}
		}
		if _, done := lost[id]; done {
			continue
		}
		if ack.RF == 0 || ack.Copies >= majority(ack.RF) {
			continue
		}
		for _, k := range r.kills {
			if k.node == ack.ServedBy && k.at.After(ack.At) {
				lost[id] = fmt.Sprintf("acked by %s with %d of %d copies (%s), and %s was killed afterwards", ack.ServedBy, ack.Copies, ack.RF, ack.Outcome, k.node)
				break
			}
		}
	}
	return lost
}

// Run applies the schedule to c while Clients clients write and read through l, then settles
// the cluster and returns the ledger's six verdicts. The schedule is logged before the first
// step. The caller creates the database and the bucket the ledger names and fails the test on
// a verdict that is not OK; a violation is a finding, never something Run repairs.
func (s Schedule) Run(t *testing.T, c *Cluster, l *Ledger) []Verdict {
	t.Helper()
	t.Logf("%s\n%s", s.Header(), s)
	r := &runner{t: t, c: c, l: l, s: s, unavail: map[string]bool{}, killedNow: map[string]bool{}, stalledNow: map[string]bool{}}
	l.Note(c, "before the first step")
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < s.Clients; i++ {
		wg.Add(1)
		go r.client(i, &stop, &wg)
	}
	start := time.Now()
	sleepTo := func(at time.Duration) {
		if wait := at - time.Since(start); wait > 0 {
			time.Sleep(wait)
		}
	}
	next := 0
	for next < len(s.Steps) || len(r.undos) > 0 {
		if len(r.undos) > 0 && (next >= len(s.Steps) || r.undos[0].due <= s.Steps[next].At) {
			undo := r.undos[0]
			r.undos = r.undos[1:]
			sleepTo(undo.due)
			r.l.Fault(undo.what)
			undo.do()
			continue
		}
		st := s.Steps[next]
		next++
		sleepTo(st.At)
		r.apply(st)
	}
	sleepTo(s.Duration)
	stop.Store(true)
	r.settle()
	wg.Wait()
	l.Note(c, "after settling")
	lost := r.lossSet(l.Acks())
	t.Logf("declared loss set: %d ids excused", len(lost))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return l.Check(ctx, c, CheckOptions{Lost: lost, Settle: 20 * time.Second})
}
