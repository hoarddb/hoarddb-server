package harness

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// The invariant names the fault tables print.
const (
	InvNoAckedWriteLost   = "no-acked-write-lost"
	InvReadsMonotone      = "reads-monotone-per-client"
	InvOneTailPerPair     = "one-tail-per-pair"
	InvAckVocabularySound = "ack-vocabulary-sound"
	InvRefusalsAreNamed   = "refusals-are-named"
	InvQuiescenceConverge = "quiescence-converges"
)

const (
	defaultSettle     = 15 * time.Second
	defaultMaxRefused = 50
)

// namedRefusals are the codes a running node may answer with while a fault is on (I5).
var namedRefusals = []cluster.Code{
	cluster.CodeLeaseLapsed, cluster.CodeNoQuorum, cluster.CodeNotOwner, cluster.CodeDocumentExists, cluster.CodeStateMoved,
	cluster.CodeRFUnsupported, cluster.CodeInvalidArgument, cluster.CodeOwnerDown, cluster.CodeUnavailable,
	// STALE_EPOCH is the fence's client face while a membership change commits
	// (the entry ring conventions spec section 7); the run-length bound still guards "not forever".
	cluster.CodeStaleEpoch,
}

// Check judges everything recorded against the six invariants and returns one Verdict each,
// in a fixed order. It never fails the test: the caller decides what a row expects.
func (l *Ledger) Check(ctx context.Context, c *Cluster, opts CheckOptions) []Verdict {
	if opts.Settle <= 0 {
		opts.Settle = defaultSettle
	}
	if opts.MaxRefused <= 0 {
		opts.MaxRefused = defaultMaxRefused
	}
	l.mu.Lock()
	snap := ledgerSnapshot{acks: slices.Clone(l.acks), obs: slices.Clone(l.obs), events: slices.Clone(l.events), ledger: l}
	l.mu.Unlock()

	lost, present := l.judgeLoss(ctx, c, snap, opts)
	excused := excusedLines(snap, opts.Lost)
	verdicts := []Verdict{
		lost,
		snap.checkMonotone(opts.Lost),
		snap.checkOneTail(),
		snap.checkVocabulary(),
		snap.checkRefusals(opts.MaxRefused),
		l.checkQuiescence(ctx, c, opts.Settle),
	}
	for i := range verdicts {
		verdicts[i].Excused = excused
	}
	l.logRun(snap, verdicts, excused, present)
	return verdicts
}

// ledgerSnapshot is what Check judges: the records as they stood when it began.
type ledgerSnapshot struct {
	acks   []ackRec
	obs    []obsRec
	events []Event
	ledger *Ledger
}

func (s ledgerSnapshot) eventBefore(n int) *Event { return eventBefore(s.events, n) }

func verdictOf(name string, problems []string, after *Event) Verdict {
	if len(problems) == 0 {
		return Verdict{Name: name, OK: true}
	}
	return Verdict{Name: name, Detail: strings.Join(problems, "; "), Problems: problems, After: after}
}

// judgeLoss is I1: every acknowledged write outside the declared loss set is readable once
// the cluster has settled. It also reports the declared ids that turned out readable.
//
// Precondition: writers to one id are serialised (one writer at a time per id), because
// "latest ack" and "came after it" are the ledger's record order of acks; two concurrent
// writers of one id can ack in an order that is not the order the owner applied them, and the
// check would then call a correct run lost.
func (l *Ledger) judgeLoss(ctx context.Context, c *Cluster, snap ledgerSnapshot, opts CheckOptions) (Verdict, []string) {
	// need[id] is every acknowledged write of id in the order the ledger recorded the acks.
	need := map[string][]ackRec{}
	for _, rec := range snap.acks {
		need[rec.ID] = append(need[rec.ID], rec)
	}
	pending := map[string]string{}
	var present []string
	for id := range need {
		if reason, declared := opts.Lost[id]; declared {
			if _, ok := l.readable(ctx, c, id, need[id]); ok {
				present = append(present, fmt.Sprintf("%s: excused but present (%s)", id, reason))
			}
			continue
		}
		pending[id] = "never read"
	}
	deadline := time.Now().Add(opts.Settle)
	for len(pending) > 0 {
		for id := range pending {
			why, ok := l.readable(ctx, c, id, need[id])
			if ok {
				delete(pending, id)
			} else {
				pending[id] = why
			}
		}
		if len(pending) == 0 || ctx.Err() != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	ids := make([]string, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var problems []string
	for _, id := range ids {
		rec := need[id][len(need[id])-1]
		problems = append(problems, fmt.Sprintf("acked write %s (client %q seq %d, %s/%d via %s, served by %s) is not readable after %s: %s",
			id, rec.Client, rec.Seq, rec.Outcome, rec.Copies, rec.Via, rec.ServedBy, opts.Settle, pending[id]))
	}
	return verdictOf(InvNoAckedWriteLost, problems, snap.eventBefore(len(snap.events))), present
}

// readable asks every running node for id once; it is satisfied by the first answer that is
// the latest acknowledged write of id or a write that came after it. Another client's write
// counts as "since" only when that write was itself acknowledged after, in the ledger's
// record order; a document older than the latest ack is a lost write.
func (l *Ledger) readable(ctx context.Context, c *Cluster, id string, acked []ackRec) (string, bool) {
	latest := acked[len(acked)-1]
	why := "no node is running"
	for _, via := range c.Running() {
		readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		doc, err := c.Node(via).Get(readCtx, l.opts.DB, l.opts.Bucket, id)
		cancel()
		if err != nil {
			why = fmt.Sprintf("a read through %s answered: %v", via, err)
			continue
		}
		body := bson.Raw(doc.Body)
		seq, ok := body.Lookup("seq").AsInt64OK()
		if !ok {
			why = fmt.Sprintf("the document read through %s has no numeric seq", via)
			continue
		}
		// A document with no name field cannot say whose seq it carries, so it can neither
		// reflect nor follow an ack: it is reported as not yet readable, not guessed at and
		// not allowed to panic the judge.
		writer, named := body.Lookup("name").StringValueOK()
		if !named {
			why = fmt.Sprintf("the document read through %s has no name field, so its writer cannot be judged", via)
			continue
		}
		if !reflectsOrFollows(acked, writer, seq) {
			why = fmt.Sprintf("a read through %s holds seq %d of %q, older than the latest acknowledged write (client %q seq %d)", via, seq, writer, latest.Client, latest.Seq)
			continue
		}
		return "", true
	}
	return why, false
}

// reflectsOrFollows says the document (writer, seq) is the latest acked write of its id or
// came after it. A document no ack names was never acknowledged: it counts as later only
// when its writer's own acks are all below it.
func reflectsOrFollows(acked []ackRec, writer string, seq int64) bool {
	matched, highest := -1, int64(-1)
	for i, rec := range acked {
		if rec.Client != writer {
			continue
		}
		highest = max(highest, rec.Seq)
		if rec.Seq == seq {
			matched = i
		}
	}
	if matched >= 0 {
		return matched == len(acked)-1
	}
	return seq > highest
}

// checkMonotone is I2 offline: it replays the acks and observations in the order they were
// recorded through the same state the online check uses.
//
// Precondition: the reads of one client name come from one goroutine (one goroutine per
// client), so their record order is the order the client saw them; two goroutines sharing a
// client name can record a later read first and the check would report a regression that
// never happened.
//
// An id in lost is skipped, as I1 skips it: its latest ack was declared discarded by a
// failover, so an older read or NOT_FOUND of it afterwards is the declared loss, not a regression.
func (s ledgerSnapshot) checkMonotone(lost map[string]string) Verdict {
	type step struct {
		order int
		ack   *ackRec
		obs   *obsRec
	}
	steps := make([]step, 0, len(s.acks)+len(s.obs))
	for i := range s.acks {
		if _, declared := lost[s.acks[i].ID]; !declared {
			steps = append(steps, step{order: s.acks[i].order, ack: &s.acks[i]})
		}
	}
	for i := range s.obs {
		if _, declared := lost[s.obs[i].ID]; !declared {
			steps = append(steps, step{order: s.obs[i].order, obs: &s.obs[i]})
		}
	}
	slices.SortFunc(steps, func(a, b step) int { return a.order - b.order })
	state := newMonotoneState()
	var problems []string
	var after *Event
	for _, st := range steps {
		if st.ack != nil {
			state.ack(st.ack.Ack)
			continue
		}
		if detail := state.read(st.obs.Obs); detail != "" {
			if after == nil {
				after = s.eventBefore(st.obs.events)
			}
			problems = append(problems, detail)
		}
	}
	return verdictOf(InvReadsMonotone, problems, after)
}

// monotoneState is what I2 remembers. Seq is a per-writer counter, so served reads compare per
// client, id and writer. A served read followed by NOT_FOUND is a stale read, and so is a read
// of the client's own writes below a seq the client holds an applied ack for: the spec promises
// "no stale read" at a handoff (entry-ring-write-path-and-replication, HANDOFF_READY: "B is
// current (no missed write) and A has stopped ordering and answering (no stale read)";
// entry-ring-conventions rule 35). An applied_degraded ack promises less and is not used. A run
// with cluster.stale_reads on promises arbitrarily old answers and must not use this check.
type monotoneState struct {
	last   map[[3]string]Obs
	served map[[2]string]Obs
	own    map[[2]string]int64
}

func newMonotoneState() monotoneState {
	return monotoneState{last: map[[3]string]Obs{}, served: map[[2]string]Obs{}, own: map[[2]string]int64{}}
}

func (m *monotoneState) ack(a Ack) {
	// Only an ack that reached a majority of the copies promises the client its own write
	// survives a failover; a concern-one ack is applied on the owner alone and may be
	// discarded, so a later older read is not a regression.
	if a.Outcome != machine.OutcomeApplied || a.Copies < majority(a.RF) {
		return
	}
	key := [2]string{a.Client, a.ID}
	m.own[key] = max(m.own[key], a.Seq)
}

// read records o and returns what went backwards, or "" when nothing did.
func (m *monotoneState) read(o Obs) string {
	idKey := [2]string{o.Client, o.ID}
	if o.Code == cluster.CodeNotFound {
		if prev, seen := m.served[idKey]; seen {
			return fmt.Sprintf("client %q read %s as NOT_FOUND through %s after reading seq %d of %q through %s (served by %s)",
				o.Client, o.ID, o.Via, prev.Seq, prev.Writer, prev.Via, prev.ServedBy)
		}
		return ""
	}
	if o.Code != 0 || o.Seq < 0 {
		return ""
	}
	var detail string
	key := [3]string{o.Client, o.ID, o.Writer}
	if prev, seen := m.last[key]; seen && o.Seq < prev.Seq {
		detail = fmt.Sprintf("client %q read %s at seq %d through %s (served by %s) after reading seq %d through %s (served by %s)",
			o.Client, o.ID, o.Seq, o.Via, o.ServedBy, prev.Seq, prev.Via, prev.ServedBy)
	} else if acked := m.own[idKey]; o.Writer == o.Client && o.Seq < acked {
		detail = fmt.Sprintf("client %q read its own write %s at seq %d through %s (served by %s) after an applied ack of seq %d",
			o.Client, o.ID, o.Seq, o.Via, o.ServedBy, acked)
	}
	m.last[key] = o
	m.served[idKey] = o
	return detail
}

// checkOneTail is I3. Two owners acknowledging one id at one epoch are two tails; and no
// node may hold a position from an epoch the cluster had not committed when it was sampled.
func (s ledgerSnapshot) checkOneTail() Verdict {
	type slot struct {
		id    string
		epoch uint64
	}
	owners := map[slot][]string{}
	var problems []string
	var after *Event
	for _, rec := range s.acks {
		if rec.ServedBy == "" {
			continue
		}
		key := slot{rec.ID, rec.Pos.Epoch}
		if !slices.Contains(owners[key], rec.ServedBy) {
			owners[key] = append(owners[key], rec.ServedBy)
			if len(owners[key]) == 2 {
				if after == nil {
					after = s.eventBefore(rec.events)
				}
				problems = append(problems, fmt.Sprintf("id %s was acknowledged at epoch %d by two owners (%s), so the pair has two tails", rec.ID, rec.Pos.Epoch, strings.Join(owners[key], " and ")))
			}
		}
	}
	for i, event := range s.events {
		for node, pos := range event.Positions {
			if pos.Epoch > event.Epoch {
				if after == nil {
					after = s.eventBefore(i)
				}
				problems = append(problems, fmt.Sprintf("%s held position %d/%d at %q while the committed epoch was %d: a tail from an epoch nobody committed", node, pos.Epoch, pos.Seq, event.What, event.Epoch))
			}
		}
	}
	return verdictOf(InvOneTailPerPair, problems, after)
}

// majority is the copies a write concern of majority needs at replication factor rf.
func majority(rf uint8) uint8 { return rf/2 + 1 }

// copiesNeeded is the copies, owner included, a write that asked for level needs at
// replication factor rf. The zero level is the database default, which is majority.
func copiesNeeded(level machine.Concern, rf uint8) uint8 {
	switch level {
	case machine.ConcernOne:
		return 1
	case machine.ConcernAll:
		return rf
	default:
		return majority(rf)
	}
}

// checkVocabulary is I4: the outcome word agrees with the copies confirmed, and one request
// never has both an unknown and an applied answer. The threshold is the copies the write's
// own concern needs (Ack.Concern, copiesNeeded): a concern-one write answered applied with
// one copy is sound, and an applied ack at the default concern with fewer than a majority is
// not.
func (s ledgerSnapshot) checkVocabulary() Verdict {
	type request struct {
		client, id string
		seq        int64
	}
	applied, unknown := map[request]bool{}, map[request]bool{}
	var problems []string
	var after *Event
	flag := func(rec ackRec, format string, args ...any) {
		if after == nil {
			after = s.eventBefore(rec.events)
		}
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	for _, rec := range s.acks {
		need := copiesNeeded(rec.Concern, rec.RF)
		req := request{rec.Client, rec.ID, rec.Seq}
		switch rec.Outcome {
		case machine.OutcomeApplied:
			applied[req] = true
			if rec.Copies < need {
				flag(rec, "%s answered applied with %d copies at RF %d, which needs %d", rec.ID, rec.Copies, rec.RF, need)
			}
		case machine.OutcomeAppliedDegraded:
			if rec.Copies < 1 || rec.Copies >= need {
				flag(rec, "%s answered applied_degraded with %d copies at RF %d, which needs at least 1 and fewer than %d", rec.ID, rec.Copies, rec.RF, need)
			}
		case machine.OutcomeUnknown:
			unknown[req] = true
		default:
			flag(rec, "%s was acknowledged with outcome %d, which is none of applied, applied_degraded or unknown", rec.ID, rec.Outcome)
		}
		if applied[req] && unknown[req] {
			flag(rec, "request %s seq %d of %q was answered both unknown and applied", rec.ID, rec.Seq, rec.Client)
		}
	}
	return verdictOf(InvAckVocabularySound, problems, after)
}

// checkRefusals is I5: a running node answers with a named code, and not forever.
func (s ledgerSnapshot) checkRefusals(maxRefused int) Verdict {
	run := map[string]int{}
	var problems []string
	var after *Event
	flag := func(rec obsRec, format string, args ...any) {
		if after == nil {
			after = s.eventBefore(rec.events)
		}
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	for _, rec := range s.obs {
		refused := rec.Text != "" || (rec.Code != 0 && rec.Code != cluster.CodeNotFound)
		if !refused {
			run[rec.Via] = 0
			continue
		}
		if !rec.ViaRunning {
			continue
		}
		run[rec.Via]++
		switch {
		case rec.Code == 0:
			flag(rec, "running node %s answered %q for %s with no code: a bare deadline or transport error", rec.Via, rec.Text, rec.ID)
		case !slices.Contains(namedRefusals, rec.Code):
			flag(rec, "running node %s answered %s for %s, which is not a refusal a fault may cause", rec.Via, rec.Code, rec.ID)
		}
		if run[rec.Via] == maxRefused+1 {
			flag(rec, "running node %s refused %d requests in a row", rec.Via, run[rec.Via])
		}
	}
	return verdictOf(InvRefusalsAreNamed, problems, after)
}

// checkQuiescence waits for the nodes that hold the probe pair to agree on its position.
func (l *Ledger) checkQuiescence(ctx context.Context, c *Cluster, settle time.Duration) Verdict {
	if l.opts.ProbeID == "" {
		return Verdict{Name: InvQuiescenceConverge, OK: true, Detail: "no probe pair was named"}
	}
	deadline := time.Now().Add(settle)
	var detail string
	for {
		agreed, text := l.probeAgreement(ctx, c)
		if agreed {
			return Verdict{Name: InvQuiescenceConverge, OK: true}
		}
		detail = text
		if ctx.Err() != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	l.mu.Lock()
	after := eventBefore(l.events, len(l.events))
	l.mu.Unlock()
	return Verdict{Name: InvQuiescenceConverge, Detail: detail, Problems: []string{detail}, After: after}
}

func (l *Ledger) probeAgreement(ctx context.Context, c *Cluster) (bool, string) {
	running := c.Running()
	if len(running) == 0 {
		return false, "no node is running"
	}
	_, copies, err := c.Node(running[0]).Owner(ctx, l.opts.DB, l.opts.Bucket, l.opts.ProbeID)
	if err != nil {
		return false, fmt.Sprintf("placing the probe %s: %v", l.opts.ProbeID, err)
	}
	positions := map[string]cluster.Position{}
	for _, id := range running {
		if !slices.Contains(copies, id) {
			continue
		}
		pos, err := c.Node(id).PairPosition(ctx, l.opts.DB, l.opts.Bucket, l.opts.ProbeID)
		if err != nil {
			return false, fmt.Sprintf("reading the probe position on %s: %v", id, err)
		}
		positions[id] = pos
	}
	var first *cluster.Position
	for _, pos := range positions {
		if first == nil {
			first = &pos
		} else if *first != pos {
			return false, fmt.Sprintf("the nodes holding the probe pair disagree: %v", positions)
		}
	}
	return true, ""
}

// excusedLines prints the declared loss set, one line per id, sorted.
func excusedLines(snap ledgerSnapshot, declared map[string]string) []string {
	ids := make([]string, 0, len(declared))
	for id := range declared {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	lines := make([]string, 0, len(ids))
	for _, id := range ids {
		var found *ackRec
		for i := range snap.acks {
			if snap.acks[i].ID == id {
				found = &snap.acks[i]
			}
		}
		if found == nil {
			lines = append(lines, fmt.Sprintf("%s: never acked; excused: %s", id, declared[id]))
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: acked %s/%d via %s at %s; excused: %s", id, found.Outcome, found.Copies, found.Via, found.At.Format(time.RFC3339Nano), declared[id]))
	}
	return lines
}

// logRun prints the schedule, the verdicts and the loss set once, even when all is well.
func (l *Ledger) logRun(snap ledgerSnapshot, verdicts []Verdict, excused, present []string) {
	var out strings.Builder
	out.WriteString("ledger schedule:")
	for _, event := range snap.events {
		kind := "fault"
		if event.Sampled {
			kind = "note"
		}
		fmt.Fprintf(&out, "\n  %s %s %q epoch %d state_seq %d", event.At.Format("15:04:05.000"), kind, event.What, event.Epoch, event.StateSeq)
	}
	out.WriteString("\nledger verdicts:")
	for _, v := range verdicts {
		status := "ok"
		if !v.OK {
			status = "VIOLATED: " + v.Detail
			if v.After != nil {
				status += fmt.Sprintf(" (after %q)", v.After.What)
			}
		}
		fmt.Fprintf(&out, "\n  %s: %s", v.Name, status)
	}
	if len(excused) > 0 {
		out.WriteString("\nledger declared loss set:\n  " + strings.Join(excused, "\n  "))
	}
	if len(present) > 0 {
		out.WriteString("\nledger declared ids that are present anyway:\n  " + strings.Join(present, "\n  "))
	}
	l.tb.Logf("%s", out.String())
}
