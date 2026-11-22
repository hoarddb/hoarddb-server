package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// Ack is one acknowledged write as the client saw it. Seq is the client's own counter written
// into the document body ({"name": client, "seq": n}), so a read observes it without positions.
type Ack struct {
	Client, ID string
	Seq        int64
	Pos        cluster.Position
	Deduped    bool
	Outcome    machine.Outcome
	Copies, RF uint8
	// Concern is the level the write asked for (wire.WithConcern); zero is the database
	// default, which is majority.
	Concern machine.Concern
	// Via is the entry node; ServedBy is the owner that ordered the write (Inserted.ServedBy).
	Via, ServedBy string
	// Running is Cluster.Running() at ack time.
	Running []string
	At      time.Time
}

// Obs is one read or write that produced an answer or a named refusal. Seq is -1 for NOT_FOUND
// and for a refusal; Code is 0 when served. Text and ViaRunning are only set on a refusal.
type Obs struct {
	Client, ID string
	// Writer is the body's name field: whose counter Seq is. Seq is a per-writer counter, so
	// two reads compare only when they carry the same Writer.
	Writer   string
	Seq      int64
	Code     cluster.Code
	Via      string
	ServedBy string
	At       time.Time
	// Text is the error's words, kept when the error had no cluster.Code.
	Text string
	// ViaRunning says the entry node was up when the call started and when it ended; a dead
	// node's deadline is the spec's client_timeout, a live node's is an I5 violation.
	ViaRunning bool
}

// Event is a fault injection or a sampled cluster observation, for attribution.
type Event struct {
	At       time.Time
	What     string
	Epoch    uint64
	StateSeq uint64
	// Positions is the probe pair's PairPosition on every running node, from Note.
	Positions map[string]cluster.Position
	// Sampled is true for a Note and false for a fault the test injected.
	Sampled bool
}

// Verdict is one invariant's judgement. After names the last event before the first violation.
type Verdict struct {
	Name   string
	OK     bool
	Detail string
	// Problems is Detail one violation at a time, so a caller can tell one from another
	// without splitting text that may itself contain a "; ".
	Problems []string
	Excused  []string
	After    *Event
}

// LedgerOptions names the bucket the ledger writes to and the pair it samples.
type LedgerOptions struct {
	DB, Bucket string
	// ProbeID is the document id whose pair Note samples.
	ProbeID string
}

// CheckOptions tunes Check.
type CheckOptions struct {
	// Lost is the row's declared loss set, id -> reason, printed in every verdict, never silent.
	Lost map[string]string
	// Settle is how long I1 keeps re-reading (default 15 s, the quorumFailoverBudget precedent).
	Settle time.Duration
	// MaxRefused is I5's bound on consecutive refusals a live node may give (default 50).
	MaxRefused int
}

// ackRec and obsRec remember how many events had been recorded when they were, so an
// invariant names the event before a violation without comparing clocks.
type ackRec struct {
	Ack
	events int
	// order numbers acks and observations together, so the offline I2 check can replay a read
	// after the acks that came before it.
	order int
}

type obsRec struct {
	Obs
	events int
	order  int
}

// Ledger records what clients saw during a fault schedule and judges it against the
// invariants of the membership-and-failure spec.
type Ledger struct {
	tb   testing.TB
	opts LedgerOptions

	mu     sync.Mutex
	next   map[string]int64
	acks   []ackRec
	obs    []obsRec
	events []Event
	// recorded counts acks and observations; it is their shared order.
	recorded int
	// monotone is the online I2 check's state.
	monotone monotoneState
	// rf is the last replication factor any node reported for the ledger's database.
	rf uint8
}

// NewLedger returns an empty ledger that reports through tb.
func NewLedger(tb testing.TB, opts LedgerOptions) *Ledger {
	return &Ledger{tb: tb, opts: opts, next: map[string]int64{}, monotone: newMonotoneState()}
}

// Write inserts a document for client under id through node via, numbering it with the
// client's next seq, and records the ack or the refusal.
func (l *Ledger) Write(ctx context.Context, c *Cluster, via, client, id string, options ...wire.WriteOption) (wire.Inserted, error) {
	return l.write(ctx, c, via, client, id, false, options)
}

// Upsert is Write through the upsert path.
func (l *Ledger) Upsert(ctx context.Context, c *Cluster, via, client, id string, options ...wire.WriteOption) (wire.Inserted, error) {
	return l.write(ctx, c, via, client, id, true, options)
}

func (l *Ledger) write(ctx context.Context, c *Cluster, via, client, id string, upsert bool, options []wire.WriteOption) (wire.Inserted, error) {
	l.mu.Lock()
	l.next[client]++
	seq := l.next[client]
	l.mu.Unlock()
	running := c.Running()
	node, nodeUp := c.nodeIfRunning(via)
	upBefore := nodeUp && slices.Contains(running, via)
	var (
		got wire.Inserted
		err error
	)
	if upBefore {
		doc := bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: client}, {Key: "seq", Value: seq}}
		if upsert {
			got, err = node.Upsert(ctx, l.opts.DB, l.opts.Bucket, doc, options...)
		} else {
			got, err = node.Insert(ctx, l.opts.DB, l.opts.Bucket, doc, options...)
		}
	} else {
		err = fmt.Errorf("node %s is not running", via)
	}
	if err != nil {
		l.recordRefusal(client, id, via, err, upBefore && slices.Contains(c.Running(), via))
		return wire.Inserted{}, err
	}
	l.FeedAck(Ack{
		Client: client, ID: id, Seq: seq, Pos: got.Pos, Deduped: got.Deduped, Outcome: got.Outcome,
		Copies: got.CopiesConfirmed, RF: l.rfOf(c, via), Concern: wire.ConcernOf(options...), Via: via, ServedBy: got.ServedBy, Running: running, At: time.Now(),
	})
	return got, nil
}

// Read gets id through node via and records the seq in the body, or the refusal. It
// returns -1 with the error for NOT_FOUND and for a refusal.
func (l *Ledger) Read(ctx context.Context, c *Cluster, via, client, id string) (int64, error) {
	node, nodeUp := c.nodeIfRunning(via)
	upBefore := nodeUp && slices.Contains(c.Running(), via)
	var (
		doc wire.Document
		err error
	)
	if upBefore {
		doc, err = node.Get(ctx, l.opts.DB, l.opts.Bucket, id)
	} else {
		err = fmt.Errorf("node %s is not running", via)
	}
	if err != nil {
		l.recordRefusal(client, id, via, err, upBefore && slices.Contains(c.Running(), via))
		return -1, err
	}
	// A document with no name field is recorded with an empty Writer: it is nobody's counter,
	// so it never compares with another read and never counts as the client's own write.
	writer := ""
	if name, named := bson.Raw(doc.Body).Lookup("name").StringValueOK(); named {
		writer = name
	}
	seq, ok := bson.Raw(doc.Body).Lookup("seq").AsInt64OK()
	if !ok {
		err = fmt.Errorf("the document %s read through %s has no numeric seq: %v", id, via, bson.Raw(doc.Body))
		l.recordRefusal(client, id, via, err, true)
		return -1, err
	}
	l.FeedObs(Obs{Client: client, ID: id, Writer: writer, Seq: seq, Via: via, ServedBy: doc.ServedBy, At: time.Now(), ViaRunning: true})
	return seq, nil
}

func (l *Ledger) recordRefusal(client, id, via string, err error, viaRunning bool) {
	o := Obs{Client: client, ID: id, Seq: -1, Via: via, At: time.Now(), ViaRunning: viaRunning}
	var coded *cluster.Error
	if errors.As(err, &coded) {
		o.Code = coded.Code
	} else {
		o.Text = err.Error()
	}
	l.FeedObs(o)
}

// rfOf is the replication factor of the ledger's database as node via sees it. A node that has
// just restarted has not loaded the committed state yet and reports no database; the factor then
// comes from another running node, or the last one seen, because RF 0 would make I4 read a sound
// applied_degraded ack as "needs fewer than 1 copy".
func (l *Ledger) rfOf(c *Cluster, via string) uint8 {
	for _, id := range append([]string{via}, c.Running()...) {
		for _, database := range c.Node(id).State().Databases {
			if database.Name == l.opts.DB && database.RF != 0 {
				return l.settleRF(uint8(database.RF))
			}
		}
	}
	return l.settleRF(0)
}

// settleRF remembers found when it is a real factor and otherwise returns the one remembered.
func (l *Ledger) settleRF(found uint8) uint8 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if found != 0 {
		l.rf = found
	}
	return l.rf
}

// FeedAck records an acknowledgement. Write calls it; a test of the judge, or a replay of a
// recorded run, calls it by hand.
func (l *Ledger) FeedAck(ack Ack) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acks = append(l.acks, ackRec{Ack: ack, events: len(l.events), order: l.recorded})
	l.recorded++
	l.monotone.ack(ack)
}

// FeedObs records an observation. Read calls it; a test of the judge calls it by hand.
// A served read that regresses fails the test at the moment it is seen (I2, online).
func (l *Ledger) FeedObs(o Obs) {
	l.mu.Lock()
	l.obs = append(l.obs, obsRec{Obs: o, events: len(l.events), order: l.recorded})
	l.recorded++
	regression := l.monotone.read(o)
	after := eventBefore(l.events, len(l.events))
	l.mu.Unlock()
	if regression != "" {
		l.tb.Errorf("reads-monotone-per-client: %s%s", regression, afterText(after))
	}
}

// Fault records an injected fault, for example "kill n2" or "partition n1|n2,n3".
func (l *Ledger) Fault(what string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, Event{At: time.Now(), What: what})
}

// Note samples the committed epoch and state_seq, and the probe pair's position, on every
// running node, and records them as an Event.
func (l *Ledger) Note(c *Cluster, what string) {
	event := Event{At: time.Now(), What: what, Sampled: true, Positions: map[string]cluster.Position{}}
	for _, id := range c.Running() {
		node := c.Node(id)
		if l.opts.ProbeID != "" {
			if pos, err := node.PairPosition(context.Background(), l.opts.DB, l.opts.Bucket, l.opts.ProbeID); err == nil {
				event.Positions[id] = pos
			}
		}
		// State is read after the position so the sampled epoch is never older than it.
		state := node.State()
		event.Epoch = max(event.Epoch, uint64(state.Epoch))
		event.StateSeq = max(event.StateSeq, uint64(state.StateSeq))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

// Acks returns a copy of the acknowledgements in the order they were recorded.
func (l *Ledger) Acks() []Ack {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Ack, len(l.acks))
	for i, rec := range l.acks {
		out[i] = rec.Ack
	}
	return out
}

// Obs returns a copy of the observations in the order they were recorded.
func (l *Ledger) Obs() []Obs {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Obs, len(l.obs))
	for i, rec := range l.obs {
		out[i] = rec.Obs
	}
	return out
}

// Events returns a copy of the events in the order they were recorded.
func (l *Ledger) Events() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Event(nil), l.events...)
}

// eventBefore is the last injected fault among the first n events, or the last event of
// any kind when none was injected; nil when there is none.
func eventBefore(events []Event, n int) *Event {
	n = min(n, len(events))
	for i := n - 1; i >= 0; i-- {
		if !events[i].Sampled {
			event := events[i]
			return &event
		}
	}
	if n > 0 {
		event := events[n-1]
		return &event
	}
	return nil
}

func afterText(event *Event) string {
	if event == nil {
		return " (before any event)"
	}
	return fmt.Sprintf(" (after %q)", event.What)
}
