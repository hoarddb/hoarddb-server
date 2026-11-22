package machine

import (
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// The reference model and the harness around SeedRecord. The harness plays the
// caller of the machine (U45): it executes each outcome against a toy pair
// store, and an oracle replays the source's log in position order onto a plain
// map. Nothing here shares code with seed.go.

// seedStore is one pair's range: keys "h/<doc>" (header, "00" is the delete
// tombstone), "f/<doc>/Name", "c/<doc>" and "s/<doc>/<n>" (a fifo store) and
// "x/<position>" (a dedupe entry, an ordinary key). d is the node-local
// reclaim record of a delete and is not part of the range.
type seedStore struct {
	m map[string]string
	d map[string]bool
}

func newSeedStore() *seedStore { return &seedStore{m: map[string]string{}, d: map[string]bool{}} }

type seedOpKind uint8

const (
	seedOpInsert seedOpKind = iota + 1
	seedOpSet
	seedOpPush
	seedOpDelete
)

type seedOp struct {
	pos      cluster.Position
	kind     seedOpKind
	doc, val string
	size     int64
}

// seedTouch records that an op put or deleted a key.
type seedTouch func(key string, deleted bool)

func (st *seedStore) put(k, v string, touch seedTouch) {
	st.m[k] = v
	if touch != nil {
		touch(k, false)
	}
}

func (st *seedStore) del(k string, touch seedTouch) {
	if _, ok := st.m[k]; ok {
		delete(st.m, k)
		if touch != nil {
			touch(k, true)
		}
	}
}

// apply is one replicated batch (conv §11): a pure function of the store and
// the op, so every copy reaches the same state from the same order.
func (st *seedStore) apply(op seedOp, touch seedTouch) {
	switch op.kind {
	case seedOpInsert:
		// An insert over a tombstone cleans both of the document's prefixes
		// in its own batch and removes the reclaim record.
		for _, k := range slices.Collect(maps.Keys(st.m)) {
			if strings.HasPrefix(k, "f/"+op.doc+"/") || strings.HasPrefix(k, "s/"+op.doc+"/") || k == "c/"+op.doc {
				st.del(k, touch)
			}
		}
		delete(st.d, op.doc)
		st.put("h/"+op.doc, "live", touch)
		st.put("f/"+op.doc+"/Name", op.val, touch)
	case seedOpSet:
		st.put("f/"+op.doc+"/Name", op.val, touch)
	case seedOpPush:
		n, _ := strconv.Atoi(st.m["c/"+op.doc])
		n++
		st.put("c/"+op.doc, strconv.Itoa(n), touch)
		st.put(fmt.Sprintf("s/%s/%03d", op.doc, n), op.val, touch)
	case seedOpDelete:
		st.put("h/"+op.doc, "00", touch)
		st.d[op.doc] = true
	}
	st.put("x/"+op.pos.String(), "r", touch)
}

type seedChunk struct {
	kvs    [][2]string
	cursor []byte
	dedupe int
	bytes  int64
	final  bool
}

func (c seedChunk) event() ChunkStored {
	return ChunkStored{Cursor: c.cursor, Bytes: c.bytes, Entries: len(c.kvs), DedupeEntries: c.dedupe, Final: c.final}
}

type seedFrame struct {
	pos  cluster.Position
	size int64
}

// seedCoverage counts what a run exercised, so the test can fail if a seed
// set stops reaching the interesting rows.
type seedCoverage struct {
	readyRandom, readyFinal                  int
	restart1, restart3, restart4             int
	catchUps, dupChunks, resumes, syncErrors int
	lostFrames, dupFrames, partialAttempts   int
	lateChunks                               int
	earlierCuts                              int
}

func (c *seedCoverage) add(o seedCoverage) {
	c.readyRandom += o.readyRandom
	c.readyFinal += o.readyFinal
	c.restart1 += o.restart1
	c.restart3 += o.restart3
	c.restart4 += o.restart4
	c.catchUps += o.catchUps
	c.dupChunks += o.dupChunks
	c.resumes += o.resumes
	c.syncErrors += o.syncErrors
	c.lostFrames += o.lostFrames
	c.dupFrames += o.dupFrames
	c.partialAttempts += o.partialAttempts
	c.lateChunks += o.lateChunks
	c.earlierCuts += o.earlierCuts
}

type seedHarness struct {
	t   *testing.T
	rng *rand.Rand
	cfg SeedConfig
	cov seedCoverage

	// The source: its log in position order, its store, and its position.
	log   []seedOp
	byPos map[cluster.Position]seedOp
	src   *seedStore
	seq   uint64

	// The receiver: the machine's record and the pair's range.
	rec   SeedRecord
	local *seedStore
	id    [16]byte

	// The stream of the current attempt, as the source holds it.
	pinned    bool
	pinCut    cluster.Position
	chunks    []seedChunk
	nextChunk int
	lastChunk int
	inflight  []seedFrame
	quiet     bool // no loss, no duplication, no injected errors

	// What the caller remembers for its checks.
	pendingSync  bool
	finalApplied bool
	drainLast    cluster.Position
	readyOnce    bool
	restarts     int // consecutive restarts since the last test action
}

func newSeedHarness(t *testing.T, rng *rand.Rand, epoch uint64) *seedHarness {
	return &seedHarness{
		t: t, rng: rng, byPos: map[cluster.Position]seedOp{}, src: newSeedStore(), local: newSeedStore(), lastChunk: -1,
		cfg: SeedConfig{SyncBytes: 150, BufferBytes: 400, WindowBytes: 8 << 20, Epoch: epoch},
	}
}

// tail is the source's appended position: the last record that exists, which
// belongs to an earlier epoch while the committed epoch has ordered nothing.
func (h *seedHarness) tail() cluster.Position {
	if h.seq == 0 && len(h.log) > 0 {
		return h.log[len(h.log)-1].pos
	}
	return seedPos(h.cfg.Epoch, h.seq)
}

// commit orders one op at the source, as the owner would.
func (h *seedHarness) commit(op seedOp) seedOp {
	h.seq++
	op.pos = seedPos(h.cfg.Epoch, h.seq)
	h.log = append(h.log, op)
	h.byPos[op.pos] = op
	h.src.apply(op, nil)
	if h.pinned && h.rec.State == SeedSeeding {
		f := seedFrame{pos: op.pos, size: op.size}
		switch r := h.rng.Intn(100); {
		case !h.quiet && r < 8:
			h.cov.lostFrames++
		case !h.quiet && r < 16:
			h.cov.dupFrames++
			h.inflight = append(h.inflight, f, f)
		default:
			h.inflight = append(h.inflight, f)
		}
	}
	return op
}

// randomOp picks an op that is valid for the source's current state.
func (h *seedHarness) randomOp() seedOp {
	doc := fmt.Sprintf("d%d", 1+h.rng.Intn(4))
	size := int64(10 + h.rng.Intn(110))
	val := fmt.Sprintf("v%d", h.rng.Intn(1000))
	live := h.src.m["h/"+doc] == "live"
	switch r := h.rng.Intn(10); {
	case !live:
		return seedOp{kind: seedOpInsert, doc: doc, val: val, size: size}
	case r < 3:
		return seedOp{kind: seedOpSet, doc: doc, val: val, size: size}
	case r < 7:
		return seedOp{kind: seedOpPush, doc: doc, val: val, size: size}
	default:
		return seedOp{kind: seedOpDelete, doc: doc, size: size}
	}
}

func (h *seedHarness) snapshotChunks() []seedChunk {
	keys := slices.Sorted(maps.Keys(h.src.m))
	if len(keys) == 0 {
		return []seedChunk{{final: true}}
	}
	var chunks []seedChunk
	for len(keys) > 0 {
		n := min(len(keys), 1+h.rng.Intn(4))
		c := seedChunk{cursor: []byte(keys[n-1])}
		for _, k := range keys[:n] {
			c.kvs = append(c.kvs, [2]string{k, h.src.m[k]})
			c.bytes += int64(len(k) + len(h.src.m[k]))
			if strings.HasPrefix(k, "x/") {
				c.dedupe++
			}
		}
		keys = keys[n:]
		c.final = len(keys) == 0
		chunks = append(chunks, c)
	}
	return chunks
}

// apply feeds the machine and checks the invariants that hold after every event.
func (h *seedHarness) apply(ev SeedEvent) SeedOutcome {
	h.t.Helper()
	prev := seedClone(h.rec)
	out := h.rec.Apply(ev, h.cfg)
	r := h.rec
	if out.Restart != 0 && !seedSame(prev, r) {
		h.t.Fatalf("%T: restart %d changed the record %+v -> %+v", ev, out.Restart, prev, r)
	}
	// A snapshot holds its cut until the drain, except a record reloaded in the buffer
	// phase: it resumes on the same cut with Applied already past it (see restartNode).
	resumedFromBuffer := prev.Phase == PhaseBuffer && r.Phase == PhaseSnapshot
	if r.State == SeedSeeding && r.Phase == PhaseSnapshot && r.Applied != r.Cut && !resumedFromBuffer {
		h.t.Fatalf("%T: applied %v moved off the cut %v during the snapshot", ev, r.Applied, r.Cut)
	}
	if (r.State == SeedReady && prev.State != SeedReady) != out.MarkReady {
		h.t.Fatalf("%T: state %d -> %d with MarkReady %v", ev, prev.State, r.State, out.MarkReady)
	}
	if out.MarkReady && !out.RerunSweeps {
		h.t.Fatalf("%T: ready without the sweeps re-run", ev)
	}
	if prev.State == SeedSeeding && r.State == SeedSeeding && prev.SeedID == r.SeedID && r.Applied.Less(prev.Applied) {
		h.t.Fatalf("%T: applied went back %v -> %v", ev, prev.Applied, r.Applied)
	}
	var sum int64
	for i, p := range r.Buffered {
		if (i > 0 && !r.Buffered[i-1].Less(p)) || !r.Applied.Less(p) {
			h.t.Fatalf("%T: buffered %v is not ascending above applied %v", ev, r.Buffered, r.Applied)
		}
		sum += r.bufferedSize[i]
	}
	if sum != r.BufferedBytes || r.BufferedBytes > h.cfg.BufferBytes {
		h.t.Fatalf("%T: buffered bytes %d (sum %d, bound %d)", ev, r.BufferedBytes, sum, h.cfg.BufferBytes)
	}
	for _, p := range out.ApplyBuffered {
		want := cluster.Position{Epoch: prev.Applied.Epoch, Seq: prev.Applied.Seq + 1}
		if prev.Applied.Epoch < h.cfg.Epoch {
			want = seedPos(h.cfg.Epoch, 1)
		}
		if p != want {
			h.t.Fatalf("%T: applied %v out of order after %v", ev, p, prev.Applied)
		}
	}
	if out.ApplyChunk && prev.Phase != PhaseSnapshot {
		h.t.Fatalf("%T: a chunk applied in phase %d", ev, prev.Phase)
	}
	return out
}

// handle executes an outcome as the caller would.
func (h *seedHarness) handle(out SeedOutcome) {
	h.t.Helper()
	for _, p := range out.ApplyBuffered {
		h.local.apply(h.byPos[p], nil)
	}
	if out.Sync {
		h.pendingSync = true
	}
	if out.MarkReady {
		h.onReady()
	}
	switch out.Restart {
	case 0:
	case 1:
		h.cov.restart1++
	case 3:
		h.cov.restart3++
	case 4:
		h.cov.restart4++
	default:
		h.t.Fatalf("restart reason %d is not defined", out.Restart)
	}
	if out.Restart != 0 {
		h.startAttempt(h.quiet || h.rng.Intn(6) != 0)
		return
	}
	if out.AskCatchUp != nil {
		h.catchUp(*out.AskCatchUp)
	}
}

// startAttempt writes a fresh attempt: reset when the range holds anything,
// then register unless the test wants to leave the attempt half made.
func (h *seedHarness) startAttempt(register bool) {
	h.t.Helper()
	h.restarts++
	if h.restarts > 40 {
		h.t.Fatalf("restarting forever: the attempt cannot register (record %+v, committed epoch %d)", h.rec, h.cfg.Epoch)
	}
	nonEmpty := len(h.local.m) > 0
	h.id[0]++
	out := h.apply(AttemptWritten{SeedID: h.id, Source: "src", LocalNonEmpty: nonEmpty})
	if h.rec.State != SeedSeeding || (nonEmpty && out.Phase != PhaseReset) {
		h.t.Fatalf("attempt over %d local keys: state %d phase %d", len(h.local.m), h.rec.State, out.Phase)
	}
	if out.Phase == PhaseReset {
		h.local = newSeedStore()
	}
	h.pinned, h.inflight, h.pendingSync, h.finalApplied, h.drainLast, h.lastChunk = false, nil, false, false, cluster.Position{}, -1
	h.readyOnce = false
	if register {
		h.register()
	} else {
		h.cov.partialAttempts++
	}
}

// register pins a snapshot at the source's appended position.
func (h *seedHarness) register() {
	h.t.Helper()
	h.pinCut, h.pinned = h.tail(), true
	if h.pinCut.Epoch < h.cfg.Epoch {
		h.cov.earlierCuts++
	}
	h.chunks, h.nextChunk, h.lastChunk, h.inflight = h.snapshotChunks(), 0, -1, nil
	h.handle(h.apply(RegisterAccepted{Cut: h.pinCut, WindowBytes: h.cfg.WindowBytes}))
}

func (h *seedHarness) deliverChunk() {
	h.t.Helper()
	if h.rec.State != SeedSeeding || h.rec.Phase != PhaseSnapshot || !h.pinned || h.nextChunk >= len(h.chunks) {
		return
	}
	c := h.chunks[h.nextChunk]
	out := h.apply(c.event())
	if out.Restart != 0 {
		h.handle(out)
		return
	}
	if !out.ApplyChunk {
		h.t.Fatalf("the next chunk %d was not applied: %+v record %+v", h.nextChunk, out, h.rec)
	}
	for _, kv := range c.kvs {
		h.local.m[kv[0]] = kv[1]
	}
	if c.final {
		h.finalApplied = true
	}
	h.lastChunk, h.nextChunk = h.nextChunk, h.nextChunk+1
	h.handle(out)
}

// dupChunk resends a chunk already delivered: it must change nothing.
func (h *seedHarness) dupChunk() {
	h.t.Helper()
	if h.rec.State != SeedSeeding || !h.pinned || h.lastChunk < 0 {
		return
	}
	c := h.chunks[h.rng.Intn(h.lastChunk+1)]
	prev := seedClone(h.rec)
	out := h.apply(c.event())
	if out.Restart != 0 {
		h.handle(out)
		return
	}
	if out.ApplyChunk || !seedSame(prev, h.rec) {
		h.t.Fatalf("a duplicate chunk changed the record or was applied: %+v %+v -> %+v", out, prev, h.rec)
	}
	h.cov.dupChunks++
}

// lateChunk delivers a chunk of a stale stream after the snapshot is over, one
// whose last key is past the cursor: applying it would put an old value over
// newer data, so the machine must take nothing from it.
func (h *seedHarness) lateChunk() {
	h.t.Helper()
	if h.rec.State != SeedSeeding || (h.rec.Phase != PhaseBuffer && h.rec.Phase != PhaseNone) {
		return
	}
	prev := seedClone(h.rec)
	out := h.apply(ChunkStored{Cursor: []byte("zzzz"), Bytes: 5, Entries: 1})
	if out.Restart != 0 {
		h.handle(out)
		return
	}
	if out.ApplyChunk || !seedSame(prev, h.rec) {
		h.t.Fatalf("a late chunk was applied or changed the record: %+v %+v -> %+v", out, prev, h.rec)
	}
	h.cov.lateChunks++
}

func (h *seedHarness) deliverFrame() {
	h.t.Helper()
	if len(h.inflight) == 0 {
		return
	}
	f := h.inflight[0]
	h.inflight = h.inflight[1:]
	h.handle(h.apply(LiveWriteBuffered{Pos: f.pos, Bytes: f.size}))
}

// step applies a buffered position, usually the first and sometimes a later one
// so the gap row is reached.
func (h *seedHarness) step() {
	h.t.Helper()
	if h.rec.State != SeedSeeding || h.rec.Phase != PhaseBuffer || len(h.rec.Buffered) == 0 {
		return
	}
	i := 0
	if !h.quiet && h.rng.Intn(5) == 0 {
		i = h.rng.Intn(len(h.rec.Buffered))
	}
	p := h.rec.Buffered[i]
	out := h.apply(BufferApplyStep{Pos: p, Bytes: h.byPos[p].size})
	if out.AskCatchUp != nil && i == 0 && *out.AskCatchUp == p {
		h.t.Fatalf("asked for the position it was given")
	}
	h.handle(out)
}

// catchUp feeds the source's log from the given position to its tail, as a
// CATCHUP_CHUNK would.
func (h *seedHarness) catchUp(from cluster.Position) {
	h.t.Helper()
	h.cov.catchUps++
	for p := from; ; p.Seq++ {
		op, ok := h.byPos[p]
		if !ok || p.Epoch != h.cfg.Epoch {
			return
		}
		if h.rec.State != SeedSeeding || h.rec.Phase != PhaseBuffer {
			return
		}
		out := h.apply(BufferApplyStep{Pos: p, Bytes: op.size})
		if out.Restart != 0 {
			h.handle(out)
			return
		}
		if len(out.ApplyBuffered) != 1 {
			h.t.Fatalf("catch-up position %v refused: %+v record %+v", p, out, h.rec)
		}
		h.handle(out)
	}
}

func (h *seedHarness) drain() {
	h.t.Helper()
	tail := h.tail()
	out := h.apply(BufferDrained{Last: tail})
	if out.Phase == PhaseNone && out.Sync && out.Restart == 0 {
		h.drainLast = tail
	}
	h.handle(out)
}

func (h *seedHarness) syncDone() {
	h.t.Helper()
	if !h.pendingSync {
		return
	}
	h.pendingSync = false
	var err error
	if !h.quiet && h.rng.Intn(12) == 0 {
		err = errors.New("fsync failed")
		h.cov.syncErrors++
	}
	h.handle(h.apply(SyncDone{Err: err}))
}

// restartNode is a crash and restart: only the persisted record survives, the
// buffer and the in-flight frames are gone, and the caller resumes.
func (h *seedHarness) restartNode() {
	h.t.Helper()
	if h.rec.State != SeedSeeding {
		return
	}
	h.rec.Buffered, h.rec.bufferedSize, h.rec.BufferedBytes, h.rec.Dirty = nil, nil, 0, 0
	h.inflight, h.pendingSync = nil, false
	switch {
	case h.rec.Phase == PhaseReset:
		h.local = newSeedStore() // the reset delete is owed, and redoing it is harmless
		h.register()
	case !h.pinned || h.rng.Intn(10) < 3: // the pin is gone: SEED_RESTART reason 1
		h.cov.restart1++
		h.startAttempt(true)
	default:
		h.cov.resumes++
		prev := seedClone(h.rec)
		// A record reloaded in the buffer phase resumes as a snapshot on its cut: the
		// buffer only ever lived in memory, so nothing of it survives the restart
		// (machine seed.go, register/resume-same-cut).
		want := seedClone(prev)
		if want.Phase == PhaseBuffer {
			want.Phase, want.Buffered, want.bufferedSize, want.BufferedBytes = PhaseSnapshot, nil, nil, 0
		}
		out := h.apply(RegisterAccepted{Cut: h.pinCut, WindowBytes: h.cfg.WindowBytes})
		if out.Restart != 0 || !seedSame(want, h.rec) {
			h.t.Fatalf("resuming on the same cut: %+v %+v -> %+v", out, want, h.rec)
		}
		h.nextChunk = 0
		for h.nextChunk < len(h.chunks) && len(h.rec.Cursor) > 0 && string(h.chunks[h.nextChunk].cursor) <= string(h.rec.Cursor) {
			h.nextChunk++
		}
		h.lastChunk = h.nextChunk - 1
		switch {
		case prev.Phase == PhaseBuffer:
			// The source has nothing past the cursor the resumed record holds, so it
			// answers the next ack with an empty final chunk; that ends the snapshot
			// again and the drain follows.
			final := h.apply(ChunkStored{Final: true, Entries: 0})
			if final.Restart != 0 || !final.ApplyChunk || h.rec.Phase != PhaseBuffer {
				h.t.Fatalf("the empty final chunk after the resume: %+v phase %d", final, h.rec.Phase)
			}
			h.handle(final)
			h.catchUp(seedPos(h.rec.Applied.Epoch, h.rec.Applied.Seq+1))
		case h.rec.Phase == PhaseNone:
			h.pendingSync = true // a record loaded drained owes its sync
		}
	}
}

func (h *seedHarness) sourceDies() {
	h.t.Helper()
	h.pinned, h.inflight = false, nil
	if h.rec.State != SeedSeeding {
		return
	}
	out := h.apply(RegisterRefused{Reason: 3})
	if out.Restart != 1 {
		h.t.Fatalf("a source that is not a holder: %+v", out)
	}
	h.handle(out)
}

// epochChanges moves the committed epoch and probes the machine with an event
// of the old attempt.
func (h *seedHarness) epochChanges() {
	h.t.Helper()
	old := h.cfg.Epoch
	h.cfg.Epoch++
	h.seq = 0
	h.pinned = false
	if h.rec.State != SeedSeeding {
		return
	}
	var ev SeedEvent
	switch h.rng.Intn(6) {
	case 5:
		ev = LiveWriteBuffered{Pos: seedPos(h.cfg.Epoch+1, 1), Bytes: 10} // the source is ahead
	case 0:
		ev = LiveWriteBuffered{Pos: seedPos(old, 1000), Bytes: 10}
	case 1:
		ev = ChunkStored{Cursor: []byte("zzz"), Bytes: 10, Entries: 1}
	case 2:
		ev = BufferDrained{Last: seedPos(h.cfg.Epoch, 0)}
	case 3:
		ev = SyncDone{}
	default:
		ev = BufferApplyStep{Pos: seedPos(h.cfg.Epoch, 1), Bytes: 10}
	}
	registered := h.rec.Phase != PhaseReset
	live, isLive := ev.(LiveWriteBuffered)
	ahead := isLive && live.Pos.Epoch > h.cfg.Epoch
	out := h.apply(ev)
	if (registered || ahead) && out.Restart != 3 {
		h.t.Fatalf("%T after an epoch change on a registered attempt (phase %d): %+v", ev, h.rec.Phase, out)
	}
	h.handle(out)
}

// reference replays the source's log through upTo, in position order, onto a
// plain map. last says, for every key an op touched, whether the final touch
// deleted it.
func (h *seedHarness) reference(upTo cluster.Position) (*seedStore, map[string]bool) {
	st, last := newSeedStore(), map[string]bool{}
	for _, op := range h.log {
		if upTo.Less(op.pos) {
			break
		}
		st.apply(op, func(k string, deleted bool) { last[k] = deleted })
	}
	return st, last
}

// checkContents compares the receiver's range with the reference at upTo.
func (h *seedHarness) checkContents(upTo cluster.Position, why string) {
	h.t.Helper()
	want, last := h.reference(upTo)
	for k, deleted := range last {
		got, has := h.local.m[k]
		switch {
		case deleted && has:
			h.t.Fatalf("%s: deleted key %q reappeared with %q", why, k, got)
		case !deleted && (!has || got != want.m[k]):
			h.t.Fatalf("%s: key %q lost or stale: have %q (present %v), want %q", why, k, got, has, want.m[k])
		}
	}
	if !maps.Equal(h.local.m, want.m) {
		h.t.Fatalf("%s: contents differ from the reference at %v:\n got  %v\n want %v", why, upTo, h.local.m, want.m)
	}
}

// onReady checks the invariants of the moment a record becomes ready.
func (h *seedHarness) onReady() {
	h.t.Helper()
	r := h.rec
	switch {
	case !h.finalApplied:
		h.t.Fatalf("ready before the snapshot was complete")
	case h.drainLast.Zero() || r.Applied.Less(h.drainLast):
		h.t.Fatalf("ready at applied %v before the buffer drained through the source's appended %v", r.Applied, h.drainLast)
	case r.Applied.Less(h.pinCut):
		h.t.Fatalf("ready at applied %v below the cut %v", r.Applied, h.pinCut)
	case len(r.Buffered) != 0:
		h.t.Fatalf("ready with %v still buffered", r.Buffered)
	case h.readyOnce:
		h.t.Fatalf("ready twice in one attempt")
	}
	h.readyOnce = true
	h.checkContents(r.Applied, fmt.Sprintf("ready at %v", r.Applied))
}

// randomPhase runs n random actions.
func (h *seedHarness) randomPhase(n int) {
	h.t.Helper()
	for i := 0; i < 8; i++ {
		h.commit(h.randomOp())
	}
	h.startAttempt(true)
	step := 0
	actions := []struct {
		weight int
		run    func()
	}{
		{24, func() {
			if step < n/4 || h.rng.Intn(8) == 0 { // the source quiets down so attempts can finish
				h.commit(h.randomOp())
			}
		}},
		{36, h.deliverFrame},
		{40, h.deliverChunk},
		{8, h.dupChunk},
		{4, h.lateChunk},
		{24, h.step},
		{18, h.drain},
		{18, h.syncDone},
		{2, h.restartNode},
		{1, h.sourceDies},
		{1, h.epochChanges},
		{10, func() {
			if h.rec.State == SeedSeeding && h.rec.Phase == PhaseReset && !h.pinned {
				h.register()
			}
		}},
	}
	total := 0
	for _, a := range actions {
		total += a.weight
	}
	for i := 0; i < n; i++ {
		step = i
		h.restarts = 0
		before := h.rec.State
		r := h.rng.Intn(total)
		for _, a := range actions {
			if r < a.weight {
				a.run()
				break
			}
			r -= a.weight
		}
		if before != SeedReady && h.rec.State == SeedReady {
			h.cov.readyRandom++
			h.startAttempt(true) // reseed from a ready copy, as a log-floor miss would
		}
	}
}

// quiesce stops the source and the faults and drives the attempt to ready.
func (h *seedHarness) quiesce() {
	h.t.Helper()
	h.quiet = true
	if h.rec.State != SeedSeeding {
		h.startAttempt(true)
	}
	for i := 0; i < 5000; i++ {
		h.restarts = 0
		r := &h.rec
		switch {
		case r.State == SeedReady:
			h.cov.readyFinal++
			h.checkContents(h.tail(), "after quiescing")
			if r.Applied != h.tail() {
				h.t.Fatalf("ready at %v, want the source's tail %v", r.Applied, h.tail())
			}
			return
		case r.State != SeedSeeding:
			h.t.Fatalf("state %d while quiescing", r.State)
		case h.pendingSync:
			h.syncDone()
		case r.Phase == PhaseReset:
			h.register()
		case r.Phase == PhaseSnapshot && !h.pinned:
			h.startAttempt(true)
		case len(h.inflight) > 0:
			h.deliverFrame()
		case r.Phase == PhaseSnapshot:
			h.deliverChunk()
		case r.Phase == PhaseBuffer && len(r.Buffered) > 0:
			h.step()
		case r.Phase == PhaseBuffer:
			h.drain()
		default:
			h.t.Fatalf("stuck in state %d phase %d", r.State, r.Phase)
		}
	}
	h.t.Fatalf("not ready after 5000 steps: %+v", h.rec)
}

// TestSeedModelAgainstReference runs 20 fixed seeds of random event sequences
// (register, snapshot chunk, buffered write, delete, re-insert, duplicate
// chunk, resume after restart, source death, epoch change mid-seed, sync)
// against the reference map and checks the invariants every time the machine
// reports ready.
func TestSeedModelAgainstReference(t *testing.T) {
	var total seedCoverage
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprintf("seed%02d", seed), func(t *testing.T) {
			h := newSeedHarness(t, rand.New(rand.NewSource(seed)), 5)
			h.randomPhase(1500)
			h.quiesce()
			total.add(h.cov)
		})
	}
	t.Logf("coverage over 20 seeds: %+v", total)
	if total.readyRandom < 10 {
		t.Errorf("only %d readies in the random phases; the content check at ready needs more", total.readyRandom)
	}
	for name, n := range map[string]int{
		"ready during the random phase": total.readyRandom, "final ready": total.readyFinal,
		"restart reason 1": total.restart1, "restart reason 3 (epoch)": total.restart3, "restart reason 4": total.restart4,
		"catch-ups": total.catchUps, "duplicate chunks": total.dupChunks, "resumes": total.resumes,
		"sync errors": total.syncErrors, "lost frames": total.lostFrames, "duplicated frames": total.dupFrames,
		"half-made attempts": total.partialAttempts, "late chunks": total.lateChunks, "cuts in an earlier epoch": total.earlierCuts,
	} {
		if n == 0 {
			t.Errorf("the 20 seeds never reached: %s", name)
		}
	}
}

// TestSeedDeleteThenReinsertSameIDDuringSeed is the worked example of seeding
// Formats 3: source A owns the slot at 41:100, p1 has Name "Ada" and a fifo of
// three elements, R registers at the cut. A then orders 41:101 (delete p1) and
// 41:102 (insert p1 with Name "Grace"). Wherever in the stream those two
// frames land, R ends identical to A: the three deleted elements do not come
// back, and the re-inserted p1 is not lost.
func TestSeedDeleteThenReinsertSameIDDuringSeed(t *testing.T) {
	setup := func(t *testing.T) *seedHarness {
		h := newSeedHarness(t, rand.New(rand.NewSource(7)), 41)
		h.quiet = true
		h.seq = 96
		h.commit(seedOp{kind: seedOpInsert, doc: "p1", val: "Ada", size: 20}) // 41:97
		for _, e := range []string{"e1", "e2", "e3"} {
			h.commit(seedOp{kind: seedOpPush, doc: "p1", val: e, size: 20}) // 41:98..100
		}
		h.startAttempt(true)
		if h.pinCut != seedPos(41, 100) {
			t.Fatalf("cut = %v, want 41:100", h.pinCut)
		}
		return h
	}
	chunks := len(setup(t).chunks)
	for delivered := 0; delivered <= chunks; delivered++ {
		t.Run(fmt.Sprintf("after-%d-of-%d-chunks", delivered, chunks), func(t *testing.T) {
			h := setup(t)
			for i := 0; i < delivered; i++ {
				h.deliverChunk()
			}
			h.commit(seedOp{kind: seedOpDelete, doc: "p1", size: 15})               // 41:101
			h.commit(seedOp{kind: seedOpInsert, doc: "p1", val: "Grace", size: 25}) // 41:102
			if got, want := h.src.m["f/p1/Name"], "Grace"; got != want || h.src.m["h/p1"] != "live" {
				t.Fatalf("source p1 = %q header %q", got, h.src.m["h/p1"])
			}
			for k := range h.src.m {
				if strings.HasPrefix(k, "s/p1/") || k == "c/p1" {
					t.Fatalf("source still holds %q after the re-insert", k)
				}
			}
			h.quiesce()
			if !maps.Equal(h.local.m, h.src.m) {
				t.Fatalf("R differs from A:\n R %v\n A %v", h.local.m, h.src.m)
			}
			for k := range h.local.m {
				if strings.HasPrefix(k, "s/p1/") || k == "c/p1" {
					t.Errorf("R holds %q: a deleted element came back", k)
				}
			}
			if h.local.m["f/p1/Name"] != "Grace" || h.local.m["h/p1"] != "live" {
				t.Errorf("R lost the re-insert: %v", h.local.m)
			}
			if h.local.d["p1"] || h.src.d["p1"] {
				t.Errorf("a reclaim record survived the re-insert")
			}
			if h.rec.Applied != seedPos(41, 102) {
				t.Errorf("applied = %v, want 41:102", h.rec.Applied)
			}
		})
	}

	t.Run("delete-only-keeps-the-tombstone-and-the-elements-on-both", func(t *testing.T) {
		h := setup(t)
		h.commit(seedOp{kind: seedOpDelete, doc: "p1", size: 15})
		h.quiesce()
		if !maps.Equal(h.local.m, h.src.m) || h.local.m["h/p1"] != "00" || h.local.m["s/p1/003"] != "e3" {
			t.Fatalf("R %v A %v: both must hold the tombstone and the unswept elements", h.local.m, h.src.m)
		}
	})

	t.Run("a-re-registration-after-the-re-insert-carries-Grace-and-resets", func(t *testing.T) {
		h := setup(t)
		h.deliverChunk()
		h.commit(seedOp{kind: seedOpDelete, doc: "p1", size: 15})
		h.commit(seedOp{kind: seedOpInsert, doc: "p1", val: "Grace", size: 25})
		h.startAttempt(true) // the source died: a fresh cut at 41:102 over a non-empty range
		if h.pinCut != seedPos(41, 102) {
			t.Fatalf("cut = %v", h.pinCut)
		}
		h.quiesce()
		if !maps.Equal(h.local.m, h.src.m) || h.local.m["f/p1/Name"] != "Grace" {
			t.Fatalf("R %v A %v", h.local.m, h.src.m)
		}
	})
}
