package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The source side of the seed channel (seeding sections 4 and 5, conventions 6.1 and 12).
//
// # Connection lifecycle
//
// The internode channel is request and reply: one frame in, one frame out, in order, on a
// connection that opened with a handshake for the channel. A seed is therefore a pull. The
// receiver sends SEED_REGISTER and gets SEED_ACCEPT (or one of the refusals). It then sends
// SEED_ACK, which states its applied position and its absolute free credit, and the reply to
// each ACK is the next SEED_CHUNK (or SEED_RESTART). The ACK that follows the final chunk
// ends the seed and is answered with nothing. The credit is a bound on the size of the
// chunk the reply may carry, so the source never sends past it.
//
// The connection carries no seed state. The session is the seed_id: a pin keyed by it holds
// the snapshot, the cut, the cursor and the chunk index at the source. A receiver whose
// connection broke (the server closes one that is idle for seed_idle_seconds, a peer
// restarts, a reply is lost) dials again and sends SEED_REGISTER with the same seed_id and
// the last key it applied; the source answers with the pinned cut and resume=1 and streams
// on from that key, with chunk indices counted again from 0. One connection per (receiver,
// seed_id) would pin a descriptor and a goroutine to every seed for no gain, and would not
// survive the very failure the pin exists for. The peer network keeps one connection per
// source per channel and a seed call is short (one chunk), so a stalled pull delays another
// seed from the same receiver by one call at most.
//
// # The cut
//
// An RF 1 owner write appends its position unsynced in the first section of its pair's lock
// and applies it in the second, after the fsync (advance.go, writepath.go); a locked-path
// write, RF 2 and larger, holds the lock from its append through its commit. takeCut runs
// under PairSet.With, which first waits for the pending owner writes to apply, so at the cut
// every appended write is applied and AppliedPos names the last of them. So: a write at or
// below AppliedPos is in the snapshot; a write that appends after the cut is released applies
// after the snapshot opened, so it is not in the snapshot and its position is above
// AppliedPos (positions are assigned in order under the lock). Every write at or below the
// cut is in the snapshot, every write above it is delivered by the ordinary WRITE stream or
// by catch-up, and none is in both. The cut is AppliedPos and not AppendedSeq: a position
// that was appended but whose apply failed (write path row 9) is not in the snapshot, so it
// must be above the cut, and it is, by one.
//
// # Cost
//
// Per pin: one snapshot handle (a read transaction held open, which keeps the Badger
// versions it can see from being collected), one cursor key, about 150 bytes. Pins are
// bounded by incoming_seeds_max per source and are dropped on completion, after
// seed_resume_hold_seconds without a call from the receiver, and on Close. Nothing is per
// bucket, per database or per slot held, no file descriptor or mapping is added, and one
// goroutine per node (the reaper) ends with the node. A membership change does no work here.

// SeedOptions are the replication.* bounds of the seed channel. A zero field takes the
// default of config.DefaultReplicationConfig.
type SeedOptions struct {
	// WindowBytes is replication.seed_window_bytes: the credit a SEED_ACCEPT opens.
	WindowBytes int
	// ChunkBytes and ChunkEntries are replication.seed_chunk_bytes and _entries: a chunk
	// ends at the first of the two and an entry is never split.
	ChunkBytes, ChunkEntries int
	// ResumeHold is replication.seed_resume_hold_seconds: how long a pin outlives the last
	// call of its receiver.
	ResumeHold time.Duration
	// Idle is replication.seed_idle_seconds: the silence after which the source closes a
	// seed connection and the receiver gives up on a call.
	Idle time.Duration
	// DiskHighWaterPct is replication.seed_disk_high_water_pct: a source above it refuses a
	// new seed with reason 1.
	DiskHighWaterPct int
	// IncomingMax is replication.incoming_seeds_max: the pins a source holds at once.
	IncomingMax int
	// BusyRetryAfter is the retryAfterMs of SEED_BUSY. The spec gives no number (SPEC-GAP T2-4).
	BusyRetryAfter time.Duration
	// MaxConns is the connection cap of the seed channel's server; 0 takes the transport's.
	MaxConns int
	// DiskUsedPct reads the percentage of the data directory's filesystem in use; nil reads
	// it with statfs. A test replaces it.
	DiskUsedPct func() (int, error)
	// Now reads the clock the hold is measured on; nil takes time.Now.
	Now func() time.Time
	// SlotsInFlight is replication.seed_slots_in_flight: how many pairs this node pulls at
	// once as a receiver. The pull has this many goroutines and no more, however many pairs
	// are queued.
	SlotsInFlight int
	// SyncBytes is replication.seed_sync_bytes: the receiver syncs its store after this many
	// bytes of chunks.
	SyncBytes int64
	// BufferBytes is replication.seed_buffer_bytes: the bound of the live-write buffer of a
	// seeding pair (built by T4; the seed machine already reads it).
	BufferBytes int64
	// RetryBackoff is how long a pull waits before it asks again after a failed call, a BUSY
	// without a hint, a disk refusal or a restart. The spec gives no number (SPEC-GAP T3-5).
	RetryBackoff time.Duration
	// BytesPerSecond is replication.seed_bytes_per_second: the node-wide byte rate of every pull
	// this node runs as a receiver. Zero is unlimited, and withDefaults leaves it alone: the
	// config's default is the config's to set, not this package's.
	BytesPerSecond int64
}

// seedDefaults mirrors config.DefaultReplicationConfig (a test keeps them equal), because
// this package takes no dependency on config.
const (
	seedDefaultWindowBytes  = 8388608
	seedDefaultChunkBytes   = 1048576
	seedDefaultChunkEntries = 1024
	seedDefaultHold         = 300 * time.Second
	seedDefaultIdle         = 120 * time.Second
	seedDefaultHighWaterPct = 85
	seedDefaultIncomingMax  = 4
	seedDefaultBusyRetry    = time.Second
	seedDefaultSlots        = 2
	seedDefaultSyncBytes    = 67108864
	seedDefaultBufferBytes  = 268435456
	seedDefaultRetry        = 500 * time.Millisecond
)

func (o SeedOptions) withDefaults() SeedOptions {
	if o.WindowBytes <= 0 {
		o.WindowBytes = seedDefaultWindowBytes
	}
	if o.ChunkBytes <= 0 {
		o.ChunkBytes = seedDefaultChunkBytes
	}
	if o.ChunkEntries <= 0 {
		o.ChunkEntries = seedDefaultChunkEntries
	}
	if o.ResumeHold <= 0 {
		o.ResumeHold = seedDefaultHold
	}
	if o.Idle <= 0 {
		o.Idle = seedDefaultIdle
	}
	if o.DiskHighWaterPct <= 0 {
		o.DiskHighWaterPct = seedDefaultHighWaterPct
	}
	if o.IncomingMax <= 0 {
		o.IncomingMax = seedDefaultIncomingMax
	}
	if o.BusyRetryAfter <= 0 {
		o.BusyRetryAfter = seedDefaultBusyRetry
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.SlotsInFlight <= 0 {
		o.SlotsInFlight = seedDefaultSlots
	}
	if o.SyncBytes <= 0 {
		o.SyncBytes = seedDefaultSyncBytes
	}
	if o.BufferBytes <= 0 {
		o.BufferBytes = seedDefaultBufferBytes
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = seedDefaultRetry
	}
	return o
}

// seedMaskResume is bit 0 of SEED_REGISTER.mask: the requester's range is non-empty and its
// cut is on disk, so this is a resume (owner decision 2026-11-01, Q9). Every other bit is
// reserved and refused with reason 5.
const seedMaskResume byte = 1

// SEED_REFUSED reasons (conventions 6.1).
const (
	seedRefusedDisk       byte = 1
	seedRefusedDropped    byte = 2
	seedRefusedNotHolder  byte = 3
	seedRefusedNotCurrent byte = 4
	seedRefusedMask       byte = 5
)

// SEED_RESTART reasons (conventions 6.1).
const (
	seedRestartPinGone      byte = 1
	seedRestartEpochChanged byte = 3
)

// SeedSourceConfig is what a SeedSource reads from its node.
type SeedSourceConfig struct {
	// Self is this node's id.
	Self string
	// Options are the replication bounds.
	Options SeedOptions
	// Pairs is the node's pair set: the pair lock the cut is taken under.
	Pairs *cluster.PairSet
	// KV opens the snapshots.
	KV interface{ NewSnapshot() storage.KVSnapshot }
	// Committed and StateSeq are the node's committed epoch and state sequence.
	Committed, StateSeq func() uint64
	// Database reports whether database db exists at the committed epoch.
	Database func(db uint64) bool
	// Owner is the pair's owner at the committed epoch.
	Owner func(slot uint16, db uint64) (string, bool)
	// Holds reports whether node is in the pair's copy set at the committed epoch.
	Holds func(slot uint16, db uint64, node string) bool
	// SlotState is this node's own record state for the pair.
	SlotState func(slot uint16, db uint64) machine.SlotState
	// Deferred reports whether the pair's boot replay has not finished (BUG-0760): a cut taken
	// now would pin a position below records this node acknowledged. Nil means never.
	Deferred func(slot uint16, db uint64) bool
	// RetiredIntact reports that this node's own record of the pair is retired and its range
	// is whole: it left the copy set from ready in this process and its range delete has not
	// started (machine.SlotRecord.RetiredIntact). It reads false after a restart.
	RetiredIntact func(slot uint16, db uint64) bool
}

// SeedSource answers SEED_REGISTER and SEED_ACK for the pairs this node holds.
type SeedSource struct {
	cfg SeedSourceConfig
	opt SeedOptions

	mu     sync.Mutex
	pins   map[[16]byte]*seedPin
	credit *transport.CreditWindow
	closed bool
}

// seedPin is one seed's state at its source.
type seedPin struct {
	// mu serialises the calls of one seed and guards every field below it.
	mu         sync.Mutex
	id         [16]byte
	slot       uint16
	db         uint64
	requester  string
	snapshot   storage.KVSnapshot
	cut        cluster.Position
	epochAtReg uint64
	ownerAtReg bool
	// retired says this node served the pin as a retired holder whose range is whole
	// (BUG-0835): it stands only while the record still reads RetiredIntact.
	retired   bool
	cursor    []byte
	index     uint64
	finalSent bool
	closed    bool
	// touched is the last call of the receiver; it is read and written under SeedSource.mu.
	touched time.Time
}

// NewSeedSource returns a source over cfg.
func NewSeedSource(cfg SeedSourceConfig) *SeedSource {
	opt := cfg.Options.withDefaults()
	return &SeedSource{cfg: cfg, opt: opt, pins: map[[16]byte]*seedPin{}, credit: transport.NewCreditWindow()}
}

// Run drops the pins whose receiver went silent for the hold, until ctx ends.
func (s *SeedSource) Run(ctx context.Context) {
	tick := min(max(s.opt.ResumeHold/4, 10*time.Millisecond), 10*time.Second)
	timer := time.NewTicker(tick)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.sweep()
		}
	}
}

// Close releases every pin and refuses later registrations.
func (s *SeedSource) Close() {
	s.mu.Lock()
	s.closed = true
	gone := make([]*seedPin, 0, len(s.pins))
	for id, pin := range s.pins {
		gone = append(gone, pin)
		delete(s.pins, id)
		s.credit.Forget(id)
	}
	s.mu.Unlock()
	for _, pin := range gone {
		pin.release()
	}
}

// Pins is the number of seeds this source holds now.
func (s *SeedSource) Pins() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pins)
}

// Holds reports whether a pin that is not closed exists for (slot, db) whose registered
// receiver is requester: the one node besides the copy set that may ask this source for
// CATCHUP_REQ, because the closing round of its seed does (seeding completion 2.3).
//
// The slot, database and receiver of a pin are fixed before it is published, so they are
// matched under s.mu; the closed flag is read under the pin's own mu after s.mu is released.
// A pin's mu is held while retire takes s.mu, so holding both here would invert that order.
//
// The pin serves only while this node still stands as a source for the pair: its own record is
// ready, or retired with its range whole for a pin it served as a retired holder (the standing
// it needed to register the pin), and, once the epoch moved, it still holds the pair in the
// role it registered as. A source that reset its own range or lost the copy
// has a tail that moved back, and the pinned closing round would call the receiver current at a
// position the source dropped.
// Cost: one pass over at most incoming_seeds_max pins; nothing is per bucket.
func (s *SeedSource) Holds(slot uint16, db uint64, requester string) bool {
	s.mu.Lock()
	var matching []*seedPin
	for _, pin := range s.pins {
		if pin.slot == slot && pin.db == db && pin.requester == requester {
			matching = append(matching, pin)
		}
	}
	s.mu.Unlock()
	if len(matching) == 0 {
		return false
	}
	ready := s.cfg.SlotState(slot, db) == machine.SlotReady
	for _, pin := range matching {
		pin.mu.Lock()
		live := !pin.closed && (ready || pin.retired) && s.sourceStands(pin)
		pin.mu.Unlock()
		if live {
			return true
		}
	}
	return false
}

// touch counts a CATCHUP_REQ from the pin's receiver as activity: its closing round can outlast
// the hold, and a pin swept under it would send the receiver back to a full re-pull.
func (s *SeedSource) touch(slot uint16, db uint64, requester string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.opt.Now()
	for _, pin := range s.pins {
		if pin.slot == slot && pin.db == db && pin.requester == requester {
			pin.touched = now
		}
	}
}

func (p *seedPin) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		if p.snapshot != nil {
			p.snapshot.Discard()
		}
	}
}

// sweep drops every pin whose receiver has been silent for the hold.
func (s *SeedSource) sweep() {
	now := s.opt.Now()
	s.mu.Lock()
	var gone []*seedPin
	for id, pin := range s.pins {
		if now.Sub(pin.touched) > s.opt.ResumeHold {
			gone = append(gone, pin)
			delete(s.pins, id)
			s.credit.Forget(id)
		}
	}
	s.mu.Unlock()
	for _, pin := range gone {
		pin.release()
	}
}

// Serve answers one inbound seed-channel request. Only SEED_REGISTER and SEED_ACK are
// requests; the other kinds are replies the receiver reads (conventions 6.1).
func (s *SeedSource) Serve(from string, req transport.Message) (*transport.Message, error) {
	switch req.Env.Kind {
	case transport.KindSeedRegister:
		return s.register(from, req)
	case transport.KindSeedAck:
		return s.ack(from, req)
	}
	return nil, fmt.Errorf("%w: the seed channel of this node accepts SEED_REGISTER (0x20) and SEED_ACK (0x26), not kind 0x%02x; the other seed kinds are answers to those", transport.ErrInvalidKind, req.Env.Kind)
}

// payloadOf rebuilds the codec's payload (envelope then body) from a received message.
func payloadOf(msg transport.Message) []byte {
	return append(transport.EncodeEnvelope(msg.Env), msg.Body...)
}

// messageOf splits a codec payload into the envelope and body a ChannelHandler returns.
func messageOf(payload []byte) (*transport.Message, error) {
	env, err := transport.DecodeEnvelope(payload)
	if err != nil {
		return nil, fmt.Errorf("reading back an encoded seed message: %w", err)
	}
	return &transport.Message{Env: env, Body: payload[transport.EnvelopeSize:]}, nil
}

// header is the envelope a reply carries: the source's committed epoch and state sequence
// and the pair of the request.
func (s *SeedSource) header(req transport.Envelope) transport.Envelope {
	return transport.Envelope{Version: transport.EnvelopeVersion, Epoch: s.cfg.Committed(), StateSeq: s.cfg.StateSeq(), Slot: req.Slot, DBID: req.DBID}
}

func (s *SeedSource) refuse(req transport.Envelope, id [16]byte, reason byte, detail string) (*transport.Message, error) {
	payload, err := transport.EncodeSeedRefused(transport.SeedRefused{Header: s.header(req), SeedID: id, Reason: reason, Detail: detail})
	if err != nil {
		return nil, fmt.Errorf("encoding SEED_REFUSED reason %d: %w", reason, err)
	}
	return messageOf(payload)
}

func (s *SeedSource) restart(req transport.Envelope, id [16]byte, reason byte) (*transport.Message, error) {
	payload, err := transport.EncodeSeedRestart(transport.SeedRestart{Header: s.header(req), SeedID: id, Reason: reason})
	if err != nil {
		return nil, fmt.Errorf("encoding SEED_RESTART reason %d: %w", reason, err)
	}
	return messageOf(payload)
}

func (s *SeedSource) accept(req transport.Envelope, pin *seedPin, resume bool) (*transport.Message, error) {
	return messageOf(transport.EncodeSeedAccept(transport.SeedAccept{
		Header: s.header(req), SeedID: pin.id, Cut: seedPosition(pin.cut), WindowBytes: uint32(min(s.opt.WindowBytes, 1<<32-1)), Resume: resume,
	}))
}

func seedPosition(pos cluster.Position) transport.SeedPosition {
	return transport.SeedPosition{Epoch: pos.Epoch, Seq: pos.Seq}
}

// register answers SEED_REGISTER. The order is the cheapest refusal first: the structure of
// the request, the epoch, the database, this node's own record, whether it is the right
// source, then a known pin (a resume) and last the limits that apply to a new pin only.
//
// A pair whose boot replay has not finished is refused before all of them, with an error and
// not a seed answer: a cut taken now pins a position below records this node acknowledged, and
// a SEED_RESTART or SEED_REFUSED would make the puller restart or rotate an attempt that only
// has to wait. The puller reads an error as a call that failed and asks again. The deferred
// set is filled before the node serves and only shrinks afterwards, so no pin exists for a
// deferred pair and none can be made between this check and the cut.
func (s *SeedSource) register(from string, req transport.Message) (*transport.Message, error) {
	reg, err := transport.DecodeSeedRegister(payloadOf(req))
	if err != nil {
		return nil, err
	}
	env, slot, db := req.Env, req.Env.Slot, req.Env.DBID
	if s.cfg.Deferred != nil && s.cfg.Deferred(slot, db) {
		return nil, pairReplayingError(s.cfg.Self, slot, db)
	}
	if reg.Mask&^seedMaskResume != 0 {
		return s.refuse(env, reg.SeedID, seedRefusedMask, fmt.Sprintf(
			"SEED_REGISTER sets mask 0x%02x, but only bit 0 (resume) is defined. Send mask 0 for a new attempt or 1 to resume one", reg.Mask))
	}
	if reg.Mask&seedMaskResume == 0 && len(reg.Cursor) != 0 {
		return s.refuse(env, reg.SeedID, seedRefusedMask, "SEED_REGISTER carries a cursor but not the resume bit. A new attempt starts with an empty cursor; set mask bit 0 to resume from one")
	}
	if len(reg.Cursor) != 0 && !bytes.HasPrefix(reg.Cursor, storage.PairPrefix(slot, db)) {
		return s.refuse(env, reg.SeedID, seedRefusedMask, fmt.Sprintf(
			"the cursor of SEED_REGISTER is not a key of slot %d of database %d. Re-register with the last key this node applied from this seed, or start a new attempt", slot, db))
	}
	if env.Epoch > s.cfg.Committed() {
		// Conventions 12 contract 3: only a later epoch is a mismatch. This node's view is
		// behind the requester's, so it cannot say who owns the pair.
		return s.restart(env, reg.SeedID, seedRestartEpochChanged)
	}
	if !s.cfg.Database(db) {
		return s.refuse(env, reg.SeedID, seedRefusedDropped, fmt.Sprintf(
			"database %d does not exist on %s at epoch %d; it was dropped. Drop the partial range on the requester and stop seeding it", db, s.cfg.Self, s.cfg.Committed()))
	}
	refused, detail, retired := s.sourceRefusal(slot, db, from)
	if refused != 0 {
		s.dropPinOf(reg.SeedID, from)
		return s.refuse(env, reg.SeedID, refused, detail)
	}
	owner, known := s.cfg.Owner(slot, db)
	if !known {
		s.dropPinOf(reg.SeedID, from)
		return s.refuse(env, reg.SeedID, seedRefusedNotCurrent, fmt.Sprintf(
			"slot %d of database %d has no owner at epoch %d, so no node can say which position is current. Run 'nodes' to see which members are down, then retry", slot, db, s.cfg.Committed()))
	}
	if retired && owner == s.cfg.Self {
		// A retired holder serves the owner, which must be another node. A retired record
		// stays retired when its node comes back into the copy set before the gate passes,
		// and the owner comes from the topology alone, so this node can be the owner again;
		// its range then stops where it retired and misses every write acknowledged since.
		s.dropPinOf(reg.SeedID, from)
		return s.refuse(env, reg.SeedID, seedRefusedNotCurrent, fmt.Sprintf(
			"%s owns slot %d of database %d at epoch %d, but its record of the pair is retired: its copy stops where it left the copy set and misses the writes acknowledged since, so it is not a seed source until it is ready again. Seed from a ready copy; 'nodes' lists the members", s.cfg.Self, slot, db, s.cfg.Committed()))
	}
	if owner != s.cfg.Self && owner != from {
		// Seeding 5: normally the owner is the source. The requester follows this once.
		s.dropPinOf(reg.SeedID, from)
		payload, err := transport.EncodeSeedRedirect(transport.SeedRedirect{Header: s.header(env), SeedID: reg.SeedID, Source: owner})
		if err != nil {
			return nil, fmt.Errorf("encoding SEED_REDIRECT to %s: %w", owner, err)
		}
		return messageOf(payload)
	}
	return s.pinFor(from, env, reg, pinRole{ownerSelf: owner == s.cfg.Self, retired: retired})
}

// sourceRefusal applies the seed machine's own rows (machine.SeedSourceAsked) to this
// node's record of the pair. A node outside the pair's copy set holds nothing, whatever
// its record says, with one exception: a retired holder whose range is whole still holds
// the copy it had when it was ready, and may serve it (BUG-0835) to the pair's owner, and only
// when the owner is another node: register refuses (reason 4) when this node is itself the
// owner and sends every other requester to the owner. The machine cannot make that call: the
// record it is asked with carries the state and the flag, not who owns the pair. retired
// reports the exception, so the pin keeps checking that the range is still whole.
func (s *SeedSource) sourceRefusal(slot uint16, db uint64, requester string) (reason byte, detail string, retired bool) {
	state := s.cfg.SlotState(slot, db)
	retired = state == machine.SlotRetired && s.cfg.RetiredIntact(slot, db)
	if !retired && !s.cfg.Holds(slot, db, s.cfg.Self) {
		state = machine.SlotAbsent
	}
	record := machine.SourceStanding(state, retired)
	act := record.Apply(machine.SeedSourceAsked{Requester: requester})
	switch act.SeedRefused {
	case 3:
		return seedRefusedNotHolder, fmt.Sprintf(
			"%s holds no copy of slot %d of database %d at epoch %d. Ask the owner of that slot; 'nodes' lists the members", s.cfg.Self, slot, db, s.cfg.Committed()), false
	case 4:
		return seedRefusedNotCurrent, fmt.Sprintf(
			"%s is not a ready copy of slot %d of database %d (its record is %s). A node that is still seeding, or that retired and has started deleting the slot or restarted since, is never a source. Ask the owner, or wait until %s is ready", s.cfg.Self, slot, db, slotStateName(state), s.cfg.Self), false
	}
	return 0, "", retired
}

// pinRole is the role a node registered a pin in: the pair's owner, or a retired holder whose
// range was whole.
type pinRole struct {
	ownerSelf, retired bool
}

func slotStateName(state machine.SlotState) string {
	switch state {
	case machine.SlotSeeding:
		return "seeding"
	case machine.SlotReady:
		return "ready"
	case machine.SlotRetired:
		return "retired"
	case machine.SlotRetiring:
		return "retiring"
	}
	return "absent"
}

// pinFor answers a registration that this node may serve: a known seed_id within the hold
// gets its pinned cut back with resume=1 (owner decision Q4), anything else gets a new pin
// and a new cut.
func (s *SeedSource) pinFor(from string, env transport.Envelope, reg transport.SeedRegister, role pinRole) (*transport.Message, error) {
	slot, db := env.Slot, env.DBID
	s.sweep()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errStarting
	}
	if pin := s.pins[reg.SeedID]; pin != nil && pin.slot == slot && pin.db == db && pin.requester == from {
		pin.touched = s.opt.Now()
		s.mu.Unlock()
		return s.resume(env, pin, reg)
	}
	if s.pins[reg.SeedID] != nil {
		// Another requester, slot or database holds this seed_id. Overwriting its pin would
		// leave that snapshot open with nothing able to release it.
		s.mu.Unlock()
		return s.restart(env, reg.SeedID, seedRestartPinGone)
	}
	if reg.Mask&seedMaskResume != 0 {
		s.mu.Unlock()
		// The requester holds data cut at a snapshot this node no longer pins: it cannot be
		// continued, and only a new attempt, which resets the range, can replace it.
		return s.restart(env, reg.SeedID, seedRestartPinGone)
	}
	// One pull per (slot, database) from a requester: a registration with a new seed_id for a
	// pair the requester already has a pin for means it abandoned that attempt (it restarted
	// on a fresh cut, SPEC-GAP T3-8). Holding the old snapshot for the whole resume hold would
	// fill incoming_seeds_max with seeds nobody will ask for again.
	var superseded []*seedPin
	for id, held := range s.pins {
		if held.requester == from && held.slot == slot && held.db == db {
			superseded = append(superseded, held)
			delete(s.pins, id)
			s.credit.Forget(id)
		}
	}
	if len(superseded) > 0 {
		s.mu.Unlock()
		for _, held := range superseded {
			held.release()
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, errStarting
		}
	}
	if len(s.pins) >= s.opt.IncomingMax {
		s.mu.Unlock()
		payload := transport.EncodeSeedBusy(transport.SeedBusy{Header: s.header(env), SeedID: reg.SeedID, RetryAfterMs: uint32(s.opt.BusyRetryAfter.Milliseconds())})
		return messageOf(payload)
	}
	if used, err := s.diskUsedPct(); err != nil || used >= s.opt.DiskHighWaterPct {
		s.mu.Unlock()
		detail := fmt.Sprintf("the disk of %s is %d%% full, at or above seed_disk_high_water_pct %d. Free space on %s or raise replication.seed_disk_high_water_pct, then retry", s.cfg.Self, used, s.opt.DiskHighWaterPct, s.cfg.Self)
		if err != nil {
			detail = fmt.Sprintf("%s cannot read its own disk usage (%v), so it will not start a seed. Check the data directory, then retry", s.cfg.Self, err)
		}
		return s.refuse(env, reg.SeedID, seedRefusedDisk, detail)
	}
	pin := &seedPin{id: reg.SeedID, slot: slot, db: db, requester: from, ownerAtReg: role.ownerSelf, retired: role.retired, touched: s.opt.Now()}
	// The pin is in the map, and its slot counted, before the cut is taken; its own lock
	// keeps a call for the same seed_id waiting until the cut exists.
	// Lock order: SeedSource.mu, then a pin's mu, only here, where the pin is new and nobody
	// else can hold its lock. Everywhere else a pin's mu is taken first and s.mu inside it, so
	// this must never become a lookup of a pin that exists.
	pin.mu.Lock()
	s.pins[reg.SeedID] = pin
	s.mu.Unlock()
	// The epoch is read before the cut, so a commit that lands in between shows at the first
	// ack; and the role is read again after it, because the checks in register were made
	// before the cut and the pair may have been dropped since (the cut would then be an
	// empty pair made up on the spot), or, for a retired holder, its delete may have started:
	// the flag drops before the first delete batch, so reading it intact after the snapshot
	// opened proves the snapshot holds the whole range.
	pin.epochAtReg = s.cfg.Committed()
	s.takeCut(pin)
	if !s.holdsRole(pin) {
		pin.mu.Unlock()
		s.dropPin(reg.SeedID)
		return s.restart(env, reg.SeedID, seedRestartEpochChanged)
	}
	s.mu.Lock()
	openErr := s.credit.Open(transport.SeedAccept{SeedID: pin.id, Cut: seedPosition(pin.cut), WindowBytes: uint32(s.opt.WindowBytes)})
	s.mu.Unlock()
	reply, err := s.accept(env, pin, false)
	pin.mu.Unlock()
	if err != nil || openErr != nil {
		s.dropPin(reg.SeedID)
		return nil, errors.Join(err, openErr)
	}
	return reply, nil
}

// takeCut opens the snapshot and reads the cut under the pair's lock. The argument is in
// the comment at the top of this file.
func (s *SeedSource) takeCut(pin *seedPin) {
	s.cfg.Pairs.With(pin.slot, pin.db, func(pair *cluster.Pair) {
		pin.snapshot = s.cfg.KV.NewSnapshot()
		pin.cut = pair.AppliedPos
	})
}

// resume answers a registration of a known seed_id: the pinned cut, resume=1, and the
// stream continues after the last key the requester applied.
func (s *SeedSource) resume(env transport.Envelope, pin *seedPin, reg transport.SeedRegister) (*transport.Message, error) {
	pin.mu.Lock()
	defer pin.mu.Unlock()
	if pin.closed {
		return s.restart(env, pin.id, seedRestartPinGone)
	}
	pin.cursor = append(pin.cursor[:0], reg.Cursor...)
	pin.index, pin.finalSent = 0, false
	s.mu.Lock()
	openErr := s.credit.Open(transport.SeedAccept{SeedID: pin.id, Cut: seedPosition(pin.cut), WindowBytes: uint32(s.opt.WindowBytes)})
	s.mu.Unlock()
	if openErr != nil {
		return nil, fmt.Errorf("reopening the credit of seed %x: %w", pin.id, openErr)
	}
	return s.accept(env, pin, true)
}

// dropPinOf drops the pin of id only when requester registered it, so a refusal to one
// node never ends another node's seed.
func (s *SeedSource) dropPinOf(id [16]byte, requester string) {
	s.mu.Lock()
	pin := s.pins[id]
	if pin != nil && pin.requester != requester {
		pin = nil
	}
	if pin != nil {
		delete(s.pins, id)
		s.credit.Forget(id)
	}
	s.mu.Unlock()
	if pin != nil {
		pin.release()
	}
}

// dropPin removes and releases the pin of id, if there is one.
func (s *SeedSource) dropPin(id [16]byte) {
	s.mu.Lock()
	pin := s.pins[id]
	delete(s.pins, id)
	s.credit.Forget(id)
	s.mu.Unlock()
	if pin != nil {
		pin.release()
	}
}

// retire removes and releases a pin whose lock the caller holds.
func (s *SeedSource) retire(pin *seedPin) {
	s.mu.Lock()
	if s.pins[pin.id] == pin {
		delete(s.pins, pin.id)
		s.credit.Forget(pin.id)
	}
	s.mu.Unlock()
	if !pin.closed {
		pin.closed = true
		pin.snapshot.Discard()
	}
}

func (s *SeedSource) diskUsedPct() (int, error) {
	if s.opt.DiskUsedPct != nil {
		return s.opt.DiskUsedPct()
	}
	return 0, nil
}

// ack answers SEED_ACK: the next chunk within the credit, the end of the seed, or a restart.
func (s *SeedSource) ack(from string, req transport.Message) (*transport.Message, error) {
	ack, err := transport.DecodeSeedAck(payloadOf(req))
	if err != nil {
		return nil, err
	}
	env := req.Env
	s.mu.Lock()
	pin := s.pins[ack.SeedID]
	if pin == nil || pin.requester != from || pin.slot != env.Slot || pin.db != env.DBID {
		s.mu.Unlock()
		return s.restart(env, ack.SeedID, seedRestartPinGone)
	}
	pin.touched = s.opt.Now()
	observeErr := s.credit.Observe(ack)
	s.mu.Unlock()
	if observeErr != nil {
		return nil, observeErr
	}
	pin.mu.Lock()
	defer pin.mu.Unlock()
	if pin.closed {
		return s.restart(env, ack.SeedID, seedRestartPinGone)
	}
	if env.Epoch > s.cfg.Committed() || !s.sourceStands(pin) {
		s.retire(pin)
		return s.restart(env, ack.SeedID, seedRestartEpochChanged)
	}
	if pin.finalSent {
		s.retire(pin)
		return nil, nil
	}
	reply, err := s.chunk(env, pin, int(ack.CreditBytes))
	var tooLarge *seedEntryTooLarge
	if errors.As(err, &tooLarge) {
		// No window the receiver can open fits this entry, so no later ack would either. The
		// codec has no reason for it and SEED_RESTART carries no text; reason 5 is the nearest
		// refusal and its detail names the fix (SPEC-GAP T2-5).
		s.retire(pin)
		return s.refuse(env, ack.SeedID, seedRefusedMask, fmt.Sprintf(
			"an entry of slot %d of database %d is %d bytes, larger than seed_window_bytes %d, so it can never be sent in one window. Raise replication.seed_window_bytes on the receiver and the source above %d, then retry", pin.slot, pin.db, tooLarge.size, s.opt.WindowBytes, tooLarge.size))
	}
	return reply, err
}

// seedEntryTooLarge is chunk's answer for an entry that does not fit a full window.
type seedEntryTooLarge struct{ size int }

func (e *seedEntryTooLarge) Error() string {
	return fmt.Sprintf("an entry of %d bytes does not fit the seed window", e.size)
}

// sourceStands reports whether this node is still a source for the pin's requester: a pin
// served by a retired holder stands only while the range is whole, checked on every call
// because the delete can start without the epoch moving; and after the epoch moved, it is a
// holder, and it is the owner (or, when the requester was the owner, the requester still is).
func (s *SeedSource) sourceStands(pin *seedPin) bool {
	if pin.retired && !s.cfg.RetiredIntact(pin.slot, pin.db) {
		return false
	}
	if s.cfg.Committed() == pin.epochAtReg {
		return true
	}
	return s.holdsRole(pin)
}

// holdsRole reports whether, at the committed epoch now, this node still holds the pair and
// is the owner it registered as (or the requester is the owner, when it registered for one).
// A retired holder is outside the copy set by definition; it holds the pair while its range
// is whole.
func (s *SeedSource) holdsRole(pin *seedPin) bool {
	if !s.cfg.Database(pin.db) {
		return false
	}
	if pin.retired {
		if !s.cfg.RetiredIntact(pin.slot, pin.db) {
			return false
		}
	} else if !s.cfg.Holds(pin.slot, pin.db, s.cfg.Self) {
		return false
	}
	owner, known := s.cfg.Owner(pin.slot, pin.db)
	if !known {
		return false
	}
	if pin.ownerAtReg {
		return owner == s.cfg.Self
	}
	return owner == pin.requester
}

// entryBytes is the size of one chunk entry on the wire: two uvarint lengths, key, value.
func entryBytes(key, value []byte) int {
	return uvarintWidth(uint64(len(key))) + len(key) + uvarintWidth(uint64(len(value))) + len(value)
}

func uvarintWidth(number uint64) int {
	width := 1
	for number >= 0x80 {
		number >>= 7
		width++
	}
	return width
}

// chunk reads the next chunk from the pinned snapshot, after the pin's cursor. It stops at
// the first of seed_chunk_entries, seed_chunk_bytes and the receiver's credit, never splits
// an entry, and sends nothing when even the first entry does not fit the credit
// (SPEC-GAP T2-5). The pair lock is not held: the snapshot is fixed.
func (s *SeedSource) chunk(env transport.Envelope, pin *seedPin, credit int) (*transport.Message, error) {
	limit := min(s.opt.ChunkBytes, credit)
	prefix := storage.PairPrefix(pin.slot, pin.db)
	lower := prefix
	if len(pin.cursor) != 0 {
		lower = append(append(make([]byte, 0, len(pin.cursor)+1), pin.cursor...), 0)
	}
	iter := pin.snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyRangeEnd(prefix)})
	defer iter.Close()
	var entries []transport.SeedEntry
	used := 0
	for iter.Seek(lower); iter.Valid() && len(entries) < s.opt.ChunkEntries; iter.Next() {
		key := iter.Key()
		value, err := iter.Value()
		if err != nil {
			return nil, fmt.Errorf("reading the value of key %x for seed %x: %w", key, pin.id, err)
		}
		size := entryBytes(key, value)
		if len(entries) == 0 && size > credit || len(entries) > 0 && used+size > limit {
			break
		}
		entries = append(entries, transport.SeedEntry{Key: append([]byte(nil), key...), Value: value})
		used += size
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("walking the snapshot of seed %x: %w", pin.id, err)
	}
	if len(entries) == 0 && iter.Valid() {
		value, err := iter.Value()
		if err != nil {
			return nil, fmt.Errorf("reading the value of key %x for seed %x: %w", iter.Key(), pin.id, err)
		}
		if size := entryBytes(iter.Key(), value); size > s.opt.WindowBytes {
			return nil, &seedEntryTooLarge{size: size}
		}
		// The entry fits a full window and not the credit the receiver has free right now.
		return nil, nil
	}
	final := !iter.Valid()
	payload, err := transport.EncodeSeedChunk(transport.SeedChunk{Header: s.header(env), SeedID: pin.id, Index: pin.index, Entries: entries, Final: final})
	if err != nil {
		return nil, fmt.Errorf("encoding chunk %d of seed %x: %w", pin.index, pin.id, err)
	}
	reply, err := messageOf(payload)
	if err != nil {
		return nil, err
	}
	if len(entries) != 0 {
		pin.cursor = append(pin.cursor[:0], entries[len(entries)-1].Key...)
	}
	pin.index++
	pin.finalSent = final
	return reply, nil
}
