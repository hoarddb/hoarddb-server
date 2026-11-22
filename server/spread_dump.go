package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The dump of a spread collection fans in at its router (spread MVP note,
// section 3, B3): one dump stream of the collection, under its name, whose
// restore path is a whole bucket's. The router holds a session per collection
// being dumped and relays its parts one at a time, in part order: each part's
// own dump session runs at the part's owner (DUMP_START, CHUNK and DONE
// naming the part, sent through the router's read door), and the router keeps
// the three digests (documents, stores, dead) over what it relayed, which the
// client checks its files against at DUMP_DONE. The router's session claims
// no dump slot, so a part on the router's own node can. Each part is a
// snapshot; the whole is not (spec section 7.4).
//
// Each node has one dump slot (claimDump), and a spread collection's parts
// span the members, so a second concurrent dump meets ErrDumpAlreadyRunning
// at some part, possibly partway through, under the first collection's name.
//
// Gate: every dump frame is one routed read and takes the router gate shared
// once, holding no fence; part frames take no gate.
//
// Cost (rule 11): one session (a few hundred bytes and one goroutine that
// watches the client's connection) per collection being dumped, released at
// DUMP_DONE, on a refused frame or when the client's connection ends.
// Nothing per template or part otherwise.

// spreadDumpSession is a router's relay of one spread collection's dump.
type spreadDumpSession struct {
	// mu serialises the client's frames of the session, and its abort.
	mu sync.Mutex

	database, bucket string
	startedBy        string
	startedAt        time.Time
	request          DumpStartRequest
	// parts are the collection's created parts at the start, in order; next
	// is the one being relayed, and open says its owner's session is started.
	parts []int
	next  int
	open  bool
	// ended says the session was released: by DUMP_DONE, a refusal or the
	// client's connection. lastActive is when a frame of it last ran.
	ended      bool
	lastActive time.Time

	// digests and partDigests hash the relayed bytes per section, of the
	// whole dump and of the part being relayed.
	digests, partDigests map[string]hash.Hash
	// sections and partSections count the relayed bytes per section.
	sections, partSections map[string]int64
	documents, elements    int64
	dead, bytes            int64

	watchCancel context.CancelFunc
}

// spreadDumpMap is the router's sessions, one per collection being dumped.
type spreadDumpMap struct {
	mu       sync.Mutex
	sessions map[placementKey]*spreadDumpSession
}

// claim registers session for key, or returns the one running.
func (m *spreadDumpMap) claim(key placementKey, session *spreadDumpSession) *spreadDumpSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	if running, ok := m.sessions[key]; ok {
		return running
	}
	if m.sessions == nil {
		m.sessions = make(map[placementKey]*spreadDumpSession)
	}
	m.sessions[key] = session
	return nil
}

// get is key's session, nil when none runs.
func (m *spreadDumpMap) get(key placementKey) *spreadDumpSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[key]
}

// release forgets session, if it is still key's.
func (m *spreadDumpMap) release(key placementKey, session *spreadDumpSession) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sessions[key] == session {
		delete(m.sessions, key)
	}
}

// running is how many collections are being dumped through this router.
func (m *spreadDumpMap) running() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// sectionDigests is one empty digest per dump section.
func sectionDigests() map[string]hash.Hash {
	return map[string]hash.Hash{"": sha256.New(), dumpSectionStores: sha256.New(), dumpSectionDead: sha256.New()}
}

// sectionDigest is a section's digest as a dump's done reports it: the
// documents' always, the stores' and dead's only when that section held
// anything, as a whole bucket's dump reports them.
func sectionDigest(digests map[string]hash.Hash, sections map[string]int64, section string) string {
	if section != "" && sections[section] == 0 {
		return ""
	}
	return "sha256:" + hex.EncodeToString(digests[section].Sum(nil))
}

// relayedRefusal is a part owner's refusal of a dump frame, relayed to the
// client as the owner gave it (its code and text), the part shown as its
// collection.
type relayedRefusal struct {
	payload []byte
}

func (e *relayedRefusal) Error() string { return answerMessage(e.payload) }

// routeDump answers one of the client's dump frames for the collection.
func (r *spreadRouter) routeDump(ctx context.Context, frame *transport.Frame) *transport.Frame {
	if err := requireRootIdentity(ctx, r.s); err != nil {
		return errorFrame(frame, err)
	}
	release, gateErr := r.holdGate(ctx, false, frame.Opcode)
	if gateErr != nil {
		return errorFrame(frame, gateErr)
	}
	defer release()
	var (
		answer any
		err    error
	)
	switch frame.Opcode {
	case transport.OpDumpStart:
		answer, err = r.startDump(ctx, frame)
	case transport.OpDumpChunk:
		answer, err = r.dumpChunk(ctx, frame)
	default:
		answer, err = r.dumpDone(ctx, frame)
	}
	var relayed *relayedRefusal
	if errors.As(err, &relayed) {
		return transport.NewFrameWithID(transport.OpError, frame.RequestID, relayed.payload)
	}
	if err != nil {
		return errorFrame(frame, err)
	}
	reply, err := respond(frame, frame.Opcode, answer)
	if err != nil {
		return errorFrame(frame, err)
	}
	return reply
}

// startDump starts the collection's dump: the template's checks and
// manifest, the created parts as the born set reads now, and a session.
func (r *spreadRouter) startDump(ctx context.Context, frame *transport.Frame) (*DumpStartResponse, error) {
	var req DumpStartRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nil, fmt.Errorf("unmarshal dump start request: %w", err)
	}
	born, err := r.s.readBornSet(r.t.database, r.t.name)
	if err != nil {
		return nil, err
	}
	if born.dropping() {
		return nil, droppingError(r.t.database, r.t.name)
	}
	b, snap, err := r.s.resolveForRead(r.t.database, r.t.name)
	if err != nil {
		return nil, err
	}
	plan, err := r.s.planDump(b, snap, req)
	snap.Discard()
	if err != nil {
		return nil, err
	}
	session := &spreadDumpSession{database: r.t.database, bucket: r.t.name, startedBy: sessionUsername(ctx), startedAt: time.Now(),
		request: req, digests: sectionDigests(), sections: map[string]int64{}}
	for p := 0; p < r.t.parts; p++ {
		if born.state(p) == partCreated {
			session.parts = append(session.parts, p)
		}
	}
	session.lastActive = session.startedAt
	running := r.s.spreadDumps.claim(r.t.key, session)
	if running != nil && r.takeOverIdleDump(running) {
		running = r.s.spreadDumps.claim(r.t.key, session)
	}
	if running != nil {
		return nil, fmt.Errorf("%w: dump of %s/%s is already running on %s (started by %q at %s); wait for it to finish, or dump a different server",
			ErrDumpAlreadyRunning, running.database, running.bucket, dumpNodeLabel(r.s), running.startedBy, running.startedAt.Format(time.RFC3339))
	}
	r.watchDump(ctx, session)
	r.s.audit.Log(AuditEntry{Event: EventDumpStart, User: session.startedBy, Target: r.t.database + "/" + r.t.name, Result: "ok"})
	return &DumpStartResponse{Success: true, Message: plan.message, Manifest: plan.manifest}, nil
}

// takeOverIdleDump ends running when no frame of it has run for the dump pin
// ceiling, and reports whether it did. A client whose dump was forwarded here
// by another node can go away without this node's seeing its connection end
// (the forward's connection is the internode one), and the collection's next
// dump must not then be refused for ever.
func (r *spreadRouter) takeOverIdleDump(running *spreadDumpSession) bool {
	running.mu.Lock()
	defer running.mu.Unlock()
	if !running.ended && time.Since(running.lastActive) <= r.s.dumpPinCeiling() {
		return false
	}
	r.s.logger.Warn("ending a dump whose client sent nothing within the dump pin ceiling",
		"database", running.database, "bucket", running.bucket, "ceiling", r.s.dumpPinCeiling())
	r.endDump(running)
	return true
}

// watchDump ends the session when the client's connection ends, as a whole
// bucket's dump is released (watchDumpConnection): the part being relayed is
// released at its owner.
func (r *spreadRouter) watchDump(ctx context.Context, session *spreadDumpSession) {
	watchCtx, cancel := context.WithCancel(context.Background())
	session.watchCancel = cancel
	go func() {
		select {
		case <-ctx.Done():
			session.mu.Lock()
			defer session.mu.Unlock()
			if !session.ended {
				r.s.logger.Warn("dump aborted: connection closed", "database", session.database, "bucket", session.bucket)
				r.endDump(session)
			}
		case <-watchCtx.Done():
		}
	}()
}

// activeDump is the collection's running session, or the refusal a whole
// bucket gives when none runs.
func (r *spreadRouter) activeDump() (*spreadDumpSession, error) {
	session := r.s.spreadDumps.get(r.t.key)
	if session == nil {
		return nil, fmt.Errorf("%w: no active dump for %s/%s — call dump start first, or it already finished",
			ErrDumpNoActiveSession, r.t.database, r.t.name)
	}
	return session, nil
}

// endDump releases session: the part being relayed is released at its
// owner, and the session is forgotten. The caller holds session.mu.
func (r *spreadRouter) endDump(session *spreadDumpSession) {
	if session.ended {
		return
	}
	session.ended = true
	if session.open {
		// Best effort, on a context of its own: the client's may be gone.
		ctx, cancel := context.WithTimeout(context.Background(), clusterListingTimeout)
		defer cancel()
		if _, err := r.partDumpFrame(ctx, transport.OpDumpDone, session.parts[session.next], DumpDoneRequest{Database: r.t.database}); err != nil {
			r.s.logger.Warn("releasing a part's dump", "database", r.t.database, "bucket", r.t.name, "error", err)
		}
		session.open = false
	}
	if session.watchCancel != nil {
		session.watchCancel()
	}
	r.s.spreadDumps.release(r.t.key, session)
}

// partDumpFrame sends one dump frame naming part p to its owner and returns
// its answer's payload; a refusal comes back as an error, a part that is gone
// as the lost-part refusal (B9), and no part name in either.
func (r *spreadRouter) partDumpFrame(ctx context.Context, op transport.Opcode, p int, request any) ([]byte, error) {
	part := partName(r.t.name, p)
	payload, err := bson.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encoding the %s of a part of bucket %q: %w", op, r.t.name, err)
	}
	payload, err = stampPayload(payload, bson.D{{Key: "bucket", Value: part}})
	if err != nil {
		return nil, fmt.Errorf("naming a part of bucket %q in its dump: %w", r.t.name, err)
	}
	answer, err := r.door.readPart(ctx, transport.NewFrame(op, payload), r.t.database, part)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			return nil, r.door.lostPart(r.t.database, r.t.name, part, false)
		}
		return nil, r.displayed(err, part)
	}
	if hook := answer.TakeWrittenHook(); hook != nil {
		hook()
	}
	if answerNamesNoBucket(int32(answer.Opcode), answer.Payload) {
		return nil, r.door.lostPart(r.t.database, r.t.name, part, false)
	}
	if answer.Opcode == transport.OpError {
		return nil, &relayedRefusal{payload: displayedAnswer(answer.Payload, part, r.t.name)}
	}
	return answer.Payload, nil
}

// dumpChunk relays the next chunk of the collection: the part being relayed,
// started at its owner when it is not yet, and on its end the next part. A
// refusal ends the session, as a whole bucket's failed chunk does.
func (r *spreadRouter) dumpChunk(ctx context.Context, _ *transport.Frame) (*DumpChunkResponse, error) {
	session, err := r.activeDump()
	if err != nil {
		return nil, err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.ended {
		return nil, fmt.Errorf("%w: the dump of %s/%s ended when its client's connection closed; start it again",
			ErrDumpNoActiveSession, r.t.database, r.t.name)
	}
	session.lastActive = time.Now()
	for {
		if session.next >= len(session.parts) {
			return &DumpChunkResponse{Success: true, EOF: true}, nil
		}
		p := session.parts[session.next]
		if !session.open {
			start := session.request
			start.Database = r.t.database
			if _, err := r.partDumpFrame(ctx, transport.OpDumpStart, p, start); err != nil {
				r.endDump(session)
				return nil, err
			}
			session.open, session.partDigests, session.partSections = true, sectionDigests(), map[string]int64{}
		}
		payload, err := r.partDumpFrame(ctx, transport.OpDumpChunk, p, DumpChunkRequest{Database: r.t.database})
		if err != nil {
			r.endDump(session)
			return nil, err
		}
		var chunk DumpChunkResponse
		if err := bson.Unmarshal(payload, &chunk); err != nil {
			r.endDump(session)
			return nil, fmt.Errorf("decoding a part's dump chunk of bucket %q: %w", r.t.name, err)
		}
		session.record(chunk)
		if !chunk.EOF {
			return &DumpChunkResponse{Success: true, Data: chunk.Data, Documents: chunk.Documents, Section: chunk.Section}, nil
		}
		// The part is relayed: its owner checks what the router received
		// against what it sent, and releases its slot.
		if _, err := r.partDumpFrame(ctx, transport.OpDumpDone, p, DumpDoneRequest{Database: r.t.database,
			Digest:       sectionDigest(session.partDigests, session.partSections, ""),
			StoresDigest: sectionDigest(session.partDigests, session.partSections, dumpSectionStores),
			DeadDigest:   sectionDigest(session.partDigests, session.partSections, dumpSectionDead)}); err != nil {
			session.open = false
			r.endDump(session)
			return nil, err
		}
		session.open = false
		session.next++
		last := session.next == len(session.parts)
		if len(chunk.Data) > 0 || last {
			return &DumpChunkResponse{Success: true, Data: chunk.Data, Documents: chunk.Documents, Section: chunk.Section, EOF: last}, nil
		}
	}
}

// record adds a relayed chunk to the session's digests and counts.
func (session *spreadDumpSession) record(chunk DumpChunkResponse) {
	if len(chunk.Data) == 0 {
		return
	}
	session.digests[chunk.Section].Write(chunk.Data)
	session.partDigests[chunk.Section].Write(chunk.Data)
	session.sections[chunk.Section] += int64(len(chunk.Data))
	session.partSections[chunk.Section] += int64(len(chunk.Data))
	session.bytes += int64(len(chunk.Data))
	switch chunk.Section {
	case dumpSectionStores:
		session.elements += int64(chunk.Documents)
	case dumpSectionDead:
		session.dead += int64(chunk.Documents)
	default:
		session.documents += int64(chunk.Documents)
	}
}

// dumpDone ends the collection's dump: the client's digests are checked
// against what the router relayed, as a whole bucket's are against what its
// walk sent, and the session is released whatever the check says.
func (r *spreadRouter) dumpDone(ctx context.Context, frame *transport.Frame) (*DumpDoneResponse, error) {
	var req DumpDoneRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nil, fmt.Errorf("unmarshal dump done request: %w", err)
	}
	session, err := r.activeDump()
	if err != nil {
		return nil, err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	r.endDump(session)
	target := r.t.database + "/" + r.t.name
	digest := sectionDigest(session.digests, session.sections, "")
	storesDigest := sectionDigest(session.digests, session.sections, dumpSectionStores)
	deadDigest := sectionDigest(session.digests, session.sections, dumpSectionDead)
	for _, check := range []struct{ client, server, file string }{
		{req.Digest, digest, ".bson file"},
		{req.StoresDigest, storesDigest, ".stores.bson file"},
		{req.DeadDigest, deadDigest, ".dead.bson file"},
	} {
		if check.client != "" && check.client != check.server {
			r.s.audit.Log(AuditEntry{Event: EventDumpDone, User: session.startedBy, Target: target,
				Result: fmt.Sprintf("digest mismatch: client=%s server=%s", check.client, check.server)})
			return nil, fmt.Errorf("%w: bucket %s: client reported %s, server streamed %s — the %s may be corrupt or truncated",
				ErrDumpDigestMismatch, target, check.client, check.server, check.file)
		}
	}
	r.s.audit.Log(AuditEntry{Event: EventDumpDone, User: session.startedBy, Target: target, Result: "ok"})
	return &DumpDoneResponse{Success: true, Bytes: session.bytes, Documents: session.documents, Digest: digest,
		Elements: session.elements, StoresDigest: storesDigest, Dead: session.dead, DeadDigest: deadDigest}, nil
}
