package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Node-to-node wire types. These ride on the already-specified cluster
// opcodes GOSSIP (0x90), REPL_PUSH (0x91) and REPL_ACK (0x92); no new opcodes
// were needed to make replication real.
//
// ReplEntry carries a client opcode together with the original request
// payload. That is deliberate: a replica applies an entry through exactly the
// same dispatch path a local client request takes, so there is one
// implementation of every mutation rather than two that can drift apart.
type ReplEntry struct {
	// Seq is the replication sequence assigned by the node that owns the
	// bucket. It is what acknowledgements are measured against and is only
	// comparable within one source's stream.
	Seq uint64 `bson:"seq"`
	// Op is the client opcode that produced this entry. It is carried as
	// int32 because the BSON encoder does not encode uint8 directly.
	Op int32 `bson:"op"`
	// Bucket is the bucket's name. Its placement key is (database, Bucket),
	// the database decoded from Payload (entryPlacement): the entry
	// carries no database column of its own.
	Bucket string `bson:"bucket,omitempty"`
	// Key is best-effort WAL key metadata.
	Key []byte `bson:"key,omitempty"`
	// Payload is the original request body, replayed verbatim.
	Payload []byte `bson:"payload,omitempty"`
	// OwnerOutcome is what the owner's own apply of the entry did — unknown,
	// applied, or refused by a unique index (ownerOutcome*) — so a replica
	// that disagrees can say so (GAP-14 build doc S-5). The owner applies
	// before it ships, so it knows. It is not in the WAL: a replica compares
	// it once, on its live apply.
	OwnerOutcome int32 `bson:"owner_outcome,omitempty"`
	// Term is the term of the bucket's copy on the node that ships the entry
	// (replication spec §20.8.3): the epoch its owner opened it at. It is
	// absent for a bucket that predates this build, whose entries are the
	// bytes they were. Nothing reads it until the hand-off of dispatch 10.
	Term uint64 `bson:"term,omitempty"`
	// TypeLimitDeferred says the entering node could not resolve the bucket's
	// type when it measured a client's OpPut, so the owner must apply the
	// bucket's real size limit when it applies the entry (BUG-0582). Only a
	// client put at its door sets it; read repair, restore, a replica and a
	// replay never do, so the owner measures none of them. Optional: a frame
	// without it means not deferred. It is not in the WAL: appendWAL stores
	// only bucket, op, key and payload.
	TypeLimitDeferred bool `bson:"type_limit_deferred,omitempty"`
	// FilterDeferred says the entering node could not tell whether the bucket
	// is spread when it met a client's filtered UPDATE, so the owner judges
	// it when it applies the entry (build step 12c). Only a client update at
	// its door sets it; a replica and a replay never do. Optional, and not in
	// the WAL.
	FilterDeferred bool `bson:"filter_deferred,omitempty"`
}

// ReplResponse is the framed response produced by applying one entry. Carrying
// the opcode as well as the payload means the coordinator never needs a
// hand-maintained table mapping request opcodes to response opcodes: the node
// that applied the entry already produced the correct response frame.
type ReplResponse struct {
	Opcode  int32  `bson:"opcode"`
	Payload []byte `bson:"payload,omitempty"`

	// onWritten is the written hook of the frame the local handler answered
	// with — a forwarded query's execution slot — carried to the REPL_ACK
	// frame that takes this response to the wire. BSON never encodes an
	// unexported field.
	onWritten func()
}

// ReplPushRequest asks a peer to apply entries.
//
// The same frame serves both hops of a write:
//   - coordinator -> owner: ReturnResponse is true, so the owner's response is
//     relayed to the client, and the owner takes responsibility for placing
//     the entry on the remaining replicas.
//   - owner -> replica: ReturnResponse is false; the replica applies the entry
//     and the acknowledgement is the AppliedThrough sequence.
type ReplPushRequest struct {
	Token    string `bson:"token"`
	FromNode string `bson:"from_node"`
	// Entries are ordered; a peer applies them in order.
	Entries []ReplEntry `bson:"entries"`
	// WriteConcern is the coordinating node's durability level, honoured by
	// the owner when it places replicated entries.
	WriteConcern int `bson:"write_concern,omitempty"`
	// ReturnResponse asks the receiver to include per-entry responses.
	ReturnResponse bool `bson:"return_response,omitempty"`
	// Epoch is the sender's current membership epoch. A push that changes
	// state is refused when it is not the receiver's (spec §19.3.7).
	Epoch uint64 `bson:"epoch,omitempty"`
	// Store is the sender's own word on whether its store is encrypted at rest
	// (Peer.Store). A sealed receiver reads it on a forwarded request, whose
	// answer goes back to the sender (sealed_peers.go). Additive: a sender from
	// before the field says nothing.
	Store storeClaim `bson:"store,omitempty"`
	// Stream is present on every push of the sender's own stream — the
	// handshake and every frame after it — and absent on a forwarded client
	// request (spec §19.3.3).
	Stream *ReplStream `bson:"stream,omitempty"`
	// Handoff makes the push a question (step 9.6): what are this node's
	// copies of the buckets it names. It carries no entries and changes
	// nothing.
	Handoff *ReplHandoff `bson:"handoff,omitempty"`
	// Catalog makes the push a question about a database (catalog_question.go): it carries nothing else and changes nothing. Internode only.
	Catalog *ReplCatalogAsk `bson:"catalog,omitempty"`
	// ReadWaitMs is how long the receiver, the bucket's owner, may hold a
	// forwarded read for its take-over (on-demand pull spec §8): what is left
	// of the read's one bound. Zero is the whole bound. Internode only.
	ReadWaitMs int64 `bson:"read_wait_ms,omitempty"`
	// ServeFromCopy marks a read forwarded to the bucket's final previous owner
	// (spec §6.2): the receiver answers it from its own open copy, whatever its
	// ring says, or refuses it with retry. It never sends it on, waits or
	// answers "not found". Internode only.
	ServeFromCopy bool `bson:"serve_from_copy,omitempty"`
	// Rerouted marks a read an owner sent on once because ownership moved
	// while it waited (spec §8): it is never rerouted again, and a receiver
	// that does not own its bucket answers it retry. Internode only.
	Rerouted bool `bson:"rerouted,omitempty"`
	// Batch makes a forwarded request one owner's sub-batch of a client's
	// BATCH (batch-request spec §5.2 step 6): its entries are independent
	// items, run concurrently with their bucket's chain rule, and every one is
	// answered in BatchAnswers. It requires ReturnResponse and no Stream,
	// Catalog or Handoff. Sent only to a member that advertises the batch
	// feature; an older build decodes it as an unknown member and ignores it.
	Batch bool `bson:"batch,omitempty"`
}

// ReplStream says which life of the sender's log a stream push belongs to.
// A receiver applies an entry only under the incarnation it has recorded for
// the sender, because a position is a fact about one life of one log.
type ReplStream struct {
	// Incarnation is the sender's current incarnation
	// (storage.WAL.Incarnation).
	Incarnation []byte `bson:"incarnation,omitempty"`
	// Open makes the push a handshake: it carries no entries, and the
	// receiver answers with what it has recorded for the sender.
	Open bool `bson:"open,omitempty"`
	// Rebase, with Open, makes the receiver clamp its position for the
	// sender and record the sender's current incarnation.
	Rebase *ReplRebase `bson:"rebase,omitempty"`
	// AdvanceTo, on a push with no entries, says no entry at or below it is
	// owed to the receiver (spec §19.3.9, the advance notice).
	AdvanceTo uint64 `bson:"advance_to,omitempty"`
}

// advanceTo is the advance notice a push carries, or 0: a forwarded request
// carries no stream.
func (s *ReplStream) advanceTo() uint64 {
	if s == nil {
		return 0
	}
	return s.AdvanceTo
}

// ReplRebase is how a sender moves a receiver's record onto its current
// incarnation (spec §19.3.3 step 3): the receiver checks that what it has
// recorded is still From, then sets its position to
// max(min(position, ValidThrough), StartAt) and lowers every snapshot position
// above ValidThrough to it. It records the incarnation the push names and
// Log, the sender's log id (storage.WAL.LogID), which it reports at every
// later handshake: that is how a sender tells a log it re-created from one it
// restored (spec §20.4).
type ReplRebase struct {
	From         []byte `bson:"from,omitempty"`
	ValidThrough uint64 `bson:"valid_through,omitempty"`
	StartAt      uint64 `bson:"start_at,omitempty"`
	Log          []byte `bson:"log,omitempty"`
}

// ReplPosition is what a receiver has recorded for a sender: the incarnation
// and the log id it learned its position under, and the position. Known is
// false only when it holds neither a position nor a snapshot position.
type ReplPosition struct {
	Known          bool   `bson:"known,omitempty"`
	Incarnation    []byte `bson:"incarnation,omitempty"`
	Log            []byte `bson:"log,omitempty"`
	AppliedThrough uint64 `bson:"applied_through,omitempty"`
	// HighWater is the highest sequence of the sender's log this node holds
	// anything at: its position or a snapshot position, whichever incarnation
	// it was learned under. A sender that started gives no write a sequence
	// at or below it (BUG-0481, start_hold.go). Absent from a build before it,
	// which the sender reads as AppliedThrough.
	HighWater uint64 `bson:"high_water,omitempty"`
}

// ReplPushResponse reports what a peer managed to apply. AppliedThrough is the
// highest sequence from the sender's stream that this node has durably applied;
// it is the number the sender's write-concern accounting counts, so it is only
// advanced after both the WAL append and the local apply succeed.
type ReplPushResponse struct {
	Success        bool           `bson:"success"`
	AppliedThrough uint64         `bson:"applied_through"`
	Responses      []ReplResponse `bson:"responses,omitempty"`
	Error          string         `bson:"error,omitempty"`
	// DroppedGenerations are the receiver's dropped incarnations of each
	// database an entry of this frame was passed over or refused for because
	// of them, so the sender learns the drop it missed (G-11 (a)).
	DroppedGenerations []DatabaseTombstones `bson:"dropped_generations,omitempty"`
	// Position answers a handshake: what this node has recorded for the
	// sender, after any rebase the handshake asked for.
	Position *ReplPosition `bson:"position,omitempty"`
	// Refusal names why a stream push was refused, so the sender can tell a
	// refusal it must open the stream again for from one it waits out.
	Refusal string `bson:"refusal,omitempty"`
	// Epoch is the receiver's membership epoch, set on an "epoch" refusal
	// (spec §19.3.7).
	Epoch uint64 `bson:"epoch,omitempty"`
	// WriteOutcome is the owner's word, on a forwarded write it refused or
	// could not confirm, on whether the write was stored (spec §19.3.11):
	// not_applied, applied or unknown. CopiesConfirmed and ReplicationFactor
	// come with applied. A coordinator answers its client with all three, so
	// the owner's answer survives the forward.
	WriteOutcome      string `bson:"write_outcome,omitempty"`
	CopiesConfirmed   int    `bson:"copies_confirmed,omitempty"`
	ReplicationFactor int    `bson:"replication_factor,omitempty"`
	// NamedAllAt is a removed node's notice that it has named every bucket it
	// holds (ReplHandoff.NamedAllAt), carried on its answer to a member's
	// probe, so a member that forgot it in a restart is told again
	// (BUG-0702). Internode only.
	NamedAllAt uint64 `bson:"named_all_at,omitempty"`
	// Copies answers a question (step 9.6), one per bucket asked, in order.
	Copies []ReplCopy `bson:"copies,omitempty"`
	// Catalog answers a catalog question. Internode only.
	Catalog *ReplCatalogAnswer `bson:"catalog,omitempty"`
	// BatchAnswers answers a Batch push: one per entry, in entry order, each
	// carrying what one forwarded entry's answer carries (Success, Error,
	// Refusal, WriteOutcome, CopiesConfirmed, ReplicationFactor, Responses of
	// length 0 or 1). Internode only.
	BatchAnswers []ReplPushResponse `bson:"batch_answers,omitempty"`
	// BatchNotRun says a Batch push was refused before any of its entries
	// ran: its token, its sender, a membership mismatch, an entry's depth, or
	// a malformed combination. Only it, or an epoch or member-mismatch
	// refusal, lets the coordinator answer the sub-batch not_applied; an
	// older build sets none of this (§5.3). Internode only.
	BatchNotRun bool `bson:"batch_not_run,omitempty"`
}

// DatabaseTombstones names the dropped incarnations of one database.
type DatabaseTombstones struct {
	Database    string  `bson:"database"`
	Generations []int64 `bson:"generations"`
}

// GossipRequest announces a node and carries its view of the membership and
// the epoch that view belongs to. Membership used to be static (every node
// configured with the same member set) and gossip only verified agreement;
// with membership epochs it is how a node behind a shape change learns the
// newer member set and resyncs instead of staying poisoned. Nodes is the
// addressed member list and supersedes the legacy Members id list.
type GossipRequest struct {
	Token   string   `bson:"token"`
	NodeID  string   `bson:"node_id"`
	Addr    string   `bson:"addr"`
	Epoch   uint64   `bson:"epoch"`
	Nodes   []Peer   `bson:"nodes,omitempty"`
	Members []string `bson:"members,omitempty"` // legacy ids; superseded by Nodes
	// Features names what this build can honour, so a peer can refuse an
	// operation the sender could not carry out rather than carrying it out
	// differently. Optional and additive on the wire: a peer that predates the
	// field omits it, and an omission from a member that gossiped is the signal.
	Features []string `bson:"features,omitempty"`
	// Instance is the sender's instance nonce, drawn at process start: two
	// processes under one node id send two (BUG-0600, node_instance.go).
	// Optional and additive; the instance_nonce feature tells an omission
	// from an older build.
	Instance []byte `bson:"instance,omitempty"`
	// AppliedMemtableMB is the memtable size the sender's store runs at, and
	// MemoryLimitBytes the memory limit it read at start (0 when none). Both
	// optional and additive; the memtable_mb feature tells an omission from a
	// zero. Every member must run the same size, because it sets the largest
	// batch a member can apply in one transaction (memtable-host-scaled-design.md
	// §3). GAP-38 §9.3 takes this field over under the same name.
	AppliedMemtableMB int   `bson:"applied_memtable_mb,omitempty"`
	MemoryLimitBytes  int64 `bson:"memory_limit_bytes,omitempty"`
	// Bootstrapped says the sender's member list comes from its seed settings
	// and has not been confirmed: its agreement confirms nobody else's view
	// (join-freeze spec §5.4 and §16, F-A).
	Bootstrapped bool `bson:"bootstrapped"`
	// Settled is the sender's own Settled epoch, SettledOf the best it has for
	// every other member, and HandOff how far its hand-off of the last
	// membership change has got (join-freeze spec §3.4, §8.5).
	Settled   uint64           `bson:"settled,omitempty"`
	SettledOf []MemberSettled  `bson:"settled_of,omitempty"`
	HandOff   *HandOffProgress `bson:"handoff,omitempty"`
}

// GossipResponse returns the receiving node's view of the membership.
type GossipResponse struct {
	Success bool     `bson:"success"`
	Message string   `bson:"message,omitempty"`
	Epoch   uint64   `bson:"epoch"`
	Nodes   []Peer   `bson:"nodes,omitempty"`
	Members []string `bson:"members,omitempty"`
	Error   string   `bson:"error,omitempty"`
	// Self identifies the responder. A target's member list can contain an
	// address-identity entry for itself (a node booted from bare seed
	// addresses) alongside its configured NODE_ID entry, both on the same
	// Addr — so the cluster-add identity probe must not guess between them.
	// Optional and additive: older peers omit it and the probe falls back
	// to matching by address.
	Self Peer `bson:"self,omitempty"`
	// Features is the responder's own feature list; see GossipRequest.
	Features []string `bson:"features,omitempty"`
	// Instance is the responder's instance nonce; see GossipRequest.
	Instance []byte `bson:"instance,omitempty"`
	// DuplicateID says the responder sees two live processes under the
	// asker's node id, so each of them learns it and refuses user changes
	// (BUG-0600). Optional and additive; false from an older build.
	DuplicateID bool `bson:"duplicate_id,omitempty"`
	// AppliedMemtableMB and MemoryLimitBytes are the responder's; see
	// GossipRequest.
	AppliedMemtableMB int   `bson:"applied_memtable_mb,omitempty"`
	MemoryLimitBytes  int64 `bson:"memory_limit_bytes,omitempty"`
	// History is the responder's membership record of each epoch above the
	// asker's, at most gossipHistoryLimit, so an asker that missed several
	// changes adopts each in turn (step 9.4).
	History []MembershipRecord `bson:"history,omitempty"`
	// RemovedAt is the epoch at which the responder recorded its own removal
	// from the cluster, 0 when it has none. A join asks it of a newcomer: a node
	// that keeps that record refuses every write, and is added back only once
	// its data directory is emptied (removed_node.go).
	RemovedAt uint64 `bson:"removed_at,omitempty"`
	// Bootstrapped is the responder's mark; see GossipRequest.
	Bootstrapped bool `bson:"bootstrapped"`
	// Settled, SettledOf and HandOff are the responder's; see GossipRequest.
	Settled   uint64           `bson:"settled,omitempty"`
	SettledOf []MemberSettled  `bson:"settled_of,omitempty"`
	HandOff   *HandOffProgress `bson:"handoff,omitempty"`
}

// memberIDs returns the member node IDs in deterministic order.
func (c *Cluster) memberIDs() []string {
	members := c.Members()
	ids := make([]string, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.ID)
	}
	return ids
}

// recordNode records a peer in the server's known-node table so that LIST
// NODES reports the cluster as it actually is.
func (s *Server) recordNode(nodeID, addr string) {
	if nodeID == "" {
		return
	}
	s.knownNodesMu.Lock()
	defer s.knownNodesMu.Unlock()
	if existing, ok := s.knownNodes[nodeID]; ok {
		if addr != "" {
			existing.Addr = addr
		}
		existing.Status = "active"
		return
	}
	s.knownNodes[nodeID] = &NodeInfo{
		NodeID:   nodeID,
		Addr:     addr,
		JoinedAt: time.Now(),
		Status:   "active",
	}
}

// recordObservedFingerprint stores the certificate fingerprint learned from a
// verified internode connection for a peer. Unlike the old join request field,
// this value comes from the TLS handshake the dial just completed, so the
// recorded identity is evidence rather than a self-asserted claim.
func (s *Server) recordObservedFingerprint(nodeID, fingerprint string) {
	if nodeID == "" || fingerprint == "" {
		return
	}
	s.knownNodesMu.Lock()
	defer s.knownNodesMu.Unlock()
	if existing, ok := s.knownNodes[nodeID]; ok {
		existing.Fingerprint = fingerprint
		return
	}
	s.knownNodes[nodeID] = &NodeInfo{
		NodeID:      nodeID,
		Fingerprint: fingerprint,
		JoinedAt:    time.Now(),
		Status:      "active",
	}
}

// selfPeer is c.self in the wire Peer shape (ring.Node → Peer), with this
// node's own word on whether its store is sealed and where clients reach it.
func (c *Cluster) selfPeer() Peer {
	return Peer{ID: c.self.ID, Addr: c.self.Addr, Store: c.s.storeClaim(), Driver: c.s.ownDriverAddress()}
}

// handleGossip processes a membership announcement from a peer node.
func handleGossip(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	c := s.cluster
	if c == nil {
		return nodeResponse(transport.OpGossip, frame, GossipResponse{
			Success: false,
			Error:   "replication is not enabled on this node",
		}, nil)
	}

	var req GossipRequest
	noteRequestDecode("handleGossip", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nodeResponse(transport.OpGossip, frame, GossipResponse{
			Success:  false,
			Error:    fmt.Sprintf("decode gossip request: %v", err),
			Self:     c.selfPeer(),
			Features: clusterFeatures(),
		}, nil)
	}

	if !c.ValidateToken(req.Token) {
		// An unauthenticated caller learns nothing of this node's memory or
		// memtable (AppliedMemtableMB and MemoryLimitBytes stay unset).
		c.logger.Warn("gossip rejected: invalid cluster token", "node_id", req.NodeID, "addr", req.Addr)
		return nodeResponse(transport.OpGossip, frame, GossipResponse{
			Success:  false,
			Error:    "invalid cluster token",
			Self:     c.selfPeer(),
			Features: clusterFeatures(),
		}, nil)
	}

	if err := c.refuseSenderNotOfConnection(ctx, transport.OpGossip, req.NodeID); err != nil {
		return nodeResponse(transport.OpGossip, frame, GossipResponse{
			Success:  false,
			Error:    err.Error(),
			Self:     c.selfPeer(),
			Features: clusterFeatures(),
		}, nil)
	}

	s.recordNode(req.NodeID, req.Addr)
	c.recordMemberFeatures(req.NodeID, req.Features)
	c.observePeerMemtable(req.NodeID, req.Features, req.AppliedMemtableMB)
	sess, _ := transport.SessionFromContext(ctx)
	c.observeInstance(req.NodeID, req.Instance, gossipSource(remoteAddr(sess)))
	duplicateID := c.instances.liveDuplicate(req.NodeID, time.Now())

	// The token is valid, so the sender's record of itself is its word on
	// whether its store is sealed. A node in encryption.mode=require exchanges
	// no membership with a plaintext peer, and tells it why.
	sender := ring.Node{ID: req.NodeID, Addr: req.Addr}
	if senderStore := c.learnStoreFromGossip(req); senderStore != claimSealed && s.requiresSealedPeers() && req.NodeID != c.self.ID {
		known := peerKnowledge{claim: senderStore, answered: true}
		refusal := joinRefusal(sender, known, "exchange cluster membership with "+describePeer(sender))
		if _, member := c.memberByID(req.NodeID); member {
			c.noteMembershipRefusal(req.NodeID, refusal)
		}
		return nodeResponse(transport.OpGossip, frame, GossipResponse{
			Success: false,
			Error:   refusal.Error(),
			Self:    c.selfPeer(),
		}, nil)
	}

	// Reconcile membership: agree, resync to a newer epoch, or flag a genuine
	// divergence. A disagreement is reported to the sender as well as recorded
	// locally, so the operator sees it from either side.
	// An asker that is behind is sent the epochs it missed.
	var history []MembershipRecord
	switch {
	case req.Epoch != 0 && req.Epoch < c.Epoch() && req.Bootstrapped:
		// An asker on a provisional view is also sent the cluster's record of
		// its own provisional epoch, which its join writes over the view it
		// started on: a joiner then knows its buckets' previous owners
		// (on-demand pull spec §6.7, O-7 (b)).
		history = c.historyAbove(req.Epoch - 1)
	case req.Epoch != 0 && req.Epoch < c.Epoch():
		history = c.historyAbove(req.Epoch)
	}
	settled, settledOf, handOff := c.settledGossip()
	if err := c.reconcileRequest(req); err != nil {
		return nodeResponse(transport.OpGossip, frame, GossipResponse{
			Success:           false,
			Error:             err.Error(),
			Epoch:             c.Epoch(),
			Nodes:             c.memberNodes(),
			Members:           c.memberIDs(),
			Self:              c.selfPeer(),
			Features:          clusterFeatures(),
			Instance:          c.instance,
			DuplicateID:       duplicateID,
			AppliedMemtableMB: c.s.memtable.appliedMB,
			MemoryLimitBytes:  c.s.memtable.advertisedLimitBytes(),
			History:           history,
			RemovedAt:         c.removedAt.Load(),
			Bootstrapped:      c.bootstrapped.Load(),
			Settled:           settled,
			SettledOf:         settledOf,
			HandOff:           handOff,
		}, nil)
	}
	c.mergeSettled(req.NodeID, req.Settled, req.SettledOf, req.HandOff)

	return nodeResponse(transport.OpGossip, frame, GossipResponse{
		Success:           true,
		Message:           "membership agrees",
		Epoch:             c.Epoch(),
		Nodes:             c.memberNodes(),
		Members:           c.memberIDs(),
		Self:              c.selfPeer(),
		Features:          clusterFeatures(),
		Instance:          c.instance,
		DuplicateID:       duplicateID,
		AppliedMemtableMB: c.s.memtable.appliedMB,
		MemoryLimitBytes:  c.s.memtable.advertisedLimitBytes(),
		History:           history,
		RemovedAt:         c.removedAt.Load(),
		Bootstrapped:      c.bootstrapped.Load(),
		Settled:           settled,
		SettledOf:         settledOf,
		HandOff:           handOff,
	}, nil)
}

// handleReplPush applies entries sent by another node. This is the only
// inbound replication path: it serves both "apply as owner and place on the
// replica set" and "apply as replica", decided by the request's kind — a
// forwarded write, or a push of the sender's stream — and never by this node's
// ring alone (spec §19.3.6, R-1 and R-2).
func handleReplPush(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	c := s.cluster
	if c == nil {
		return nodeResponse(transport.OpReplAck, frame, ReplPushResponse{
			Success: false,
			Error:   "replication is not enabled on this node",
		}, nil)
	}

	var req ReplPushRequest
	noteRequestDecode("handleReplPush", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nodeResponse(transport.OpReplAck, frame, ReplPushResponse{
			Success: false,
			Error:   fmt.Sprintf("decode replication request: %v", err),
		}, nil)
	}

	if !c.ValidateToken(req.Token) {
		c.logger.Warn("replication rejected: invalid cluster token", "from", req.FromNode, "entries", len(req.Entries))
		return nodeResponse(transport.OpReplAck, frame, ReplPushResponse{
			Success:     false,
			Error:       "invalid cluster token",
			BatchNotRun: req.Batch,
		}, nil)
	}
	if err := c.refuseSenderNotOfConnection(ctx, transport.OpReplPush, req.FromNode); err != nil {
		refused := ReplPushResponse{Success: false, Error: err.Error(), BatchNotRun: req.Batch}
		if req.ReturnResponse {
			refused.WriteOutcome = outcomeNotApplied
		}
		return nodeResponse(transport.OpReplAck, frame, refused, nil)
	}

	// A replication push is authenticated cluster traffic and must not be
	// applied while this node believes the membership is inconsistent.
	// The refusal carries its kind: without one the coordinator answered its
	// client INTERNAL, which a driver never retries, for a refusal that clears
	// by itself (BUG-0704).
	if c.mismatch.Load() {
		refused := ReplPushResponse{
			Success:     false,
			Error:       ErrMemberMismatch.Error(),
			Refusal:     refusalMemberMismatch,
			BatchNotRun: req.Batch,
		}
		if req.ReturnResponse {
			refused.WriteOutcome = outcomeNotApplied
		}
		return nodeResponse(transport.OpReplAck, frame, refused, nil)
	}

	// An entry is a client's request, which was validated at the node it
	// entered; one that does not validate here is refused before anything
	// decodes it, and so is the push, as the client's door would have refused it.
	for _, e := range req.Entries {
		if _, err := validateRequest(transport.Opcode(e.Op), e.Payload); err != nil {
			refused := ReplPushResponse{Success: false, Error: fmt.Sprintf("entry %d of bucket %q: %v", e.Seq, e.Bucket, err), BatchNotRun: req.Batch}
			if req.ReturnResponse {
				refused.WriteOutcome = outcomeNotApplied
			}
			return nodeResponse(transport.OpReplAck, frame, refused, nil)
		}
	}

	// A question about a database changes nothing, and is answered from this
	// node's own state on any epoch.
	if req.Catalog != nil {
		return nodeResponse(transport.OpReplAck, frame, c.answerCatalog(req), nil)
	}

	// A push of the sender's own stream is applied only under the incarnation
	// this node has recorded for it: a position is a fact about one life of a
	// log, and is never compared with a sequence from another. A handshake is answered before anything else. A push that
	// would change state and is neither a stream push nor a forwarded request
	// is refused too: there is no third kind, and one would pass the
	// incarnation check unseen (A-4.3).
	if req.Stream != nil {
		defer c.lockStreamFrom(req.FromNode)()
		if req.Stream.Open {
			return nodeResponse(transport.OpReplAck, frame, c.answerHandshake(req.FromNode, req.Stream), nil)
		}
		if !c.streamIncarnationIs(req.FromNode, req.Stream.Incarnation) {
			return refusePush(frame, refusalReopen, fmt.Errorf(
				"the push names an incarnation of %s's log that this node has not recorded; open the stream again", req.FromNode))
		}
		// Entries of this stream that a stopped run logged and did not apply
		// are recorded as void before anything else of it is logged: a copy
		// logged beside an orphan that a crash leaves replayable would be
		// applied twice (replica_orphans.go).
		if err := c.payOrphanVoidDebt(req.FromNode); err != nil {
			return nodeResponse(transport.OpReplAck, frame, ReplPushResponse{Success: false, Error: err.Error()}, nil)
		}
	} else if !req.ReturnResponse && pushChangesState(req.Entries) {
		return refusePush(frame, refusalReopen, fmt.Errorf(
			"a push from %s that changes state is neither a stream push nor a forwarded request; open the stream", req.FromNode))
	}
	// A removed node's notice that it has named every bucket it holds is a fact
	// about its hand-off, whatever epoch either side is on (owner ruling R7).
	if req.Handoff != nil && req.Handoff.NamedAllAt != 0 {
		c.noteRemovedNamedAll(req.FromNode, req.Handoff.NamedAllAt)
		return nodeResponse(transport.OpReplAck, frame, ReplPushResponse{Success: true}, nil)
	}
	// A member's probe of this removed node is answered on any epoch too, with
	// this node's notice when the member may be told it (BUG-0702).
	if req.Handoff != nil && len(req.Handoff.Ask) == 0 && c.removed() {
		return nodeResponse(transport.OpReplAck, frame, c.answerRemovedProbe(req), nil)
	}
	if refused := c.refuseAcrossEpochs(req); refused != nil {
		return nodeResponse(transport.OpReplAck, frame, *refused, nil)
	}
	// A client's BATCH forwarded to this owner: every entry is answered, none
	// stops the others, and a bucket's entries apply in order
	// (batch-request spec §5.3).
	if req.Batch && !ignoreBatchFlagForTesting.Load() {
		return c.serveBatchPush(ctx, frame, &req)
	}
	// A question (step 9.6) is answered and changes nothing.
	if req.Handoff != nil {
		return nodeResponse(transport.OpReplAck, frame, c.answerQuestion(req), nil)
	}

	resp := ReplPushResponse{Success: true}
	// The written hooks of the entries' responses: a forwarded query's slot on
	// this, its owner. They ride on the REPL_ACK frame, or fire here on a path
	// that writes none of the responses they hold.
	var written []func()
	// positionNotInLog records that the stream position moved in a way no log
	// entry of the sender's origin carries: an entry skipped because a snapshot
	// position covers it, or an advance notice. The log rebuild at start cannot
	// recover either, so the move needs a synced checkpoint before the
	// acknowledgement leaves; every other move is in the log.
	positionNotInLog := false
	// Zero is "the request named no concern": the owner's apply resolves it for
	// the entry's own bucket, once the factor is known.
	wc := WriteConcernLevel(req.WriteConcern)

	// take answers one entry's outcome into resp and reports whether the push
	// stops there.
	take := func(e ReplEntry, entryResp ReplResponse, err error, forwardedWrite bool) bool {
		// An entry of a term below the copy's is a former owner's that arrived
		// after the copy moved on (the receiver's rule): it is passed over
		// unlogged and counted, and the position moves past it with no log
		// entry behind it, so it is checkpointed before the answer leaves.
		if errors.Is(err, errEntryOfOlderTerm) && !req.ReturnResponse {
			c.entriesOfOlderTerm.Add(1)
			c.logger.Debug("passed over an entry of an older term", "from", req.FromNode, "bucket", e.Bucket, "seq", e.Seq, "error", err)
			if settleErr := c.settleDivergence(req.FromNode, e.Seq); settleErr != nil {
				resp.Success = false
				resp.Error = settleErr.Error()
				fireWritten(written)
				written = nil
				return true
			}
			if e.Seq != 0 {
				c.raisePositionOwingCheckpoint(req.FromNode, e.Seq)
				positionNotInLog = true
			}
			if e.Seq > resp.AppliedThrough {
				resp.AppliedThrough = e.Seq
			}
			return false
		}

		if entryResp.onWritten != nil {
			written = append(written, entryResp.onWritten)
		}
		// The drops that made this node pass over or refuse the entry go back
		// to the sender, which learns from them the drop it missed (G-11 (a)).
		c.reportTombstones(&resp, e, err)
		if err != nil {
			// The entry is reported as not applied and AppliedThrough is not
			// advanced past it: the sender's write concern must not be
			// satisfied by an entry this node failed to apply.
			tagEntryRefusal(&resp, e, err, forwardedWrite)
			fireWritten(written)
			written = nil
			return true
		}

		if req.ReturnResponse {
			resp.Responses = append(resp.Responses, entryResp)
		}
		// A replica's position has moved past the entry already, inside
		// applyAsReplica, while its unapplied mark was still open. Nothing is
		// written for it: the entry is in this node's log with its origin, and
		// the position is rebuilt from there (rebuildPositionsFromLog).
		if e.Seq > resp.AppliedThrough {
			resp.AppliedThrough = e.Seq
		}
		return false
	}
	// A run's entries are the sender's stream, never a forwarded write.
	takeRunEntry := func(e ReplEntry, entryResp ReplResponse, err error) bool { return take(e, entryResp, err, false) }
	run := replicaRun{c: c, sender: req.FromNode}

	for _, e := range req.Entries {
		op := transport.Opcode(e.Op)
		changes := replicationRequired(op)
		c.logger.Debug("applying replicated entry",
			"from", req.FromNode, "op", op, "bucket", e.Bucket, "seq", e.Seq, "changes", changes)

		// A bucket entry of the sender's stream is logged into the run and
		// answered when the run is settled (BUG-0521). Any other entry, and one
		// the run cannot take, is answered on its own below, after the run.
		if run.joins(&req, e) {
			joined, err := run.admit(e)
			if joined {
				continue
			}
			if err != nil && len(run.logged) == 0 {
				// Refused with nothing ahead of it unapplied: the refusal
				// applyAsReplica would give.
				if take(e, ReplResponse{}, err, false) {
					break
				}
				continue
			}
		}
		if run.settle(ctx, takeRunEntry) {
			break
		}

		// Before anything is logged or read: a request forwarded by a
		// coordinator that is not known to be sealed is not served.
		if err := c.refuseForwardedFromPlaintextCoordinator(req, e); err != nil {
			resp.Success = false
			resp.Error = err.Error()
			if req.ReturnResponse {
				resp.WriteOutcome = outcomeNotApplied
			}
			fireWritten(written)
			written = nil
			break
		}

		// Replication is at-least-once: an acknowledgement lost in flight makes
		// the sender re-send, and a sender that restarted starts its position for
		// this peer at zero. A replica must therefore never apply the same
		// sequence twice — non-idempotent stores (FIFO, LIFO) would gain a
		// duplicate element, and a re-sent pop takes a different element than the
		// one it originally took.
		//
		// The check only reads. The position is recorded *after* the apply
		// succeeds, because recording it for an entry that failed would make the
		// failure permanent: the retry would be skipped as already applied.
		if !req.ReturnResponse && changes && !c.shouldApply(req.FromNode, e.Seq) {
			if e.Seq > resp.AppliedThrough {
				resp.AppliedThrough = e.Seq
			}
			continue
		}

		// A snapshot pulled onto this node already contains every entry of
		// its bucket from that source at or below the sequence it was taken
		// at (Cluster.pullBucket records it). Those entries are contents this
		// node holds, not work: applying them would replay the bucket's
		// history over the snapshot — a counter rewinding, a popped heap
		// element coming back. They are acknowledged without being
		// appended or applied, so the sender's position moves past them. The
		// snapshot is of one database's bucket; an entry of a same-named
		// bucket in another database is work, and seededCovers tells them
		// apart by the database in the entry's payload. Database
		// DDL is never passed over by position: a drop names the generations
		// it removes, so it commutes with every pull (G-4, G-6).
		//
		// Database DDL logged before generations existed is passed over too,
		// and never applied (G-8): it cannot say which incarnation it is about,
		// and on the build that logged it a member that missed it lost it for
		// good, which passing it over reproduces and nothing worse.
		//
		// The snapshot holds those entries only once it has landed. Its position
		// is recorded before it materialises (the catch-up-first ordering needs that), so until
		// the bucket's store is open the push is refused here and nothing at or
		// past the entry is acknowledged (R-4). A pull that is landing is waited
		// for first, a bounded while, so a landing does not fail the frame and
		// take the stream out of the in-sync set (BUG-0729); the run is settled,
		// so this push holds no fence the landing needs.
		//
		// The judgement runs without the bucket's fence, so a pull can record
		// its position and begin landing after it and before the create takes
		// its fence. The entry is judged once more after waiting when either
		// refusal would follow and a pull now lands; one deadline covers both
		// waits, so the retry does not double the wait past the sender's bound.
		var landingUntil time.Time
		rejudged := false
	judge:
		if !req.ReturnResponse && changes && e.Bucket != "" {
			landingUntil = c.awaitLanding(ctx, e, landingUntil)
		}
		snapshotCovers := !req.ReturnResponse && changes && e.Seq != 0 && e.Bucket != "" && c.seededCovers(req.FromNode, e)
		if snapshotCovers {
			if _, database := decodeFrameMeta(e.Payload).scope(op); !c.s.bucketStoreOpen(database, e.Bucket) {
				if !rejudged && c.pullLanding(database, e.Bucket) != nil {
					rejudged = true
					goto judge
				}
				resp.Success = false
				resp.Refusal = refusalAwaitingCopy
				resp.Error = fmt.Sprintf("bucket %q in database %q: this node holds a snapshot of it from %s taken at or after sequence %d "+
					"and has not finished putting it in place; nothing at or past that sequence is acknowledged until it has",
					e.Bucket, database, req.FromNode, e.Seq)
				fireWritten(written)
				written = nil
				break
			}
		}
		if snapshotCovers || (!req.ReturnResponse && changes && e.Seq != 0 && legacyDatabaseDDL(e)) {
			// A snapshot that covers a diverged entry is the re-seed that
			// repairs it (R8-E): the record settles.
			if err := c.settleDivergence(req.FromNode, e.Seq); err != nil {
				resp.Success = false
				resp.Error = err.Error()
				fireWritten(written)
				written = nil
				break
			}
			c.raisePositionOwingCheckpoint(req.FromNode, e.Seq)
			positionNotInLog = true
			if e.Seq > resp.AppliedThrough {
				resp.AppliedThrough = e.Seq
			}
			continue
		}

		if hook := afterReplicaJudgedHook.Load(); hook != nil && !req.ReturnResponse {
			(*hook)("", e.Bucket)
		}
		forwardedWrite := req.ReturnResponse && changes && e.Bucket != ""
		entryResp, refused, err := c.serveForwardedEntry(ctx, &req, e, wc)
		if refused == nil && !rejudged && !req.ReturnResponse && errors.Is(err, errSnapshotPending) {
			rejudged = true
			goto judge
		}
		if refused != nil {
			refused.applyTo(&resp)
			fireWritten(written)
			written = nil
			break
		}

		if take(e, entryResp, err, forwardedWrite) {
			break
		}
	}
	// The loop settles the run before it answers any entry that does not join
	// it, so what is left here is the end of the frame.
	run.settle(ctx, takeRunEntry)

	// An advance notice (spec §19.3.9): the sender says that no entry of its
	// stream at or below AdvanceTo is owed to this node. It moves the position
	// with no log entry behind it, so it is checkpointed and synced below,
	// before the answer leaves (T-96).
	if advanceTo := req.Stream.advanceTo(); advanceTo > 0 && resp.Success {
		c.raisePositionOwingCheckpoint(req.FromNode, advanceTo)
		positionNotInLog = true
		resp.AppliedThrough = max(resp.AppliedThrough, advanceTo)
	}

	// A node must never acknowledge a position it will not remember. The
	// sender prunes its log on the strength of the acknowledgement, so a
	// position that a restart loses leaves this node below the sender's horizon
	// for good.
	// A frame refused because its checkpoint failed leaves the position raised
	// in memory, so its retry is skipped as already applied: the debt is checked
	// for as well as the move that created it.
	if positionNotInLog || (!req.ReturnResponse && c.checkpointIsOwed(req.FromNode)) {
		if err := c.checkpointPosition(req.FromNode, true); err != nil {
			c.logger.Error("could not checkpoint the replication position; the frame is not acknowledged",
				"from", req.FromNode, "error", err)
			resp = ReplPushResponse{Success: false, Error: fmt.Sprintf("checkpointing the replication position of %s: %v", req.FromNode, err)}
			fireWritten(written)
			written = nil
		}
	}

	ack, err := nodeResponse(transport.OpReplAck, frame, resp, nil)
	if err != nil {
		fireWritten(written)
		return ack, err
	}
	for _, hook := range written {
		ack.AddWrittenHook(hook)
	}
	return ack, nil
}

// entryRefusal is an entry refused before it was applied, with no error to
// tag: the refusal kind, the write outcome and the text a forwarded push's
// answer carries for it.
type entryRefusal struct {
	refusal string
	outcome string
	message string
}

// applyTo answers a push with the refusal.
func (r *entryRefusal) applyTo(resp *ReplPushResponse) {
	resp.Success = false
	if r.refusal != "" {
		resp.Refusal = r.refusal
	}
	resp.WriteOutcome = r.outcome
	resp.Error = r.message
}

// serveForwardedEntry applies or answers one entry of a push, after the
// checks of the push and of the sender's stream: a forwarded write as the
// bucket's owner, a forwarded read, or a stream entry as a replica. The
// request's kind picks the branch, never this node's ring (spec §19.3.6). It
// is the per-entry body of handleReplPush's loop, and a batch push's entries
// run through the same function (batch-request spec §5.3), so the two cannot
// drift. refused is set for an entry answered before anything ran; err is
// what take tags.
func (c *Cluster) serveForwardedEntry(ctx context.Context, req *ReplPushRequest, e ReplEntry, wc WriteConcernLevel) (entryResp ReplResponse, refused *entryRefusal, err error) {
	op := transport.Opcode(e.Op)
	changes := replicationRequired(op)
	// A stream push is applied as a replica whoever owns the bucket here
	// (R-1), because re-originating it would give the entry a second sequence
	// and ship it again; a forwarded mutation is applied only by the bucket's
	// owner (R-2), because one applied on a replica is stored there alone and
	// answered as a success. Database DDL has no owner and is never forwarded.
	if req.ReturnResponse && changes && e.Bucket != "" {
		key := entryPlacement(e)
		if !c.actsAsOwner(key) {
			return ReplResponse{}, &entryRefusal{refusal: refusalNotOwner, outcome: outcomeNotApplied, message: fmt.Sprintf(
				"%s does not own bucket %q on its view of the membership (epoch %d); nothing was applied",
				c.self.ID, e.Bucket, c.Epoch())}, nil
		}
		// A member's frame skips the authorization default deny, which is
		// what refuses an unknown opcode from a client, and dispatch finds
		// out only after the append. An owner older than its coordinator
		// would log and ship a write it cannot run, which newer replicas
		// apply while the client is told nothing happened (BUG-0561).
		if !implementsOpcode(op) {
			return ReplResponse{}, &entryRefusal{outcome: outcomeNotApplied, message: unimplementedForwardedOpcodeText(op, c.self.ID)}, nil
		}
		// The join fence a client's write meets in Cluster.write, met here
		// before the unique pre-check reads the bucket and before anything
		// is logged: a bucket whose pull has not landed is
		// answered "not yet". It fences on the ownership decided above and
		// does not read the ring again.
		_, database := decodeFrameMeta(e.Payload).scope(op)
		if hook := afterOwnerDecidedHook.Load(); hook != nil {
			(*hook)(database, e.Bucket)
		}
		fenced, fenceErr := c.fenceUnheldBucket(ctx, op, database, e.Bucket, e.Payload)
		if fenceErr != nil {
			return ReplResponse{}, nil, withOutcome(outcomeNotApplied, fenceErr)
		}
		entryResp, err = c.applyAsOwner(fenced, e, key, wc)
		return entryResp, nil, err
	}
	// A replica applying shipped entries, or a forwarded read that carries no
	// placement obligation. A forwarded read of a bucket whose copy is still
	// being pulled here meets the read's join fence, which answers
	// awaiting_copy.
	var sent *transport.Frame
	if req.ReturnResponse && !changes {
		sent, err = c.forwardedRead(ctx, req, e, op)
		// A read this node would answer from its store is routed instead
		// when it names a spread template: at the owner, or at the previous
		// owner a take-over sent it to (ServeFromCopy), whose born set is
		// complete (spread MVP note, section 2.1).
		if sent == nil && err == nil && e.Bucket != "" {
			// Its sender admitted it (Cluster.forwardRead), so a fan-out is
			// not admitted again here (R1, rule 3).
			if _, database := decodeFrameMeta(e.Payload).scope(op); c.s.spreadTemplateHeld(database, e.Bucket) {
				sent, err = c.routeSpreadRead(withFanOutAdmitted(ctx), transport.NewFrame(op, e.Payload), database, e.Bucket)
			}
		}
	}
	switch {
	case sent != nil:
		entryResp = ReplResponse{Opcode: int32(sent.Opcode), Payload: sent.Payload, onWritten: sent.TakeWrittenHook()}
	case err == nil:
		entryResp, err = c.applyAsReplica(ctx, e, changes, req.FromNode, !req.ReturnResponse)
	}
	return entryResp, nil, err
}

// tagEntryRefusal answers a push with the error one entry failed with: the
// refusal kind a coordinator matches by sentinel (forwardRefusal) and, on a
// forwarded write, the owner's word on whether it was stored.
func tagEntryRefusal(resp *ReplPushResponse, e ReplEntry, err error, forwardedWrite bool) {
	resp.Success = false
	resp.Error = err.Error()
	if errors.Is(err, ErrJoinSyncIncomplete) {
		resp.Refusal = refusalAwaitingCopy
	}
	if errors.Is(err, errAwaitingHandoff) {
		resp.Refusal = refusalAwaitingHandoff
	}
	if errors.Is(err, ErrHandoffInProgress) {
		resp.Refusal = refusalHandoffInProgress
	}
	var diverged *replicaDivergence
	if errors.As(err, &diverged) {
		resp.Refusal = diverged.record.Reason
	}
	if tag := alterRefusalTag(err); tag != "" && forwardedWrite && transport.Opcode(e.Op) == transport.OpAlterBucket {
		resp.Refusal = tag
	}
	// A forwarded mutation of a bucket no member holds (BUG-0686): the
	// coordinator answers its client NOT_FOUND, not a retry.
	if forwardedWrite && errors.Is(err, ErrBucketNotFound) && resp.Refusal == "" {
		resp.Refusal = refusalBucketNotFound
	}
	if forwardedWrite {
		answerWriteOutcome(resp, err)
	}
	// The owner's view pauses schema changes: the coordinator, which may have
	// lifted its own pause a gossip round earlier, tells its client the
	// owner's reason as UNAVAILABLE (review F-6).
	if errors.Is(err, ErrSchemaChangePaused) {
		resp.Refusal = refusalSchemaPaused
		resp.WriteOutcome = outcomeNotApplied
		resp.Error = schemaRefusalText(err)
	}
}

// forwardedRead is what a read another node forwarded here is answered with
// before applyAsReplica: nil and nil let applyAsReplica answer it from this
// node's store; a frame is the answer of a forward this node made in its
// turn; an error refuses it.
//
// A read marked ServeFromCopy, sent to the bucket's final previous owner,
// skips the on-demand path (on-demand pull spec §6.2): it is answered from
// this node's open copy whatever its ring says, or refused with retry when the
// copy is gone, never "not found" and never sent on. Any other read meets the
// join fence; at the bucket's owner, a read of a bucket it has not taken over,
// or holds nothing of in the settling window, is answered as at the client's
// door (§8, site 2), never from the empty store, waiting at most what its
// ReadWaitMs left it. A read already rerouted once that reaches a node that
// does not own its bucket is refused with retry, not answered from a replica
// or sent on: the reroute is keyed on the request (§8).
func (c *Cluster) forwardedRead(ctx context.Context, req *ReplPushRequest, e ReplEntry, op transport.Opcode) (*transport.Frame, error) {
	_, database := decodeFrameMeta(e.Payload).scope(op)
	if req.ServeFromCopy {
		if e.Bucket == "" || c.s.bucketServed(database, e.Bucket) || c.s.bucketStoreOpen(database, e.Bucket) {
			return nil, nil
		}
		return nil, readAwaitingCopy("this node", database, e.Bucket)
	}
	if err := c.fenceUnlandedRead(database, e.Bucket); err != nil || e.Bucket == "" {
		return nil, err
	}
	key := placementOf(database, e.Bucket)
	if !c.actsAsOwner(key) {
		// A spread template's read is the router's, and this node is not
		// the router on its view (B1): retried, never routed from a born set
		// that may lag, never answered from the template's store.
		if req.Rerouted || c.s.spreadTemplateHeld(database, e.Bucket) {
			return nil, readMovedOnRetry(database, e.Bucket)
		}
		return nil, nil
	}
	epoch := c.Epoch()
	awaits, err := c.ownerReadAwaitsTakeOver(key, epoch, database, e.Bucket)
	switch {
	case err != nil:
		return nil, err
	case c.readsOnDemand(op, awaits, key, epoch, database, e.Bucket):
		wait := onDemandWait
		if req.ReadWaitMs > 0 {
			wait = min(wait, time.Duration(req.ReadWaitMs)*time.Millisecond)
		}
		answer := c.forwardedReadOnDemand(ctx, onDemandRequest{op: op, key: key, epoch: epoch, database: database, bucket: e.Bucket,
			wait: wait, rerouted: req.Rerouted})
		if answer.refusal != nil {
			c.noteOnDemand(answer.outcome, answer.waited)
			return nil, answer.refusal
		}
		reply, served, err := c.sendOnDemandAnswer(ctx, transport.NewFrame(op, e.Payload), e.Bucket, answer)
		if served || err != nil {
			return nil, err
		}
		return reply, nil
	case awaits:
		c.wakeTakeOver(database, e.Bucket)
		return nil, c.handoffInProgress(database, e.Bucket)
	}
	return nil, nil
}

// afterUnappliedReleaseHook, when set, runs the moment applyAsReplica releases
// an entry's unapplied mark. That is the earliest point a retention pass can
// stop counting the entry as unapplied, so a test runs one there: the node's
// position must already be past the entry, or the pass truncates an entry
// nothing records.
var afterUnappliedReleaseHook atomic.Pointer[func(ReplEntry)]

// releaseUnapplied releases a replica entry's unapplied mark.
func (c *Cluster) releaseUnapplied(floor uint64, entry ReplEntry) {
	c.unapplied.close(floor)
	if hook := afterUnappliedReleaseHook.Load(); hook != nil {
		(*hook)(entry)
	}
}

// unimplementedForwardedOpcodeText is an owner's refusal of a forwarded write
// whose opcode its build does not implement. The coordinator relays it to the
// client as "owner X refused ...", so it says what to do as well as why.
func unimplementedForwardedOpcodeText(op transport.Opcode, owner string) string {
	return fmt.Sprintf("%s does not implement opcode 0x%02X (%s), so it refused the write before logging it: nothing was applied. "+
		"The member that forwarded it runs a newer build; upgrade %s to that build, or retry once every member runs the same build",
		owner, uint8(op), op, owner)
}

// answerWriteOutcome puts on a forwarded write's refusal what the owner knows
// of it: whether it was stored, how many copies hold it, and the refusal kind
// whose sentinel the coordinator matches it by (Cluster.forward). A refusal
// that nothing classified is unknown: the entry may have been logged.
func answerWriteOutcome(resp *ReplPushResponse, err error) {
	resp.WriteOutcome = outcomeUnknown
	var outcome *writeOutcomeError
	if errors.As(err, &outcome) {
		resp.WriteOutcome = outcome.outcome
		resp.CopiesConfirmed = outcome.confirmed
		resp.ReplicationFactor = outcome.factor
	}
	switch {
	case errors.Is(err, ErrWriteConcernTimeout):
		resp.Refusal = refusalConcernNotMet
	case errors.Is(err, ErrOwnershipMoved):
		resp.Refusal = refusalOwnershipMoved
	}
}

// afterOwnerDecidedHook, when set, runs in handleReplPush once a forwarded
// write's node has decided it owns the bucket and before the join fence: the
// window where that race lives. Tests use it to adopt a membership there. Nil in
// production.
var afterOwnerDecidedHook atomic.Pointer[func(database, bucket string)]

// afterReplicaAdmittedHook, when set, runs in applyAsReplica once an entry has
// passed refuseUnservable — for a create, the "is a pull of it pending" check
// — and before it is logged and applied, inside the bucket's fence. Tests use
// it to hold a create in the window a pull's snapshot position must not be
// recorded in. Nil in production.
var afterReplicaAdmittedHook atomic.Pointer[func(c *Cluster, entry ReplEntry)]

// fireWritten runs written hooks whose responses will not be written.
func fireWritten(written []func()) {
	for _, hook := range written {
		hook()
	}
}

// applyAsReplica durably records and applies an entry shipped by the bucket's
// owner; a forwarded read changes nothing and is only dispatched. The
// bucket's apply fence is held across both steps — shared, or exclusively for
// a drop — so a snapshot taken on this node (snapshotBucket) sees either all
// of an entry or none of it. sender is the node whose stream carried the entry:
// the log records it beside the entry's sequence in that node's log, which is
// what marks the entry as not this node's to ship.
//
// recordPosition moves the node's position for sender past the entry once it
// has applied, and does it here, before the unapplied mark is released: a
// retention pass that ran between the two would see the entry as neither
// unapplied nor applied, checkpoint a position without it, and truncate it out
// of the log, and a kill then leaves no record of it anywhere.
func (c *Cluster) applyAsReplica(ctx context.Context, entry ReplEntry, changes bool, sender string, recordPosition bool) (ReplResponse, error) {
	if !changes {
		return c.applyLocal(ctx, entry)
	}
	// The entry is another node's: a drop it names can teach this node
	// generations it then logs again as its own (G-12).
	ctx = withPeerEntry(ctx, true)
	if entry.Bucket == "" {
		// Database-level DDL: no bucket, so no fence and no bucket to order.
		unappliedFloor := c.unapplied.open(c.walLastSequence)
		defer c.releaseUnapplied(unappliedFloor, entry)
		localSeq, err := c.appendWAL(entry, replicaOrigin(sender, entry))
		if err != nil {
			return ReplResponse{}, err
		}
		applyCtx, outcome := withApplyOutcome(withApplySeq(ctx, localSeq))
		resp, err := c.applyLocal(applyCtx, entry)
		// A drop or a create its store failed is answered Success false with
		// no Go error. Taken as applied, it was acknowledged and never
		// re-sent, and the replica kept a dropped database live (BUG-0572) or
		// never held a created one (BUG-0573). It is refused, so the owner
		// re-sends it, and voided as the bucket branch's store failure is
		// (BUG-0570), before the deferred release lets retention pass it. A
		// create marks its store failures in outcome, because its Success
		// false is also a name refusal a re-send could never cure.
		if err == nil {
			unapplied := databaseDropLeftUnapplied(entry, resp, localSeq)
			if outcome.storeFailure != nil {
				unapplied = &entryLeftUnappliedError{err: fmt.Errorf("applying %s of database %q at local seq %d: %w",
					transport.Opcode(entry.Op), decodeFrameMeta(entry.Payload).Name, localSeq, outcome.storeFailure)}
			}
			if unapplied != nil {
				if recordPosition {
					c.voidUnapplied(sender, localSeq)
				}
				return ReplResponse{}, unapplied
			}
		}
		if err == nil && recordPosition {
			c.markApplied(sender, entry.Seq)
		}
		// Database DDL past a recorded divergence settles it as any applied
		// entry does: the position moved past the refused entry, and a record
		// left standing would lower it again at the next start (a database
		// drop is one exit of ruling 53).
		if err == nil {
			if settleErr := c.settleDivergence(sender, entry.Seq); settleErr != nil {
				return ReplResponse{}, settleErr
			}
		}
		return resp, err
	}
	fence := c.s.acquireFence(entryPlacement(entry), fenceModeFor(transport.Opcode(entry.Op)))
	logged, err := c.logAsReplica(fence, entry, sender, c.appendWAL)
	if err != nil {
		fence.release()
		return ReplResponse{}, err
	}
	resp, err := c.finishAsReplica(ctx, logged, sender, recordPosition)
	logged.release(c)
	// An entry its store left unapplied is voided as a run's stopped entry is
	// (BUG-0570). recordPosition is a push of sender's stream, whose lock
	// handleReplPush holds, as voidOrphans needs.
	if recordPosition && leftUnapplied(err) {
		c.voidUnapplied(sender, logged.localSeq)
	}
	return resp, err
}

// databaseDropLeftUnapplied returns the failure of a database drop whose
// answer, resp, says it did not apply, or nil. Every Success false a drop
// answers on a replica is its store's: reading the database's generations,
// logging the drop it taught this node, or committing the drop. A drop every
// copy refuses alike (a name no database may have) is an error frame, and a
// drop of a database this node does not hold records its tombstones and
// succeeds, so both settle as before and are not re-sent for ever. A create
// is not judged here: its Success false is also a name refusal every member
// gives, which a re-send could never cure, so createDatabaseCtx marks its
// store failures in the apply's outcome instead.
func databaseDropLeftUnapplied(entry ReplEntry, resp ReplResponse, localSeq uint64) error {
	if transport.Opcode(entry.Op) != transport.OpDeleteDB || transport.Opcode(resp.Opcode) == transport.OpError ||
		applyOutcomeOf(resp, nil) != ownerOutcomeUnknown {
		return nil
	}
	message, _ := bson.Raw(resp.Payload).Lookup("message").StringValueOK()
	return &entryLeftUnappliedError{err: fmt.Errorf("applying the drop of database %q at local seq %d: %s",
		decodeFrameMeta(entry.Payload).Name, localSeq, message)}
}

// replicaLogged is a replica's entry between its append and its apply: the
// bucket's fence, the entry's apply ticket and unapplied mark, and the sequence
// this node's log gave it.
type replicaLogged struct {
	entry          ReplEntry
	fence          *fenceHold
	ticket         *applyTicket
	unappliedFloor uint64
	localSeq       uint64
	// voidThrough is how far a stop at this entry voids the sender's stream:
	// the last entry of its run, or the entry itself. stopVoided says the stop's
	// void was committed with the entry's consumption (stopRecorder).
	voidThrough uint64
	stopVoided  bool
}

// release retires the entry's ticket if its apply did not, then its unapplied
// mark, then its fence, the order applyAsReplica has always left them in.
func (l *replicaLogged) release(c *Cluster) {
	l.fence.entry.closeApply(l.ticket)
	c.releaseUnapplied(l.unappliedFloor, l.entry)
	l.fence.release()
}

// logAsReplica admits a bucket entry shipped by sender and appends it with
// appendFn, inside fence, which the caller holds and releases if this fails.
// Every refusal is decided here, before anything is logged.
func (c *Cluster) logAsReplica(fence *fenceHold, entry ReplEntry, sender string,
	appendFn func(ReplEntry, storage.WALOrigin) (uint64, error)) (*replicaLogged, error) {
	// A push this node cannot apply yet is refused before it is logged: the
	// owner re-sends it until it is accepted, and logging every re-send grew
	// the WAL by an entry per retry while the replica stayed fenced.
	if err := c.refuseUnservable(entry, true); err != nil {
		return nil, err
	}
	if op := transport.Opcode(entry.Op); op == transport.OpCreateBucket {
		name, database := decodeFrameMeta(entry.Payload).scope(op)
		if err := c.refuseReplayedCreateOnFullDisk(sender, database, name); err != nil {
			return nil, err
		}
	}
	if err := c.refuseRecordedBatchTooLarge(sender, entry); err != nil {
		return nil, err
	}
	if err := c.refuseRecordedDivergence(sender, entry); err != nil {
		return nil, err
	}
	if err := c.refuseByLineage(entry); err != nil {
		return nil, err
	}
	// An entry this node could not stamp is refused before it is logged, as
	// its owner and every other replica refuse it: a broken or older peer
	// cannot make this node log what it cannot version.
	if err := refuseUnversionable(entry, entry.Term, entry.Seq); err != nil {
		return nil, err
	}
	if hook := afterReplicaAdmittedHook.Load(); hook != nil {
		(*hook)(c, entry)
	}
	unappliedFloor := c.unapplied.open(c.walLastSequence)
	applyTicket := fence.entry.openApply(c.walLastSequence)
	// The apply sequence is the one this node's own log gives the entry, which
	// the bucket's applied_seq is compared against at replay. entry.Seq is the
	// owner's and stays the write version (build doc SA-17).
	localSeq, err := appendFn(entry, replicaOrigin(sender, entry))
	if err != nil {
		fence.entry.closeApply(applyTicket)
		c.releaseUnapplied(unappliedFloor, entry)
		return nil, err
	}
	fence.entry.assignApply(applyTicket, localSeq)
	return &replicaLogged{entry: entry, fence: fence, ticket: applyTicket, unappliedFloor: unappliedFloor, localSeq: localSeq, voidThrough: localSeq}, nil
}

// finishAsReplica applies an entry logAsReplica logged, once its log record is
// durable, and judges the outcome. The caller releases logged afterwards.
func (c *Cluster) finishAsReplica(ctx context.Context, logged *replicaLogged, sender string, recordPosition bool) (ReplResponse, error) {
	entry, localSeq := logged.entry, logged.localSeq
	// A replica records no failure (spec §4.4 rule 5): its retry is the
	// owner's redelivery, which needs this push answered Success false. The
	// handler reports a store failure inside its response, so it is carried
	// out here and returned as the error handleReplPush answers with.
	// The lineage moves to the owner's sequence, entry.Seq — the same number
	// on every copy — and never to localSeq (build doc §1 trap 6).
	applyCtx, outcome := withApplyOutcome(withLineage(ctx, entry.Seq, false))
	if entry.OwnerOutcome == ownerOutcomeApplied || entry.OwnerOutcome == ownerOutcomeOwed {
		applyCtx = withOwnerCommitted(applyCtx)
	}
	applyCtx = withOwnerDecided(applyCtx, ownerDecisionOf(entry.OwnerOutcome))
	if judgedByLineage(transport.Opcode(entry.Op), entry.Bucket) {
		applyCtx = withEntryTerm(applyCtx, entry.Term, !c.sourceClamped(sender))
	}
	// A stop the store judges is recorded in the batch that consumes the
	// entry (stopRecorder); recordPosition is a push of sender's stream, under
	// its stream lock, as the void needs.
	var stop *stopRecorder
	if recordPosition {
		stop = &stopRecorder{c: c, source: sender, entry: entry, through: logged.voidThrough}
		applyCtx = withStopRecorder(applyCtx, stop)
	}
	resp, err := c.applyInTurn(applyCtx, logged.fence.entry, logged.ticket, entry, localSeq)
	stopRecorded := stop != nil && stop.staged && outcome.storeFailure == nil
	if stopRecorded {
		c.settleStagedVoid(sender)
		logged.stopVoided = true
	}
	// The store passed the entry over by the receiver's rule and consumed its
	// local sequence: the copy holds it, or it is of another term. Nothing
	// else about it is judged.
	if err == nil && outcome.lineage != nil {
		return c.passedOverByLineage(sender, entry, outcome.lineage, recordPosition)
	}
	c.noteReplicaDisagreement(entry, resp, err)
	// A divergence the store consumed is recorded already, in the batch that
	// consumed it: never acknowledged (I-H4), and a re-send is refused before
	// it is logged (R8-E).
	if stopRecorded && stop.diverged != nil {
		refused := &replicaDivergence{source: sender, record: *stop.diverged}
		c.holdDivergence(refused)
		c.noteDivergence(refused)
		return ReplResponse{}, refused
	}
	// A replica's live apply records no failure, so the store's mark stays
	// below the entry and replay would apply it (replica_orphans.go).
	if err == nil && outcome.storeFailure != nil {
		return ReplResponse{}, &entryLeftUnappliedError{err: fmt.Errorf("applying %s for bucket %q at local seq %d: %w",
			transport.Opcode(entry.Op), entry.Bucket, localSeq, outcome.storeFailure)}
	}
	// A create of another incarnation of a bucket held here is refused and
	// recorded, never acknowledged: the sender stalls on it, so nothing of the
	// stray incarnation reaches this copy (review M-7).
	if err == nil && outcome.anotherIncarnation != nil {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.OpCreateBucket)
		return ReplResponse{}, c.recordDivergence(sender, &replicaDivergence{source: sender,
			record: divergenceRecord{Seq: entry.Seq, Database: database, Bucket: entry.Bucket,
				Incarnation: outcome.anotherIncarnation.held, Created: outcome.anotherIncarnation.created, Reason: refusalAnotherIncarnation}})
	}
	if err == nil && ownerCommittedWhatThisNodeCannot(entry, outcome) {
		refused := c.replicaBatchTooLarge(entry)
		if recordErr := c.recordSizeRefusal(sender, entry.Seq); recordErr != nil {
			return ReplResponse{}, errors.Join(refused, recordErr)
		}
		return ReplResponse{}, refused
	}
	// The owner applied the entry and this node refused it for a reason the
	// owner did not share: never acknowledged (I-H4). Recorded before the
	// answer, so a re-send is refused before it is logged (R8-E).
	if err == nil && entry.OwnerOutcome == ownerOutcomeApplied && applyOutcomeOf(resp, nil) == ownerOutcomeUnknown {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
		var held uint64
		if st, stateErr := c.s.bucketStateNamed(database, entry.Bucket); stateErr == nil {
			held = st.Incarnation
		}
		refused := &replicaDivergence{source: sender, record: divergenceRecord{
			Seq: entry.Seq, Database: database, Bucket: entry.Bucket, Incarnation: held, Reason: refusalDiverged}}
		recordErr := c.recordDivergence(sender, refused)
		c.noteDivergence(refused)
		return ReplResponse{}, recordErr
	}
	// The owner's insert was put over an _id_ this copy held: the copies agree
	// now, so nothing is held, but the difference they had is said and counted.
	if err == nil && outcome.heldUnknownID && outcome.committed {
		c.noteHeldID(sender, entry.Payload, entry.Op, entry.Bucket, entry.Seq)
	}
	if err == nil && recordPosition {
		c.markApplied(sender, entry.Seq)
	}
	if err == nil && judgedByLineage(transport.Opcode(entry.Op), entry.Bucket) {
		c.noteReconciled(entry, sender)
	}
	if err == nil {
		if _, settleErr := c.settleSizeRefusal(sender, entry.Seq); settleErr != nil {
			return ReplResponse{}, settleErr
		}
		if settleErr := c.settleDivergence(sender, entry.Seq); settleErr != nil {
			return ReplResponse{}, settleErr
		}
	}
	return resp, err
}

// noteReconciled counts an entry applied from a sender that is not the
// bucket's owner on this node's ring while the bucket carries a hand-off
// mark: a ceded or removed owner finishing the delivery of its term. While
// nothing is unsettled it costs two atomic reads and the epoch's.
func (c *Cluster) noteReconciled(entry ReplEntry, sender string) {
	if !c.mayBeUnsettled(c.Epoch()) {
		return
	}
	_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
	key := placementOf(database, entry.Bucket)
	if c.placementMarksOf(key) == (placementMarks{}) {
		return
	}
	if owner, err := c.Owner(key); err == nil && owner.ID != sender {
		c.entriesReconciled.Add(1)
	}
}

// replicaOrigin is the origin of an entry applied from sender's stream: entry.Seq
// is the sender's own sequence for it. The owner's outcome is logged so that
// replay judges the entry as finishAsReplica does: a refusal of an entry its
// owner applied whose divergence record a crash took is found again, not
// counted applied (BUG-0581). An outcome no build defines is logged unknown,
// which is how finishAsReplica already treats it.
func replicaOrigin(sender string, entry ReplEntry) storage.WALOrigin {
	var outcome byte
	switch entry.OwnerOutcome {
	case ownerOutcomeApplied, ownerOutcomeRefusedByConstraint, ownerOutcomeOwed:
		outcome = byte(entry.OwnerOutcome)
	}
	return storage.WALOrigin{Kind: storage.WALOriginReplica, Source: sender, Seq: entry.Seq, OwnerOutcome: outcome, Term: entry.Term}
}

// nodeResponse frames a node-to-node response payload.
func nodeResponse(op transport.Opcode, req *transport.Frame, payload any, err error) (*transport.Frame, error) {
	body, merr := bson.Marshal(payload)
	if merr != nil {
		return transport.NewFrameWithID(transport.OpError, req.RequestID, nil), fmt.Errorf("marshal %s response: %w", op, merr)
	}
	return transport.NewFrameWithID(op, req.RequestID, body), err
}
