package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The plaintext-peer refusal (at-rest-encryption spec §5.5, §8.1, §8.9;
// owner ruling of 2026-09-30).
//
// Each node seals what it stores under its own key and no key crosses the
// wire, so a sealed node that hands a bucket's data to a peer whose store is
// plaintext has put that data on a disk in the clear — and the transfer
// succeeds. A sealed node therefore gives a bucket's data only to a peer that
// has said, itself, that its store is sealed. "Gives" is both directions of a
// connection: what it sends (replication, catch-up, a forwarded write), and
// what it answers (a pull, a versioned read, a request a coordinator forwards).
//
// Three rules keep that true.
//
//  1. A node speaks only for itself. The claim is one word on the node's own
//     gossip record (Peer.Store) and on each request whose answer carries a
//     bucket's data. A receiver never takes one node's word about another.
//  2. What this node remembers about a peer decides only whether to refuse
//     early. It is never a licence to send: before anything that places data
//     leaves, the peer's claim is confirmed on the connection it will travel
//     on, and the send is refused if a socket of that connection has been
//     re-established since (peer.confirmSealed). A peer that went down sealed
//     and came back plaintext under the same identity is asked again, because
//     coming back is a new connection. membership.json's record of sealed
//     peers is a hint of the same kind.
//  3. For what this node answers, the requester's claim rides the request
//     itself, so it is the word of the process that is asking, now.
//
// What a peer's word is worth. It travels only on the internode channel: a
// connection whose certificate is pinned and whose every request
// carries the cluster token. A peer that lies — says sealed and stores
// plaintext — must hold the cluster token and a pinned certificate, which
// makes it a member of the cluster already. That is the live attacker inside
// the trust boundary the spec puts out of scope (§1.2, stated non-goals:
// encryption at rest stops offline theft of a disk, not a party holding the
// node's credentials). The claim is a guard against misconfiguration, not an
// attestation.
//
// The cost is one word per peer and nothing per bucket: a check is a map
// lookup per replica-set member per operation, and a membership change does no
// work over the buckets for it (AGENTS.md rule 11).

// storeClaim is a node's own word on whether its store is encrypted at rest.
type storeClaim string

const (
	// claimUnreported is a node that answered and said nothing: it runs a
	// version from before the claim existed. Its store may well be sealed, so
	// it is never described as plaintext — only as not saying.
	claimUnreported storeClaim = ""
	claimPlaintext  storeClaim = "plaintext"
	claimSealed     storeClaim = "sealed"
)

// storeSealed reports whether this node's store is sealed at rest, by the mode
// the node was started in. The cluster paths ask it because a refusal should
// follow the configuration. The operator surface does not: it reports
// storeSeal, the structural check of the files (encryption_status.go), so a
// mode that disagreed with the store could not be reported as sealed.
func (s *Server) storeSealed() bool {
	return modeSealsStore(s.config.Encryption.Mode)
}

// storeClaim is what this node says of its own store on the internode channel.
func (s *Server) storeClaim() storeClaim {
	if s.storeSealed() {
		return claimSealed
	}
	return claimPlaintext
}

// requiresSealedPeers reports whether this node refuses a plaintext peer at
// join as well as at placement: encryption.mode=require. In mode on it admits
// the peer, warns once, and relies on the placement refusal.
func (s *Server) requiresSealedPeers() bool {
	return s.config.Encryption.Mode == "require"
}

const (
	// sealedProbeInterval bounds how often a node asks a peer that has never
	// answered whether its store is sealed. Without it every refused write to a
	// replica set holding a down peer would cost a dial timeout.
	sealedProbeInterval = 2 * time.Second
	// sealedProbeTimeout bounds that one question.
	sealedProbeTimeout = 5 * time.Second
	// plaintextRefusalLogInterval rate-limits the log line for a refusal that
	// repeats on a timer (catch-up runs every verifyPeer round, a held batch is
	// retried every replRetryInterval) or per bucket (a sweep over a million
	// buckets is a million refusals of one peer).
	plaintextRefusalLogInterval = 5 * time.Minute
	// confirmAttempts bounds how often a send re-asks a peer whose sockets keep
	// being re-established under it.
	confirmAttempts = 3
)

// peerKnowledge is what this node knows of a peer's store: the peer's last
// claim, and whether it has ever made one.
type peerKnowledge struct {
	claim storeClaim
	// answered is false for a peer that has never been heard from — it is
	// down, or this node has not reached it yet.
	answered bool
}

func (k peerKnowledge) sealed() bool { return k.answered && k.claim == claimSealed }

// sealedPeerTable is what each peer has said about its own store.
type sealedPeerTable struct {
	mu sync.RWMutex
	// claims holds each peer's last first-hand claim. A peer with no entry has
	// never answered and is given nothing.
	claims map[string]storeClaim
	// probedAt is when a peer with no claim was last asked.
	probedAt map[string]time.Time
	// announced records the claim this node last logged for a peer that is not
	// sealed, so the warning is once per peer and state, not once per gossip
	// round.
	announced map[string]storeClaim
	// refusalLoggedAt is when a refusal to a peer was last logged.
	refusalLoggedAt map[string]time.Time
}

func newSealedPeerTable(persistedSealed []string) *sealedPeerTable {
	table := &sealedPeerTable{
		claims:          make(map[string]storeClaim, len(persistedSealed)),
		probedAt:        make(map[string]time.Time),
		announced:       make(map[string]storeClaim),
		refusalLoggedAt: make(map[string]time.Time),
	}
	for _, id := range persistedSealed {
		table.claims[id] = claimSealed
	}
	return table
}

// knowledge returns what is known of a peer's store.
func (t *sealedPeerTable) knowledge(id string) peerKnowledge {
	t.mu.RLock()
	defer t.mu.RUnlock()
	claim, answered := t.claims[id]
	return peerKnowledge{claim: claim, answered: answered}
}

// record stores a peer's claim and reports whether it changed the set of
// peers known sealed, which is all that membership.json persists: a plaintext
// cluster's nodes record each other and never rewrite the file for it.
func (t *sealedPeerTable) record(id string, claim storeClaim) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasSealed := t.claims[id] == claimSealed
	t.claims[id] = claim
	if claim == claimSealed {
		// A peer that comes back sealed and later stops being so is announced
		// again.
		delete(t.announced, id)
	}
	return wasSealed != (claim == claimSealed)
}

// sealedIDs returns the peers known sealed among members, sorted: the set
// membership.json persists.
func (t *sealedPeerTable) sealedIDs(members []ring.Node) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var ids []string
	for _, member := range members {
		if t.claims[member.ID] == claimSealed {
			ids = append(ids, member.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// retain forgets every peer outside members, so the table is bounded by the
// member count however many nodes have come and gone.
func (t *sealedPeerTable) retain(members []ring.Node) {
	keep := make(map[string]struct{}, len(members))
	for _, member := range members {
		keep[member.ID] = struct{}{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, perPeer := range []map[string]storeClaim{t.claims, t.announced} {
		for id := range perPeer {
			if _, member := keep[id]; !member {
				delete(perPeer, id)
			}
		}
	}
	for _, perPeer := range []map[string]time.Time{t.probedAt, t.refusalLoggedAt} {
		for id := range perPeer {
			if _, member := keep[id]; !member {
				delete(perPeer, id)
			}
		}
	}
}

// firstAnnouncement reports whether a peer's not-sealed claim has not been
// logged yet, and marks it logged.
func (t *sealedPeerTable) firstAnnouncement(id string, claim storeClaim) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if logged, announced := t.announced[id]; announced && logged == claim {
		return false
	}
	t.announced[id] = claim
	return true
}

// due reports whether the interval has passed since at[id], and stamps it.
func (t *sealedPeerTable) due(at map[string]time.Time, id string, interval time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if last, stamped := at[id]; stamped && now.Sub(last) < interval {
		return false
	}
	at[id] = now
	return true
}

// recordPeerStore stores what a peer said about its own store on an
// authenticated internode exchange, persists a change to the sealed set with
// the membership, and says so once when a sealed node learns a peer is not
// sealed.
func (c *Cluster) recordPeerStore(peer ring.Node, claim storeClaim) {
	if peer.ID == "" || peer.ID == c.self.ID {
		return
	}
	if c.sealedPeers.record(peer.ID, claim) {
		if err := c.persistSealedPeers(); err != nil {
			c.logger.Warn("could not persist which peers are encrypted; this node asks them again after a restart",
				"node_id", peer.ID, "error", err)
		}
	}
	if claim == claimSealed || !c.s.storeSealed() || !c.sealedPeers.firstAnnouncement(peer.ID, claim) {
		return
	}
	if claim == claimUnreported {
		c.logger.Warn("a peer runs a version that does not report whether its store is encrypted: "+
			"this node never places, replicates or migrates data onto it until it does. "+
			"Upgrade that node to this version, or remove it from the cluster with 'remove-node'",
			"node_id", peer.ID, "addr", peer.Addr, "encryption_mode", c.s.config.Encryption.Mode)
		return
	}
	if c.s.requiresSealedPeers() {
		c.logger.Error("a peer's store is not encrypted and this node requires encrypted peers (encryption.mode=require): "+
			"it exchanges no membership with that peer and never places, replicates or migrates data onto it. "+
			"Enable encryption on that node from a fresh data directory, or remove it from the cluster with 'remove-node'",
			"node_id", peer.ID, "addr", peer.Addr)
		return
	}
	c.logger.Warn("a peer's store is not encrypted: this node (encryption.mode=on) admits it to the cluster "+
		"but never places, replicates or migrates data onto it. "+
		"Enable encryption on that node from a fresh data directory, or remove it from the cluster with 'remove-node'; "+
		"set encryption.mode=require on this node to refuse such a peer at join",
		"node_id", peer.ID, "addr", peer.Addr)
}

// learnStoreFromGossip reads the sender's own entry of a gossip request,
// records it when the sender is a member, and returns it. A request carries
// the sender's whole member list; only the entry the sender wrote about itself
// is its word. A sender outside the member set is not recorded: the table is
// bounded by the members, and a node is asked directly when it is proposed
// (newcomerStore).
func (c *Cluster) learnStoreFromGossip(req GossipRequest) storeClaim {
	claim := claimUnreported
	driver := ""
	for _, listed := range req.Nodes {
		if listed.ID == req.NodeID {
			claim = listed.Store
			driver = listed.Driver
			break
		}
	}
	if sender, member := c.memberByID(req.NodeID); member {
		c.recordPeerStore(sender, claim)
		c.recordPeerDriver(sender, driver)
	}
	return claim
}

// learnStoreFromGossipResponse records what the peer at the other end of an
// exchange said of itself, and nothing else in the answer: its member list
// names other nodes, and a node's word about another node is not taken. The
// answer arrived on a pinned, token-authenticated connection to this peer, so
// its record of itself counts whether or not it accepted the exchange. Self is
// matched by address as well as identity: a node can be listed under its
// address and announce its configured id (GossipResponse.Self). A peer that
// accepted the exchange without such a record runs a version from before the
// claim; a refusal without one says nothing at all.
func (c *Cluster) learnStoreFromGossipResponse(peer ring.Node, resp GossipResponse) {
	switch announced := resp.Self; {
	case announced.ID == peer.ID, announced.Addr != "" && announced.Addr == peer.Addr:
		c.recordPeerStore(peer, announced.Store)
		c.recordPeerDriver(peer, announced.Driver)
	case resp.Success:
		c.recordPeerStore(peer, claimUnreported)
		c.recordPeerDriver(peer, "")
	}
}

// peerStore returns what a member has said of its store. A member that has
// never answered is asked once, now, over its peer connection — so a sealed
// peer this node has not gossiped with yet is not refused for that — and at
// most once per sealedProbeInterval after a probe that got no answer.
func (c *Cluster) peerStore(ctx context.Context, member ring.Node) peerKnowledge {
	known := c.sealedPeers.knowledge(member.ID)
	if known.answered {
		return known
	}
	if !c.sealedPeers.due(c.sealedPeers.probedAt, member.ID, sealedProbeInterval) {
		return known
	}
	// The exchange records the peer's answer (peer.gossip); its error is the
	// peer being down or disagreeing about membership, and either way what
	// matters is whether an answer arrived. It is bounded: the caller is a
	// write or a pull, and a peer that accepts a connection and never answers
	// must not hold either.
	probeCtx, cancel := context.WithTimeout(ctx, sealedProbeTimeout)
	defer cancel()
	if err := c.peerFor(member).gossip(probeCtx); err != nil {
		c.logger.Debug("asking a peer whether its store is encrypted", "node_id", member.ID, "error", err)
	}
	return c.sealedPeers.knowledge(member.ID)
}

// plaintextPeerError is the refusal to give a bucket's data to a peer that is
// not known to be sealed. It names the peer, says which of three things is
// known of it — it said plaintext, it does not say, it has not answered — and
// gives the way out that fits (AGENTS.md rule 3). Only a peer that said its
// store is plaintext is told to re-create it: following that advice wipes a
// node, so it is never given on a guess.
type plaintextPeerError struct {
	peer  ring.Node
	known peerKnowledge
	// action is what was refused, as the object of "refusing to".
	action string
	mode   string
	// alternative, when set, is a way out that needs no change to the peer,
	// given before the ones that do.
	alternative string
}

func (e *plaintextPeerError) Error() string {
	return fmt.Sprintf("refusing to %s: %s, and this node's store is encrypted (encryption.mode=%s), so %s. "+
		"%s%s, or remove it from the cluster with 'remove-node %s'. Nothing was sent",
		e.action, peerStoreState(e.peer, e.known), e.mode, peerStoreRisk(e.known),
		e.alternative, peerStoreFix(e.peer, e.known, "encryption.mode=on"), e.peer.ID)
}

// peerStoreState is the clause both refusals open with: what is known of the
// peer's store.
func peerStoreState(peer ring.Node, known peerKnowledge) string {
	switch {
	case !known.answered:
		return fmt.Sprintf("peer %s has not answered, so whether its store is encrypted is not known", describePeer(peer))
	case known.claim == claimPlaintext:
		return fmt.Sprintf("the store of peer %s is not encrypted", describePeer(peer))
	default:
		return fmt.Sprintf("peer %s runs a version that does not report whether its store is encrypted", describePeer(peer))
	}
}

// peerStoreRisk is what would follow from sending: a fact for a peer that said
// plaintext, a possibility for one that has not said.
func peerStoreRisk(known peerKnowledge) string {
	if known.answered && known.claim == claimPlaintext {
		return "the data would be written to that node's disk in the clear"
	}
	return "the data could be written to that node's disk in the clear"
}

// peerStoreFix is the first way out of both refusals, by what is known.
func peerStoreFix(peer ring.Node, known peerKnowledge, modes string) string {
	switch {
	case !known.answered:
		return fmt.Sprintf("Start %s and try again: an encrypted node says so as soon as it answers", peer.ID)
	case known.claim == claimPlaintext:
		return fmt.Sprintf("Enable encryption on %s by starting it from a fresh data directory with %s and an encryption.keystore",
			peer.ID, modes)
	default:
		return fmt.Sprintf("Upgrade %s to this version: an encrypted node then says so, and nothing about its data directory needs to change",
			peer.ID)
	}
}

// describePeer names a peer by identity, with its address when that differs.
func describePeer(peer ring.Node) string {
	if peer.Addr == "" || peer.Addr == peer.ID {
		return peer.ID
	}
	return fmt.Sprintf("%s (%s)", peer.ID, peer.Addr)
}

// refusal builds the refusal for member, or nil when it is known sealed.
func (c *Cluster) refusal(member ring.Node, known peerKnowledge, action string) error {
	if known.sealed() {
		return nil
	}
	return &plaintextPeerError{peer: member, known: known, action: action, mode: c.s.config.Encryption.Mode}
}

// refusePlaintextPeer returns the refusal for handing a bucket's data to
// member, or nil when this node's store is plaintext or the member has said
// its own is sealed. It may ask the member (peerStore).
func (c *Cluster) refusePlaintextPeer(ctx context.Context, member ring.Node, action string) error {
	if !c.s.storeSealed() || member.ID == c.self.ID {
		return nil
	}
	return c.refusal(member, c.peerStore(ctx, member), action)
}

// refuseKnownPlaintextPeer is refusePlaintextPeer without the question: it
// answers from what the member has already said, for a caller that must not
// wait on the network.
func (c *Cluster) refuseKnownPlaintextPeer(member ring.Node, action string) error {
	if !c.s.storeSealed() || member.ID == c.self.ID {
		return nil
	}
	return c.refusal(member, c.sealedPeers.knowledge(member.ID), action)
}

// logPlaintextRefusal records a refusal that repeats, at most once per
// plaintextRefusalLogInterval per peer.
func (c *Cluster) logPlaintextRefusal(member ring.Node, refusal error) {
	if !c.sealedPeers.due(c.sealedPeers.refusalLoggedAt, member.ID, plaintextRefusalLogInterval) {
		return
	}
	c.logger.Warn("refused to send a bucket's data to a peer that is not known to be encrypted",
		"node_id", member.ID, "addr", member.Addr, "error", refusal)
}

// placesBucketData reports whether an entry puts a bucket, or anything in one,
// on the node it is sent to. A bucket drop is the exception among mutations: it
// carries names only and removes data, so it still reaches a plaintext peer —
// which is how an operator clears a bucket such a peer already holds.
func placesBucketData(entry ReplEntry) bool {
	op := transport.Opcode(entry.Op)
	return entry.Bucket != "" && replicationRequired(op) && op != transport.OpDeleteBucket
}

// carriesBucketData is placesBucketData plus a restore, the one client write
// that travels as a forwarded read does (replicationRequired) while its
// payload is the bucket's contents.
func carriesBucketData(entry ReplEntry) bool {
	switch transport.Opcode(entry.Op) {
	case transport.OpRestoreStart, transport.OpRestoreChunk, transport.OpRestoreDone:
		return entry.Bucket != ""
	}
	return placesBucketData(entry)
}

// placementAction names a refused entry: the operation and its bucket.
func placementAction(entry ReplEntry, verb string) string {
	return fmt.Sprintf("%s %s for bucket %q", verb, transport.Opcode(entry.Op), entry.Bucket)
}

// refusePlacementOnPlaintextReplica is the owner's check before it logs or
// applies a write: every other member of the bucket's replica set must be
// sealed, or the write is refused whole. Refusing before the append is what
// keeps a refused create from leaving a bucket that looks created on this node
// and exists nowhere else (spec §5.5).
func (c *Cluster) refusePlacementOnPlaintextReplica(ctx context.Context, entry ReplEntry, key placementKey) error {
	if !c.s.storeSealed() || !placesBucketData(entry) {
		return nil
	}
	// The set the entry will be shipped to, not the bucket's set now: a create
	// that declares a factor is delivered at that factor, which can reach members
	// the registry does not place the bucket on yet.
	replicas, err := c.deliveryTargets(entry, key)
	if transport.Opcode(entry.Op) == transport.OpAlterBucket {
		// An alter is judged at the factor it asks for (spec §4.3 R12), never
		// at its delivery union: until the owner stamps it, its
		// previous_factor is whatever the client sent.
		replicas, err = c.alterTargets(entry, key)
	}
	if err != nil {
		return err
	}
	for _, replica := range replicas {
		if err := c.refusePlaintextPeer(ctx, replica, placementAction(entry, "replicate")); err != nil {
			return err
		}
	}
	return nil
}

// refuseForwardToPlaintextOwner is the coordinator's check before it forwards
// a write to the bucket's owner: the owner stores what it is sent.
func (c *Cluster) refuseForwardToPlaintextOwner(ctx context.Context, owner ring.Node, entry ReplEntry) error {
	if !c.s.storeSealed() || !placesBucketData(entry) {
		return nil
	}
	return c.refusePlaintextPeer(ctx, owner, placementAction(entry, "forward"))
}

// refuseRestoreToPlaintextOwner is the same check for a restore, the one
// client write that is routed to the owner as a read is (replicationRequired)
// and so never passes through Cluster.write.
func (c *Cluster) refuseRestoreToPlaintextOwner(ctx context.Context, owner ring.Node, op transport.Opcode, bucket string) error {
	switch op {
	case transport.OpRestoreStart, transport.OpRestoreChunk, transport.OpRestoreDone:
		return c.refusePlaintextPeer(ctx, owner, fmt.Sprintf("forward %s for bucket %q", op, bucket))
	}
	return nil
}

// refuseShipToPlaintextReplica is the owner's second check, at the moment an
// applied entry is queued for a replica. The ship list is the delivery set
// computed before the apply (addressInFence), not read again here: a peer that
// became plaintext since refusePlacementOnPlaintextReplica is refused from
// what it has already said (refuseKnownPlaintextPeer), and a member added
// during the write is not in this entry's list at all — catch-up reaches it
// later and refuses it there (refuseReplayToPlaintextPeer). It does not ask
// the peer: the writer holds a ship ticket that every later write to that peer
// waits behind.
func (c *Cluster) refuseShipToPlaintextReplica(replica ring.Node, entry ReplEntry) error {
	if !placesBucketData(entry) {
		return nil
	}
	return c.refuseKnownPlaintextPeer(replica, placementAction(entry, "replicate"))
}

// holdBackFromRefusedReplica stalls the stream to a replica the owner has just
// refused an entry (refuseShipToPlaintextReplica). Skipping the replica
// without a stall let its next acknowledgement, or an advance notice, move its
// cursor past an entry it never received, and nothing sent it later (BUG-0313).
// Stalled, the stream is served from the log from the cursor up, and the
// replay is refused while the replica is not known sealed
// (refuseReplayToPlaintextPeer), so the cursor stays below the entry until the
// replica receives it. A stalled stream is asked for no advance notice
// (askForAdvance) and a stall ends the generation an idle sender would send one
// in.
func (c *Cluster) holdBackFromRefusedReplica(replica ring.Node, seq uint64, refusal error) {
	c.peerFor(replica).stall(fmt.Sprintf("sequence %d and everything after it is held in this node's log: %v", seq, refusal))
}

// refuseReplayToPlaintextPeer is the sender's check before it ships a replay:
// the log replay a lagging peer is given is the history of every bucket it
// hosts, the creates included, so a plaintext peer the ring has made a replica
// would be handed the buckets whole. The sender tries again after every
// back-off and every wake, so the refusal is logged at a bounded rate and
// stalls the stream; it is returned so the stall's reason names the fix.
func (c *Cluster) refuseReplayToPlaintextPeer(member ring.Node, entries int) error {
	refusal := c.refuseKnownPlaintextPeer(member, fmt.Sprintf("replay %d logged entries of the buckets it hosts", entries))
	if refusal == nil {
		return nil
	}
	c.logPlaintextRefusal(member, refusal)
	return refusal
}

// errPlaintextPeerRefused marks, in the text a refusal crosses the wire as,
// that the node asked holds the data and will not give it to the asker. A
// puller tells it from "does not hold it" and from a failure, and asks the
// next member (Cluster.ensureMigrated).
var errPlaintextPeerRefused = errors.New("pull refused: the pulling node is not known to be encrypted")

// refuseToAnswerPeer is the check on everything a sealed node answers with a
// bucket's data: a pull, a versioned read, a request a coordinator forwards.
// The requester's claim rides the request, so it is the word of the process
// asking, now — there is no record of it to go stale. A requester that names
// no one or claims nothing runs a version from before the claim.
func (c *Cluster) refuseToAnswerPeer(fromNode string, claim storeClaim, action, alternative string) error {
	if !c.s.storeSealed() || claim == claimSealed {
		return nil
	}
	if fromNode == "" {
		return fmt.Errorf("refusing to %s: the requesting node did not say who it is or whether its store is encrypted — "+
			"it runs a version from before this check — and this node's store is encrypted (encryption.mode=%s), "+
			"so the data could be written to that node's disk in the clear. "+
			"%sUpgrade that node to this version: an encrypted node then says so. Nothing was sent",
			action, c.s.config.Encryption.Mode, alternative)
	}
	requester, member := c.memberByID(fromNode)
	if !member {
		requester = ring.Node{ID: fromNode}
	}
	refusal := &plaintextPeerError{
		peer: requester, known: peerKnowledge{claim: claim, answered: true},
		action: action, mode: c.s.config.Encryption.Mode, alternative: alternative,
	}
	if member {
		// It is the requester's own word, so it is recorded like a gossip claim.
		c.recordPeerStore(requester, claim)
		c.logPlaintextRefusal(requester, refusal)
	}
	return refusal
}

// refuseSnapshotForPlaintextTarget is the source's check when a node asks to
// pull a bucket (MIGRATE_START) — the one door the operator's migrate, the
// rebalance sweep, a removal's hand-off, replica seeding and the load-time
// relocation all come through. The puller must be a member and must say its
// store is sealed.
func (c *Cluster) refuseSnapshotForPlaintextTarget(req MigrateStartRequest) error {
	if !c.s.storeSealed() {
		return nil
	}
	action := fmt.Sprintf("hand bucket %q in database %q to the node pulling it", req.Bucket, req.Database)
	if err := c.refuseToAnswerPeer(req.FromNode, req.Store, action, ""); err != nil {
		return fmt.Errorf("%w: %w", err, errPlaintextPeerRefused)
	}
	if _, member := c.memberByID(req.FromNode); !member {
		return fmt.Errorf("refusing to %s: %s is not a member of this cluster, and this node's store is encrypted (encryption.mode=%s). "+
			"Add the node with 'join' first. Nothing was sent: %w",
			action, req.FromNode, c.s.config.Encryption.Mode, errPlaintextPeerRefused)
	}
	return nil
}

// refuseForwardedFromPlaintextCoordinator is the check on a bucket-scoped
// request a coordinator forwards to this node (ReplPushRequest.ReturnResponse):
// the answer goes to the coordinator, and a read's or a dump's answer is the
// bucket's data. A sealed node serves its buckets through a sealed coordinator
// or to a client connected to it, and tells the client which node that is. The
// rule is on the request, not on a list of opcodes whose answers carry data,
// so a new opcode cannot be left off it. A bucket drop is served: it carries
// names only, and it is how a bucket is removed whichever node the operator is
// connected to.
func (c *Cluster) refuseForwardedFromPlaintextCoordinator(req ReplPushRequest, entry ReplEntry) error {
	if !req.ReturnResponse || entry.Bucket == "" || transport.Opcode(entry.Op) == transport.OpDeleteBucket {
		return nil
	}
	action := fmt.Sprintf("serve %s for bucket %q through the coordinating node", transport.Opcode(entry.Op), entry.Bucket)
	alternative := fmt.Sprintf("Connect the client to an encrypted node instead — this one is %s. Otherwise: ", describePeer(c.self))
	return c.refuseToAnswerPeer(req.FromNode, req.Store, action, alternative)
}

// joinRefusal is encryption.mode=require refusing a peer at join.
func joinRefusal(peer ring.Node, known peerKnowledge, action string) error {
	return fmt.Errorf("refusing to %s: %s, and this node requires every cluster member to encrypt its store (encryption.mode=require). "+
		"%s, then try again; or set encryption.mode=on on this node to admit it with a warning "+
		"(a node in that mode still never places data on a peer that is not known to be encrypted)",
		action, peerStoreState(peer, known), peerStoreFix(peer, known, "encryption.mode=on or require"))
}

// refuseMembershipWithPlaintextPeer is the require rule for a member this node
// exchanges membership with: a proposer, or a gossip partner. It answers from
// what the peer has said, asking it when it has said nothing.
func (c *Cluster) refuseMembershipWithPlaintextPeer(ctx context.Context, member ring.Node, action string) error {
	if !c.s.requiresSealedPeers() || member.ID == c.self.ID {
		return nil
	}
	known := c.peerStore(ctx, member)
	if known.sealed() {
		return nil
	}
	return joinRefusal(member, known, action)
}
