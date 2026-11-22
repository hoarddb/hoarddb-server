package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// This file is the privilege model from spec 4. It is a table, not a habit: a
// single map from opcode to (action, scope), consulted once before dispatch.
// A handler never re-checks, and nothing is enforced twice.

// Action is the closed set of privileges. It is public API the moment an
// operator types one of the role names.
type Action uint8

const (
	// ActionNone requires no privilege beyond a valid AUTH.
	ActionNone Action = iota
	// ActionRead is read access to a database.
	ActionRead
	// ActionWrite is read plus mutation.
	ActionWrite
	// ActionDBAdmin covers bucket and database administration.
	ActionDBAdmin
	// ActionUserAdmin covers user CRUD on the store.
	ActionUserAdmin
	// ActionClusterAdmin covers node, rebalance and migration operations.
	ActionClusterAdmin
	// ActionBackup covers dump and backup.
	ActionBackup
	// ActionRestore covers restore and import.
	ActionRestore
)

// String returns the privilege name used in errors and audit entries.
func (a Action) String() string {
	switch a {
	case ActionRead:
		return roleReader
	case ActionWrite:
		return roleWriter
	case ActionDBAdmin:
		return roleDBManager
	case ActionUserAdmin:
		return roleUserManager
	case ActionClusterAdmin:
		return roleClusterManager
	case ActionBackup:
		return roleBackupOperator
	case ActionRestore:
		return roleRestoreOperator
	default:
		return "none"
	}
}

// ScopeKind says which part of a frame a rule's resource comes from.
type ScopeKind uint8

const (
	// ScopeNone means the operation is not scoped to a database or bucket.
	ScopeNone ScopeKind = iota
	// ScopeDatabase takes the resource from the frame's database (or, for
	// database DDL, its name).
	ScopeDatabase
	// ScopeBucket takes the resource from the frame's bucket.
	ScopeBucket
)

// OpRule is one opcode's authorization requirement.
type OpRule struct {
	Action Action
	Scope  ScopeKind
}

// opRules is the authorization table. Default deny: an opcode absent here is
// refused with INTERNAL, so a new handler that forgets to declare its
// privilege fails closed. TestEveryOpcodeHasARule parses transport/opcodes.go
// and fails the build when a constant is missing, so the table cannot drift.
//
// The map keys the whole closed set of opcodes: auth, heartbeats and
// observability carry ActionNone explicitly rather than being exempted
// silently.
var opRules = map[transport.Opcode]OpRule{
	// Authentication flow.
	transport.OpAuth:     {ActionNone, ScopeNone},
	transport.OpAuthOK:   {ActionNone, ScopeNone},
	transport.OpAuthFail: {ActionNone, ScopeNone},

	// Hash store.
	transport.OpPut:            {ActionWrite, ScopeDatabase},
	transport.OpUpsert:         {ActionWrite, ScopeDatabase},
	transport.OpUpsertOK:       {ActionNone, ScopeNone},
	transport.OpGet:            {ActionRead, ScopeDatabase},
	transport.OpGetResponse:    {ActionNone, ScopeNone},
	transport.OpDelete:         {ActionWrite, ScopeDatabase},
	transport.OpExists:         {ActionRead, ScopeDatabase},
	transport.OpExistsResponse: {ActionNone, ScopeNone},

	// Batch.
	transport.OpBatchPut:      {ActionWrite, ScopeDatabase},
	transport.OpBatchPutOK:    {ActionNone, ScopeNone},
	transport.OpBatchUpsert:   {ActionWrite, ScopeDatabase},
	transport.OpBatchUpsertOK: {ActionNone, ScopeNone},
	// A BATCH envelope names no database: each item is authorized on its own,
	// against its own database, before any item runs (batch-request spec
	// §4.2). A scoped row here would refuse the envelope at the gate.
	transport.OpBatch:   {ActionNone, ScopeNone},
	transport.OpBatchOK: {ActionNone, ScopeNone},

	// FIFO.
	transport.OpFIFOPush:        {ActionWrite, ScopeDatabase},
	transport.OpFIFOPop:         {ActionWrite, ScopeDatabase},
	transport.OpFIFOPopResponse: {ActionNone, ScopeNone},
	transport.OpFIFOPeek:        {ActionRead, ScopeDatabase},

	// Leases on a queue or heap bucket: each moves entries, so each is a write.
	transport.OpQueueClaim:   {ActionWrite, ScopeDatabase},
	transport.OpQueueAck:     {ActionWrite, ScopeDatabase},
	transport.OpQueueRelease: {ActionWrite, ScopeDatabase},
	transport.OpQueueExtend:  {ActionWrite, ScopeDatabase},

	// The dead list: reading it is a read, moving entries a write. DEAD_RESTORE
	// holds the restore action but a client frame carrying it is refused before
	// this gate (NewHandler), so the row only names the rule.
	transport.OpDeadList:    {ActionRead, ScopeDatabase},
	transport.OpDeadRequeue: {ActionWrite, ScopeDatabase},
	transport.OpDeadDiscard: {ActionWrite, ScopeDatabase},
	transport.OpDeadRestore: {ActionRestore, ScopeDatabase},

	// Field stores (nested buckets): a field write is a write to its bucket,
	// a field read a read of it.
	transport.OpFieldPush:   {ActionWrite, ScopeDatabase},
	transport.OpFieldPop:    {ActionWrite, ScopeDatabase},
	transport.OpFieldPeek:   {ActionRead, ScopeDatabase},
	transport.OpFieldPut:    {ActionWrite, ScopeDatabase},
	transport.OpFieldGet:    {ActionRead, ScopeDatabase},
	transport.OpFieldDelete: {ActionWrite, ScopeDatabase},
	transport.OpFieldLen:    {ActionRead, ScopeDatabase},
	transport.OpFieldList:   {ActionRead, ScopeDatabase},

	// LIFO.
	transport.OpLIFOPush:        {ActionWrite, ScopeDatabase},
	transport.OpLIFOPop:         {ActionWrite, ScopeDatabase},
	transport.OpLIFOPopResponse: {ActionNone, ScopeNone},
	transport.OpLIFOPeek:        {ActionRead, ScopeDatabase},

	// B+tree.
	transport.OpBTreeInsert:   {ActionWrite, ScopeDatabase},
	transport.OpBTreeLookup:   {ActionRead, ScopeDatabase},
	transport.OpBTreeRange:    {ActionRead, ScopeDatabase},
	transport.OpBTreeResponse: {ActionNone, ScopeNone},
	transport.OpBTreeFirst:    {ActionRead, ScopeDatabase},
	transport.OpBTreeLast:     {ActionRead, ScopeDatabase},

	// Heap.
	transport.OpHeapPush:        {ActionWrite, ScopeDatabase},
	transport.OpHeapPop:         {ActionWrite, ScopeDatabase},
	transport.OpHeapPopResponse: {ActionNone, ScopeNone},

	// Blob.
	transport.OpBlobWrite:        {ActionWrite, ScopeDatabase},
	transport.OpBlobRead:         {ActionRead, ScopeDatabase},
	transport.OpBlobReadResponse: {ActionNone, ScopeNone},

	// Database and bucket management.
	transport.OpCreateDB:     {ActionDBAdmin, ScopeDatabase},
	transport.OpCreateBucket: {ActionDBAdmin, ScopeDatabase},
	// Declaring store fields is bucket DDL, ruled as a create is.
	transport.OpDeclareStoreFields: {ActionDBAdmin, ScopeDatabase},
	// Altering a bucket's replication factor is bucket DDL, ruled as a create
	// is (bucket-replication-rev8-spec §4.4).
	transport.OpAlterBucket:  {ActionDBAdmin, ScopeDatabase},
	transport.OpDeleteDB:     {ActionDBAdmin, ScopeDatabase},
	transport.OpDeleteBucket: {ActionDBAdmin, ScopeDatabase},
	transport.OpListBuckets:  {ActionRead, ScopeDatabase},
	transport.OpListDBs:      {ActionNone, ScopeNone},
	transport.OpBucketStats:  {ActionRead, ScopeDatabase},
	transport.OpBucketInfo:   {ActionRead, ScopeDatabase},

	// Document and query.
	transport.OpCount:           {ActionRead, ScopeDatabase},
	transport.OpDistinct:        {ActionRead, ScopeDatabase},
	transport.OpUpdate:          {ActionWrite, ScopeDatabase},
	transport.OpSearch:          {ActionRead, ScopeDatabase},
	transport.OpScan:            {ActionRead, ScopeDatabase},
	transport.OpGetFast:         {ActionRead, ScopeDatabase},
	transport.OpGetFastResponse: {ActionNone, ScopeNone},

	// Cluster: the operator-facing mutations (add, remove, migrate) require
	// cluster-manager and are dispatched below the gate so that this table is
	// what decides them. The node-to-node opcodes are dispatched above it and
	// carry ActionNone: the driver listener refuses them by name and their
	// handlers check the cluster token. OpClusterPropose is one of those; its
	// cluster-manager entry is not what protects it.
	transport.OpGossip:       {ActionNone, ScopeNone},
	transport.OpReplPush:     {ActionNone, ScopeNone},
	transport.OpReplAck:      {ActionNone, ScopeNone},
	transport.OpMigrateStart: {ActionNone, ScopeNone},
	transport.OpMigrateData:  {ActionNone, ScopeNone},
	transport.OpMigrateDone:  {ActionNone, ScopeNone},
	// The control probe is node-to-node, like gossip: dispatched above the gate.
	transport.OpClusterProbe:      {ActionNone, ScopeNone},
	transport.OpClusterProbeReply: {ActionNone, ScopeNone},
	// The ring opcodes are served by wire.NewRingHandler, which authorizes every
	// request itself (it resolves the bucket id first, so this table cannot).
	// They carry ActionNone so the legacy gate does not stand in for that check;
	// the legacy handler has no case for them. RING_ADMIN is the same: its commands
	// name the database in the body, and ringAction maps each to the legacy create
	// and read opcodes that do the same thing.
	transport.OpRingList:       {ActionNone, ScopeNone},
	transport.OpRingCount:      {ActionNone, ScopeNone},
	transport.OpRingKeyed:      {ActionNone, ScopeNone},
	transport.OpRingBatch:      {ActionNone, ScopeNone},
	transport.OpRingGetMany:    {ActionNone, ScopeNone},
	transport.OpRingAdmin:      {ActionNone, ScopeNone},
	transport.OpRingTopology:   {ActionNone, ScopeNone},
	transport.OpRingReply:      {ActionNone, ScopeNone},
	transport.OpClusterPropose: {ActionClusterAdmin, ScopeNone},
	transport.OpClusterAdd:     {ActionClusterAdmin, ScopeNone},
	transport.OpMigrate:        {ActionClusterAdmin, ScopeNone},
	transport.OpClusterRemove:  {ActionClusterAdmin, ScopeNone},
	transport.OpConsistentGet:  {ActionRead, ScopeDatabase},
	transport.OpVersionedGet:   {ActionNone, ScopeNone},

	transport.OpMembershipChange: {ActionNone, ScopeNone},

	// Dump and restore (dump-restore-spec.md §10). ScopeDatabase because every
	// frame in both sessions names the database and bucket it acts on, like
	// the other bucket-level operations above.
	//
	// These are the two actions Action was extended for, but §10 also ships a
	// stricter gate: each dump/restore handler calls requireRootIdentity
	// (server/dump.go), so root-only is what actually holds today. A client
	// holding the backup or restore role passes this table and is then refused
	// by the handler, which fails closed. Consolidating the two layers — an
	// OpRule that says root-only, or dropping the handler gate so the roles
	// become usable — is a decision for dump-restore-spec.md §10, not a merge
	// decision. Until it is made, the handler gate is the one that binds and
	// must not be removed on the strength of this table.
	transport.OpDumpStart:    {ActionBackup, ScopeDatabase},
	transport.OpDumpChunk:    {ActionBackup, ScopeDatabase},
	transport.OpDumpDone:     {ActionBackup, ScopeDatabase},
	transport.OpRestoreStart: {ActionRestore, ScopeDatabase},
	transport.OpRestoreChunk: {ActionRestore, ScopeDatabase},
	transport.OpRestoreDone:  {ActionRestore, ScopeDatabase},

	// Heartbeat and observability.
	transport.OpPing:        {ActionNone, ScopeNone},
	transport.OpPong:        {ActionNone, ScopeNone},
	transport.OpError:       {ActionNone, ScopeNone},
	transport.OpHealthCheck: {ActionNone, ScopeNone},
	// The node list names every member's address, so it is for the role that
	// runs the cluster; 'status' (OP_HEALTH_CHECK) stays open to every user.
	transport.OpListNodes: {ActionClusterAdmin, ScopeNone},

	// Bulk streaming: defined but unimplemented, so no privilege is granted
	// beyond AUTH; the handler answers "unsupported opcode".
	transport.OpStreamOpen: {ActionNone, ScopeNone},
	transport.OpStreamData: {ActionNone, ScopeNone},
	transport.OpStreamEnd:  {ActionNone, ScopeNone},
	transport.OpStreamAck:  {ActionNone, ScopeNone},

	// User management. The store is global in v1, so these are ScopeNone.
	transport.OpUserCreate: {ActionUserAdmin, ScopeNone},
	transport.OpUserAlter:  {ActionUserAdmin, ScopeNone},
	transport.OpUserDrop:   {ActionUserAdmin, ScopeNone},
	transport.OpUserList:   {ActionUserAdmin, ScopeNone},
	transport.OpUserGet:    {ActionUserAdmin, ScopeNone},
	transport.OpUserGrant:  {ActionUserAdmin, ScopeNone},
	transport.OpUserRevoke: {ActionUserAdmin, ScopeNone},

	// Auth-config replication is node-only.
	transport.OpAuthConfigPush: {ActionNone, ScopeNone},
	transport.OpAuthConfigAck:  {ActionNone, ScopeNone},
}

// authzDenialInterval is how often a repeated denial is reported in full. In
// between, repeats increment a counter and are summarised on the next full
// line, so a misconfigured client in a retry loop cannot flood the audit log
// with the cluster's authorization layout (spec 4 rule 5).
const authzDenialInterval = 60 * time.Second

// denialKey is the dedup key: one full report per (user, action, resource)
// per interval.
type denialKey struct {
	User     string
	Action   Action
	Resource string
}

// denialState tracks the last full report and the suppressed repeats.
type denialState struct {
	LastFull time.Time
	Count    int
}

// denialTracker batches repeated authorization denials.
type denialTracker struct {
	mu      sync.Mutex
	entries map[denialKey]denialState
}

// denialTrackerMaxEntries bounds the tracker. With keys cut to denialKeyBytes per string
// (two strings per key) and a 40-byte value, the map holds at most 4096 x (256 + 40 + map
// overhead), about 1.5 MB, however many distinct resources clients name.
const denialTrackerMaxEntries = 4096

// denialKeyBytes is how much of a user and of a resource the dedup key keeps. The audit
// line still carries the full strings; two resources sharing their first 128 bytes share one
// report per interval, which is the cheaper way to be wrong.
const denialKeyBytes = 128

// boundedDenialKey cuts the key's strings to denialKeyBytes.
func boundedDenialKey(key denialKey) denialKey {
	key.User = key.User[:min(len(key.User), denialKeyBytes)]
	key.Resource = key.Resource[:min(len(key.Resource), denialKeyBytes)]
	return key
}

// shouldEmit reports whether a full denial line is due, and how many repeats
// were suppressed since the last one. It is called under the tracker's lock.
func (t *denialTracker) shouldEmit(key denialKey, now time.Time) (emit bool, suppressed int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key = boundedDenialKey(key)
	if t.entries == nil {
		t.entries = make(map[denialKey]denialState)
	}
	state, ok := t.entries[key]
	if !ok || now.Sub(state.LastFull) >= authzDenialInterval {
		if !ok && len(t.entries) >= denialTrackerMaxEntries {
			t.makeRoom(now)
		}
		t.entries[key] = denialState{LastFull: now}
		return true, state.Count
	}
	state.Count++
	t.entries[key] = state
	return false, 0
}

// makeRoom frees space when the tracker is full. It drops the entries whose interval has
// passed and the ones denied only once (a flood of one-off resources is exactly that), and
// keeps the keys that repeated inside their interval, which are the ones the dedup exists
// for. One pass frees most of the map, so the cost is O(1) amortized per new key. If every
// entry is a live repeat it drops one arbitrary entry: that key reports once more.
func (t *denialTracker) makeRoom(now time.Time) {
	for key, state := range t.entries {
		if state.Count == 0 || now.Sub(state.LastFull) >= authzDenialInterval {
			delete(t.entries, key)
		}
	}
	for key := range t.entries {
		if len(t.entries) < denialTrackerMaxEntries {
			return
		}
		delete(t.entries, key)
	}
}

// errNotAClusterMember refuses a frame on the internode listener from a
// session that did not authenticate as a cluster member.
var errNotAClusterMember error = &sentencedError{
	message: "permission denied: the internode listener serves cluster members only; " +
		"connect to the driver port (HOARDDB_TCP_LISTEN, 7433 by default) with a user account",
	cause: ErrPermissionDenied,
}

// errMemberOpcodeNotSent refuses a gated frame on a cluster member's session
// whose opcode members do not send as themselves (memberGatedOpcodes).
var errMemberOpcodeNotSent error = &sentencedError{
	message: "permission denied: a cluster member's internode session may send only the node-to-node opcodes; " +
		"this opcode is a client request, which a follower forwards inside a replication push, never raw",
	cause: ErrPermissionDenied,
}

// memberGatedOpcodes is the closed set of opcodes that reach authorize on a
// cluster member's session and are admitted without opRules: the ones a member
// sends as itself over the internode connection. Every other member frame is
// dispatched above the gate (server/dispatch.go) or is ActionNone. OpMigrate
// is the only one today (server/migrate.go:askNodeToMigrate); a client frame a
// follower forwards travels inside OpReplPush instead.
var memberGatedOpcodes = map[transport.Opcode]struct{}{
	transport.OpMigrate: {},
}

// clusterMemberSession reports whether sess is a cluster member's internode
// connection: it arrived on the internode listener and authenticated as the
// node identity, which takes the cluster token. The listener alone is not
// enough. The exemption exists because a coordinator authorizes the original
// client before it forwards, and only a member forwards; a session that is
// merely on the port has been authorized by nobody.
func clusterMemberSession(sess *transport.Session) bool {
	if sess == nil || !sess.Peer {
		return false
	}
	identity, ok := sess.Identity()
	return ok && identity.Username == NodeUser
}

// authorize applies opRules to one authenticated frame. A cluster-member
// session (clusterMemberSession: arrived on the internode listener and
// authenticated as NodeUser) is exempt for memberGatedOpcodes only. A client
// frame a follower forwards is not sent raw as OpPut or OpGet: Cluster.forward
// (server/write_path.go) wraps it as an entry inside an OpReplPush request
// (ReplPushRequest), and OpReplPush is dispatched above this gate by
// server/dispatch.go. The only gated opcode a member sends as itself is
// OpMigrate (server/migrate.go:askNodeToMigrate). Members are trusted; a
// compromised member can act as any user on the owner of a forwarded write,
// and carrying the acting user's identity on forwarded frames is v2
// (spec 4 rule 3).
func (s *Server) authorize(ctx context.Context, frame *transport.Frame) error {
	sess, ok := transport.SessionFromContext(ctx)
	if !ok || sess == nil {
		// A direct handler call with no session (tests) is not a client
		// frame; there is no identity to authorize and no transport to
		// refuse on.
		return nil
	}
	if clusterMemberSession(sess) {
		if _, sentByMembers := memberGatedOpcodes[frame.Opcode]; sentByMembers {
			return nil
		}
	}

	rule, ok := opRules[frame.Opcode]
	if !ok {
		s.logger.Error("no authorization rule for opcode; refusing (default deny)", "opcode", frame.Opcode)
		return fmt.Errorf("no authorization rule is declared for opcode 0x%02X %q; refusing the frame (default deny)",
			uint8(frame.Opcode), frame.Opcode.String())
	}
	if rule.Action == ActionNone {
		return nil
	}
	if sess.Peer {
		// On the internode listener and not a member: the AUTH frame that
		// would make it one passed above as ActionNone; anything else is
		// refused whatever account the session holds.
		if clusterMemberSession(sess) {
			return errMemberOpcodeNotSent
		}
		return errNotAClusterMember
	}

	identity, ok := sess.Identity()
	if !ok {
		return &sentencedError{message: "permission denied: the connection has no authenticated identity", cause: ErrPermissionDenied}
	}
	resource, err := ruleResource(frame, rule)
	if err != nil {
		return err
	}
	if identityAllows(identity, rule.Action, resource) {
		return nil
	}
	s.recordDenial(identity.Username, rule.Action, resource, frame.Opcode, identity.Grants)
	return &sentencedError{
		message: fmt.Sprintf("permission denied: user %q lacks %q on %q\n       %s's roles: %s\n       ask an administrator to run: grant role %s@%s to %s",
			identity.Username, rule.Action.String(), displayResource(resource), identity.Username, renderGrants(identity.Grants),
			rule.Action.String(), displayResource(resource), identity.Username),
		cause: ErrPermissionDenied,
	}
}

// unscopedOpcodes are the node-wide cluster operations. Cluster roles are cluster-wide (owner
// ruling, 2026-11-01): they need the unscoped grant (cluster-manager@* or root@*), and a grant on
// a named database is denied, whatever the rule's ScopeNone says about the frame.
// unscopedResource is the resource such an operation is asked on: databaseMatches is true for it
// only when the grant is on "*".
const unscopedResource = "*"

var unscopedOpcodes = map[transport.Opcode]struct{}{
	transport.OpClusterAdd:    {},
	transport.OpClusterRemove: {},
}

// ruleResource derives the resource a rule is checked against. Rule 4: an op
// whose frame lacks the field it needs is refused, never defaulted to "*".
// A payload the gate cannot decode is refused too. It used to be passed
// through as an empty resource, which databaseMatches treats as covered by
// any grant, on the assumption that the handler would fail the same decode —
// but the handler decodes a different struct, so a field only frameMeta
// reads (a non-string write_concern) let a one-database user into every
// database.
func ruleResource(frame *transport.Frame, rule OpRule) (string, error) {
	if rule.Scope == ScopeNone {
		if _, unscoped := unscopedOpcodes[frame.Opcode]; unscoped {
			return unscopedResource, nil
		}
		return "", nil
	}
	bucket, database, err := frameScope(frame)
	if err != nil {
		return "", fmt.Errorf("the %q request's payload could not be read to find the %s it names (%w); refusing rather than checking it as unscoped. %s",
			frame.Opcode.String(), scopeName(rule.Scope), err, payloadAdvice(frame.Opcode))
	}
	resource := database
	if rule.Scope == ScopeBucket {
		resource = bucket
	}
	if resource == "" {
		return "", fmt.Errorf("the %q request does not name a %s; refusing rather than assuming access to every one. Set the request's %s field to the one you mean (in the CLI: 'use <database>' first, or add 'in <database>' to the statement)",
			frame.Opcode.String(), scopeName(rule.Scope), scopeName(rule.Scope))
	}
	return resource, nil
}

// frameScope reads the bucket and database a scoped frame names, in the
// encoding its opcode's handler reads.
//
// GET_FAST is the one scoped opcode whose payload is not BSON, and it is
// decoded here by the function handleGetFast calls, so the gate checks the
// database the handler will read. Reading it as BSON refused every GET_FAST
// and, worse, authorized a BSON-shaped one against a database the
// handler never saw. A scoped opcode added with another payload encoding
// needs its own case here: without one it fails closed, as GET_FAST did.
func frameScope(frame *transport.Frame) (bucket, database string, err error) {
	var meta frameMeta
	if !hasWireLayoutPayload(frame.Opcode) && len(frame.Payload) > 0 {
		meta, err = decodeFrameMetaErr("frameScope", frame.Opcode, frame.Payload)
		if err != nil {
			return "", "", err
		}
	}
	return scopeOfFrame(frame, meta)
}

// hasWireLayoutPayload reports whether op's payload is a compact binary layout
// of its own rather than BSON. Such a payload has no frameMeta to decode.
func hasWireLayoutPayload(op transport.Opcode) bool {
	return op == transport.OpGetFast
}

// scopeOfFrame is the one reading of the bucket and database a client frame
// names, shared by the authorization gate and the cluster's router so that
// they cannot disagree about a payload. meta is
// the frame's BSON reading, which it ignores for an opcode with a layout of its
// own. An opcode added with another payload encoding needs a case here; without
// one the gate fails closed and the router would run the frame locally, which
// TestGateAndRouterAgreeOnTheScopeOfEveryScopedOpcode refuses.
//
// An opcode whose rule is ScopeNone names no scope, whatever fields its payload
// carries: the gate checks it as unscoped, so the router must not place it by a
// bucket either. TestUnscopedOpcodesNameNoScope holds that for every rule.
func scopeOfFrame(frame *transport.Frame, meta frameMeta) (bucket, database string, err error) {
	if rule, ok := opRules[frame.Opcode]; ok && rule.Scope == ScopeNone {
		return "", "", nil
	}
	if frame.Opcode == transport.OpGetFast {
		getFast, err := transport.UnmarshalGetFast(frame.Payload)
		if err != nil {
			return "", "", err
		}
		return getFast.Bucket, getFast.Database, nil
	}
	bucket, database = meta.scope(frame.Opcode)
	return bucket, database, nil
}

// payloadAdvice is the fix for a payload frameScope could not read: what the
// opcode's payload has to look like.
func payloadAdvice(op transport.Opcode) string {
	if op == transport.OpGetFast {
		return "GET_FAST's payload is not BSON: send a 1-byte database length and the database, a 1-byte bucket length and the bucket, " +
			"then a 2-byte little-endian key length and the key, or send the same read as a GET"
	}
	return "Send database, bucket, name and write_concern as strings"
}

// scopeName names a scope for an error message.
func scopeName(scope ScopeKind) string {
	if scope == ScopeBucket {
		return "bucket"
	}
	return "database"
}

// identityAllows reports whether the identity's scoped grant set satisfies an
// action on a resource.
//
// It requires one grant whose database matches the resource and whose role
// satisfies the action, so a role held in one database cannot satisfy another
// . An identity with no scoped grants fails closed: there is no
// fallback to the flat role/database lists, because that fallback is how the
// cross-database leak comes back.
func identityAllows(identity transport.Identity, action Action, resource string) bool {
	for _, grant := range identity.Grants {
		if !databaseMatches(grant.DB, resource) {
			continue
		}
		if roleSatisfies(grant.Role, action) {
			return true
		}
	}
	return false
}

// databaseMatches reports whether a grant's database covers the resource. A
// grant of "*" covers every database; an empty resource (a ScopeNone action)
// is covered by any grant, and the role still has to satisfy the action.
func databaseMatches(grantDB, resource string) bool {
	if resource == "" {
		return true
	}
	return grantDB == "*" || grantDB == resource
}

// roleSatisfies reports whether a role grants an action. A retired role name
// is an unknown role and grants nothing.
func roleSatisfies(role string, action Action) bool {
	switch role {
	case roleRoot:
		return true
	case roleReader:
		return action == ActionRead
	case roleWriter:
		return action == ActionRead || action == ActionWrite
	case roleDBManager:
		return action == ActionRead || action == ActionWrite || action == ActionDBAdmin
	case roleUserManager:
		return action == ActionUserAdmin
	case roleClusterManager:
		return action == ActionClusterAdmin
	case roleBackupOperator:
		// backup-operator is a de facto read-any-database (spec 3.1).
		return action == ActionBackup || action == ActionRead
	case roleRestoreOperator:
		return action == ActionRestore
	default:
		return false
	}
}

// sessionGrants reconstructs the typed, database-scoped grant set a session
// carries, from the record and the flat claims it presented. The wire claims
// stay flat for driver compatibility; this is where the pairing is restored.
//
// A grant survives iff its role is in the presented roles and its database is
// in the presented databases, with "*" honouring every value. A global record
// grant is bounded by the presented databases, so a narrowed token stays
// narrowed: a session can never hold a grant the presented claims did not
// carry.
func sessionGrants(record *UserRecord, presentedRoles, presentedDatabases []string) []transport.ScopedGrant {
	if record == nil {
		return nil
	}
	var grants []transport.ScopedGrant
	for _, grant := range record.Roles {
		if !valueInClaims(grant.Role, presentedRoles) {
			continue
		}
		for _, database := range effectiveDatabases(grant.DB, presentedDatabases) {
			grants = append(grants, transport.ScopedGrant{Role: grant.Role, DB: database})
		}
	}
	return grants
}

// valueInClaims reports whether value survives a presented flat claim list.
func valueInClaims(value string, presented []string) bool {
	for _, allowed := range presented {
		if allowed == "*" || allowed == value {
			return true
		}
	}
	return false
}

// effectiveDatabases is a grant's database intersected with the databases the
// claims presented. A global grant ("*") expands to the presented databases;
// a scoped grant survives only when the claims carry it.
func effectiveDatabases(grantDB string, presented []string) []string {
	if len(presented) == 0 {
		return nil
	}
	if grantDB != "*" {
		if valueInClaims(grantDB, presented) {
			return []string{grantDB}
		}
		return nil
	}
	seen := make(map[string]struct{}, len(presented))
	databases := make([]string, 0, len(presented))
	for _, database := range presented {
		if _, ok := seen[database]; ok {
			continue
		}
		seen[database] = struct{}{}
		databases = append(databases, database)
	}
	return databases
}

// recordDenial writes the full denial to the log and audit log at most once
// per (user, action, resource) per interval, and counts the rest. It prints
// the user's scoped grants, so the line matches spec 4 rule 5's example.
func (s *Server) recordDenial(user string, action Action, resource string, opcode transport.Opcode, grants []transport.ScopedGrant) {
	emit, suppressed := s.denials.shouldEmit(denialKey{User: user, Action: action, Resource: resource}, time.Now())
	if !emit {
		return
	}
	message := fmt.Sprintf("permission denied: user %q lacks %q on %q (required by %s)",
		user, action.String(), displayResource(resource), opcode.String())
	message += fmt.Sprintf("; %s's roles: %s", user, renderGrants(grants))
	message += fmt.Sprintf("; fix: grant role %s@%s to %s", action.String(), displayResource(resource), user)
	if suppressed > 0 {
		message += fmt.Sprintf(" [%d identical denials suppressed since the last report]", suppressed)
	}
	s.logger.Warn("authorization denied",
		"user", user, "action", action.String(), "resource", resource,
		"opcode", opcode.String(), "suppressed", suppressed)
	s.audit.Log(AuditEntry{Event: EventAuthzDenied, User: user, Target: "resource:" + resource, Result: message})
}

// renderGrants renders a scoped grant set the way spec 4 rule 5 prints it:
// writer@app, reader@logs. An identity with no grants says so rather than
// printing an empty list.
func renderGrants(grants []transport.ScopedGrant) string {
	if len(grants) == 0 {
		return "(none)"
	}
	roles := make([]string, 0, len(grants))
	for _, grant := range grants {
		roles = append(roles, grant.Role+"@"+grant.DB)
	}
	sort.Strings(roles)
	return strings.Join(roles, ", ")
}

// displayResource renders a scope for a message. A ScopeNone action has no
// resource; "*" says the privilege is store-wide rather than leaving an
// unusable `grant role user-manager@` in the fix.
func displayResource(resource string) string {
	if resource == "" {
		return "*"
	}
	return resource
}
