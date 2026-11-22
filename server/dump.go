// Package server — the dump session (the dump-restore-spec record).
//
// This file wires the read-only enumeration capability (store/enumerate.go)
// to the wire: OpDumpStart/OpDumpChunk/OpDumpDone. Restore's handler bodies
// (OpRestoreStart/Chunk/Done) land in a later commit; the opcodes already
// exist (transport/opcodes.go) and fall through dispatch's default case with
// a named "not implemented" error until then.
//
// Root gate. Until per-user permissions (opRules) exist, every dump/restore
// opcode is refused for any identity but the configured root user
// (dump-restore-spec.md §10, §11). requireRootIdentity is the one place that
// makes the check, so lifting it later is one edit — Task 3's restore
// handlers must call this same function rather than reimplementing it.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Sentinel errors for the dump path. Each is wrapped with fmt.Errorf so
// callers match with errors.Is rather than string comparison (AGENTS.md
// rule 4).
var (
	// ErrDumpRequiresRoot is returned for every dump/restore opcode when the
	// caller's identity is not the configured root user.
	ErrDumpRequiresRoot = errors.New("dump/restore requires the root user")
	// ErrDumpAlreadyRunning means a second OpDumpStart arrived while this
	// server already has a live dump (dump-restore-spec.md §10: one live
	// dump per server).
	ErrDumpAlreadyRunning = errors.New("a dump is already running on this server")
	// ErrDumpNeedsNewerClient means a bucket declares store fields and the
	// client did not say it writes them (DumpStartRequest.AcceptStores): the
	// dump is refused rather than streamed without its elements.
	ErrDumpNeedsNewerClient = errors.New("this dump needs a newer client")
	// ErrDumpUnreadableIndex means a bucket holds an index written by a newer
	// version of hoardDB, which this binary can neither describe in a manifest
	// nor restore: the dump is refused rather than lose the constraint.
	ErrDumpUnreadableIndex = errors.New("bucket holds an index this version cannot read")
	// ErrDumpNoActiveSession means OpDumpChunk/OpDumpDone named a bucket this
	// node has no open dump session for — including the case where cluster
	// membership moved bucket ownership mid-dump (dump-restore-spec.md
	// §10.1): the new owner never received OpDumpStart, so it has no pin and
	// no cursor for the bucket.
	ErrDumpNoActiveSession = errors.New("no active dump session")
	// ErrDumpDocumentTooLarge means a bucket holds a document too large to
	// fit in a single wire frame; OpDumpStart refuses the whole bucket
	// rather than send a truncated dump (dump-restore-spec.md §4).
	ErrDumpDocumentTooLarge = errors.New("document exceeds the maximum dumpable size")
	// ErrDumpPinCeilingExceeded means a queue bucket's compaction pin
	// outlived the configured ceiling and was force-released
	// (dump-restore-spec.md §16.3).
	ErrDumpPinCeilingExceeded = errors.New("dump compaction-pin ceiling exceeded")
	// ErrDumpDigestMismatch means the client's OpDumpDone digest disagrees
	// with the bytes this node actually streamed.
	ErrDumpDigestMismatch = errors.New("dump digest mismatch")
	// ErrDumpBucketBeingRestored means OpDumpStart named a bucket that a
	// restore currently holds a rebuild pin on (dump-restore-spec.md §9.7,
	// server/restore.go's bucketPin). Dump and restore never touch the same
	// bucket at once, in either direction.
	ErrDumpBucketBeingRestored = errors.New("bucket is being restored")
	// ErrDumpNodeStopping means the node began shutting down while a dump
	// was starting or streaming: its walk reads the node store,
	// which Stop closes, so the dump ends short and has to be run again.
	ErrDumpNodeStopping = errors.New("this node is shutting down")

	// errDumpCanceled unwinds the walk goroutine's IterateDump callbacks
	// when the session ends before the walk reaches the end of the bucket.
	// It is translated back to a nil walk error: the session ending on
	// purpose is not a read failure.
	errDumpCanceled = errors.New("dump session ended before the walk finished")
)

// defaultDumpPinCeiling is the ceiling on how long one dump of a queue bucket
// may run (dump-restore-spec.md §16.3, amended to 5 minutes) when the operator
// has not set config.Server.DumpPinCeilingSeconds (zero/unset in config.Load).
// The segment store's compaction pin it was named for is gone with the
// segment store (build doc Step 12): a dump now holds one store snapshot for
// its life, which keeps compaction from discarding the versions it reads (§5
// SA-20). The ceiling still bounds a queue dump as it did, and whether it
// should bound every dump, or none, is Step 19's (SA-20).
const defaultDumpPinCeiling = 5 * time.Minute

// dumpPinCeilingOverrideForTest, when non-zero, replaces the configured
// ceiling outright. config.Server.DumpPinCeilingSeconds is whole seconds
// (matching HOARDDB_LISTEN-style int config keys), too coarse for a test
// that wants to prove force-release without waiting real minutes; this
// follows the same test-hook pattern as server/auth.go's tokenDecodeHook.
// Nil/zero in production.
var dumpPinCeilingOverrideForTest time.Duration

// startDumpTestBarrier, when non-nil, is invoked once by startDump
// immediately after claimDump returns a session — the earliest possible
// instant s.dump is visible to another goroutine. A test uses this to force a
// concurrent OpDumpDone into exactly that window and prove the fix for the
// whole-branch review's Important #3: even at the earliest possible race
// point, releaseDump releases the session's slot and its walk exits. Nil in
// production.
var startDumpTestBarrier func()

// dumpWalkTestHook, when non-nil, is invoked by runDumpWalk before it opens
// its iterator. A test uses it to hold a walk that exists but has not yet
// read the store, the state the shutdown panic needs, and to tell
// whether a walk was launched at all. Atomic because a test that fails
// leaves its walk behind, still to read the hook when the test's cleanup
// clears it. Nil in production.
var dumpWalkTestHook atomic.Pointer[func()]

// dumpManifestFormatVersion is the format this server writes. It is stamped
// into every DumpBucketManifest, whatever the bucket declares: a bucket's index
// declaration (index_specs, GAP-14), its store fields (the manifest's stores
// and the dump's "stores" section, GAP-22) and its dead entries (the dump's
// "dead" section, claim-ack-spec §7.2) are sections a manifest carries or
// omits, not versions of their own.
//
// Format 3 is the first that can hold a "dead" section. An older server's
// restoreChunk treats every section that is not "stores" as documents, so it
// would push dead records back as live ones; refusing the version is what
// stops that (claim-ack-spec §7.3, owner 2026-10-15).
//
// There is no format 1 (owner ruling 2026-09-30): no release wrote one, so no
// dump exists that restore would have to keep reading. The ruling also keeps
// a hole closed — a format-1 manifest has no index_specs, so restoring one
// for a bucket with a unique index would declare the index without its
// uniqueness. Accepting an older version again reopens that.
const dumpManifestFormatVersion = 3

// restorableFormat reports whether this server restores a dump of format
// version: 2, which developers hold, and 3. It is a function, not a comparison
// at its one call site, so that the accepted set has a single owner.
func restorableFormat(version int) bool {
	return version == 2 || version == dumpManifestFormatVersion
}

// dumpSectionStores names the chunks of a dump that carry field-store
// element records rather than documents.
const dumpSectionStores = "stores"

// dumpSectionDead names the chunks that carry a queue's or heap's dead
// entries as dumpDeadRecord documents, never as documents of the bucket.
const dumpSectionDead = "dead"

// DumpIndexSpec is one index descriptor as a dump carries it: the declaration,
// without a state — a restored bucket starts ready and is indexed by the
// inserts that restore it.
type DumpIndexSpec struct {
	ID                 int32    `bson:"id" json:"id"`
	Name               string   `bson:"name" json:"name"`
	Fields             []string `bson:"fields" json:"fields"`
	Unique             bool     `bson:"unique,omitempty" json:"unique,omitempty"`
	ExpireAfterSeconds *int64   `bson:"expire_after_seconds,omitempty" json:"expire_after_seconds,omitempty"`
}

// dumpFrameOverhead is the headroom reserved under transport.MaxFrameLength
// for the BSON wrapping of one OpDumpChunk response and the frame header
// itself, so a document that "just barely" fits is not refused by an
// off-by-a-few-bytes miscalculation.
const dumpFrameOverhead = 64 * 1024

// maxDumpDocumentBytes is the largest single document OpDumpStart will
// agree to dump. A queue entry is bounded only by the node store's largest
// batch, not by the 16 MiB BSON convention, so this is a real limit to
// enforce, not a theoretical one (dump-restore-spec.md §4, §11).
const maxDumpDocumentBytes = transport.MaxFrameLength - dumpFrameOverhead

// requireRootIdentity is the single root-only gate for every dump and
// restore opcode. Until role enforcement (opRules) exists, any identity
// other than the configured root user is refused outright — see
// dump-restore-spec.md §10 ("refused for a non-root connection until roles
// exist") and §11's security table.
//
// A bucket-scoped dump/restore request forwarded to a different node
// (Cluster.forward, used by the owner-routing every one of these six
// opcodes takes — see replicationRequired in write_path.go) arrives on the
// owner over the internode connection. That connection's AUTH runs through
// server/dispatch.go:finishAuth, which records the node identity (NodeUser)
// on the session once the cluster token checks out, so the owner's ctx
// carries a Session with Peer set and the NodeUser identity.
// requireRootIdentity exempts it only when clusterMemberSession
// (server/authz.go) holds: arrived on the internode listener and
// authenticated as NodeUser. The exemption therefore belongs to the node
// identity, not the listener — a peer session with any other identity is
// refused with errNotAClusterMember. The owner still cannot re-derive the
// original client's identity from the forwarded frame, so the gate that
// matters runs once, at the coordinator, in Cluster.handleClientFrame
// (write_path.go), using the real client session before Cluster.forward
// wraps the frame in an OpReplPush entry. This function is what that
// coordinator-side check calls, and it is also what a standalone
// (non-cluster) node's direct dispatch and an owner serving its own bucket
// locally both call — in both of those cases ctx already carries the real
// client identity, so the check is meaningful there too.
func requireRootIdentity(ctx context.Context, s *Server) error {
	sess, ok := transport.SessionFromContext(ctx)
	if !ok || sess == nil {
		return fmt.Errorf("%w: no authenticated session on this connection", ErrDumpRequiresRoot)
	}
	if sess.Peer {
		// Internode hop: the coordinator that owns the original client
		// connection already enforced this gate, with the real identity,
		// before forwarding. Trust it rather than fail every cross-node
		// dump/restore for a reason that has nothing to do with who the
		// original caller was. Only a cluster member forwards, so the
		// trust is the node identity's and not the listener's.
		if clusterMemberSession(sess) {
			return nil
		}
		return fmt.Errorf("%w: %w", ErrDumpRequiresRoot, errNotAClusterMember)
	}
	identity, ok := sess.Identity()
	if !ok || identity.Username == "" {
		return fmt.Errorf("%w: connection is not authenticated", ErrDumpRequiresRoot)
	}
	root := s.config.Server.RootUser
	if root == "" || identity.Username != root {
		return fmt.Errorf("%w: connected as %q; dump and restore require the root user until per-user permissions land",
			ErrDumpRequiresRoot, identity.Username)
	}
	return nil
}

// isDumpOrRestoreOpcode reports whether op is one of the six dump/restore
// opcodes handled together for routing and root-gating purposes.
func isDumpOrRestoreOpcode(op transport.Opcode) bool {
	switch op {
	case transport.OpDumpStart, transport.OpDumpChunk, transport.OpDumpDone,
		transport.OpRestoreStart, transport.OpRestoreChunk, transport.OpRestoreDone:
		return true
	default:
		return false
	}
}

// isRestoreOpcode reports whether op is one of the three restore opcodes.
// RESTORE_START creates the bucket it writes into, so a restore is routed like
// a read only to keep it from travelling as one replicated entry, and "this
// node holds no copy yet" is its ordinary starting state.
func isRestoreOpcode(op transport.Opcode) bool {
	switch op {
	case transport.OpRestoreStart, transport.OpRestoreChunk, transport.OpRestoreDone:
		return true
	default:
		return false
	}
}

// DumpStartRequest asks the server to begin a dump session for one bucket.
// Scope beyond a single bucket (a whole database, or the whole
// installation) is a CLI-side concern: the CLI enumerates buckets
// (OpListDBs/OpListBuckets) and issues one OpDumpStart per bucket, so each
// wire request is bucket-scoped the same way a bucket-scoped read is
// (dump-restore-spec.md §10.1) and the existing owner-routing in
// serveRead/Cluster.forward delivers it to the node that actually holds the
// bucket.
type DumpStartRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	// AcceptStores says the client writes a dump's "stores" section. A bucket
	// that declares store fields is refused to a client that does not: a dump
	// that silently omitted every element would restore every post and none
	// of its comments, under a matching digest (nested buckets F-12).
	AcceptStores bool `bson:"accept_stores,omitempty"`
	// AcceptDead says the client writes a dump's "dead" section to a file of
	// its own. A bucket holding dead entries is refused to a client that does
	// not: a format-2 client appends every section it does not know to its
	// documents file, and a restore would push the dead entries back as live
	// ones (claim-ack build B-14).
	AcceptDead bool `bson:"accept_dead,omitempty"`
}

// DumpStore is one declared store field in a dump manifest: its name, its
// engine and, for an L3 store, the name of the L2 store it is declared on.
// Names, never ids: a restore declares the stores afresh and routes each
// element by its field's name, so no id can re-point an element at another
// field.
type DumpStore struct {
	Name   string `bson:"name" json:"name"`
	Engine string `bson:"engine" json:"engine"`
	Parent string `bson:"parent,omitempty" json:"parent,omitempty"`
}

// dumpElementRecord is one field-store element as a dump carries it, in the
// "stores" section: the field's name, the parent document's key, the
// element's key — its element key (hash, btree, blob), its position (fifo,
// lifo) or its priority and entry id (heap) — and the element document. A
// blobStore field's value is its blob's metadata and never its content, as a
// blob bucket's dump is.
type dumpElementRecord struct {
	Field string      `bson:"field"`
	Owner string      `bson:"owner"`
	Key   interface{} `bson:"key"`
	Value bson.Raw    `bson:"value"`
	// Path is an L3 element's L2 element: its field's name and its key, the
	// same kinds of key an element record's own key is. Absent at L2.
	Path []dumpPathStep `bson:"path,omitempty"`
}

// dumpPathStep is one ancestor of an L3 element in a dump record.
type dumpPathStep struct {
	Field string      `bson:"field"`
	Key   interface{} `bson:"key"`
}

// dumpDeadRecord is one dead entry as the "dead" section carries it
// (claim-ack-spec §7.2). Data is the entry's document, for a heap
// {_id_, priority, value}. There is no dead id: restore makes a fresh one.
type dumpDeadRecord struct {
	Data       bson.Raw  `bson:"data"`
	Deliveries int32     `bson:"deliveries"`
	DeadAt     time.Time `bson:"dead_at"`
	Reason     string    `bson:"reason"`
}

// dumpHeapKey is a heap field element's key in a dump record.
type dumpHeapKey struct {
	Priority int64  `bson:"priority"`
	ID       string `bson:"id"`
}

// DumpBucketManifest carries every metadata.json field (dump-restore-spec.md
// §4) the server can report without opening a second transaction on top of
// the one real walk. Bytes, Digest and Documents are deliberately absent
// here: an upfront DocCount() (or a separate size-probe pass) would be a
// second, independent Badger read transaction, and a document written
// between that transaction and the real walk's would make this count
// silently disagree with what OpDumpChunk actually streams — exactly the
// "documents count lied" bug class store.Enumerator's own doc comment and
// spec §19 C-3 exist to prevent, just relocated from a cached counter to a
// multi-pass dump session (fix-round-1 finding). The CLI computes Bytes and
// Digest itself as it writes the .bson file, and treats OpDumpDone's
// response — taken from the same walk this manifest's caller kicked off —
// as the authoritative Documents count for metadata.json.
type DumpBucketManifest struct {
	FormatVersion  int        `bson:"format_version"`
	HoardDBVersion string     `bson:"hoarddb_version"`
	ProtocolMajor  uint8      `bson:"protocol_major"`
	SourceNode     string     `bson:"source_node"`
	Database       string     `bson:"database"`
	Bucket         string     `bson:"bucket"`
	StoreType      BucketType `bson:"store_type"`
	// Indexes is each index's first field, sorted: the summary a reader of
	// the manifest sees. IndexSpecs is what a restore declares from.
	Indexes []string `bson:"indexes,omitempty"`
	// IndexSpecs is the declaration in id order, which is what restores a
	// unique index as unique and every id as its source's.
	IndexSpecs    []DumpIndexSpec `bson:"index_specs,omitempty"`
	BlobsIncluded bool            `bson:"blobs_included"`
	// Stores is the bucket's declared store fields, absent when it has none.
	Stores []DumpStore `bson:"stores,omitempty"`
	// ReplicationFactor is the factor the bucket declared at create, absent
	// for one that inherits the node's. Absent is what every dump written
	// before the field says, and it means inherit, so the format version
	// does not change (bucket-replication spec §17.4.10).
	ReplicationFactor int `bson:"replication_factor,omitempty"`
	// Placement is the hash bucket's recorded placement, "spread" or "whole",
	// so a restore creates the bucket as it was dumped: a unique index on a
	// whole bucket is refused on a spread one. Absent for every other type,
	// which is always whole. Adding it did not change the format version:
	// before v1.0 a dump without it is not a supported input (document
	// placement spec E28), and a restore of one takes the create's default.
	Placement string `bson:"placement,omitempty"`
}

// DumpStartResponse answers OpDumpStart.
type DumpStartResponse struct {
	Success  bool                `bson:"success"`
	Message  string              `bson:"message,omitempty"`
	Manifest *DumpBucketManifest `bson:"manifest,omitempty"`
}

// DumpChunkRequest asks for the next chunk of the named bucket's open dump.
type DumpChunkRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
}

// DumpChunkResponse carries up to transport.DumpChunkSize bytes of
// concatenated whole BSON documents — never a partial document — plus EOF
// once the bucket is exhausted.
type DumpChunkResponse struct {
	Success   bool   `bson:"success"`
	Message   string `bson:"message,omitempty"`
	Data      []byte `bson:"data,omitempty"`
	EOF       bool   `bson:"eof"`
	Documents int    `bson:"documents,omitempty"`
	// Section is "stores" for a chunk of field-store element records, "dead"
	// for one of dead entries, empty for documents. A chunk never mixes
	// sections. Documents come first, except that a heap's dead twins are sent
	// as the walk meets them.
	Section string `bson:"section,omitempty"`
}

// DumpDoneRequest acknowledges the bucket's digest and asks the server to
// release the session. Digest is the client's own SHA-256 of the .bson file
// it wrote, "sha256:<hex>"; an empty Digest skips the cross-check (used on
// an aborted or failed bucket, where there is nothing meaningful to check).
type DumpDoneRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Digest   string `bson:"digest,omitempty"`
	// StoresDigest is the client's SHA-256 of the "stores" section it wrote,
	// checked like Digest; empty skips the check.
	StoresDigest string `bson:"stores_digest,omitempty"`
	// DeadDigest is the client's SHA-256 of the "dead" section it wrote,
	// checked like Digest; empty skips the check.
	DeadDigest string `bson:"dead_digest,omitempty"`
}

// DumpDoneResponse reports what the server actually streamed, so the CLI can
// cross-check its own tally.
type DumpDoneResponse struct {
	Success   bool   `bson:"success"`
	Message   string `bson:"message,omitempty"`
	Bytes     int64  `bson:"bytes"`
	Documents int64  `bson:"documents"`
	Digest    string `bson:"digest,omitempty"`
	// Elements and StoresDigest are the "stores" section's count and digest.
	Elements     int64  `bson:"elements,omitempty"`
	StoresDigest string `bson:"stores_digest,omitempty"`
	// Dead and DeadDigest are the "dead" section's count and digest.
	Dead       int64  `bson:"dead,omitempty"`
	DeadDigest string `bson:"dead_digest,omitempty"`
}

// dumpItem is one document or element record on its way from the walk to a
// chunk.
type dumpItem struct {
	section string
	doc     []byte
}

// dumpSession is the node-local state for one bucket's live dump. Exactly
// one exists per server at a time (dump-restore-spec.md §10), held on
// Server.dump under Server.dumpMu.
type dumpSession struct {
	database   string
	bucket     string
	bucketType BucketType
	startedAt  time.Time
	startedBy  string
	pinCeiling time.Duration

	// setupMu guards pinTimer and watchCancel: each is written
	// once, from startDump/watchDumpConnection, but can legitimately be read
	// from a different goroutine that fires before the write is visible to
	// it by ordinary program order alone — the pin-ceiling timer's own
	// callback is the case that matters, since time.AfterFunc's callback
	// goroutine has no happens-before relationship with the assignment of
	// its own *time.Timer to session.pinTimer (caught by -race under
	// TestDumpPinCeilingForceReleases during self-review: a 50ms ceiling
	// "always" completes the assignment first in wall-clock time, which is
	// exactly the kind of accidentally-correct-today code AGENTS.md's
	// concurrency rule exists to catch before it isn't).
	setupMu  sync.Mutex
	pinTimer *time.Timer

	// docs carries one document's raw bytes at a time from the walk
	// goroutine to whichever OpDumpChunk call is currently pulling.
	// Buffered shallowly so the walk can run a little ahead of the wire
	// without holding the whole bucket in memory.
	docs chan dumpItem
	// walkErr carries the walk goroutine's final result exactly once, sent
	// before docs is closed — so a reader that has observed docs closed can
	// read walkErr without blocking.
	walkErr chan error
	// walkDone is closed when runDumpWalk returns. release blocks on it
	// (when non-nil — a session released before its walk ever started, e.g.
	// the oversized-document probe failing, has no walk to wait for) so
	// nothing can still be touching the bucket's storage engine after
	// release returns. Without this, a test (or a real caller) that closes
	// a bucket's engine right after releasing a dump can race a walk
	// goroutine that has not been scheduled yet into a "DB Closed" panic —
	// caught by TestDumpConcurrentRefused during review.
	walkDone chan struct{}
	// cancel unblocks the walk goroutine's pending send to docs promptly
	// when the session ends before the walk finished (an early OpDumpDone or
	// a dropped connection), so release never waits on a document nobody
	// will read.
	cancel     chan struct{}
	cancelOnce sync.Once

	// pullMu serialises OpDumpChunk calls against this session's pending
	// buffer. The wire contract is one request in flight per connection, but
	// this guards the (out-of-contract) case of two connections pulling the
	// same session concurrently rather than corrupting the byte stream.
	pullMu  sync.Mutex
	pending *dumpItem

	// digest accumulates SHA-256 over exactly the bytes handed to a
	// completed OpDumpChunk pull — the same bytes streamed over the wire —
	// so OpDumpDone can check the client's digest of the .bson file it wrote
	// against what this node actually sent. A digest here is corruption
	// detection, not authenticity (dump-restore-spec.md §4, §11): it says
	// the two ends agree on what was transferred, nothing about who wrote
	// it. Touched only by the walk goroutine.
	digest       hash.Hash
	bytesEmitted int64 // atomic
	docsEmitted  int64 // atomic; informational only, never the reported count
	// storesDigest and elementsEmitted are the "stores" section's, kept apart
	// so each file the client writes is checked against its own bytes.
	storesDigest    hash.Hash
	elementsEmitted int64 // atomic
	// deadDigest and deadEmitted are the "dead" section's, and acceptDead is
	// what the client said it writes; set before the walk starts.
	deadDigest  hash.Hash
	deadEmitted int64 // atomic
	acceptDead  bool

	// forcedErr is set when the session was torn down for a reason the
	// caller did not ask for (connection close, pin ceiling), so a request
	// that arrives just after release can be told why instead of a bare
	// "no active dump".
	forcedErr error

	// watchCancel is guarded by setupMu; see its doc comment above.
	watchCancel context.CancelFunc
	releaseOnce sync.Once
	stopOnce    sync.Once
}

// stopWalk cancels the walk (a no-op if it already reached end-of-bucket)
// and waits for runDumpWalk to fully return before this call returns —
// idempotent and safe to call more than once (release also calls it).
//
// dumpDone calls this explicitly, before reading session.digest, rather
// than relying on the deferred releaseDump to do it: a client that calls
// OpDumpDone without first draining every OpDumpChunk to EOF (an aborted or
// early-finished bucket — dump-restore-spec.md §16.4's "continues past an
// unreadable bucket" is exactly this from the CLI's side) would otherwise
// have session.digest.Sum read here while runDumpWalk's still-running
// callback is concurrently calling session.digest.Write for the next
// document — a real data race, not a theoretical one: caught by -race under
// TestTCPServerDumpKillConnectionReleasesPin during self-review, where the
// second dump's OpDumpDone followed OpDumpStart immediately with no
// OpDumpChunk calls at all.
func (session *dumpSession) stopWalk() {
	session.stopOnce.Do(func() {
		session.cancelOnce.Do(func() { close(session.cancel) })
		// walkDone is read under setupMu because startDump can still be
		// writing it concurrently: a dump session is reachable (via
		// activeDumpFor) the instant claimDump publishes s.dump, but
		// startDump does not create walkDone until just before launching
		// the walk goroutine, a few statements later on its own goroutine.
		// An OpDumpDone landing in exactly that window used to race this
		// read against that write with no synchronization at all — latent
		// in this code before this fix too, just never exercised by a test
		// adversarial enough to hit it until
		// TestDumpStartPublishesSessionOnlyAfterFullyWired's explicit
		// barrier found it under -race during self-review. Copying walkDone
		// out while holding the lock, then waiting on the copy outside it,
		// avoids holding setupMu for the (potentially long) duration of the
		// wait.
		session.setupMu.Lock()
		walkDone := session.walkDone
		session.setupMu.Unlock()
		if walkDone != nil {
			// Bounded: once cancel is closed, the walk's next select (or the
			// one after, if a document is already in flight) takes the
			// cancel branch instead of blocking on a send nobody will read
			// (docs is buffered 2, so at most two more documents are
			// processed first).
			<-walkDone
		}
	})
}

// claimDump reserves the server's single dump slot, or refuses naming the
// dump already running. Everything a racing release reads is written before
// s.dump is published, inside this one dumpMu critical section (whole-branch
// review, Important #3).
//
// session.walkDone is deliberately NOT created here — it stays created in
// startDump, immediately before the walk goroutine that will close it is
// launched (fix round 1 of this fix: an earlier version of this change
// created walkDone here too, which broke TestRestoreBucketPinsCoordinateWithDump
// and TestClaimDumpAndClaimBucketRestorePinMutuallyExclusive — both call
// claimDump directly, to test pin bookkeeping, without ever starting a walk
// goroutine; releaseDump's stopWalk then blocked forever on a walkDone
// nothing would ever close). An OpDumpDone that lands before startDump
// launches the walk closes session.cancel and finds no walk to wait for;
// launchDumpWalk sees cancel closed and launches none. This
// comment used to call that case safe because the late walk was "bounded"
// to two documents. Bounded is not awaited: nothing waited for that walk,
// its digest writes raced dumpDone's read, and Stop could close the store
// under it.
func (s *Server) claimDump(database, bucket string, bucketType BucketType, user string) (*dumpSession, error) {
	s.dumpMu.Lock()
	defer s.dumpMu.Unlock()
	if s.dump != nil {
		// A part's name is shown as its collection's: a spread collection's
		// dump claims this slot part by part, and its refusal reaches the
		// client of another collection's dump (spread MVP note, section 3).
		return nil, fmt.Errorf("%w: dump of %s/%s is already running on %s (started by %q at %s); wait for it to finish, or dump a different server",
			ErrDumpAlreadyRunning, s.dump.database, displayBucket(s.dump.bucket), dumpNodeLabel(s), s.dump.startedBy, s.dump.startedAt.Format(time.RFC3339))
	}
	// The other half of the bucket-pin axis (dump-restore-spec.md §9.7,
	// server/restore.go): a bucket being rebuilt by a restore is not safe to
	// dump either — a dump's read transaction against a bucket mid-drop, or
	// mid-recreate, would either see a half-rebuilt bucket or race the
	// recreate outright. Dump and restore are mutually exclusive on the same
	// bucket regardless of --drop; only restore-vs-restore may overlap.
	//
	// restoreActiveLocked, not bucketHasActiveRestore: this function already
	// holds s.dumpMu for its whole body, and restore.go's pin registry is
	// now guarded by that same mutex (fix round 1, Important #1) — calling
	// the self-locking bucketHasActiveRestore here would deadlock.
	if holder, ok := s.restoreActiveLocked(database, bucket); ok {
		return nil, fmt.Errorf("%w: bucket %s/%s is being restored (by %q); wait for it to finish or dump a different bucket",
			ErrDumpBucketBeingRestored, database, bucket, holder)
	}
	ceiling := s.dumpPinCeiling()
	session := &dumpSession{
		database:     database,
		bucket:       bucket,
		bucketType:   bucketType,
		startedAt:    time.Now(),
		startedBy:    user,
		digest:       sha256.New(),
		storesDigest: sha256.New(),
		deadDigest:   sha256.New(),
		docs:         make(chan dumpItem, 2),
		walkErr:      make(chan error, 1),
		cancel:       make(chan struct{}),
		pinCeiling:   ceiling,
	}
	s.dump = session
	return session, nil
}

// dumpPinCeiling is how long a dump may hold what it pins: the node's setting,
// or the default.
func (s *Server) dumpPinCeiling() time.Duration {
	if dumpPinCeilingOverrideForTest > 0 {
		return dumpPinCeilingOverrideForTest
	}
	if ceiling := time.Duration(s.config.Server.DumpPinCeilingSeconds) * time.Second; ceiling > 0 {
		return ceiling
	}
	return defaultDumpPinCeiling
}

// dumpNodeLabel names this node in a dump's refusal.
func dumpNodeLabel(s *Server) string {
	if s.config.Server.NodeID == "" {
		return "this node"
	}
	return s.config.Server.NodeID
}

// activeDumpFor resolves the session an OpDumpChunk/OpDumpDone request
// applies to, or a named refusal — including the just-released session's
// forced-failure reason, when there is one, so a client that raced a pin
// ceiling or an aborted connection sees why rather than a bare "no active
// dump".
func (s *Server) activeDumpFor(database, bucket string) (*dumpSession, error) {
	s.dumpMu.Lock()
	current := s.dump
	last := s.lastDump
	s.dumpMu.Unlock()

	if current != nil && current.database == database && current.bucket == bucket {
		return current, nil
	}
	if last != nil && last.database == database && last.bucket == bucket && last.forcedErr != nil {
		return nil, last.forcedErr
	}
	if current != nil {
		return nil, fmt.Errorf("%w: no active dump for %s/%s (the current dump on this node is for %s/%s)",
			ErrDumpNoActiveSession, database, displayBucket(bucket), current.database, displayBucket(current.bucket))
	}
	return nil, fmt.Errorf("%w: no active dump for %s/%s — call dump start first, or it already finished",
		ErrDumpNoActiveSession, database, displayBucket(bucket))
}

// releaseDump releases everything a claimed session holds, exactly once:
// the pin-ceiling timer (if any), the connection-close
// watcher, and the server's single dump slot. Every exit path in startDump,
// dumpDone, the pin-ceiling timer and the connection watcher calls this
// through defer rather than releasing pieces individually, so a failed dump
// never blocks the next one (dump-restore-spec.md §10, §16.3).
//
// forcedErr is non-nil only when the release was not requested by the
// caller (a pin-ceiling force-release or a closed connection); it is
// recorded under s.dumpMu, alongside publishing s.lastDump, so a later
// activeDumpFor call on another goroutine is guaranteed (by the mutex's
// happens-before) to see the fully-written value rather than racing this
// assignment — caught by -race under TestDumpPinCeilingForceReleases during
// self-review: forcedErr was originally written directly on session before
// this call, with no synchronization with activeDumpFor's read at all.
func (s *Server) releaseDump(session *dumpSession, forcedErr error) {
	session.releaseOnce.Do(func() {
		session.stopWalk()
		session.setupMu.Lock()
		pinTimer := session.pinTimer
		watchCancel := session.watchCancel
		session.setupMu.Unlock()
		if pinTimer != nil {
			pinTimer.Stop()
		}
		if watchCancel != nil {
			watchCancel()
		}
		s.dumpMu.Lock()
		session.forcedErr = forcedErr
		if s.dump == session {
			s.dump = nil
		}
		s.lastDump = session
		s.dumpMu.Unlock()

		// forcedErr is non-nil only on the two background-abort paths (the
		// pin-ceiling timer, watchDumpConnection) — never on an ordinary
		// OpDumpDone or a startDump failure, which already report their own
		// outcome to the caller synchronously. Without this event those two
		// paths leave a Start with no matching Done in the audit log,
		// indistinguishable from "still running" (dump-restore-spec.md §11,
		// §19 A-14) — closing the gap Task 2 explicitly left open.
		if forcedErr != nil {
			s.audit.Log(AuditEntry{Event: EventDumpAborted, User: session.startedBy,
				Target: session.database + "/" + displayBucket(session.bucket), Result: forcedErr.Error()})
		}
	})
}

// watchDumpConnection releases the session immediately when ctx is done —
// the connection closing or resetting, since ctx is the connection-scoped
// context every frame handler receives (transport/tcp.go's serveCtx). This
// is the common abort case (dump-restore-spec.md §7.2, §16.3): a killed
// client must not leave a pin, or the server's single dump slot, held
// forever.
func (s *Server) watchDumpConnection(ctx context.Context, session *dumpSession) {
	watchCtx, cancel := context.WithCancel(context.Background())
	session.setupMu.Lock()
	session.watchCancel = cancel
	session.setupMu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			s.logger.Warn("dump aborted: connection closed", "database", session.database, "bucket", displayBucket(session.bucket))
			s.releaseDump(session, fmt.Errorf("dump of %s/%s aborted: client connection closed", session.database, displayBucket(session.bucket)))
		case <-watchCtx.Done():
		}
	}()
}

// bucketEnumerator returns the read-only dump walk of a read view's engine
// (store/delivery_dump.go, store/enumerate.go). The view is resolveForRead's: its engine
// was built over the dump session's one snapshot, so the walk reads the
// bucket's range in that snapshot and nothing else (spec §7, build doc SA-20).
// This is the one place that knows which inMemoryBucket field holds which
// engine for a given BucketType.
func bucketEnumerator(view *inMemoryBucket) (store.DumpEnumerator, error) {
	switch view.bucketType {
	case BucketTypeHash:
		return store.DocumentsOnly(view.hashStore), nil
	case BucketTypeBTree:
		return store.DocumentsOnly(view.btreeStore), nil
	case BucketTypeHeap:
		return view.heapStore, nil
	case BucketTypeFIFO, BucketTypeLIFO:
		return view.queueStore, nil
	case BucketTypeBlob:
		return store.DocumentsOnly(view.blobStore), nil
	default:
		return nil, fmt.Errorf("dumping bucket type %q is not supported", view.bucketType)
	}
}

// bucketIndexNames returns the declared secondary index field names, sorted,
// or nil if the bucket has none. An index this binary could not decode has no
// fields to name, so it contributes none.
func bucketIndexNames(b *inMemoryBucket) []string {
	specs := b.declaredIndexes()
	if len(specs) == 0 {
		return nil
	}
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		if len(spec.Fields) == 0 || indexesAPath(spec) {
			continue
		}
		names = append(names, spec.Fields[0].Field)
	}
	sort.Strings(names)
	return names
}

// pathIndexDumpNote is the note a dump start carries for each recorded index
// on a path it leaves out of the manifest, which the CLI prints on stderr; ""
// when there is none.
func pathIndexDumpNote(b *inMemoryBucket, database, bucket string) string {
	var notes []string
	for _, spec := range b.declaredIndexes() {
		if indexesAPath(spec) {
			notes = append(notes, fmt.Sprintf("index %s of bucket %s/%s indexes a path (%s), which this release cannot index, "+
				"so it is unusable and this dump leaves it out; a restore of this dump will not have it. "+
				"Index a top-level copy of the value instead", spec.Name, database, bucket, strings.Join(spec.FieldNames(), ", ")))
		}
	}
	return strings.Join(notes, "\n")
}

// refuseUnreadableIndexes fails a dump of a bucket that holds an index this
// binary cannot read. Such an index is held in the catalog byte for byte, but
// a manifest can only describe what this binary understands: dumping around
// it would restore a unique constraint as a plain index, or without it, and
// say nothing (rule 10). The newer binary that wrote it can dump it whole.
func refuseUnreadableIndexes(b *inMemoryBucket, database, bucket string) error {
	for _, spec := range b.declaredIndexes() {
		if spec.State != store.IndexUnreadable {
			continue
		}
		return fmt.Errorf("%w: bucket %s/%s holds index %s, written by a newer version of hoardDB (descriptor version %d; this binary reads up to %d), "+
			"and a dump from this version would restore it as a plain index or leave it out. "+
			"Dump the bucket with the newer version of hoardDB, which can read it",
			ErrDumpUnreadableIndex, database, bucket, spec.Name, spec.Version, store.IndexSpecVersion)
	}
	return nil
}

// dumpIndexSpecs returns a bucket's descriptors as a dump carries them, in id
// order. startDump refuses a bucket holding an index this binary cannot read,
// so every descriptor here is one it can declare again. An index on a path is
// left out: it is unusable, and a restore could not declare it (E-17), so
// pathIndexDumpNote tells the client it was dropped (spec §4.9.7).
func dumpIndexSpecs(b *inMemoryBucket) []DumpIndexSpec {
	var carried []DumpIndexSpec
	for _, spec := range usableIndexSpecs(b.declaredIndexes()) {
		dumped := DumpIndexSpec{ID: int32(spec.ID), Name: spec.Name, Fields: spec.FieldNames(), Unique: spec.Unique}
		if spec.TTL != nil {
			expireAfter := spec.TTL.ExpireAfterSeconds
			dumped.ExpireAfterSeconds = &expireAfter
		}
		carried = append(carried, dumped)
	}
	sort.Slice(carried, func(i, j int) bool { return carried[i].ID < carried[j].ID })
	return carried
}

// runDumpWalk drives the bucket's IterateDump walk in the background —
// the single Badger read transaction for this bucket's dump (spec §8.1: "one
// read-only transaction per bucket is a consistent view") — feeding
// session.docs so OpDumpChunk calls can pull a bounded amount of data per
// wire request instead of buffering the whole bucket in memory.
//
// The oversized-document check (dump-restore-spec.md §4: refuse the whole
// bucket, naming it, the key, and the limit, pointing at blob buckets as the
// alternative) is folded into this same walk rather than run as a separate
// pre-pass: fix-round-1 found the separate pass (server/dump.go's earlier
// probeDumpableSize) opened its own transaction, so a document written
// between that pass and this one could slip past the refusal entirely —
// exactly backwards from what the check exists for. Checking here means one
// transaction sees every document exactly once, and a bucket that turns out
// to hold an oversized document errors out of the walk before any of ITS
// bytes are queued; only documents seen and cleared before it (in whatever
// order this engine's iteration produces) may already be in
// OpDumpChunk-delivered chunks by then, which is an accepted, narrower
// trade against a consistent single-transaction read — the same trade
// dump-restore-spec.md §8.2/§8.3 already make for concurrent
// pushes/pops and cross-bucket consistency.
//
// Cancellation is cooperative: once session.cancel closes, the next document
// (or the one currently blocked on send) aborts the walk without emitting
// it, and the walk's own error is downgraded to nil — a session ending on
// purpose (OpDumpDone, connection close) is not a read failure.
//
// A bucket that declares store fields then walks its element records, in the
// same snapshot (walkElements, nil for a bucket with none), so the elements
// dumped are the ones of the documents dumped.
func (s *Server) runDumpWalk(session *dumpSession, enumerator store.DumpEnumerator, walkElements func(emit func(record []byte) error) error) {
	defer close(session.walkDone)
	if hook := dumpWalkTestHook.Load(); hook != nil {
		(*hook)()
	}
	err := enumerator.IterateDump(func(key, value []byte) error {
		if len(value) > maxDumpDocumentBytes {
			label := "queue element"
			if len(key) > 0 {
				label = fmt.Sprintf("key %q", key)
			}
			return fmt.Errorf("%w: bucket %q holds a %d-byte document (%s) that exceeds the %d-byte per-frame dump limit; store payloads that large in a blob bucket instead",
				ErrDumpDocumentTooLarge, session.bucket, len(value), label, maxDumpDocumentBytes)
		}
		return session.emit("", value, s.ctx.Done())
	}, func(record store.DeadRecord) error {
		return s.emitDeadRecord(session, record)
	})
	if err == nil && walkElements != nil {
		err = walkElements(func(record []byte) error {
			if len(record) > maxDumpDocumentBytes {
				return fmt.Errorf("%w: bucket %q holds a %d-byte store-field element that exceeds the %d-byte per-frame dump limit",
					ErrDumpDocumentTooLarge, session.bucket, len(record), maxDumpDocumentBytes)
			}
			return session.emit(dumpSectionStores, record, s.ctx.Done())
		})
	}
	if errors.Is(err, errDumpCanceled) {
		err = nil
	}
	session.walkErr <- err
	close(session.docs)
}

// emitDeadRecord sends one dead entry as a "dead" section record. A client
// that did not say it writes the section is refused here too, for the entry the
// start-time check cannot see: a heap twin, which exists only in the walk.
func (s *Server) emitDeadRecord(session *dumpSession, record store.DeadRecord) error {
	if !session.acceptDead {
		return fmt.Errorf("%w: bucket %s/%s holds a dead entry, and this client does not write them into a dump: "+
			"its dump would put the entry among the live documents. Upgrade hoarddb to this server's version and dump again",
			ErrDumpNeedsNewerClient, session.database, session.bucket)
	}
	encoded, err := bson.Marshal(dumpDeadRecord{
		Data: record.Data, Deliveries: record.Deliveries, DeadAt: time.UnixMilli(record.DeadAt).UTC(), Reason: record.Reason,
	})
	if err != nil {
		return fmt.Errorf("encoding a dead entry of %s/%s for the dump: %w", session.database, session.bucket, err)
	}
	if len(encoded) > maxDumpDocumentBytes {
		return fmt.Errorf("%w: bucket %q holds a %d-byte dead entry that exceeds the %d-byte per-frame dump limit",
			ErrDumpDocumentTooLarge, session.bucket, len(encoded), maxDumpDocumentBytes)
	}
	return session.emit(dumpSectionDead, encoded, s.ctx.Done())
}

// launchDumpWalk starts session's walk goroutine, or reports why it must not
// run. It returns false and no error when an OpDumpDone ended the session
// before this point: that session is released and its digest already read,
// so a walk started now would stream to nobody, and nothing would wait for
// it. It returns ErrDumpNodeStopping once Stop has begun waiting for walks.
//
// The decision to launch, walkDone's creation and s.dumpWG.Add(1) are one
// step, under s.dumpMu and then session.setupMu (the only place both are
// held; nothing takes them in the other order):
//
//   - dumpMu orders the Add against Stop. stopDumpWalks sets
//     dumpWalksStopped under dumpMu before it calls Wait, so an Add here
//     either happens before that Wait, which then covers the walk, or sees
//     the flag and launches nothing. That is the rule reclaim.go's
//     startReclaimer and capacity_sampler.go's startCapacitySampler keep by
//     construction, since they start before Stop can run: Add before the go
//     statement, never inside the goroutine, where it could come after Wait
//     has already returned. A dump can start at any time, so here the
//     ordering has to be taken, not assumed.
//   - setupMu orders the launch against stopWalk. stopWalk closes cancel
//     before it reads walkDone under setupMu, so either this check sees
//     cancel closed and launches nothing, or stopWalk's read comes after
//     this critical section, finds walkDone, and waits for the walk.
//
// walkDone is created here, not in claimDump: it must exist only once the
// goroutine that will close it is about to be launched (see claimDump's doc
// comment for why an earlier version, which created it in claimDump, broke
// two tests that claim a dump session directly without ever starting a
// walk). It goes through setupMu, same as the pin-ceiling timer: a session
// is reachable via activeDumpFor from the instant claimDump publishes
// s.dump, so a concurrent OpDumpDone's stopWalk can be reading walkDone as
// this writes it (caught by -race under
// TestDumpStartPublishesSessionOnlyAfterFullyWired).
func (s *Server) launchDumpWalk(session *dumpSession, enumerator store.DumpEnumerator, walkElements func(emit func(record []byte) error) error, snap storage.KVSnapshot) (bool, error) {
	s.dumpMu.Lock()
	defer s.dumpMu.Unlock()
	if s.dumpWalksStopped {
		return false, fmt.Errorf("%w: dump of %s/%s was not started; retry once the node is back up",
			ErrDumpNodeStopping, session.database, session.bucket)
	}
	session.setupMu.Lock()
	defer session.setupMu.Unlock()
	select {
	case <-session.cancel:
		return false, nil
	default:
	}
	session.walkDone = make(chan struct{})
	s.dumpWG.Add(1)
	go func() {
		// Done is deferred first so it runs last: the snapshot is discarded
		// before Stop's Wait can return, and the discard touches the store.
		defer s.dumpWG.Done()
		defer snap.Discard()
		s.runDumpWalk(session, enumerator, walkElements)
	}()
	return true, nil
}

// stopDumpWalks is Stop's half of launchDumpWalk's ordering. The caller has
// already cancelled s.ctx, which is what unblocks a walk parked on a
// document no OpDumpChunk will read; waiting before that cancel would hang
// Stop on such a walk forever.
func (s *Server) stopDumpWalks() {
	s.dumpMu.Lock()
	s.dumpWalksStopped = true
	s.dumpMu.Unlock()
	s.dumpWG.Wait()
}

// emit hands one document, element or dead record to the chunk puller, and
// counts it into its section's digest. stopping is the server's s.ctx.Done().
func (session *dumpSession) emit(section string, value []byte, stopping <-chan struct{}) error {
	buf := append([]byte(nil), value...)
	select {
	case session.docs <- dumpItem{section: section, doc: buf}:
		switch section {
		case dumpSectionStores:
			session.storesDigest.Write(buf)
			atomic.AddInt64(&session.elementsEmitted, 1)
		case dumpSectionDead:
			session.deadDigest.Write(buf)
			atomic.AddInt64(&session.deadEmitted, 1)
		default:
			session.digest.Write(buf)
			atomic.AddInt64(&session.docsEmitted, 1)
		}
		atomic.AddInt64(&session.bytesEmitted, int64(len(buf)))
		return nil
	case <-session.cancel:
		return errDumpCanceled
	case <-stopping:
		// The node is stopping and Stop waits for this walk before it
		// closes the store; with no reader left, the send above would
		// block that wait forever. Not errDumpCanceled: nobody ended this
		// session on purpose, and a nil walk error would let OpDumpChunk
		// report the truncated bucket as a clean EOF.
		return fmt.Errorf("%w: the dump of %s/%s stopped before the end of the bucket; dump it again once the node is back up",
			ErrDumpNodeStopping, session.database, session.bucket)
	}
}

// startDump handles OpDumpStart: root gate, single-dump-per-server claim,
// the pin-ceiling timer for queue buckets, and kicking off the one background
// walk (one Badger read transaction, spec §8.1) that both streams documents
// to OpDumpChunk and checks each one against the oversized-document limit —
// see runDumpWalk's doc comment for why the count and the size check both
// live there now instead of in separate upfront passes.
func (s *Server) startDump(ctx context.Context, req DumpStartRequest) (*DumpStartResponse, error) {
	if err := requireRootIdentity(ctx, s); err != nil {
		return nil, err
	}
	if req.Database == "" || req.Bucket == "" {
		return nil, fmt.Errorf("dump requires a database and a bucket; got database=%q bucket=%q", req.Database, req.Bucket)
	}
	// The dump reads one snapshot, opened as the name resolved (the reader
	// rule, spec §5.2.2), for the session's whole life: the analogue of the
	// segment store's compaction pin (build doc SA-20). The walk discards it
	// when it returns; every earlier exit discards it here.
	b, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	walkOwnsSnapshot := false
	defer func() {
		if !walkOwnsSnapshot {
			snap.Discard()
		}
	}()

	plan, err := s.planDump(b, snap, req)
	if err != nil {
		return nil, err
	}
	bucketType, enumerator, walkElements := b.bucketType, plan.enumerator, plan.walkElements

	username := sessionUsername(ctx)

	session, err := s.claimDump(req.Database, req.Bucket, bucketType, username)
	if err != nil {
		return nil, err
	}
	// Every exit path from here must release the slot this claimed. This
	// defer covers the pieces that can only be created after claimDump
	// returns: walkDone, the walk goroutine, the connection watcher, and the
	// pin-ceiling timer.
	started := false
	defer func() {
		if !started {
			s.releaseDump(session, nil)
		}
	}()

	if startDumpTestBarrier != nil {
		startDumpTestBarrier()
	}

	// No DocCount() call here, deliberately (fix-round-1 finding): it would
	// be a second, independent Badger read transaction on top of the one
	// runDumpWalk opens below, and a document written between the two could
	// make an upfront count disagree with what is actually streamed —
	// silently, since nothing would cross-check them. The single walk below
	// is this bucket's one transaction; its own tally (session.docsEmitted,
	// surfaced in OpDumpDone's response) is what's authoritative.

	session.acceptDead = req.AcceptDead
	launched, err := s.launchDumpWalk(session, enumerator, walkElements, snap)
	if err != nil {
		return nil, err
	}
	walkOwnsSnapshot = launched
	// Not launched means an OpDumpDone already ended and released this
	// session: there is no walk for the watcher or the pin-ceiling timer to
	// bound, and arming either now would outlive the session.
	if launched {
		s.watchDumpConnection(ctx, session)
	}

	if launched && (bucketType == BucketTypeFIFO || bucketType == BucketTypeLIFO) {
		ceiling := session.pinCeiling
		timer := time.AfterFunc(ceiling, func() {
			s.logger.Error("dump pin ceiling exceeded; force-releasing",
				"database", req.Database, "bucket", req.Bucket, "ceiling", ceiling)
			s.releaseDump(session, fmt.Errorf("%w: bucket %s/%s exceeded the %s compaction-pin ceiling; dump a continuously-popped queue in a maintenance window instead",
				ErrDumpPinCeilingExceeded, req.Database, req.Bucket, ceiling))
		})
		session.setupMu.Lock()
		session.pinTimer = timer
		session.setupMu.Unlock()
	}
	started = true

	s.audit.Log(AuditEntry{Event: EventDumpStart, User: username,
		Target: req.Database + "/" + displayBucket(req.Bucket), Result: "ok"})

	return &DumpStartResponse{Success: true, Message: plan.message, Manifest: plan.manifest}, nil
}

// dumpPlan is what a dump of one bucket starts from: its manifest, the note
// the start answers with, and the walks of its documents and elements.
type dumpPlan struct {
	manifest     *DumpBucketManifest
	message      string
	enumerator   store.DumpEnumerator
	walkElements func(emit func([]byte) error) error
}

// planDump makes the checks a dump of b makes before it claims anything, in
// the snapshot it reads, and builds its manifest. A spread collection's
// router plans the dump of its template this way (spread_dump.go): the same
// refusals and the same manifest, with no slot claimed and nothing walked.
func (s *Server) planDump(b *inMemoryBucket, snap storage.KVSnapshot, req DumpStartRequest) (dumpPlan, error) {
	if err := refuseUnreadableIndexes(b, req.Database, req.Bucket); err != nil {
		return dumpPlan{}, err
	}
	enumerator, enumErr := bucketEnumerator(b)
	indexes, indexSpecs := bucketIndexNames(b), dumpIndexSpecs(b)
	if enumErr != nil {
		return dumpPlan{}, fmt.Errorf("dump %s/%s: %w", req.Database, req.Bucket, enumErr)
	}
	stores, walkElements := dumpStoresOf(b, snap)
	if len(stores) > 0 && !req.AcceptStores {
		return dumpPlan{}, fmt.Errorf("%w: bucket %s/%s declares store fields (%s), and this client does not write them into a dump: "+
			"its dump would hold every document and none of their elements. Upgrade hoarddb to this server's version and dump again",
			ErrDumpNeedsNewerClient, req.Database, req.Bucket, declaredEngines(b.storeFields()))
	}
	if err := refuseDeadForOldClient(b, snap, req); err != nil {
		return dumpPlan{}, err
	}
	factor, placement, err := declaredAt(snap, req.Database, req.Bucket)
	if err != nil {
		return dumpPlan{}, fmt.Errorf("dump %s/%s: %w", req.Database, req.Bucket, err)
	}
	return dumpPlan{
		message:      pathIndexDumpNote(b, req.Database, req.Bucket),
		enumerator:   enumerator,
		walkElements: walkElements,
		manifest: &DumpBucketManifest{
			FormatVersion:     dumpManifestFormatVersion,
			HoardDBVersion:    Version,
			ProtocolMajor:     transport.ProtocolMajor,
			SourceNode:        s.config.Server.NodeID,
			Database:          req.Database,
			Bucket:            req.Bucket,
			StoreType:         b.bucketType,
			Indexes:           indexes,
			IndexSpecs:        indexSpecs,
			BlobsIncluded:     false,
			Stores:            stores,
			ReplicationFactor: factor,
			Placement:         placement,
		},
	}, nil
}

// refuseDeadForOldClient refuses a bucket that holds dead entries to a client
// that does not write the "dead" section (build B-14). It reads the bucket's
// state in the dump's own snapshot, so the count it refuses on is the one the
// walk will see; a bucket with none costs one state read, which the walk makes
// anyway.
func refuseDeadForOldClient(view *inMemoryBucket, snap storage.KVSnapshot, req DumpStartRequest) error {
	if req.AcceptDead {
		return nil
	}
	switch view.bucketType {
	case BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap:
	default:
		return nil
	}
	state, err := store.LoadBucketState(snap, view.slot, view.databaseID, view.id.Load())
	if err != nil {
		return fmt.Errorf("dump %s/%s: reading the dead entry count: %w", req.Database, req.Bucket, err)
	}
	if state.Dead == 0 {
		return nil
	}
	return fmt.Errorf("%w: bucket %s/%s holds %d dead entries, and this client does not write them into a dump: "+
		"its dump would put them among the live documents, and a restore would revive them. Upgrade hoarddb to this server's version and dump again, "+
		"or list them with 'dead()' and requeue or discard them first",
		ErrDumpNeedsNewerClient, req.Database, req.Bucket, state.Dead)
}

// declaredAt is the factor and the placement database/bucket declared, as its
// catalog entry reads in snap: the snapshot the dump walks, so the manifest
// describes the bucket whose documents it streams. The factor is zero when
// the bucket declared none; the placement is empty for a bucket that is not a
// hash bucket.
func declaredAt(snap storage.KVSnapshot, database, bucket string) (int, string, error) {
	encoded, err := snap.Get(catalogKey(database, bucket))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("reading the catalog entry of bucket %q in database %q: %w", bucket, database, err)
	}
	var entry catalogEntry
	if err := bson.Unmarshal(encoded, &entry); err != nil {
		return 0, "", fmt.Errorf("decoding the catalog entry of bucket %q in database %q: %w", bucket, database, err)
	}
	return int(entry.ReplicationFactor), entry.Placement, nil
}

// dumpStoresOf is a bucket's store fields as its dump manifest names them,
// and the walk that emits their element records from snap — nil for a
// bucket that declares none.
func dumpStoresOf(view *inMemoryBucket, snap storage.KVSnapshot) ([]DumpStore, func(emit func([]byte) error) error) {
	declared := view.storeFields().stores
	if len(declared) == 0 {
		return nil, nil
	}
	names := make(map[int32]string, len(declared))
	for _, field := range declared {
		names[field.ID] = field.Name
	}
	stores := make([]DumpStore, 0, len(declared))
	for _, field := range declared {
		stores = append(stores, DumpStore{Name: field.Name, Engine: field.Engine, Parent: names[field.Parent]})
	}
	layout := view.fieldLayout()
	slot, databaseID, id := view.slot, view.databaseID, view.id.Load()
	walk := func(emit func([]byte) error) error {
		return store.WalkFieldElements(snap, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id}, layout, func(element store.FieldElementRecord) error {
			record, err := dumpElementOf(snap, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id}, names, layout, element)
			if err != nil {
				return err
			}
			return emit(record)
		})
	}
	return stores, walk
}

// dumpKeyOf is an element's key in a dump record: its position (fifo, lifo),
// its priority and entry id (heap), or its element key.
func dumpKeyOf(engine store.FieldEngine, entry store.FieldEntry) interface{} {
	switch engine {
	case store.FieldFIFO, store.FieldLIFO:
		return int64(entry.Position)
	case store.FieldHeap:
		return dumpHeapKey{Priority: entry.Priority, ID: entry.EntryID}
	default:
		return entry.ElementKey
	}
}

// dumpElementOf encodes one element as its dump record.
func dumpElementOf(snap storage.KVSnapshot, ref storage.BucketRef, names map[int32]string, layout store.FieldLayout, element store.FieldElementRecord) ([]byte, error) {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	field, engine := names[int32(element.FieldID)], layout[element.FieldID]
	record := dumpElementRecord{Field: field, Owner: element.DocumentKey, Value: element.Entry.Value, Key: dumpKeyOf(engine, element.Entry)}
	if engine == store.FieldBlob {
		metadata, err := store.FieldBlobDumpDocument(snap, slot, databaseID, id, element.Entry.Value)
		if err != nil {
			return nil, err
		}
		record.Value = metadata
	}
	for _, ancestor := range element.Ancestors {
		record.Path = append(record.Path, dumpPathStep{Field: names[int32(ancestor.FieldID)], Key: dumpKeyOf(layout[ancestor.FieldID], ancestor.Entry)})
	}
	encoded, err := bson.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encoding the dump record of an element of %q under document %q: %w", field, element.DocumentKey, err)
	}
	return encoded, nil
}

// dumpChunk handles OpDumpChunk: pull whole documents from the session's
// background walk until transport.DumpChunkSize is reached, EOF, or a walk
// error. A document is never split across chunks — one that would overflow
// the target is held on session.pending for the next call, which is why a
// single oversized-but-still-frameable document can make one chunk larger
// than the nominal target.
func (s *Server) dumpChunk(ctx context.Context, req DumpChunkRequest) (*DumpChunkResponse, error) {
	if err := requireRootIdentity(ctx, s); err != nil {
		return nil, err
	}
	session, err := s.activeDumpFor(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}

	session.pullMu.Lock()
	defer session.pullMu.Unlock()

	var buf []byte
	section := ""
	docsInChunk := 0
	for {
		var item dumpItem
		if session.pending != nil {
			item = *session.pending
			session.pending = nil
		} else {
			var ok bool
			item, ok = <-session.docs
			if !ok {
				// The walk finished — cleanly or with an error. walkErr is
				// sent before docs is closed (runDumpWalk), so this read
				// cannot block.
				if walkErr := <-session.walkErr; walkErr != nil {
					// A walk error (an oversized document found mid-walk, or
					// a storage read failure) ends the dump right here: no
					// more chunks are coming and the client has no reason to
					// call OpDumpDone for a bucket that never finished
					// starting. Release now — same as any other failed-dump
					// exit path — so this bucket's failure does not hold the
					// server's single dump slot for the next attempt.
					s.releaseDump(session, nil)
					return nil, fmt.Errorf("reading %s/%s for dump: %w", req.Database, req.Bucket, walkErr)
				}
				return &DumpChunkResponse{Success: true, Data: buf, EOF: true, Documents: docsInChunk, Section: section}, nil
			}
		}
		// A chunk never mixes documents and element records: the client
		// writes each to its own file.
		if len(buf) > 0 && (len(buf)+len(item.doc) > transport.DumpChunkSize || item.section != section) {
			session.pending = &item
			return &DumpChunkResponse{Success: true, Data: buf, Documents: docsInChunk, Section: section}, nil
		}
		section = item.section
		buf = append(buf, item.doc...)
		docsInChunk++
		if len(buf) >= transport.DumpChunkSize {
			return &DumpChunkResponse{Success: true, Data: buf, Documents: docsInChunk, Section: section}, nil
		}
	}
}

// dumpDone handles OpDumpDone: check the client's digest acknowledgement
// against what this node actually streamed, then release the session
// unconditionally (defer, so a digest mismatch still releases the pin —
// AGENTS.md rule on never leaving a failed operation holding a lock the next
// one needs).
func (s *Server) dumpDone(ctx context.Context, req DumpDoneRequest) (*DumpDoneResponse, error) {
	if err := requireRootIdentity(ctx, s); err != nil {
		return nil, err
	}
	session, err := s.activeDumpFor(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer s.releaseDump(session, nil)
	// Ensure the walk has fully stopped — and so made its last
	// session.digest.Write call — before reading it below. See stopWalk's
	// doc comment: an OpDumpDone that arrives before every chunk has been
	// drained to EOF would otherwise race the still-running walk.
	session.stopWalk()

	serverDigest := "sha256:" + hex.EncodeToString(session.digest.Sum(nil))
	storesDigest := ""
	elementsEmitted := atomic.LoadInt64(&session.elementsEmitted)
	if elementsEmitted > 0 {
		storesDigest = "sha256:" + hex.EncodeToString(session.storesDigest.Sum(nil))
	}
	deadDigest := ""
	deadEmitted := atomic.LoadInt64(&session.deadEmitted)
	if deadEmitted > 0 {
		deadDigest = "sha256:" + hex.EncodeToString(session.deadDigest.Sum(nil))
	}
	bytesEmitted := atomic.LoadInt64(&session.bytesEmitted)
	docsEmitted := atomic.LoadInt64(&session.docsEmitted)
	username := sessionUsername(ctx)
	target := req.Database + "/" + displayBucket(req.Bucket)

	if req.Digest != "" && req.Digest != serverDigest {
		s.audit.Log(AuditEntry{Event: EventDumpDone, User: username, Target: target,
			Result: fmt.Sprintf("digest mismatch: client=%s server=%s", req.Digest, serverDigest)})
		return nil, fmt.Errorf("%w: bucket %s: client reported %s, server streamed %s — the .bson file may be corrupt or truncated",
			ErrDumpDigestMismatch, target, req.Digest, serverDigest)
	}

	if req.StoresDigest != "" && req.StoresDigest != storesDigest {
		s.audit.Log(AuditEntry{Event: EventDumpDone, User: username, Target: target,
			Result: fmt.Sprintf("stores digest mismatch: client=%s server=%s", req.StoresDigest, storesDigest)})
		return nil, fmt.Errorf("%w: bucket %s: client reported %s for its store fields, server streamed %s — the .stores.bson file may be corrupt or truncated",
			ErrDumpDigestMismatch, target, req.StoresDigest, storesDigest)
	}

	if req.DeadDigest != "" && req.DeadDigest != deadDigest {
		s.audit.Log(AuditEntry{Event: EventDumpDone, User: username, Target: target,
			Result: fmt.Sprintf("dead digest mismatch: client=%s server=%s", req.DeadDigest, deadDigest)})
		return nil, fmt.Errorf("%w: bucket %s: client reported %s for its dead entries, server streamed %s — the .dead.bson file may be corrupt or truncated",
			ErrDumpDigestMismatch, target, req.DeadDigest, deadDigest)
	}

	s.audit.Log(AuditEntry{Event: EventDumpDone, User: username, Target: target, Result: "ok"})
	return &DumpDoneResponse{
		Success:      true,
		Bytes:        bytesEmitted,
		Documents:    docsEmitted,
		Digest:       serverDigest,
		Elements:     elementsEmitted,
		StoresDigest: storesDigest,
		Dead:         deadEmitted,
		DeadDigest:   deadDigest,
	}, nil
}

// sessionUsername reads the authenticated username from ctx, or "" when
// there is none (a forwarded internode request — see requireRootIdentity's
// doc comment). Used for audit entries, which tolerate an empty user.
func sessionUsername(ctx context.Context) string {
	sess, ok := transport.SessionFromContext(ctx)
	if !ok || sess == nil {
		return ""
	}
	identity, ok := sess.Identity()
	if !ok {
		return ""
	}
	return identity.Username
}

// handleDumpStart is the wire entry point for OpDumpStart.
func handleDumpStart(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DumpStartRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal dump start request: %w", err))
	}
	resp, err := s.startDump(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpDumpStart, resp)
}

// handleDumpChunk is the wire entry point for OpDumpChunk.
func handleDumpChunk(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DumpChunkRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal dump chunk request: %w", err))
	}
	resp, err := s.dumpChunk(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpDumpChunk, resp)
}

// handleDumpDone is the wire entry point for OpDumpDone.
func handleDumpDone(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DumpDoneRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal dump done request: %w", err))
	}
	resp, err := s.dumpDone(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpDumpDone, resp)
}
