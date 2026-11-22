// Package server provides the gRPC server implementation for hoardDB.
//
// This file defines request/response types used across the gRPC service.
// These are plain Go structs for now; proper protobuf definitions will
// be generated in a later stage.
package server

import (
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// InsertRequest is sent to insert a document into a bucket.
type InsertRequest struct {
	Database     string                 `json:"database"`
	Bucket       string                 `json:"bucket"`
	Key          string                 `json:"key,omitempty"`
	Data         map[string]interface{} `json:"data"`
	WriteConcern string                 `json:"write_concern,omitempty"`
	// verb is the write's intent, set by the handler that decoded the request
	// and invisible to BSON (write_verb.go), so a client cannot set it.
	verb writeVerb
	// Admitted is set only by read repair's own put (Cluster.repairKey): it
	// re-puts a document an owner already stores, so the handler does not
	// refuse it for a top-level name no path can reach (E-18), which a legacy
	// document may hold (typed-fields spec §5.6). The client door refuses a
	// request that sets it (E-12); replication and replay keep it.
	Admitted bool `bson:"admitted,omitempty" json:"admitted,omitempty"`
	// RepairBound is node-internal, as DeleteRequest.RepairBound is: only a
	// consistent get's put repair sets it, one above the highest version the
	// read saw, and a client frame carrying it is refused. It is exclusive —
	// a hash or btree put applies only while the key's stored version is
	// below it, an absent key being version 0 — so a write that landed after
	// the read is never overwritten by the older value the read returned
	// (BUG-0480). 0, the field absent, is an unbounded put.
	RepairBound uint64 `bson:"repair_bound,omitempty" json:"repair_bound,omitempty"`
	// RepairRead is node-internal and set beside RepairBound: where the
	// bucket's owner was when the consistent get read it. The owner drops the
	// repair before logging it when its own log after that position holds a
	// write of the key, a delete included, which RepairBound cannot see
	// (BUG-0603). A put that carries RepairBound without it is dropped.
	RepairRead *RepairReadPosition `bson:"repair_read,omitempty" json:"repair_read,omitempty"`
}

// RepairReadPosition is the owner's copy of a bucket as a consistent get read
// it: the owner, the copy's incarnation and term, and its position, the
// owner's own log sequence of the last entry of the term it had applied.
type RepairReadPosition struct {
	Owner       string `bson:"owner"`
	Incarnation uint64 `bson:"incarnation"`
	Term        uint64 `bson:"term"`
	Through     uint64 `bson:"through"`
}

// Every data mutation's response carries CopiesConfirmed and
// ReplicationFactor (spec §19.3.11).
//
// A write concern waits for the copies of the bucket that are in sync, not for
// its stored factor (owner decision D-2), so `all` on a bucket whose replicas
// are out succeeds against fewer copies. The number in the response is what
// makes that honest: CopiesConfirmed is how many nodes hold the write when it
// is answered — the owner and every replica it was routed to that has
// acknowledged it — and ReplicationFactor is the bucket's stored factor. A
// client that wants the old meaning of `all` compares the two. There is no
// single write response type, so the pair is declared on each shape and
// server/write_path.go:stampWriteOutcome fills them in one place, on the
// owner. On a single node, or for DDL, neither is set, and omitempty keeps
// both off the wire.
//
// WriteOutcome is set beside them, to "unknown", on a response that answers
// the owner's own store failure with Success false rather than an error: the
// write is logged and shipped and may well be stored. Otherwise it
// is empty and off the wire; an error carries its outcome in ErrorResponse.
//
// Both BSON names are pinned. Untagged, the codec lowercases the Go field name
// and CopiesConfirmed would travel as "copiesconfirmed" for good.

// InsertResponse is returned after a successful insert.
type InsertResponse struct {
	ID                string `json:"id"`
	Success           bool   `json:"success"`
	Message           string `json:"message,omitempty"`
	Seq               uint64 `json:"seq,omitempty"`
	CopiesConfirmed   int    `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int    `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
}

// FindRequest is sent to look up a document by key.
type FindRequest struct {
	Database string `json:"database"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
}

// FindResponse is returned with the found document.
//
// The bson tags spell out the names the driver used by default, and omit Data
// when it is nil: a nil bson.Raw is not a valid embedded document, so encoding
// a miss failed and the reply went out empty. Message has no
// omitempty on purpose: today an empty one is sent as "", and the bytes of a
// hit must not change.
type FindResponse struct {
	Found   bool     `bson:"found" json:"found"`
	Data    bson.Raw `bson:"data,omitempty" json:"data,omitempty"`
	Message string   `bson:"message" json:"message,omitempty"`
}

// PushRequest is sent to push a document onto a FIFO/LIFO/heap store.
type PushRequest struct {
	Database     string                 `json:"database"`
	Bucket       string                 `json:"bucket"`
	Data         map[string]interface{} `json:"data"`
	Priority     int64                  `json:"priority,omitempty"` // heap only
	WriteConcern string                 `json:"write_concern,omitempty"`
	// verb is the write's intent, as InsertRequest.verb: the push handlers set
	// verbInsert, so a push into a hash or btree bucket refuses a held _id_.
	verb writeVerb
}

// PushResponse is returned after a successful push.
type PushResponse struct {
	ID                string `json:"id"`
	Success           bool   `json:"success"`
	Message           string `json:"message,omitempty"`
	Seq               uint64 `json:"seq,omitempty"`
	CopiesConfirmed   int    `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int    `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
}

// PopRequest asks to remove and return the next Limit entries of a fifo, lifo
// or heap bucket. Absent or zero means 1; the server refuses a negative Limit
// and one above maxPopLimit. ResultCeiling is set by the bucket's owner before
// it logs the pop (Cluster.applyAsOwner); a client's value is overwritten
// there and ignored by a node that logs nothing.
//
// Every member carries an explicit bson tag, as PeekRequest's do: with json
// tags alone the bson encoding is the driver's lowercase default and omitempty
// is not honoured.
type PopRequest struct {
	Database      string `bson:"database" json:"database"`
	Bucket        string `bson:"bucket" json:"bucket"`
	Limit         int    `bson:"limit,omitempty" json:"limit,omitempty"`
	ResultCeiling int64  `bson:"result_ceiling,omitempty" json:"-"`
	// LeaseNow is the owner's stamped time for the return step of expired
	// leases (claim-ack-spec 5.1). A client's value is ignored.
	LeaseNow int64 `bson:"lease_now,omitempty" json:"-"`
}

// PopResponse carries the removed entries in pop order. Items is empty when
// Found is false. There is no single-entry member: a one-entry pop answers a
// one-item list.
//
// Tagged like FindResponse, for the same reason.
type PopResponse struct {
	Found             bool       `bson:"found" json:"found"`
	Items             []bson.Raw `bson:"items,omitempty" json:"items,omitempty"`
	Message           string     `bson:"message" json:"message,omitempty"`
	CopiesConfirmed   int        `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int        `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string     `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
	// Pending and Dead count the bucket's claimed and dead entries after the
	// pop, from the state the apply holds. Each is absent at zero, so a bucket
	// that never claimed answers byte for byte as before (claim-ack-spec 8.1).
	Pending int64 `bson:"pending,omitempty" json:"pending,omitempty"`
	Dead    int64 `bson:"dead,omitempty" json:"dead,omitempty"`
}

// PeekRequest asks for the next Limit entries a pop would take, removing none
// of them. Absent or zero means 1; the server refuses a negative Limit and one
// above maxPeekLimit.
//
// Every member carries an explicit bson tag, as the field messages do: with
// json tags alone the bson encoding is the driver's lowercase default and
// omitempty is not honoured.
type PeekRequest struct {
	Database string `bson:"database" json:"database"`
	Bucket   string `bson:"bucket" json:"bucket"`
	Limit    int    `bson:"limit,omitempty" json:"limit,omitempty"`
}

// PeekResponse carries the peeked entries in pop order. Items is empty when
// Found is false. There is no single-entry field: a one-entry peek answers a
// one-item list.
//
// Tagged like FindResponse, for the same reason.
type PeekResponse struct {
	Found   bool       `bson:"found" json:"found"`
	Items   []bson.Raw `bson:"items,omitempty" json:"items,omitempty"`
	Message string     `bson:"message" json:"message,omitempty"`
	// Pending and Dead count the bucket's claimed and dead entries, which a peek
	// does not show; Expired says the soonest lease has ended by this node's
	// clock, so the next claim or pop returns it first (claim-ack-spec 8.1,
	// 8.2). Each is absent when zero or false.
	Pending int64 `bson:"pending,omitempty" json:"pending,omitempty"`
	Dead    int64 `bson:"dead,omitempty" json:"dead,omitempty"`
	Expired bool  `bson:"expired,omitempty" json:"expired,omitempty"`
}

// AuthRequest is sent to authenticate a user.
type AuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	// ProtocolVersion is the ALPN major the client believes it is speaking,
	// sent as an assertion so a mismatched pair yields a named failure at
	// AUTH rather than a decode failure later (tcp-protocol-spec.md 3.1).
	// Zero means the client did not send it.
	ProtocolVersion uint32 `json:"protocol_version" bson:"protocol_version"`
	// Capabilities lists features the client understands. Unknown strings
	// are ignored, never an error. "pipeline_window=<n>" was never
	// implemented and is struck (query-pipelining spec, OC-4): it has no
	// effect and must not be sent.
	Capabilities []string `json:"capabilities" bson:"capabilities"`
	// Client is a driver name/version string, for logs and support.
	Client string `json:"client" bson:"client"`
	// Token is a resumption credential minted by a previous password AUTH.
	// A non-empty Token means token semantics and the Password in the same
	// frame is ignored entirely (spec 3.2 rule 3). Appended last with
	// omitempty so a password AUTH frame stays byte-identical on the wire.
	Token string `json:"token,omitempty" bson:"token,omitempty"`
}

// AuthResponse is returned after authentication.
type AuthResponse struct {
	Success bool   `json:"success"`
	Token   string `json:"token,omitempty"`
	Message string `json:"message,omitempty"`
	// TokenExpiresAt is this token's own expiry (unix seconds); a driver
	// refreshes at half of it. TokenDeadline is the absolute end of the
	// refresh chain, inherited and never extended; when it passes the pool
	// must fall back to a password. They are deliberately two fields (spec
	// 4.1: an earlier draft conflated them, which would have refreshed a
	// 900 s token hours after it died).
	TokenExpiresAt int64 `json:"token_expires_at" bson:"token_expires_at"`
	TokenDeadline  int64 `json:"token_deadline" bson:"token_deadline"`
	// ProtocolVersion is the version the server allocated to this
	// connection, so a driver can detect a mismatch at AUTH instead of
	// mid-session.
	ProtocolVersion uint32 `json:"protocol_version" bson:"protocol_version"`
	// ServerVersion is the server build version.
	ServerVersion string `json:"server_version" bson:"server_version"`
	// Capabilities is what this server advertises (tcp-protocol-spec.md 3.1).
	Capabilities []string `json:"capabilities" bson:"capabilities"`
	// PasswordMinLength is this server's password_min_length
	// (HOARDDB_PASSWORD_MIN_LENGTH). The server never receives a new user's
	// plaintext, so it cannot measure one: a client that creates a user or
	// changes a password refuses a shorter one before deriving the credential
	// (BUG-0493). A client that skips the check is not measured. Zero means
	// the server did not report a minimum.
	PasswordMinLength int32 `json:"password_min_length" bson:"password_min_length"`
}

// AuthFailResponse is the optional payload carried by an OpAuthFail frame:
// a reason code and a human-readable message. Mirrors ErrorResponse. A driver
// treats a nil payload as AuthFailCredentials, which is today's behaviour.
type AuthFailResponse struct {
	Reason  uint8  `bson:"reason"`
	Message string `bson:"message,omitempty"`
}

// HealthRequest is sent to check node health.
type HealthRequest struct{}

// HealthResponse is returned with node health information.
type HealthResponse struct {
	NodeID      string `json:"node_id"`
	Uptime      string `json:"uptime"`
	Version     string `json:"version"`
	BucketCount int    `json:"bucket_count"`
	Healthy     bool   `json:"healthy"`
	// Reasons names what makes Healthy false, one sentence each with the fix
	// where there is one. Empty when the node is healthy and from an older
	// server.
	Reasons []string `bson:"reasons,omitempty" json:"reasons,omitempty"`
	// Capacity is the node's last capacity sample, one row per resource. An
	// older server does not send it. The bson tags are explicit because the
	// fields above go on the wire as their lowercased Go names.
	Capacity *HealthCapacity `bson:"capacity,omitempty" json:"capacity,omitempty"`
	// Memtable is the node store's memtable size and why. An older server
	// does not send it.
	Memtable *HealthMemtable `bson:"memtable,omitempty" json:"memtable,omitempty"`
	// WAL is the node's write-ahead log: whether its segments are
	// preallocated and why. Absent on a node with no log and from an older
	// server.
	WAL *HealthWAL `bson:"wal,omitempty" json:"wal,omitempty"`
	// Encryption is the node's encryption at rest as the store's structure
	// shows it. An older server does not send it.
	Encryption *HealthEncryption `bson:"encryption,omitempty" json:"encryption,omitempty"`
	// Membership says whether a cluster node has been added: "confirmed", or
	// "not added yet" while its member list comes only from its seed settings
	// (join-freeze spec §5.4). Empty on a standalone server and from an older
	// one. Served while the node refuses everything else, so a script or a
	// test can wait on it.
	Membership string `bson:"membership,omitempty" json:"membership,omitempty"`
	// MembershipDetail says why, when Membership is MembershipRefused: the
	// peer that will not exchange membership with this node, and its reason.
	MembershipDetail string `bson:"membership_detail,omitempty" json:"membership_detail,omitempty"`
	// StoreRefusal is why the node store refuses writes, when it does: it
	// cannot persist them. Healthy is false while it is set.
	StoreRefusal string `bson:"store_refusal,omitempty" json:"store_refusal,omitempty"`
	// Duplicates is one sentence, with the fix, per node id this node sees two
	// live processes running under: its own, or a member's (BUG-0600). Not a
	// reason the node is unhealthy: the node itself may be fine. Empty when
	// there are none and from an older server.
	Duplicates []string `bson:"duplicates,omitempty" json:"duplicates,omitempty"`
	// PasswordKey is a ring cluster member's password key status (ring user store spec 5.2):
	// held, fetching, none_yet or witness. Empty on a node that is not a cluster member and from
	// an older server.
	PasswordKey string `bson:"password_key,omitempty" json:"password_key,omitempty"`
	// Host is the machine this node runs on: CPUs, memory and load. It is read
	// per node and costs the same at any bucket count. It is served to an
	// authenticated HEALTH_CHECK caller only; the HTTP probes never carry it.
	// An older server does not send it.
	Host *HealthHost `bson:"host,omitempty" json:"host,omitempty"`
}

// HealthHost is what a node reports about the machine it runs on. The
// numbers are what this process can use, not what the machine has, when a
// container limits it: a node in a two-CPU, 4 GiB container reports two and
// 4 GiB on a 64-core host. The tags are explicit because untagged fields
// travel lower-cased without underscores.
type HealthHost struct {
	// CPUs is how many CPUs the process may run on: runtime.NumCPU (which
	// honours the CPU affinity mask), reduced to the cgroup CPU quota,
	// rounded up, when one is set. At least 1.
	CPUs int `bson:"cpus" json:"cpus"`
	// HostCPUs is runtime.NumCPU, present only when a cgroup quota made CPUs
	// smaller than it.
	HostCPUs int `bson:"host_cpus,omitempty" json:"host_cpus,omitempty"`
	// MemoryTotalBytes is the memory the node can use: the cgroup memory
	// limit when one below the host's is set, else the host's MemTotal.
	// MemorySource says which. Zero (absent) off Linux or when /proc cannot
	// be read.
	MemoryTotalBytes int64 `bson:"memory_total_bytes,omitempty" json:"memory_total_bytes,omitempty"`
	// MemoryUsedBytes is MemoryTotalBytes minus what can be handed out
	// without swapping: MemTotal - MemAvailable for the host, and the cgroup's
	// memory.current less its inactive file cache for a cgroup limit. It
	// covers everything in that scope, not only this process.
	MemoryUsedBytes int64 `bson:"memory_used_bytes,omitempty" json:"memory_used_bytes,omitempty"`
	// MemorySource is MemorySourceHost or MemorySourceCgroup.
	MemorySource string `bson:"memory_source,omitempty" json:"memory_source,omitempty"`
	// Load is the machine's load average, from /proc/loadavg. It is the
	// host's, not the cgroup's: the kernel keeps no per-cgroup load average.
	// Absent when it cannot be read, so an idle 0.00 is not mistaken for it.
	Load *HealthLoad `bson:"load,omitempty" json:"load,omitempty"`
}

// The values of HealthHost.MemorySource.
const (
	MemorySourceHost   = "host"
	MemorySourceCgroup = "cgroup"
)

// HealthLoad is the 1, 5 and 15 minute load averages.
type HealthLoad struct {
	OneMinute      float64 `bson:"one_minute" json:"one_minute"`
	FiveMinutes    float64 `bson:"five_minutes" json:"five_minutes"`
	FifteenMinutes float64 `bson:"fifteen_minutes" json:"fifteen_minutes"`
}

// The values of HealthResponse.Membership.
const (
	MembershipConfirmed   = "confirmed"
	MembershipNotAddedYet = "not added yet"
	// MembershipRefused is a node that cannot be confirmed because a member
	// and it refuse to exchange membership (an encryption-mode mismatch);
	// 'join' would not help.
	MembershipRefused = "membership refused"
	// MembershipWaitingForFounders is a founder of a new cluster that has not
	// heard every founder in its seed list yet; MembershipDetail lists them.
	MembershipWaitingForFounders = "waiting for founders"
)

// HealthEncryption is what the CLI's encryption verb prints. It names modes,
// key ids and file names, never key bytes (spec §6.5).
type HealthEncryption struct {
	// Mode is encryption.mode: off, on or require. Keystore is
	// encryption.keystore: off, keyfile, command or systemd-creds.
	Mode     string `bson:"mode" json:"mode"`
	Keystore string `bson:"keystore" json:"keystore"`
	// Sealed is the structural check's result, computed once after the store
	// opened, not the configured mode (spec §5.3). Problem says why it is
	// false when the check could not confirm a store that should be sealed,
	// and is empty for a plaintext node.
	Sealed  bool   `bson:"sealed" json:"sealed"`
	Problem string `bson:"problem,omitempty" json:"problem,omitempty"`
	// KEKKid and NodeMasterKid are key ids, empty when the keystore is off.
	KEKKid        string `bson:"kek_kid,omitempty" json:"kek_kid,omitempty"`
	NodeMasterKid string `bson:"node_master_kid,omitempty" json:"node_master_kid,omitempty"`
	// NotSealed lists what this node writes in the clear whatever the mode
	// (spec §1.2), so the verb cannot overstate what sealed means.
	NotSealed []string `bson:"not_sealed,omitempty" json:"not_sealed,omitempty"`
}

// HealthMemtable is status's view of the memtable the node store was opened
// at (memtable-host-scaled-design.md §5.3).
type HealthMemtable struct {
	// MB is the size applied at this start.
	MB int `bson:"mb" json:"mb"`
	// Source says why: host, no-limit-read or upgrade for the store's own
	// choice, config or env for an explicit setting.
	Source string `bson:"source" json:"source"`
	// ChoiceMB is the store's own choice, which applies when nothing is set.
	ChoiceMB int `bson:"choice_mb" json:"choice_mb"`
	// ChosenFromLimitBytes is the memory limit the choice was made from, 0
	// when none was read. ChosenAt is the Unix time it was made.
	ChosenFromLimitBytes int64 `bson:"chosen_from_limit_bytes" json:"chosen_from_limit_bytes"`
	ChosenAt             int64 `bson:"chosen_at" json:"chosen_at"`
	// LimitBytes is this start's memory limit, 0 when it could not be read.
	LimitBytes int64 `bson:"limit_bytes" json:"limit_bytes"`
	// BatchBytes is the largest atomic batch at MB.
	BatchBytes int64 `bson:"batch_bytes" json:"batch_bytes"`
	// Large is true when the start logged that MB is large for LimitBytes.
	Large bool `bson:"large" json:"large"`
	// Differs lists the cluster members gossiping a different size.
	Differs []MemtablePeer `bson:"differs,omitempty" json:"differs,omitempty"`
}

// HealthWAL is status's view of the write-ahead log's segments
// (storage.wal_preallocate).
type HealthWAL struct {
	// Preallocated is the mode in effect.
	Preallocated bool `bson:"preallocated" json:"preallocated"`
	// Configured is the value of storage.wal_preallocate: off, on or auto.
	Configured string `bson:"configured" json:"configured"`
	// Reason is why auto chose as it did, empty for off and on.
	Reason string `bson:"reason,omitempty" json:"reason,omitempty"`
	// SpareReady is whether a filled next segment is waiting.
	SpareReady bool `bson:"spare_ready" json:"spare_ready"`
	// Refusal is why the log refuses writes now, empty when it does not.
	Refusal string `bson:"refusal,omitempty" json:"refusal,omitempty"`
}

// MemtablePeer is a member and the memtable size it runs.
type MemtablePeer struct {
	NodeID string `bson:"node_id" json:"node_id"`
	MB     int    `bson:"mb" json:"mb"`
}

// HealthCapacity is status's view of the capacity sampler's last sample.
type HealthCapacity struct {
	// SampledAt is the Unix time of the last completed sample, 0 before the
	// first. Rows older than three sample intervals read "stale".
	SampledAt int64         `bson:"sampled_at" json:"sampled_at"`
	Rows      []CapacityRow `bson:"rows" json:"rows"`
}

// CapacityRow is one resource's capacity in status: fds, mappings, memory or
// disk. State is ok, approaching, red, unavailable or stale.
type CapacityRow struct {
	Resource string `bson:"resource" json:"resource"`
	// Ratio is nil when the resource could not be read, so an unavailable row
	// never carries a healthy-looking 0.
	Ratio     *float64 `bson:"ratio,omitempty" json:"ratio,omitempty"`
	Threshold float64  `bson:"threshold" json:"threshold"`
	State     string   `bson:"state" json:"state"`
}

// NodeListRequest asks for the cluster's nodes. An empty payload asks for the
// nodes alone, as History unset does.
type NodeListRequest struct {
	// History asks for the answering node's membership history too: the
	// member set of each epoch it remembers, for 'nodes history'.
	History bool `bson:"history,omitempty" json:"history,omitempty"`
}

// NodeListResponse is returned with the list of known cluster nodes.
type NodeListResponse struct {
	Nodes []*NodeInfo `json:"nodes"`
	// MemberCount is the number of members in the answering node's own epoch
	// record, which Nodes (the known-peer table) is not: a peer that has not
	// gossiped yet is a member missing from Nodes, and a peer that gossiped
	// without being added is a row that is not a member (BUG-0708). Zero with
	// Standalone unset means the server predates the field, and is unknown.
	MemberCount int `bson:"member_count,omitempty" json:"member_count,omitempty"`
	// Epoch is the membership epoch MemberCount is read at; zero when standalone.
	Epoch uint64 `bson:"epoch,omitempty" json:"epoch,omitempty"`
	// Standalone is set when the node has no cluster at all, which is not a
	// cluster of one member.
	Standalone bool `bson:"standalone,omitempty" json:"standalone,omitempty"`
	// SchemaChanges is whether this node's view pauses schema changes while a
	// node joins, and why; nil on a standalone server (join-freeze spec §7.2).
	SchemaChanges *SchemaChangesReport `bson:"schema_changes,omitempty" json:"schema_changes,omitempty"`
	// MembershipHistory is, when asked, the member set of each membership
	// epoch the answering node remembers, oldest first: at most the last 64.
	MembershipHistory []MembershipRecord `bson:"membership_history,omitempty" json:"membership_history,omitempty"`
	// HistoryReported is set when the answer carries the history, so an
	// empty history is told apart from a node that does not report one.
	HistoryReported bool `bson:"history_reported,omitempty" json:"history_reported,omitempty"`
}

// SchemaChangesReport is what 'nodes' prints about the join freeze: whether
// schema changes are paused on the answering node's view, by which joiners,
// what the pause waits on, how far the copying has got, the copies to make by
// hand when auto-rebalance is off, and the refusal's text. SettledAt is the
// lowest epoch every member has settled.
type SchemaChangesReport struct {
	Paused    bool              `bson:"paused" json:"paused"`
	Epoch     uint64            `bson:"epoch" json:"epoch"`
	SettledAt uint64            `bson:"settled_at" json:"settled_at"`
	Joiners   []string          `bson:"joiners,omitempty" json:"joiners,omitempty"`
	WaitingOn []WaitedOnMember  `bson:"waiting_on,omitempty" json:"waiting_on,omitempty"`
	Stalled   []string          `bson:"stalled,omitempty" json:"stalled,omitempty"`
	Copied    int               `bson:"copied" json:"copied"`
	Total     int               `bson:"total" json:"total"`
	Migrate   []PlacementToCopy `bson:"migrate,omitempty" json:"migrate,omitempty"`
	Message   string            `bson:"message,omitempty" json:"message,omitempty"`
}

// WaitedOnMember is a member the paused schema changes wait on, and its state.
type WaitedOnMember struct {
	Node  string `bson:"node" json:"node"`
	State string `bson:"state" json:"state"`
}

// DeleteRequest is sent to delete a document by key.
type DeleteRequest struct {
	Database string `json:"database"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	// ExpireIf makes the delete conditional: a TTL sweep's expiry, which
	// removes the document only if it is still expired when the delete
	// applies, on each node that applies it (GAP-14 build doc Step 16).
	ExpireIf *ExpireCondition `bson:"expire_if,omitempty" json:"expire_if,omitempty"`
	// RepairBound is node-internal: only a consistent get's repair of a key
	// its owner no longer holds sets it, and a client frame carrying it is
	// refused. It is exclusive — the delete applies only while the key's
	// stored version is below it — so 0, the field absent, is an unbounded
	// delete, as every other delete is. Each node that applies the delete
	// compares it with its own stored version.
	RepairBound uint64 `bson:"repair_bound,omitempty" json:"repair_bound,omitempty"`
}

// ExpireCondition is a TTL expiry's bound, computed once on the bucket's
// owner: the document goes only if Field holds a date at or before DateBefore
// or a number of Unix seconds at or before SecondsBefore.
type ExpireCondition struct {
	Field         string             `bson:"field" json:"field"`
	DateBefore    primitive.DateTime `bson:"date_before" json:"date_before"`
	SecondsBefore int64              `bson:"seconds_before" json:"seconds_before"`
}

// DeleteResponse is returned after a successful delete.
type DeleteResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	// Spared reports that a conditional delete found its document no longer
	// expired and left it.
	Spared bool `bson:"spared,omitempty" json:"spared,omitempty"`
	// Skipped reports that a conditional delete found its document expired
	// and left it whole, because its store fields hold more elements than one
	// batch deletes. Message says which, and how to empty them.
	Skipped           bool   `bson:"skipped,omitempty" json:"skipped,omitempty"`
	CopiesConfirmed   int    `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int    `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
}

// CreateDatabaseRequest is sent to create a new database.
type CreateDatabaseRequest struct {
	Name string `json:"name"`

	// Generation is the incarnation this create makes, minted by the node the
	// client reached before the create is logged, and DroppedGenerations every
	// incarnation of the name that node knows to be dropped, which a receiver
	// applies first (the replication-database-ddl-design record G-1, G-2).
	// Internode only: a client's values are overwritten (G-9). Absent on an
	// entry logged before generations existed. The bson tags are not optional:
	// the codec refuses a uint64 above MaxInt64, so the fields are int64.
	Generation         int64   `bson:"generation,omitempty" json:"-"`
	DroppedGenerations []int64 `bson:"dropped_generations,omitempty" json:"-"`
}

// CreateDatabaseResponse is returned after creating a database.
type CreateDatabaseResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// BucketType enumerates the supported bucket storage types.
type BucketType string

const (
	// BucketTypeHash is a key-value hash table bucket.
	BucketTypeHash BucketType = "hash"
	// BucketTypeBTree is a sorted key-value bucket.
	BucketTypeBTree BucketType = "btree"
	// BucketTypeFIFO is a first-in-first-out queue bucket.
	BucketTypeFIFO BucketType = "fifo"
	// BucketTypeLIFO is a last-in-first-out stack bucket.
	BucketTypeLIFO BucketType = "lifo"
	// BucketTypeHeap is a priority queue bucket.
	BucketTypeHeap BucketType = "heap"
	// BucketTypeBlob is a binary data bucket.
	BucketTypeBlob BucketType = "blob"
)

// CreateBucketRequest is sent to create a new bucket within a database.
type CreateBucketRequest struct {
	Database   string                 `json:"database"`
	Name       string                 `json:"name"`
	Type       BucketType             `json:"type"`
	SchemaJSON string                 `json:"schema_json,omitempty"`
	Options    map[string]interface{} `json:"options,omitempty"`
	Encrypted  bool                   `json:"encrypted,omitempty"`
	// Term is the membership epoch the create was logged under, the bucket's
	// first term (replication spec §20.8.1, A-9.1). The bucket's owner sets it
	// before the append, so its own apply, every replica's and every replay
	// read the same number. Internode only: a client's value is overwritten. A
	// create logged before the field gives term 0.
	Term uint64 `bson:"term,omitempty" json:"-"`
	// Incarnation is the bucket's identity across its copies (replication
	// dispatch 10, R8-A), minted by the owner in the same stamp as Term.
	// Internode only: a client's value is overwritten. A create logged before
	// the field gives 0.
	Incarnation uint64 `bson:"incarnation,omitempty" json:"-"`

	// ReplicationFactor is how many copies of this bucket the cluster keeps.
	// Zero means the bucket declared nothing and inherits the node default;
	// it does not mean zero copies.
	//
	// The bson tag is not optional. The codec lowercases an untagged Go field
	// name, so this would otherwise travel as "replicationfactor" — and a wire
	// name is permanent once a driver has seen it. frameMeta already accepts
	// both "writeconcern" and "write_concern" because the tag was left off.
	ReplicationFactor int `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`

	// DatabaseGeneration is the incarnation of the database the bucket is
	// created in, stamped by the node the client reached, with
	// DroppedGenerations, the incarnations of the database that node knows to
	// be dropped, which the owner applies before it creates anything
	// (SA-DG-14; G-5, G-6). Internode only: a client's values are overwritten
	// (G-9). Absent on a create logged before generations existed, which is
	// read as the database's legacy generation.
	DatabaseGeneration int64   `bson:"db_gen,omitempty" json:"-"`
	DroppedGenerations []int64 `bson:"dropped_generations,omitempty" json:"-"`

	// Part and PartOf mark the create of a part of a spread collection: the
	// part number, and the incarnation of the template the part belongs to.
	// Internode only: the router builds the create, and a client frame that
	// names a part or sets either is refused before it is routed (B7). A part
	// is recorded whole.
	Part   int32  `bson:"part,omitempty" json:"-"`
	PartOf uint64 `bson:"part_of,omitempty" json:"-"`
}

// CreateBucketResponse is returned after creating a bucket.
type CreateBucketResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	// Placement is what a hash bucket was recorded with, "spread" or
	// "whole"; absent for every other type and for a refusal. The tag is
	// explicit because a wire name is permanent once a driver has seen it.
	Placement string `bson:"placement,omitempty" json:"placement,omitempty"`
}

// AlterBucketRequest changes a bucket's replication factor
// (bucket-replication-rev8-spec §3.2). A client sends Database, Name and
// ReplicationFactor. The bucket's owner stamps the other three before the
// entry is logged, replacing anything a client sent under their names, so
// that every member decides from the entry alone: PreviousFactor is the
// bucket's effective factor before the change, Epoch the membership epoch the
// owner decided it owned the bucket in, and Generation the database
// generation of the bucket's incarnation. There is no Type: a store-type
// conversion is an opcode of its own.
type AlterBucketRequest struct {
	Database          string `bson:"database" json:"database"`
	Name              string `bson:"name" json:"name"`
	ReplicationFactor int32  `bson:"replication_factor" json:"replication_factor"`
	PreviousFactor    int32  `bson:"previous_factor" json:"-"`
	Epoch             uint64 `bson:"epoch" json:"-"`
	Generation        int64  `bson:"generation" json:"-"`
}

// AlterBucketResponse answers an alter (spec §4.2). Pending names the members
// an increase newly places, which will copy the bucket; Releasing the members
// a decrease un-places, whose copies will be deleted.
type AlterBucketResponse struct {
	Success           bool     `bson:"success" json:"success"`
	Message           string   `bson:"message" json:"message"`
	ReplicationFactor int32    `bson:"replication_factor" json:"replication_factor"`
	PreviousFactor    int32    `bson:"previous_factor" json:"previous_factor"`
	Pending           []string `bson:"pending,omitempty" json:"pending,omitempty"`
	Releasing         []string `bson:"releasing,omitempty" json:"releasing,omitempty"`
}

// SearchRequest is sent to search a bucket by indexed field values.
type SearchRequest struct {
	Database string                 `json:"database"`
	Bucket   string                 `json:"bucket"`
	Filter   map[string]interface{} `json:"filter"`
	Limit    int                    `json:"limit,omitempty"`
}

// SearchResponse is returned with search results from secondary indexes.
// Each result is the document's stored BSON, passed through undecoded as
// FindResponse.Data is; on the wire it is the same BSON subdocument a decoded
// map would have produced.
type SearchResponse struct {
	Results []bson.Raw `json:"results"`
	// Total is the number of documents returned, not the number that match:
	// a search has no skip and does not count past its limit.
	Total   int    `json:"total"`
	Message string `json:"message,omitempty"`
	// HasMore is true when a further matching document exists beyond Limit,
	// the same name and meaning as ScanResponse.HasMore. A search cannot be
	// paged, so the answer to a truncated search is a larger limit.
	HasMore bool `bson:"has_more" json:"has_more"`
	// IndexUsed names the index that served the search, or says it was a
	// full scan (GAP-27): a slow answer that says why it is slow.
	IndexUsed string `bson:"index_used" json:"index_used"`
}

// ScanRequest asks for one bounded page of a bucket's documents, in the
// store's natural order (spec the scan-find-all-spec record 4.2), after
// applying an optional server-side filter. Skip and Limit page over that
// order; Limit 0 means the server default. Sort and Projection are optional:
// Sort maps field names to 1 (ascending) or -1 (descending); Projection maps
// field names to 1 (include) or 0 (exclude).
type ScanRequest struct {
	Database   string                 `bson:"database" json:"database"`
	Bucket     string                 `bson:"bucket" json:"bucket"`
	Filter     map[string]interface{} `bson:"filter,omitempty" json:"filter,omitempty"`
	Skip       int                    `bson:"skip,omitempty" json:"skip,omitempty"`
	Limit      int                    `bson:"limit,omitempty" json:"limit,omitempty"`
	Sort       map[string]int         `bson:"sort,omitempty" json:"sort,omitempty"`
	Projection map[string]int         `bson:"projection,omitempty" json:"projection,omitempty"`
	// ExpectBucketID is the bucket_id of the page this one continues. Absent,
	// the scan behaves as it always has. Present, the scan is served only if
	// the name still resolves to that bucket; a drop and re-create in between
	// makes it a different bucket, and a different bucket's rows are not a
	// continuation (shared-storage spec §3.1, T-25).
	ExpectBucketID int64 `bson:"expect_bucket_id,omitempty" json:"expect_bucket_id,omitempty"`
}

// ScanResponse carries one page of a scan plus the exact match count for the
// whole bucket. Found distinguishes "the bucket has no matching documents"
// from "the bucket does not exist", the convention ListBucketsResponse uses.
// Each document is BSON: the stored bytes untouched for a scan with no
// filter, sort or projection, and the filtered/projected document re-encoded
// otherwise.
type ScanResponse struct {
	Found     bool       `bson:"found" json:"found"`
	Database  string     `bson:"database" json:"database"`
	Bucket    string     `bson:"bucket" json:"bucket"`
	Documents []bson.Raw `bson:"documents" json:"documents"`
	Total     int64      `bson:"total" json:"total"`
	HasMore   bool       `bson:"has_more" json:"has_more"`
	Message   string     `bson:"message,omitempty" json:"message,omitempty"`
	// BucketID names the bucket that served the page. It is opaque: a client
	// echoes it as ScanRequest.ExpectBucketID and never does arithmetic on it.
	BucketID int64 `bson:"bucket_id,omitempty" json:"bucket_id,omitempty"`
	// IndexUsed is always "none — full scan": find has no index path in this
	// release, and saying so is what tells a developer why find is slower
	// than search (GAP-27, index-parity build doc RD-9).
	IndexUsed string `bson:"index_used,omitempty" json:"index_used,omitempty"`
}

// LoginRequest is sent to authenticate a user. It is the client-side twin
// of AuthRequest: the same wire keys over BSON, kept in step so a driver
// that marshals one and the server that unmarshals the other agrees on
// every field, including the capability negotiation added for the TCP
// driver listener (tcp-protocol-spec.md 3.1).
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	// ProtocolVersion, Capabilities and Client mirror AuthRequest; see its
	// comments. New multi-word fields carry an explicit bson tag because
	// the BSON library lowercases Go field names when none is given, which
	// would silently mismatch the snake_case names every driver documents.
	ProtocolVersion uint32   `json:"protocol_version" bson:"protocol_version"`
	Capabilities    []string `json:"capabilities" bson:"capabilities"`
	Client          string   `json:"client" bson:"client"`
}

// LoginResponse is returned with an auth token. Client-side twin of
// AuthResponse; the new fields advertise the negotiated protocol version
// and server capabilities.
type LoginResponse struct {
	Token    string `json:"token"`
	Username string `json:"username"`
	Message  string `json:"message,omitempty"`
	// ProtocolVersion, ServerVersion and Capabilities mirror AuthResponse.
	ProtocolVersion uint32   `json:"protocol_version" bson:"protocol_version"`
	ServerVersion   string   `json:"server_version" bson:"server_version"`
	Capabilities    []string `json:"capabilities" bson:"capabilities"`
}

// ErrorResponse is the payload carried by an OpError frame. MaxResultBytes
// and RetainedBytes are present only with RESULT_TOO_LARGE from a query's
// accounting (query-pipelining spec W-3): the node's ceiling, and the encoded
// bytes the query held when it stopped, a lower bound on what it needed. They
// are optional and additive, so a driver that does not know them still reads
// code and message.
type ErrorResponse struct {
	Code           int    `bson:"code"    json:"code"`
	Message        string `bson:"message" json:"message"`
	MaxResultBytes int64  `bson:"max_result_bytes,omitempty" json:"max_result_bytes,omitempty"`
	RetainedBytes  int64  `bson:"retained_bytes,omitempty" json:"retained_bytes,omitempty"`
	// WriteOutcome is set on an error for a data write (isDataWrite), on a
	// standalone node as on a cluster: "not_applied" (nothing was stored;
	// retry freely), "applied" (stored on the owner; do not retry a push or
	// an $inc) or "unknown" (it may be stored). A standalone node leaves it
	// empty where its store failed the apply, which a client reads as unknown.
	// It is never set on a read or on DDL. CopiesConfirmed and
	// ReplicationFactor are set with "applied" (spec §19.3.11).
	WriteOutcome      string `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
	CopiesConfirmed   int    `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int    `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	// ConstraintRefusal marks an INVALID_ARGUMENT that is a document-level
	// refusal deterministic on every copy — a path refusal (typed-fields spec
	// E-14 to E-16) — rather than a malformed request. Replay reads the
	// encoded frame, not the Go error, so the mark travels in the frame
	// (build doc SA-4): replay counts the entry rejected, not skipped.
	ConstraintRefusal bool `bson:"constraint_refusal,omitempty" json:"constraint_refusal,omitempty"`
	// Results is, on a refused bulk write, one entry per request position:
	// the position the refusal names carries its code and every other
	// position is not_applied (build doc BD-5).
	Results []WriteResult `bson:"results,omitempty" json:"results,omitempty"`
}

// BatchInsertRequest is sent to insert multiple documents in one call.
type BatchInsertRequest struct {
	Database string                   `json:"database"`
	Bucket   string                   `json:"bucket"`
	Data     []map[string]interface{} `json:"data"`
	// WriteConcern sets durability per request: "one", "majority" or "all".
	// Empty means the node's configured default.
	WriteConcern string `json:"write_concern,omitempty"`
	// verb is the write's intent, as InsertRequest.verb.
	verb writeVerb
}

// BatchInsertResponse is returned after a batch insert.
type BatchInsertResponse struct {
	IDs               []string `json:"ids"`
	Count             int32    `json:"count"`
	Success           bool     `json:"success"`
	Message           string   `json:"message,omitempty"`
	CopiesConfirmed   int      `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int      `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string   `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
	// Results is one entry per request position, in request order.
	Results []WriteResult `bson:"results,omitempty" json:"results,omitempty"`
}

// WriteResult is the outcome of one position of a bulk write. It rides on a
// BatchInsertResponse when the batch was applied and on an ErrorResponse when
// it was refused, so a caller finds the document a refusal names by position
// instead of parsing a sentence.
type WriteResult struct {
	// ID is the document's _id_, minted or supplied.
	ID string `bson:"id" json:"id"`
	// WriteOutcome is "applied" or "not_applied".
	WriteOutcome string `bson:"write_outcome" json:"write_outcome"`
	// Code is the transport.ErrorCode that refused this position, 0 for none.
	Code int32 `bson:"code,omitempty" json:"code,omitempty"`
	// Message says why this position was refused, or that a later position of
	// the same batch replaced it.
	Message string `bson:"message,omitempty" json:"message,omitempty"`
}

// ListDatabasesRequest asks which databases exist. On a cluster the node the
// client reached asks every member and merges the answers (BUG-0468).
type ListDatabasesRequest struct{}

// DatabaseInfo describes a single database on the node.
type DatabaseInfo struct {
	Name        string `json:"name"`
	BucketCount int    `json:"bucket_count"`
}

// ListDatabasesResponse is returned with every database the node holds,
// ordered by name.
type ListDatabasesResponse struct {
	Databases []DatabaseInfo `json:"databases"`
	Message   string         `json:"message,omitempty"`
}

// ListBucketsRequest asks for the buckets inside one database.
type ListBucketsRequest struct {
	Database string `json:"database"`
}

// BucketInfo describes a single bucket.
type BucketInfo struct {
	Name    string     `json:"name"`
	Type    BucketType `json:"type"`
	Entries int64      `json:"entries"`
	// Stores names the bucket's declared store fields — names only, so a
	// listing costs O(buckets × declared names), never O(elements).
	Stores []string `json:"stores,omitempty"`
	// ReplicationFactor is the number of copies this node believes the bucket
	// has: the factor it declared, else the node's own. Inherited says which. It
	// is read from this node's state alone (no ring lookup, no request to
	// another node), so a listing stays O(buckets in the database). The wire
	// names are pinned: an untagged field would travel as "replicationfactor".
	ReplicationFactor int  `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	Inherited         bool `bson:"inherited,omitempty" json:"inherited,omitempty"`
	// Outside says this node's copy is outside the bucket's replica set since
	// a membership change: it is kept until every member of the set holds what
	// it holds, and then released (replication spec §20.8.7). Read
	// from the node's placement marks, so a listing stays O(buckets in the
	// database). The CLI prints it as "leaving".
	Outside bool `bson:"outside,omitempty" json:"outside,omitempty"`
	// Pending and Dead count a queue's or heap's claimed and dead entries, so
	// an operator finds the buckets that hold dead entries in one listing. They
	// come from the state document the entry count is read from, and are absent
	// at zero.
	Pending int64 `bson:"pending,omitempty" json:"pending,omitempty"`
	Dead    int64 `bson:"dead,omitempty" json:"dead,omitempty"`

	// InSync, State, NotInSync and PendingCopies are the bucket's owner's
	// view of its copies (replication spec §12.1). They are filled on the
	// owner's row only, from what the owner holds in memory, with no request
	// to another member; a node that is not the owner says State
	// "owner-elsewhere" and nothing else. An older server sends none of them.
	//
	// InSync is 1 plus the bucket's addressees whose stream from the owner is
	// not stalled. State is one of the State* constants. NotInSync names the
	// addressees whose stream is stalled. PendingCopies are the members still
	// to receive their copy, with the time the alter that placed them applied.
	// The spec calls the last one "pending"; BucketInfo's Pending is the
	// claimed-entries count above, so the wire name is pending_copies.
	InSync        int           `bson:"in_sync,omitempty" json:"in_sync,omitempty"`
	State         string        `bson:"state,omitempty" json:"state,omitempty"`
	NotInSync     []string      `bson:"not_in_sync,omitempty" json:"not_in_sync,omitempty"`
	PendingCopies []PendingCopy `bson:"pending_copies,omitempty" json:"pending_copies,omitempty"`
}

// The values of BucketInfo.State. Describe bucket adds two, StateOwnerUnreachable
// and StateTakingOver, when it cannot read the owner's view.
const (
	// StateOK says every addressee is in sync and the factor is met.
	StateOK = "ok"
	// StateCopying says at least one member is still to receive its copy.
	StateCopying = "copying"
	// StateUnderReplicated says a stream is stalled or there are fewer members
	// than the factor.
	StateUnderReplicated = "under-replicated"
	// StateOwnerElsewhere says this node is not the bucket's owner, so it
	// holds no counts.
	StateOwnerElsewhere = "owner-elsewhere"
	// StateStandalone says the node has no other member to copy to.
	StateStandalone = "standalone"
	// StateOwnerUnreachable says describe bucket could not reach the owner and
	// answered from this node's own view.
	StateOwnerUnreachable = "owner-unreachable"
	// StateTakingOver says the owner is taking the bucket over and has not
	// finished.
	StateTakingOver = "taking-over"
)

// PendingCopy is one member that has not received its copy of a bucket yet.
type PendingCopy struct {
	// Node is the member's id.
	Node string `bson:"node" json:"node"`
	// Since is when the alter that placed the member applied, in Unix seconds
	// on the owner's clock.
	Since int64 `bson:"since" json:"since"`
}

// BucketInfoRequest asks for one bucket's declaration (OpBucketInfo).
type BucketInfoRequest struct {
	Database string `json:"database"`
	Bucket   string `json:"bucket"`
}

// BucketInfoResponse is one bucket's BucketInfo, the row ListBuckets would
// give for it. It is answered without walking a store, so it costs the same
// for a bucket whose store fields hold a million elements as for an empty
// one. A missing bucket or database is an ERROR frame, as for BUCKET_STATS.
type BucketInfoResponse struct {
	Found    bool       `bson:"found" json:"found"`
	Database string     `bson:"database" json:"database"`
	Bucket   BucketInfo `bson:"bucket" json:"bucket"`
	// StoreFields is each declared store field with its engine, in
	// declaration order: the same top-level set as Bucket.Stores, which keeps
	// its names. Read from the catalog, so it costs O(declared stores). It
	// sits here and not on BucketInfo because ListBuckets shares that row.
	StoreFields []StoreFieldInfo `bson:"store_fields,omitempty" json:"store_fields,omitempty"`
}

// StoreFieldInfo names one declared store field and its engine. Engine is
// spelled as BUCKET_STATS spells it: hash, btree, fifo, lifo, heap or blob.
type StoreFieldInfo struct {
	Name   string `bson:"name" json:"name"`
	Engine string `bson:"engine" json:"engine"`
}

// ListBucketsResponse is returned with every bucket in a database, ordered
// by name.
type ListBucketsResponse struct {
	Found    bool         `json:"found"`
	Database string       `json:"database"`
	Buckets  []BucketInfo `json:"buckets"`
	Message  string       `json:"message,omitempty"`
	// Encrypted is the node's state, "yes" or "no", the same for every bucket
	// in the list (spec §5.3). An older server sends none.
	Encrypted string `bson:"encrypted,omitempty" json:"encrypted,omitempty"`
}

// DeleteDatabaseRequest asks to drop a database and every bucket in it.
type DeleteDatabaseRequest struct {
	Name string `json:"name"`

	// Generations are the incarnations this drop removes: those live on the
	// node the client reached, or those a node learned and logs again (G-3,
	// G-12). Internode only, overwritten when a client sends them (G-9);
	// absent on a drop logged before generations existed.
	Generations []int64 `bson:"generations,omitempty" json:"-"`
}

// DeleteDatabaseResponse is returned after dropping a database.
type DeleteDatabaseResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// DeleteBucketRequest asks to drop a single bucket.
type DeleteBucketRequest struct {
	Database string `json:"database"`
	Name     string `json:"name"`

	// PreviousFactor is stamped by the bucket's owner, before the drop is
	// logged, when the bucket declared a factor: it is the set the drop must
	// reach, which the drop itself forgets. Nothing but the delivery filter
	// reads it (server/drop_delivery.go).
	PreviousFactor int32 `bson:"previous_factor,omitempty" json:"previous_factor,omitempty"`

	// Incarnation is the incarnation the drop removes (replication dispatch
	// 10, R8-C), stamped by the owner from its own copy before the drop is
	// logged. A member applies the drop to a copy of exactly that incarnation
	// and to nothing else. Internode only: a client's value is overwritten. A
	// drop logged before the field names 0, which matches only a copy of 0.
	Incarnation uint64 `bson:"incarnation,omitempty" json:"-"`
}

// DeleteBucketResponse is returned after dropping a bucket.
type DeleteBucketResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// CountRequest asks for the number of entries in a bucket, optionally filtered.
// If Filter is provided, Count returns the number of matching documents.
type CountRequest struct {
	Database string                 `json:"database"`
	Bucket   string                 `json:"bucket"`
	Filter   map[string]interface{} `json:"filter,omitempty"`
}

// CountResponse carries the entry count of a bucket.
type CountResponse struct {
	Count   int64  `json:"count"`
	Message string `json:"message,omitempty"`
	// Pending, Dead and Expired are PeekResponse's, for an unfiltered count of
	// a queue or heap. They carry bson tags because the wire codec ignores json
	// tags, and a member written in this struct's style would be encoded with no
	// omitempty and make every idle answer longer.
	Pending int64 `bson:"pending,omitempty" json:"pending,omitempty"`
	Dead    int64 `bson:"dead,omitempty" json:"dead,omitempty"`
	Expired bool  `bson:"expired,omitempty" json:"expired,omitempty"`
}

// DistinctRequest asks for the distinct values of a field in a bucket,
// optionally filtered.
type DistinctRequest struct {
	Database string                 `json:"database"`
	Bucket   string                 `json:"bucket"`
	Field    string                 `json:"field"`
	Filter   map[string]interface{} `json:"filter,omitempty"`
}

// DistinctResponse carries the distinct values of a field.
type DistinctResponse struct {
	Values  []interface{} `json:"values"`
	Message string        `json:"message,omitempty"`
}

// UpdateRequest applies update operators to one existing document.
// The document is addressed by Key and the operators are applied in the
// order Set, Unset, Inc, Push, AddToSet, Mul, Min, Max, Rename.
// Multi allows updating multiple documents when combined with Filter.
type UpdateRequest struct {
	Database string                 `json:"database"`
	Bucket   string                 `json:"bucket"`
	Key      string                 `json:"key,omitempty"`
	Set      map[string]interface{} `json:"set,omitempty"`
	Unset    []string               `json:"unset,omitempty"`
	Inc      map[string]int64       `json:"inc,omitempty"`
	Push     map[string]interface{} `json:"push,omitempty"`
	AddToSet map[string]interface{} `json:"add_to_set,omitempty"`
	Mul      map[string]float64     `json:"mul,omitempty"`
	Min      map[string]interface{} `json:"min,omitempty"`
	Max      map[string]interface{} `json:"max,omitempty"`
	Rename   map[string]string      `json:"rename,omitempty"`
	Filter   map[string]interface{} `json:"filter,omitempty"`
	Multi    bool                   `json:"multi,omitempty"`
	// MaxDocumentBytes is the document limit the update is held to. The node a
	// client's update enters stamps it (server/document_limit.go), so every
	// applier of the entry gives the same answer; a value the client sends is
	// replaced.
	MaxDocumentBytes int `json:"max_document_bytes,omitempty" bson:"max_document_bytes,omitempty"`
	// Admitted is set only by the router of a spread bucket on its own write
	// of the template's born set (spread_route.go): that document is the
	// router's, not the collection's, so the bucket's typed-field declaration
	// does not judge it. The client door refuses a request that sets it
	// (E-12); replication and replay keep it, so every copy applies it alike.
	Admitted bool `bson:"admitted,omitempty" json:"admitted,omitempty"`
}

// UpdateResponse is returned after applying an update.
type UpdateResponse struct {
	Success           bool                   `json:"success"`
	Doc               map[string]interface{} `json:"doc,omitempty"`
	Message           string                 `json:"message,omitempty"`
	CopiesConfirmed   int                    `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int                    `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string                 `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
	// DocOmitted is set only on an update answered inside a BATCH whose reply
	// budget was full: the update is applied, and its document was left out
	// (batch-request spec §4.5). Read the document if it is needed.
	DocOmitted bool `bson:"doc_omitted,omitempty" json:"doc_omitted,omitempty"`
}

// BatchItem is one item of a BATCH request: the opcode and payload of the
// request the item would send on its own (batch-request spec §3.2).
type BatchItem struct {
	Opcode  int32    `bson:"opcode" json:"opcode"`
	Payload bson.Raw `bson:"payload" json:"payload"`
}

// BatchRequest is a BATCH: up to 1,000 independent items, and one write
// concern for every write item. A batch is not a transaction: each item is
// applied, refused or left unknown on its own.
type BatchRequest struct {
	Items        []BatchItem `bson:"items" json:"items"`
	WriteConcern string      `bson:"writeconcern,omitempty" json:"writeconcern,omitempty"`
}

// BatchAnswer is one item's answer: the opcode and payload of the lone reply,
// an ERROR included.
type BatchAnswer struct {
	Opcode  int32    `bson:"opcode" json:"opcode"`
	Payload bson.Raw `bson:"payload" json:"payload"`
}

// BatchResponse is BATCH_OK: one answer per item, in item order. answers[i]
// answers items[i].
type BatchResponse struct {
	Answers []BatchAnswer `bson:"answers" json:"answers"`
}

// BTreeRangeRequest asks for every entry whose key falls inside a range.
type BTreeRangeRequest struct {
	Database string `json:"database"`
	Bucket   string `json:"bucket"`
	Low      string `json:"low,omitempty"`
	High     string `json:"high,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

// BTreeEntry is one entry of a range scan. Key is the key as it was written
// (control and NUL bytes included); only a key that is not valid UTF-8, which
// cannot be a BSON string, is "0x…" hex. Value is the
// stored BSON document exactly as FindResponse.Data carries a point read's:
// raw, never decoded and re-encoded on the way out.
type BTreeEntry struct {
	Key   string   `bson:"key" json:"key"`
	Value bson.Raw `bson:"value,omitempty" json:"value,omitempty"`
}

// BTreeRangeResponse carries the result of a range scan or edge lookup.
type BTreeRangeResponse struct {
	Entries []BTreeEntry `json:"entries"`
	Message string       `json:"message,omitempty"`
}

// BlobWriteRequest stores binary data in a blob bucket.
type BlobWriteRequest struct {
	Database string                 `json:"database"`
	Bucket   string                 `json:"bucket"`
	Key      string                 `json:"key,omitempty"`
	Data     []byte                 `json:"data"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// BlobWriteResponse is returned after a blob write.
type BlobWriteResponse struct {
	Success           bool   `json:"success"`
	Key               string `json:"key,omitempty"`
	BlobID            string `json:"blob_id,omitempty"`
	Size              int    `json:"size"`
	Message           string `json:"message,omitempty"`
	CopiesConfirmed   int    `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int    `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
}

// BlobReadRequest asks for a blob by its bucket key.
type BlobReadRequest struct {
	Database string `json:"database"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
}

// BlobReadResponse carries the blob payload and its metadata.
type BlobReadResponse struct {
	Found    bool                   `json:"found"`
	BlobID   string                 `json:"blob_id,omitempty"`
	Data     []byte                 `json:"data,omitempty"`
	Size     int                    `json:"size"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Message  string                 `json:"message,omitempty"`
}

// BucketStatsRequest asks for statistics about one bucket.
type BucketStatsRequest struct {
	Database string `json:"database"`
	Bucket   string `json:"bucket"`
}

// BucketStatsResponse describes a bucket's type, size and indexes.
type BucketStatsResponse struct {
	Found    bool       `json:"found"`
	Database string     `json:"database"`
	Bucket   string     `json:"bucket"`
	Type     BucketType `json:"type,omitempty"`
	Entries  int64      `json:"entries"`
	// LogicalBytes is the sum of the bucket's keys and values, before
	// compression and compaction.
	LogicalBytes int64    `bson:"logical_bytes,omitempty" json:"logical_bytes"`
	Indexes      []string `json:"indexes,omitempty"`
	Message      string   `json:"message,omitempty"`
	// Stores is the bucket's declared store fields with their element counts,
	// summed over every document, and counted per call.
	Stores []StoreStats `json:"stores,omitempty"`
	// Encrypted is the node's state, "yes" or "no": every bucket shares the
	// node's store (spec §5.3). An older server sends none.
	Encrypted string `bson:"encrypted,omitempty" json:"encrypted,omitempty"`
	// Fields are the bucket's declared typed fields, absent when it declares
	// none, and FieldTypes its mode: strict, warn or off, absent for a bucket
	// that declares neither (typed-fields spec §8.3). The tags are explicit:
	// untagged, FieldTypes would travel as "fieldtypes".
	Fields     []FieldInfo `bson:"fields,omitempty" json:"fields,omitempty"`
	FieldTypes string      `bson:"field_types,omitempty" json:"field_types,omitempty"`
	// WritesRefused says why every write to the bucket is refused while reads
	// are served: its recorded typed-field declaration was written by a newer
	// hoardDB, or fails this version's checks (typed-fields spec §8.4). Absent
	// for a bucket that takes writes.
	WritesRefused string `bson:"writes_refused,omitempty" json:"writes_refused,omitempty"`
	// ReplicationFactor is the number of copies the answering node believes
	// the bucket has, and Inherited says it is the node's own default and not
	// a factor the bucket declared: the same two fields, with the same
	// meaning, that BucketInfo carries. Absent from an older server. The wire
	// names are pinned: untagged, ReplicationFactor would travel as
	// "replicationfactor".
	ReplicationFactor int  `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	Inherited         bool `bson:"inherited,omitempty" json:"inherited,omitempty"`
	// Placement is "spread" or "whole" for a hash bucket and absent for every
	// other type, which is always whole.
	Placement string `bson:"placement,omitempty" json:"placement,omitempty"`

	// InSync, State, NotInSync and PendingCopies are the owner's view of the
	// bucket's copies, as BucketInfo carries them for `show buckets`. A
	// describe is answered by the owner, so they are the owner's: State is
	// owner-unreachable when the owner could not be asked and the answer is
	// this node's own, and taking-over while the owner has not finished taking
	// the bucket over (replication spec §12.2). Nodes are the replica set's
	// node ids, owner first.
	InSync        int           `bson:"in_sync,omitempty" json:"in_sync,omitempty"`
	State         string        `bson:"state,omitempty" json:"state,omitempty"`
	Nodes         []string      `bson:"nodes,omitempty" json:"nodes,omitempty"`
	NotInSync     []string      `bson:"not_in_sync,omitempty" json:"not_in_sync,omitempty"`
	PendingCopies []PendingCopy `bson:"pending_copies,omitempty" json:"pending_copies,omitempty"`
	// PreviousOwner is the node the owner is taking the bucket over from,
	// when it is known.
	PreviousOwner string `bson:"previous_owner,omitempty" json:"previous_owner,omitempty"`
	// PendingCopy is the line a member adds when it holds a needs-a-copy
	// record for the bucket: since when, the last attempt and why it did not
	// land, and the next retry. It is composed on the node the client asked,
	// from that node's own record, and is empty from every other.
	PendingCopy string `bson:"pending_copy,omitempty" json:"pending_copy,omitempty"`
}

// FieldInfo is one declared typed field as describe prints it.
type FieldInfo struct {
	Name string `bson:"name" json:"name"`
	// Type is the canonical text form: "int", "array<object { Sku string }>".
	Type     string `bson:"type" json:"type"`
	Required bool   `bson:"required,omitempty" json:"required,omitempty"`
	// Store is the store field the typed field belongs to; empty for the
	// bucket's own fields.
	Store string `bson:"store,omitempty" json:"store,omitempty"`
}

// PingResponse answers an OpPing keepalive.
type PingResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// Wire types for user management (spec 6.1). The creation path never carries
// a plaintext password: the client derives Credential locally and the server
// only ever stores the peppered value.

// UserRole is the wire form of one {role, db} grant.
type UserRole struct {
	Role string `bson:"role" json:"role"`
	DB   string `bson:"db" json:"db"`
}

// UserCredential is the wire form of a client-derived Argon2id credential.
// The hash is the client's raw Argon2id output; the server peppers it before
// storing (spec 6.2.1).
type UserCredential struct {
	Algo   string       `bson:"algo" json:"algo"`
	Salt   string       `bson:"salt" json:"salt"`
	Hash   string       `bson:"hash" json:"hash"`
	Params Argon2Params `bson:"params" json:"params"`
}

// UserCreateRequest creates a user. It carries no plaintext password.
type UserCreateRequest struct {
	// RequestID is a UUIDv4 the master dedups on for a bounded window
	// (spec 6.1) so a retried frame cannot double-apply.
	RequestID  string         `bson:"request_id"`
	Username   string         `bson:"username"`
	Credential UserCredential `bson:"credentials"`
	Roles      []UserRole     `bson:"roles"`
	// Enabled defaults to true when absent.
	Enabled *bool `bson:"enabled,omitempty"`
}

// UserAlterRequest replaces a user's role set and enabled flag, and optionally
// changes the password. Version is the config version the client last saw; a
// mismatch refuses the alter so a concurrent grant is not silently dropped by
// the replacement (spec 6.1). Roles, Enabled and Credentials are all
// optional-with-presence: absent means unchanged, and an explicitly empty
// Roles replaces the set with nothing. That distinction is what stops a
// credentials-only alter from silently stripping every grant (spec 12.4).
type UserAlterRequest struct {
	RequestID   string          `bson:"request_id"`
	Username    string          `bson:"username"`
	Roles       *[]UserRole     `bson:"roles,omitempty"`
	Enabled     *bool           `bson:"enabled,omitempty"`
	Version     uint64          `bson:"version"`
	Credentials *UserCredential `bson:"credentials,omitempty"`
}

// UserDropRequest removes a user. The last-admin guard applies (spec 4.7).
type UserDropRequest struct {
	RequestID string `bson:"request_id"`
	Username  string `bson:"username"`
}

// UserListRequest lists users; it never includes hashes or salts.
type UserListRequest struct{}

// UserGetRequest describes one user.
type UserGetRequest struct {
	Username string `bson:"username"`
}

// UserGrantRequest adds one {role, db} grant.
type UserGrantRequest struct {
	RequestID string   `bson:"request_id"`
	Username  string   `bson:"username"`
	Role      UserRole `bson:"role"`
}

// UserRevokeRequest removes one {role, db} grant.
type UserRevokeRequest struct {
	RequestID string   `bson:"request_id"`
	Username  string   `bson:"username"`
	Role      UserRole `bson:"role"`
}

// UserInfo is a user as the wire reports it: never a hash, salt or pepper.
type UserInfo struct {
	Username          string     `bson:"username" json:"username"`
	Roles             []UserRole `bson:"roles" json:"roles"`
	Enabled           bool       `bson:"enabled" json:"enabled"`
	Epoch             uint32     `bson:"epoch" json:"epoch"`
	CreatedAt         time.Time  `bson:"created_at" json:"created_at"`
	PasswordChangedAt time.Time  `bson:"password_changed_at" json:"password_changed_at"`
}

// UserMutationResponse is returned by create/alter/drop/grant/revoke.
type UserMutationResponse struct {
	Success bool   `bson:"success" json:"success"`
	Message string `bson:"message,omitempty" json:"message,omitempty"`
	// Version is the config version after the mutation, so the client can
	// present it to a subsequent alter.
	Version uint64 `bson:"version,omitempty" json:"version,omitempty"`
	// Changed names the fields the master actually applied. A client requires
	// the field it asked about to be present; a response carrying no list
	// comes from a server that does not know the field, so the client refuses
	// to call it success rather than let a rolling upgrade silently no-op.
	// A server that does not know the field cannot name it, which is what
	// makes this sound where the version comparison was not (spec 6.1 item 3).
	Changed []string `bson:"changed,omitempty" json:"changed,omitempty"`
}

// UserListResponse carries every user, ordered by name, plus the master that
// serves user changes and the config version.
type UserListResponse struct {
	Users   []UserInfo `bson:"users" json:"users"`
	Message string     `bson:"message,omitempty" json:"message,omitempty"`
	// Version is the config version, and Master/MasterAddr name the node that
	// serves mutations, so the 'master' verb needs no extra opcode. MasterAddr
	// is where a client connects to that node (its driver listener, tcp.listen),
	// never its ring address; it is empty until that node's address has reached
	// this one in gossip.
	Version    uint64 `bson:"version,omitempty" json:"version,omitempty"`
	Master     string `bson:"master,omitempty" json:"master,omitempty"`
	MasterAddr string `bson:"master_addr,omitempty" json:"master_addr,omitempty"`
	// MasterAddrNote says why MasterAddr is empty and what to do about it: the
	// master's address has not arrived, or it listens for clients on loopback only.
	MasterAddrNote string `bson:"master_addr_note,omitempty" json:"master_addr_note,omitempty"`
}

// UserGetResponse carries one user or a not-found flag, plus the config
// version an alter must present.
type UserGetResponse struct {
	Found   bool     `bson:"found" json:"found"`
	User    UserInfo `bson:"user" json:"user"`
	Version uint64   `bson:"version,omitempty" json:"version,omitempty"`
	Message string   `bson:"message,omitempty" json:"message,omitempty"`
}
