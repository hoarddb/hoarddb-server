package server

import (
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The wire types of the lease verbs (claim-ack-spec 9.2). Every member carries
// an explicit bson tag, as PopRequest's do: with json tags alone the bson
// encoding is the driver's lowercase default and omitempty is not honoured. A
// member the owner stamps carries json:"-" so a client's value never reaches
// the handler through a json decoder, and the handler ignores it unless the
// frame was logged (Server.leaseNow).

// ClaimRequest takes up to Limit entries under one lease. Limit 0 is 1;
// LeaseMs 0 is the default lease.
type ClaimRequest struct {
	Database      string `bson:"database" json:"database"`
	Bucket        string `bson:"bucket" json:"bucket"`
	Limit         int    `bson:"limit,omitempty" json:"limit,omitempty"`
	LeaseMs       int64  `bson:"lease_ms,omitempty" json:"lease_ms,omitempty"`
	LeaseNow      int64  `bson:"lease_now,omitempty" json:"-"`
	LeaseNonce    []byte `bson:"lease_nonce,omitempty" json:"-"`
	ResultCeiling int64  `bson:"result_ceiling,omitempty" json:"-"`
}

// ClaimResponse carries the claimed entries in pop order, the handle of each,
// and the lease they are held under. Found is true only when it took at least
// one entry.
type ClaimResponse struct {
	Found bool       `bson:"found" json:"found"`
	Items []bson.Raw `bson:"items,omitempty" json:"items,omitempty"`
	// ItemIDs is parallel to Items: the hex origin suffix ack names an entry by.
	ItemIDs []string `bson:"item_ids,omitempty" json:"item_ids,omitempty"`
	// Deliveries is parallel to Items and absent when every value is 1.
	Deliveries    []int32   `bson:"deliveries,omitempty" json:"deliveries,omitempty"`
	LeaseID       string    `bson:"lease_id,omitempty" json:"lease_id,omitempty"`
	LeaseMs       int64     `bson:"lease_ms,omitempty" json:"lease_ms,omitempty"`
	LeaseDeadline time.Time `bson:"lease_deadline,omitempty" json:"lease_deadline,omitempty"`
	Pending       int64     `bson:"pending,omitempty" json:"pending,omitempty"`
	Dead          int64     `bson:"dead,omitempty" json:"dead,omitempty"`
	// Returned is how many entries this apply wrote back to the queue.
	Returned          int64  `bson:"returned,omitempty" json:"returned,omitempty"`
	Message           string `bson:"message" json:"message,omitempty"`
	CopiesConfirmed   int    `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int    `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
}

// LeaseRequest is an ack, a release or an extend of one lease. ItemIDs is
// ack's alone and LeaseMs extend's alone.
type LeaseRequest struct {
	Database string   `bson:"database" json:"database"`
	Bucket   string   `bson:"bucket" json:"bucket"`
	LeaseID  string   `bson:"lease_id" json:"lease_id"`
	ItemIDs  []string `bson:"item_ids,omitempty" json:"item_ids,omitempty"`
	LeaseMs  int64    `bson:"lease_ms,omitempty" json:"lease_ms,omitempty"`
	LeaseNow int64    `bson:"lease_now,omitempty" json:"-"`
}

// LeaseResponse is a lease verb's answer. An unknown or expired lease is
// Success false with LeaseState saying which, in band, and it is never
// write_outcome not_applied: the apply that answered it may have returned
// other expired leases (claim-ack-spec 9.2, Q-7).
type LeaseResponse struct {
	Success    bool   `bson:"success" json:"success"`
	LeaseState string `bson:"lease_state" json:"lease_state"`
	Acked      int    `bson:"acked,omitempty" json:"acked,omitempty"`
	Missing    int    `bson:"missing,omitempty" json:"missing,omitempty"`
	Remaining  int    `bson:"remaining,omitempty" json:"remaining,omitempty"`
	Released   int    `bson:"released,omitempty" json:"released,omitempty"`
	// LeaseMs and LeaseDeadline are extend's: what was granted.
	LeaseMs           int64     `bson:"lease_ms,omitempty" json:"lease_ms,omitempty"`
	LeaseDeadline     time.Time `bson:"lease_deadline,omitempty" json:"lease_deadline,omitempty"`
	Message           string    `bson:"message,omitempty" json:"message,omitempty"`
	CopiesConfirmed   int       `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int       `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string    `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
}

// DeadListRequest pages a bucket's dead entries, oldest first. Limit 0 is 10.
// After is the DeadID of the last entry of the page before, exclusive.
type DeadListRequest struct {
	Database string `bson:"database" json:"database"`
	Bucket   string `bson:"bucket" json:"bucket"`
	Limit    int    `bson:"limit,omitempty" json:"limit,omitempty"`
	After    string `bson:"after,omitempty" json:"after,omitempty"`
}

// DeadListResponse is one page of dead entries. Next is the DeadID of the last
// entry listed, and is present only when more remain.
type DeadListResponse struct {
	Found   bool       `bson:"found" json:"found"`
	Items   []DeadItem `bson:"items,omitempty" json:"items,omitempty"`
	Next    string     `bson:"next,omitempty" json:"next,omitempty"`
	Message string     `bson:"message" json:"message,omitempty"`
}

// DeadItem is one dead entry: its handle, the entry itself, how many times it
// was claimed, when it died and why.
type DeadItem struct {
	DeadID     string    `bson:"dead_id" json:"dead_id"`
	Data       bson.Raw  `bson:"data" json:"data"`
	Deliveries int32     `bson:"deliveries" json:"deliveries"`
	DeadAt     time.Time `bson:"dead_at" json:"dead_at"`
	// Reason is max_deliveries or collided.
	Reason string `bson:"reason" json:"reason"`
}

// DeadMoveRequest requeues or discards one dead entry by its DeadID, or the
// first ones of the list with All. Exactly one of the two is given. After, with
// All, starts the walk past that dead_id (exclusive, as dead's after pages): a
// request considers at most 100 entries and a refused one stays at the head, so
// a caller steps past refusals by sending the last refused dead_id as After.
// Absent, it starts at the head (BUG-0637).
type DeadMoveRequest struct {
	Database string `bson:"database" json:"database"`
	Bucket   string `bson:"bucket" json:"bucket"`
	DeadID   string `bson:"dead_id,omitempty" json:"dead_id,omitempty"`
	All      bool   `bson:"all,omitempty" json:"all,omitempty"`
	After    string `bson:"after,omitempty" json:"after,omitempty"`
}

// DeadMoveResponse says what a requeue or a discard did. Success is true when
// nothing it was asked to move was refused. Left counts the dead entries the
// bucket still holds, refused ones included, so a caller that repeats an all
// request loops while Done is above 0 and never while Left is.
type DeadMoveResponse struct {
	Success           bool          `bson:"success" json:"success"`
	Done              int           `bson:"done" json:"done"`
	Left              int64         `bson:"left,omitempty" json:"left,omitempty"`
	Refused           []DeadRefusal `bson:"refused,omitempty" json:"refused,omitempty"`
	Message           string        `bson:"message,omitempty" json:"message,omitempty"`
	CopiesConfirmed   int           `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int           `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string        `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
}

// DeadRefusal is one dead entry a requeue or a discard left where it was.
type DeadRefusal struct {
	DeadID  string `bson:"dead_id" json:"dead_id"`
	Message string `bson:"message" json:"message"`
}

// DeadRestoreRequest writes one dead entry from a dump. A restore sends it;
// a client frame carrying it is refused before routing (claim-ack-spec 7.3).
type DeadRestoreRequest struct {
	Database   string    `bson:"database" json:"database"`
	Bucket     string    `bson:"bucket" json:"bucket"`
	Data       bson.Raw  `bson:"data" json:"data"`
	Deliveries int32     `bson:"deliveries" json:"deliveries"`
	DeadAt     time.Time `bson:"dead_at" json:"dead_at"`
	Reason     string    `bson:"reason" json:"reason"`
}
