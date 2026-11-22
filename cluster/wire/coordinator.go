package wire

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// Sender orders one write. The owner's *Writer is one; the forwarding path of a later
// unit is another.
type Sender interface {
	// Write orders req and returns the owner's answer, or a *cluster.Error refusal.
	Write(ctx context.Context, req WriteRequest) (WriteAnswer, error)
}

// CoordinatorConfig is the collaborators of a Coordinator.
type CoordinatorConfig struct {
	// Sender orders the write: the owner's Writer, or the forwarding path to it.
	Sender Sender
	// Mint makes an _id_ for an insert that has none; nil takes cluster.MintID.
	Mint func() (string, error)
}

// Coordinator is the node or driver that first receives an insert (decisions §1). It
// is the only place an _id_ is minted, and it mints before the placement key is
// computed, so the slot is known and the owner only orders.
type Coordinator struct {
	cfg CoordinatorConfig
}

// NewCoordinator returns a Coordinator over cfg.
func NewCoordinator(cfg CoordinatorConfig) *Coordinator {
	if cfg.Mint == nil {
		cfg.Mint = cluster.MintID
	}
	return &Coordinator{cfg: cfg}
}

// InsertRequest is one insert the surface has already resolved against the
// declaration: names for the placement key, ids and field entries for the Op.
type InsertRequest struct {
	// Database and Bucket are the names the placement key is built from.
	Database, Bucket string
	// DBID is the database id the owner's pair is keyed by.
	DBID uint64
	// ID is the string _id_ the client supplied or minted; empty with no encoded id in
	// Template asks the coordinator to derive or mint one.
	ID string
	// Template carries BucketID, DeclVersion, SchemaVersionSeen and Fields. It may
	// also carry EncID and IDValue, the already-encoded _id_ of a non-string id (int,
	// date, ...), which are used as given; otherwise the coordinator sets them.
	Template cluster.Op
	// RequestID is the client's request id, kept across retries.
	RequestID [16]byte
	// OpIndex is the operation's index in its client batch, 0 for a single insert.
	OpIndex uint8
	// BatchSize is the number of operations in the client batch, 0 for a single insert.
	BatchSize int
	// Deadline is the client's remaining budget; 0 means none.
	Deadline time.Duration
	// Concern is the write concern asked for; zero takes the database's default.
	Concern machine.Concern
}

// InsertAnswer is what an insert returns: the id, always, because a minted id exists
// nowhere else.
type InsertAnswer struct {
	// ID is the document's string _id_; empty when the _id_ is not a string.
	ID string
	// EncID is the document's _id_ in the EncodeComponent encoding, always set.
	EncID []byte
	// Minted says this coordinator minted ID.
	Minted bool
	// Pos is the position the insert holds.
	Pos cluster.Position
	// Deduped says the answer was recomposed from the first attempt.
	Deduped bool
	// CopiesConfirmed counts the owner.
	CopiesConfirmed uint8
	// Outcome is the write_outcome of the owner's answer.
	Outcome machine.Outcome
	// HoldCause says why the pair is held when Held is set.
	HoldCause string
	// Forward says this node is not the owner and the caller forwards the insert.
	Forward bool
	// Held says the pair is in a hold: the insert took no position.
	Held bool
	// ServedBy names the owner that ordered a forwarded insert; empty when this node did.
	ServedBy string
}

// Insert resolves the _id_, computes its slot, and hands the Op to the Sender.
//
// An _id_ the client sent (ID, or an encoded one in Template) is used as given. When
// there is none and the request carries a RequestID, the id is cluster.DeriveID of
// (RequestID, OpIndex): a retry with the same request id gets the same id, hence the
// same slot and pair, and the pair's dedupe entry answers it with the recorded
// position and the same id (queries §1, conventions §18; this derivation amends
// their "mint at the coordinator" so that the retry stays dedupable). With no
// RequestID (a bare wire client, which has no dedupe) the id is drawn fresh from
// Mint, a retry draws another, and a second document results: the non-dedupable
// case of build G10. A failed mint is MINT_FAILED and nothing is written.
func (c *Coordinator) Insert(ctx context.Context, req InsertRequest) (InsertAnswer, error) {
	op := req.Template
	id, minted := req.ID, false
	if len(op.EncID) == 0 {
		if id == "" {
			minted = true
			var err error
			if req.RequestID != ([16]byte{}) {
				id = cluster.DeriveID(req.RequestID, req.OpIndex)
			} else if id, err = c.cfg.Mint(); err != nil {
				return InsertAnswer{}, &cluster.Error{
					Code: cluster.CodeMintFailed,
					Message: cluster.CodeMintFailed.Text(cluster.CondMintFailed, map[string]string{
						"reason": err.Error(), "bucket": req.Bucket,
					}),
				}
			}
		}
		encoded, err := store.EncodeComponent(id)
		if err != nil {
			return InsertAnswer{}, fmt.Errorf("encoding the _id_ %q of bucket %q: %w", id, req.Bucket, err)
		}
		op.EncID, op.IDValue = encoded, stringValue(id)
	}
	op.Class = cluster.OpInsert
	answer, err := c.cfg.Sender.Write(ctx, WriteRequest{
		Slot:      storage.SlotOf(storage.PlacementKey(req.Database, req.Bucket, op.EncID)),
		DBID:      req.DBID,
		Op:        op,
		RequestID: req.RequestID,
		OpIndex:   req.OpIndex,
		BatchSize: req.BatchSize,
		Deadline:  req.Deadline,
		Concern:   req.Concern,
	})
	if err != nil {
		return InsertAnswer{}, fmt.Errorf("inserting %x into bucket %q: %w", op.EncID, req.Bucket, err)
	}
	// The record is durable and the answer's Outcome says so, but a failed apply means the
	// document is not readable until the pair has replayed; Node.send refuses the same answer.
	if err := bucketApplyFailureError(req.Database, req.Bucket, answer); err != nil {
		return InsertAnswer{}, fmt.Errorf("inserting %x into bucket %q: %w", op.EncID, req.Bucket, err)
	}
	return InsertAnswer{
		ID: id, EncID: op.EncID, Minted: minted, Pos: answer.Pos, Deduped: answer.Deduped,
		CopiesConfirmed: answer.CopiesConfirmed, Outcome: answer.Outcome, Forward: answer.Forward, Held: answer.Held, HoldCause: answer.HoldCause, ServedBy: answer.ServedBy,
	}, nil
}

// stringValue is the type-tagged value of a string: tag 0x02, a 4-byte little-endian
// length counting the trailing zero, the bytes, the zero.
func stringValue(text string) []byte {
	out := make([]byte, 0, len(text)+6)
	out = append(out, 0x02)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(text)+1))
	out = append(out, text...)
	return append(out, 0)
}
