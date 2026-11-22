package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// How `describe bucket` is answered (bucket-replication-rev8-spec §12.2). The
// describe is the owner's: a node that is not the owner forwards it, with the
// existing read forward and no new frame, and adds to the owner's answer what
// only it knows, its own pending copy. When the owner cannot be asked, the
// node answers from its own copy and says so; while the owner takes the bucket
// over it answers, and says so, where a read is refused. Nothing here is kept
// per bucket.

// pendingCopyLine says what this node knows of its own pending copy: since when
// its record stands, the last attempt and why it did not land, and the next
// retry. Times are UTC, to the second.
func pendingCopyLine(outcome pendingCopyOutcome) string {
	line := "pending copy here: since " + outcome.since.UTC().Format(time.RFC3339)
	if outcome.lastAttempt.IsZero() {
		line += ", not tried yet"
	} else {
		line += ", last attempt " + outcome.lastAttempt.UTC().Format(time.RFC3339) + ": " + outcome.reason
	}
	return line + ", next retry " + outcome.nextRetry.UTC().Format(time.RFC3339)
}

// forwardDescribe sends a describe to the bucket's owner and returns its answer
// with this node's own pending copy added. A describe the owner could not be
// asked is answered from this node's copy (describeWithoutOwner); any other
// failure, the owner's own refusal included, is the client's answer as it is.
func (c *Cluster) forwardDescribe(ctx context.Context, frame *transport.Frame, owner ring.Node, key placementKey, database, bucket string) *transport.Frame {
	resp, err := c.forward(ctx, owner, ReplEntry{Op: int32(frame.Opcode), Bucket: bucket, Payload: frame.Payload}, 0, readForward{})
	if errors.Is(err, ErrForwardLost) {
		c.logger.Warn("describe forwarded to owner failed", "bucket", bucket, "owner", owner.ID, "error", err)
		return c.describeWithoutOwner(ctx, frame, key, database, bucket, err)
	}
	if err != nil {
		c.logger.Warn("describe forwarded to owner failed", "bucket", bucket, "owner", owner.ID, "error", err)
		return errorFrame(frame, err)
	}
	answer := transport.NewFrameWithID(transport.Opcode(resp.Opcode), frame.RequestID, resp.Payload)
	outcome, pending := c.pendingCopyOf(key)
	if !pending || answer.Opcode != transport.OpBucketStats {
		return answer
	}
	return c.restampDescribe(frame, answer, func(described *BucketStatsResponse) {
		described.PendingCopy = pendingCopyLine(outcome)
	})
}

// describeWithoutOwner answers a describe whose owner did not answer, from this
// node's own copy when it holds one it may serve, saying the count is unknown:
// owner-unreachable. A node with no such copy has nothing to answer from and
// returns why the owner could not be asked.
func (c *Cluster) describeWithoutOwner(ctx context.Context, frame *transport.Frame, key placementKey, database, bucket string, forwardErr error) *transport.Frame {
	// A spread template's copy holds the born set and none of the
	// collection's documents: only its router describes it (B1).
	if serves, _ := c.holdsServableCopy(key, database, bucket); !serves || c.s.spreadTemplateHeld(database, bucket) {
		return errorFrame(frame, forwardErr)
	}
	return c.restampDescribe(frame, c.dispatchLocal(ctx, frame), func(described *BucketStatsResponse) {
		withheldView(described, StateOwnerUnreachable, "")
	})
}

// describeWhileTakingOver answers a describe at the owner that has not taken
// the bucket over, from its copy, where a read is refused: the count is the
// previous owner's to give, so the answer says taking-over and names it when it
// is known.
func (c *Cluster) describeWhileTakingOver(ctx context.Context, frame *transport.Frame, key placementKey) *transport.Frame {
	status := c.takeOverStatusOf(key)
	return c.restampDescribe(frame, c.dispatchLocal(ctx, frame), func(described *BucketStatsResponse) {
		previous := ""
		if status.sourceKnown {
			previous = status.source.ID
		}
		withheldView(described, StateTakingOver, previous)
	})
}

// withheldView says the owner's count is not available: it clears every part of
// the view this node's own state could have filled and names why.
func withheldView(described *BucketStatsResponse, state, previousOwner string) {
	described.InSync, described.Nodes, described.NotInSync, described.PendingCopies = 0, nil, nil, nil
	described.State, described.PreviousOwner = state, previousOwner
}

// dispatchLocal runs a client frame in this node's own server.
func (c *Cluster) dispatchLocal(ctx context.Context, frame *transport.Frame) *transport.Frame {
	answer, err := c.s.dispatch(ctx, frame)
	if err != nil {
		return errorFrame(frame, err)
	}
	return answer
}

// restampDescribe decodes a describe's answer, lets change rewrite it and
// encodes it again under the request's id. An error frame goes back as it is.
func (c *Cluster) restampDescribe(request, answer *transport.Frame, change func(*BucketStatsResponse)) *transport.Frame {
	if answer.Opcode != transport.OpBucketStats {
		return answer
	}
	var described BucketStatsResponse
	if err := bson.Unmarshal(answer.Payload, &described); err != nil {
		return errorFrame(request, fmt.Errorf("decoding the answer to %s to add this node's view: %w", request.Opcode, err))
	}
	change(&described)
	return respondFrame(request, transport.OpBucketStats, &described)
}
