package server

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// This file holds the transport handlers for database/bucket management and
// the operations that were previously only reachable over gRPC. Every
// handler follows the same shape: decode the request, call the Server
// method, re-encode the response under the opcode the client expects.

// handleListDatabases processes a list databases request.
func handleListDatabases(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	// A client may send no payload at all: a BSON decode of nothing fails, and
	// the empty request is the whole of what such a client asks.
	var req memberListDatabasesRequest
	if len(frame.Payload) > 0 {
		if err := bson.Unmarshal(frame.Payload, &req); err != nil {
			return errorResponse(frame, fmt.Errorf("unmarshal list databases request: %w", err))
		}
	}
	resp, err := s.listDatabases(req.OwnedCounts)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpListDBs, databasesVisibleTo(ctx, resp))
}

// NoDatabasesMessage is a database listing's message when the store holds no
// database at all. A listing that is empty because the caller holds no role
// says so instead, and the CLI tells the two apart by this value.
const NoDatabasesMessage = "no databases yet — create one with 'create database <name>'"

// databasesVisibleTo narrows a database listing to what the caller may see: the
// databases a user holds a role on, or every one for a role on "*". A cluster
// member asking for its part of a listing, and a call with no session, see
// them all; the member that answers the client narrows the merged listing. The
// cost is the databases times the caller's grants, never the buckets.
func databasesVisibleTo(ctx context.Context, listing *ListDatabasesResponse) *ListDatabasesResponse {
	sess, ok := transport.SessionFromContext(ctx)
	if !ok || sess == nil || sess.Peer {
		return listing
	}
	identity, ok := sess.Identity()
	if !ok {
		return &ListDatabasesResponse{Databases: []DatabaseInfo{}}
	}
	visible := make([]DatabaseInfo, 0, len(listing.Databases))
	for _, database := range listing.Databases {
		if identityHoldsGrantOn(identity, database.Name) {
			visible = append(visible, database)
		}
	}
	if len(visible) == len(listing.Databases) {
		return listing
	}
	narrowed := &ListDatabasesResponse{Databases: visible}
	if len(visible) == 0 {
		narrowed.Message = fmt.Sprintf("user %q holds no role on any database, so none is listed; an administrator grants one with 'grant role reader@<database> to %s'",
			identity.Username, identity.Username)
	}
	return narrowed
}

// handleListBuckets processes a list buckets request.
func handleListBuckets(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req ListBucketsRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal list buckets request: %w", err))
	}
	resp, err := s.ListBuckets(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpListBuckets, resp)
}

// handleBucketStats processes a bucket stats request.
func handleBucketStats(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req BucketStatsRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal bucket stats request: %w", err))
	}
	resp, err := s.BucketStats(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpBucketStats, resp)
}

// handleBucketInfo processes a one-bucket declaration request.
func handleBucketInfo(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req BucketInfoRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal bucket info request: %w", err))
	}
	resp, err := s.BucketInfo(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpBucketInfo, resp)
}

// handleCount processes a count request.
func handleCount(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req CountRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal count request: %w", err))
	}
	resp, err := s.CountCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpCount, resp)
}

// handleDistinct processes a distinct request.
func handleDistinct(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DistinctRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal distinct request: %w", err))
	}
	resp, err := s.DistinctCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpDistinct, resp)
}

// handleUpdate processes an update request.
func handleUpdate(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req UpdateRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal update request: %w", err))
	}
	resp, err := s.UpdateCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpUpdate, resp)
}

// handleEdge processes a btree first/last request.
func handleEdge(ctx context.Context, frame *transport.Frame, s *Server, last bool) (*transport.Frame, error) {
	var req BTreeRangeRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal btree edge request: %w", err))
	}
	resp, err := s.Edge(req.Database, req.Bucket, last)
	if err != nil {
		return errorResponse(frame, err)
	}
	op := transport.OpBTreeFirst
	if last {
		op = transport.OpBTreeLast
	}
	return respond(frame, op, resp)
}

// handleBlobWrite processes a blob write request.
func handleBlobWrite(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req BlobWriteRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal blob write request: %w", err))
	}
	resp, err := s.BlobWriteCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpBlobWrite, resp)
}

// handleBlobRead processes a blob read request.
func handleBlobRead(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req BlobReadRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal blob read request: %w", err))
	}
	resp, err := s.BlobRead(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpBlobReadResponse, resp)
}

// handleDeleteDB processes a drop database request.
func handleDeleteDB(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DeleteDatabaseRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal delete database request: %w", err))
	}
	resp, err := s.DeleteDatabaseCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	s.audit.Log(AuditEntry{Event: EventSchemaDrop, Target: "database:" + req.Name, Result: "ok"})
	return respond(frame, transport.OpDeleteDB, resp)
}

// handleDeleteBucket processes a drop bucket request.
func handleDeleteBucket(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DeleteBucketRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal delete bucket request: %w", err))
	}
	// A drop the cluster stamped, shipped or is replaying is a drop entry: it
	// removes one incarnation and is recorded (R8-C). A client's drop to a node
	// with no cluster is not.
	switch {
	case s.cluster != nil && generationsTrusted(ctx):
		ctx = withDropEntry(ctx, droppedIncarnation{incarnation: req.Incarnation, originSeq: lineageFrom(ctx).originSeq, epoch: s.cluster.Epoch()})
	case s.cluster != nil && s.cluster.recordsLoneDrops():
		ctx = withLoneDrop(ctx)
	}
	resp, err := s.DeleteBucketCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	s.audit.Log(AuditEntry{Event: EventSchemaDrop, Target: "bucket:" + req.Database + "." + displayBucket(req.Name), Result: "ok"})
	return respond(frame, transport.OpDeleteBucket, resp)
}

// identityHoldsGrantOn reports whether any grant of the identity, of any role, covers the
// database. It is the rule of the legacy database listing, which the ring's show_databases
// shares so the two list the same databases to the same user.
func identityHoldsGrantOn(identity transport.Identity, database string) bool {
	for _, grant := range identity.Grants {
		if grant.DB == "*" || grant.DB == database {
			return true
		}
	}
	return false
}
