package wire

import (
	"context"
	"errors"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file serves RING_LIST and RING_COUNT (entry-ring-client-wire.md section 1): the
// bucket-wide reads, which name the bucket and carry no document id. The success reply is
// RING_REPLY with the entries and the page's cursor, or the count.

func (s *ringServer) list(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	req, err := DecodeRingList(frame.Payload)
	if err != nil {
		return ringFailure(frame, err)
	}
	reply, err := s.serveList(ctx, req)
	if err != nil {
		return ringFailure(frame, err)
	}
	return s.replyFrame(frame, reply)
}

func (s *ringServer) count(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	req, err := DecodeRingCount(frame.Payload)
	if err != nil {
		return ringFailure(frame, err)
	}
	reply, err := s.serveCount(ctx, req)
	if err != nil {
		return ringFailure(frame, err)
	}
	return s.replyFrame(frame, reply)
}

// replyFrame stamps the node's view on reply and wraps it in the RING_REPLY answering frame.
func (s *ringServer) replyFrame(frame *transport.Frame, reply RingReply) (*transport.Frame, error) {
	epoch, stateSeq := int64(s.node.topology.Committed()), s.node.topology.seq.Load()
	reply.Epoch, reply.StateSeq = &epoch, &stateSeq
	payload, err := EncodeRingReply(reply)
	if err != nil {
		return ringFailure(frame, err)
	}
	return transport.NewFrameWithID(transport.OpRingReply, frame.RequestID, payload), nil
}

// admitNamed is the two asks a list or a count makes before it reads anything: whether the
// session holds the class on any database, which refuses a session with no grants before a
// lookup, then whether it holds it on the database the request names. The second ask is
// made on the names as sent, before the bucket is looked up, so a bucket that does not
// exist and a bucket the session may not use are one ringDenied answer.
func (s *ringServer) admitNamed(ctx context.Context, class RingClass, database, bucket string) error {
	if err := s.authz.Authorize(ctx, RingAccess{Class: class}); err != nil {
		return ringDenied()
	}
	if err := s.authz.Authorize(ctx, RingAccess{Database: database, Bucket: bucket, Class: class}); err != nil {
		return ringDenied()
	}
	return nil
}

// namedBucket resolves a bucket by the names a list or count carries. A name the database
// does not declare is NOT_FOUND with the fix, not the UNAVAILABLE that the declaration
// fetch's own error would become on the wire.
func (s *ringServer) namedBucket(ctx context.Context, database, bucket string) (bucketRef, error) {
	ref, err := s.node.bucket(ctx, database, bucket)
	if errors.Is(err, ErrBucketNotFound) {
		return bucketRef{}, &cluster.Error{Code: cluster.CodeNotFound, Message: cluster.CodeNotFound.Text(cluster.CondBucketNotFound, map[string]string{
			"db": database, "bucket": bucket,
		})}
	}
	return ref, err
}

func (s *ringServer) serveList(ctx context.Context, req RingListRequest) (RingReply, error) {
	if err := s.admitNamed(ctx, RingClassList, req.Database, req.Bucket); err != nil {
		return RingReply{}, err
	}
	ref, err := s.namedBucket(ctx, req.Database, req.Bucket)
	if err != nil {
		return RingReply{}, err
	}
	// A list names no document, so the owner a stale client is told about is the one node
	// that holds every slot; the empty id is only the argument checkClientEpoch needs.
	if err := s.node.checkClientEpoch(uint64(req.Epoch), ref, nil); err != nil {
		return RingReply{}, err
	}
	if err := refuseLocalWithoutExpand(req.Options, "a bucket list"); err != nil {
		return RingReply{}, err
	}
	if err := checkExpandNearFields(ref, req.Fields, req.Options.Expand); err != nil {
		return RingReply{}, err
	}
	limit := 0
	if req.Limit != nil {
		limit = int(*req.Limit)
	}
	page, err := s.node.listPage(ctx, pageRequest{ref: ref, filter: req.Filter, limit: limit, cursor: req.Cursor, fields: req.Fields, ceiling: pageBytes})
	if err != nil {
		return RingReply{}, err
	}
	if len(req.Options.Expand) > 0 {
		return s.expandEntries(ctx, ref, req, page)
	}
	var entries []byte
	for _, entry := range page.Entries {
		entries = cluster.AppendEntry(entries, entry)
	}
	return RingReply{Entries: entries, More: page.More, Cursor: page.Cursor}, nil
}

func (s *ringServer) serveCount(ctx context.Context, req RingCountRequest) (RingReply, error) {
	if err := s.admitNamed(ctx, RingClassCount, req.Database, req.Bucket); err != nil {
		return RingReply{}, err
	}
	ref, err := s.namedBucket(ctx, req.Database, req.Bucket)
	if err != nil {
		return RingReply{}, err
	}
	total, err := s.node.countBucket(ctx, ref, req.Filter)
	if err != nil {
		return RingReply{}, err
	}
	return RingReply{Count: &total}, nil
}
