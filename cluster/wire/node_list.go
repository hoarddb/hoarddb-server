package wire

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The page bounds of conventions section 16: a page is 100 documents unless the caller
// says otherwise, at most 10000, and at most 16 MiB of entries. The byte ceiling is a
// node setting in a cluster (query.max_result_bytes); this build has no such setting, so
// it is the default.
const (
	defaultPageLimit = 100
	maxPageLimit     = 10000
	pageBytes        = 16 << 20
)

// ListPageRequest asks for one page of a bucket's documents.
type ListPageRequest struct {
	Database, Bucket string
	// Filter is an equality match on declared scalar fields, or nil.
	Filter bson.D
	// Limit is the page size, 1 to 10000; 0 means the default of 100.
	Limit int
	// Cursor continues a previous page; nil starts at the beginning.
	Cursor []byte
	// Fields names the scalar fields to return besides _id_, or nil for every field.
	Fields []string
}

// ListPage is one page of a list.
type ListPage struct {
	// Entries are in (slot, encoded id) order.
	Entries []cluster.ListEntry
	// More says another page follows; Cursor is then set.
	More bool
	// Cursor names the last entry of this page; the next page starts strictly after it.
	Cursor []byte
}

// pageRequest is a list page over a resolved bucket.
type pageRequest struct {
	ref bucketRef
	// filter is the BSON equality document, or nil.
	filter []byte
	limit  int
	cursor []byte
	// fields names the scalar fields to return besides _id_; nil returns every field.
	fields []string
	// ceiling is the most bytes the page holds; a first entry over it is refused.
	ceiling int64
}

// ownedMask admits the slots of ref's database this node owns, so a scan of them is
// answered here, and returns them. It is a loop over the 8192 slots: constant in the
// number of buckets.
func (n *Node) ownedMask(ref bucketRef) cluster.SlotMask {
	mask := ownedSlots(n.topology, ref.dbID)
	for slot := uint16(0); slot < storage.SlotCount; slot++ {
		if mask.Has(slot) {
			n.own.Acquire(slot, ref.dbID)
		}
	}
	return mask
}

// rawFilter encodes a filter document, or returns nil for none.
func rawFilter(filter bson.D, db, bucket string) ([]byte, error) {
	if len(filter) == 0 {
		return nil, nil
	}
	raw, err := bson.Marshal(filter)
	if err != nil {
		return nil, fmt.Errorf("encoding the filter for %s.%s: %w", db, bucket, err)
	}
	return raw, nil
}

// ListPage returns one page of the bucket's documents that equal every field of the
// filter, in (slot, encoded id) order, and the cursor that continues it. The server keeps
// no cursor state. It reads this node's slots: the whole bucket on a single node.
func (n *Node) ListPage(ctx context.Context, req ListPageRequest) (ListPage, error) {
	ref, err := n.bucket(ctx, req.Database, req.Bucket)
	if err != nil {
		return ListPage{}, err
	}
	raw, err := rawFilter(req.Filter, req.Database, req.Bucket)
	if err != nil {
		return ListPage{}, err
	}
	if req.Fields != nil {
		if err := checkFieldNames("fields", req.Fields); err != nil {
			return ListPage{}, err
		}
	}
	return n.listPage(ctx, pageRequest{ref: ref, filter: raw, limit: req.Limit, cursor: req.Cursor, fields: req.Fields, ceiling: pageBytes})
}

func (n *Node) listPage(ctx context.Context, req pageRequest) (ListPage, error) {
	ref := req.ref
	limit, err := pageLimit(req.limit, ref)
	if err != nil {
		return ListPage{}, err
	}
	var project []uint16
	if req.fields != nil {
		if project, err = ref.projection(req.fields); err != nil {
			return ListPage{}, err
		}
	}
	var resume cluster.ListCursor
	if len(req.cursor) > 0 {
		resume, err = cluster.DecodeCursor(req.cursor, ref.dbID, ref.entry.Decl.BucketID)
		var bad *cluster.CursorError
		if errors.As(err, &bad) {
			return ListPage{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondBadCursor, map[string]string{
				"db": ref.db, "bucket": ref.name, "reason": bad.Reason,
			})}
		}
		if err != nil {
			return ListPage{}, fmt.Errorf("reading the cursor of %s.%s: %w", ref.db, ref.name, err)
		}
	}
	mask := n.ownedMask(ref)
	epoch := n.topology.Committed()
	if err := n.scanner.Admit(ctx, epoch, ref.dbID, ref.entry.Decl.BucketID, &mask); err != nil {
		return ListPage{}, fmt.Errorf("listing %s.%s: %w", ref.db, ref.name, err)
	}
	reply, err := n.scanner.ListAdmitted(ctx, cluster.SlotListRequest{
		Epoch: epoch, DBID: ref.dbID, BucketID: ref.entry.Decl.BucketID, Filter: req.filter, Mask: mask,
		ResumeSlot: resume.Slot, ResumeID: resume.EncID, Want: limit, MaxBytes: req.ceiling, Fields: project,
	})
	if err != nil {
		return ListPage{}, fmt.Errorf("listing %s.%s: %w", ref.db, ref.name, err)
	}
	page := ListPage{Entries: reply.Entries, More: reply.More}
	if len(page.Entries) > 0 {
		if first := int64(len(cluster.AppendEntry(nil, page.Entries[0]))); first > req.ceiling {
			return ListPage{}, &cluster.Error{Code: cluster.CodeResultTooLarge, Message: cluster.CodeResultTooLarge.Text(cluster.CondResultTooLarge, map[string]string{
				"db": ref.db, "bucket": ref.name, "n": strconv.FormatInt(first, 10),
			})}
		}
	}
	if !page.More {
		return page, nil
	}
	if len(page.Entries) == 0 {
		return ListPage{}, fmt.Errorf("listing %s.%s: the scan reported more documents and returned none", ref.db, ref.name)
	}
	last := page.Entries[len(page.Entries)-1]
	page.Cursor = cluster.EncodeCursor(cluster.ListCursor{DatabaseID: ref.dbID, BucketID: ref.entry.Decl.BucketID, Slot: last.Slot, EncID: last.EncID})
	return page, nil
}

// pageLimit applies conventions section 16: 0 is the default, below 1 or above the
// maximum is refused.
func pageLimit(limit int, ref bucketRef) (int, error) {
	args := map[string]string{
		"n": strconv.Itoa(limit), "max": strconv.Itoa(maxPageLimit), "default": strconv.Itoa(defaultPageLimit), "db": ref.db, "bucket": ref.name,
	}
	switch {
	case limit == 0:
		return defaultPageLimit, nil
	case limit < 0:
		return 0, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondLimitBelowOne, args)}
	case limit > maxPageLimit:
		return 0, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondLimitAboveMax, args)}
	}
	return limit, nil
}

// CountRequest asks how many documents of a bucket match a filter.
type CountRequest struct {
	Database, Bucket string
	// Filter is an equality match on declared scalar fields, or nil.
	Filter bson.D
}

// Count returns the number of live documents of the bucket that equal every field of the
// filter. It walks the document headers of this node's slots: the cost is the bucket's
// documents, and no per-bucket counter is kept.
func (n *Node) Count(ctx context.Context, req CountRequest) (int64, error) {
	ref, err := n.bucket(ctx, req.Database, req.Bucket)
	if err != nil {
		return 0, err
	}
	raw, err := rawFilter(req.Filter, req.Database, req.Bucket)
	if err != nil {
		return 0, err
	}
	return n.countBucket(ctx, ref, raw)
}

func (n *Node) countBucket(ctx context.Context, ref bucketRef, filter []byte) (int64, error) {
	mask := n.ownedMask(ref)
	count, err := n.scanner.Count(ctx, cluster.SlotCountRequest{
		Epoch: n.topology.Committed(), DBID: ref.dbID, BucketID: ref.entry.Decl.BucketID, Filter: filter, Mask: mask,
	})
	if err != nil {
		return 0, fmt.Errorf("counting %s.%s: %w", ref.db, ref.name, err)
	}
	return count, nil
}
