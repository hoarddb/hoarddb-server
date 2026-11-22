package ringclient

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Bucket is a handle on one bucket. It holds no connection and does no I/O until a method
// is called; the bucket's header is learned on first use and cached on the Client.
type Bucket struct {
	client           *Client
	database, bucket string
	// batch, when set, makes the write methods queue their operation on it instead of
	// sending a request; see [Bucket.In].
	batch *Batch
}

// Bucket returns the handle for bucket name of database.
func (c *Client) Bucket(database, name string) *Bucket {
	return &Bucket{client: c, database: database, bucket: name}
}

// Info returns the bucket's header, from the cache or, the first time, from describe_bucket.
func (b *Bucket) Info(ctx context.Context) (BucketInfo, error) {
	if info, ok := b.client.cachedHeader(b.database, b.bucket); ok {
		return info, nil
	}
	return b.client.DescribeBucket(ctx, b.database, b.bucket)
}

func (b *Bucket) where() string { return b.database + "." + b.bucket }

func (info BucketInfo) field(name string) (FieldInfo, bool) {
	for _, field := range info.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return FieldInfo{}, false
}

// declared lists the field names of the given kind for an error message.
func (info BucketInfo) declared(stores bool) string {
	var names []string
	for _, field := range info.Fields {
		if field.Name != "_id_" && isStoreType(field.Type) == stores {
			names = append(names, field.Name+" "+field.Type)
		}
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// storeField finds a declared store field, or says which fields are stores.
func (b *Bucket) storeField(info BucketInfo, name string) (FieldInfo, error) {
	field, ok := info.field(name)
	switch {
	case !ok:
		return FieldInfo{}, undeclaredField{fmt.Sprintf("ringclient: bucket %s declares no field %q. Its store fields are: %s. "+
			"Add one with 'alter bucket %s in %s { add %s <type> }', or check the spelling", b.where(), name, info.declared(true), b.bucket, b.database, name)}
	case !isStoreType(field.Type):
		return FieldInfo{}, fmt.Errorf("ringclient: field %q of bucket %s is a %s, not a store. Its store fields are: %s. "+
			"Write a scalar with Insert, Upsert or Doc", name, b.where(), field.Type, info.declared(true))
	}
	return field, nil
}

// header builds the request header for one document under the cached bucket header.
func headerOp(info BucketInfo, class byte, encoded []byte) cluster.Op {
	return cluster.Op{Class: class, BucketID: info.ID, DeclVersion: info.DeclVersion, SchemaVersionSeen: info.SchemaVersionSeen, EncID: encoded}
}

func readHeader(info BucketInfo, class byte, encoded []byte) wire.RingRead {
	return wire.RingRead{Class: class, BucketID: info.ID, DeclVersion: info.DeclVersion, SchemaVersionSeen: info.SchemaVersionSeen, EncID: encoded}
}

// unknownBucketID is the phrase of the server's refusal for a bucket id it does not have.
const unknownBucketID = "no bucket has the id"

// staleBucketID reports whether refusal says the bucket id the request carried is unknown,
// which is what a cached header answers after the bucket was dropped and created again.
func staleBucketID(refusal *Error) bool {
	return (refusal.Code == cluster.CodePermissionDenied || refusal.Code == cluster.CodeNotFound) &&
		strings.Contains(refusal.Message, unknownBucketID)
}

// undeclaredField is the refusal to build a request that names a field the cached header does
// not declare. The header may be older than an alter, so keyed learns it again once before it
// believes the refusal.
type undeclaredField struct{ message string }

func (e undeclaredField) Error() string { return e.message }

// keyed sends the request build makes. On SCHEMA_NOT_CURRENT it learns the bucket's
// header again and sends the request once more under the same request id: the refusal wrote
// nothing, so the id is still unused. A request that names a field the cached header does not
// declare also learns the header again, once, because an alter by another client may have
// added the field since. An unknown-bucket-id refusal does the same once, but
// only when the bucket's header really changed (a dropped bucket created again under the
// same name); an unchanged header means the refusal is true and is returned.
func (b *Bucket) keyed(ctx context.Context, options []CallOption, build func(BucketInfo) (wire.RingKeyedRequest, error)) (wire.RingReply, error) {
	if b.batch != nil {
		return b.batch.queue(ctx, b, options, build)
	}
	config := newCallConfig(options)
	id := config.requestID
	if !config.hasRequestID {
		id = b.client.NextRequestID()
	}
	info, err := b.Info(ctx)
	if err != nil {
		return wire.RingReply{}, err
	}
	refreshedSchema, redescribed, relearned := false, false, false
	for {
		request, err := build(info)
		var undeclared undeclaredField
		if errors.As(err, &undeclared) && !relearned {
			relearned = true
			if info, err = b.client.DescribeBucket(ctx, b.database, b.bucket); err != nil {
				return wire.RingReply{}, fmt.Errorf("refreshing the header of %s after an undeclared field: %w", b.where(), err)
			}
			continue
		}
		if errors.As(err, &undeclared) {
			// Still undeclared after the header was refreshed: the name is wrong, not stale.
			return wire.RingReply{}, &Error{Code: cluster.CodeFieldType, Message: strings.TrimPrefix(undeclared.message, "ringclient: ")}
		}
		if err != nil {
			return wire.RingReply{}, err
		}
		request.Prologue.DeadlineMs = config.deadlineMs
		body, err := wire.EncodeRingKeyed(request)
		if err != nil {
			return wire.RingReply{}, fmt.Errorf("ringclient: encoding the request for %s: %w", b.where(), err)
		}
		payload, err := b.client.call(ctx, transport.OpRingKeyed, id, body)
		var refusal *Error
		if errors.As(err, &refusal) && refusal.Code == cluster.CodeSchemaNotCurrent && !refreshedSchema {
			refreshedSchema = true
			if info, err = b.client.DescribeBucket(ctx, b.database, b.bucket); err != nil {
				return wire.RingReply{}, fmt.Errorf("refreshing the header of %s after SCHEMA_NOT_CURRENT: %w", b.where(), err)
			}
			continue
		}
		if errors.As(err, &refusal) && staleBucketID(refusal) && !redescribed {
			redescribed = true
			fresh, describeErr := b.client.DescribeBucket(ctx, b.database, b.bucket)
			if describeErr != nil {
				return wire.RingReply{}, fmt.Errorf("refreshing the header of %s after %s: %w", b.where(), refusal.Code, describeErr)
			}
			changed := fresh.ID != info.ID || fresh.DeclVersion != info.DeclVersion || fresh.SchemaVersionSeen != info.SchemaVersionSeen
			info = fresh
			if changed {
				continue
			}
		}
		if err != nil {
			return wire.RingReply{}, err
		}
		reply, err := wire.DecodeRingReply(payload)
		if err != nil {
			return wire.RingReply{}, fmt.Errorf("ringclient: decoding the reply from %s: %w", b.where(), err)
		}
		return reply, nil
	}
}

func writeResult(reply wire.RingReply) WriteResult {
	written := WriteResult{Outcome: reply.WriteOutcome, Deduped: reply.Deduped, NotApplied: reply.WriteOutcome == "not_applied",
		Reason: reply.Reason, GuardField: reply.GuardField, DocumentTruncated: reply.DocumentTruncated}
	if len(reply.Document) > 0 {
		written.Document = bson.Raw(reply.Document)
	}
	if reply.CopiesConfirmed != nil {
		written.Copies = int(*reply.CopiesConfirmed)
	}
	if reply.Position != nil {
		written.Position = reply.Position.String()
	}
	return written
}

// scalarFields turns a Doc into field entries in id order, refusing a name the bucket does
// not declare or a store.
func (b *Bucket) scalarFields(info BucketInfo, doc Doc) ([]cluster.FieldOp, error) {
	fields := make([]cluster.FieldOp, 0, len(doc))
	for name, value := range doc {
		field, ok := info.field(name)
		switch {
		case !ok || name == "_id_":
			return nil, undeclaredField{fmt.Sprintf("ringclient: bucket %s declares no field %q. Its scalar fields are: %s. "+
				"Check the spelling; the document id is the id argument, not a field", b.where(), name, info.declared(false))}
		case isStoreType(field.Type):
			return nil, fmt.Errorf("ringclient: field %q of bucket %s is a %s store, so it is not a member of a Doc. "+
				"Write to it with Push or Put after the document exists", name, b.where(), field.Type)
		}
		typed, err := typedValue(value)
		if err != nil {
			return nil, fmt.Errorf("ringclient: field %q of %s: %w", name, b.where(), err)
		}
		fields = append(fields, cluster.FieldOp{FieldID: field.ID, Value: typed})
	}
	slices.SortFunc(fields, func(a, c cluster.FieldOp) int { return int(a.FieldID) - int(c.FieldID) })
	return fields, nil
}

func (b *Bucket) writeDocument(ctx context.Context, class byte, id string, doc Doc, options []CallOption) (WriteResult, error) {
	if config := newCallConfig(options); len(config.guard) > 0 || config.returnMode != "" {
		return WriteResult{}, noGuardOnWholeWrite(class)
	}
	encoded, typedID, err := encodedID(id)
	if err != nil {
		return WriteResult{}, err
	}
	reply, err := b.keyed(ctx, options, func(info BucketInfo) (wire.RingKeyedRequest, error) {
		fields, err := b.scalarFields(info, doc)
		if err != nil {
			return wire.RingKeyedRequest{}, err
		}
		op := headerOp(info, class, encoded)
		op.IDValue, op.Fields = typedID, fields
		return wire.RingKeyedRequest{Op: op}, nil
	})
	if err != nil {
		return WriteResult{}, err
	}
	return writeResult(reply), nil
}

// Insert creates document id with the given scalar fields. An id that exists is an error.
// The id must be a string. A field the Doc leaves out is absent from the document.
func (b *Bucket) Insert(ctx context.Context, id string, doc Doc, options ...CallOption) (WriteResult, error) {
	return b.writeDocument(ctx, cluster.OpInsert, id, doc, options)
}

// Upsert creates document id or replaces its scalar fields; the document's stores are kept.
func (b *Bucket) Upsert(ctx context.Context, id string, doc Doc, options ...CallOption) (WriteResult, error) {
	return b.writeDocument(ctx, cluster.OpUpsert, id, doc, options)
}

// Delete removes document id and the stores it carries. With [If] it removes the document
// only while the guard holds; with [Return] ReturnBefore it sends the document back.
func (b *Bucket) Delete(ctx context.Context, id string, options ...CallOption) (WriteResult, error) {
	encoded, _, err := encodedID(id)
	if err != nil {
		return WriteResult{}, err
	}
	config := newCallConfig(options)
	reply, err := b.keyed(ctx, options, func(info BucketInfo) (wire.RingKeyedRequest, error) {
		op := headerOp(info, cluster.OpDelete, encoded)
		if err := b.applyGuard(info, &op, config); err != nil {
			return wire.RingKeyedRequest{}, err
		}
		return wire.RingKeyedRequest{Op: op}, nil
	})
	if err != nil {
		return WriteResult{}, err
	}
	return writeResult(reply), nil
}

// noGuardOnWholeWrite refuses If and Return on an insert or upsert: sending the write
// without them would let the caller believe it was conditional.
func noGuardOnWholeWrite(class byte) error {
	if class == cluster.OpInsert {
		return &Error{Code: cluster.CodeInvalidArgument, Message: "an insert returns its id and takes no If or Return. Insert already refuses a duplicate id; use Update with If to change a document only while it holds the values you expect"}
	}
	return &Error{Code: cluster.CodeInvalidArgument, Message: "an upsert has no document to guard and takes no If or Return. Use Insert, which refuses a duplicate id, or Update with If on a document that exists"}
}
