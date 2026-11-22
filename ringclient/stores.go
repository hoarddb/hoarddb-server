package ringclient

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

const (
	defaultPageLimit = 100
	// maxPeekCount is the most elements a peek or a pop reads: the server's pop limit cap,
	// which is the contract (build SPEC-GAP U85-4). A store list page may be 1000.
	maxPeekCount = 100
	// maxElementKey is the longest element key or heap entry id.
	maxElementKey = 1024
	// maxNearestVector is the longest query vector a nearest sends: 4096 float32 values.
	maxNearestVector = 16384
)

func rawList(elements [][]byte) []bson.Raw {
	out := make([]bson.Raw, len(elements))
	for i, element := range elements {
		out[i] = bson.Raw(element)
	}
	return out
}

// expandedList is rawList that leaves a read with no expansion with none, so a plain page
// carries no empty slice.
func expandedList(answers [][]byte) []bson.Raw {
	if len(answers) == 0 {
		return nil
	}
	return rawList(answers)
}

// storeRead sends a read class that names a store field of document id.
func (b *Bucket) storeRead(ctx context.Context, id, field string, class byte, fill func(*wire.RingRead)) (wire.RingReply, error) {
	encoded, _, err := encodedID(id)
	if err != nil {
		return wire.RingReply{}, err
	}
	return b.keyed(ctx, nil, func(info BucketInfo) (wire.RingKeyedRequest, error) {
		declared, err := b.storeField(info, field)
		if err != nil {
			return wire.RingKeyedRequest{}, err
		}
		read := readHeader(info, class, encoded)
		read.FieldID = declared.ID
		fill(&read)
		return wire.RingKeyedRequest{Read: &read}, nil
	})
}

// Peek returns up to count elements (1 to 100) a pop would take, next first, without
// removing them. An empty store gives an empty slice.
func (b *Bucket) Peek(ctx context.Context, id, field string, count int) ([]bson.Raw, error) {
	page, err := b.PeekKeyed(ctx, id, field, count)
	return page.Elements, err
}

// PeekKeyed is Peek that also returns the entry ids and priorities of a heapStore's
// elements. A fifoStore or lifoStore has no keys.
func (b *Bucket) PeekKeyed(ctx context.Context, id, field string, count int) (ElementPage, error) {
	if count < 1 || count > maxPeekCount {
		return ElementPage{}, fmt.Errorf("ringclient: Peek asks for %d elements; ask for 1 to %d (to read more, page with StoreList)", count, maxPeekCount)
	}
	reply, err := b.storeRead(ctx, id, field, wire.RingClassPeek, func(read *wire.RingRead) { read.Limit = uint16(count) })
	if err != nil {
		return ElementPage{}, err
	}
	return ElementPage{Elements: rawList(reply.Elements), Keys: reply.Keys, Priorities: reply.Priorities}, nil
}

// StoreGet reads the element a hashStore or btreeStore field holds under key, or a heapStore
// field holds under entry id key, without removing it. An absent key is an error wrapping
// [ErrNotFound].
func (b *Bucket) StoreGet(ctx context.Context, id, field, key string) (bson.Raw, error) {
	if len(key) < 1 || len(key) > maxElementKey {
		return nil, fmt.Errorf("ringclient: StoreGet of field %q reads a key of 1 to %d bytes and this one is %d", field, maxElementKey, len(key))
	}
	reply, err := b.storeRead(ctx, id, field, wire.RingClassStoreGet, func(read *wire.RingRead) { read.Key = key })
	if err != nil {
		return nil, err
	}
	if reply.Found == nil || !*reply.Found || len(reply.Elements) != 1 {
		return nil, fmt.Errorf("ringclient: field %q of %q in bucket %s has no key %q. List the keys with StoreList: %w", field, id, b.where(), key, ErrNotFound)
	}
	return bson.Raw(reply.Elements[0]), nil
}

// StoreList reads one page of a store field, limit elements at most (0 means 100, the
// ceiling is 1000). Pass the previous page's Cursor as after to continue; nil starts at the
// first element.
func (b *Bucket) StoreList(ctx context.Context, id, field string, limit int, after []byte) (ElementPage, error) {
	if limit == 0 {
		limit = defaultPageLimit
	}
	if limit < 1 || limit > 1000 {
		return ElementPage{}, fmt.Errorf("ringclient: StoreList asks for %d elements; ask for 1 to 1000", limit)
	}
	reply, err := b.storeRead(ctx, id, field, wire.RingClassStoreList, func(read *wire.RingRead) {
		read.Limit, read.After = uint16(limit), after
	})
	if err != nil {
		return ElementPage{}, err
	}
	return ElementPage{Elements: rawList(reply.Elements), Keys: reply.Keys, Priorities: reply.Priorities, More: reply.More, Cursor: reply.Cursor}, nil
}

// StoreListExpanded is StoreList of a hashStore or btreeStore that also reads, by key, the
// document each element key names in the far bucket: one call, one round trip, never a scan.
// expand names the far bucket (or leaves it empty when the field is declared 'of' it); Fields and
// Where apply as in ExpandRequest. A nil expand with local true sends local alone, which the
// node refuses: local belongs to keyed reads and to expand. A key whose document is gone comes back with expanded false.
// local lets a ready copy on the node answer; the page says which side did in Served.
func (b *Bucket) StoreListExpanded(ctx context.Context, id, field string, limit int, after []byte, expand *ExpandRequest, local bool) (ElementPage, error) {
	if limit == 0 {
		limit = defaultPageLimit
	}
	if limit < 1 || limit > 1000 {
		return ElementPage{}, fmt.Errorf("ringclient: StoreListExpanded asks for %d elements; ask for 1 to 1000", limit)
	}
	var requests []ExpandRequest
	if expand != nil {
		requests = []ExpandRequest{*expand}
	}
	options, err := readOptions(requests, local)
	if err != nil {
		return ElementPage{}, err
	}
	reply, err := b.storeRead(ctx, id, field, wire.RingClassStoreList, func(read *wire.RingRead) {
		read.Limit, read.After, read.Options = uint16(limit), after, options
	})
	if err != nil {
		return ElementPage{}, err
	}
	return ElementPage{Elements: rawList(reply.Elements), Keys: reply.Keys, Priorities: reply.Priorities, More: reply.More, Cursor: reply.Cursor,
		Expanded: expandedList(reply.Expanded), Served: reply.Served}, nil
}

// StoreCount returns how many elements a store field holds.
func (b *Bucket) StoreCount(ctx context.Context, id, field string) (int64, error) {
	reply, err := b.storeRead(ctx, id, field, wire.RingClassStoreCount, func(*wire.RingRead) {})
	if err != nil {
		return 0, err
	}
	if reply.Count == nil {
		return 0, fmt.Errorf("ringclient: the server answered a store count of %s without a count", field)
	}
	return *reply.Count, nil
}

// LeaseGet reads the messages a lease still holds, so a client that lost the reply of a claim
// can recover them. Found is false when the lease is gone; then claim again.
func (b *Bucket) LeaseGet(ctx context.Context, id, field string, lease [16]byte) (LeaseRead, error) {
	reply, err := b.storeRead(ctx, id, field, wire.RingClassLeaseGet, func(read *wire.RingRead) { read.Lease = lease })
	if err != nil {
		return LeaseRead{}, err
	}
	found := reply.Found != nil && *reply.Found
	return LeaseRead{Found: found, Elements: rawList(reply.Elements)}, nil
}

// leaseOp sends a claim, ack, release or extend on a fifoStore or lifoStore field.
func (b *Bucket) leaseOp(ctx context.Context, id, field string, options []CallOption, fill func(*cluster.Op)) (wire.RingReply, error) {
	encoded, _, err := encodedID(id)
	if err != nil {
		return wire.RingReply{}, err
	}
	return b.keyed(ctx, options, func(info BucketInfo) (wire.RingKeyedRequest, error) {
		declared, err := b.storeField(info, field)
		if err != nil {
			return wire.RingKeyedRequest{}, err
		}
		op := headerOp(info, 0, encoded)
		op.Fields = []cluster.FieldOp{{FieldID: declared.ID}}
		fill(&op)
		return wire.RingKeyedRequest{Op: op}, nil
	})
}

// Claim leases up to count messages from a fifoStore or lifoStore field without removing
// them. Ack the lease when they are handled, Release it to put them back, or let it run out.
// A store with nothing to lease gives a zero lease id and no elements.
func (b *Bucket) Claim(ctx context.Context, id, field string, count int, options ...CallOption) (Lease, error) {
	if count < 1 || count > 65535 {
		return Lease{}, fmt.Errorf("ringclient: Claim asks for %d messages; ask for 1 to 65535 (the server's own ceiling applies)", count)
	}
	reply, err := b.leaseOp(ctx, id, field, options, func(op *cluster.Op) { op.Class, op.Count = cluster.OpClaim, uint16(count) })
	if err != nil {
		return Lease{}, err
	}
	lease := Lease{Elements: rawList(reply.Elements), Write: writeResult(reply)}
	copy(lease.ID[:], reply.Lease)
	return lease, nil
}

func (b *Bucket) leaseChange(ctx context.Context, id, field string, options []CallOption, fill func(*cluster.Op)) (LeaseChange, error) {
	reply, err := b.leaseOp(ctx, id, field, options, fill)
	if err != nil {
		return LeaseChange{}, err
	}
	return LeaseChange{Write: writeResult(reply), Live: reply.Found != nil && *reply.Found}, nil
}

// Ack confirms a lease: its messages are removed for good.
func (b *Bucket) Ack(ctx context.Context, id, field string, lease [16]byte, options ...CallOption) (LeaseChange, error) {
	return b.leaseChange(ctx, id, field, options, func(op *cluster.Op) { op.Class, op.LeaseID = cluster.OpAck, lease })
}

// Release gives a lease back: its messages return to the store.
func (b *Bucket) Release(ctx context.Context, id, field string, lease [16]byte, options ...CallOption) (LeaseChange, error) {
	return b.leaseChange(ctx, id, field, options, func(op *cluster.Op) { op.Class, op.LeaseID = cluster.OpRelease, lease })
}

// Extend moves a live lease's deadline to leaseMs milliseconds (1000 to 3600000) from now.
func (b *Bucket) Extend(ctx context.Context, id, field string, lease [16]byte, leaseMs int64, options ...CallOption) (LeaseChange, error) {
	return b.leaseChange(ctx, id, field, options, func(op *cluster.Op) {
		op.Class, op.LeaseID, op.Until = cluster.OpExtend, lease, leaseMs
	})
}

// Nearest returns the K elements of a store field of document id closest to query.Vector, best first, with their
// keys and scores. A hashStore, btreeStore, heapStore, fifoStore or lifoStore field takes it; a blobStore is refused.
func (b *Bucket) Nearest(ctx context.Context, id, field string, query NearestQuery) (NearestResult, error) {
	k, metric, element, err := resolveNearest(query)
	if err != nil {
		return NearestResult{}, err
	}
	reply, err := b.storeRead(ctx, id, field, wire.RingClassNearest, func(read *wire.RingRead) {
		read.Nearest = &wire.RingNearest{Metric: metric, K: uint8(k), Field: element, Vector: query.Vector}
	})
	if err != nil {
		return NearestResult{}, err
	}
	return nearestResultOf(field, reply)
}

// nearestResultOf turns a nearest reply into the caller's result, refusing a reply that cannot be
// read as one: no counts, or fewer scores than elements.
func nearestResultOf(field string, reply wire.RingReply) (NearestResult, error) {
	if reply.Scanned == nil || reply.Skipped == nil {
		return NearestResult{}, fmt.Errorf("ringclient: the server answered a nearest of %s without scanned and skipped", field)
	}
	if len(reply.Scores) != len(reply.Elements) {
		return NearestResult{}, fmt.Errorf("ringclient: the server answered a nearest without a score per element (%d elements, %d scores)", len(reply.Elements), len(reply.Scores))
	}
	return NearestResult{
		Elements: rawList(reply.Elements),
		Keys:     reply.Keys,
		Scores:   reply.Scores,
		Scanned:  int(*reply.Scanned),
		Skipped:  int(*reply.Skipped),
	}, nil
}

// resolveNearest applies the defaults of a NearestQuery and refuses a bad one before anything is sent.
func resolveNearest(query NearestQuery) (k int, metric byte, element string, err error) {
	k = query.K
	if k == 0 {
		k = 10
	}
	if k < 1 || k > maxPeekCount {
		return 0, 0, "", fmt.Errorf("ringclient: Nearest asks for k %d; ask for 1 to %d", query.K, maxPeekCount)
	}
	switch query.Metric {
	case "", "cosine":
		metric = wire.RingMetricCosine
	case "dot":
		metric = wire.RingMetricDot
	case "l2":
		metric = wire.RingMetricL2
	default:
		return 0, 0, "", fmt.Errorf("ringclient: Nearest metric %q is not cosine, dot or l2", query.Metric)
	}
	if size := len(query.Vector); size < 4 || size > maxNearestVector || size%4 != 0 {
		return 0, 0, "", fmt.Errorf("ringclient: Nearest vector is %d bytes; send 4 to %d bytes, a multiple of 4", size, maxNearestVector)
	}
	element = query.Field
	if element == "" {
		element = "Vector"
	}
	return k, metric, element, nil
}
