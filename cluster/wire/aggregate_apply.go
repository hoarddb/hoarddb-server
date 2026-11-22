package wire

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// aggregateValueMax is the longest aggregate value (a by form's sum, abs and count).
const aggregateValueMax = 25

// aggregateKeyBytes is the most one aggregate key set or delete adds to a batch under a store prefix of
// prefixLen bytes: the prefix, the inner byte, the id, a by value at its cap, the longest value and the batch's
// own per-entry overhead. It is an upper bound because elementBy refuses a by value over AggregateByCap, so
// no aggregate key is longer; a plain sum's key is shorter.
func aggregateKeyBytes(prefixLen int) int64 {
	return int64(prefixLen + 2 + store.AggregateByCap + aggregateValueMax + batchEntryOverhead)
}

// specsOf turns a record's aggregate stamp into the specs the accumulator applies.
func specsOf(stamp []cluster.AggregateStamp) []store.AggregateSpec {
	specs := make([]store.AggregateSpec, len(stamp))
	for i, stampEntry := range stamp {
		specs[i] = store.AggregateSpec{ID: stampEntry.AggID, Kind: stampEntry.Kind, Field: splitPath(stampEntry.Field), By: splitPath(stampEntry.By)}
	}
	return specs
}

func splitPath(path string) []string {
	if path == "" {
		return nil
	}
	return strings.Split(path, ".")
}

// aggregateBatch is the accumulator of one apply, nil when the record carries no stamp.
func aggregateBatch(snapshot storage.KVSnapshot, batch storage.KVBatch, ref store.FieldRef, op cluster.Op) *store.AggregateBatch {
	if len(op.Aggregates) == 0 {
		return nil
	}
	return store.NewAggregateBatch(snapshot, batch, ref.StorePrefix(), specsOf(op.Aggregates))
}

// aggAdd applies one element to agg. A refusal here means admission let an element through
// that the apply cannot sum, which the owner's dry run exists to prevent, so it is an apply
// failure and not a user refusal.
func aggAdd(agg *store.AggregateBatch, element []byte, sign int) error {
	if err := agg.Add(element, sign); err != nil {
		return fmt.Errorf("maintaining the store's aggregates: %w", err)
	}
	return nil
}

// heapElementAt reads the element stored at exactly (priority, entryID), which a heap push
// overwrites. It is one Get, where FieldHeapEntry would walk the heap.
func heapElementAt(snapshot storage.KVSnapshot, ref store.FieldRef, priority int64, entryID string) ([]byte, bool, error) {
	remainder, err := store.HeapRemainder(priority, entryID)
	if err != nil {
		return nil, false, fmt.Errorf("keying heap entry %q: %w", entryID, err)
	}
	raw, err := snapshot.Get(store.FieldElementKey(ref.StorePrefix(), storage.NSQueue, remainder))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("reading heap entry %q: %w", entryID, err)
	}
	return raw, true, nil
}

// aggregateRefusal is the owner's dry run at check 6a: it applies the verb's element changes to the
// aggregates the op carries (the stamp the owner took from the admitted declaration) against the
// snapshot the apply will read and returns the text of the first element an aggregate cannot take, ""
// when the write is fine or the field keeps none. W-A1 and W-A2 are refused here, before a position,
// because after the append the same refusal would fail on every replay and fence the pair.
func (a *DocumentApplier) aggregateRefusal(slot uint16, db uint64, op cluster.Op, engine store.FieldEngine, arg verbArg) (string, error) {
	if op.Class != cluster.OpStoreVerb || len(op.Fields) != 1 || len(op.Aggregates) == 0 {
		return "", nil
	}
	field := op.Fields[0]
	specs := specsOf(op.Aggregates)
	shape, fits := verbShapeOf(field.Verb, engine)
	if !fits {
		return "", nil
	}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	ref := a.fieldRef(slot, db, op.BucketID, op.EncID, field.FieldID, engine)
	adds, removes, err := verbElements(snapshot, ref, shape, arg)
	if err != nil {
		return "", err
	}
	if refusal := a.aggregateBatchRefusal(ref, len(specs)*(len(adds)+len(removes))); refusal != "" {
		return refusal, nil
	}
	err = store.CheckAggregates(snapshot, ref.StorePrefix(), specs, adds, removes)
	var refused *store.AggregateError
	if errors.As(err, &refused) {
		return aggregateRefusedText(refused), nil
	}
	if err != nil {
		return "", fmt.Errorf("checking the aggregates of field %d: %w", field.FieldID, err)
	}
	return "", nil
}

// verbElements lists the elements a verb adds to and removes from its store, reading the
// removed ones from the snapshot: the same reads fillVerb makes, so the dry run and the apply
// see one element set.
func verbElements(snapshot storage.KVSnapshot, ref store.FieldRef, shape verbShape, arg verbArg) (adds, removes [][]byte, err error) {
	switch shape {
	case shapeElement:
		return [][]byte{arg.Element}, nil, nil
	case shapeHeapPush:
		old, found, err := heapElementAt(snapshot, ref, arg.Priority, arg.EntryID)
		if err != nil {
			return nil, nil, err
		}
		if found {
			removes = append(removes, old)
		}
		return [][]byte{arg.Element}, removes, nil
	case shapePut:
		old, found, err := store.FieldGet(snapshot, ref, arg.Key)
		if err != nil {
			return nil, nil, fmt.Errorf("reading the element a put would replace: %w", err)
		}
		if found {
			removes = append(removes, old)
		}
		return [][]byte{arg.Element}, removes, nil
	case shapeInc:
		old, next, err := incPlan(snapshot, ref, arg)
		if err != nil {
			return nil, nil, fmt.Errorf("planning the inc the aggregates would follow: %w", err)
		}
		if old != nil {
			removes = append(removes, old)
		}
		return [][]byte{next}, removes, nil
	case shapePop:
		n := max(arg.Count, 1)
		err := store.FieldPeekN(snapshot, ref, n, func(entry store.FieldEntry) error {
			removes = append(removes, append([]byte(nil), entry.Value...))
			return nil
		})
		return nil, removes, err
	case shapeTakeKey:
		old, found, err := store.FieldGet(snapshot, ref, arg.Key)
		if err != nil {
			return nil, nil, fmt.Errorf("reading the element a take would remove: %w", err)
		}
		if found {
			removes = append(removes, old)
		}
		return nil, removes, nil
	case shapeTakeEntry:
		entry, found, err := store.FieldHeapEntry(snapshot, ref, arg.EntryID)
		if err != nil {
			return nil, nil, fmt.Errorf("reading the element a take would remove: %w", err)
		}
		if found {
			removes = append(removes, entry.Value)
		}
		return nil, removes, nil
	}
	return nil, nil, nil
}

// aggregateBatchRefusal refuses a write whose aggregate keys, added to the batch the verb
// already takes, would not fit one batch. keys is the number of aggregate keys the write may
// set or delete. "" when they fit.
func (a *DocumentApplier) aggregateBatchRefusal(ref store.FieldRef, keys int) string {
	prefix := len(ref.StorePrefix())
	bytes := int64(keys) * aggregateKeyBytes(prefix)
	room := a.kv.MaxBatchBytes() - insertBatchReserve
	if bytes <= room {
		return ""
	}
	return fmt.Sprintf("AGGREGATE_BATCH: the write would set or delete %d aggregate keys of up to %d bytes each, %d bytes in all, and a batch has %d bytes left after the element and the pair record. "+
		"Nothing was written. Declare fewer aggregates on the store, or move fewer elements in one write: pop, take or claim fewer", keys, aggregateKeyBytes(prefix), bytes, room)
}

// aggregateRefusedText words an element an aggregate refused: W-A1 for a missing or non-numeric
// field and W-A2 for a value or a sum outside plus or minus 2^62. Both name the path and quote
// the statement that shows the store's aggregates.
func aggregateRefusedText(refused *store.AggregateError) string {
	const fix = "or run 'describe bucket <bucket> in <database>' to see the aggregates this store keeps"
	element := refusedElementText(refused.Element)
	if refused.Overflow {
		return fmt.Sprintf("AGGREGATE_RANGE: the element %s would add its %q values up, in absolute value, past 2^62: %s. Nothing was written. "+
			"Store money in integer minor units, or split the store, %s", element, refused.Path, refused.Reason, fix)
	}
	return fmt.Sprintf("AGGREGATE_FIELD: the element %s cannot be counted by the store's aggregates: %s (path %q). Nothing was written. "+
		"Give every element a number at the summed path (an int or a double, never a decimal) and a string, number, bool, datetime or ObjectID at each by path, %s", element, refused.Reason, refused.Path, fix)
}

// aggregateElementTextMax bounds the element a refusal quotes, so a large element does not make the refusal
// a second copy of the write.
const aggregateElementTextMax = 200

// refusedElementText renders the refused element as extended JSON, cut to aggregateElementTextMax bytes.
func refusedElementText(element []byte) string {
	text := bson.Raw(element).String()
	if len(text) > aggregateElementTextMax {
		text = text[:aggregateElementTextMax] + "..."
	}
	return text
}

// leaseAggregateRefusal charges a claim, ack, release or extend the aggregate keys it may touch: up to
// MaxClaimLimit leased elements leave the store in one ack, and the return step of any of them may
// dead-letter entries. It reads the stamp the op carries. "" when it keeps none or the keys fit.
func (a *DocumentApplier) leaseAggregateRefusal(slot uint16, db uint64, op cluster.Op, engine store.FieldEngine) string {
	if len(op.Fields) != 1 || len(op.Aggregates) == 0 {
		return ""
	}
	return a.aggregateBatchRefusal(a.fieldRef(slot, db, op.BucketID, op.EncID, op.Fields[0].FieldID, engine), len(op.Aggregates)*store.MaxClaimLimit)
}
