package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The kinds of an aggregate read (RingAggregateRead.Kind).
const (
	// AggregateReadDescribe lists every aggregate of the store field and its value.
	AggregateReadDescribe uint8 = 0
	// AggregateReadSum reads a plain sum; AggregateReadSumBy a sum per by value;
	// AggregateReadCountBy a count per by value.
	AggregateReadSum     uint8 = 1
	AggregateReadSumBy   uint8 = 2
	AggregateReadCountBy uint8 = 3
)

const (
	// aggregateDefaultLimit and aggregateMaxLimit bound the by values one read returns.
	aggregateDefaultLimit = 100
	aggregateMaxLimit     = 10000
	// aggregateWarnValues is how many distinct by values describe tolerates before it warns.
	aggregateWarnValues = 10000
)

// RingAggregateRead is the form of a RingClassAggregate: the kind and the element paths that
// name one declared aggregate. Field is the summed path (sum and sum by), By the grouping path
// (sum by and count by); both are empty for describe.
type RingAggregateRead struct {
	Kind      uint8
	Field, By string
}

// AggregateStats are node-scoped counts of aggregate reads, by read kind, and of refusals: never per bucket.
// The kinds are the AggregateRead constants above; the exposition the spec names as
// hoarddb_store_aggregate_reads_total{kind} reads these.
type AggregateStats struct {
	Refusals    atomic.Uint64
	ReadsByKind [4]atomic.Uint64
}

// Reads is the number of successful aggregate reads of every kind.
func (s *AggregateStats) Reads() uint64 {
	var total uint64
	for i := range s.ReadsByKind {
		total += s.ReadsByKind[i].Load()
	}
	return total
}

// countRead counts one successful read of kind; a kind outside the four is not counted.
func (s *AggregateStats) countRead(kind uint8) {
	if int(kind) < len(s.ReadsByKind) {
		s.ReadsByKind[kind].Add(1)
	}
}

// AggregateAnswer is what AggregateOf found: the BSON document, and for a by form whether
// another page exists and the cursor to continue from.
type AggregateAnswer struct {
	Document []byte
	More     bool
	Cursor   []byte
}

// AggregateOf answers an aggregate read on the node that holds the document, from the same snapshot
// the elements are read from, so a sum never disagrees with the elements it summarises. The aggregate
// is resolved from the declaration the read was admitted with, which on a forwarded read is the
// owner's own, never the sender's. A plain sum is one Get; a by form is one prefix walk of at most
// limit keys in key order, resumed after the by key in after; describe lists every aggregate.
func (r *Reader) AggregateOf(ctx context.Context, field FieldRead, read RingAggregateRead, limit int, after []byte) (AggregateAnswer, error) {
	return routeFieldAdmitted(ctx, r, field.target(), func(snapshot storage.KVSnapshot, ref store.FieldRef, admitted ReadVerdict) (AggregateAnswer, error) {
		set, _ := admitted.Decl.Aggregates.Set(field.FieldID)
		if read.Kind == AggregateReadDescribe {
			return describeAggregates(snapshot, ref, set)
		}
		entry, ok := findAggregate(set, read)
		names := r.aggregateNames(field, admitted)
		if !ok {
			return AggregateAnswer{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: notDeclaredText(names, set, read)}
		}
		if entry.State == machine.AggStateBuilding {
			return AggregateAnswer{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"AGGREGATE_BUILDING: %q on %s is still being built from the elements the store already holds, so its value would be short. Nothing was read. Run 'describe bucket %s in %s' and read it again when it says ready",
				aggregateText(entry), names.where(), names.bucket, names.db)}
		}
		if limit == 0 {
			limit = aggregateDefaultLimit
		}
		return readAggregate(snapshot, ref, entry, limit, after)
	}, nil, func(ctx context.Context, owner string, hopped ReadRequest) (AggregateAnswer, error) {
		answer, err := r.cfg.Forward.ForwardFieldRead(ctx, owner, FieldForward{Req: hopped, Op: FieldOpAggregate, FieldID: field.FieldID, Engine: field.Engine,
			Aggregate: read, Limit: limit, After: after})
		return answer.Aggregate, err
	})
}

// aggregateNamesOf are the names a refusal of an aggregate read quotes.
type aggregateNamesOf struct{ db, bucket, field string }

func (n aggregateNamesOf) where() string { return n.db + "." + n.bucket + "." + n.field }

// aggregateNames reads the database and bucket names the gate holds and the field's name from the
// admitted table, falling back to ids when a name is not known here.
func (r *Reader) aggregateNames(field FieldRead, admitted ReadVerdict) aggregateNamesOf {
	args := r.cfg.Gate.refusalArgs(field.Request)
	names := aggregateNamesOf{db: args["db"], bucket: args["bucket"], field: fmt.Sprintf("#%d", field.FieldID)}
	if entry, ok := entryByID(admitted.Table, field.FieldID); ok {
		names.field = entry.Name
	}
	return names
}

// readAggregate reads one aggregate's keys; it is the body of AggregateOf and of describe.
func readAggregate(snapshot storage.KVSnapshot, ref store.FieldRef, entry machine.AggregateEntry, limit int, after []byte) (AggregateAnswer, error) {
	prefix := ref.StorePrefix()
	if entry.Kind == machine.AggSum {
		key := store.AggregateKey(prefix, entry.ID, nil)
		sum := store.AggNumber{}
		raw, err := snapshot.Get(key)
		switch {
		case errors.Is(err, storage.ErrKeyNotFound):
		case err != nil:
			return AggregateAnswer{}, fmt.Errorf("reading the sum of aggregate %d: %w", entry.ID, err)
		default:
			if sum, _, err = store.DecodeSumValue(key, raw); err != nil {
				return AggregateAnswer{}, fmt.Errorf("decode sum of aggregate %q on key %x: %w", aggregateText(entry), key, err)
			}
		}
		document, err := bson.Marshal(bson.D{{Key: "sum", Value: sumValue(sum)}})
		return AggregateAnswer{Document: document}, err
	}
	limit = min(max(limit, 1), aggregateMaxLimit)
	bounds := store.AggregateRange(prefix, entry.ID)
	if len(after) > 0 {
		bounds.Lower = append(store.AggregateKey(prefix, entry.ID, after), 0x00)
	}
	it := snapshot.NewIterator(bounds)
	defer it.Close()
	var values bson.A
	var answer AggregateAnswer
	for it.Seek(nil); it.Valid(); it.Next() {
		if len(values) == limit {
			answer.More = true
			break
		}
		key := bytes.Clone(it.Key())
		raw, err := it.Value()
		if err != nil {
			return AggregateAnswer{}, fmt.Errorf("reading aggregate %d: %w", entry.ID, err)
		}
		byKey := key[len(store.AggregateKey(prefix, entry.ID, nil)):]
		by, err := store.DecodeByComponent(byKey)
		if err != nil {
			return AggregateAnswer{}, fmt.Errorf("reading the by value of aggregate %d: %w", entry.ID, err)
		}
		row := bson.D{{Key: "by", Value: by}}
		if entry.Kind == machine.AggSumBy {
			sum, _, n, err := store.DecodeSumByValue(key, raw)
			if err != nil {
				return AggregateAnswer{}, fmt.Errorf("decode sum by value of aggregate %q on key %x: %w", aggregateText(entry), key, err)
			}
			row = append(row, bson.E{Key: "sum", Value: sumValue(sum)}, bson.E{Key: "n", Value: int64(n)})
		} else {
			n, err := store.DecodeCountByValue(key, raw)
			if err != nil {
				return AggregateAnswer{}, fmt.Errorf("decode count of aggregate %q on key %x: %w", aggregateText(entry), key, err)
			}
			row = append(row, bson.E{Key: "n", Value: int64(n)})
		}
		values = append(values, row)
		answer.Cursor = byKey
	}
	if err := it.Err(); err != nil {
		return AggregateAnswer{}, fmt.Errorf("walking aggregate %d: %w", entry.ID, err)
	}
	if !answer.More {
		answer.Cursor = nil
	}
	if values == nil {
		values = bson.A{}
	}
	document, err := bson.Marshal(bson.D{{Key: "values", Value: values}})
	answer.Document = document
	return answer, err
}

// sumValue is a sum as the BSON number it is: an int64 until a double arrived, then a double.
func sumValue(sum store.AggNumber) any {
	if sum.Float {
		return sum.Real
	}
	return sum.Whole
}

// describeAggregates answers a describe: every aggregate of the field in id order, with its
// plain value or the number of by values it holds (counted up to aggregateWarnValues + 1, so a
// store with a runaway dimension is named, not walked to the end).
func describeAggregates(snapshot storage.KVSnapshot, ref store.FieldRef, set machine.AggregateSet) (AggregateAnswer, error) {
	{
		listed := bson.A{}
		for _, entry := range set.Entries {
			aggregateRow := bson.D{{Key: "id", Value: int32(entry.ID)}, {Key: "aggregate", Value: aggregateText(entry)}, {Key: "state", Value: aggregateStateWord(entry.State)}}
			if entry.State == machine.AggStateReady {
				if entry.Kind == machine.AggSum {
					answer, err := readAggregate(snapshot, ref, entry, 1, nil)
					if err != nil {
						return AggregateAnswer{}, err
					}
					var plain struct {
						Sum any `bson:"sum"`
					}
					if err := bson.Unmarshal(answer.Document, &plain); err != nil {
						return AggregateAnswer{}, fmt.Errorf("reading aggregate %d back: %w", entry.ID, err)
					}
					aggregateRow = append(aggregateRow, bson.E{Key: "sum", Value: plain.Sum})
				} else {
					values, warn, err := countByValues(snapshot, ref, entry)
					if err != nil {
						return AggregateAnswer{}, err
					}
					aggregateRow = append(aggregateRow, bson.E{Key: "by_values", Value: values})
					if warn {
						aggregateRow = append(aggregateRow, bson.E{Key: "warning", Value: fmt.Sprintf("more than %d distinct by values: each is a key under this store. Pick a dimension with fewer values, or drop the aggregate", aggregateWarnValues)})
					}
				}
			}
			listed = append(listed, aggregateRow)
		}
		document, err := bson.Marshal(bson.D{{Key: "aggregates", Value: listed}})
		return AggregateAnswer{Document: document}, err
	}
}

// countByValues counts the distinct by values of an aggregate, stopping at aggregateWarnValues + 1.
func countByValues(snapshot storage.KVSnapshot, ref store.FieldRef, entry machine.AggregateEntry) (count int64, warn bool, err error) {
	it := snapshot.NewIterator(store.AggregateRange(ref.StorePrefix(), entry.ID))
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		count++
		if count > aggregateWarnValues {
			return count, true, nil
		}
	}
	return count, false, it.Err()
}

func aggregateStateWord(state uint8) string {
	if state == machine.AggStateBuilding {
		return "building"
	}
	return "ready"
}

// aggregateOfRead sends an aggregate read through the reader, which resolves it on the node that holds
// the document, forwarding it there when this node does not. It counts the read, or the refusal, in the
// node-wide stats.
func (n *Node) aggregateOfRead(ctx context.Context, call fieldCall, read RingAggregateRead, limit int, after []byte) (AggregateAnswer, error) {
	slot := call.ref.slot(call.encID)
	n.own.Acquire(slot, call.ref.dbID)
	field := FieldRead{
		Request: ReadRequest{Slot: slot, DBID: call.ref.dbID, BucketID: call.ref.entry.Decl.BucketID, EncID: call.encID},
		FieldID: call.fieldID, Engine: call.engine,
	}
	answer, err := n.reader.AggregateOf(ctx, field, read, limit, after)
	var refusal *cluster.Error
	if errors.As(err, &refusal) && strings.HasPrefix(refusal.Message, "AGGREGATE_") {
		n.Aggregates.Refusals.Add(1)
	}
	if err != nil {
		return AggregateAnswer{}, err
	}
	n.Aggregates.countRead(read.Kind)
	return answer, nil
}

func findAggregate(set machine.AggregateSet, read RingAggregateRead) (machine.AggregateEntry, bool) {
	for _, entry := range set.Entries {
		if entry.Kind == read.Kind && entry.Field == read.Field && entry.By == read.By {
			return entry, true
		}
	}
	return machine.AggregateEntry{}, false
}

// notDeclaredText names the aggregate asked for, the ones the store keeps, and the statement that
// declares the one asked for on a new bucket.
func notDeclaredText(names aggregateNamesOf, set machine.AggregateSet, read RingAggregateRead) string {
	asked := aggregateText(machine.AggregateEntry{Kind: read.Kind, Field: read.Field, By: read.By})
	kept := "none"
	if len(set.Entries) > 0 {
		var words []string
		for _, entry := range set.Entries {
			words = append(words, aggregateText(entry))
		}
		kept = fmt.Sprint(words)
	}
	return fmt.Sprintf("AGGREGATE_NOT_DECLARED: %s keeps no %q (it keeps: %s). Nothing was read. "+
		"An aggregate is declared when the bucket is created, as in 'create bucket Property in app { Expenses btreeStore with (sum Amount, sum Amount by Currency, count by Category) }'",
		names.where(), asked, kept)
}

// AggregateRead reads a maintained aggregate of one store field of one document, at the node that holds
// it or, through the node-to-node lane, from any other. It is the Node API the ring read class 0x8B serves;
// kind and paths name the aggregate, limit and after page a by form (limit 0 takes the default of 100).
func (n *Node) AggregateRead(ctx context.Context, target StoreTarget, read RingAggregateRead, limit int, after []byte) (AggregateAnswer, error) {
	call, err := n.fieldCall(ctx, target, "aggregate")
	if err != nil {
		return AggregateAnswer{}, err
	}
	return n.aggregateOfRead(ctx, call, read, limit, after)
}
