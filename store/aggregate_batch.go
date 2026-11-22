package store

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/hoarddb/hoarddb-server/storage"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// AggregateBound is the most the absolute values of the elements one aggregate key covers may add up
// to: 2^62. A key keeps, besides the sum S of the summed field, the sum A of the absolute values
// (the abs member of its value), and an add that would take A past the bound is refused. That keeps
// the sum exact under every removal, with no further check, by this argument: S is the sum of the
// elements, so |S| <= A by the triangle inequality, and A <= 2^62 after every add; a removal of an
// element v present in the store leaves S' = S - v and A' = A - |v|, and |S'| <= A' <= 2^62 < 2^63 is
// the true result, so the int64 subtraction cannot wrap and a double sum stays finite, whichever
// elements leave and in whatever order (pop, take, ack, dead-letter). ErrAggregateOverflow can
// therefore only come from an add, which is refused before a position is taken. A bound on the sum
// alone is not enough: -2^61, -2^61, +2^61 x 4 keeps |S| <= 2^62 at every add, yet acking the first
// two leaves 2^63.
const AggregateBound int64 = 1 << 62

// AggregateByCap is the most bytes an encoded by value may take (about 1020 ASCII characters, about 340 of three-byte UTF-8). A
// by value is part of the aggregate key, and an unbounded one could pass the dry run, which never writes
// the key, and fail the apply at the store's key limit after the append. The cap also makes the batch
// charge an upper bound: one aggregate key is at most the store prefix, 2 bytes, the cap and 25 bytes.
const AggregateByCap = 1024

// AggregateSpec is one aggregate of a store field as the apply sees it: the stamp's id and
// kind with the element paths split on ".". Field is empty for count by and By for a plain sum.
type AggregateSpec struct {
	ID, Kind uint8
	Field    []string
	By       []string
}

// AggregateError is the refusal of an element an aggregate cannot take: the path is the
// element field that failed, Reason says why. Overflow marks a sum or value that left its
// range (W-A2) as opposed to a field that is missing or the wrong type (W-A1).
type AggregateError struct {
	Path     string
	Reason   string
	Overflow bool
	// Element is the element the dry run refused, set by CheckAggregates so the refusal text can name it.
	Element []byte
}

// Error implements error.
func (e *AggregateError) Error() string {
	return fmt.Sprintf("aggregate path %q: %s", e.Path, e.Reason)
}

// Unwrap lets errors.Is match ErrAggregateOverflow on a range refusal.
func (e *AggregateError) Unwrap() error {
	if e.Overflow {
		return ErrAggregateOverflow
	}
	return nil
}

// aggCell is one aggregate key the batch will write: its sum and count as they will be once
// the batch commits, and whether the key existed before.
type aggCell struct {
	spec    AggregateSpec
	key     []byte
	sum     AggNumber
	abs     AggNumber
	count   uint64
	touched bool
}

// AggregateBatch accumulates the aggregate deltas of one apply in memory and writes each key
// once on Flush. The snapshot never shows the batch's own earlier writes, so a pop of five
// elements with one by value must see its own running total, which is what the cells hold.
type AggregateBatch struct {
	snap        storage.KVSnapshot
	batch       storage.KVBatch
	storePrefix []byte
	specs       []AggregateSpec
	cells       map[string]*aggCell
	order       []string
}

// A nil *AggregateBatch is the batch of a store with no aggregates: Add and Flush do nothing.
//
// NewAggregateBatch returns an accumulator over a store whose keys start with storePrefix.
// batch may be nil, which makes Flush a no-op: CheckAggregates uses it for a dry run.
func NewAggregateBatch(snap storage.KVSnapshot, batch storage.KVBatch, storePrefix []byte, specs []AggregateSpec) *AggregateBatch {
	return &AggregateBatch{snap: snap, batch: batch, storePrefix: storePrefix, specs: specs, cells: make(map[string]*aggCell)}
}

// aggPlan is what one spec does for one element, computed before any cell changes.
type aggPlan struct {
	spec  AggregateSpec
	by    []byte
	delta AggNumber
}

// Add applies one element entering (sign +1) or leaving (sign -1) the store to every spec.
// An *AggregateError means the element cannot be summed and no cell changed.
func (b *AggregateBatch) Add(element []byte, sign int) error {
	if b == nil {
		return nil
	}
	plans := make([]aggPlan, 0, len(b.specs))
	for _, spec := range b.specs {
		plan := aggPlan{spec: spec}
		if spec.Kind != 3 {
			delta, err := elementNumber(element, spec.Field)
			if err != nil {
				return err
			}
			plan.delta = delta
		}
		if spec.Kind != 1 {
			by, err := elementBy(element, spec.By)
			if err != nil {
				return err
			}
			plan.by = by
		}
		plans = append(plans, plan)
	}
	staged := make([]aggCell, len(plans))
	for i, plan := range plans {
		cell, err := b.load(plan.spec, plan.by)
		if err != nil {
			return err
		}
		staged[i] = *cell
		if err := applyDelta(&staged[i], plan, sign); err != nil {
			return err
		}
	}
	for i, plan := range plans {
		cell, _ := b.load(plan.spec, plan.by)
		*cell = staged[i]
		cell.touched = true
	}
	return nil
}

// load returns the cell of (spec, by), reading the committed value the first time.
func (b *AggregateBatch) load(spec AggregateSpec, by []byte) (*aggCell, error) {
	key := AggregateKey(b.storePrefix, spec.ID, by)
	if cell, ok := b.cells[string(key)]; ok {
		return cell, nil
	}
	cell := &aggCell{spec: spec, key: key}
	raw, err := b.snap.Get(key)
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
	case err != nil:
		return nil, fmt.Errorf("reading aggregate key: %w", err)
	default:
		if err := decodeCell(cell, raw); err != nil {
			return nil, err
		}
	}
	b.cells[string(key)] = cell
	b.order = append(b.order, string(key))
	return cell, nil
}

func decodeCell(cell *aggCell, raw []byte) error {
	switch cell.spec.Kind {
	case 1:
		var err error
		cell.sum, cell.abs, err = DecodeSumValue(cell.key, raw)
		return err
	case 2:
		var err error
		cell.sum, cell.abs, cell.count, err = DecodeSumByValue(cell.key, raw)
		return err
	default:
		count, err := DecodeCountByValue(cell.key, raw)
		cell.count = count
		return err
	}
}

// applyDelta moves one cell by one element. An add keeps the sum of absolute values inside
// AggregateBound; a removal needs no check (see AggregateBound) and an overflow there is a broken
// invariant, reported as the tripwire it is.
func applyDelta(cell *aggCell, plan aggPlan, sign int) error {
	path := strings.Join(plan.spec.Field, ".")
	if plan.spec.Kind != 3 {
		var next, nextAbs AggNumber
		var err error
		if sign > 0 {
			next, err = cell.sum.Add(plan.delta)
			if err == nil {
				nextAbs, err = cell.abs.Add(plan.delta.Absolute())
			}
		} else {
			next, err = cell.sum.Sub(plan.delta)
			if err == nil {
				nextAbs, err = cell.abs.Sub(plan.delta.Absolute())
			}
		}
		if err != nil {
			return &AggregateError{Path: path, Reason: err.Error(), Overflow: true}
		}
		if sign < 0 && nextAbs.Float && nextAbs.Real < 0 {
			nextAbs = AggNumber{Float: true} // rounding drift only; an int below zero is a broken invariant, below
		}
		if sign < 0 && !nextAbs.Float && nextAbs.Whole < 0 {
			return &AggregateError{Path: path, Reason: "an element left that this aggregate never summed: the aggregate and the elements disagree", Overflow: true}
		}
		if sign > 0 && (nextAbs.Float && nextAbs.Real > float64(AggregateBound) || !nextAbs.Float && nextAbs.Whole > AggregateBound) {
			return &AggregateError{Path: path, Reason: fmt.Sprintf("the absolute values of the elements would add up to %v, past 2^62", nextAbs.asDouble()), Overflow: true}
		}
		cell.sum, cell.abs = next, nextAbs
	}
	if plan.spec.Kind == 1 {
		return nil
	}
	if sign > 0 {
		cell.count++
	} else if cell.count == 0 {
		return &AggregateError{Path: strings.Join(plan.spec.By, "."), Reason: "an element left a by value that holds none: the aggregate and the elements disagree", Overflow: false}
	} else {
		cell.count--
	}
	return nil
}

// Flush writes every touched cell: a plain sum is always set (never deleted, it holds 0 for an
// empty store), a by cell whose count returned to zero is deleted, so a store holds one key per
// distinct value it still has.
func (b *AggregateBatch) Flush() error {
	if b == nil || b.batch == nil {
		return nil
	}
	for _, name := range b.order {
		cell := b.cells[name]
		if !cell.touched {
			continue
		}
		var err error
		switch {
		case cell.spec.Kind == 1:
			err = b.batch.Set(cell.key, EncodeSumValue(cell.sum, cell.abs))
		case cell.count == 0:
			err = b.batch.Delete(cell.key)
		case cell.spec.Kind == 2:
			err = b.batch.Set(cell.key, EncodeSumByValue(cell.sum, cell.abs, cell.count))
		default:
			err = b.batch.Set(cell.key, EncodeCountByValue(cell.count))
		}
		if err != nil {
			return fmt.Errorf("writing aggregate key: %w", err)
		}
	}
	return nil
}

// CheckAggregates is the dry run an owner makes before it orders a write: it applies the
// removals then the adds to a throwaway accumulator and returns the first *AggregateError,
// writing nothing.
func CheckAggregates(snap storage.KVSnapshot, storePrefix []byte, specs []AggregateSpec, adds, removes [][]byte) error {
	dry := NewAggregateBatch(snap, nil, storePrefix, specs)
	for _, element := range removes {
		if err := addForCheck(dry, element, -1); err != nil {
			return err
		}
	}
	for _, element := range adds {
		if err := addForCheck(dry, element, 1); err != nil {
			return err
		}
	}
	return nil
}

// addForCheck adds one element to the dry-run accumulator and, when it refuses the element, records the
// element on the refusal.
func addForCheck(dry *AggregateBatch, element []byte, sign int) error {
	err := dry.Add(element, sign)
	var refused *AggregateError
	if errors.As(err, &refused) {
		refused.Element = element
	}
	return err
}

// elementValue reads the value at path inside the element document.
func elementValue(element []byte, path []string) (bson.RawValue, error) {
	where := strings.Join(path, ".")
	fieldValue, err := bson.Raw(element).LookupErr(path...)
	if err != nil {
		return bson.RawValue{}, &AggregateError{Path: where, Reason: "the element has no such field"}
	}
	return fieldValue, nil
}

// elementNumber reads a summable number: int32 and int64 exactly, a finite double. A decimal is
// refused (spec Q2), as is anything else, and an integer past AggregateBound.
func elementNumber(element []byte, path []string) (AggNumber, error) {
	fieldValue, err := elementValue(element, path)
	if err != nil {
		return AggNumber{}, err
	}
	where := strings.Join(path, ".")
	switch fieldValue.Type {
	case bsontype.Int32:
		whole, _ := fieldValue.Int32OK()
		return AggNumber{Whole: int64(whole)}, nil
	case bsontype.Int64:
		whole, _ := fieldValue.Int64OK()
		if whole > AggregateBound || whole < -AggregateBound {
			return AggNumber{}, &AggregateError{Path: where, Reason: fmt.Sprintf("%d is outside plus or minus 2^62", whole), Overflow: true}
		}
		return AggNumber{Whole: whole}, nil
	case bsontype.Double:
		real, _ := fieldValue.DoubleOK()
		if math.IsNaN(real) || math.IsInf(real, 0) {
			return AggNumber{}, &AggregateError{Path: where, Reason: "a double that is NaN or infinite cannot be summed"}
		}
		return AggNumber{Float: true, Real: real}, nil
	case bsontype.Decimal128:
		return AggNumber{}, &AggregateError{Path: where, Reason: "a decimal cannot be summed in this release"}
	default:
		return AggNumber{}, &AggregateError{Path: where, Reason: fmt.Sprintf("the field is a %s, not an int or a double", fieldValue.Type)}
	}
}

// elementBy reads the group value and returns its EncodeComponent form: a string, an int or
// double number, a bool, a datetime or an ObjectID. Anything else has no key.
func elementBy(element []byte, path []string) ([]byte, error) {
	fieldValue, err := elementValue(element, path)
	if err != nil {
		return nil, err
	}
	where := strings.Join(path, ".")
	var key any
	switch fieldValue.Type {
	case bsontype.String:
		key, _ = fieldValue.StringValueOK()
	case bsontype.Int32:
		whole, _ := fieldValue.Int32OK()
		key = int64(whole)
	case bsontype.Int64:
		key, _ = fieldValue.Int64OK()
	case bsontype.Double:
		key, _ = fieldValue.DoubleOK()
	case bsontype.Boolean:
		key, _ = fieldValue.BooleanOK()
	case bsontype.DateTime:
		millis, _ := fieldValue.DateTimeOK()
		key = primitive.DateTime(millis)
	case bsontype.ObjectID:
		key, _ = fieldValue.ObjectIDOK()
	default:
		return nil, &AggregateError{Path: where, Reason: fmt.Sprintf("a %s cannot be grouped by: use a string, number, bool, datetime or ObjectID", fieldValue.Type)}
	}
	encoded, err := EncodeComponent(key)
	if err != nil {
		return nil, &AggregateError{Path: where, Reason: err.Error()}
	}
	if len(encoded) > AggregateByCap {
		return nil, &AggregateError{Path: where, Reason: fmt.Sprintf("the by value takes %d bytes as a key and a by value may take at most %d (about %d ASCII characters, fewer for multi-byte text)", len(encoded), AggregateByCap, AggregateByCap-4)}
	}
	return encoded, nil
}
