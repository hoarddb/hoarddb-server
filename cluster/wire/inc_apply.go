package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// incTerm is one path of an inc and the amount to add, in update's typed form (the type byte
// then the value bytes), so incResult takes it as it stands.
type incTerm struct {
	Path   []string
	Amount []byte
}

// incError is an inc the element cannot take: a non-number where it adds, or an add that would
// overflow. It is the developer's to fix, as opposed to a failed read of the store.
type incError struct{ err error }

func (e *incError) Error() string { return e.err.Error() }
func (e *incError) Unwrap() error { return e.err }

// emptyDocument is the BSON document with no field, the element an inc over an absent key starts from.
var emptyDocument = []byte{5, 0, 0, 0, 0}

// incTermsOf reads the increments document of a decoded inc arg into terms, in the order given.
func incTermsOf(increments []byte) ([]incTerm, error) {
	elements, err := bson.Raw(increments).Elements()
	if err != nil {
		return nil, fmt.Errorf("reading the increments: %w", err)
	}
	terms := make([]incTerm, len(elements))
	for i, element := range elements {
		amount := element.Value()
		terms[i] = incTerm{Path: strings.Split(element.Key(), "."), Amount: append([]byte{byte(amount.Type)}, amount.Value...)}
	}
	return terms, nil
}

// incElement returns old (nil when the element is absent) with every term added, in the order given. It
// adds with incResult, so an inc of a store element and update's $inc cannot disagree: two int32s stay an
// int32 unless the sum leaves its range and then widen to an int64, an int64 that would overflow and a
// non-finite double are refused, and any double makes a double. A path the element lacks is created with
// the amount, and so are its parents. A path that holds a non-number, or a parent that is not a
// document, is refused naming the path. Fields no term names keep their bytes and their order.
func incElement(old []byte, terms []incTerm) ([]byte, error) {
	document := old
	if document == nil {
		document = emptyDocument
	}
	for _, term := range terms {
		next, err := incPath(document, term.Path, term.Amount, strings.Join(term.Path, "."))
		if err != nil {
			return nil, &incError{err: err}
		}
		document = next
	}
	return document, nil
}

// incPath adds amount at path inside document and returns the document rewritten; full is the whole
// dotted path, for messages.
func incPath(document []byte, path []string, amount []byte, full string) ([]byte, error) {
	elements, err := bson.Raw(document).Elements()
	if err != nil {
		return nil, fmt.Errorf("the element is not a BSON document, so path %q cannot be added to: %w", full, err)
	}
	body := make([]byte, 0, len(document)+len(amount)+len(full)+8)
	placed := false
	for _, element := range elements {
		if placed || element.Key() != path[0] {
			body = append(body, element...)
			continue
		}
		placed = true
		current := element.Value()
		if len(path) > 1 {
			if current.Type != bsontype.EmbeddedDocument {
				return nil, fmt.Errorf("path %q needs %q to be a document and it is a %s; inc adds to numbers only", full, path[0], current.Type)
			}
			inner, err := incPath(current.Value, path[1:], amount, full)
			if err != nil {
				return nil, err
			}
			body = appendIncElement(body, bsontype.EmbeddedDocument, path[0], inner)
			continue
		}
		sum, err := incResult(append([]byte{byte(current.Type)}, current.Value...), amount)
		if err != nil {
			return nil, fmt.Errorf("path %q holds a %s: %w", full, current.Type, err)
		}
		body = appendIncElement(body, bsontype.Type(sum[0]), path[0], sum[1:])
	}
	if !placed {
		body = appendCreated(body, path, amount)
	}
	return frameIncDocument(body), nil
}

// appendCreated appends the element of an absent path: the amount itself, or the documents that lead to it.
func appendCreated(body []byte, path []string, amount []byte) []byte {
	if len(path) == 1 {
		return appendIncElement(body, bsontype.Type(amount[0]), path[0], amount[1:])
	}
	inner := appendCreated(nil, path[1:], amount)
	return appendIncElement(body, bsontype.EmbeddedDocument, path[0], frameIncDocument(inner))
}

func appendIncElement(body []byte, kind bsontype.Type, key string, value []byte) []byte {
	body = append(body, byte(kind))
	body = append(body, key...)
	body = append(body, 0)
	return append(body, value...)
}

// frameIncDocument frames elements as a document: the length, the elements, the terminator.
func frameIncDocument(elements []byte) []byte {
	document := binary.LittleEndian.AppendUint32(make([]byte, 0, len(elements)+5), uint32(len(elements)+5))
	document = append(document, elements...)
	return append(document, 0)
}

// incPlan is what an inc does to the element under key: the element now (old, nil when absent) and the
// element after (next). It reads from snapshot, so the owner's precondition, the dry run at check 6a and
// the apply all compute from the one prior element.
func incPlan(snapshot storage.KVSnapshot, ref store.FieldRef, arg verbArg) (old, next []byte, err error) {
	old, found, err := store.FieldGet(snapshot, ref, arg.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("reading the element an inc would change: %w", err)
	}
	if !found {
		old = nil
	}
	terms, err := incTermsOf(arg.Element)
	if err != nil {
		return nil, nil, err
	}
	next, err = incElement(old, terms)
	return old, next, err
}

// incRefusal is W-I1 and W-I2: the text refusing an inc whose element has a non-number where it adds, or
// whose sum would leave an int64, and "" when the inc applies. The aggregates' refusal of the new element
// (W-A1) is aggregateRefusal's, which reads the same plan.
func (a *DocumentApplier) incRefusal(slot uint16, db uint64, op cluster.Op, engine store.FieldEngine, arg verbArg) (string, error) {
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	ref := a.fieldRef(slot, db, op.BucketID, op.EncID, op.WrittenFields()[0].FieldID, engine)
	_, next, err := incPlan(snapshot, ref, arg)
	var refused *incError
	if errors.As(err, &refused) {
		return fmt.Sprintf("inc on key %q: %s. Nothing was written. Add only to paths that hold a number: put the element again with a number at that path, "+
			"or take it and inc afresh (help inc)", arg.Key, refused), nil
	}
	if err == nil && len(next) > maxVerbArgBytes {
		// A put of this element is refused at the same bound, so repeated incs of new paths cannot grow it past what put takes.
		return fmt.Sprintf("inc on key %q would make the element %d bytes and at most %d bytes are taken, the bound a put has. Nothing was written. "+
			"Keep the element smaller: take it and split it over more keys, or inc paths that already exist (help inc)", arg.Key, len(next), maxVerbArgBytes), nil
	}
	return "", err
}

// incResultFits refuses an inc that asks for the new element back when it would not fit a dedupe entry,
// before a position is taken, as a pop or take is refused.
func (a *DocumentApplier) incResultFits(slot uint16, db uint64, op cluster.Op, engine store.FieldEngine, arg verbArg) (cluster.Code, error) {
	if a.resultCap <= 0 || !arg.Return {
		return 0, nil
	}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	ref := a.fieldRef(slot, db, op.BucketID, op.EncID, op.WrittenFields()[0].FieldID, engine)
	_, next, err := incPlan(snapshot, ref, arg)
	var refused *incError
	if errors.As(err, &refused) {
		return 0, nil // incRefusal words it
	}
	if err != nil {
		return 0, err
	}
	if len(next) > a.resultCap {
		return cluster.CodeResultTooLarge, nil
	}
	return 0, nil
}

// explainIncResult is the text of an inc refused for a return-after element larger than a dedupe entry
// holds, "" when op is not an inc or the refusal was for another reason. It repeats the plan because
// Explain is given the op alone.
func (a *DocumentApplier) explainIncResult(slot uint16, db uint64, op cluster.Op, where string) string {
	if len(op.WrittenFields()) != 1 || op.WrittenFields()[0].Verb != cluster.VerbInc {
		return ""
	}
	engine, err := a.engines.FieldEngine(db, op.BucketID, op.WrittenFields()[0].FieldID)
	if err != nil {
		return ""
	}
	arg, err := decodeVerbArg(shapeInc, op.WrittenFields()[0].Value)
	if err != nil {
		return ""
	}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	ref := a.fieldRef(slot, db, op.BucketID, op.EncID, op.WrittenFields()[0].FieldID, engine)
	_, next, err := incPlan(snapshot, ref, arg)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("an inc on %s with return would return the element (%d bytes) and a dedupe entry holds %d; nothing was written; "+
		"run inc without return and read it with get", where, len(next), a.resultCap)
}

// fillInc applies an inc: the element under key plus the terms, replacing the element and moving the
// declared aggregates from the old element to the new one in the same batch. The recorded result is the
// new element when the inc asked for it and nothing otherwise.
func (a *DocumentApplier) fillInc(write store.FieldWrite, ref store.FieldRef, arg verbArg, agg *store.AggregateBatch, rec AppliedRecord) ([]byte, error) {
	old, next, err := incPlan(write.Snap, ref, arg)
	if err != nil {
		return nil, fmt.Errorf("position %s: computing the inc of key %q: %w", rec.Pos, arg.Key, err)
	}
	if old != nil {
		if err := aggAdd(agg, old, -1); err != nil {
			return nil, fmt.Errorf("position %s: %w", rec.Pos, err)
		}
	}
	if err := aggAdd(agg, next, 1); err != nil {
		return nil, fmt.Errorf("position %s: %w", rec.Pos, err)
	}
	if err := store.FieldPut(write, ref, arg.Key, next); err != nil {
		return nil, fmt.Errorf("writing the inc of position %s: %w", rec.Pos, err)
	}
	if arg.Return {
		return next, nil
	}
	return nil, nil
}
