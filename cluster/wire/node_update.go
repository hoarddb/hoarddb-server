package wire

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
)

// UpdateRequest changes scalar fields of an existing document.
type UpdateRequest struct {
	Database, Bucket, ID string
	// Set replaces the value of each field it names.
	Set bson.D
	// Unset removes each field it names.
	Unset []string
	// Inc adds each value to the number its field holds; a field with no value takes the
	// amount.
	Inc bson.D
	// Mul multiplies the number each field holds by the value; a field with no value takes
	// zero of the value's type, as MongoDB's $mul does.
	Mul bson.D
	// Min keeps the smaller of the stored value and the given one; Max the larger.
	Min, Max bson.D
	// Push appends the value to the array its field holds; a field with no value becomes an
	// array of that one value.
	Push bson.D
	// AddToSet appends the value to the array unless an equal element is there.
	AddToSet bson.D
	// Rename moves the value of each field named by a key into the declared field named by
	// the string value, which must be a scalar field of the same declared type.
	Rename bson.D
}

// Updated is what an update returns.
type Updated struct {
	// Pos is the log position the update holds.
	Pos cluster.Position
	// Deduped says the answer was recomposed from an earlier attempt of the same request.
	Deduped bool
}

// Update changes scalar fields of an existing document with set, unset and inc. It is one
// logged, fenced owner write: all of its operations land together or none does. A document
// that does not exist, or is deleted, is refused; a store field is changed with its own
// verbs. An inc is not idempotent by value, so a retry must carry the request id of the
// first attempt, which the dedupe window answers.
func (n *Node) Update(ctx context.Context, req UpdateRequest, options ...WriteOption) (Updated, error) {
	opts, err := newWriteOptions(options)
	if err != nil {
		return Updated{}, err
	}
	ref, err := n.bucket(ctx, req.Database, req.Bucket)
	if err != nil {
		return Updated{}, err
	}
	fields, err := ref.updateOps(req)
	if err != nil {
		return Updated{}, err
	}
	encID, err := store.EncodeComponent(req.ID)
	if err != nil {
		return Updated{}, fmt.Errorf("encoding the _id_ %q: %w", req.ID, err)
	}
	op := ref.template()
	op.Class, op.Fields = cluster.OpUpdate, fields
	answer, err := n.send(ctx, ref, encID, op, opts)
	if err != nil {
		return Updated{}, err
	}
	return Updated{Pos: answer.Pos, Deduped: answer.Deduped}, nil
}

// updateOps turns a request into the update operations of conventions section 9.1, one per
// field in field-id order, and refuses with the field's name what the owner would refuse.
func (b bucketRef) updateOps(req UpdateRequest) ([]cluster.FieldOp, error) {
	var fields []cluster.FieldOp
	for _, part := range []struct {
		doc  bson.D
		code byte
	}{{req.Set, cluster.UpdateSet}, {req.Inc, cluster.UpdateInc}, {req.Mul, cluster.UpdateMul}, {req.Min, cluster.UpdateMin},
		{req.Max, cluster.UpdateMax}, {req.Push, cluster.UpdatePush}, {req.AddToSet, cluster.UpdateAddToSet}} {
		if len(part.doc) == 0 {
			continue
		}
		for _, element := range part.doc {
			if element.Key == "_id_" {
				return nil, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
					"an update of %s.%s cannot change _id_. Nothing was changed. Change the other fields, or insert the document under its new _id_", b.db, b.name)}
			}
		}
		if part.code == cluster.UpdatePush || part.code == cluster.UpdateAddToSet {
			if err := b.refuseModifier(part.code, part.doc); err != nil {
				return nil, err
			}
		}
		_, ops, err := b.documentOp(part.doc)
		if err != nil {
			return nil, err
		}
		for _, op := range ops {
			op.OpCode = part.code
			fields = append(fields, op)
		}
	}
	for _, name := range req.Unset {
		entry, ok := entryNamed(b.table, name)
		if !ok || isStoreType(entry.Type) {
			return nil, &cluster.Error{Code: cluster.CodeFieldType, Message: cluster.CodeFieldType.Text(cluster.CondFieldNotDeclared, map[string]string{
				"field": name, "db": b.db, "bucket": b.name, "declared": declaredScalars(b.table),
			})}
		}
		fields = append(fields, cluster.FieldOp{FieldID: entry.ID, OpCode: cluster.UpdateUnset})
	}
	renames, err := b.renameOps(req.Rename)
	if err != nil {
		return nil, err
	}
	fields = append(fields, renames...)
	slices.SortStableFunc(fields, func(a, c cluster.FieldOp) int { return int(a.FieldID) - int(c.FieldID) })
	if reason := updateOpsRefusal(fields, b.fieldWord); reason != "" {
		return nil, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("update of %s.%s: %s", b.db, b.name, reason)}
	}
	return fields, nil
}

// fieldWord words a field id by its declared name.
func (b bucketRef) fieldWord(id uint16) string {
	if entry, ok := entryByID(b.table, id); ok {
		return "field " + entry.Name
	}
	return fieldIDWord(id)
}

// renameOps turns {source: "target"} pairs into rename operations, refusing with names what
// the owner would refuse by ids.
func (b bucketRef) renameOps(pairs bson.D) ([]cluster.FieldOp, error) {
	fields := make([]cluster.FieldOp, 0, len(pairs))
	for _, pair := range pairs {
		targetName, ok := pair.Value.(string)
		if !ok {
			return nil, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"rename of %s in %s.%s needs the target field's name as a string, got %T. Nothing was changed. Send {%s: \"<declared field>\"}", pair.Key, b.db, b.name, pair.Value, pair.Key)}
		}
		source, ok := entryNamed(b.table, pair.Key)
		if !ok || isStoreType(source.Type) {
			return nil, &cluster.Error{Code: cluster.CodeFieldType, Message: cluster.CodeFieldType.Text(cluster.CondFieldNotDeclared, map[string]string{
				"field": pair.Key, "db": b.db, "bucket": b.name, "declared": declaredScalars(b.table),
			})}
		}
		target, ok := entryNamed(b.table, targetName)
		if !ok {
			return nil, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"rename of %s in %s.%s: %q is not a declared field, and a field cannot get a name the bucket does not declare. Nothing was changed. Declare %q first with alter bucket (add a field of type %s), or rename to one of the declared scalars: %s",
				pair.Key, b.db, b.name, targetName, targetName, source.Type, declaredScalars(b.table))}
		}
		if reason := b.renameTargetRefusal(source, target); reason != "" {
			return nil, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("rename of %s in %s.%s: %s", source.Name, b.db, b.name, reason)}
		}
		fields = append(fields, cluster.FieldOp{FieldID: source.ID, OpCode: cluster.UpdateRenameTo, Value: binary.BigEndian.AppendUint16(nil, target.ID)})
	}
	return fields, nil
}

// renameTargetRefusal words why a value cannot move from source to target, or returns "":
// the target is a store, or is declared with another type word.
func (b bucketRef) renameTargetRefusal(source, target machine.FieldEntry) string {
	switch {
	case isStoreType(target.Type):
		return fmt.Sprintf("%s is a %s, and a rename moves a value into a scalar field. Nothing was changed. Change a store with its own verbs: push, pop, put or take", target.Name, target.Type)
	case b.declaresReference(target.ID):
		return fmt.Sprintf("%s is declared of another bucket, and a rename would move a value into it that nobody checked is an id. Nothing was changed. Set %s to the id as text, and unset %s", target.Name, target.Name, source.Name)
	case source.Type != target.Type:
		return fmt.Sprintf("%s is declared %s and %s is declared %s, and a rename moves a value between fields of the same type. Nothing was changed. Read the value and write it with set, or declare a field of type %s with alter bucket", source.Name, source.Type, target.Name, target.Type, source.Type)
	}
	return ""
}

// declaresReference reports that the field with this id is declared 'of' another bucket.
func (b bucketRef) declaresReference(fieldID uint16) bool {
	_, declared := b.entry.Decl.ReferenceOf(fieldID)
	return declared
}

// referenceRefusal is W-F1 for update: every operation that writes a value into a field
// declared 'of' a bucket must write text or null, the form of an _id_. It reads no document.
// A rename is refused into such a field by renameRefusal.
func (b bucketRef) referenceRefusal(fields []cluster.FieldOp, targets referenceNamer) error {
	for _, field := range fields {
		if field.OpCode == cluster.UpdateUnset || field.OpCode == cluster.UpdateRenameTo || len(field.Value) == 0 {
			continue
		}
		entry, ok := entryByID(b.table, field.FieldID)
		if !ok {
			continue
		}
		if err := ringCheckReferenceValue(b, entry, field.Value, targets); err != nil {
			return err
		}
	}
	return nil
}

// renameRefusal checks the rename operations of an update sent by id against the declaration:
// the target is a declared scalar field of the source's type.
func (b bucketRef) renameRefusal(fields []cluster.FieldOp) error {
	for _, field := range fields {
		if field.OpCode != cluster.UpdateRenameTo {
			continue
		}
		source, _ := entryByID(b.table, field.FieldID)
		target, ok := entryByID(b.table, binary.BigEndian.Uint16(field.Value))
		if !ok {
			return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"rename of %s in %s.%s names field id %d, which the bucket does not declare. Nothing was changed. Declare the target first with alter bucket (add a field of type %s)",
				source.Name, b.db, b.name, binary.BigEndian.Uint16(field.Value), source.Type)}
		}
		if reason := b.renameTargetRefusal(source, target); reason != "" {
			return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("rename of %s in %s.%s: %s", source.Name, b.db, b.name, reason)}
		}
	}
	return nil
}

// refuseModifier refuses MongoDB's each, position, slice and sort of push and addToSet by
// name: this build appends one plain value. It reads the BSON the operand marshals to, so a
// bson.D, a bson.M, a struct or a bson.Raw are all seen.
func (b bucketRef) refuseModifier(code byte, doc bson.D) error {
	for _, element := range doc {
		kind, raw, err := bson.MarshalValue(element.Value)
		if err != nil || kind != bson.TypeEmbeddedDocument {
			continue
		}
		elements, err := bson.Raw(raw).Elements()
		if err != nil {
			continue
		}
		for _, inner := range elements {
			if strings.HasPrefix(inner.Key(), "$") {
				return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
					"%s of %s in %s.%s does not take %s: only one plain value is appended. Nothing was changed. Send the value itself, or one update for each value",
					updateCodeName(code), element.Key, b.db, b.name, inner.Key())}
			}
		}
	}
	return nil
}
