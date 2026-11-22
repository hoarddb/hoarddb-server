package wire

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// Field projection (build SPEC-GAP U87-1 to U87-7): a get or a list may name the declared
// scalar fields to return. The body keeps _id_ first and the named scalars in field-id
// order; a store field is never part of a projection, so a projected read opens no store
// state at all.

const (
	// ringMaxFields is the most names a projection holds.
	ringMaxFields = 64
	// ringMaxFieldName is the longest field name a projection carries; a declared field
	// name is at most this long.
	ringMaxFieldName = 255
	// ringMaxFieldsBytes bounds the BSON array of a list request's fields member, checked
	// before the array is unpacked: 64 names of 255 bytes with their element headers.
	ringMaxFieldsBytes = ringMaxFields*(ringMaxFieldName+8) + 64
)

// idFieldName is the name of the document id in a body. A projection always returns it, so
// naming it is allowed and changes nothing.
const idFieldName = "_id_"

func projectionRefusal(format string, args ...any) error {
	return ringRefusal(format, args...)
}

// checkFieldNames validates the names of a projection as they ride on the wire. member
// is the wire member's name, for the message.
// CheckRingGetFields says whether names are a fields list a get_fields carries: 1 to 64 names, each
// valid UTF-8 of 1 to 256 bytes, none twice. A client calls it to refuse one key before it builds
// a frame, with the text the server's codec would give.
func CheckRingGetFields(names []string) error { return checkFieldNames("fields", names) }

func checkFieldNames(member string, names []string) error {
	switch {
	case len(names) == 0:
		return projectionRefusal("%s is empty. Send at least one field name such as [\"Name\"], or leave %s out to get every field", member, member)
	case len(names) > ringMaxFields:
		return projectionRefusal("%s has %d names and at most %d are taken. Send the names you need, or leave %s out to get every field", member, len(names), ringMaxFields, member)
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		switch {
		case len(name) == 0 || len(name) > ringMaxFieldName:
			return projectionRefusal("a name in %s is %d bytes and a field name is 1 to %d. Send names that 'describe bucket <name>' lists", member, len(name), ringMaxFieldName)
		case !utf8.ValidString(name):
			return projectionRefusal("a name in %s is not valid UTF-8. Send field names as text", member)
		}
		if _, twice := seen[name]; twice {
			return projectionRefusal("%s names %q twice. Send each field name once", member, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// rawFieldNames reads the fields member of a list request, which has been through the
// bounded validator. present is false when the member is absent. Its size is bounded
// before it is unpacked and every element must be a string, so what comes back is what
// checkFieldNames can judge.
func rawFieldNames(raw []byte) (names []string, present bool, err error) {
	value, lookupErr := bson.Raw(raw).LookupErr("fields")
	if lookupErr != nil {
		return nil, false, nil
	}
	array, isArray := value.ArrayOK()
	if !isArray {
		return nil, true, projectionRefusal("fields is a %s and must be an array of field names such as [\"Name\", \"Age\"]", value.Type)
	}
	if len(array) > ringMaxFieldsBytes {
		return nil, true, projectionRefusal("fields is %d bytes and at most %d are taken (%d names of %d bytes). Send the names you need, or leave fields out to get every field", len(array), ringMaxFieldsBytes, ringMaxFields, ringMaxFieldName)
	}
	elements, elementsErr := array.Elements()
	if elementsErr != nil {
		return nil, true, projectionRefusal("fields is not a valid array: %v. Send an array of field names such as [\"Name\", \"Age\"]", elementsErr)
	}
	if len(elements) > ringMaxFields {
		return nil, true, projectionRefusal("fields has %d names and at most %d are taken. Send the names you need, or leave fields out to get every field", len(elements), ringMaxFields)
	}
	names = make([]string, 0, len(elements))
	for position, element := range elements {
		name, isString := element.Value().StringValueOK()
		if !isString {
			return nil, true, projectionRefusal("fields must hold strings and element %d is a %s. Send field names such as [\"Name\", \"Age\"]", position, element.Value().Type)
		}
		names = append(names, name)
	}
	return names, true, nil
}

// projection resolves the names of a projection against the bucket's declaration and
// returns the field ids to keep, ascending, which is the order of a body. _id_ is dropped
// (it is always returned), so the result may be empty but is never nil. An undeclared name
// is FIELD_TYPE with the describe fix; a store field is FIELD_TYPE naming the verb that
// reads it.
func (b bucketRef) projection(names []string) ([]uint16, error) {
	ids := make([]uint16, 0, len(names))
	for _, name := range names {
		if name == idFieldName {
			continue
		}
		entry, found := entryNamed(b.table, name)
		switch {
		case !found:
			return nil, &cluster.Error{Code: cluster.CodeFieldType, Message: fmt.Sprintf(
				"field %q is not declared in %s.%s (declared scalar fields: %s). Nothing was read. Run 'describe bucket %s in %s' to see the fields, or correct the name",
				name, b.db, b.name, declaredScalars(b.table), b.name, b.db)}
		case isStoreType(entry.Type):
			verb := "list"
			if entry.Type == "blobStore" {
				verb = "count" // the one verb a blobStore has on the ring
			}
			readIt := fmt.Sprintf("db.%s[\"<id>\"].%s.%s()", b.name, name, verb)
			return nil, &cluster.Error{Code: cluster.CodeFieldType, Message: fmt.Sprintf(
				"field %q in %s.%s is a %s store field, and fields returns scalar fields only. Nothing was read. Run '%s' to read the store, or take %q out of fields",
				name, b.db, b.name, strings.TrimSuffix(entry.Type, "Store"), readIt, name)}
		}
		ids = append(ids, entry.ID)
	}
	slices.Sort(ids)
	return slices.Compact(ids), nil
}

// keepsField reports whether a body with this projection holds the scalar field id; a nil
// projection holds every field.
func keepsField(project []uint16, id uint16) bool {
	return project == nil || slices.Contains(project, id)
}
