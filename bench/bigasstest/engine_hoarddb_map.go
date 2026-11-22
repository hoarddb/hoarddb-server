package main

import (
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// ringScalars lists the scalar (non-store) field names of each bucket of each layout in
// declared order. A full Get also returns a placeholder text for every store field, so the
// adapter asks for the scalars by name.
var ringScalars = map[model.Layout]map[string][]string{}

func init() {
	for _, layout := range []model.Layout{model.Social, model.Chat, model.Ledger, model.Agent} {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			panic(fmt.Sprintf("model rejects layout %s: %v", layout, err)) // invariant: the layouts are compiled in
		}
		buckets := map[string][]string{}
		for _, bucket := range gen.Schema() {
			for _, field := range bucket.Fields {
				if !field.IsStore() {
					buckets[bucket.Name] = append(buckets[bucket.Name], field.Name)
				}
			}
		}
		ringScalars[layout] = buckets
	}
}

// ringIDField is the key the server puts the document id under in a stored document.
const ringIDField = "_id_"

// ringFields decodes a stored BSON document into model fields in stored order: strings,
// int64 (an int32 widens), and a binary as []byte. The id member is left out. Any other
// BSON type is an error, because the four layouts hold nothing else.
func ringFields(raw bson.Raw) ([]model.Field, error) {
	members, err := raw.Elements()
	if err != nil {
		return nil, fmt.Errorf("reading the members of a stored document: %w", err)
	}
	fields := make([]model.Field, 0, len(members))
	for _, member := range members {
		name, err := member.KeyErr()
		if err != nil {
			return nil, fmt.Errorf("reading a member name of a stored document: %w", err)
		}
		if name == ringIDField {
			continue
		}
		value, err := member.ValueErr()
		if err != nil {
			return nil, fmt.Errorf("reading member %q of a stored document: %w", name, err)
		}
		switch value.Type {
		case bson.TypeString:
			fields = append(fields, model.Field{Name: name, Value: value.StringValue()})
		case bson.TypeInt32:
			fields = append(fields, model.Field{Name: name, Value: int64(value.Int32())})
		case bson.TypeInt64:
			fields = append(fields, model.Field{Name: name, Value: value.Int64()})
		case bson.TypeDateTime:
			fields = append(fields, model.Field{Name: name, Value: value.DateTime()})
		case bson.TypeBinary:
			_, data := value.Binary()
			fields = append(fields, model.Field{Name: name, Value: append([]byte(nil), data...)})
		default:
			return nil, fmt.Errorf("member %q of a stored document has BSON type %s, which the four layouts do not use", name, value.Type)
		}
	}
	return fields, nil
}

// ringDocument turns a stored document of bucket into a model document.
func ringDocument(bucket, id string, raw bson.Raw) (model.Document, error) {
	fields, err := ringFields(raw)
	if err != nil {
		return model.Document{}, fmt.Errorf("document %s/%s: %w", bucket, id, err)
	}
	return model.Document{Bucket: bucket, ID: id, Fields: fields}, nil
}

// ringElement turns one element a store returned into a model element. key is empty for a
// fifo or lifo store.
func ringElement(ref StoreRef, key string, raw bson.Raw) (model.Element, error) {
	fields, err := ringFields(raw)
	if err != nil {
		return model.Element{}, fmt.Errorf("element of %s/%s/%s: %w", ref.Bucket, ref.Owner, ref.Store, err)
	}
	return model.Element{Bucket: ref.Bucket, Owner: ref.Owner, Store: ref.Store, Key: key, Body: fields}, nil
}

// ringBody is the BSON a push or put sends for an element: the fields in the model's order,
// an embedding vector as a generic binary (the layout's wire form).
func ringBody(fields []model.Field) bson.D {
	body := make(bson.D, len(fields))
	for i, field := range fields {
		value := field.Value
		if packed, ok := field.Value.([]byte); ok {
			value = primitive.Binary{Subtype: 0, Data: packed}
		}
		body[i] = bson.E{Key: field.Name, Value: value}
	}
	return body
}

// ringDoc is the scalar fields of a document as the client marshals them.
func ringDoc(fields []model.Field) map[string]any {
	doc := make(map[string]any, len(fields))
	for _, field := range fields {
		if packed, ok := field.Value.([]byte); ok {
			doc[field.Name] = primitive.Binary{Subtype: 0, Data: packed}
			continue
		}
		doc[field.Name] = field.Value
	}
	return doc
}
