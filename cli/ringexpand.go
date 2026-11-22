package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// This file is fetch-through in the CLI: the expand, where and local options of list and get,
// and how an expanded page prints. The server does the reads; here the options are parsed and
// each row's expansion is printed under the row.

// ringExpandReader is what a bucket offers beyond RingBucket for fetch-through. The live
// client has both; a test double that does not is refused with the fix.
type ringExpandReader interface {
	GetExpanded(ctx context.Context, id string, fields []string, expand []ringclient.ExpandRequest, local bool) (document, expanded bson.Raw, err error)
	StoreListExpanded(ctx context.Context, id, field string, limit int, after []byte, expand *ringclient.ExpandRequest, local bool) (ringclient.ElementPage, error)
}

// ringExpandOptions are the parsed expand, where and local members of a statement.
type ringExpandOptions struct {
	expand []ringclient.ExpandRequest
	local  bool
}

// ringTarget splits "<Bucket>" or "<Database>.<Bucket>" as the options write it.
func ringExpandTarget(text string) (database, bucket string) {
	for i := 0; i < len(text); i++ {
		if text[i] == '.' {
			return text[:i], text[i+1:]
		}
	}
	return "", text
}

// ringExpandOne reads one expansion's value: true (the declared reference), a bucket name, or
// {bucket: "<Bucket>", fields: [...]}. usage is what a refusal tells the developer to run.
func ringExpandOne(field string, spelled Value, usage string) (ringclient.ExpandRequest, error) {
	request := ringclient.ExpandRequest{Field: field}
	switch spelled.Kind {
	case ValueBool:
		if !spelled.Bool {
			return request, ringRefusal("PARSE_ERROR", "expand takes true, a bucket name or {bucket: \"<Bucket>\", fields: [...]}, and false names nothing to read. Run '%s'", usage)
		}
	case ValueString:
		request.Database, request.Bucket = ringExpandTarget(spelled.Text)
	case ValueDoc:
		for _, entry := range spelled.Entries {
			switch entry.Key {
			case "bucket":
				if entry.Value.Kind != ValueString {
					return request, ringRefusal("PARSE_ERROR", "bucket is a bucket name in double quotes, such as \"User\" or \"ref.User\". Run '%s'", usage)
				}
				request.Database, request.Bucket = ringExpandTarget(entry.Value.Text)
			case "fields":
				names, err := ringFieldNames(entry.Value, usage)
				if err != nil {
					return request, err
				}
				request.Fields = names
			default:
				return request, ringRefusal("PARSE_ERROR", "an expansion takes bucket and fields, and %q is not one. Run '%s'", entry.Key, usage)
			}
		}
	default:
		return request, ringRefusal("PARSE_ERROR", "expand takes true, a bucket name or {bucket: \"<Bucket>\", fields: [...]}. Run '%s'", usage)
	}
	return request, nil
}

// ringExpandFields reads expand of a bucket list or a get: {<Field>: true | "<Bucket>" | {...}, ...}.
func ringExpandFields(spelled Value, usage string) ([]ringclient.ExpandRequest, error) {
	if spelled.Kind != ValueDoc || len(spelled.Entries) == 0 {
		return nil, ringRefusal("PARSE_ERROR", "expand names the fields to read, each with true, a bucket name or {bucket, fields}. Run '%s'", usage)
	}
	var requests []ringclient.ExpandRequest
	for _, entry := range spelled.Entries {
		request, err := ringExpandOne(entry.Key, entry.Value, usage)
		if err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, nil
}

// ringWhere attaches a where document to the one expansion it filters.
func ringWhere(requests []ringclient.ExpandRequest, where *Value, usage string) error {
	if where == nil {
		return nil
	}
	if len(requests) != 1 || where.Kind != ValueDoc {
		return ringRefusal("PARSE_ERROR", "where is an equality document on the one expanded document, such as {Name: \"Ada\"}, and needs exactly one expanded field. Run '%s'", usage)
	}
	requests[0].Where = ringBSON(*where)
	return nil
}

// ringBoolOption reads local: true|false.
func ringBoolOption(key string, spelled Value, usage string) (bool, error) {
	if spelled.Kind != ValueBool {
		return false, ringRefusal("PARSE_ERROR", "%s is true or false. Run '%s'", key, usage)
	}
	return spelled.Bool, nil
}

// ringExpansionLines prints the expansions of one row, one indented line per expanded field.
func (s *RingSession) ringExpansionLines(answer bson.Raw) ([]string, error) {
	elements, err := answer.Elements()
	if err != nil {
		return nil, fmt.Errorf("the server sent an expansion that is not valid BSON: %w", err)
	}
	lines := make([]string, 0, len(elements))
	for _, element := range elements {
		inner, ok := element.Value().DocumentOK()
		if !ok {
			return nil, fmt.Errorf("the server sent the expansion %q as something other than a document", element.Key())
		}
		if !inner.Lookup("expanded").Boolean() {
			reason := "not found"
			if text, has := inner.Lookup("reason").StringValueOK(); has && text == "id_type" {
				reason = "not an id of that bucket"
			}
			lines = append(lines, fmt.Sprintf("  %s -> (%s)", element.Key(), reason))
			continue
		}
		doc, ok := inner.Lookup("doc").DocumentOK()
		if !ok {
			return nil, fmt.Errorf("the server sent the expansion %q without its document", element.Key())
		}
		text, err := s.ringJSON(doc)
		if err != nil {
			return nil, err
		}
		lines = append(lines, fmt.Sprintf("  %s -> %s", element.Key(), text))
	}
	return lines, nil
}

// ringServedLine is the line that says which side answered an expanded read.
func ringServedLine(served string) []string {
	if served == "" {
		return nil
	}
	return []string{"served: " + served}
}

// ringExpandText writes expansions back the way the statement spelled them, for a "more:" line.
func ringExpandText(requests []ringclient.ExpandRequest) string {
	parts := make([]string, len(requests))
	for i, request := range requests {
		parts[i] = request.Field + ": " + ringExpandOneText(request)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// ringExpandOneText is the value of one expansion: true, a bucket name, or the object form when
// the statement named fields. A where is not written back: it is a BSON document here.
func ringExpandOneText(request ringclient.ExpandRequest) string {
	bucket := request.Bucket
	if request.Database != "" {
		bucket = request.Database + "." + request.Bucket
	}
	if len(request.Fields) == 0 {
		if bucket == "" {
			return "true"
		}
		return strconv.Quote(bucket)
	}
	quoted := make([]string, len(request.Fields))
	for i, field := range request.Fields {
		quoted[i] = strconv.Quote(field)
	}
	object := "fields: [" + strings.Join(quoted, ", ") + "]"
	if bucket != "" {
		object = "bucket: " + strconv.Quote(bucket) + ", " + object
	}
	return "{" + object + "}"
}
