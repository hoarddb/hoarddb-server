package cli

import (
	"context"
	"fmt"
	"slices"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringNearestArgs is the usage line of nearest's one argument, for refusals.
const ringNearestArgs = `{vector: <binary>, k: <n>, metric: "<metric>", field: "<name>"}`

// nearestQueryOf reads nearest's one document argument. The vector is required and must be a binary
// of subtype 0, which the $binary literal makes; k, metric and field may be left out and then take
// the client's defaults. Any other key is refused by name, so a misspelt member cannot run a default
// query without a word.
func (c ringStoreCall) nearestQueryOf() (ringclient.NearestQuery, error) {
	if err := c.arity(ringNearestArgs, 1, 1); err != nil {
		return ringclient.NearestQuery{}, err
	}
	doc, err := c.document(0, ringNearestArgs)
	if err != nil {
		return ringclient.NearestQuery{}, err
	}
	var query ringclient.NearestQuery
	hasVector := false
	for _, entry := range doc {
		switch entry.Key {
		case "vector":
			binary, ok := entry.Value.(primitive.Binary)
			if !ok || binary.Subtype != 0 {
				return ringclient.NearestQuery{}, c.badArguments(ringNearestArgs,
					`takes a binary here: write the vector as {"$binary": {"base64": "<base64>", "subType": "00"}}`)
			}
			query.Vector = binary.Data
			hasVector = true
		case "k":
			k, ok := entry.Value.(int64)
			if !ok || k < 1 || k > 100 {
				return ringclient.NearestQuery{}, c.badArguments(ringNearestArgs, "takes k from 1 to 100")
			}
			query.K = int(k)
		case "metric":
			metric, ok := entry.Value.(string)
			if !ok || !slices.Contains([]string{"cosine", "dot", "l2"}, metric) {
				return ringclient.NearestQuery{}, c.badArguments(ringNearestArgs, "takes metric cosine, dot or l2")
			}
			query.Metric = metric
		case "field":
			field, ok := entry.Value.(string)
			if !ok || field == "" {
				return ringclient.NearestQuery{}, c.badArguments(ringNearestArgs,
					"takes field as a non-empty string, the name of the element field that holds the vector")
			}
			query.Field = field
		default:
			return ringclient.NearestQuery{}, c.badArguments(ringNearestArgs,
				fmt.Sprintf("takes vector, k, metric and field, and %q is not one of them", terminalSafe(entry.Key)))
		}
	}
	if !hasVector {
		return ringclient.NearestQuery{}, c.badArguments(ringNearestArgs, "needs the vector: {vector: <binary>}")
	}
	return query, nil
}

// nearestLines renders a nearest result, best first: the score to six places, the key when the store
// has keys, then the element as stored (the same text list prints). Elements that lack the field are
// counted, not printed, and one line says how many; an empty result says so rather than printing nothing.
func (s *RingSession) nearestLines(c ringStoreCall, result ringclient.NearestResult, field string) ([]string, error) {
	if len(result.Scores) != len(result.Elements) {
		return nil, fmt.Errorf("the nearest answer holds %d elements and %d scores, one score per element is expected; "+
			"the server and this client disagree, so update whichever is older", len(result.Elements), len(result.Scores))
	}
	lines := make([]string, 0, len(result.Elements)+1)
	keyed := len(result.Keys) == len(result.Elements)
	for i, element := range result.Elements {
		text, err := s.ringJSON(element)
		if err != nil {
			return nil, err
		}
		line := fmt.Sprintf("%.6f  ", result.Scores[i])
		if keyed {
			line += terminalSafe(result.Keys[i]) + "  "
		}
		lines = append(lines, line+text)
		if s.pretty && i < len(result.Elements)-1 {
			lines = append(lines, "")
		}
	}
	if result.Skipped > 0 {
		noun := "elements"
		if result.Skipped == 1 {
			noun = "element"
		}
		lines = append(lines, fmt.Sprintf("skipped: %d %s without field \"%s\"", result.Skipped, noun, terminalSafe(field)))
	}
	if len(result.Elements) == 0 && result.Skipped == 0 {
		lines = append(lines, "(empty)")
	}
	return lines, nil
}

// nearest runs the verb: it reads the query, asks the bucket, and prints nearestLines. An absent vector
// field name is the client default "Vector", named here so the skipped line says which field it skipped.
func (s *RingSession) nearest(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	query, err := c.nearestQueryOf()
	if err != nil {
		return RingOutput{}, err
	}
	result, err := c.bucket.Nearest(ctx, c.id, c.field.Name, query)
	if err != nil {
		return RingOutput{}, err
	}
	field := query.Field
	if field == "" {
		field = "Vector"
	}
	lines, err := s.nearestLines(c, result, field)
	if err != nil {
		return RingOutput{}, err
	}
	return RingOutput{Lines: lines}, nil
}
