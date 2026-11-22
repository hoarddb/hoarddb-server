package cli

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/ringclient"

	"github.com/hoarddb/hoarddb-bson/bson"
)

const (
	ringSumArgs      = `"<Field>"[, {by: "<Field>"}]`
	ringCountByArgs  = `{by: "<Field>"}`
	ringAggregateMax = 10000
)

// aggregateOptions reads the {by: "<field>", limit: <n>, cursor: "<hex>"} document of a sum or a
// count, returning the three. A key outside those is refused, naming the form that works.
func (c ringStoreCall) aggregateOptions(index int, args string) (by string, limit int, after []byte, err error) {
	if index >= len(c.values) {
		return "", 0, nil, nil
	}
	doc, err := c.document(index, args)
	if err != nil {
		return "", 0, nil, err
	}
	for _, entry := range doc {
		switch entry.Key {
		case "by":
			text, ok := entry.Value.(string)
			if !ok || text == "" {
				return "", 0, nil, c.badArguments(args, "takes by as a double-quoted element field path")
			}
			by = text
		case "limit":
			number, ok := ringInt(entry.Value)
			if !ok || number < 1 || number > ringAggregateMax {
				return "", 0, nil, c.badArguments(args, fmt.Sprintf("takes limit as a number from 1 to %d", ringAggregateMax))
			}
			limit = int(number)
		case "cursor":
			text, ok := entry.Value.(string)
			if !ok {
				return "", 0, nil, c.badArguments(args, "takes cursor as the hex string a 'more:' line prints")
			}
			if after, err = hex.DecodeString(text); err != nil {
				return "", 0, nil, c.badArguments(args, fmt.Sprintf("takes the cursor a 'more:' line prints, and %q is not hex", text))
			}
		default:
			return "", 0, nil, c.badArguments(args, fmt.Sprintf("does not take %q; it takes by, limit and cursor", entry.Key))
		}
	}
	return by, limit, after, nil
}

// ringInt reads an integer a document decoded to, whichever Go width the decoder chose.
func ringInt(decoded any) (int64, bool) {
	switch number := decoded.(type) {
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case int:
		return int64(number), true
	}
	return 0, false
}

// aggregateStore runs sum("<field>"[, {by}]), count({by}) and describe().
func (s *RingSession) aggregateStore(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	var query ringclient.AggregateQuery
	switch c.verb {
	case "describe":
		if err := c.arity("", 0, 0); err != nil {
			return RingOutput{}, fmt.Errorf("describe on %s: %w", c.target(), err)
		}
		query.Kind = "describe"
	case "count":
		if err := c.arity(ringCountByArgs, 1, 1); err != nil {
			return RingOutput{}, fmt.Errorf("count on %s: %w", c.target(), err)
		}
		by, limit, after, err := c.aggregateOptions(0, ringCountByArgs)
		if err != nil {
			return RingOutput{}, fmt.Errorf("count on %s: %w", c.target(), err)
		}
		if by == "" {
			return RingOutput{}, c.badArguments(ringCountByArgs, "needs by: the element field to count per value (a plain count is count() with no argument)")
		}
		query = ringclient.AggregateQuery{Kind: "count by", By: by, Limit: limit, After: after}
	default:
		if err := c.arity(ringSumArgs, 1, 2); err != nil {
			return RingOutput{}, fmt.Errorf("sum on %s: %w", c.target(), err)
		}
		field, err := c.text(0, ringSumArgs)
		if err != nil {
			return RingOutput{}, fmt.Errorf("sum on %s: %w", c.target(), err)
		}
		by, limit, after, err := c.aggregateOptions(1, ringSumArgs)
		if err != nil {
			return RingOutput{}, fmt.Errorf("sum on %s: %w", c.target(), err)
		}
		query = ringclient.AggregateQuery{Kind: "sum", Field: field, Limit: limit, After: after}
		if by != "" {
			query.Kind, query.By = "sum by", by
		}
	}
	page, err := c.bucket.Aggregate(ctx, c.id, c.field.Name, query)
	if err != nil {
		return RingOutput{}, fmt.Errorf("reading %s on %s: %w", query.Kind, c.target(), err)
	}
	return ringAggregateLines(c, query, page)
}

// ringAggregateLines prints an aggregate answer: a sum as its number, a by form as one line per
// value, describe as one line per aggregate, and a 'more:' line when a by form has another page.
func ringAggregateLines(c ringStoreCall, query ringclient.AggregateQuery, page ringclient.AggregatePage) (RingOutput, error) {
	switch query.Kind {
	case "sum":
		sumField, err := page.Document.LookupErr("sum")
		if err != nil {
			return RingOutput{}, fmt.Errorf("the sum answer has no sum: %w", err)
		}
		return ringLines(ringNumberText(sumField)), nil
	case "describe":
		return ringDescribeLines(page.Document)
	}
	values, err := page.Document.LookupErr("values")
	if err != nil {
		return RingOutput{}, fmt.Errorf("the answer has no values: %w", err)
	}
	items, err := values.Array().Values()
	if err != nil {
		return RingOutput{}, fmt.Errorf("reading the values of the answer: %w", err)
	}
	if len(items) == 0 {
		return ringLines("(empty)"), nil
	}
	lines := make([]string, 0, len(items)+1)
	for _, group := range items {
		row := group.Document()
		line := ringValueText(row.Lookup("by")) + "  "
		if sum, err := row.LookupErr("sum"); err == nil {
			line += ringNumberText(sum) + "  "
		}
		lines = append(lines, line+ringNumberText(row.Lookup("n"))+" element(s)")
	}
	if page.More {
		parts := []string{}
		if query.By != "" && query.Kind == "sum by" {
			parts = append(parts, fmt.Sprintf("by: %q", query.By))
		}
		parts = append(parts, fmt.Sprintf("cursor: %q", hex.EncodeToString(page.Cursor)))
		verb := fmt.Sprintf("sum(%q, {%s})", query.Field, strings.Join(parts, ", "))
		if query.Kind == "count by" {
			verb = fmt.Sprintf("count({by: %q, cursor: %q})", query.By, hex.EncodeToString(page.Cursor))
		}
		lines = append(lines, fmt.Sprintf("more: %s.%s", c.target(), verb))
	}
	return RingOutput{Lines: lines}, nil
}

func ringDescribeLines(document bson.Raw) (RingOutput, error) {
	listed, err := document.LookupErr("aggregates")
	if err != nil {
		return RingOutput{}, fmt.Errorf("the describe answer has no aggregates: %w", err)
	}
	items, err := listed.Array().Values()
	if err != nil {
		return RingOutput{}, fmt.Errorf("reading the aggregates of the answer: %w", err)
	}
	if len(items) == 0 {
		return ringLines("(this store keeps no aggregates)"), nil
	}
	var lines []string
	for _, aggregateRow := range items {
		row := aggregateRow.Document()
		line := fmt.Sprintf("%s  %s", row.Lookup("aggregate").StringValue(), row.Lookup("state").StringValue())
		if sum, err := row.LookupErr("sum"); err == nil {
			line += "  " + ringNumberText(sum)
		}
		if distinct, err := row.LookupErr("by_values"); err == nil {
			line += fmt.Sprintf("  %s distinct value(s)", ringNumberText(distinct))
		}
		if warning, err := row.LookupErr("warning"); err == nil {
			line += "  WARNING: " + warning.StringValue()
		}
		lines = append(lines, line)
	}
	return RingOutput{Lines: lines}, nil
}

// ringNumberText prints a BSON number: an integer without a point, a double as Go prints it.
func ringNumberText(number bson.RawValue) string {
	if whole, ok := number.Int64OK(); ok {
		return fmt.Sprint(whole)
	}
	if whole, ok := number.Int32OK(); ok {
		return fmt.Sprint(whole)
	}
	if real, ok := number.DoubleOK(); ok {
		return fmt.Sprint(real)
	}
	return number.String()
}

// ringValueText prints a by value: a string bare, anything else as BSON prints it.
func ringValueText(raw bson.RawValue) string {
	if text, ok := raw.StringValueOK(); ok {
		return terminalSafe(text)
	}
	return raw.String()
}
