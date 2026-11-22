package cli

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringStoreCall is one store verb on one document's own store field.
type ringStoreCall struct {
	bucket   RingBucket
	database string
	name     string
	id       string
	field    ringclient.FieldInfo
	verb     string
	values   []Value
}

// target is how the statement spells its store field, for messages.
func (c ringStoreCall) target() string {
	return fmt.Sprintf("db.%s[%q].%s", c.name, c.id, c.field.Name)
}

// form is the statement that works, for messages.
func (c ringStoreCall) form(args string) string {
	return fmt.Sprintf("%s.%s(%s)", c.target(), c.verb, args)
}

func (c ringStoreCall) badArguments(args, why string) *RingError {
	return ringRefusal("PARSE_ERROR", "%s %s. Run '%s'", c.verb, why, c.form(args))
}

func (c ringStoreCall) text(index int, args string) (string, error) {
	if index >= len(c.values) || c.values[index].Kind != ValueString {
		return "", c.badArguments(args, "needs a double-quoted string here")
	}
	return c.values[index].Text, nil
}

func (c ringStoreCall) document(index int, args string) (bson.D, error) {
	if index >= len(c.values) || c.values[index].Kind != ValueDoc {
		return nil, c.badArguments(args, "needs a document here")
	}
	doc, _ := ringBSON(c.values[index]).(bson.D)
	return doc, nil
}

func (c ringStoreCall) arity(args string, least, most int) error {
	if len(c.values) < least || len(c.values) > most {
		return c.badArguments(args, fmt.Sprintf("takes %s, and %d were given", ringArity(least, most), len(c.values)))
	}
	return nil
}

func ringArity(least, most int) string {
	switch {
	case least == most && least == 0:
		return "no arguments"
	case least == most:
		return fmt.Sprintf("%d argument(s)", least)
	}
	return fmt.Sprintf("%d to %d arguments", least, most)
}

// lease reads a 32-hex lease id from argument index.
func (c ringStoreCall) lease(index int, args string) ([16]byte, error) {
	var lease [16]byte
	text, err := c.text(index, args)
	if err != nil {
		return lease, err
	}
	raw, decodeErr := hex.DecodeString(text)
	if decodeErr != nil || len(raw) != len(lease) {
		return lease, c.badArguments(args, fmt.Sprintf("takes the lease id claim printed: 32 hex digits, and %q is not", text))
	}
	copy(lease[:], raw)
	return lease, nil
}

// ringStoreMissing names an empty store or an absent key, never a bare "not found".
func (c ringStoreCall) empty(what string) *RingError {
	return &RingError{Code: "NOT_FOUND", Exit: ExitRefused, Message: fmt.Sprintf(
		"%s of %q in %s.%s %s. Run '%s.count()' to look without removing, or '%s' to add",
		c.field.Name, c.id, c.database, c.name, what, c.target(), c.target()+".push({…})")}
}

func (s *RingSession) storeVerb(ctx context.Context, bucket RingBucket, st Statement, database, name string) (RingOutput, error) {
	call := ringStoreCall{bucket: bucket, database: database, name: name, id: ringText(st, "id"), verb: ringText(st, "verb")}
	if values, ok := st.Arg("values"); ok {
		call.values = values.Values
	}
	info, err := bucket.Info(ctx)
	if err != nil {
		return RingOutput{}, err
	}
	fieldName := ringText(st, "field")
	var stores []string
	for _, field := range info.Fields {
		if ringStoreType(field.Type) {
			stores = append(stores, field.Name)
		}
		if field.Name == fieldName {
			call.field = field
		}
	}
	if call.field.Name == "" || !ringStoreType(call.field.Type) {
		return RingOutput{}, ringRefusal("FIELD_TYPE", "%s.%s declares no store field %q (its stores: %s). Run 'describe bucket %s in %s' to see the declaration. "+
			"Adding a field to a bucket that exists is not available yet",
			database, name, fieldName, ringStoreList(stores), name, database)
	}
	if refusal := ringUnwiredStoreVerb(call); refusal != nil {
		return RingOutput{}, refusal
	}
	switch call.verb {
	case "get":
		if ringLeasable(call.field.Type) {
			return s.leaseVerb(ctx, call)
		}
		return s.storeGet(ctx, call)
	case "push":
		return s.push(ctx, call)
	case "put":
		return s.put(ctx, call)
	case "inc":
		return s.inc(ctx, call)
	case "pop":
		return s.pop(ctx, call)
	case "take":
		return s.take(ctx, call)
	case "nearest":
		return s.nearest(ctx, call)
	case "peek":
		return s.peek(ctx, call)
	case "list":
		return s.listStore(ctx, call)
	case "count":
		return s.countStore(ctx, call)
	case "sum", "describe":
		return s.aggregateStore(ctx, call)
	}
	return s.leaseVerb(ctx, call)
}

func ringStoreList(stores []string) string {
	if len(stores) == 0 {
		return "none"
	}
	return strings.Join(stores, ", ")
}

// ringLeasable reports whether a declared store type is one a claim can lease.
func ringLeasable(word string) bool { return slices.Contains(ringQueueStores, word) }

// ringUnwiredStoreVerb refuses the pairings the verb table lists as NotYet, with
// the text that says so (SPEC-GAP U52-6): heap push/take and blob put. It reads ringVerbs, so a help page and a refusal cannot disagree.
func ringUnwiredStoreVerb(c ringStoreCall) *RingError {
	verb, known := ringVerbByName(c.verb)
	if !known || !slices.Contains(verb.NotYet, c.field.Type) {
		return nil
	}
	if c.field.Type == "blobStore" {
		return ringRefusal("", "%s on a blobStore is not available yet: cluster/wire exports no blob %s, so the request cannot be sent. "+
			"Count works: %s.count()", c.verb, c.verb, c.target())
	}
	return ringRefusal("", "%s on a heapStore is not available yet: a CLI session has no heap push or take statement (the Go client method PushHeap can push). "+
		"Peek, list and count work: %s.peek()", c.verb, c.target())
}

// ringStoreVerbLines checks the outcome and renders the "now holds" line.
func (s *RingSession) afterStoreWrite(ctx context.Context, c ringStoreCall, result ringclient.WriteResult, past string) (RingOutput, error) {
	note, err := ringWritten(result, c.database, c.name, c.id)
	if err != nil {
		return RingOutput{}, err
	}
	count, err := c.bucket.StoreCount(ctx, c.id, c.field.Name)
	if err != nil {
		return ringLines(fmt.Sprintf("OK  %s%s", past, note),
			fmt.Sprintf("Note: the new element count could not be read (%s). Run '%s.count()'", ringFailure(err).Message, c.target())), nil
	}
	return ringLines(fmt.Sprintf("OK  %s: %s now holds %s%s", past, c.field.Name, ringElements(count), note)), nil
}

func (s *RingSession) push(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	const args = "{…}"
	if err := c.arity(args, 1, 1); err != nil {
		return RingOutput{}, err
	}
	element, err := c.document(0, args)
	if err != nil {
		return RingOutput{}, err
	}
	result, err := c.bucket.Push(ctx, c.id, c.field.Name, element)
	if err != nil {
		return RingOutput{}, err
	}
	return s.afterStoreWrite(ctx, c, result, "pushed")
}

func (s *RingSession) put(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	const args = `"<key>", {…}`
	if err := c.arity(args, 2, 2); err != nil {
		return RingOutput{}, err
	}
	key, err := c.text(0, args)
	if err != nil {
		return RingOutput{}, err
	}
	element, err := c.document(1, args)
	if err != nil {
		return RingOutput{}, err
	}
	result, err := c.bucket.Put(ctx, c.id, c.field.Name, key, element)
	if err != nil {
		return RingOutput{}, err
	}
	return s.afterStoreWrite(ctx, c, result, fmt.Sprintf("put %q", key))
}

// ringIncArgs is the argument list of inc as a refusal quotes it.
const ringIncArgs = `"<key>", {<path>: <number>, …}[, {return: "after"}]`

// inc runs inc("<key>", {<path>: <number>, …}[, {return: "after"}]). An if: option is refused: a guard on an
// element is not in this release.
func (s *RingSession) inc(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	if err := c.arity(ringIncArgs, 2, 3); err != nil {
		return RingOutput{}, err
	}
	key, err := c.text(0, ringIncArgs)
	if err != nil {
		return RingOutput{}, err
	}
	amounts, err := c.document(1, ringIncArgs)
	if err != nil {
		return RingOutput{}, err
	}
	increments := make(map[string]any, len(amounts))
	for _, amount := range amounts {
		if _, repeated := increments[amount.Key]; repeated {
			return RingOutput{}, c.badArguments(ringIncArgs, fmt.Sprintf("names %q twice and would add only one of them. Give each path once with the total amount", amount.Key))
		}
		increments[amount.Key] = amount.Value
	}
	returnAfter, err := c.incOptions(2)
	if err != nil {
		return RingOutput{}, err
	}
	if !returnAfter {
		result, err := c.bucket.Inc(ctx, c.id, c.field.Name, key, increments)
		if err != nil {
			return RingOutput{}, err
		}
		return s.afterStoreWrite(ctx, c, result, fmt.Sprintf("inc %q", key))
	}
	element, result, err := c.bucket.IncAfter(ctx, c.id, c.field.Name, key, increments)
	if err != nil {
		return RingOutput{}, err
	}
	written, err := s.afterStoreWrite(ctx, c, result, fmt.Sprintf("inc %q", key))
	if err != nil {
		return RingOutput{}, err
	}
	line, err := s.ringJSON(element)
	if err != nil {
		return RingOutput{}, err
	}
	return RingOutput{Lines: append([]string{line}, written.Lines...)}, nil
}

// incOptions reads the {return: "after"} document of an inc at index, and whether it asks for the element back.
func (c ringStoreCall) incOptions(index int) (returnAfter bool, err error) {
	if index >= len(c.values) {
		return false, nil
	}
	options, err := c.document(index, ringIncArgs)
	if err != nil {
		return false, err
	}
	for _, option := range options {
		switch option.Key {
		case "return":
			if text, ok := option.Value.(string); !ok || text != "after" {
				return false, c.badArguments(ringIncArgs, `takes return as "after", the only element an inc can give back`)
			}
			returnAfter = true
		case "if":
			return false, c.badArguments(ringIncArgs, "does not take if: yet, because guards on an element are not in this release. Run the inc without it; to guard a write, use update with if: on a document's scalar field (help update)")
		default:
			return false, c.badArguments(ringIncArgs, fmt.Sprintf(`does not take %q; it takes return: "after"`, option.Key))
		}
	}
	return returnAfter, nil
}

func (s *RingSession) removed(c ringStoreCall, raw bson.Raw, err error, what string) (RingOutput, error) {
	if err != nil {
		if ringClientMiss(err) {
			return RingOutput{}, c.empty(what)
		}
		return RingOutput{}, err
	}
	line, err := s.ringJSON(raw)
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(line), nil
}

// ringMaxPop is the most elements one pop or peek takes: the server's cap of 100.
const ringMaxPop = 100

func (s *RingSession) pop(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	const args = "<n>"
	if err := c.arity(args, 0, 1); err != nil {
		return RingOutput{}, err
	}
	if len(c.values) == 0 {
		raw, err := c.bucket.Pop(ctx, c.id, c.field.Name)
		return s.removed(c, raw, err, "is empty, so there is nothing to pop")
	}
	number := ringCountArg(c.values[0])
	if number < 1 || number > ringMaxPop {
		return RingOutput{}, c.badArguments(args, fmt.Sprintf("takes a number of elements from 1 to %d ({limit: <n>} works too)", ringMaxPop))
	}
	elements, err := c.bucket.PopN(ctx, c.id, c.field.Name, int(number))
	if err != nil {
		return RingOutput{}, err
	}
	if len(elements) == 0 {
		return RingOutput{}, c.empty("is empty, so there is nothing to pop")
	}
	lines, err := s.ringElementLines(elements)
	return RingOutput{Lines: lines}, err
}

// storeGet reads one element of a hashStore, btreeStore or heapStore field by key.
func (s *RingSession) storeGet(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	const args = `"<key>"`
	if err := c.arity(args, 1, 1); err != nil {
		return RingOutput{}, err
	}
	key, err := c.text(0, args)
	if err != nil {
		return RingOutput{}, err
	}
	raw, err := c.bucket.StoreGet(ctx, c.id, c.field.Name, key)
	if ringClientMiss(err) {
		if c.field.Type == "heapStore" {
			return RingOutput{}, &RingError{Code: "NOT_FOUND", Exit: ExitRefused, Message: fmt.Sprintf(
				"%s of %q in %s.%s has no entry id %q. Run '%s.list()' to see what it holds (a CLI session has no heap push statement; the Go client method PushHeap can add one)",
				c.field.Name, c.id, c.database, c.name, key, c.target())}
		}
		return RingOutput{}, &RingError{Code: "NOT_FOUND", Exit: ExitRefused, Message: fmt.Sprintf(
			"%s of %q in %s.%s has no key %q. Run '%s.list()' to see what it holds, or '%s.put(%q, {…})' to add one",
			c.field.Name, c.id, c.database, c.name, key, c.target(), c.target(), key)}
	}
	if err != nil {
		return RingOutput{}, err
	}
	line, err := s.ringJSON(raw)
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(line), nil
}

func (s *RingSession) take(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	const args = `"<key>"`
	if err := c.arity(args, 1, 1); err != nil {
		return RingOutput{}, err
	}
	key, err := c.text(0, args)
	if err != nil {
		return RingOutput{}, err
	}
	raw, err := c.bucket.Take(ctx, c.id, c.field.Name, key)
	return s.removed(c, raw, err, fmt.Sprintf("has no key %q, so there is nothing to take", key))
}

func (s *RingSession) ringElementLines(elements []bson.Raw) ([]string, error) {
	return s.ringPageLines(ringclient.ElementPage{Elements: elements})
}

// ringPageLines renders the elements of a page, each after its key when it has one, as v1.0
// printed a keyed store: the key, two spaces, the element. A heapStore's element follows its
// entry id and its priority, so a person can take or read it by id.
func (s *RingSession) ringPageLines(page ringclient.ElementPage) ([]string, error) {
	lines := make([]string, 0, len(page.Elements))
	for i, element := range page.Elements {
		line, err := s.ringJSON(element)
		if err != nil {
			return nil, err
		}
		if len(page.Keys) == len(page.Elements) {
			prefix := terminalSafe(page.Keys[i]) + "  "
			if len(page.Priorities) == len(page.Elements) {
				prefix += fmt.Sprint(page.Priorities[i]) + "  "
			}
			line = prefix + line
		}
		lines = append(lines, line)
		if s.pretty && i < len(page.Elements)-1 {
			lines = append(lines, "")
		}
	}
	return lines, nil
}

func (s *RingSession) peek(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	count := int64(1)
	if err := c.arity("<n>", 0, 1); err != nil {
		return RingOutput{}, err
	}
	if len(c.values) == 1 {
		number := ringCountArg(c.values[0])
		if number < 1 || number > ringMaxPop {
			return RingOutput{}, c.badArguments("<n>", fmt.Sprintf("takes a number of elements from 1 to %d ({limit: <n>} works too; list pages through more)", ringMaxPop))
		}
		count = number
	}
	page, err := c.bucket.PeekKeyed(ctx, c.id, c.field.Name, int(count))
	if err != nil {
		return RingOutput{}, err
	}
	if len(page.Elements) == 0 {
		return ringLines("(empty)"), nil
	}
	lines, err := s.ringPageLines(page)
	return RingOutput{Lines: lines}, err
}

func (s *RingSession) listStore(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	const args = "{limit: <n>, cursor: \"<hex>\"}"
	if err := c.arity(args, 0, 1); err != nil {
		return RingOutput{}, err
	}
	var limit int
	var after []byte
	hasLimit := false
	var expand *ringclient.ExpandRequest
	local := false
	if len(c.values) == 1 {
		doc, err := c.document(0, args)
		if err != nil {
			return RingOutput{}, err
		}
		usage := c.target() + `.list({expand: true})`
		for _, entry := range c.values[0].Entries {
			switch entry.Key {
			case "local":
				value, err := ringBoolOption("local", entry.Value, usage)
				if err != nil {
					return RingOutput{}, err
				}
				local = value
			case "expand":
				request, err := ringExpandOne("", entry.Value, usage)
				if err != nil {
					return RingOutput{}, err
				}
				expand = &request
			}
		}
		var rest bson.D
		for _, entry := range doc {
			if entry.Key != "expand" && entry.Key != "local" {
				rest = append(rest, entry)
			}
		}
		doc = rest
		for _, entry := range doc {
			switch value := entry.Value.(type) {
			case int64:
				if entry.Key != "limit" || value < 0 || value > 1000 {
					return RingOutput{}, c.badArguments(args, "takes limit from 0 to 1000 and a cursor")
				}
				limit, hasLimit = int(value), true
			case string:
				cursor, err := ringDecodeCursor(value)
				if entry.Key != "cursor" || err != nil {
					return RingOutput{}, c.badArguments(args, "takes limit and cursor, and the cursor is the hex the last page printed")
				}
				after = cursor
			default:
				return RingOutput{}, c.badArguments(args, "takes limit and cursor")
			}
		}
	}
	var page ringclient.ElementPage
	var err error
	if expand != nil || local {
		reader, ok := c.bucket.(ringExpandReader)
		if !ok {
			return RingOutput{}, ringRefusal("INVALID_ARGUMENT", "this connection cannot expand or read locally. Run '%s.list()' to read the elements alone", c.target())
		}
		page, err = reader.StoreListExpanded(ctx, c.id, c.field.Name, limit, after, expand, local)
	} else {
		page, err = c.bucket.StoreList(ctx, c.id, c.field.Name, limit, after)
	}
	if err != nil {
		return RingOutput{}, err
	}
	if len(page.Elements) == 0 && !page.More {
		return ringLines("(empty)"), nil
	}
	lines, err := s.ringPageLines(page)
	if err != nil {
		return RingOutput{}, err
	}
	if len(page.Expanded) == len(page.Elements) && len(page.Expanded) > 0 {
		lines, err = s.ringExpandedPageLines(page)
		if err != nil {
			return RingOutput{}, err
		}
	}
	lines = append(lines, ringServedLine(page.Served)...)
	if page.More {
		parts := []string{}
		if hasLimit {
			parts = append(parts, fmt.Sprintf("limit: %d", limit))
		}
		if expand != nil {
			parts = append(parts, "expand: "+ringExpandOneText(*expand))
		}
		if local {
			parts = append(parts, "local: true")
		}
		parts = append(parts, fmt.Sprintf("cursor: %q", hex.EncodeToString(page.Cursor)))
		lines = append(lines, fmt.Sprintf("more: %s.list({%s})", c.target(), strings.Join(parts, ", ")))
	}
	return RingOutput{Lines: lines}, nil
}

func (s *RingSession) countStore(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	if len(c.values) > 0 {
		return s.aggregateStore(ctx, c)
	}
	count, err := c.bucket.StoreCount(ctx, c.id, c.field.Name)
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(fmt.Sprint(count)), nil
}

func (s *RingSession) leaseVerb(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	if c.verb == "claim" {
		return s.claim(ctx, c)
	}
	const args = `"<lease>"`
	least, most, shown := 1, 1, args
	if c.verb == "extend" {
		least, most, shown = 2, 2, args+", <ms>"
	}
	if err := c.arity(shown, least, most); err != nil {
		return RingOutput{}, err
	}
	lease, err := c.lease(0, shown)
	if err != nil {
		return RingOutput{}, err
	}
	text := hex.EncodeToString(lease[:])
	switch c.verb {
	case "get":
		read, err := c.bucket.LeaseGet(ctx, c.id, c.field.Name, lease)
		if err != nil {
			return RingOutput{}, err
		}
		if !read.Found {
			return ringLines(fmt.Sprintf("lease %s is not live (it was acked, released or has expired). Run '%s.claim(1)' to lease again", text, c.target())), nil
		}
		lines, err := s.ringElementLines(read.Elements)
		return RingOutput{Lines: lines}, err
	case "ack":
		change, err := c.bucket.Ack(ctx, c.id, c.field.Name, lease)
		if err != nil {
			return RingOutput{}, err
		}
		note, err := ringWritten(change.Write, c.database, c.name, c.id)
		if err != nil {
			return RingOutput{}, err
		}
		return ringLines("OK  acked lease " + text + note), nil
	case "release":
		change, err := c.bucket.Release(ctx, c.id, c.field.Name, lease)
		if err != nil {
			return RingOutput{}, err
		}
		if !change.Live {
			return ringLines(fmt.Sprintf("lease %s is not live (it was acked, released or has expired), so nothing was released", text)), nil
		}
		return ringLines("OK  released lease " + text), nil
	}
	if c.values[1].Kind != ValueInt {
		return RingOutput{}, c.badArguments(shown, "takes the new deadline in milliseconds, from 1000 to 3600000")
	}
	change, err := c.bucket.Extend(ctx, c.id, c.field.Name, lease, c.values[1].Int)
	if err != nil {
		return RingOutput{}, err
	}
	if !change.Live {
		return ringLines(fmt.Sprintf("lease %s is not live (it was acked, released or has expired), so it was not extended", text)), nil
	}
	return ringLines(fmt.Sprintf("OK  extended lease %s to %d ms", text, c.values[1].Int)), nil
}

func (s *RingSession) claim(ctx context.Context, c ringStoreCall) (RingOutput, error) {
	count := int64(1)
	if err := c.arity("<n>", 0, 1); err != nil {
		return RingOutput{}, err
	}
	if len(c.values) == 1 {
		number := ringCountArg(c.values[0])
		if number < 1 {
			return RingOutput{}, c.badArguments("<n>", "takes a number of messages, 1 or more ({limit: <n>} works too)")
		}
		count = number
	}
	lease, err := c.bucket.Claim(ctx, c.id, c.field.Name, int(count))
	if err != nil {
		return RingOutput{}, err
	}
	if len(lease.Elements) == 0 {
		return ringLines(fmt.Sprintf("(nothing to claim: %s is empty)", c.field.Name)), nil
	}
	lines, err := s.ringElementLines(lease.Elements)
	if err != nil {
		return RingOutput{}, err
	}
	return RingOutput{Lines: append([]string{"lease " + hex.EncodeToString(lease.ID[:])}, lines...)}, nil
}

// ringCountArg reads a count given as <n> or as v1.0's {limit: <n>}; anything
// else is 0, which the caller refuses.
func ringCountArg(v Value) int64 {
	if v.Kind == ValueInt {
		return v.Int
	}
	if v.Kind == ValueDoc && len(v.Entries) == 1 && v.Entries[0].Key == "limit" && v.Entries[0].Value.Kind == ValueInt {
		return v.Entries[0].Value.Int
	}
	return 0
}

// ringExpandedPageLines prints a store page whose elements were expanded: each element, then the
// document its key names, or that it was not found.
func (s *RingSession) ringExpandedPageLines(page ringclient.ElementPage) ([]string, error) {
	var lines []string
	for i := range page.Elements {
		single := ringclient.ElementPage{Elements: page.Elements[i : i+1]}
		if len(page.Keys) == len(page.Elements) {
			single.Keys = page.Keys[i : i+1]
		}
		row, err := s.ringPageLines(single)
		if err != nil {
			return nil, err
		}
		lines = append(lines, row...)
		far, err := s.ringExpansionLines(page.Expanded[i])
		if err != nil {
			return nil, err
		}
		lines = append(lines, far...)
	}
	return lines, nil
}
