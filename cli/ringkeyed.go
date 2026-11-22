package cli

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringElements counts in words: "1 element", "2 elements".
func ringElements(n int64) string {
	if n == 1 {
		return "1 element"
	}
	return fmt.Sprintf("%d elements", n)
}

// ringJSON renders a document in the session's flavour: the printer v1.0
// uses (keys sorted, dates as text unless extjson is on, indented when
// pretty is on).
func (s *RingSession) ringJSON(raw bson.Raw) (string, error) { return s.ringText("", raw) }

// ringText is ringJSON with the document's id put in when the body lacks one.
func (s *RingSession) ringText(id string, raw bson.Raw) (string, error) {
	fields, failed := decodedDocument(raw)
	if fields == nil && (failed == "" || failed == "{}") {
		fields = map[string]interface{}{}
	} else if failed != "" {
		return "", fmt.Errorf("the server sent a document that is not valid BSON: %s", failed)
	}
	if _, has := fields["_id_"]; !has && id != "" {
		fields["_id_"] = id
	}
	return formatDocument(fields, s.extJSON, s.pretty, nil), nil
}

// ringStoreType reports whether a declared type is one of the six stores.
func ringStoreType(word string) bool { return strings.HasSuffix(word, "Store") }

// ringDocument renders one document as the server sent it: the scalar fields,
// then each store field as "<type> (<n> elements)", which the owner writes into
// the body so the CLI needs no count request per store. A body without _id_
// gets it added.
func (s *RingSession) ringDocument(id string, raw bson.Raw) (string, error) {
	return s.ringText(id, raw)
}

// ringClientMiss reports a single-value read that found nothing, as the client
// raises it itself. A server NOT_FOUND (a missing bucket) is a *ringclient.Error
// with its own text and is not a miss in this sense.
func ringClientMiss(err error) bool {
	var refused *ringclient.Error
	return errors.Is(err, ringclient.ErrNotFound) && !errors.As(err, &refused)
}

// ringMissingDocument is conventions Errors' NOT_FOUND (document) text.
func ringMissingDocument(database, bucket, id string) *RingError {
	return &RingError{Code: "NOT_FOUND", Exit: ExitRefused, Message: fmt.Sprintf(
		"document %q is not in %s.%s. Run 'db.%s.list({limit: 10})' to see what exists, or 'db.%s.insert({_id_: %q, <field>: <value>})' to create it",
		id, database, bucket, bucket, bucket, id)}
}

// ringExpiredDocument is the ordinary miss followed by the trailing line a developer greps for: the
// document is there on disk but past its 'expire at' time, which reads as absent until the node
// removes it (expiry spec 3.2).
func ringExpiredDocument(database, bucket, id string) *RingError {
	miss := ringMissingDocument(database, bucket, id)
	miss.Message += "\nexpired: true"
	return miss
}

// ringWritten checks a write's outcome. applied prints plain; applied_degraded
// prints a note; anything else may or may not have applied, which is ExitUnavailable.
func ringWritten(written ringclient.WriteResult, database, bucket, id string) (note string, err error) {
	switch written.Outcome {
	case "applied":
		return "", nil
	case "applied_degraded":
		return fmt.Sprintf(" (applied_degraded: %d copies confirmed, fewer than the replication factor; the others catch up in the background)", written.Copies), nil
	}
	return "", &RingError{Exit: ExitUnavailable, Message: fmt.Sprintf(
		"the write was not acknowledged (outcome %q), so it may or may not have applied. Run 'db.%s[%q].get()' in %s to see whether it did",
		written.Outcome, bucket, id, database)}
}

func (s *RingSession) keyed(ctx context.Context, st Statement) (RingOutput, error) {
	database, name := ringText(st, "database"), ringText(st, "bucket")
	bucket := s.api.Bucket(database, name)
	switch st.Verb {
	case "get":
		return s.getDocument(ctx, bucket, st, database, name)
	case "insert", "upsert":
		return s.writeDocument(ctx, bucket, st, database, name)
	case "update":
		return s.updateDocument(ctx, bucket, st, database, name)
	case "delete":
		return s.deleteDocument(ctx, bucket, st, database, name)
	case "list":
		return s.listDocuments(ctx, bucket, st, database, name)
	case "count":
		return s.countDocuments(ctx, bucket, st)
	}
	return s.storeVerb(ctx, bucket, st, database, name)
}

func (s *RingSession) getDocument(ctx context.Context, bucket RingBucket, st Statement, database, name string) (RingOutput, error) {
	id := ringText(st, "id")
	fields, err := ringGetFields(st, name)
	if err != nil {
		return RingOutput{}, err
	}
	options, err := ringGetExpand(st, name)
	if err != nil {
		return RingOutput{}, err
	}
	if len(options.expand) > 0 || options.local {
		return s.getExpanded(ctx, bucket, id, fields, options, database, name)
	}
	var raw bson.Raw
	if fields == nil {
		raw, err = bucket.Get(ctx, id)
	} else {
		raw, err = bucket.GetFields(ctx, id, fields)
	}
	if ringClientMiss(err) {
		return RingOutput{}, ringMissingDocument(database, name, id)
	}
	if ringclient.IsExpired(err) {
		return RingOutput{}, ringExpiredDocument(database, name, id)
	}
	if err != nil {
		return RingOutput{}, err
	}
	line, err := s.ringDocument(id, raw)
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(line), nil
}

// ringDocumentArg splits a document literal into its _id_ and the other fields.
func ringDocumentArg(st Statement, name string) (id string, hasID bool, fields ringclient.Doc, err error) {
	doc, _ := st.Arg("doc")
	fields = ringclient.Doc{}
	for _, entry := range doc.Doc.Entries {
		if entry.Key != "_id_" {
			fields[entry.Key] = ringBSON(entry.Value)
			continue
		}
		if id, err = ringEntryID(entry, st, name); err != nil {
			return "", false, nil, err
		}
		hasID = true
	}
	return id, hasID, fields, nil
}

// ringEntryID reads one _id_ entry, which is a string in this release.
func ringEntryID(entry DocEntry, st Statement, name string) (string, error) {
	if entry.Value.Kind != ValueString {
		return "", ringRefusal("PARSE_ERROR", "_id_ is %s, and an _id_ is a string in this release. Write it in double quotes: db.%s.%s({_id_: %q, …})",
			ringRenderValue(entry.Value), name, st.Verb, ringRenderValue(entry.Value))
	}
	return entry.Value.Text, nil
}

// ringRefuseOptions refuses if and return by name on an insert or upsert, whatever their
// value and even when the bucket declares a field of that name: they are options of update
// and delete, and sending them as data would store a wrong document and report success. A
// field with such a name is written through update's $set.
func ringRefuseOptions(st Statement, name string) error {
	doc, ok := st.Arg("doc")
	if !ok {
		return nil
	}
	for _, entry := range doc.Doc.Entries {
		switch {
		case entry.Key == "if" && st.Verb == "upsert":
			return ringRefusal("PARSE_ERROR", "upsert takes no 'if': an upsert has no document to guard. Use db.%s.insert, which already refuses a duplicate, or update with 'if' on a document that exists. A field named if is written with update's $set", name)
		case entry.Key == "if":
			return ringRefusal("PARSE_ERROR", "insert takes no 'if': an insert has no existing document to guard. Use db.%s.update with 'if' to change a document only while it holds the values you expect. A field named if is written with update's $set", name)
		case entry.Key == "return":
			return ringRefusal("PARSE_ERROR", "an %s takes no 'return': an %s answers its id. Use db.%s.update with return to get a document back. A field named return is written with update's $set", st.Verb, st.Verb, name)
		}
	}
	return nil
}

func (s *RingSession) writeDocument(ctx context.Context, bucket RingBucket, st Statement, database, name string) (RingOutput, error) {
	if err := ringRefuseOptions(st, name); err != nil {
		return RingOutput{}, err
	}
	id, hasID, fields, err := ringDocumentArg(st, name)
	if err != nil {
		return RingOutput{}, err
	}
	if !hasID {
		if st.Verb == "upsert" {
			return RingOutput{}, ringRefusal("PARSE_ERROR", "upsert needs the _id_ of the document to replace: db.%s.upsert({_id_: \"<id>\", …})", name)
		}
		if id, err = s.mintID(); err != nil {
			return RingOutput{}, err
		}
	}
	write := bucket.Insert
	if st.Verb == "upsert" {
		write = bucket.Upsert
	}
	written, err := write(ctx, id, fields)
	if err != nil {
		return RingOutput{}, err
	}
	note, err := ringWritten(written, database, name, id)
	if err != nil {
		return RingOutput{}, err
	}
	if !hasID {
		note += " (no _id_ was given, so one was made)"
	}
	return ringLines("OK  id=" + id + note), nil
}

// ringUpdateOperators are the update operators of db.<bucket>.update, after MongoDB's.
const ringUpdateOperators = "$set, $unset, $inc, $mul, $min, $max, $push, $addToSet, $rename"

// ringEntries is the document of a statement's operator argument, keys in the order written.
func ringEntries(entries []DocEntry) bson.D {
	doc := make(bson.D, len(entries))
	for i, entry := range entries {
		doc[i] = bson.E{Key: entry.Key, Value: ringBSON(entry.Value)}
	}
	return doc
}

func (s *RingSession) updateDocument(ctx context.Context, bucket RingBucket, st Statement, database, name string) (RingOutput, error) {
	usage := fmt.Sprintf("db.%s.update({_id_: \"<id>\", $set: {<field>: <value>}})", name)
	doc, _ := st.Arg("doc")
	var id string
	var change ringclient.Update
	hasID, changes := false, 0
	seen := map[string]bool{}
	for _, entry := range doc.Doc.Entries {
		if entry.Key == "if" || entry.Key == "return" {
			continue
		}
		if entry.Key == "_id_" && entry.Value.Kind == ValueString {
			id, hasID = entry.Value.Text, true
			continue
		}
		if entry.Value.Kind != ValueDoc || !strings.Contains(", "+ringUpdateOperators+",", ", "+entry.Key+",") {
			return RingOutput{}, ringRefusal("PARSE_ERROR", "update takes _id_ and the operators %s, and %q is none of them (or has the wrong type). Run '%s'", ringUpdateOperators, entry.Key, usage)
		}
		if seen[entry.Key] {
			return RingOutput{}, ringRefusal("PARSE_ERROR", "update names %s twice. Put every field of one operator in one document: %s: {<field>: <value>, …}", entry.Key, entry.Key)
		}
		seen[entry.Key] = true
		fields := ringEntries(entry.Value.Entries)
		if len(fields) == 0 {
			return RingOutput{}, ringRefusal("PARSE_ERROR", "%s names no field. Run '%s'", entry.Key, usage)
		}
		changes++
		switch entry.Key {
		case "$set":
			for _, field := range fields {
				if field.Key == "_id_" {
					return RingOutput{}, ringRefusal("PARSE_ERROR", "$set cannot change _id_: a document's id is its address. Delete it and insert the new id with 'db.%s.insert({_id_: \"<id>\", …})'", name)
				}
			}
			change.Set = fields
		case "$unset":
			for _, field := range fields {
				change.Unset = append(change.Unset, field.Key)
			}
		case "$inc":
			change.Inc = fields
		case "$mul":
			change.Mul = fields
		case "$min":
			change.Min = fields
		case "$max":
			change.Max = fields
		case "$push":
			change.Push = fields
		case "$addToSet":
			change.AddToSet = fields
		case "$rename":
			change.Rename = fields
		}
	}
	if !hasID || changes == 0 {
		return RingOutput{}, ringRefusal("PARSE_ERROR", "update needs the document's _id_ and an operator such as $set (all: %s). Run '%s'", ringUpdateOperators, usage)
	}
	// One owner write: all the operators apply together or none does, and another writer
	// between a read and a write cannot be overwritten (SPEC-GAP U52-3 is closed).
	options, _, guard, err := ringGuarded(st, name)
	if err != nil {
		return RingOutput{}, err
	}
	written, err := bucket.Update(ctx, id, change, options...)
	if err != nil {
		return RingOutput{}, err
	}
	return s.ringWriteResult(written, database, name, id, "updated", guard)
}

// ringGuarded reads the optional if and return keys of an update or delete: the call
// options they become, the return mode, and the guard entries the statement named.
func ringGuarded(st Statement, name string) ([]ringclient.CallOption, ringclient.ReturnMode, []DocEntry, error) {
	usage := fmt.Sprintf("db.%s.%s({_id_: \"<id>\", if: {Status: \"processing\"}, return: \"after\", <operator>: {<field>: <value>}})", name, st.Verb)
	doc, _ := st.Arg("doc")
	var options []ringclient.CallOption
	var guard []DocEntry
	var mode ringclient.ReturnMode
	seen := map[string]bool{}
	for _, entry := range doc.Doc.Entries {
		switch entry.Key {
		case "if":
			if seen["if"] {
				return nil, "", nil, ringRefusal("PARSE_ERROR", "%s names 'if' twice. Put every equality of one guard in one document: if: {Status: \"processing\", …}. Run '%s'", st.Verb, usage)
			}
			seen["if"] = true
			if entry.Value.Kind != ValueDoc || len(entry.Value.Entries) == 0 {
				return nil, "", nil, ringRefusal("PARSE_ERROR", "'if' takes equalities only, such as if: {Status: \"processing\"}. Run '%s'", usage)
			}
			for _, equality := range entry.Value.Entries {
				if equality.Value.Kind == ValueDoc && len(equality.Value.Entries) > 0 && strings.HasPrefix(equality.Value.Entries[0].Key, "$") {
					return nil, "", nil, ringRefusal("PARSE_ERROR", "'if' takes equalities only, such as if: {Status: \"processing\"}. A range guard is not available yet; guard on null and set a deadline instead")
				}
			}
			guard = entry.Value.Entries
			options = append(options, ringclient.If(ringclient.Guard(ringEntries(guard))))
		case "return":
			if seen["return"] {
				return nil, "", nil, ringRefusal("PARSE_ERROR", "%s names 'return' twice. Name \"after\" or \"before\" once. Run '%s'", st.Verb, usage)
			}
			seen["return"] = true
			if entry.Value.Kind != ValueString || (entry.Value.Text != string(ringclient.ReturnAfter) && entry.Value.Text != string(ringclient.ReturnBefore)) {
				return nil, "", nil, ringRefusal("PARSE_ERROR", "return is \"after\" or \"before\", and %s is neither. Write return: \"after\" to get the document as the write left it, or return: \"before\" for it as it was", ringRenderValue(entry.Value))
			}
			mode = ringclient.ReturnMode(entry.Value.Text)
			options = append(options, ringclient.Return(mode))
		}
	}
	return options, mode, guard, nil
}

// ringWriteResult prints one update or delete: the OK line, with the document when the
// statement asked for it, or the block a guard that refused the write produces.
func (s *RingSession) ringWriteResult(written ringclient.WriteResult, database, name, id, done string, guard []DocEntry) (RingOutput, error) {
	if written.NotApplied {
		lines := []string{fmt.Sprintf("NOT APPLIED  %s %q: %s is %s, not %s. Nothing was changed.",
			name, id, written.GuardField, s.ringGuardSeen(written.Document, written.GuardField), ringGuardWanted(guard, written.GuardField))}
		if len(written.Document) > 0 {
			document, err := s.ringDocument(id, written.Document)
			if err != nil {
				return RingOutput{}, err
			}
			lines = append(lines, document)
		}
		lines = append(lines, fmt.Sprintf("Re-read with db.%s[%q].get(), then retry with the value you see.", name, id))
		return RingOutput{}, &RingError{Exit: ExitRefused, Message: strings.Join(lines, "\n")}
	}
	note, err := ringWritten(written, database, name, id)
	if err != nil {
		return RingOutput{}, err
	}
	line := "OK  " + done + " id=" + id + note
	if written.DocumentTruncated {
		return ringLines(line + fmt.Sprintf(" (already applied; the document is too large to repeat here, run db.%s[%q].get())", name, id)), nil
	}
	if len(written.Document) == 0 {
		return ringLines(line), nil
	}
	document, err := s.ringDocument(id, written.Document)
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(line, document), nil
}

// ringGuardSeen renders the guarded field as the current document holds it, in the text
// a get prints it, or "unset" when the document does not carry the field.
func (s *RingSession) ringGuardSeen(document bson.Raw, field string) string {
	fields, failed := decodedDocument(document)
	if failed != "" || fields == nil {
		return "unset"
	}
	current, has := fields[field]
	if !has {
		return "unset"
	}
	text, err := json.Marshal(printableValue(current))
	if err != nil {
		return fmt.Sprintf("%q", fmt.Sprintf("%v", current))
	}
	return jsonEscapeControls(string(text))
}

// ringGuardWanted renders the value the statement guarded on.
func ringGuardWanted(guard []DocEntry, field string) string {
	for _, entry := range guard {
		if entry.Key == field {
			return ringRenderValue(entry.Value)
		}
	}
	return "the value you asked for"
}

func (s *RingSession) deleteDocument(ctx context.Context, bucket RingBucket, st Statement, database, name string) (RingOutput, error) {
	usage := fmt.Sprintf("db.%s.delete({_id_: \"<id>\"})", name)
	doc, _ := st.Arg("doc")
	var id string
	hasID := false
	for _, entry := range doc.Doc.Entries {
		switch entry.Key {
		case "_id_":
			parsed, err := ringEntryID(entry, st, name)
			if err != nil {
				return RingOutput{}, err
			}
			id, hasID = parsed, true
		case "if", "return":
			// Guard and return are read by ringGuarded below.
		default:
			return RingOutput{}, ringRefusal("PARSE_ERROR", "delete takes _id_, if and return, and %q is none of them. Run '%s'", entry.Key, usage)
		}
	}
	if !hasID {
		return RingOutput{}, ringRefusal("PARSE_ERROR", "delete needs the _id_ of the document: db.%s.delete({_id_: \"<id>\"})", name)
	}
	options, mode, guard, err := ringGuarded(st, name)
	if err != nil {
		return RingOutput{}, err
	}
	if mode == ringclient.ReturnAfter {
		return RingOutput{}, ringRefusal("INVALID_ARGUMENT", "a delete has no after; use return: \"before\" to get the document back")
	}
	written, err := bucket.Delete(ctx, id, options...)
	if err != nil {
		return RingOutput{}, err
	}
	return s.ringWriteResult(written, database, name, id, "deleted", guard)
}

// ringQuery is the {filter, fields, limit, cursor} document of list and count.
type ringQuery struct {
	filter *Value
	// fields are the scalar fields a list returns besides _id_; nil returns every field.
	fields []string
	limit  int32
	hasLim bool
	cursor []byte
	hex    string
	// expand, where and local are fetch-through's options of a bucket list.
	expand []ringclient.ExpandRequest
	where  *Value
	local  bool
}

// ringFieldNames reads the fields argument: a list of field names in double quotes, at
// least one and none twice. usage is the statement the refusals tell the developer to run.
func ringFieldNames(value Value, usage string) ([]string, error) {
	if value.Kind != ValueArray {
		return nil, ringRefusal("PARSE_ERROR", `fields is a list of field names in double quotes, such as ["Name", "Bio"]. Run '%s'`, usage)
	}
	if len(value.Elems) == 0 {
		return nil, ringRefusal("PARSE_ERROR", `fields is empty. Name at least one field, such as ["Name"], or leave fields out to get every field. Run '%s'`, usage)
	}
	names := make([]string, 0, len(value.Elems))
	for _, element := range value.Elems {
		if element.Kind != ValueString {
			return nil, ringRefusal("PARSE_ERROR", `fields holds %s, and a field name is in double quotes, such as ["Name"]. Run '%s'`, ringRenderValue(element), usage)
		}
		if ringHasWord(names, element.Text) {
			return nil, ringRefusal("PARSE_ERROR", `fields names %q twice. Name each field once. Run '%s'`, element.Text, usage)
		}
		names = append(names, element.Text)
	}
	return names, nil
}

// ringGetFields reads the optional {fields: [...]} argument of get, or nil for every field.
func ringGetFields(st Statement, name string) ([]string, error) {
	doc, ok := st.Arg("doc")
	if !ok {
		return nil, nil
	}
	usage := fmt.Sprintf(`db.%s["<id>"].get({fields: ["Name", "Bio"]})`, name)
	var fields []string
	seen := false
	for _, entry := range doc.Doc.Entries {
		if entry.Key == "expand" || entry.Key == "local" {
			continue
		}
		if entry.Key != "fields" {
			return nil, ringRefusal("PARSE_ERROR", "get takes fields, expand and local, and %q is not one. Run '%s'", entry.Key, usage)
		}
		if seen {
			return nil, ringRefusal("PARSE_ERROR", "get takes one fields argument, and fields is given twice. Put every name in one array. Run '%s'", usage)
		}
		seen = true
		names, err := ringFieldNames(entry.Value, usage)
		if err != nil {
			return nil, err
		}
		fields = names
	}
	return fields, nil
}

func ringQueryArg(st Statement, name string, allowed ...string) (ringQuery, error) {
	var query ringQuery
	doc, ok := st.Arg("doc")
	if !ok {
		return query, nil
	}
	usage := fmt.Sprintf("db.%s.%s({%s})", name, st.Verb, strings.Join(allowed, ": …, ")+": …")
	for _, entry := range doc.Doc.Entries {
		if !ringHasWord(allowed, entry.Key) {
			return query, ringRefusal("PARSE_ERROR", "%s takes %s, and %q is not one. Run '%s'", st.Verb, strings.Join(allowed, ", "), entry.Key, usage)
		}
		switch entry.Key {
		case "filter":
			if entry.Value.Kind != ValueDoc {
				return query, ringRefusal("PARSE_ERROR", "filter is an equality document such as {Name: \"Ada\"}. Run '%s'", usage)
			}
			filterDoc := entry.Value
			query.filter = &filterDoc
		case "fields":
			names, err := ringFieldNames(entry.Value, fmt.Sprintf(`db.%s.list({fields: ["Name", "Bio"], limit: 10})`, name))
			if err != nil {
				return query, err
			}
			query.fields = names
		case "expand":
			requests, err := ringExpandFields(entry.Value, fmt.Sprintf(`db.%s.list({expand: {Author: true}, limit: 10})`, name))
			if err != nil {
				return query, err
			}
			query.expand = requests
		case "where":
			whereDoc := entry.Value
			query.where = &whereDoc
		case "local":
			local, err := ringBoolOption("local", entry.Value, fmt.Sprintf(`db.%s.list({expand: {Author: true}, local: true})`, name))
			if err != nil {
				return query, err
			}
			query.local = local
		case "limit":
			if entry.Value.Kind != ValueInt || entry.Value.Int < 0 || entry.Value.Int > 1<<30 {
				return query, ringRefusal("PARSE_ERROR", "limit is a whole number of documents, 0 for the default. Run 'db.%s.list({limit: 10})'", name)
			}
			query.limit, query.hasLim = int32(entry.Value.Int), true
		case "cursor":
			if entry.Value.Kind != ValueString {
				return query, ringRefusal("PARSE_ERROR", "cursor is the hex string the last page printed, in double quotes. Run 'db.%s.list({cursor: \"<hex>\"})'", name)
			}
			cursor, err := ringDecodeCursor(entry.Value.Text)
			if err != nil {
				return query, err
			}
			query.cursor, query.hex = cursor, entry.Value.Text
		}
	}
	return query, nil
}

func ringHasWord(words []string, word string) bool {
	for _, candidate := range words {
		if candidate == word {
			return true
		}
	}
	return false
}

func (q ringQuery) filterValue() any {
	if q.filter == nil {
		return nil
	}
	return ringBSON(*q.filter)
}

func (s *RingSession) listDocuments(ctx context.Context, bucket RingBucket, st Statement, database, name string) (RingOutput, error) {
	query, err := ringQueryArg(st, name, "filter", "fields", "limit", "cursor", "expand", "where", "local")
	if err != nil {
		return RingOutput{}, err
	}
	if err := ringWhere(query.expand, query.where, fmt.Sprintf(`db.%s.list({expand: {Author: true}, where: {Name: "Ada"}})`, name)); err != nil {
		return RingOutput{}, err
	}
	if query.where != nil && len(query.expand) == 0 {
		return RingOutput{}, ringRefusal("PARSE_ERROR", `where filters the expanded documents, so it needs expand. Run 'db.%s.list({expand: {Author: true}, where: {Name: "Ada"}})', or use filter for the listed documents`, name)
	}
	page, err := bucket.List(ctx, query.filterValue(), ringclient.PageRequest{Limit: query.limit, Cursor: query.cursor, Fields: query.fields, Expand: query.expand, Local: query.local})
	if err != nil {
		return RingOutput{}, err
	}
	if len(page.Entries) == 0 && !page.More {
		return ringLines("(no documents)"), nil
	}
	var out RingOutput
	for i, entry := range page.Entries {
		line, err := s.ringDocument(entry.ID, entry.Body)
		if err != nil {
			return RingOutput{}, err
		}
		out.Lines = append(out.Lines, line)
		if i < len(page.Expanded) {
			lines, err := s.ringExpansionLines(page.Expanded[i])
			if err != nil {
				return RingOutput{}, err
			}
			out.Lines = append(out.Lines, lines...)
		}
		if s.pretty && i < len(page.Entries)-1 {
			out.Lines = append(out.Lines, "")
		}
	}
	if page.ExpandFiltered > 0 {
		out.Lines = append(out.Lines, fmt.Sprintf("where removed %d rows from this page (it is not an index: the page was read first)", page.ExpandFiltered))
	}
	out.Lines = append(out.Lines, ringServedLine(page.Served)...)
	if page.More {
		var parts []string
		if query.filter != nil {
			parts = append(parts, "filter: "+ringRenderValue(*query.filter))
		}
		if query.fields != nil {
			quoted := make([]string, len(query.fields))
			for i, field := range query.fields {
				quoted[i] = strconv.Quote(field)
			}
			parts = append(parts, "fields: ["+strings.Join(quoted, ", ")+"]")
		}
		if query.hasLim {
			parts = append(parts, fmt.Sprintf("limit: %d", query.limit))
		}
		if len(query.expand) > 0 {
			parts = append(parts, "expand: "+ringExpandText(query.expand))
		}
		if query.local {
			parts = append(parts, "local: true")
		}
		parts = append(parts, fmt.Sprintf("cursor: %q", hex.EncodeToString(page.Cursor)))
		out.Lines = append(out.Lines, fmt.Sprintf("more: db.%s.list({%s})", name, strings.Join(parts, ", ")))
	}
	return out, nil
}

func (s *RingSession) countDocuments(ctx context.Context, bucket RingBucket, st Statement) (RingOutput, error) {
	name := ringText(st, "bucket")
	query, err := ringQueryArg(st, name, "filter")
	if err != nil {
		return RingOutput{}, err
	}
	count, err := bucket.Count(ctx, query.filterValue())
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(fmt.Sprint(count)), nil
}

// ringGetExpand reads the expand and local members of get.
func ringGetExpand(st Statement, name string) (ringExpandOptions, error) {
	var options ringExpandOptions
	doc, ok := st.Arg("doc")
	if !ok {
		return options, nil
	}
	usage := fmt.Sprintf(`db.%s["<id>"].get({expand: {Author: true}})`, name)
	for _, entry := range doc.Doc.Entries {
		switch entry.Key {
		case "expand":
			requests, err := ringExpandFields(entry.Value, usage)
			if err != nil {
				return options, err
			}
			options.expand = requests
		case "local":
			local, err := ringBoolOption("local", entry.Value, usage)
			if err != nil {
				return options, err
			}
			options.local = local
		}
	}
	return options, nil
}

func (s *RingSession) getExpanded(ctx context.Context, bucket RingBucket, id string, fields []string, options ringExpandOptions, database, name string) (RingOutput, error) {
	reader, ok := bucket.(ringExpandReader)
	if !ok {
		return RingOutput{}, ringRefusal("INVALID_ARGUMENT", "this connection cannot expand or read locally. Run 'db.%s[%q].get()' to read the document alone", name, id)
	}
	raw, expanded, err := reader.GetExpanded(ctx, id, fields, options.expand, options.local)
	if ringClientMiss(err) {
		return RingOutput{}, ringMissingDocument(database, name, id)
	}
	if err != nil {
		return RingOutput{}, err
	}
	line, err := s.ringDocument(id, raw)
	if err != nil {
		return RingOutput{}, err
	}
	out := RingOutput{Lines: []string{line}}
	if expanded != nil {
		lines, err := s.ringExpansionLines(expanded)
		if err != nil {
			return RingOutput{}, err
		}
		out.Lines = append(out.Lines, lines...)
	}
	return out, nil
}
