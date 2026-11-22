package cli

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The declaration grammar of create and alter bucket bodies (nested buckets,
// GAP-22; spec §10.4, S-9), and the per-document field-store address form
// (S-6): db.Post["p1"].Comments.push({...}).

// bucketDeclaration is a create or alter body, read entry by entry.
type bucketDeclaration struct {
	// options are the key: value entries — type, indices, replication_factor.
	options map[string]interface{}
	// stores are the store field declarations, L3 ones nested.
	stores []server.StoreDeclaration
	// scalars are the scalar field declarations as written ("Caption
	// string"): accepted and not enforced (S-9 (i)).
	scalars []string
	// storeScalars are the scalar declarations written inside a store
	// field's braces, qualified with the store field they sit in
	// ("Comments.Author string"). They are noted, not enforced, like the
	// top-level ones (S-9 (i); BUG-0283).
	storeScalars []string
	// uniques are the fields declared "<Field> <type> unique": the one
	// scalar keyword that is enforced, as a unique index (S-15).
	uniques []string
	// removals are the entries shaped like removing a field.
	removals []string
}

// scalarFieldTypes are the words a scalar declaration may name. They are
// accepted as documentation and enforced by nothing: the server keeps no
// document schema.
var scalarFieldTypes = map[string]bool{
	"string": true, "uuid": true, "int": true, "integer": true, "int32": true, "int64": true,
	"number": true, "float": true, "double": true, "decimal": true, "bool": true, "boolean": true,
	"date": true, "datetime": true, "timestamp": true, "binary": true, "object": true, "array": true, "any": true,
}

// parseDeclarationBody reads a create or alter body { … }: a comma-separated
// list whose entries are options (key: value), store declarations
// (Name fifoStore, optionally followed by { … } for the stores of its
// elements), scalar declarations (Name string [modifiers…]), or the legacy
// two-word "type fifo". Anything else is refused, quoted: a declaration read
// as something it is not is a store the user believes exists.
func parseDeclarationBody(body string) (bucketDeclaration, error) {
	declaration := bucketDeclaration{options: map[string]interface{}{}}
	entries, err := splitTopLevel(unbrace(body))
	if err != nil {
		return bucketDeclaration{}, err
	}
	for _, entry := range entries {
		if err := declaration.add(entry); err != nil {
			return bucketDeclaration{}, err
		}
	}
	return declaration, nil
}

func (d *bucketDeclaration) add(entry string) error {
	if colon := topLevelIndex(entry, ':'); colon >= 0 {
		option, err := parseOptionDocument("{" + entry + "}")
		if err != nil {
			return fmt.Errorf("the entry %q reads as an option (key: value) but does not parse: %w", entry, err)
		}
		for key, value := range option {
			if value == nil {
				d.removals = append(d.removals, entry)
				continue
			}
			d.options[key] = value
		}
		return nil
	}
	header, nested := entry, ""
	if brace := topLevelIndex(entry, '{'); brace >= 0 {
		header, nested = strings.TrimSpace(entry[:brace]), entry[brace:]
	}
	words := strings.Fields(header)
	if len(words) < 2 {
		return usagef("the entry %q declares nothing: write a store field (Comments fifoStore), a scalar field (Caption string) or an option (type: hash)", entry)
	}
	name, kind := words[0], strings.ToLower(words[1])
	switch {
	case strings.HasSuffix(kind, "store"):
		store := server.StoreDeclaration{Name: name, Engine: words[1]}
		if len(words) > 2 {
			return usagef("the store field %q takes no modifiers, got %q", name, strings.Join(words[2:], " "))
		}
		if nested != "" {
			inner, err := parseDeclarationBody(nested)
			if err != nil {
				return fmt.Errorf("the fields of %q: %w", name, err)
			}
			if len(inner.options) > 0 || len(inner.removals) > 0 {
				return usagef("the fields of %q declare fields only, not options, got %q", name, nested)
			}
			if len(inner.uniques) > 0 {
				return usagef("the field %q inside the store field %q declares unique, but unique indexes on store-field elements are not available: only a bucket's own fields are indexed. "+
					"Declare the field on the bucket itself (create bucket <name> { %s string unique }), or give the elements their own bucket with a unique field on it",
					inner.uniques[0], name, inner.uniques[0])
			}
			for _, scalar := range inner.scalars {
				d.storeScalars = append(d.storeScalars, name+"."+scalar)
			}
			store.Fields = inner.stores
		}
		d.stores = append(d.stores, store)
	case nested != "":
		return usagef("only a store field holds { … } fields: %q", entry)
	case strings.EqualFold(name, "type") && len(words) == 2:
		d.options["type"] = kind
	case kind == "drop" || kind == "remove":
		d.removals = append(d.removals, entry)
	case scalarFieldTypes[kind]:
		d.addScalar(words)
	default:
		return usagef("the entry %q is not a declaration: a store field is Name fifoStore (or lifoStore, hashStore, btreeStore, heapStore, blobStore), "+
			"a scalar field is Name string (or uuid, int, number, bool, date…), an option is key: value", entry)
	}
	return nil
}

// addScalar files a scalar declaration. "unique" is the one modifier the
// server enforces (S-15): it moves the field to uniques and leaves the other
// modifiers, if any, in the noted-not-enforced list.
func (d *bucketDeclaration) addScalar(words []string) {
	kept := []string{words[0], words[1]}
	unique := false
	for _, modifier := range words[2:] {
		if strings.EqualFold(modifier, "unique") {
			unique = true
			continue
		}
		kept = append(kept, modifier)
	}
	if unique {
		d.uniques = append(d.uniques, words[0])
	}
	if !unique || len(kept) > 2 {
		d.scalars = append(d.scalars, strings.Join(kept, " "))
	}
}

// indicesOption is the create request's indices option with the DDL's unique
// fields added, each as the object the indices option itself takes, so the
// server's parseIndexSpecs stays the only parser of it. An explicit indices
// option keeps its place ahead of the DDL ones; a duplicate is the server's
// refusal, not merged here.
func (d bucketDeclaration) indicesOption(explicit interface{}) interface{} {
	if len(d.uniques) == 0 {
		return explicit
	}
	var list []interface{}
	switch declared := explicit.(type) {
	case nil:
	case []interface{}:
		list = append(list, declared...)
	default:
		list = append(list, declared)
	}
	for _, field := range d.uniques {
		list = append(list, map[string]interface{}{"fields": []interface{}{field}, "unique": true})
	}
	return list
}

// indexList prints the create request's indices option the way describe bucket
// names an index: a plain index as its field, a unique one as "<Field> (unique)".
// An entry is a field name or the object form with "field" or "fields" and an
// optional "unique"; anything else prints as it was written.
func indexList(indices interface{}) string {
	entries, ok := indices.([]interface{})
	if !ok {
		return fmt.Sprintf("%v", indices)
	}
	labels := make([]string, 0, len(entries))
	for _, entry := range entries {
		spec, isObject := entry.(map[string]interface{})
		if !isObject {
			labels = append(labels, fmt.Sprintf("%v", entry))
			continue
		}
		label := fmt.Sprintf("%v", spec["field"])
		if list, isList := spec["fields"].([]interface{}); isList {
			names := make([]string, 0, len(list))
			for _, field := range list {
				names = append(names, fmt.Sprintf("%v", field))
			}
			label = strings.Join(names, ", ")
		}
		if unique, _ := spec["unique"].(bool); unique {
			label += " (unique)"
		}
		labels = append(labels, label)
	}
	return "[" + strings.Join(labels, ", ") + "]"
}

// checkNames refuses one name declared twice in a statement, as a scalar
// and as a store included (spec §4.5 rule 3, a declaration-time rule, S-9
// (ii)). Duplicates among stores are the server validator's.
func (d bucketDeclaration) checkNames(bucket string) error {
	scalars := make(map[string]bool, len(d.scalars)+len(d.uniques))
	for _, scalar := range d.scalars {
		scalars[strings.Fields(scalar)[0]] = true
	}
	for _, unique := range d.uniques {
		scalars[unique] = true
	}
	for _, store := range d.stores {
		if scalars[store.Name] {
			return usagef("%q is declared both as a scalar field and as a store field on bucket %q: a field is a store or a value, never both", store.Name, bucket)
		}
	}
	return nil
}

// storesOption is the declared stores as the create request carries them.
func storesOption(stores []server.StoreDeclaration) []interface{} {
	list := make([]interface{}, 0, len(stores))
	for _, store := range stores {
		entry := map[string]interface{}{"name": store.Name, "engine": store.Engine}
		if len(store.Fields) > 0 {
			entry["fields"] = storesOption(store.Fields)
		}
		list = append(list, entry)
	}
	return list
}

// scalarsToNote is every scalar declaration the body names, top-level and
// inside a store field's braces, for the one not-enforced note. A nested
// declaration keeps its store field's name as a qualifier, so the note says
// where the scalar sits (BUG-0283).
func (d bucketDeclaration) scalarsToNote() []string {
	if len(d.storeScalars) == 0 {
		return d.scalars
	}
	scalars := make([]string, 0, len(d.scalars)+len(d.storeScalars))
	scalars = append(scalars, d.scalars...)
	return append(scalars, d.storeScalars...)
}

// scalarNote is printed once for a statement that declared scalar fields:
// they are accepted and enforced by nothing, and saying so is what keeps the
// silence from being a lie (S-9 (i); F-18).
func scalarNote(scalars []string) string {
	return fmt.Sprintf("note: field types on scalar fields (%s) are not enforced in this release; only store fields are created.", strings.Join(scalars, ", "))
}

// unbrace strips a body's outer braces.
func unbrace(body string) string {
	body = strings.TrimSpace(body)
	if strings.HasPrefix(body, "{") && strings.HasSuffix(body, "}") {
		body = body[1 : len(body)-1]
	}
	return body
}

// splitTopLevel splits on commas outside braces, brackets and quotes.
func splitTopLevel(body string) ([]string, error) {
	var entries []string
	depth, start := 0, 0
	var quote byte
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '{' || c == '[':
			depth++
		case c == '}' || c == ']':
			depth--
		case c == ',' && depth == 0:
			entries = append(entries, strings.TrimSpace(body[start:i]))
			start = i + 1
		}
	}
	if depth != 0 || quote != 0 {
		return nil, usagef("the body %q has an unclosed brace, bracket or quote", body)
	}
	if last := strings.TrimSpace(body[start:]); last != "" {
		entries = append(entries, last)
	}
	var kept []string
	for _, entry := range entries {
		if entry != "" {
			kept = append(kept, entry)
		}
	}
	return kept, nil
}

// topLevelIndex is the first index of c outside braces, brackets and quotes,
// or -1.
func topLevelIndex(entry string, c byte) int {
	depth := 0
	var quote byte
	for i := 0; i < len(entry); i++ {
		switch {
		case quote != 0:
			if entry[i] == quote {
				quote = 0
			}
		case entry[i] == '"' || entry[i] == '\'':
			quote = entry[i]
		case entry[i] == c && depth == 0:
			return i
		case entry[i] == '{' || entry[i] == '[':
			depth++
		case entry[i] == '}' || entry[i] == ']':
			depth--
		}
	}
	return -1
}

// alterRemovalError is Q3's refusal, client-side: removing a store field is
// not supported in 1.0.
func alterRemovalError(bucket string, removals []string) error {
	return usagef("removing a store field is not supported in 1.0 (%s): it would destroy every document's elements of it in %q with no preview and no undo. "+
		"alter bucket only adds store fields: alter bucket %s { Tags hashStore }", strings.Join(removals, ", "), bucket, bucket)
}

// handleAlterBucket routes an alter body by spec 4.1's dispatch table: store
// declarations only are a declare; one option key is routed by alterOptions; a
// removal is refused with Q3's reason; a mix is refused naming both halves.
func (h *CommandHandler) handleAlterBucket(cmd *Cmd) error {
	detail, _ := cmd.Options["detail"].(string)
	declaration, err := parseDeclarationBody(detail)
	if err != nil {
		return fmt.Errorf("alter bucket %s: %w", cmd.Name, err)
	}
	switch {
	case len(declaration.removals) > 0:
		return alterRemovalError(cmd.Name, declaration.removals)
	case len(declaration.uniques) > 0:
		return usagef("alter bucket %s cannot add a unique index (%s): this release declares indexes only when a bucket is created:\n"+
			"         create bucket %s { type: hash, %s string unique }\n"+
			"       to add one to an existing bucket, drop and recreate it; note that dropping deletes its data: 'drop bucket %s'",
			cmd.Name, strings.Join(declaration.uniques, ", "), cmd.Name, declaration.uniques[0], cmd.Name)
	case len(declaration.stores) > 0 && len(declaration.options) > 0:
		return usagef("alter bucket %s mixes store fields (%s) with options (%s): declare the store fields in one statement, and change the options in another",
			cmd.Name, storeNamesOf(declaration.stores), optionNamesOf(declaration.options))
	case len(declaration.stores) == 0:
		return h.alterOptions(cmd, declaration.options)
	}
	dbName := h.database(cmd)
	if dbName == "" {
		return noBucketDatabase("alter", cmd.Name)
	}
	if err := declaration.checkNames(cmd.Name); err != nil {
		return err
	}
	req := server.DeclareStoreFieldsRequest{Database: dbName, Name: cmd.Name}
	for _, entry := range storesOption(declaration.stores) {
		req.Stores = append(req.Stores, entry.(map[string]interface{}))
	}
	var resp server.DeclareStoreFieldsResponse
	if err := h.send(transport.OpDeclareStoreFields, &req, &resp); err != nil {
		return fmt.Errorf("alter bucket %s: %w", cmd.Name, err)
	}
	if !resp.Success {
		return &RefusedError{Message: resp.Message}
	}
	h.printf("OK  %s\n", terminalSafeText(resp.Message))
	if scalars := declaration.scalarsToNote(); len(scalars) > 0 {
		h.printf("%s\n", scalarNote(scalars))
	}
	return nil
}

// alterOptions handles an alter body that declares no store field: exactly one
// option key is acted on, and the key decides how (spec 4.1). Two or more are
// refused with one statement per key, because each is its own change.
func (h *CommandHandler) alterOptions(cmd *Cmd, options map[string]interface{}) error {
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	switch {
	case len(keys) > 1:
		statements := make([]string, 0, len(keys))
		for _, key := range keys {
			statements = append(statements, alterStatement(cmd.Name, key, options[key]))
		}
		return usagef("alter bucket takes one change per statement, and the body for %s names %d (%s): run each on its own:\n         %s",
			cmd.Name, len(keys), strings.Join(keys, ", "), strings.Join(statements, "\n         "))
	case len(keys) == 1 && keys[0] == "replication_factor":
		return h.alterReplicationFactor(cmd, options["replication_factor"])
	case len(keys) == 1 && keys[0] == "placement":
		return alterPlacementRefusal(cmd.Name, h.database(cmd), options["placement"])
	case len(keys) == 1 && keys[0] == "type":
		return usagef("bucket %q keeps its store type: changing a bucket's type is not available yet. "+
			"To move its documents, create a bucket of the new type and copy them.", cmd.Name)
	}
	return errAlterBucket(cmd.Name)
}

// alterStatement is the statement that makes one option's change alone.
func alterStatement(bucket, key string, value interface{}) string {
	text := scalarString(value)
	if word, isString := value.(string); isString && !bareWord.MatchString(word) {
		text = strconv.Quote(word)
	}
	return fmt.Sprintf("alter bucket %s { %s: %s }", bucket, key, text)
}

// bareWord is what an option value may be written as without quotes.
var bareWord = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// alterReplicationFactor sends OpAlterBucket (0x8E) and prints the owner's
// sentence. The server judges every refusal; the CLI only rejects a factor
// that is not a count, which no server could read as one.
func (h *CommandHandler) alterReplicationFactor(cmd *Cmd, rawFactor interface{}) error {
	dbName := h.database(cmd)
	if dbName == "" {
		return noBucketDatabase("alter", cmd.Name)
	}
	factor, err := replicationFactorOption(rawFactor)
	if err != nil || factor > math.MaxInt32 {
		return usagef("replication_factor must be a whole number of copies, 1 or more, got %s. Write it as a number: alter bucket %s { replication_factor: 3 }",
			scalarString(rawFactor), cmd.Name)
	}
	request := server.AlterBucketRequest{Database: dbName, Name: cmd.Name, ReplicationFactor: int32(factor)}
	var response server.AlterBucketResponse
	if err := h.send(transport.OpAlterBucket, &request, &response); err != nil {
		return fmt.Errorf("alter bucket %s: %w", cmd.Name, err)
	}
	if !response.Success {
		return &RefusedError{Message: response.Message}
	}
	h.printf("OK  %s\n", terminalSafeText(response.Message))
	return nil
}

func storeNamesOf(stores []server.StoreDeclaration) string {
	names := make([]string, 0, len(stores))
	for _, store := range stores {
		names = append(names, store.Name)
	}
	return strings.Join(names, ", ")
}

func optionNamesOf(options map[string]interface{}) string {
	names := make([]string, 0, len(options))
	for name := range options {
		names = append(names, name)
	}
	return strings.Join(names, ", ")
}

// parseFieldAddress reads the address form's bracketed part: after the
// bucket name, ["<document key>"].<field>, optionally [<element key>].<field>
// for L3, then .<method>. An element key is a quoted string or an integer
// (a fifo or lifo position).
func parseFieldAddress(cmd *Cmd, address string) error {
	docKey, rest, err := bracketed(address)
	if err != nil {
		return err
	}
	key, ok := docKey.(string)
	if !ok {
		return usagef(`a document key is quoted: db.%s["key"].<field>.<verb>(...), got [%v]`, cmd.Bucket, docKey)
	}
	cmd.DocKey = key
	field, rest, err := dottedName(rest)
	if err != nil {
		return err
	}
	if strings.HasPrefix(rest, "[") {
		var element interface{}
		if element, rest, err = bracketed(rest); err != nil {
			return err
		}
		cmd.Path = []interface{}{field, element}
		if field, rest, err = dottedName(rest); err != nil {
			return err
		}
	}
	cmd.Field = field
	method, rest, err := dottedName(rest)
	if err != nil {
		return err
	}
	if rest != "" {
		return usagef("unexpected %q after .%s", rest, method)
	}
	cmd.Method = method
	return nil
}

// bracketed reads [<key>] at the start of s: a quoted string or an integer.
func bracketed(s string) (interface{}, string, error) {
	if !strings.HasPrefix(s, "[") {
		return nil, "", usagef("expected [<key>], got %q", s)
	}
	end := -1
	var quote byte
	for i := 1; i < len(s); i++ {
		switch {
		case quote != 0:
			if s[i] == quote {
				quote = 0
			}
		case s[i] == '"' || s[i] == '\'':
			quote = s[i]
		case s[i] == ']':
			end = i
		}
		if end >= 0 {
			break
		}
	}
	if end < 0 {
		return nil, "", usagef("unclosed [ in %q", s)
	}
	inner := strings.TrimSpace(s[1:end])
	if len(inner) >= 2 && (inner[0] == '"' || inner[0] == '\'') && inner[len(inner)-1] == inner[0] {
		return inner[1 : len(inner)-1], s[end+1:], nil
	}
	position, err := strconv.ParseInt(inner, 10, 64)
	if err != nil {
		return nil, "", usagef("a key in [ ] is a quoted string or an integer position, got [%s]", inner)
	}
	return position, s[end+1:], nil
}

// dottedName reads .<name> at the start of s.
func dottedName(s string) (string, string, error) {
	if !strings.HasPrefix(s, ".") {
		return "", "", usagef("expected .<name>, got %q", s)
	}
	s = s[1:]
	end := strings.IndexAny(s, ".[")
	if end < 0 {
		end = len(s)
	}
	if end == 0 {
		return "", "", usagef("expected a name after the dot, got %q", s)
	}
	return s[:end], s[end:], nil
}

// handleField runs a field-store verb addressed as
// db.<bucket>["<key>"].<field>.<verb>(...).
func (h *CommandHandler) handleField(dbName string, cmd *Cmd) error {
	req := server.FieldRequest{Database: dbName, Bucket: cmd.Bucket, Key: cmd.DocKey, Field: cmd.Field, Path: cmd.Path, WriteConcern: h.wc}
	method := strings.ToLower(cmd.Method)
	address := fmt.Sprintf("db.%s[%q].%s.%s()", cmd.Bucket, cmd.DocKey, cmd.Field, method)
	switch method {
	case "push":
		if len(cmd.Args) > 0 {
			data, err := parseDocument(cmd.Args[0])
			if err != nil {
				return fmt.Errorf("%s: %w", address, err)
			}
			req.Data = data
			// A heap field orders by priority; every other engine keeps the
			// field as the user wrote it (the same rule as for buckets).
			if priority, isNumber, fractionErr := heapPriority(data["priority"]); isNumber {
				if fractionErr != nil {
					if err := h.refuseFractionOnHeapField(dbName, cmd, address, fractionErr); err != nil {
						return err
					}
				} else {
					req.Priority = priority
				}
			}
		}
		return h.fieldWrite(transport.OpFieldPush, req, address)
	case "pop":
		return h.fieldPop(req, cmd.Args, address)
	case "put":
		if len(cmd.Args) < 2 {
			return usagef(`usage: db.%s[%q].%s.put("key", {...})`, cmd.Bucket, cmd.DocKey, cmd.Field)
		}
		req.Element = unquote(cmd.Args[0])
		data, err := parseDocument(cmd.Args[1])
		if err != nil {
			return fmt.Errorf("%s: %w", address, err)
		}
		req.Data = data
		if _, hasPayload := data["data"]; hasPayload {
			// Only a blob field takes "data" as its bytes; on every other
			// engine it is an ordinary key of the element, so the engine
			// is looked up before the key is taken out of the document.
			isBlob, err := h.fieldIsBlob(dbName, cmd)
			if err != nil {
				return fmt.Errorf("%s: %w", address, err)
			}
			if isBlob {
				req.Content, req.Data = blobPayload(data)
				if err := checkBlobFitsAServer(req.Element, int64(len(req.Content))); err != nil {
					return err
				}
			}
		}
		return h.fieldWrite(transport.OpFieldPut, req, address)
	case "delete":
		if len(cmd.Args) < 1 {
			return usagef(`usage: db.%s[%q].%s.delete("key")`, cmd.Bucket, cmd.DocKey, cmd.Field)
		}
		req.Element = unquote(cmd.Args[0])
		return h.fieldWrite(transport.OpFieldDelete, req, address)
	case "peek":
		return h.fieldPeek(req, cmd.Args, address)
	case "get":
		if len(cmd.Args) < 1 {
			return usagef(`usage: db.%s[%q].%s.get("key")`, cmd.Bucket, cmd.DocKey, cmd.Field)
		}
		req.Element = unquote(cmd.Args[0])
		outPath := ""
		if len(cmd.Args) > 1 {
			options, err := parseOptionDocument(cmd.Args[1])
			if err != nil {
				return fmt.Errorf("%s: %w", address, err)
			}
			if out, ok := options["out"]; ok {
				outPath = scalarString(out)
			}
		}
		return h.fieldRead(transport.OpFieldGet, req, address, outPath)
	// Every spelling of the count answers the same call. `len`, `length` and
	// `count` all mean "how many", and the help teaches `length()` on a bucket
	// while `help fields` teaches `len()` on a store field — so a developer
	// who followed one and applied it at the other level was refused. The wire
	// call is the same in every case; only the verb word differs.
	case "len", "length", "count":
		var resp server.FieldResponse
		if err := h.send(transport.OpFieldLen, &req, &resp); err != nil {
			return fmt.Errorf("%s: %w", address, err)
		}
		h.printf("%d\n", resp.Len)
		return nil
	case "list":
		return h.fieldList(req, cmd.Args, address)
	case "claim", "ack", "release", "extend", "dead", "requeue", "discard":
		return refuseOnAStoreField(method, cmd)
	default:
		return usagef("%q is not a field-store verb: a store field takes push, pop, peek, len (or length, count), list (fifo, lifo, heap) or put, get, delete, len (or length, count), list (hash, btree, blob)", cmd.Method)
	}
}

func (h *CommandHandler) fieldWrite(op transport.Opcode, req server.FieldRequest, address string) error {
	var resp server.FieldResponse
	if err := h.send(op, &req, &resp); err != nil {
		return fmt.Errorf("%s: %w", address, err)
	}
	if !resp.Success {
		return refusedf("%s: %s", address, resp.Message)
	}
	switch {
	case op == transport.OpFieldDelete && !resp.Found:
		h.printf("NOT FOUND  %s\n", terminalSafe(resp.ElementKey))
	default:
		h.printf("OK\n")
	}
	return nil
}

// fieldIsBlob reports whether the field a command addresses is declared as a
// blobStore, from the bucket's stats.
func (h *CommandHandler) fieldIsBlob(dbName string, cmd *Cmd) (bool, error) {
	engine, err := h.fieldEngine(dbName, cmd)
	return strings.EqualFold(engine, "blob"), err
}

// fieldEngine says which engine a store field was declared with. It is empty
// for a field the bucket does not declare.
//
// A top-level field's engine is read from one fresh BUCKET_INFO, which counts
// no element; the callers decide how an element is written, so it is never the
// cached answer (freshDeclaration). A nested field's is not: BUCKET_INFO
// lists the top-level stores only and carries no parent, so a nested lookup
// still asks BUCKET_STATS, which does, at the cost of walking the stores. That
// is the one lookup left on the slow call (BUG-0520); naming the parent in
// StoreFieldInfo would remove it and is a wire change.
func (h *CommandHandler) fieldEngine(dbName string, cmd *Cmd) (string, error) {
	if len(cmd.Path) == 0 {
		return h.topLevelFieldEngine(dbName, cmd)
	}
	var stats server.BucketStatsResponse
	if err := h.send(transport.OpBucketStats, &server.BucketStatsRequest{Database: dbName, Bucket: cmd.Bucket}, &stats); err != nil {
		return "", fmt.Errorf("looking up the engine of field %q: %w", cmd.Field, err)
	}
	parent, _ := cmd.Path[0].(string)
	for _, declared := range stats.Stores {
		if declared.Name == cmd.Field && declared.Parent == parent {
			return declared.Engine, nil
		}
	}
	return "", nil
}

// topLevelFieldEngine reads a top-level field's engine from a fresh
// declaration. A bucket the server does not have has no field, as before.
func (h *CommandHandler) topLevelFieldEngine(dbName string, cmd *Cmd) (string, error) {
	declaration := h.freshDeclaration(dbName, cmd.Bucket)
	if declaration.failure != nil {
		return "", fmt.Errorf("looking up the engine of field %q: %w", cmd.Field, declaration.failure)
	}
	for _, declared := range declaration.stores {
		if declared.Name == cmd.Field {
			return declared.Engine, nil
		}
	}
	return "", nil
}

// refuseFractionOnHeapField returns fractionErr when the field is a heap, where
// the priority orders the element, and nil for any other engine, where
// "priority" is an ordinary key of the element. The engine is looked up only
// for a fraction, so the common push costs no extra round trip.
func (h *CommandHandler) refuseFractionOnHeapField(dbName string, cmd *Cmd, address string, fractionErr error) error {
	engine, err := h.fieldEngine(dbName, cmd)
	if err != nil {
		return fmt.Errorf("%s: %w", address, err)
	}
	if strings.EqualFold(engine, "heap") {
		return fmt.Errorf("%s: %w", address, fractionErr)
	}
	return nil
}

// fieldRead prints one element. A blob field's element prints as a blob
// bucket's get does: its metadata, then the text inline or a hint for binary
// content; outPath, when set, writes the content to that file instead.
func (h *CommandHandler) fieldRead(op transport.Opcode, req server.FieldRequest, address, outPath string) error {
	var resp server.FieldResponse
	if err := h.send(op, &req, &resp); err != nil {
		return fmt.Errorf("%s: %w", address, err)
	}
	switch {
	case !resp.Found:
		h.printf("NOT FOUND\n")
	case resp.BlobID != "":
		return h.printFieldBlob(resp, outPath)
	case outPath != "":
		return usagef("%s: {out: ...} writes a blobStore field's content to a file; this field is not a blobStore", address)
	default:
		h.printf("%s\n", h.doc(resp.Element))
	}
	return nil
}

// fieldPop removes the next elements of the field, in pop order, and prints one
// element document per line, as fieldPeek prints what a pop would take. An
// empty field prints EMPTY. The count is read by the reader the bucket verb
// uses, before anything is sent: a pop that ignored its arguments removed one
// element whatever it was given (BUG-0546).
func (h *CommandHandler) fieldPop(req server.FieldRequest, args []string, address string) error {
	where := fmt.Sprintf("db.%s[%q].%s", req.Bucket, req.Key, req.Field)
	limit, err := readVerbLimit("pop", where, args)
	if err != nil {
		return err
	}
	var resp server.FieldResponse
	if err := h.send(transport.OpFieldPop, &server.FieldPopRequest{FieldRequest: req, Limit: limit}, &resp); err != nil {
		return fmt.Errorf("%s: %w", address, err)
	}
	if !resp.Success {
		return refusedf("%s: %s", address, resp.Message)
	}
	if !resp.Found {
		h.printf("EMPTY\n")
		return nil
	}
	for _, item := range resp.Items {
		h.printf("%s\n", h.doc(item.Element))
	}
	return nil
}

// fieldPeek reads the next elements a pop of the field would take, in pop
// order, one element document per line. An empty field prints EMPTY, as pop on
// the field and the bucket verbs do; NOT FOUND is for a keyed read of a missing
// key. The count is read by the reader the bucket verb uses.
func (h *CommandHandler) fieldPeek(req server.FieldRequest, args []string, address string) error {
	where := fmt.Sprintf("db.%s[%q].%s", req.Bucket, req.Key, req.Field)
	limit, err := readVerbLimit("peek", where, args)
	if err != nil {
		return err
	}
	var resp server.FieldResponse
	if err := h.send(transport.OpFieldPeek, &server.FieldPeekRequest{FieldRequest: req, Limit: limit}, &resp); err != nil {
		return fmt.Errorf("%s: %w", address, err)
	}
	if !resp.Found {
		h.printf("EMPTY\n")
		return nil
	}
	for _, item := range resp.Items {
		h.printf("%s\n", h.doc(item.Element))
	}
	return nil
}

func (h *CommandHandler) printFieldBlob(resp server.FieldResponse, outPath string) error {
	if outPath != "" {
		if err := os.WriteFile(outPath, resp.Content, 0o600); err != nil {
			return usagef("writing %s: %v", outPath, err)
		}
		h.printf("OK  wrote %s (%s)\n", outPath, humanBytes(int64(len(resp.Content))))
		return nil
	}
	h.printf("Key:    %s\n", terminalSafe(resp.ElementKey))
	h.printf("Blob:   %s\n", terminalSafe(resp.BlobID))
	h.printf("Size:   %s\n", humanBytes(int64(len(resp.Content))))
	if len(resp.Element) > 0 {
		h.printf("Metadata: %s\n", h.doc(resp.Element))
	}
	if isTextPayload(resp.Content) {
		h.printf("Data:\n%s\n", terminalSafeText(strings.TrimRight(string(resp.Content), "\n")))
	} else {
		h.printf("Data:   binary (%s) — re-run with {out: \"file\"} to save it\n", humanBytes(int64(len(resp.Content))))
	}
	return nil
}

// listOptionKeys are the options a store field's list reads.
var listOptionKeys = map[string]bool{"limit": true, "after": true}

func (h *CommandHandler) fieldList(req server.FieldRequest, args []string, address string) error {
	list := server.FieldListRequest{FieldRequest: req}
	if len(args) > 0 {
		options, err := parseOptionDocument(args[0])
		if err != nil {
			return fmt.Errorf("%s: %w", address, err)
		}
		if list.Limit, err = readLimitOption(options, listOptionKeys); err != nil {
			return fmt.Errorf("%s: %w", address, err)
		}
		if after, ok := options["after"]; ok {
			if list.After, err = rawValueOf(after); err != nil {
				return fmt.Errorf("%s: after: %w", address, err)
			}
		}
	}
	var resp server.FieldListResponse
	if err := h.send(transport.OpFieldList, &list, &resp); err != nil {
		return fmt.Errorf("%s: %w", address, err)
	}
	for _, entry := range resp.Entries {
		switch {
		case entry.BlobID != "":
			h.printf("%s  blob %s\n", terminalSafe(entry.ElementKey), terminalSafe(entry.BlobID))
		case entry.ElementKey != "":
			h.printf("%s  %s\n", terminalSafe(entry.ElementKey), h.doc(entry.Element))
		default:
			h.printf("%s\n", h.doc(entry.Element))
		}
	}
	if resp.HasMore {
		h.printf("(more: pass {after: ...} with the last key to continue)\n")
	}
	return nil
}

// rawValueOf encodes one parsed value as a BSON value, for list's after.
func rawValueOf(v interface{}) (bson.RawValue, error) {
	if number, ok := v.(float64); ok && number == math.Trunc(number) {
		v = int64(number)
	}
	kind, encoded, err := bson.MarshalValue(v)
	if err != nil {
		return bson.RawValue{}, usagef("%v", err)
	}
	return bson.RawValue{Type: kind, Value: encoded}, nil
}

// unquote strips one pair of matching quotes.
func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}

// alterPlacementRefusal answers an alter that asks to change a bucket's
// placement. It is fixed when the bucket is created, because the documents
// of a spread bucket already sit in parts on several nodes; the way to change
// it is to recreate the bucket with the other placement, and the answer
// gives those statements.
func alterPlacementRefusal(bucket, database string, asked interface{}) error {
	word := scalarString(asked)
	if word != "spread" && word != "whole" {
		word = "whole"
	}
	if database == "" {
		database = "<database>"
	}
	return usagef("bucket %q keeps its placement: it is fixed when the bucket is created. "+
		"To change it, recreate the bucket (a dump does not carry blob payloads or blob store contents; copy those out first):\n"+
		"         dump --out ./dump-%s %s %s\n         drop bucket %s in %s\n         create bucket %s in %s { placement: %s }\n         restore --dir ./dump-%s",
		bucket, bucket, database, bucket, bucket, database, bucket, database, word, bucket)
}
