package cli

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/server"
)

// This file holds the argument-parsing helpers shared by the bucket command
// handlers. hoardDB documents its query syntax as JSON, but a developer typing
// at a prompt should not have to quote every key: {name: "ada"} and
// {'name': 'ada'} are accepted alongside strict JSON.

// parseDocument parses one command argument into a document of BSON values.
//
// Accepted forms:
//
//	{"name": "ada"}          strict JSON
//	{name: "ada", age: 42}   unquoted keys
//	{name: 'ada',}           single quotes, trailing comma
//	{ts: ISODate("...")}     date literal
//	{ts: {"$date": "..."}}   Extended JSON date
//	{id: {"$uuid": "..."}}   every supported Extended JSON wrapper
//
// Values follow the rule `hoarddb ingest` uses (relaxed Extended JSON): an
// integer literal is an int32 when it fits and an int64 when it does not, a
// literal with a decimal point or exponent is a double. Use it for documents
// that are stored or matched; options and DDL go through parseOptionDocument.
//
// The error names the fix rather than just the failure.
func parseDocument(arg string) (map[string]interface{}, error) {
	strict, err := strictDocumentText(arg)
	if err != nil {
		return nil, err
	}
	return decodeDataDocument(strings.TrimSpace(arg), strict)
}

// nowCallKey is the key normalizeDocument writes for a now() call, until
// decodeDataDocument turns the sub-document it names into the server's marker.
// It carries a random suffix chosen when the process starts, so no text a user
// types can spell it, and the sub-document {"$$now": true} is left alone
// (BUG-0151).
var nowCallKey = "$$now-call-" + rand.Text()

// markNowCalls replaces every sub-document that holds only nowCallKey with the
// server's now() marker, in place, at any depth. It runs after
// refuseDeprecatedTypes, so the marker the document carries afterwards came
// from a now() call and from nothing typed.
func markNowCalls(node interface{}) interface{} {
	switch typed := node.(type) {
	case map[string]interface{}:
		if _, isCall := typed[nowCallKey]; isCall && len(typed) == 1 {
			return server.NowMarker()
		}
		for name, member := range typed {
			typed[name] = markNowCalls(member)
		}
	case primitive.D:
		if len(typed) == 1 && typed[0].Key == nowCallKey {
			return server.NowMarker()
		}
		for position := range typed {
			typed[position].Value = markNowCalls(typed[position].Value)
		}
	case primitive.A:
		for position, element := range typed {
			typed[position] = markNowCalls(element)
		}
	}
	return node
}

// parseOptionDocument parses an options or DDL document: limit, sort,
// projection, replication_factor and the like. These stay plain JSON, so their
// numbers are float64 and every option reader keeps working (spec 7.2).
func parseOptionDocument(arg string) (map[string]interface{}, error) {
	strict, err := strictDocumentText(arg)
	if err != nil {
		return nil, err
	}
	return decodeOptionDocument(strings.TrimSpace(arg), strict)
}

// normalizeDocument rewrites relaxed document syntax into strict JSON:
// unquoted keys and bare words become strings, single quotes become double
// quotes, and trailing commas are dropped.
func normalizeDocument(s string) (string, error) {
	var b strings.Builder

	depth := 0
	inObject := false
	expectKey := false
	// containers and keyPosition track where a key really is. inObject and
	// expectKey are left as they were, stale after a closing bracket, because
	// the base grammar's reading of bare values depends on them; only the
	// bracketed-position rule needs the exact answer (typed-fields T-11).
	var containers []byte
	keyPosition := false

	lastSignificant := func() byte {
		out := b.String()
		for i := len(out) - 1; i >= 0; i-- {
			switch out[i] {
			case ' ', '\t', '\n', '\r':
				continue
			}
			return out[i]
		}
		return 0
	}
	dropTrailingComma := func() {
		if lastSignificant() == ',' {
			out := b.String()
			for i := len(out) - 1; i >= 0; i-- {
				if out[i] == ',' {
					b.Reset()
					b.WriteString(out[:i])
					return
				}
			}
		}
	}

	for i := 0; i < len(s); {
		c := s[i]
		switch c {
		case ' ', '\t', '\n', '\r':
			// Collapse whitespace so the output stays compact.
			j := i
			for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
				j++
			}
			if j < len(s) && s[j] != '}' && s[j] != ']' && s[j] != ',' {
				b.WriteByte(' ')
			}
			i = j

		case '{':
			depth++
			containers = append(containers, '{')
			keyPosition = true
			inObject = true
			expectKey = true
			b.WriteByte(c)
			i++

		case '[':
			depth++
			containers = append(containers, '[')
			keyPosition = false
			inObject = false
			expectKey = false
			b.WriteByte(c)
			i++

		case '}', ']':
			depth--
			if len(containers) > 0 {
				containers = containers[:len(containers)-1]
			}
			keyPosition = false
			dropTrailingComma()
			b.WriteByte(c)
			i++

		case ',':
			expectKey = inObject
			keyPosition = len(containers) > 0 && containers[len(containers)-1] == '{'
			b.WriteByte(c)
			i++

		case ':', '=':
			b.WriteByte(':')
			expectKey = false
			keyPosition = false
			i++

		case '"':
			end, err := endOfQuoted(s, i, '"')
			if err != nil {
				return "", err
			}
			b.WriteString(s[i : end+1])
			i = end + 1

		case '\'':
			end, err := endOfQuoted(s, i, '\'')
			if err != nil {
				return "", err
			}
			lit := s[i+1 : end]
			lit = strings.ReplaceAll(lit, `\`, `\\`)
			lit = strings.ReplaceAll(lit, `"`, `\"`)
			b.WriteByte('"')
			b.WriteString(lit)
			b.WriteByte('"')
			i = end + 1

		default:
			// Look ahead for date literal spellings: ISODate(...) or new Date(...)
			if !expectKey {
				if argEnd, isDateLiteral := dateLiteralAt(s, i); isDateLiteral {
					// Consume the literal and convert to Extended JSON format
					literal := s[i : argEnd+1]
					extJSON, err := dateLiteralToExtJSON(literal)
					if err != nil {
						return "", fmt.Errorf("could not read document %s: %w", s, err)
					}
					b.WriteString(extJSON)
					i = argEnd + 1
					continue
				}
			}

			if !expectKey && strings.HasPrefix(s[i:], "now()") {
				afterCall := i + len("now()")
				rest := strings.TrimLeft(s[afterCall:], " \t\r\n")
				switch {
				case strings.HasPrefix(rest, "."):
					return "", usagef("could not read document %s: now() cannot be followed by a method such as .add(...)\n"+
						"       write now() alone for the server's time of this write, or a fixed date as ISODate(\"2026-09-20T10:30:00Z\")", s)
				case rest == "" || rest[0] == ',' || rest[0] == '}' || rest[0] == ']':
					// The server replaces the marker this becomes with one instant
					// per write, before the write is logged, so every copy stores
					// the same date. The placeholder, not a fixed spelling, is what
					// stands for the call: a user's own {"$$now": true} is data.
					b.WriteString(`{"` + nowCallKey + `": true}`)
					i = afterCall
					continue
				}
			}

			j := i
			for j < len(s) && !isDocumentDelimiter(s[j]) {
				j++
				if keyPosition {
					j = skipBracketedPosition(s, j)
				}
			}
			token := strings.TrimSpace(s[i:j])
			if token == "" {
				return "", usagef("could not read document %s: unexpected character %q", s, string(c))
			}
			if !expectKey {
				if name, isCall := functionCallName(token); isCall {
					if name == "now" {
						return "", usagef("could not read document %s: write now() with nothing between the parentheses and nothing after it but a comma or a closing bracket", s)
					}
					return "", usagef("could not read document %s: %s() cannot be used in a document typed at the prompt\n"+
						"       write a date as {\"$date\": \"2026-09-20T10:30:00Z\"} or ISODate(\"2026-09-20T10:30:00Z\"), "+
						"or write now() for the server's time of this write; any other time has to come from your application", s, name)
				}
			}
			if !expectKey && isJSONLiteral(token) {
				b.WriteString(token)
			} else {
				b.WriteByte('"')
				b.WriteString(escapeJSON(token))
				b.WriteByte('"')
			}
			i = j
		}
	}

	if depth != 0 {
		return "", usagef("could not read document %s: unbalanced brackets", s)
	}
	return b.String(), nil
}

// functionCallName reports whether token starts like a function call, an
// identifier directly followed by '(' such as now(), and returns the name. A
// bare word is a string in the relaxed syntax, so without this check now()
// would be stored as the text "now()".
func functionCallName(token string) (string, bool) {
	end := 0
	for end < len(token) {
		ch := token[end]
		isLetter := ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_'
		if !isLetter && (end == 0 || ch < '0' || ch > '9') {
			break
		}
		end++
	}
	if end == 0 || end >= len(token) || token[end] != '(' {
		return "", false
	}
	return token[:end], true
}

// skipBracketedPosition returns the index after every "[digits]" that starts
// at s[at], or at when none does. A bare key in key position reads such a
// position as part of itself, so {Lines[3].Qty: 2} is the key "Lines[3].Qty"
// (typed-fields T-11). Only digits count: "[x]" stays a delimiter, so a
// mistyped position is a syntax error rather than a key holding brackets.
func skipBracketedPosition(s string, at int) int {
	for at < len(s) && s[at] == '[' {
		digitsEnd := at + 1
		for digitsEnd < len(s) && s[digitsEnd] >= '0' && s[digitsEnd] <= '9' {
			digitsEnd++
		}
		if digitsEnd == at+1 || digitsEnd >= len(s) || s[digitsEnd] != ']' {
			return at
		}
		at = digitsEnd + 1
	}
	return at
}

func isDocumentDelimiter(c byte) bool {
	switch c {
	case ':', '=', ',', '{', '}', '[', ']', '"', '\'':
		return true
	}
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// isJSONLiteral reports whether a bare token should stay unquoted (numbers,
// booleans, null) rather than becoming a string.
func isJSONLiteral(token string) bool {
	switch token {
	case "true", "false", "null":
		return true
	}
	if token == "" {
		return false
	}
	first := token[0]
	if first != '-' && first != '+' && (first < '0' || first > '9') {
		return false
	}
	var n json.Number
	return json.Unmarshal([]byte(token), &n) == nil
}

func escapeJSON(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// endOfQuoted returns the index of the closing quote for the string starting
// at start, honouring backslash escapes.
func endOfQuoted(s string, start int, quote byte) (int, error) {
	for i := start + 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case quote:
			return i, nil
		}
	}
	return 0, usagef("could not read document %s: unterminated %s string", s, string(quote))
}

// documentArg returns the first argument that is a document, if any.
func documentArg(args []string) string {
	for _, a := range args {
		if strings.HasPrefix(strings.TrimSpace(a), "{") {
			return a
		}
	}
	return ""
}

// keyFieldNames are the field names treated as a bucket key.
var keyFieldNames = []string{"_id_", "id", "key", "_key_"}

// keyFromArgs extracts the document key from a command's arguments. Both
// db.<bucket>.find("ada") and db.<bucket>.find({_id_: "ada"}) work.
func keyFromArgs(args []string) (string, error) {
	if len(args) == 0 {
		return "", nil
	}

	first := strings.TrimSpace(args[0])
	if strings.HasPrefix(first, "{") {
		doc, err := parseDocument(first)
		if err != nil {
			return "", err
		}
		for _, name := range keyFieldNames {
			if v, ok := doc[name]; ok {
				return scalarString(v), nil
			}
		}
		return "", nil
	}

	return strings.Trim(first, `"'`), nil
}

// scalarString renders a scalar document value for display or as a key.
func scalarString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		return fmt.Sprintf("%t", t)
	case float64:
		if t == float64(int64(t)) {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%g", t)
	default:
		data, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(data)
	}
}

// printDocuments renders a list of documents, one JSON object per line. The
// server sends scan and search results as raw BSON; jsonLine
// decodes each one, as it does a point read's FindResponse.Data.
func (h *CommandHandler) printDocuments(docs []bson.Raw) {
	h.printDocumentsWith(docs, nil)
}

// printDocumentsWith is printDocuments for a list that may carry the bucket's
// declared stores (cli/storefields.go); nil prints each document as it is.
func (h *CommandHandler) printDocumentsWith(docs []bson.Raw, decoration *documentDecoration) {
	if len(docs) == 0 {
		h.printf("(no documents)\n")
		return
	}
	for i, doc := range docs {
		h.printf("%s\n", h.renderDocument(doc, decoration))
		if h.pretty && i < len(docs)-1 {
			// A blank line between documents, so a list of them reads as a list:
			// the one-per-line form does not need it, the indented form does.
			h.printf("\n")
		}
	}
}

// doc renders one document for the terminal in the session's JSON flavour. Every
// document printer goes through it: the format is a property of the session
// (--extjson, --pretty), not of each call site, and jsonLine has no default left
// to forget.
func (h *CommandHandler) doc(document interface{}) string {
	return h.renderDocument(document, nil)
}

// renderDocument is doc with the bucket's decoration, when there is one.
func (h *CommandHandler) renderDocument(document interface{}, decoration *documentDecoration) string {
	return formatDocument(document, h.extendedJSON, h.pretty, decoration)
}

// formatDocument is the one place a document becomes text, for the legacy
// handler and for ring mode alike: the session's JSON flavour (extJSON) and
// layout (pretty), with the bucket's decoration when there is one.
func formatDocument(document interface{}, extJSON, pretty bool, decoration *documentDecoration) string {
	var line string
	if decoration != nil {
		line = decoratedLine(document, extJSON, decoration)
	} else {
		line = jsonLine(document, extJSON)
	}
	// encoding/json escapes the C0 controls but writes DEL and the C1 controls
	// (U+009B is a CSI on some terminals) as they are.
	line = jsonEscapeControls(line)
	if !pretty {
		return line
	}
	return indentJSON(line)
}

// indentJSON reflows a rendered document onto several lines, two spaces per level.
// It copies the characters it is given rather than decoding and re-encoding them,
// so a value the compact form rendered exactly — a 64-bit integer, a decimal, the
// text of a date — is the same value, byte for byte, when it is read on several
// lines.
func indentJSON(compact string) string {
	var b strings.Builder
	b.Grow(len(compact) + len(compact)/4)
	depth := 0
	inString, escaped := false, false

	indent := func() {
		b.WriteByte('\n')
		b.WriteString(strings.Repeat("  ", depth))
	}

	for i := 0; i < len(compact); i++ {
		ch := compact[i]
		if inString {
			b.WriteByte(ch)
			switch {
			case escaped:
				escaped = false
			case ch == '\\':
				escaped = true
			case ch == '"':
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
			b.WriteByte(ch)
		case '{', '[':
			// An empty object or array stays whole: {"a": {}} reads better than
			// three lines holding nothing.
			if i+1 < len(compact) && (compact[i+1] == '}' || compact[i+1] == ']') {
				b.WriteByte(ch)
				b.WriteByte(compact[i+1])
				i++
				continue
			}
			depth++
			b.WriteByte(ch)
			indent()
		case '}', ']':
			// A document the printer could not decode comes through as a plain
			// message, not as JSON, and this function must not miscount braces
			// it never opened.
			if depth > 0 {
				depth--
			}
			indent()
			b.WriteByte(ch)
		case ',':
			b.WriteByte(ch)
			// The compact form separates with ", "; on several lines the newline
			// does that job, so the space goes.
			if i+1 < len(compact) && compact[i+1] == ' ' {
				i++
			}
			indent()
		case ':':
			// One space after the colon whichever flavour rendered it: jsonLine
			// writes one, the driver's Extended JSON writes none.
			b.WriteByte(':')
			b.WriteByte(' ')
			if i+1 < len(compact) && compact[i+1] == ' ' {
				i++
			}
		default:
			b.WriteByte(ch)
		}
	}
	return b.String()
}

// jsonLine renders a document as single-line JSON with sorted keys so output
// is stable between runs. When extendedJSON is true, dates are rendered as
// Extended JSON ({"$date": "..."}); otherwise they render as RFC 3339 strings.
// It accepts either a decoded map or the raw BSON document the server now
// returns (server.Find/Pop/Peek carry the document bytes through untouched
// rather than decoding and re-encoding them).
func jsonLine(doc interface{}, useExtJSON bool) string {
	m, early := decodedDocument(doc)
	if early != "" {
		return early
	}
	if useExtJSON {
		// Use Extended JSON output
		return jsonLineExtJSONImpl(m)
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return objectLine(m, keys)
}

// decodedDocument decodes a document to its fields. A non-empty second result is
// the whole line to print instead: the document did not decode, or it is empty.
func decodedDocument(doc interface{}) (map[string]interface{}, string) {
	m, isMap := doc.(map[string]interface{})
	if !isMap {
		if raw, isRaw := doc.(bson.Raw); isRaw && len(raw) > 0 {
			if err := bson.Unmarshal(raw, &m); err != nil {
				return nil, fmt.Sprintf("(decode error: %v)", err)
			}
		}
	}
	if m == nil {
		return nil, "{}"
	}
	return m, ""
}

// objectLine writes m as single-line JSON in the key order given.
func objectLine(m map[string]interface{}, keys []string) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteString(": ")
		vb, err := json.Marshal(printableValue(m[k]))
		if err != nil {
			vb = []byte(fmt.Sprintf("%q", fmt.Sprintf("%v", m[k])))
		}
		b.Write(vb)
	}
	b.WriteByte('}')
	return b.String()
}

// printableValue swaps the BSON types that have an Extended JSON wrapper for
// that wrapper, at any depth, so the default printer shows what a developer can
// type back (spec 7.2 item 6). encoding/json would print a UUID as
// {"Subtype":4,"Data":"..."}, which no filter accepts. Dates keep their RFC 3339
// text and numbers their plain form: only the types that have no JSON spelling
// of their own change.
func printableValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case primitive.Binary:
		if typed.Subtype == bson.TypeBinaryUUID && len(typed.Data) == 16 {
			return map[string]interface{}{"$uuid": uuidText(typed.Data)}
		}
		return map[string]interface{}{"$binary": map[string]interface{}{
			"base64":  base64.StdEncoding.EncodeToString(typed.Data),
			"subType": fmt.Sprintf("%02x", typed.Subtype),
		}}
	case primitive.ObjectID:
		return map[string]interface{}{"$oid": typed.Hex()}
	case primitive.Decimal128:
		return map[string]interface{}{"$numberDecimal": typed.String()}
	case primitive.Regex:
		return map[string]interface{}{"$regularExpression": map[string]interface{}{"pattern": typed.Pattern, "options": typed.Options}}
	case primitive.Timestamp:
		return map[string]interface{}{"$timestamp": map[string]interface{}{"t": typed.T, "i": typed.I}}
	case map[string]interface{}:
		members := make(map[string]interface{}, len(typed))
		for name, member := range typed {
			members[name] = printableValue(member)
		}
		return members
	case primitive.A:
		return printableList(typed)
	case []interface{}:
		return printableList(typed)
	}
	return value
}

func printableList(list []interface{}) []interface{} {
	elements := make([]interface{}, len(list))
	for index, element := range list {
		elements[index] = printableValue(element)
	}
	return elements
}

// uuidText formats 16 bytes as 8-4-4-4-12 hex, the form {"$uuid": ...} reads.
func uuidText(raw []byte) string {
	text := hex.EncodeToString(raw)
	return text[0:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:32]
}

// jsonLineExtJSONImpl renders a document using Extended JSON format.
func jsonLineExtJSONImpl(m map[string]interface{}) string {
	extJSON, err := bson.MarshalExtJSON(m, false, false)
	if err != nil {
		// Fallback to standard JSON if Extended JSON fails
		data, _ := json.Marshal(m)
		return string(data)
	}

	// Parse the Extended JSON output to sort keys (to maintain stable output)
	var parsed map[string]interface{}
	if err := json.Unmarshal(extJSON, &parsed); err != nil {
		return string(extJSON)
	}

	// Re-marshal with sorted keys
	keys := make([]string, 0, len(parsed))
	for k := range parsed {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteString(": ")
		vb, err := json.Marshal(parsed[k])
		if err != nil {
			vb = []byte(fmt.Sprintf("%q", fmt.Sprintf("%v", parsed[k])))
		}
		b.Write(vb)
	}
	b.WriteByte('}')
	return b.String()
}

// jsonLineExtJSON is a test helper that renders a document in Extended JSON format.
func jsonLineExtJSON(doc interface{}) string {
	return jsonLine(doc, true)
}

// humanBytes formats a byte count for humans.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value := float64(n)
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	for _, u := range units {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, u)
		}
	}
	return fmt.Sprintf("%.1f PiB", value/unit)
}

// printBucketList renders bucket rows in aligned columns.
func (h *CommandHandler) printBucketList(buckets []server.BucketInfo) {
	if len(buckets) == 0 {
		h.printf("(no buckets)\n")
		return
	}
	nameWidth := len("NAME")
	typeWidth := len("TYPE")
	for _, b := range buckets {
		if len(terminalSafe(b.Name)) > nameWidth {
			nameWidth = len(terminalSafe(b.Name))
		}
		if len(terminalSafe(string(b.Type))) > typeWidth {
			typeWidth = len(terminalSafe(string(b.Type)))
		}
	}
	now := time.Now()
	h.printf("%-*s  %-*s  %-7s  %s\n", nameWidth, "NAME", typeWidth, "TYPE", "ENTRIES", "REPLICATION")
	for _, b := range buckets {
		h.printf("%-*s  %-*s  %-7d  %s\n", nameWidth, terminalSafe(b.Name), typeWidth, terminalSafe(string(b.Type)), b.Entries, replicationColumn(b, now))
	}
	h.printDeadBanners(buckets)
}

// replicationColumn renders a bucket's copies for `show buckets`. A server
// that sends a State gives the owner's view (replication spec §12.1):
//
//	rf 3 (3/3)                                      healthy
//	rf 3 (1/3, copying to node-b (4h))              a copy pending, with its age
//	rf 3 (2/3, node-c not in sync)                  a stalled addressee
//	rf 3 (2/3, under-replicated: 2 members)         fewer members than the factor
//	rf 3 (standalone: 1 copy)                       no one to copy to
//	rf 3 (owner elsewhere), rf 3 (leaving)          a row from a node that is not the owner
//
// A bucket on the node's own factor puts "inherited" first inside the
// parenthesis. A server that predates the fields sends no State and its rows
// print as they did: `rf 3`, `rf 1 (inherited)`, and `, leaving` for a copy
// outside the bucket's set.
func replicationColumn(b server.BucketInfo, now time.Time) string {
	if b.ReplicationFactor == 0 {
		return withLeaving("-", b)
	}
	if b.State == "" {
		return withLeaving(legacyReplicationColumn(b), b)
	}
	if b.State == server.StateStandalone && b.ReplicationFactor <= 1 {
		return legacyReplicationColumn(b)
	}
	var parts []string
	if b.Inherited {
		parts = append(parts, "inherited")
	}
	parts = append(parts, ownerViewParts(b, now)...)
	return fmt.Sprintf("rf %d (%s)", b.ReplicationFactor, strings.Join(parts, ", "))
}

// legacyReplicationColumn is `rf 3`, or `rf 1 (inherited)`.
func legacyReplicationColumn(b server.BucketInfo) string {
	if b.Inherited {
		return fmt.Sprintf("rf %d (inherited)", b.ReplicationFactor)
	}
	return fmt.Sprintf("rf %d", b.ReplicationFactor)
}

// withLeaving adds the mark of a copy this node keeps outside the bucket's set.
func withLeaving(column string, b server.BucketInfo) string {
	if b.Outside {
		return column + ", leaving"
	}
	return column
}

// ownerViewParts is what follows "inherited" inside the parenthesis for a row
// that carries a State.
func ownerViewParts(b server.BucketInfo, now time.Time) []string {
	switch {
	case b.State == server.StateOwnerElsewhere && b.Outside:
		return []string{"leaving"}
	case b.State == server.StateStandalone:
		return []string{"standalone: 1 copy"}
	case b.State == server.StateOwnerElsewhere:
		return []string{"owner elsewhere"}
	}
	return append([]string{fmt.Sprintf("%d/%d", b.InSync, b.ReplicationFactor)},
		copyNotes(b.State, b.InSync, b.PendingCopies, b.NotInSync, now)...)
}

// copyNotes is what follows the in-sync count when copies are missing: the
// members still to be copied to with their age, the addressees whose stream is
// stalled, and, when neither explains the count, that there are fewer members
// than the factor.
func copyNotes(state string, inSync int, pending []server.PendingCopy, notInSync []string, now time.Time) []string {
	var notes []string
	if len(pending) > 0 {
		copying := make([]string, len(pending))
		for i, copyPending := range pending {
			copying[i] = fmt.Sprintf("%s (%s)", copyPending.Node, formatCopyAge(copyPending.Since, now))
		}
		notes = append(notes, "copying to "+strings.Join(copying, ", "))
	}
	for _, node := range notInSync {
		notes = append(notes, node+" not in sync")
	}
	if state == server.StateUnderReplicated && len(notInSync) == 0 {
		notes = append(notes, fmt.Sprintf("under-replicated: %d members", inSync))
	}
	return notes
}

// formatCopyAge is how long ago since (Unix seconds on the owner's clock) was,
// rounded down to the largest whole unit: 45s, 12m, 4h, 3d. An owner whose
// clock is ahead of this one gives 0s, not a negative age.
func formatCopyAge(since int64, now time.Time) string {
	age := max(now.Sub(time.Unix(since, 0)), 0)
	switch {
	case age >= 24*time.Hour:
		return fmt.Sprintf("%dd", int(age/(24*time.Hour)))
	case age >= time.Hour:
		return fmt.Sprintf("%dh", int(age/time.Hour))
	case age >= time.Minute:
		return fmt.Sprintf("%dm", int(age/time.Minute))
	}
	return fmt.Sprintf("%ds", int(age/time.Second))
}

// dateLiteralAt checks whether the string at position i starts with ISODate( or new Date(,
// and if so, returns the end position of the closing paren and true.
// It recognizes both "ISODate(...)" and "new Date(...)" spellings.
func dateLiteralAt(s string, i int) (int, bool) {
	const isoDatePrefix = "ISODate("
	const newDatePrefix = "new Date("

	// Check for ISODate( spelling (case-insensitive)
	if len(s)-i >= len(isoDatePrefix) && eqCaseInsensitive(s[i:i+len(isoDatePrefix)], isoDatePrefix) {
		if end, err := findClosingParen(s, i+len(isoDatePrefix)-1); err == nil {
			return end, true
		}
		return 0, false
	}

	// Check for new Date( spelling, allowing whitespace between new and Date
	if len(s)-i >= len(newDatePrefix) {
		j := i
		// Skip "new"
		if j+3 <= len(s) && eqCaseInsensitive(s[j:j+3], "new") {
			j += 3
			// Skip whitespace
			for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
				j++
			}
			// Check for "Date("
			if j+5 <= len(s) && eqCaseInsensitive(s[j:j+5], "Date(") {
				if end, err := findClosingParen(s, j+4); err == nil {
					return end, true
				}
			}
		}
	}
	return 0, false
}

// eqCaseInsensitive compares two strings case-insensitively.
func eqCaseInsensitive(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		aLower := toLower(a[i])
		bLower := toLower(b[i])
		if aLower != bLower {
			return false
		}
	}
	return true
}

// toLower converts an ASCII character to lowercase.
func toLower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + 32
	}
	return c
}

// findClosingParen finds the closing parenthesis for an opening paren at position start.
// It returns the index of the closing paren if successful.
func findClosingParen(s string, start int) (int, error) {
	// start should be the position of the opening paren
	if start >= len(s) || s[start] != '(' {
		return 0, usagef("expected opening paren")
	}

	// Find the quoted argument
	i := start + 1
	// Skip whitespace
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}

	if i >= len(s) || (s[i] != '"' && s[i] != '\'') {
		return 0, usagef("expected quoted string in date literal")
	}

	// Find the end of the quoted string
	quote := s[i]
	endQuote, err := endOfQuoted(s, i, quote)
	if err != nil {
		return 0, err
	}

	// Now find the closing paren
	i = endQuote + 1
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}

	if i >= len(s) || s[i] != ')' {
		return 0, usagef("expected closing paren in date literal")
	}

	return i, nil
}

// dateLiteralToExtJSON converts a date literal (ISODate(...) or new Date(...))
// to an Extended JSON format {"$date": "..."}.
func dateLiteralToExtJSON(literal string) (string, error) {
	const isoDatePrefix = "ISODate("

	// Extract the quoted argument
	var start int
	if len(literal) >= len(isoDatePrefix) && eqCaseInsensitive(literal[:len(isoDatePrefix)], isoDatePrefix) {
		start = len(isoDatePrefix)
	} else if len(literal) >= 3 && eqCaseInsensitive(literal[:3], "new") {
		// new Date( format
		i := 3
		for i < len(literal) && (literal[i] == ' ' || literal[i] == '\t' || literal[i] == '\n' || literal[i] == '\r') {
			i++
		}
		if i+5 > len(literal) || !eqCaseInsensitive(literal[i:i+5], "Date(") {
			return "", usagef("malformed new Date literal")
		}
		start = i + 5 // Skip "Date("
	} else {
		return "", usagef("unrecognized date literal")
	}

	// Extract the quoted string
	if start >= len(literal) {
		return "", usagef("malformed date literal")
	}

	quote := literal[start]
	if quote != '"' && quote != '\'' {
		return "", usagef("expected quoted string")
	}

	// Find the closing quote
	end := start + 1
	for end < len(literal) {
		if literal[end] == quote && (end == 0 || literal[end-1] != '\\') {
			break
		}
		end++
	}
	if end >= len(literal) {
		return "", usagef("unterminated string in date literal")
	}

	// Extract the argument (without quotes)
	arg := literal[start+1 : end]

	// Return as Extended JSON with the string form: {"$date": "..."}
	result, err := json.Marshal(map[string]interface{}{"$date": arg})
	if err != nil {
		return "", usagef("%v", err)
	}
	return string(result), nil
}

// copyFacts is the owner's view of a bucket's copies as describe prints it
// (replication spec §12.2): the count and why it is short, the set with the
// owner first, and the pending copy a member adds from its own record. A server
// that sends no State, and a node with no one to copy to at factor 1, say
// nothing.
func copyFacts(resp server.BucketStatsResponse, now time.Time) []fact {
	var rows []fact
	switch resp.State {
	case server.StateOK, server.StateCopying, server.StateUnderReplicated:
		count := fmt.Sprintf("%d of %d in sync", resp.InSync, resp.ReplicationFactor)
		notes := copyNotes(resp.State, resp.InSync, resp.PendingCopies, resp.NotInSync, now)
		rows = append(rows, fact{"Copies", strings.Join(append([]string{count}, notes...), ", ")})
	case server.StateOwnerUnreachable:
		rows = append(rows, fact{"Copies", "unknown: the owner did not answer, so this node cannot count the copies"})
	case server.StateTakingOver:
		from := ""
		if resp.PreviousOwner != "" {
			from = " from " + resp.PreviousOwner
		}
		rows = append(rows, fact{"Copies", "unknown: the owner is taking this bucket over" + from + " and cannot count the copies yet"})
	case server.StateStandalone:
		if resp.ReplicationFactor > 1 {
			rows = append(rows, fact{"Copies", "1 (standalone: no other member to copy to)"})
		}
	}
	if len(resp.Nodes) > 0 {
		members := append([]string(nil), resp.Nodes...)
		members[0] += " (owner)"
		rows = append(rows, fact{"Nodes", strings.Join(members, ", ")})
	}
	if line, found := strings.CutPrefix(resp.PendingCopy, "pending copy here: "); found {
		rows = append(rows, fact{"Pending copy here", line})
	} else if resp.PendingCopy != "" {
		rows = append(rows, fact{"Pending copy", resp.PendingCopy})
	}
	return rows
}
