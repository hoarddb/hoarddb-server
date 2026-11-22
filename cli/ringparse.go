package cli

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ringMaxDepth bounds the nesting of a document or array literal, so a hostile
// line cannot recurse the parser into the ground.
const ringMaxDepth = 32

// ringSource says which names completion offers at a position.
type ringSource int

const (
	srcNone ringSource = iota
	srcDatabases
	srcBuckets
	srcStoreFields
	srcFields
	srcNodes
	srcTopics
)

// ringExpect is one thing the grammar could accept at the furthest position
// reached: a literal token, names from a source, or a description.
type ringExpect struct {
	lit    string
	src    ringSource
	hint   string
	topic  string
	db     string
	bucket string
}

// ringMatcher matches one line's tokens against the table. It records the
// furthest failure, which is what both the error message and Tab completion
// are built from.
type ringMatcher struct {
	line     string
	toks     []ringTok
	selected string
	args     []Arg
	form     *ringForm

	farPos    int
	farForm   *ringForm
	farExps   []ringExpect
	farCustom string
	farAt     int // byte offset the custom message points at
	failed    bool
}

func (m *ringMatcher) argText(name string) string {
	for _, a := range m.args {
		if a.Name == name {
			return a.Text
		}
	}
	return ""
}

func (m *ringMatcher) database() string {
	if text := m.argText("database"); text != "" {
		return text
	}
	return m.selected
}

// reach records that matching got as far as token index pos.
func (m *ringMatcher) reach(pos int) {
	if !m.failed || pos > m.farPos {
		m.failed = true
		m.farPos = pos
		m.farForm = m.form
		m.farExps = m.farExps[:0]
		m.farCustom = ""
	}
}

// expect records that, at token index pos, the grammar accepts exps.
func (m *ringMatcher) expect(pos int, exps ...ringExpect) {
	m.reach(pos)
	if pos != m.farPos {
		return
	}
	for _, e := range exps {
		e.db, e.bucket = m.database(), m.argText("bucket")
		m.farExps = append(m.farExps, e)
	}
}

// refuse records a specific message for the token at index pos.
func (m *ringMatcher) refuse(pos int, message string) {
	m.reach(pos)
	if pos != m.farPos || m.farCustom != "" {
		return
	}
	m.farCustom = message
	m.farAt = m.offset(pos)
}

// offset returns the byte offset of token pos, or the line length at the end.
func (m *ringMatcher) offset(pos int) int {
	if pos < len(m.toks) {
		return m.toks[pos].start
	}
	return len(m.line)
}

func (m *ringMatcher) litExpect(words ...string) []ringExpect {
	out := make([]ringExpect, 0, len(words))
	for _, word := range words {
		out = append(out, ringExpect{lit: word})
	}
	return out
}

// seq matches elems at token index pos, calling k with the position after
// them; it backtracks through options.
func (m *ringMatcher) seq(elems []ringElem, pos int, k func(int) bool) bool {
	if len(elems) == 0 {
		return k(pos)
	}
	element, rest := elems[0], elems[1:]
	mark := len(m.args)
	switch element.kind {
	case elemLit, elemFlag:
		if pos >= len(m.toks) || m.toks[pos].kind == tokString || !strings.EqualFold(m.toks[pos].text, element.text) {
			m.expect(pos, ringExpect{lit: strings.ToLower(element.text)})
			return false
		}
		if element.kind == elemFlag {
			m.args = append(m.args, Arg{Name: element.name, Kind: ArgFlag})
		}
		if m.seq(rest, pos+1, k) {
			return true
		}
	case elemOpt:
		if m.seq(element.opt, pos, func(next int) bool { return m.seq(rest, next, k) }) {
			return true
		}
		m.args = m.args[:mark]
		return m.seq(rest, pos, k)
	case elemArg:
		end, ok := m.parseArg(element, pos)
		if !ok {
			m.args = m.args[:mark]
			return false
		}
		if m.seq(rest, end, k) {
			return true
		}
	}
	m.args = m.args[:mark]
	return false
}

// nameOK reports whether text is a legal database, bucket, field or node name.
func ringNameOK(text string) bool {
	if text == "" || text[0] == '-' {
		return false
	}
	for _, r := range text {
		if !ringWordRune(r) || r == '$' {
			return false
		}
	}
	return true
}

func (m *ringMatcher) parseArg(element ringElem, pos int) (int, bool) {
	push := func(a Arg) {
		a.Name = element.name
		m.args = append(m.args, a)
	}
	src := map[ringArgKind]ringSource{kDatabase: srcDatabases, kBucket: srcBuckets, kStoreField: srcStoreFields, kMember: srcNodes}[element.arg]
	tok := func() (ringTok, bool) {
		if pos >= len(m.toks) {
			return ringTok{}, false
		}
		return m.toks[pos], true
	}
	switch element.arg {
	case kNewName, kDatabase, kBucket, kStoreField, kNewNode, kMember:
		t, ok := tok()
		if !ok || t.kind != tokWord || !ringNameOK(t.text) {
			m.expect(pos, ringExpect{src: src, hint: ringKindHint(element)})
			return 0, false
		}
		push(Arg{Kind: ArgText, Text: t.text})
		return pos + 1, true
	case kInt:
		t, ok := tok()
		if !ok || t.kind != tokWord {
			m.expect(pos, ringExpect{hint: "a number"})
			return 0, false
		}
		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil || n < 0 {
			m.refuse(pos, fmt.Sprintf("%q is not a number: write a whole number such as 10", t.text))
			m.expect(pos, ringExpect{hint: "a number"})
			return 0, false
		}
		push(Arg{Kind: ArgInt, Int: n})
		return pos + 1, true
	case kSlot:
		t, ok := tok()
		if !ok || t.kind != tokWord {
			m.expect(pos, ringExpect{hint: "a slot number"})
			return 0, false
		}
		n, err := ringSlotNumber(t.text)
		if err != nil {
			m.refuse(pos, fmt.Sprintf("%q is not a slot: write a whole number such as 5 or 0x0a4e. Run 'status seeding' to list the slots of this node", t.text))
			m.expect(pos, ringExpect{hint: "a slot number"})
			return 0, false
		}
		if n > ringLastSlot {
			m.refuse(pos, fmt.Sprintf("slot %d is past the last slot, %d. Run 'status seeding' to list the slots of this node", n, ringLastSlot))
			m.expect(pos, ringExpect{hint: "a slot number"})
			return 0, false
		}
		push(Arg{Kind: ArgInt, Int: n})
		return pos + 1, true
	case kHex:
		t, ok := tok()
		if !ok || t.kind != tokWord || !ringHexOK(t.text) {
			m.expect(pos, ringExpect{hint: "a hex cursor"})
			return 0, false
		}
		push(Arg{Kind: ArgText, Text: t.text})
		return pos + 1, true
	case kQuotedID:
		t, ok := tok()
		if !ok || t.kind != tokString {
			m.expect(pos, ringExpect{hint: `a document id in double quotes, as in "ada"`})
			return 0, false
		}
		push(Arg{Kind: ArgText, Text: t.text})
		return pos + 1, true
	case kAnyID:
		t, ok := tok()
		if !ok || (t.kind != tokString && (t.kind != tokWord || !ringNameOK(t.text))) {
			m.expect(pos, ringExpect{hint: "a document id"})
			return 0, false
		}
		push(Arg{Kind: ArgText, Text: t.text})
		return pos + 1, true
	case kAddr:
		return m.parseAddr(pos, push)
	case kChoice:
		t, ok := tok()
		if ok && t.kind == tokWord {
			for _, word := range element.choices {
				if strings.EqualFold(word, t.text) {
					push(Arg{Kind: ArgText, Text: word})
					return pos + 1, true
				}
			}
		}
		m.expect(pos, m.litExpect(element.choices...)...)
		return 0, false
	case kUser:
		t, ok := tok()
		if !ok || t.kind == tokPunct || (t.kind == tokWord && !ringNameOK(t.text)) || (t.kind == tokString && t.text == "") {
			m.expect(pos, ringExpect{hint: "a user name, in double quotes if it has punctuation"})
			return 0, false
		}
		push(Arg{Kind: ArgText, Text: t.text})
		return pos + 1, true
	case kRoleSpec:
		end, text, ok := m.roleSpecAt(pos)
		if !ok {
			return 0, false
		}
		push(Arg{Kind: ArgText, Text: text})
		return end, true
	case kRoles:
		return m.parseRoles(pos, push)
	case kTopic:
		end := pos
		for end < len(m.toks) && m.toks[end].kind == tokWord {
			end++
		}
		if end == pos {
			m.expect(pos, ringExpect{src: srcTopics, hint: "a topic"})
			return 0, false
		}
		words := make([]string, 0, end-pos)
		for _, t := range m.toks[pos:end] {
			words = append(words, t.text)
		}
		typed := strings.Join(words, " ")
		if end == len(m.toks) {
			m.expect(end, ringExpect{src: srcTopics, topic: typed})
		}
		push(Arg{Kind: ArgText, Text: typed})
		return end, true
	case kFields:
		return m.parseFields(pos, push)
	case kChanges:
		return m.parseChanges(pos, push)
	case kDatabaseOptions:
		return m.parseDatabaseOptions(pos, push)
	case kDoc:
		return m.parseDocArg(element, pos, push)
	case kValues:
		return m.parseValues(pos, push)
	}
	return 0, false
}

func ringKindHint(element ringElem) string {
	switch element.arg {
	case kNewName:
		return "a name"
	case kDatabase:
		return "a database name"
	case kBucket:
		return "a bucket name"
	case kStoreField:
		return "a store field name"
	}
	return "a node id"
}

// ringSlotNumber reads a slot: decimal digits (a leading zero is still decimal, so 010 is ten, never
// octal), or 0x / 0X and hex digits. strconv's base 0 would also take 0b, 0o and underscores, which a
// destructive command must not guess at.
func ringSlotNumber(text string) (int64, error) {
	digits, base := text, 10
	if len(text) > 2 && (text[:2] == "0x" || text[:2] == "0X") {
		digits, base = text[2:], 16
	}
	for _, r := range digits {
		isDigit := r >= '0' && r <= '9'
		isHex := base == 16 && (r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F')
		if !isDigit && !isHex {
			return 0, errors.New("not a number")
		}
	}
	return strconv.ParseInt(digits, base, 64)
}

// ringLastSlot is the last of the 8192 slots a database is divided into.
const ringLastSlot = 8191

func ringHexOK(text string) bool {
	if text == "" {
		return false
	}
	for _, r := range text {
		if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
			return false
		}
	}
	return true
}

// parseAddr reads host:port from tokens with no space between them.
func (m *ringMatcher) parseAddr(pos int, push func(Arg)) (int, bool) {
	end := pos
	for end < len(m.toks) {
		t := m.toks[end]
		if end > pos && t.start != m.toks[end-1].end {
			break
		}
		if t.kind == tokString || (t.kind == tokPunct && t.text != "." && t.text != ":") {
			break
		}
		end++
	}
	if end == pos {
		m.expect(pos, ringExpect{hint: "an address such as 127.0.0.1:4443"})
		return 0, false
	}
	var text strings.Builder
	for _, t := range m.toks[pos:end] {
		text.WriteString(t.text)
	}
	host, port, found := strings.Cut(text.String(), ":")
	portNum, err := strconv.Atoi(port)
	if !found || host == "" || err != nil || portNum < 1 || portNum > 65535 || strings.Contains(port, ":") {
		m.refuse(pos, fmt.Sprintf("%q is not an address: write host:port, for example 127.0.0.1:4443", text.String()))
		m.expect(pos, ringExpect{hint: "an address such as 127.0.0.1:4443"})
		return 0, false
	}
	push(Arg{Kind: ArgText, Text: text.String(), Span: [2]int{m.toks[pos].start, m.toks[end-1].end}})
	return end, true
}

// typeCheck validates a declared type word. It returns the canonical word, or a
// message that names the canonical spelling.
func ringTypeCheck(word string) (string, string) {
	for _, canonical := range ringTypeWords() {
		if word == canonical {
			return canonical, ""
		}
	}
	if canonical, ok := ringSynonyms[strings.ToLower(word)]; ok {
		return "", fmt.Sprintf("%q is not a type word here: the catalog records one spelling, and it is %q", word, canonical)
	}
	for _, canonical := range ringTypeWords() {
		if strings.EqualFold(word, canonical) {
			return "", fmt.Sprintf("%q is not a type word: type words are spelled exactly, so write %q", word, canonical)
		}
	}
	return "", fmt.Sprintf("%q is not a type: write one of %s, or a store: %s", word,
		strings.Join(ringScalarWords, ", "), strings.Join(ringStoreWords, ", "))
}

// parseType reads the type word of a field at token pos.
func (m *ringMatcher) parseType(pos int, field string) (string, bool) {
	if pos >= len(m.toks) || m.toks[pos].kind != tokWord {
		if pos < len(m.toks) {
			m.refuse(pos, m.noTypeMessage(pos, field))
		}
		m.expect(pos, m.litExpect(ringTypeWords()...)...)
		return "", false
	}
	canonical, problem := ringTypeCheck(m.toks[pos].text)
	if problem != "" {
		m.refuse(pos, problem+fmt.Sprintf(" ('%s %s')", field, ringSuggestion(m.toks[pos].text)))
		m.expect(pos, m.litExpect(ringTypeWords()...)...)
		return "", false
	}
	if pos+1 < len(m.toks) && (m.toks[pos+1].text == "{" || m.toks[pos+1].text == "<") && m.toks[pos+1].kind == tokPunct {
		m.refuse(pos+1, fmt.Sprintf("a type is one word: %q takes no { members } or <T> in this release. Write %q alone, or %q to accept every value", canonical, canonical, "any"))
		return "", false
	}
	return canonical, true
}

// ringSuggestion is the canonical spelling of a refused type word, or string when there is none.
func ringSuggestion(word string) string {
	if canonical, ok := ringSynonyms[strings.ToLower(word)]; ok {
		return canonical
	}
	for _, canonical := range ringTypeWords() {
		if strings.EqualFold(word, canonical) {
			return canonical
		}
	}
	return "string"
}

func ringOr(text, fallback string) string {
	if text == "" {
		return fallback
	}
	return text
}

func (m *ringMatcher) noTypeMessage(pos int, field string) string {
	found := m.toks[pos]
	if found.text == ":" && field == "type" {
		return "a bucket has no type: declare its fields, as in 'create bucket " + ringOr(m.argText("bucket"), "User") + " { Name string }'"
	}
	if found.text == ":" {
		return fmt.Sprintf("a field is written 'Name string', with no colon: write '%s string' ('%s: string' is a document, not a declaration)", field, field)
	}
	return fmt.Sprintf("field %q has no type: write it as '%s string', or use a store such as fifoStore. Types: %s, or %s",
		field, field, strings.Join(ringScalarWords, ", "), strings.Join(ringStoreWords, ", "))
}

// ringIndexCreateFix and ringIndexAlterFix are the statements an index refusal quotes. The grammar
// tests parse each one, so a refusal never names a fix the parser refuses.
const (
	ringIndexCreateFix = "create bucket Post { Author string, Ts datetime, index ByAuthor (Author, Ts desc) }"
	ringIndexAlterFix  = "alter bucket Post { add index ByAuthor (Author, Ts desc) }"
)

// ringUnbuiltNote follows every quoted index statement in a parse refusal: the grammar reads
// indexes so a statement can be printed back, but running one is refused, so the quote shows the
// syntax and is not a statement to run. Aggregates are built and carry no note.
const ringUnbuiltNote = " (the syntax is read, but this build does not build indexes yet, so running it is refused)"

// ringIndexRefusal names the problem with an index entry and the syntax that reads.
func ringIndexRefusal(problem, fix string) string {
	return problem + ": the syntax is '" + fix + "'" + ringUnbuiltNote
}

func (m *ringMatcher) wordAt(pos int, word string) bool {
	return pos < len(m.toks) && m.toks[pos].kind == tokWord && m.toks[pos].text == word
}

func (m *ringMatcher) nameAt(pos int) bool {
	return pos < len(m.toks) && m.toks[pos].kind == tokWord && ringNameOK(m.toks[pos].text)
}

func (m *ringMatcher) punctAt(pos int, text string) bool {
	return pos < len(m.toks) && m.toks[pos].kind == tokPunct && m.toks[pos].text == text
}

const (
	ringExpireCreateFix = "create bucket Session { UserId objectid, ExpiresAt datetime, expire at ExpiresAt }"
	ringExpireStoreFix  = `create bucket Device { Name string, Log fifoStore expire at "ts" }`
	ringExpireAlterFix  = "alter bucket Session { set expire at ExpiresAt }"
	// ringExpireNote follows a quoted expire statement in a parse refusal; empty now that the server builds expiry.
	ringExpireNote = ""
)

// startsExpire says whether the entry at token pos is the bucket clause 'expire at <Field>'. A
// field named expire keeps parsing, as in 'expire string': there the second word is a type.
func (m *ringMatcher) startsExpire(pos int) bool {
	return m.wordAt(pos, "expire") && m.wordAt(pos+1, "at")
}

// dropsExpire says whether the entry at token pos, after 'drop', is 'expire at': the clause that
// removes a bucket's expiry. 'drop expire' before a comma or the brace drops a field named expire,
// as 'drop index' before them drops a field named index.
func (m *ringMatcher) dropsExpire(pos int) bool {
	return m.wordAt(pos, "expire") && m.wordAt(pos+1, "at")
}

// parseExpireField reads 'expire at <Field>' at token pos, the word expire, for a create or an
// alter's 'set'. fix is the statement the refusals quote.
func (m *ringMatcher) parseExpireField(pos int, fix string) (string, int, bool) {
	if !m.nameAt(pos + 2) {
		m.refuse(pos+2, "expire at names a datetime field of the bucket: the syntax is '"+fix+"'"+ringExpireNote)
		return "", 0, false
	}
	return m.toks[pos+2].text, pos + 3, true
}

// parseStoreExpire reads 'expire at "<path>"' after a store field's type word, at token pos.
func (m *ringMatcher) parseStoreExpire(pos int, field FieldDecl) (string, int, bool) {
	if field.Type != "fifoStore" && field.Type != "lifoStore" {
		m.refuse(pos, fmt.Sprintf("field %q is a %s, and expire at \"<path>\" is for fifoStore and lifoStore in this release: the syntax is '%s'%s",
			field.Name, field.Type, ringExpireStoreFix, ringExpireNote))
		return "", 0, false
	}
	if pos+2 >= len(m.toks) || m.toks[pos+1].text != "at" || m.toks[pos+2].kind != tokString {
		m.refuse(pos, "expire at takes a quoted path inside the element: the syntax is '"+ringExpireStoreFix+"'"+ringExpireNote)
		return "", 0, false
	}
	path := m.toks[pos+2].text
	if path == "" || len(path) > 255 || strings.Count(path, ".") >= 3 {
		m.refuse(pos+2, "the expire path is one to three dot-separated keys, at most 3 deep: the syntax is '"+ringExpireStoreFix+"'"+ringExpireNote)
		return "", 0, false
	}
	return path, pos + 3, true
}

// startsIndex says whether the entry at token pos is an index: the word index, then a name, then
// '(' or a name that is no type word. A field named index keeps parsing, as in 'index string'.
func (m *ringMatcher) startsIndex(pos int) bool {
	if !m.wordAt(pos, "index") || !m.nameAt(pos+1) {
		return false
	}
	if m.punctAt(pos+2, "(") {
		return true
	}
	_, problem := ringTypeCheck(m.toks[pos+1].text)
	return problem != ""
}

// dropsIndex says whether the entry at token pos is 'drop index <Name>'. A field named index is
// dropped as 'drop index' followed by a comma or the closing brace, which is not a name.
func (m *ringMatcher) dropsIndex(pos int) bool {
	return m.wordAt(pos, "index") && m.nameAt(pos+1)
}

// parseIndexDecl reads index <Name> ( <Field> [desc], ... ) at token pos, the word index. fix is the
// statement the refusals quote.
func (m *ringMatcher) parseIndexDecl(pos int, fix string) (IndexDecl, int, bool) {
	decl := IndexDecl{Name: m.toks[pos+1].text}
	pos += 2
	if !m.punctAt(pos, "(") {
		m.refuse(pos, ringIndexRefusal("an index names its fields in parentheses, one to four", fix))
		return IndexDecl{}, 0, false
	}
	pos++
	for n := 1; ; n++ {
		if !m.nameAt(pos) {
			problem := "an index lists one to four fields"
			if n > 1 {
				problem = "a comma must be followed by a field of the index"
			}
			m.refuse(pos, ringIndexRefusal(problem, fix))
			return IndexDecl{}, 0, false
		}
		component := IndexComponentDecl{Field: m.toks[pos].text}
		pos++
		m.expect(pos, ringExpect{lit: "desc"}, ringExpect{lit: ","}, ringExpect{lit: ")"})
		if m.wordAt(pos, "desc") {
			component.Desc = true
			pos++
		} else if pos < len(m.toks) && m.toks[pos].kind == tokWord {
			m.refuse(pos, ringIndexRefusal(fmt.Sprintf("after the index field %q write desc or nothing, as ascending is the default", component.Field), fix))
			return IndexDecl{}, 0, false
		}
		decl.Components = append(decl.Components, component)
		if m.punctAt(pos, ",") {
			if n == 4 && m.nameAt(pos+1) {
				m.refuse(pos+1, ringIndexRefusal("an index has at most 4 fields", fix))
				return IndexDecl{}, 0, false
			}
			pos++
			continue
		}
		if m.punctAt(pos, ")") {
			return decl, pos + 1, true
		}
		m.refuse(pos, ringIndexRefusal("close the index's field list with ')'", fix))
		return IndexDecl{}, 0, false
	}
}

// parseFields reads { Name type, ... } of a create bucket.
func (m *ringMatcher) parseFields(pos int, push func(Arg)) (int, bool) {
	if pos >= len(m.toks) || m.toks[pos].text != "{" || m.toks[pos].kind != tokPunct {
		m.expect(pos, ringExpect{lit: "{"})
		return 0, false
	}
	open := m.toks[pos]
	var fields []FieldDecl
	var indexes []IndexDecl
	expireField := ""
	pos++
	for first := true; ; first = false {
		if first && pos < len(m.toks) && m.toks[pos].text == "}" && m.toks[pos].kind == tokPunct {
			break
		}
		if m.startsExpire(pos) {
			if expireField != "" {
				m.refuse(pos, "a bucket has one expire clause: the syntax is '"+ringExpireCreateFix+"'"+ringExpireNote)
				return 0, false
			}
			name, end, ok := m.parseExpireField(pos, ringExpireCreateFix)
			if !ok {
				return 0, false
			}
			expireField, pos = name, end
			if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != "}") {
				m.expect(pos, ringExpect{lit: ","}, ringExpect{lit: "}"})
				return 0, false
			}
			if m.toks[pos].text == "}" {
				break
			}
			pos++
			continue
		}
		if pos >= len(m.toks) || m.toks[pos].kind != tokWord || !ringNameOK(m.toks[pos].text) {
			if pos < len(m.toks) && !first && m.toks[pos].text == "}" {
				m.refuse(pos, "a comma must be followed by another field: remove the comma, or add '<Field> <type>'")
			}
			m.expect(pos, ringExpect{lit: "index"}, ringExpect{hint: "a field name"})
			if first {
				m.expect(pos, ringExpect{lit: "}"})
			}
			return 0, false
		}
		var after []ringExpect
		wasIndex := m.startsIndex(pos)
		if wasIndex {
			decl, end, ok := m.parseIndexDecl(pos, ringIndexCreateFix)
			if !ok {
				return 0, false
			}
			indexes = append(indexes, decl)
			pos = end
			after = ringAfterField(FieldDecl{})
		} else {
			name := m.toks[pos].text
			typeWord, ok := m.parseType(pos+1, name)
			if !ok {
				return 0, false
			}
			field := FieldDecl{Name: name, Type: typeWord}
			pos += 2
			if m.wordAt(pos, "of") {
				of, end, ok := m.parseOf(pos, name, typeWord)
				if !ok {
					return 0, false
				}
				field.Of, pos = of, end
			}
			if pos < len(m.toks) && m.toks[pos].kind == tokWord && strings.EqualFold(m.toks[pos].text, "with") {
				aggregates, end, ok := m.parseWith(pos, field)
				if !ok {
					return 0, false
				}
				field.Aggregates, pos = aggregates, end
			}
			if pos < len(m.toks) && m.toks[pos].kind == tokWord && m.toks[pos].text == "expire" {
				path, end, ok := m.parseStoreExpire(pos, field)
				if !ok {
					return 0, false
				}
				field.ExpirePath, pos = path, end
			}
			fields = append(fields, field)
			after = ringAfterField(field)
		}
		if wasIndex && m.wordAt(pos, "with") {
			m.refuse(pos, "'with' goes after a store field, not after an index entry: the syntax is 'create bucket Ledger { Lines hashStore with (sum Total), index ByLine (Lines) }'"+ringUnbuiltNote)
		}
		if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != "}") {
			m.expect(pos, after...)
			return 0, false
		}
		if m.toks[pos].text == "}" {
			break
		}
		pos++
	}
	push(Arg{Kind: ArgFields, Fields: fields, Indexes: indexes, ExpireField: expireField, Span: [2]int{open.end, m.toks[pos].start}})
	return pos + 1, true
}

// ringStorageKindText is cluster.ParseStorageWord's sentence for an unknown word: non-test cli code
// does not import hoardDB/cluster, so the sentence is written here and must be kept the same.
const ringStorageKindText = "storage %q is not a kind. Use storage: disk"

// parseDatabaseOptions reads { key: value, ... } at token pos: replication_factor: <n> and
// storage: disk (memory parses so the create refuses it with its own text), each at most once, in either order, with at least one. It appends
// Arg{Name: "rf"} and Arg{Name: "storage"} to m.args itself, because push would rename both to
// the element's name; ringMatcher.seq rolls m.args back when the statement fails.
func (m *ringMatcher) parseDatabaseOptions(pos int, _ func(Arg)) (int, bool) {
	if pos >= len(m.toks) || m.toks[pos].text != "{" || m.toks[pos].kind != tokPunct {
		if pos < len(m.toks) {
			m.refuse(pos, fmt.Sprintf("found %q where the database options belong. Write options in braces: { replication_factor: <n>, storage: disk }, for example '%s app { replication_factor: 2 }'",
				m.toks[pos].text, m.form.Verb))
		}
		m.expect(pos, ringExpect{lit: "{"})
		return 0, false
	}
	pos++
	var rf, storage *Arg
	keyExpects := func() []ringExpect {
		var exps []ringExpect
		if rf == nil {
			exps = append(exps, ringExpect{lit: "replication_factor"})
		}
		if storage == nil {
			exps = append(exps, ringExpect{lit: "storage"})
		}
		return exps
	}
	for {
		if pos >= len(m.toks) || m.toks[pos].kind != tokWord {
			m.expect(pos, keyExpects()...)
			return 0, false
		}
		key := m.toks[pos]
		switch {
		case key.text != "replication_factor" && key.text != "storage":
			m.refuse(pos, fmt.Sprintf("database option %q is not one of replication_factor, storage. Write { replication_factor: <n>, storage: disk }", key.text))
			m.expect(pos, keyExpects()...)
			return 0, false
		case (key.text == "replication_factor" && rf != nil) || (key.text == "storage" && storage != nil):
			m.refuse(pos, fmt.Sprintf("database option %q is given twice. Give each once", key.text))
			m.expect(pos, keyExpects()...)
			return 0, false
		}
		pos++
		if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || m.toks[pos].text != ":" {
			m.expect(pos, ringExpect{lit: ":"})
			return 0, false
		}
		pos++
		if key.text == "replication_factor" {
			if pos >= len(m.toks) || m.toks[pos].kind != tokWord {
				m.expect(pos, ringExpect{hint: "a number"})
				return 0, false
			}
			n, err := strconv.ParseInt(m.toks[pos].text, 10, 64)
			if err != nil || n < 0 {
				m.refuse(pos, fmt.Sprintf("%q is not a number: write a whole number such as 10", m.toks[pos].text))
				m.expect(pos, ringExpect{hint: "a number"})
				return 0, false
			}
			rf = &Arg{Name: "rf", Kind: ArgInt, Int: n}
		} else {
			if pos >= len(m.toks) || m.toks[pos].kind != tokWord {
				m.expect(pos, m.litExpect("disk")...)
				return 0, false
			}
			word := strings.ToLower(m.toks[pos].text)
			if word != "disk" && word != "memory" {
				m.refuse(pos, fmt.Sprintf(ringStorageKindText, m.toks[pos].text))
				m.expect(pos, m.litExpect("disk")...)
				return 0, false
			}
			storage = &Arg{Name: "storage", Kind: ArgText, Text: word}
		}
		pos++
		if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != "}") {
			m.expect(pos, ringExpect{lit: ","}, ringExpect{lit: "}"})
			return 0, false
		}
		if m.toks[pos].text == "}" {
			break
		}
		pos++
	}
	if rf != nil {
		m.args = append(m.args, *rf)
	}
	if storage != nil {
		m.args = append(m.args, *storage)
	}
	return pos + 1, true
}

// ringMaxAggregates is how many aggregates one store field may declare.
const ringMaxAggregates = 8

// ringMaxPath is how many names a field path may have, dotted.
const ringMaxPath = 3

// ringIsStoreWord reports whether word is one of the six store words.
func ringIsStoreWord(word string) bool { return slices.Contains(ringStoreWords, word) }

// parseOf reads `of <Bucket>` or `of <Database>.<Bucket>` after the type word of a field, which
// starts at token pos. It returns the target as written and the position after it.
func (m *ringMatcher) parseOf(pos int, field, typeWord string) (string, int, bool) {
	pos++
	if pos >= len(m.toks) || m.toks[pos].kind != tokWord || !ringNameOK(m.toks[pos].text) {
		m.expect(pos, ringExpect{hint: "the bucket the ids belong to, for example '" + field + " " + typeWord + " of User'"})
		return "", 0, false
	}
	target := m.toks[pos].text
	pos++
	if pos < len(m.toks) && m.toks[pos].kind == tokPunct && m.toks[pos].text == "." {
		pos++
		if pos >= len(m.toks) || m.toks[pos].kind != tokWord || !ringNameOK(m.toks[pos].text) {
			m.expect(pos, ringExpect{hint: "a bucket name after the database, for example 'of ref.User'"})
			return "", 0, false
		}
		target += "." + m.toks[pos].text
		pos++
	}
	return target, pos, true
}

// ringAfterField lists what may follow a declared field: a comma or the closing brace, and the with
// clause while a store field has not written one yet.
func ringAfterField(field FieldDecl) []ringExpect {
	exps := []ringExpect{{lit: ","}, {lit: "}"}}
	if ringIsStoreWord(field.Type) && field.Type != "blobStore" && field.Aggregates == nil {
		exps = append(exps, ringExpect{lit: "with"})
	}
	if (field.Type == "fifoStore" || field.Type == "lifoStore") && field.ExpirePath == "" && field.Aggregates == nil {
		exps = append(exps, ringExpect{lit: "expire"})
	}
	return exps
}

// ringAggregateText renders one aggregate the way the grammar writes it.
func ringAggregateText(a AggregateDecl) string {
	switch a.Kind {
	case "sum by":
		return "sum " + a.Field + " by " + a.By
	case "count by":
		return "count by " + a.By
	}
	return "sum " + a.Field
}

// parseWith reads the with (...) clause after the type word of a store field, which starts at token
// pos. A scalar, a blobStore and a malformed list are refused, each with a quoted create form.
func (m *ringMatcher) parseWith(pos int, field FieldDecl) ([]AggregateDecl, int, bool) {
	if !ringIsStoreWord(field.Type) {
		m.refuse(pos, fmt.Sprintf("'with' goes after a store field, and %s is a scalar (%s): only the stores %s take aggregates. The syntax is 'create bucket Ledger { Lines hashStore with (sum Total) }'",
			field.Name, field.Type, strings.Join(ringStoreWords, ", ")))
		return nil, 0, false
	}
	if field.Type == "blobStore" {
		m.refuse(pos, "a blob element has no fields, so a blobStore takes no 'with' clause: write 'create bucket Ledger { Data blobStore }'")
		return nil, 0, false
	}
	pos++
	if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || m.toks[pos].text != "(" {
		m.expect(pos, ringExpect{lit: "("})
		m.refuse(pos, "'with' needs its aggregates in parentheses: the syntax is 'create bucket Ledger { Lines hashStore with (sum Total) }'")
		return nil, 0, false
	}
	pos++
	if pos < len(m.toks) && m.toks[pos].kind == tokPunct && m.toks[pos].text == ")" {
		m.expect(pos, ringExpect{hint: "an aggregate such as sum Total"})
		m.refuse(pos, "the aggregate list is empty: name at least one, as in 'create bucket Ledger { Lines hashStore with (sum Total) }'")
		return nil, 0, false
	}
	var list []AggregateDecl
	for {
		start := pos
		aggregate, end, ok := m.parseAggregate(pos)
		if !ok {
			return nil, 0, false
		}
		if slices.Contains(list, aggregate) {
			m.refuse(start, fmt.Sprintf("AGGREGATE_DUPLICATE: %q is listed twice on %s: write each aggregate once, as in 'create bucket Ledger { Lines hashStore with (sum Total, count by Category) }'",
				ringAggregateText(aggregate), field.Name))
			return nil, 0, false
		}
		list = append(list, aggregate)
		pos = end
		if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != ")") {
			m.expect(pos, ringExpect{lit: ","}, ringExpect{lit: ")"})
			m.refuse(pos, "the aggregate list is not closed: write ')' after the last aggregate, as in 'create bucket Ledger { Lines hashStore with (sum Total) }'")
			return nil, 0, false
		}
		if m.toks[pos].text == ")" {
			return list, pos + 1, true
		}
		if len(list) == ringMaxAggregates {
			m.refuse(pos+1, "a store takes at most 8 aggregates, and this is the ninth: remove one, or put the rest on another store, as in 'create bucket Ledger { Lines hashStore with (sum Total, count by Category) }'")
			return nil, 0, false
		}
		pos++
	}
}

// parseAggregate reads one of sum Path, sum Path by Path and count by Path at token pos.
func (m *ringMatcher) parseAggregate(pos int) (AggregateDecl, int, bool) {
	if pos >= len(m.toks) || m.toks[pos].kind != tokWord {
		m.expect(pos, ringExpect{lit: "sum"}, ringExpect{lit: "count"})
		return AggregateDecl{}, 0, false
	}
	word := m.toks[pos].text
	switch {
	case strings.EqualFold(word, "sum"):
		field, end, ok := m.parsePath(pos+1, "sum")
		if !ok {
			return AggregateDecl{}, 0, false
		}
		if end < len(m.toks) && m.toks[end].kind == tokWord && strings.EqualFold(m.toks[end].text, "by") {
			by, next, ok := m.parsePath(end+1, "by")
			if !ok {
				return AggregateDecl{}, 0, false
			}
			return AggregateDecl{Kind: "sum by", Field: field, By: by}, next, true
		}
		return AggregateDecl{Kind: "sum", Field: field}, end, true
	case strings.EqualFold(word, "count"):
		if pos+1 >= len(m.toks) || m.toks[pos+1].kind != tokWord || !strings.EqualFold(m.toks[pos+1].text, "by") {
			m.refuse(pos, "a plain count is not an aggregate: every store already counts its elements: 'db.Ledger[\"ada\"].Lines.count()'")
			return AggregateDecl{}, 0, false
		}
		by, end, ok := m.parsePath(pos+2, "by")
		if !ok {
			return AggregateDecl{}, 0, false
		}
		return AggregateDecl{Kind: "count by", By: by}, end, true
	}
	m.expect(pos, ringExpect{lit: "sum"}, ringExpect{lit: "count"})
	return AggregateDecl{}, 0, false
}

// parsePath reads a field path after keyword: names of letters, digits and _ with dots between them,
// at most ringMaxPath names. It returns the dotted path and the token after it.
func (m *ringMatcher) parsePath(pos int, keyword string) (string, int, bool) {
	const example = "as in 'create bucket Ledger { Lines hashStore with (sum Total by Category) }'"
	var names []string
	for {
		if pos >= len(m.toks) || m.toks[pos].kind != tokWord {
			m.expect(pos, ringExpect{hint: "a field name"})
			m.refuse(pos, fmt.Sprintf("%s needs a field path after it, %s", keyword, example))
			return "", 0, false
		}
		name := m.toks[pos].text
		if !ringPathNameOK(name) {
			m.refuse(pos, fmt.Sprintf("%q is not a field path: a path is letters, digits and _, with dots between names, %s", name, example))
			return "", 0, false
		}
		names = append(names, name)
		pos++
		if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || m.toks[pos].text != "." || m.toks[pos].start != m.toks[pos-1].end {
			return strings.Join(names, "."), pos, true
		}
		if len(names) == ringMaxPath {
			m.refuse(pos+1, fmt.Sprintf("a field path is at most %d names deep, and this one has more: shorten it, %s", ringMaxPath, example))
			return "", 0, false
		}
		pos++
	}
}

// ringPathNameOK reports whether text is one name of a field path: letters, digits and _.
func ringPathNameOK(text string) bool {
	if text == "" {
		return false
	}
	for _, r := range text {
		if !ringWordRune(r) || r == '-' || r == '$' {
			return false
		}
	}
	return true
}

// parseChanges reads { add Name type, drop Name } of an alter bucket.
func (m *ringMatcher) parseChanges(pos int, push func(Arg)) (int, bool) {
	if pos >= len(m.toks) || m.toks[pos].text != "{" || m.toks[pos].kind != tokPunct {
		m.expect(pos, ringExpect{lit: "{"})
		return 0, false
	}
	open := m.toks[pos]
	var changes []FieldChange
	var indexes []IndexDecl
	var dropped []string
	expireField, dropExpire := "", false
	pos++
	for {
		if pos >= len(m.toks) || m.toks[pos].kind != tokWord || (!strings.EqualFold(m.toks[pos].text, "add") && !strings.EqualFold(m.toks[pos].text, "drop") && !strings.EqualFold(m.toks[pos].text, "set")) {
			m.expect(pos, ringExpect{lit: "add"}, ringExpect{lit: "drop"}, ringExpect{lit: "set"})
			return 0, false
		}
		if strings.EqualFold(m.toks[pos].text, "set") {
			pos++
			if !m.startsExpire(pos) {
				m.refuse(pos, "'set' in an alter bucket sets the expiry: the syntax is '"+ringExpireAlterFix+"'"+ringExpireNote)
				return 0, false
			}
			name, end, ok := m.parseExpireField(pos, ringExpireAlterFix)
			if !ok {
				return 0, false
			}
			expireField, pos = name, end
			if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != "}") {
				m.expect(pos, ringExpect{lit: ","}, ringExpect{lit: "}"})
				return 0, false
			}
			if m.toks[pos].text == "}" {
				break
			}
			pos++
			continue
		}
		add := strings.EqualFold(m.toks[pos].text, "add")
		pos++
		if !add && m.dropsExpire(pos) {
			dropExpire = true
			pos += 2
			if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != "}") {
				m.expect(pos, ringExpect{lit: ","}, ringExpect{lit: "}"})
				return 0, false
			}
			if m.toks[pos].text == "}" {
				break
			}
			pos++
			continue
		}
		if add && m.startsIndex(pos) {
			decl, end, ok := m.parseIndexDecl(pos, ringIndexAlterFix)
			if !ok {
				return 0, false
			}
			indexes = append(indexes, decl)
			pos = end
		} else if !add && m.dropsIndex(pos) {
			dropped = append(dropped, m.toks[pos+1].text)
			pos += 2
		} else {
			if pos >= len(m.toks) || m.toks[pos].kind != tokWord || !ringNameOK(m.toks[pos].text) {
				source := ringExpect{hint: "a field name"}
				if !add {
					source = ringExpect{src: srcFields, hint: "a declared field name"}
				}
				m.expect(pos, ringExpect{lit: "index"}, source)
				return 0, false
			}
			change := FieldChange{Add: add, Name: m.toks[pos].text}
			pos++
			if add {
				typeWord, ok := m.parseType(pos, change.Name)
				if !ok {
					return 0, false
				}
				change.Type = typeWord
				pos++
				if m.wordAt(pos, "of") {
					target, _, ok := m.parseOf(pos, change.Name, typeWord)
					if !ok {
						return 0, false
					}
					m.refuse(pos, "'of' goes on create bucket: alter cannot declare a reference yet. Declare it when the bucket is created, with the field written create bucket <bucket> { "+change.Name+" "+typeWord+" of "+target+" }, or add the field without 'of' and name the bucket when you expand it: expand: "+strconv.Quote(target))
					return 0, false
				}
			}
			changes = append(changes, change)
		}
		if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != "}") {
			m.expect(pos, ringExpect{lit: ","}, ringExpect{lit: "}"})
			return 0, false
		}
		if m.toks[pos].text == "}" {
			break
		}
		pos++
	}
	push(Arg{Kind: ArgChanges, Changes: changes, Indexes: indexes, DropIndexes: dropped, ExpireField: expireField, DropExpire: dropExpire, Span: [2]int{open.end, m.toks[pos].start}})
	return pos + 1, true
}

// roleSpecAt reads <role> or <role>@<database> at token pos, the tokens touching
// one another, where the database may be * for every database. A bare role is
// kept as written; v1.0 reads it as <role>@*.
func (m *ringMatcher) roleSpecAt(pos int) (end int, text string, ok bool) {
	if pos >= len(m.toks) || m.toks[pos].kind != tokWord || !ringNameOK(m.toks[pos].text) {
		m.expect(pos, ringExpect{hint: "a role such as writer@app"})
		return 0, "", false
	}
	role := m.toks[pos]
	at := pos + 1
	if at >= len(m.toks) || m.toks[at].text != "@" || m.toks[at].kind != tokPunct || m.toks[at].start != role.end {
		m.expect(at, ringExpect{lit: "@"})
		return at, role.text, true
	}
	database := at + 1
	if database >= len(m.toks) || m.toks[database].start != m.toks[at].end || !ringRoleDatabaseOK(m.toks[database]) {
		m.expect(database, ringExpect{src: srcDatabases, hint: "a database name"}, ringExpect{lit: "*"})
		return 0, "", false
	}
	return database + 1, role.text + "@" + m.toks[database].text, true
}

// parseRoles reads [<role>@<db>, ...] at token pos; the list may be empty.
func (m *ringMatcher) parseRoles(pos int, push func(Arg)) (int, bool) {
	if pos >= len(m.toks) || m.toks[pos].text != "[" || m.toks[pos].kind != tokPunct {
		m.expect(pos, ringExpect{lit: "["})
		return 0, false
	}
	open := m.toks[pos]
	pos++
	var specs []string
	for first := true; ; first = false {
		if first && pos < len(m.toks) && m.toks[pos].text == "]" && m.toks[pos].kind == tokPunct {
			break
		}
		end, text, ok := m.roleSpecAt(pos)
		if !ok {
			return 0, false
		}
		specs = append(specs, text)
		pos = end
		if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != "]") {
			m.expect(pos, ringExpect{lit: ","}, ringExpect{lit: "]"})
			return 0, false
		}
		if m.toks[pos].text == "]" {
			break
		}
		pos++
	}
	push(Arg{Kind: ArgText, Text: strings.Join(specs, ", "), Span: [2]int{open.end, m.toks[pos].start}})
	return pos + 1, true
}

// ringRoleDatabaseOK reports whether the token names the database of a role: a name, or * for all of them.
func ringRoleDatabaseOK(tok ringTok) bool {
	if tok.kind == tokPunct {
		return tok.text == "*"
	}
	return tok.kind == tokWord && ringNameOK(tok.text)
}
