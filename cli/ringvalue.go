package cli

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const ringValueHint = `a "string", a number, true, false, null, a { document } or an [ array ]`

// parseDocArg reads a document literal; completion offers the form's keys at
// the top-level key position.
func (m *ringMatcher) parseDocArg(element ringElem, pos int, push func(Arg)) (int, bool) {
	if pos >= len(m.toks) || m.toks[pos].text != "{" || m.toks[pos].kind != tokPunct {
		m.expect(pos, ringExpect{lit: "{"})
		return 0, false
	}
	open := m.toks[pos]
	doc, end, ok := m.parseDoc(pos, 1, element.keys)
	if !ok {
		return 0, false
	}
	push(Arg{Kind: ArgDoc, Doc: doc, Span: [2]int{open.end, m.toks[end-1].start}})
	return end, true
}

// parseValues reads one or more literals separated by commas.
func (m *ringMatcher) parseValues(pos int, push func(Arg)) (int, bool) {
	if pos >= len(m.toks) {
		m.expect(pos, ringExpect{hint: "a value: " + ringValueHint})
		return 0, false
	}
	start := m.toks[pos].start
	var values []Value
	for {
		parsed, end, ok := m.parseValue(pos, 1)
		if !ok {
			return 0, false
		}
		values = append(values, parsed)
		pos = end
		if pos < len(m.toks) && m.toks[pos].kind == tokPunct && m.toks[pos].text == "," {
			pos++
			continue
		}
		break
	}
	push(Arg{Kind: ArgValues, Values: values, Span: [2]int{start, m.toks[pos-1].end}})
	return pos, true
}

// parseDoc reads { key: value, ... } starting at the '{' at token pos. depth 1
// is the statement's own document, where keys is what completion offers.
func (m *ringMatcher) parseDoc(pos, depth int, keys []string) (Value, int, bool) {
	if depth > ringMaxDepth {
		m.refuse(pos, fmt.Sprintf("a document is nested more than %d levels deep; flatten it", ringMaxDepth))
		return Value{}, 0, false
	}
	doc := Value{Kind: ValueDoc}
	pos++
	for first := true; ; first = false {
		if first && pos < len(m.toks) && m.toks[pos].kind == tokPunct && m.toks[pos].text == "}" {
			return doc, pos + 1, true
		}
		if pos >= len(m.toks) || (m.toks[pos].kind != tokWord && m.toks[pos].kind != tokString) {
			if depth == 1 {
				m.expect(pos, m.litExpect(keys...)...)
				if first {
					m.expect(pos, ringExpect{lit: "}"})
				}
			}
			m.expect(pos, ringExpect{hint: "a key"})
			return Value{}, 0, false
		}
		key := m.toks[pos].text
		if pos+1 >= len(m.toks) || m.toks[pos+1].kind != tokPunct || m.toks[pos+1].text != ":" {
			m.expect(pos+1, ringExpect{lit: ":"})
			return Value{}, 0, false
		}
		entry, end, ok := m.parseValue(pos+2, depth)
		if !ok {
			return Value{}, 0, false
		}
		doc.Entries = append(doc.Entries, DocEntry{Key: key, Value: entry})
		pos = end
		if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != "}") {
			m.expect(pos, ringExpect{lit: ","}, ringExpect{lit: "}"})
			return Value{}, 0, false
		}
		if m.toks[pos].text == "}" {
			return doc, pos + 1, true
		}
		pos++
	}
}

// parseValue reads one literal at token pos.
func (m *ringMatcher) parseValue(pos, depth int) (Value, int, bool) {
	if pos >= len(m.toks) {
		m.expect(pos, ringExpect{hint: "a value: " + ringValueHint})
		return Value{}, 0, false
	}
	tok := m.toks[pos]
	switch {
	case tok.kind == tokString:
		return Value{Kind: ValueString, Text: tok.text}, pos + 1, true
	case tok.kind == tokPunct && tok.text == "{":
		doc, end, ok := m.parseDoc(pos, depth+1, nil)
		if !ok {
			return doc, end, false
		}
		binary, isBinary, why := ringBinaryLiteral(doc)
		if isBinary {
			return binary, end, true
		}
		if why != "" {
			m.refuse(pos, `{"$binary": ...} is a binary literal and takes {"$binary": {"base64": "<base64>", "subType": "00"}}; `+why)
			m.expect(pos, ringExpect{hint: "a value: " + ringValueHint})
			return Value{}, 0, false
		}
		return doc, end, true
	case tok.kind == tokPunct && tok.text == "[":
		return m.parseArray(pos, depth+1)
	case tok.kind == tokWord && tok.text == "true", tok.kind == tokWord && tok.text == "false":
		return Value{Kind: ValueBool, Bool: tok.text == "true"}, pos + 1, true
	case tok.kind == tokWord && tok.text == "null":
		return Value{Kind: ValueNull}, pos + 1, true
	case tok.kind == tokWord && ringLooksNumeric(tok.text):
		return m.parseNumber(pos)
	}
	m.refuse(pos, fmt.Sprintf("%q is not a value: write %s", tok.text, ringValueHint))
	m.expect(pos, ringExpect{hint: "a value: " + ringValueHint})
	return Value{}, 0, false
}

// ringBinaryLiteral reads a document whose only key is $binary. It returns the
// binary and true for a well-formed body. For a malformed body it returns false
// and the reason; for any other document it returns false and no reason.
func ringBinaryLiteral(doc Value) (Value, bool, string) {
	if len(doc.Entries) != 1 || doc.Entries[0].Key != "$binary" {
		return Value{}, false, ""
	}
	const needKeys = "it needs exactly the keys base64 and subType"
	body := doc.Entries[0].Value
	if body.Kind != ValueDoc || len(body.Entries) != 2 {
		return Value{}, false, needKeys
	}
	var encoded, subtype *Value
	for i := range body.Entries {
		switch body.Entries[i].Key {
		case "base64":
			encoded = &body.Entries[i].Value
		case "subType":
			subtype = &body.Entries[i].Value
		}
	}
	if encoded == nil || subtype == nil {
		return Value{}, false, needKeys
	}
	data, err := base64.StdEncoding.DecodeString(encoded.Text)
	if encoded.Kind != ValueString || err != nil {
		return Value{}, false, "base64 is not standard base64"
	}
	sub, err := hex.DecodeString(subtype.Text)
	if subtype.Kind != ValueString || err != nil || len(subtype.Text) != 2 {
		return Value{}, false, "subType is not two hex digits"
	}
	return Value{Kind: ValueBinary, Bytes: data, Subtype: sub[0]}, true, ""
}

func ringLooksNumeric(text string) bool {
	text = strings.TrimPrefix(text, "-")
	return text != "" && text[0] >= '0' && text[0] <= '9'
}

func (m *ringMatcher) parseArray(pos, depth int) (Value, int, bool) {
	if depth > ringMaxDepth {
		m.refuse(pos, fmt.Sprintf("an array is nested more than %d levels deep; flatten it", ringMaxDepth))
		return Value{}, 0, false
	}
	array := Value{Kind: ValueArray}
	pos++
	for first := true; ; first = false {
		if first && pos < len(m.toks) && m.toks[pos].kind == tokPunct && m.toks[pos].text == "]" {
			return array, pos + 1, true
		}
		element, end, ok := m.parseValue(pos, depth)
		if !ok {
			return Value{}, 0, false
		}
		array.Elems = append(array.Elems, element)
		pos = end
		if pos >= len(m.toks) || m.toks[pos].kind != tokPunct || (m.toks[pos].text != "," && m.toks[pos].text != "]") {
			m.expect(pos, ringExpect{lit: ","}, ringExpect{lit: "]"})
			return Value{}, 0, false
		}
		if m.toks[pos].text == "]" {
			return array, pos + 1, true
		}
		pos++
	}
}

// parseNumber reads an integer or a number with a fraction (two or three
// tokens, since '.' is punctuation).
func (m *ringMatcher) parseNumber(pos int) (Value, int, bool) {
	text, end := m.toks[pos].text, pos+1
	if end+1 < len(m.toks) && m.toks[end].text == "." && m.toks[end].kind == tokPunct &&
		m.toks[end].start == m.toks[pos].end && m.toks[end+1].kind == tokWord && m.toks[end+1].start == m.toks[end].end &&
		m.toks[end+1].text[0] >= '0' && m.toks[end+1].text[0] <= '9' {
		text += "." + m.toks[end+1].text
		end += 2
	}
	if !strings.ContainsAny(text, ".eExXpP") {
		if n, err := strconv.ParseInt(text, 10, 64); err == nil {
			return Value{Kind: ValueInt, Int: n}, end, true
		}
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
		reason := "is not a number"
		if errors.Is(err, strconv.ErrRange) {
			reason = "is out of range"
		}
		m.refuse(pos, fmt.Sprintf("%q %s: write a whole number or a decimal such as 36 or 1.5", text, reason))
		m.expect(pos, ringExpect{hint: "a number"})
		return Value{}, 0, false
	}
	return Value{Kind: ValueFloat, Float: f}, end, true
}

// ringRenderValue prints a literal so that it parses back to the same Value.
func ringRenderValue(v Value) string {
	switch v.Kind {
	case ValueString:
		return strconv.Quote(v.Text)
	case ValueInt:
		return strconv.FormatInt(v.Int, 10)
	case ValueFloat:
		text := strconv.FormatFloat(v.Float, 'f', -1, 64)
		if !strings.Contains(text, ".") {
			text += ".0"
		}
		return text
	case ValueBool:
		return strconv.FormatBool(v.Bool)
	case ValueNull:
		return "null"
	case ValueArray:
		parts := make([]string, len(v.Elems))
		for i, element := range v.Elems {
			parts[i] = ringRenderValue(element)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case ValueBinary:
		return `{"$binary": {"base64": "` + base64.StdEncoding.EncodeToString(v.Bytes) +
			`", "subType": "` + fmt.Sprintf("%02x", v.Subtype) + `"}}`
	}
	parts := make([]string, len(v.Entries))
	for i, entry := range v.Entries {
		key := entry.Key
		if !ringBareKey(key) {
			key = strconv.Quote(key)
		}
		parts[i] = key + ": " + ringRenderValue(entry.Value)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// ringBareKey reports whether a key lexes as one word token.
func ringBareKey(key string) bool {
	if key == "" {
		return false
	}
	for _, r := range key {
		if !ringWordRune(r) {
			return false
		}
	}
	return true
}
