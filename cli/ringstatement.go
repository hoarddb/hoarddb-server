package cli

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Parse parses one ring statement. selected is the database 'use' chose, or
// empty; it fills the database of a statement that leaves out 'in <db>'. A
// refusal is a *ParseError that names the problem, its position and the
// statement that works.
func Parse(line, selected string) (Statement, error) {
	toks, err := ringLex(line)
	if err != nil {
		return Statement{}, err
	}
	for len(toks) > 0 && toks[len(toks)-1].kind == tokPunct && toks[len(toks)-1].text == ";" {
		toks = toks[:len(toks)-1]
	}
	for i, tok := range toks {
		if tok.kind == tokPunct && tok.text == ";" {
			pos := ringCharPos(line, toks[i].start)
			if i+1 < len(toks) {
				pos = ringCharPos(line, toks[i+1].start)
			}
			return Statement{}, &ParseError{Pos: pos, Message: fmt.Sprintf(
				"a line holds one statement, but a second starts at character %d. Put each statement on its own line", pos)}
		}
	}
	if len(toks) == 0 {
		return Statement{}, &ParseError{Message: "the statement is empty. Run 'help' to list the statements, for example 'show databases'"}
	}
	if message, pos, ok := ringRetiredSpelling(line, toks); ok {
		return Statement{}, &ParseError{Pos: pos, Message: message}
	}
	m := &ringMatcher{line: line, toks: toks, selected: selected}
	for i := range ringForms {
		form := &ringForms[i]
		m.form = form
		m.args = m.args[:0]
		end := func(pos int) bool {
			if pos == len(toks) {
				return true
			}
			m.expect(pos, ringExpect{hint: "the end of the statement"})
			return false
		}
		if m.seq(form.Pattern, 0, end) {
			return ringFinish(m, form)
		}
	}
	return Statement{}, m.parseError()
}

// ringFinish turns a matched form into a Statement, filling the database.
func ringFinish(m *ringMatcher, form *ringForm) (Statement, error) {
	st := Statement{Verb: form.Verb, Args: append([]Arg(nil), m.args...)}
	if !form.NeedsDB {
		return st, nil
	}
	if _, ok := st.Arg("database"); ok {
		return st, nil
	}
	if m.selected == "" {
		return Statement{}, &ParseError{Message: ringNeedsDatabase(form, m)}
	}
	st.Args = append(st.Args, Arg{Name: "database", Kind: ArgText, Text: m.selected, Implied: true})
	return st, nil
}

func ringNeedsDatabase(form *ringForm, m *ringMatcher) string {
	text := strings.TrimSpace(m.line)
	hasIn := false
	ringWalk(form.Pattern, func(element ringElem) {
		if element.kind == elemArg && element.name == "database" {
			hasIn = true
		}
	})
	if hasIn {
		return fmt.Sprintf("%q needs a database: add 'in <db>', as in 'show buckets in app', or run 'use <db>' first and leave it out", text)
	}
	return fmt.Sprintf("%q needs a database, and none is selected: run 'use <db>' first ('show databases' lists them, 'create database <name>' makes one)", text)
}

// ringRetiredSpelling catches db.<bucket>.find(...) and db.<bucket>.get("id"),
// which queries 1 retires: one surface, one spelling.
func ringRetiredSpelling(line string, toks []ringTok) (string, int, bool) {
	if len(toks) < 6 || !strings.EqualFold(toks[0].text, "db") || toks[1].text != "." || toks[3].text != "." || toks[2].kind != tokWord {
		return "", 0, false
	}
	bucket := toks[2].text
	switch toks[4].text {
	case "find":
		return fmt.Sprintf("find is not a statement: reading one document has one spelling. Run 'db.%s[\"<id>\"].get()' to read a document by id, or 'db.%s.list({limit: 10})' to see what exists", bucket, bucket),
			ringCharPos(line, toks[4].start), true
	case "get":
		if toks[5].text == "(" && len(toks) > 6 && toks[6].kind == tokString {
			return fmt.Sprintf("get(\"<id>\") is not a statement: the id goes in the brackets, so the placement key and the element key cannot disagree. Run 'db.%s[%s].get()'",
				bucket, strconv.Quote(toks[6].text)), ringCharPos(line, toks[4].start), true
		}
	}
	return "", 0, false
}

// parseError builds the refusal from the furthest failure.
func (m *ringMatcher) parseError() *ParseError {
	if m.farPos == 0 && m.farCustom == "" && len(m.toks) > 0 {
		first := m.toks[0]
		problem := fmt.Sprintf("%q is not a statement", first.text)
		refusal := &ParseError{Pos: ringCharPos(m.line, first.start), Problem: problem,
			Message: problem + ". Run 'help' to list them, for example 'show databases'"}
		if word, form := ringNearestStatement(first.text); word != "" {
			refusal.Suggestion, refusal.Example = word, ringHeadline(form)
		}
		return refusal
	}
	at := m.offset(m.farPos)
	pos := ringCharPos(m.line, at)
	form := m.farForm
	var problem string
	if m.farCustom != "" {
		pos = ringCharPos(m.line, m.farAt)
		problem = fmt.Sprintf("%s (character %d)", m.farCustom, pos)
	} else {
		found := "the end of the statement"
		if m.farPos < len(m.toks) {
			found = strconv.Quote(m.toks[m.farPos].text)
		}
		problem = fmt.Sprintf("cannot read the statement at character %d: found %s, expected %s", pos, found, ringExpectText(m.farExps))
	}
	refusal := &ParseError{Pos: pos, Problem: problem, Message: problem}
	if form != nil {
		refusal.Form, refusal.Example, refusal.Topic = form.Syntax(), ringHeadline(form), "help "+strings.Fields(form.Verb)[0]
		refusal.Message += fmt.Sprintf(". The form is '%s'. For example '%s'. Run '%s' for every form", refusal.Form, refusal.Example, refusal.Topic)
	}
	return refusal
}

// ringNearestStatement finds the first word of a runnable statement within two edits of the word typed,
// and the form that starts with it. Words of fewer than three characters suggest nothing.
func ringNearestStatement(typed string) (string, *ringForm) {
	typed = strings.ToLower(typed)
	if len([]rune(typed)) < 3 {
		return "", nil
	}
	best, bestDistance := "", 3
	var bestForm *ringForm
	for i := range ringForms {
		form := &ringForms[i]
		if form.Unavailable {
			continue
		}
		word := form.Pattern[0].text
		if distance := ringEditDistance(typed, word); distance < bestDistance && distance > 0 {
			best, bestDistance, bestForm = word, distance, form
		}
	}
	return best, bestForm
}

// ringEditDistance is the Levenshtein distance between two words.
func ringEditDistance(a, b string) int {
	left, right := []rune(a), []rune(b)
	previous := make([]int, len(right)+1)
	for j := range previous {
		previous[j] = j
	}
	for i := 1; i <= len(left); i++ {
		current := make([]int, len(right)+1)
		current[0] = i
		for j := 1; j <= len(right); j++ {
			cost := 1
			if left[i-1] == right[j-1] {
				cost = 0
			}
			current[j] = min(previous[j]+1, current[j-1]+1, previous[j-1]+cost)
		}
		previous = current
	}
	return previous[len(right)]
}

// ringHeadline is the form's first example, the one a parse error quotes and a topic page leads with.
func ringHeadline(form *ringForm) string { return form.Examples[0].Text }

func ringExpectText(exps []ringExpect) string {
	seen := map[string]bool{}
	var words []string
	for _, e := range exps {
		text := e.hint
		if e.lit != "" {
			text = strconv.Quote(e.lit)
		}
		if text != "" && !seen[text] {
			seen[text] = true
			words = append(words, text)
		}
	}
	sort.Strings(words)
	switch len(words) {
	case 0:
		return "the end of the statement"
	case 1:
		return words[0]
	}
	return strings.Join(words[:len(words)-1], ", ") + " or " + words[len(words)-1]
}

// Arg returns the named argument of the statement.
func (s Statement) Arg(name string) (Arg, bool) {
	for _, a := range s.Args {
		if a.Name == name {
			return a, true
		}
	}
	return Arg{}, false
}

// String renders the statement as text that parses back to the same statement.
func (s Statement) String() string {
	form, ok := ringFormByVerb(s.Verb)
	if !ok {
		return ""
	}
	return ringJoin(ringRender(form.Pattern, s))
}

func ringRender(pattern []ringElem, st Statement) []string {
	var out []string
	for _, element := range pattern {
		switch element.kind {
		case elemLit:
			out = append(out, element.text)
		case elemFlag:
			out = append(out, element.text)
		case elemOpt:
			if ringPresent(element.opt, st) {
				out = append(out, ringRender(element.opt, st)...)
			}
		case elemArg:
			if element.arg == kDatabaseOptions {
				if text := ringRenderDatabaseOptions(st); text != "" {
					out = append(out, text)
				}
			} else if a, ok := st.Arg(element.name); ok {
				out = append(out, ringRenderArg(element, a))
			}
		}
	}
	return out
}

// ringRenderDatabaseOptions writes the rf and storage arguments back as the one braced argument the
// parser read them from, in the order the parser's doc names them; "" when neither was given.
func ringRenderDatabaseOptions(st Statement) string {
	var parts []string
	if rf, ok := st.Arg("rf"); ok {
		parts = append(parts, "replication_factor: "+strconv.FormatInt(rf.Int, 10))
	}
	if storage, ok := st.Arg("storage"); ok {
		parts = append(parts, "storage: "+storage.Text)
	}
	if len(parts) == 0 {
		return ""
	}
	return "{ " + strings.Join(parts, ", ") + " }"
}

// ringPresent reports whether the statement carries an argument written inside the option.
func ringPresent(pattern []ringElem, st Statement) bool {
	present := false
	ringWalk(pattern, func(element ringElem) {
		if element.kind == elemArg && element.arg == kDatabaseOptions {
			if ringRenderDatabaseOptions(st) != "" {
				present = true
			}
		} else if element.kind == elemArg || element.kind == elemFlag {
			if a, ok := st.Arg(element.name); ok && !a.Implied {
				present = true
			}
		}
	})
	return present
}

func ringRenderArg(element ringElem, a Arg) string {
	switch a.Kind {
	case ArgInt:
		return strconv.FormatInt(a.Int, 10)
	case ArgFields:
		parts := make([]string, 0, len(a.Fields)+len(a.Indexes))
		for _, f := range a.Fields {
			text := f.Name + " " + f.Type
			if len(f.Aggregates) > 0 {
				aggregates := make([]string, len(f.Aggregates))
				for i, aggregate := range f.Aggregates {
					aggregates[i] = ringAggregateText(aggregate)
				}
				text += " with (" + strings.Join(aggregates, ", ") + ")"
			}
			parts = append(parts, text)
		}
		for _, index := range a.Indexes {
			parts = append(parts, ringIndexText(index))
		}
		if len(parts) == 0 {
			return "{}"
		}
		return "{ " + strings.Join(parts, ", ") + " }"
	case ArgChanges:
		parts := make([]string, 0, len(a.Changes)+len(a.Indexes)+len(a.DropIndexes))
		for _, c := range a.Changes {
			if c.Add {
				parts = append(parts, "add "+c.Name+" "+c.Type)
			} else {
				parts = append(parts, "drop "+c.Name)
			}
		}
		for _, index := range a.Indexes {
			parts = append(parts, "add "+ringIndexText(index))
		}
		for _, name := range a.DropIndexes {
			parts = append(parts, "drop index "+name)
		}
		return "{ " + strings.Join(parts, ", ") + " }"
	case ArgDoc:
		return ringRenderValue(a.Doc)
	case ArgValues:
		parts := make([]string, len(a.Values))
		for i, v := range a.Values {
			parts[i] = ringRenderValue(v)
		}
		return strings.Join(parts, ", ")
	}
	if element.arg == kRoles {
		return "[" + a.Text + "]"
	}
	if element.arg == kQuotedID || ((element.arg == kAnyID || element.arg == kUser) && !ringNameOK(a.Text)) {
		return strconv.Quote(a.Text)
	}
	return a.Text
}

// ringIndexText renders one index entry the way the grammar writes it.
func ringIndexText(index IndexDecl) string {
	fields := make([]string, len(index.Components))
	for i, component := range index.Components {
		fields[i] = component.Field
		if component.Desc {
			fields[i] += " desc"
		}
	}
	return "index " + index.Name + " (" + strings.Join(fields, ", ") + ")"
}
