package cli

import (
	"sort"
	"strings"
)

// Completions returns the words that can come next after prefix, the line up
// to the cursor, from the grammar alone: verbs, keywords, punctuation and type
// words. Database, bucket and field names need a NameSource (CompleteWith).
func Completions(prefix, selected string) []string {
	return CompleteWith(nil, prefix, selected)
}

// CompleteWith is Completions with the names a NameSource knows. If prefix ends
// in the middle of a word, the candidates are the words that word can grow
// into; otherwise they are the tokens that can follow it. Punctuation that
// binds to the previous word ('.', '[', '(' ...) is offered without a space.
func CompleteWith(src NameSource, prefix, selected string) []string {
	toks, err := ringLex(prefix)
	if err != nil {
		return nil
	}
	endsInSpace := prefix == "" || ringIsSpaceByte(prefix[len(prefix)-1])
	partial := !endsInSpace && len(toks) > 0 && toks[len(toks)-1].kind == tokWord
	seen := map[string]bool{}
	var out []string
	add := func(candidate, typed string) {
		if candidate != "" && !seen[candidate] && strings.HasPrefix(strings.ToLower(candidate), strings.ToLower(typed)) {
			seen[candidate] = true
			out = append(out, candidate)
		}
	}
	if partial {
		last := toks[len(toks)-1]
		for _, e := range ringExpectations(src, toks[:len(toks)-1], prefix, selected) {
			add(e, last.text)
		}
	}
	for _, e := range ringExpectations(src, toks, prefix, selected) {
		if endsInSpace || !partial || (len(e) == 1 && strings.Contains(ringPunct, e)) {
			add(e, "")
		}
	}
	sort.Strings(out)
	return out
}

func ringIsSpaceByte(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }

// ringExpectations runs the table over toks and lists what could come next.
func ringExpectations(src NameSource, toks []ringTok, line, selected string) []string {
	m := &ringMatcher{line: line, toks: toks, selected: selected}
	for i := range ringForms {
		form := &ringForms[i]
		if form.Unavailable {
			continue
		}
		m.form = form
		m.args = m.args[:0]
		m.seq(form.Pattern, 0, func(pos int) bool {
			if pos == len(toks) {
				return false
			}
			m.expect(pos, ringExpect{hint: "the end of the statement"})
			return false
		})
	}
	if m.farPos != len(toks) {
		return nil
	}
	var out []string
	for _, e := range m.farExps {
		if e.lit != "" {
			out = append(out, e.lit)
		}
		out = append(out, ringNames(src, e)...)
	}
	return out
}

func ringNames(src NameSource, e ringExpect) []string {
	if e.src == srcTopics {
		return ringTopics(e.topic)
	}
	if src == nil {
		return nil
	}
	switch e.src {
	case srcDatabases:
		return src.Databases()
	case srcBuckets:
		return src.Buckets(e.db)
	case srcFields:
		return src.Fields(e.db, e.bucket)
	case srcStoreFields:
		return src.StoreFields(e.db, e.bucket)
	case srcNodes:
		return src.Nodes()
	}
	return nil
}
