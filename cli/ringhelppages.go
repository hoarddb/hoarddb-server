package cli

import (
	"fmt"
	"slices"
	"strings"
)

// The topic pages of 'help <word>' are generated here from ringForms, ringVerbs, ringStoreKinds
// and the index pages of ringgrammar.go. Nothing on a page is typed twice.

const (
	// ringParagraphWidth is the width of a wrapped paragraph inside a two-space indent.
	ringParagraphWidth = 74
	// ringSamplePrompt is the prompt a transcript on a verb page shows.
	ringSamplePrompt = "[h]oardDB [app]> "
)

// ringHeading is a page title with its rule and a blank line under it.
func ringHeading(title string) []string {
	return []string{title, strings.Repeat("=", ringWidth(title)), ""}
}

// ringIndented wraps text into lines of at most width characters after an indent.
func ringIndented(text string, indent, width int) []string {
	var lines []string
	for _, line := range ringWrapWords(text, width) {
		lines = append(lines, strings.Repeat(" ", indent)+line)
	}
	return lines
}

// ringJoinWords joins words as "a", "a and b" or "a, b and c".
func ringJoinWords(words []string) string {
	switch len(words) {
	case 0:
		return ""
	case 1:
		return words[0]
	}
	return strings.Join(words[:len(words)-1], ", ") + " and " + words[len(words)-1]
}

// ringExampleLines prints examples after a two-space indent, with notes in an aligned right column.
func ringExampleLines(examples []ringExample) []string {
	width := 0
	for _, example := range examples {
		if example.Note != "" {
			width = max(width, ringWidth(example.Text))
		}
	}
	var lines []string
	for _, example := range examples {
		line := "  " + example.Text
		if example.Note != "" {
			line += strings.Repeat(" ", width-ringWidth(example.Text)) + "  " + example.Note
		}
		lines = append(lines, line)
	}
	return lines
}

// ringStoreSyntax is how a store verb is spelled after the document and field.
func ringStoreSyntax(verb ringVerb) string {
	return `db.<bucket>["<id>"].<storeField>.` + verb.Name + verb.Args
}

// HelpFor returns the text of 'help <topic>'. A topic is a statement's first word ('create'), a
// statement ('create bucket'), a store verb ('push'), or one of 'db', 'types' and 'all'.
func HelpFor(topic string) (string, error) {
	topic = strings.ToLower(strings.Join(strings.Fields(topic), " "))
	switch topic {
	case "all":
		return HelpAll(), nil
	case "types", "stores", "fields":
		return helpTypes(), nil
	case "db":
		return helpIndex(ringDBPage), nil
	case "expire":
		return helpExpire(), nil
	}
	forms, also := ringTopicForms(topic)
	verb, isVerb := ringVerbByName(topic)
	switch {
	case isVerb && len(forms) > 0:
		return helpTwoLevel(topic, forms, verb), nil
	case isVerb:
		return helpVerb(verb), nil
	case len(forms) > 0:
		return helpForms(topic, forms, also), nil
	}
	return "", ringUnknownTopic(topic)
}

// ringUnknownTopic is the refusal for a topic with no page: it lists the topics, and a store word
// points to 'help types', where a v1.0 user types it first.
func ringUnknownTopic(topic string) error {
	fix := "Run 'help' for the first five minutes, or 'help all' for every statement."
	for _, word := range ringStoreWords {
		if strings.HasPrefix(strings.ToLower(word), strings.TrimSuffix(topic, "store")) && topic != "" {
			fix = "Try 'help types' for the stores."
		}
	}
	return fmt.Errorf("no help for %q. Topics: %s. %s", topic, strings.Join(ringTopics(""), ", "), fix)
}

// ringTopicForms finds the forms a topic names: a statement's exact verb with the longer verbs that
// begin with it, or every form that begins with the topic word. When a bare word has forms
// ring mode runs besides user statements and statements it cannot run yet, those others are named
// in also and their own pages hold them, so one page stays under 30 lines.
func ringTopicForms(topic string) (forms []ringForm, also []string) {
	for _, form := range ringForms {
		if form.Verb == topic || strings.HasPrefix(form.Verb, topic+" ") {
			forms = append(forms, form)
		}
	}
	if len(forms) == 0 {
		return nil, nil
	}
	if strings.Contains(topic, " ") {
		return forms, nil
	}
	core := slices.IndexFunc(forms, func(form ringForm) bool { return !form.Unavailable && form.Group != "Users" })
	if core < 0 || len(forms) == 1 {
		return forms, nil
	}
	var kept []ringForm
	for _, form := range forms {
		// A form the topic names exactly stays on its own page even when ring mode cannot run it,
		// so 'help status' says where 'status' stands instead of sending the reader to itself.
		if (form.Unavailable && form.Verb != topic) || form.Group == "Users" {
			also = append(also, "help "+form.Verb)
			continue
		}
		kept = append(kept, form)
	}
	return kept, also
}

func ringTitleOf(topic string, forms []ringForm) string {
	if title, ok := ringTopicTitles[topic]; ok {
		return topic + " — " + title
	}
	short := strings.TrimSuffix(forms[0].Short, ".")
	return topic + " — " + strings.ToLower(short[:1]) + short[1:]
}

func helpForms(topic string, forms []ringForm, also []string) string {
	lines := ringHeading(ringTitleOf(topic, forms))
	next := ""
	for i, form := range forms {
		if len(forms) > 1 {
			heading := form.Heading
			if heading == "" {
				heading = strings.TrimSuffix(form.Short, ".")
			}
			lines = append(lines, heading+":")
		}
		lines = append(lines, "  "+form.Syntax(), "")
		if form.Unavailable {
			lines = append(lines, ringIndented("Not available yet.", 2, ringParagraphWidth+2)...)
		} else {
			lines = append(lines, ringExampleLines(form.Examples)...)
			lines = append(lines, "")
			lines = append(lines, ringIndented(form.Long, 2, ringParagraphWidth)...)
			if form.Familiar != "" {
				lines = append(lines, ringIndented(form.Familiar, 2, ringParagraphWidth)...)
			}
		}
		if next == "" {
			next = form.Next
		}
		if i < len(forms)-1 {
			lines = append(lines, "")
		}
	}
	if len(also) > 0 {
		lines = append(lines, "", "Also: "+strings.Join(also, ", ")+".")
	}
	if next != "" {
		lines = append(lines, "", "Next: "+next)
	}
	return strings.Join(lines, "\n") + "\n"
}

// helpVerb is the page of one store verb.
func helpVerb(verb ringVerb) string {
	lines := ringHeading(verb.Name + " — " + verb.Title)
	lines = append(lines, "  "+ringStoreSyntax(verb))
	lines = append(lines, ringIndented(verb.Long, 6, ringParagraphWidth-4)...)
	lines = append(lines, "")
	if verb.Before != "" {
		lines = append(lines, ringSampleLine(verb.Before))
	}
	lines = append(lines, ringSampleLine(verb.Example))
	if verb.Result != "" {
		lines = append(lines, "  "+verb.Result)
	}
	lines = append(lines, "")
	lines = append(lines, ringIndented(ringVerbParagraph(verb), 2, ringParagraphWidth)...)
	lines = append(lines, "", "Next: "+verb.Next, "Every store and its verbs: help types.")
	return strings.Join(lines, "\n") + "\n"
}

// ringSampleLine shows a statement after the prompt when the line fits the screen, and bare when it does not.
func ringSampleLine(statement string) string {
	if ringWidth("  "+ringSamplePrompt+statement) <= ringScreenWidth {
		return "  " + ringSamplePrompt + statement
	}
	return "  " + statement
}

// ringVerbParagraph is the notes, the stores the verb runs on and the ones it cannot yet.
func ringVerbParagraph(verb ringVerb) string {
	parts := []string{}
	if verb.Notes != "" {
		parts = append(parts, verb.Notes)
	}
	parts = append(parts, "Works on "+ringJoinWords(verb.Types)+".")
	if verb.Instead != "" {
		parts = append(parts, verb.Instead)
	}
	if len(verb.NotYet) > 0 {
		stores := make([]string, len(verb.NotYet))
		for i, store := range verb.NotYet {
			stores[i] = "a " + store
		}
		parts = append(parts, verb.Name+" on "+strings.Join(stores, " or ")+" is not available yet.")
	}
	return strings.Join(parts, " ")
}

// helpTwoLevel is the page of a word that names both a bucket statement and a store verb: list,
// count and get. The reader picks the level.
func helpTwoLevel(topic string, forms []ringForm, verb ringVerb) string {
	title := topic + " — " + ringTopicTitles[topic]
	lines := ringHeading(title)
	lines = append(lines, "On a bucket (its documents):")
	for _, form := range forms {
		lines = append(lines, "  "+form.Syntax(), "")
		for _, example := range form.Examples {
			lines = append(lines, "  "+example.Text)
		}
	}
	lines = append(lines, "", "On a store field (one document's elements):", "  "+ringStoreSyntax(verb), "",
		"  "+verb.Example, "")
	if verb.Notes != "" {
		lines = append(lines, ringIndented(verb.Notes, 0, ringParagraphWidth+2)...)
	}
	lines = append(lines, ringIndented("Works on "+ringJoinWords(verb.Types)+".", 0, ringParagraphWidth+2)...)
	next := verb.Next
	if next == "" && len(forms) > 0 {
		next = forms[0].Next
	}
	lines = append(lines, "Next: "+next)
	return strings.Join(lines, "\n") + "\n"
}

// helpIndex is an index page: the forms of the table under headings, then a few examples.
func helpIndex(page ringIndexPage) string {
	lines := ringHeading(page.Title)
	lines = append(lines, ringIndented(page.Intro, 0, ringParagraphWidth+2)...)
	lines = append(lines, "")
	for _, section := range page.Sections {
		lines = append(lines, section.Heading+":")
		for _, verb := range section.Verbs {
			if form, ok := ringFormByVerb(verb); ok {
				lines = append(lines, ringColumns(form.Syntax(), ringAllColumn, []string{form.Short})...)
			}
		}
	}
	lines = append(lines, "")
	for _, example := range page.Examples {
		lines = append(lines, "  "+example)
	}
	lines = append(lines, "")
	lines = append(lines, ringIndented(page.Familiar, 0, ringParagraphWidth+2)...)
	lines = append(lines, "Next: "+page.Next)
	return strings.Join(lines, "\n") + "\n"
}

// helpTypes is 'help types': the scalar words, the six stores and the verbs each runs.
func helpTypes() string {
	lines := ringHeading("Field types")
	lines = append(lines, "Scalars (one value per document):")
	for _, row := range ringWrapGap(ringScalarWords, "  ", ringParagraphWidth+2) {
		lines = append(lines, "  "+row)
	}
	lines = append(lines, "", "Stores (every document owns its own; you pick the structure):")
	meaning := 0
	for _, kind := range ringStoreKinds {
		meaning = max(meaning, ringWidth(kind.Meaning))
	}
	for _, kind := range ringStoreKinds {
		// A row that would pass 80 columns continues under its verbs column.
		prefix := fmt.Sprintf("  %-10s  %-*s  ", kind.Word, meaning, kind.Meaning)
		verbs := ringWrapWords(strings.Join(ringStoreVerbsOf(kind.Word), " "), 80-ringWidth(prefix))
		for i, row := range verbs {
			if i == 0 {
				lines = append(lines, strings.TrimRight(prefix+row, " "))
				continue
			}
			lines = append(lines, strings.Repeat(" ", ringWidth(prefix))+row)
		}
	}
	lines = append(lines, "")
	first, second := ringSplitExample(ringSixStores, ringParagraphWidth+2)
	lines = append(lines, "  "+first, "      "+second)
	lines = append(lines, "", "integer, float, boolean and date are refused; the error names the word to use.",
		"Next: help create, help push.")
	return strings.Join(lines, "\n") + "\n"
}

// ringStoreVerbsOf lists the verbs a store type runs, leaving out those that act on a lease.
func ringStoreVerbsOf(store string) []string {
	var names []string
	for _, verb := range ringVerbs {
		if verb.Follows == "" && slices.Contains(verb.Types, store) {
			names = append(names, verb.Name)
		}
	}
	return names
}

// ringWrapGap joins words with gap into lines of at most width characters.
func ringWrapGap(words []string, gap string, width int) []string {
	var lines []string
	line := ""
	for _, word := range words {
		switch {
		case line == "":
			line = word
		case ringWidth(line)+ringWidth(gap)+ringWidth(word) <= width:
			line += gap + word
		default:
			lines = append(lines, line)
			line = word
		}
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

// ringSplitExample breaks a long statement after a comma so the first line is at most width wide.
func ringSplitExample(text string, width int) (first, second string) {
	cut := strings.LastIndex(text[:min(len(text), width-2)], ", ")
	return text[:cut+1], text[cut+2:]
}

// ringTopics lists the words 'help' completes: with nothing typed, the first word of each runnable
// statement, the store verbs and the fixed pages; after one word, the words that follow it in a verb.
func ringTopics(typed string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(word string) {
		if !seen[word] {
			seen[word] = true
			out = append(out, word)
		}
	}
	for _, form := range ringForms {
		if form.Unavailable {
			continue
		}
		words := strings.Fields(form.Verb)
		switch {
		case typed == "" && form.Verb == "field":
		case typed == "" && form.Pattern[0].text == "db":
			add("db")
			add(form.Verb)
		case typed == "":
			add(form.Pattern[0].text)
		case len(words) > 1 && words[0] == typed:
			add(words[1])
		}
	}
	if typed == "" {
		for _, name := range ringVerbNames() {
			add(name)
		}
		add("types")
		add("all")
	}
	return out
}

// helpExpire is the page of 'help expire': documents and queue elements that leave at a time the
// developer wrote down.
func helpExpire() string {
	lines := ringHeading("expire — documents and queue elements that leave at a time you wrote")
	lines = append(lines,
		"  create bucket Session { UserId objectid, ExpiresAt datetime, expire at ExpiresAt }",
		`  create bucket Device { Name string, Log fifoStore expire at "ts" }`,
		"  alter bucket Session { set expire at ExpiresAt }",
		"  alter bucket Session { drop expire at }",
		"")
	lines = append(lines, ringIndented("The field you name must hold the EXPIRY instant, not the creation time. A document whose field is at or before now is already gone: reads skip it at once and the owner removes it soon after. 'expire at CreatedAt' with a field that defaults to now() removes the document the moment it is written. A document without the field never expires.", 0, ringParagraphWidth+2)...)
	lines = append(lines, "")
	lines = append(lines, ringIndented("There is no duration form: write the time yourself, for example now plus 30 minutes in your program. A queue element's path must hold a datetime, or the push is refused. Only fifoStore and lifoStore elements expire in this release.", 0, ringParagraphWidth+2)...)
	lines = append(lines, "")
	lines = append(lines, ringIndented("Nodes compare against their own clocks, so a document can become invisible a few milliseconds apart on two nodes; the removal itself does not depend on any clock.", 0, ringParagraphWidth+2)...)
	lines = append(lines, "")
	lines = append(lines, ringIndented("A pop or claim removes at most 64 expired elements per call and may answer empty while live elements are queued behind more than that: ask again, the retention tick clears the backlog. 'count' on an expiring queue reads every element, so it costs as much as the queue is long. Two small gaps: documents written between the check that a bucket holds none and the schema bump of 'set expire at', or between the alter and its publication, are not covered; and a node that becomes owner after a failover does not remove what expired before it was seeded.", 0, ringParagraphWidth+2)...)
	lines = append(lines, "", "Next: help create bucket.")
	return strings.Join(lines, "\n") + "\n"
}
