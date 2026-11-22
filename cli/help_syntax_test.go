package cli

import (
	"bytes"
	"strings"
	"testing"
)

// TestHelpOnlyListsSyntaxTheParserAccepts: `help` listed `--` and
// `/* */` comments while ParseCommand answered "unknown command: --" to the
// first and "expected 'db.<bucket>.<method>(...)'" to the second. Every
// statement form the help prints must parse, and a line that advertises
// comments must be backed by a parser that skips them.
func TestHelpOnlyListsSyntaxTheParserAccepts(t *testing.T) {
	var out bytes.Buffer
	NewCommandHandler(&out, false).handleHelp()

	placeholders := strings.NewReplacer("<bucket>", "Orders", "<key>", "k1", "<name>", "Orders")
	var listedStatements []string
	for _, line := range strings.Split(out.String(), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "db.") {
			continue
		}
		// The description column starts after a run of spaces.
		statement, _, _ := strings.Cut(trimmed, "  ")
		listedStatements = append(listedStatements, placeholders.Replace(statement))
	}
	if len(listedStatements) == 0 {
		t.Fatal("help lists no db.<bucket>.<method>(...) statements; the extraction is stale")
	}
	for _, statement := range listedStatements {
		if _, err := ParseCommand(statement); err != nil {
			t.Errorf("help lists %q, which the parser refuses: %v", statement, err)
		}
	}

	if strings.Contains(out.String(), "Comments:") {
		for _, commented := range []string{"-- a comment", "/* c */ db.Orders.count()"} {
			if _, err := ParseCommand(commented); err != nil {
				t.Errorf("help advertises comments, but %q is refused: %v", commented, err)
			}
		}
	}
}

// statementStarts are the first words of a help line that is a statement the
// user would type. A line that begins with anything else is prose or a flag.
var statementStarts = []string{
	"db.", "use ", "show ", "describe ", "status", "encryption", "nodes", "join ", "remove-node ", "connect ", "help", "exit", "create ",
	"alter ", "drop ", "grant ", "revoke ", "user ", "master",
}

// helpPlaceholders turns the angle-bracket placeholders the help prints into
// concrete names, so an example reaches the parser as a user would type it.
var helpPlaceholders = strings.NewReplacer(
	"[role@db, ...]", "[reader@app]",
	"<role@db>", "reader@app",
	"<id@host:port>", "n2@localhost:4433",
	"<host:port>", "localhost:7433",
	"<node-id>", "n2",
	"<database>", "app",
	"<bucket>", "Orders",
	"<field>", "Comments",
	"<verb>(...)", "pop()",
	"<user>", "ada",
	"<key>", "k1",
	"<uuid>", "k1",
	"<name>", "Orders",
	"<topic>", "hash",
	"<path>", "f.bin",
	"disable|enable", "enable",
	"exit / quit", "exit",
)

// helpStatements pulls every statement example out of one help text: lines that
// begin with a statement keyword, with a shown prompt stripped and the
// description column (after two spaces) cut off.
func helpStatements(helpText string) []string {
	var statements []string
	for _, line := range strings.Split(helpText, "\n") {
		trimmed := strings.TrimSpace(line)
		if _, afterPrompt, found := strings.Cut(trimmed, "]> "); found && strings.HasPrefix(trimmed, "[h]oardDB") {
			trimmed = afterPrompt
		}
		for _, start := range statementStarts {
			if strings.HasPrefix(trimmed, start) {
				statement, _, _ := strings.Cut(trimmed, "  ")
				statements = append(statements, helpPlaceholders.Replace(strings.TrimSpace(statement)))
				break
			}
		}
	}
	return statements
}

// allHelpText is the quick reference and every topic.
func allHelpText(t *testing.T) map[string]string {
	var out bytes.Buffer
	handler := NewCommandHandler(&out, false)
	handler.handleHelp()
	pages := map[string]string{"help": out.String()}
	for _, topic := range helpTopicList() {
		pages["help "+topic] = helpTopicText(t, topic)
	}
	return pages
}

// TestHelpStatementExamplesParse: the syntax gate only read lines
// that begin with db., so a wrong create user, grant role or alter bucket line
// in the help passed. Every statement example in every page must parse.
func TestHelpStatementExamplesParse(t *testing.T) {
	forms := map[string]bool{}
	for page, text := range allHelpText(t) {
		for _, statement := range helpStatements(text) {
			forms[strings.Fields(statement)[0]] = true
			if _, err := ParseCommand(statement); err != nil {
				t.Errorf("%s prints %q, which the parser refuses: %v", page, statement, err)
			}
		}
	}
	for _, form := range []string{"create", "alter", "grant", "revoke", "drop", "use", "show"} {
		if !forms[form] {
			t.Errorf("the walk found no %q example in any help page; the extraction is stale", form)
		}
	}
}
