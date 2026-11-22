package cli

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// quotedCommand matches a command a server message tells the operator to
// type: a single-quoted string after "run", "use", "try", "or" or a "/"
// separating two alternatives. Mentions in other positions ("pass 'in
// <database>'") are phrases, not commands, and are not matched.
var quotedCommand = regexp.MustCompile(`(?:[Rr]un|[Uu]se|[Tt]ry|or|/) '([a-z][^']*)'`)

// placeholder is a <name> slot or a format verb inside a quoted command.
var placeholder = regexp.MustCompile(`<[^>]+>|%[sqdv]`)

func concreteCommand(quoted string) string {
	return placeholder.ReplaceAllString(quoted, "app")
}

// TestJoinFenceMessageNamesParsableCommands checks that the join-fence message names only parsable commands.
// ErrJoinSyncIncomplete told the operator to run 'cluster status' and
// 'bucket list', and neither parses. Every command it names must parse.
func TestJoinFenceMessageNamesParsableCommands(t *testing.T) {
	message := server.ErrJoinSyncIncomplete.Error()
	matches := quotedCommand.FindAllStringSubmatch(message, -1)
	if len(matches) == 0 {
		t.Fatalf("ErrJoinSyncIncomplete names no command for the operator to run: %s", message)
	}
	for _, match := range matches {
		command := concreteCommand(match[1])
		if _, err := ParseCommand(command); err != nil {
			t.Errorf("ErrJoinSyncIncomplete tells the operator to run %q, which the CLI rejects: %v", match[1], err)
		}
	}
}

// TestServerMessagesNameParsableCommands sweeps the server's source for the
// same class: a message that tells the operator to run a command the CLI
// does not accept. A quoted bare verb ('migrate', 'remove-node') is a
// mention of the command, and passes when the CLI knows the verb — its parse
// error is then a usage line. An unknown command fails.
func TestServerMessagesNameParsableCommands(t *testing.T) {
	sources, err := filepath.Glob(filepath.Join("..", "server", "*.go"))
	if err != nil {
		t.Fatalf("listing the server sources: %v", err)
	}
	checked := 0
	for _, path := range sources {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		for _, match := range quotedCommand.FindAllStringSubmatch(string(source), -1) {
			checked++
			_, err := ParseCommand(concreteCommand(match[1]))
			if err == nil || strings.HasPrefix(err.Error(), "usage:") {
				continue
			}
			t.Errorf("%s tells the operator to run %q, which the CLI rejects: %v", filepath.Base(path), match[1], err)
		}
	}
	if checked == 0 {
		t.Fatal("the sweep found no quoted command in the server's messages; the pattern no longer matches how they are written")
	}
}
