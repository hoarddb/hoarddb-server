package cli

import (
	"bytes"
	"errors"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
)

// TestConfigHelpStatesTheBinaryDefaults: the config topic gave the Docker
// image's values (0.0.0.0 listeners, /data) as the binary's defaults, so a
// reader running the binary on a laptop expected a server on every interface.
// The stated defaults are checked against config.DefaultConfig.
func TestConfigHelpStatesTheBinaryDefaults(t *testing.T) {
	var out bytes.Buffer
	requireHelpTopic(t, NewCommandHandler(&out, false), "config")
	cfg := config.DefaultConfig()
	for name, want := range map[string]string{
		"HOARDDB_TCP_LISTEN": cfg.TCP.Listen,
		"HOARDDB_LISTEN":     cfg.Server.Listen,
		"HOARDDB_DATA_DIR":   cfg.Storage.DataDir,
	} {
		line := ""
		for _, candidate := range strings.Split(out.String(), "\n") {
			if strings.HasPrefix(strings.TrimSpace(candidate), name+" ") {
				line = candidate
				break
			}
		}
		if !strings.Contains(line, "(default: "+want) {
			t.Errorf("%s help line %q does not state the binary default %q", name, line, want)
		}
	}
}

// TestHelpOperatorsTeachesTheFindGrammar pins `help operators` against the
// grammar the server actually implements (server/scan_filter.go). It exists
// because the topic described only the index (search) operators and listed
// $and under "not implemented yet" long after the scan filter gained it,
// $regex and $in — so a reader who trusted the help went looking for a
// wildcard that HQL does not have.
func TestHelpOperatorsTeachesTheFindGrammar(t *testing.T) {
	var out bytes.Buffer
	handler := NewCommandHandler(&out, false)
	requireHelpTopic(t, handler, "operators")
	help := out.String()

	// The find path's operators must be named, including $regex — the answer to
	// "how do I match a prefix?".
	for _, operator := range []string{"$gt", "$gte", "$lt", "$lte", "$in", "$regex", "$and"} {
		if !strings.Contains(help, operator) {
			t.Errorf("help operators never names %s, which the scan filter implements", operator)
		}
	}

	// The two filter grammars are different and the help must say so.
	for _, section := range []string{"find filters", "search filters"} {
		if !strings.Contains(help, section) {
			t.Errorf("help operators does not separate the two grammars: %q is missing", section)
		}
	}

	// A bare value is an exact match; there is no wildcard. This is the
	// misconception the topic did not correct.
	if !strings.Contains(help, "EXACT match") || !strings.Contains(help, "no wildcard operator") {
		t.Error("help operators does not say that a bare value is an exact match and that there is no wildcard")
	}

	// Nothing the server implements may be listed as unimplemented. Tokens are
	// compared whole, because "$in" is a substring of "$nin".
	implemented := map[string]bool{
		"$and": true, "$regex": true, "$in": true, "$gt": true, "$gte": true,
		"$lt": true, "$lte": true, "$set": true, "$unset": true, "$inc": true,
	}
	foundNotImplementedLine := false
	for _, line := range strings.Split(help, "\n") {
		if !strings.HasPrefix(line, "Not implemented:") {
			continue
		}
		foundNotImplementedLine = true
		for _, token := range strings.Fields(line) {
			if implemented[strings.Trim(token, ",")] {
				t.Errorf("help lists %s as not implemented, but the server implements it: %q", token, line)
			}
		}
	}
	if !foundNotImplementedLine {
		t.Error("help operators no longer names the operators it refuses")
	}
}

// TestBothHelpTextsSayWhatSearchSupports (GAP-14 build doc Step 22, C-27):
// `help operators` listed "$eq $ne $gt $gte $lt $lte $in" as what an index
// serves — $eq exists nowhere, and an index serves equality only — and the
// main help said search "needs indices". Search takes find's filter; an index
// serves its equality fields (a prefix of a compound index's), every
// predicate is checked against each document, and with no usable index it
// scans and says so. And the main help showed an index on a btree bucket,
// which the server refuses.
//
// The section used to have to name the wire field index_used, because the CLI
// dropped that field: the help described a response detail the user never saw.
// The CLI now prints it, so the help shows the shape the user actually reads.
func TestBothHelpTextsSayWhatSearchSupports(t *testing.T) {
	var operators bytes.Buffer
	requireHelpTopic(t, NewCommandHandler(&operators, false), "operators")
	topic := operators.String()
	searchSection := topic[strings.Index(topic, "search filters"):]
	if end := strings.Index(searchSection, "update operators"); end > 0 {
		searchSection = searchSection[:end]
	}
	if strings.Contains(searchSection, "$eq") {
		t.Errorf("help operators names $eq, which no filter implements: %q", searchSection)
	}
	for _, want := range []string{"equality", "full scan", "(via Email_1)", "(full scan):"} {
		if !strings.Contains(searchSection, want) {
			t.Errorf("help operators' search section does not say %q: %q", want, searchSection)
		}
	}

	var main bytes.Buffer
	NewCommandHandler(&main, false).handleHelp()
	help := main.String()
	if strings.Contains(help, "needs indices") {
		t.Error("the main help says search needs indices; without one it scans and says so")
	}
	if strings.Contains(help, "type: btree, indices") {
		t.Error("the main help shows an index on a btree bucket, which the server refuses: only hash buckets maintain an index")
	}
}

// TestHelpUnknownTopicIsAUsageError: `help nosuchtopic` printed the topics and
// reported success, so a script could not tell a typo from a found topic
// (BUG-0202). It is a usage error, which the CLI exits with ExitUsage, and a
// known topic still succeeds.
func TestHelpUnknownTopicIsAUsageError(t *testing.T) {
	session := newFixtureSession(t)

	session.out.Reset()
	err := session.cli.handle("help nosuchtopic")
	var usage *UsageError
	if !errors.As(err, &usage) {
		t.Fatalf("help nosuchtopic returned %v, want a *UsageError", err)
	}
	if got := classify(err).Exit; got != ExitUsage {
		t.Errorf("help nosuchtopic exits %d, want ExitUsage (%d)", got, ExitUsage)
	}
	if !strings.Contains(err.Error(), `"nosuchtopic"`) {
		t.Errorf("the refusal %q does not name the topic that was asked for", err)
	}
	for _, topic := range helpTopicList() {
		if !strings.Contains(err.Error(), topic) {
			t.Errorf("the refusal %q does not list the topic %q", err, topic)
		}
	}

	session.out.Reset()
	if err := session.cli.handle("help hash"); err != nil {
		t.Fatalf("help hash returned %v, want success", err)
	}
	if !strings.Contains(session.out.String(), "Hash Store Operations (Key-Value):") {
		t.Errorf("help hash printed %q, want the hash topic", session.out.String())
	}
}

// TestHelpTopicIsRoutedToItsHandler: The general help advertises
// `help <topic>`, but ParseCommand built the help command without its words,
// so the dispatcher's topic branch never ran and `help hash` printed the
// general reference. Driven through the parser and the dispatcher, as a user
// types it.
func TestHelpTopicIsRoutedToItsHandler(t *testing.T) {
	session := newFixtureSession(t)
	general := strings.TrimPrefix(runStatements(t, session.cli, session.out, "help"), "> help\n")

	t.Run("a known topic prints its own page", func(t *testing.T) {
		got := strings.TrimPrefix(runStatements(t, session.cli, session.out, "help hash"), "> help hash\n")
		if !strings.Contains(got, "Hash Store Operations (Key-Value):") {
			t.Errorf("help hash printed %q, want the hash topic", got)
		}
		if got == general {
			t.Error("help hash printed the general help")
		}
	})

	t.Run("an unknown topic names the valid ones", func(t *testing.T) {
		got := runStatements(t, session.cli, session.out, "help nosuchtopic")
		if !strings.Contains(got, `unknown help topic "nosuchtopic"`) {
			t.Errorf("help nosuchtopic printed %q, want it to say the topic is unknown", got)
		}
		for _, topic := range helpTopicList() {
			if !strings.Contains(got, topic) {
				t.Errorf("help nosuchtopic does not list the topic %q:\n%s", topic, got)
			}
		}
	})

	t.Run("bare help is still the general help", func(t *testing.T) {
		if !strings.Contains(general, "Quick Reference") {
			t.Errorf("help printed %q, want the general reference", general)
		}
	})

	t.Run("every advertised topic has a page", func(t *testing.T) {
		for _, topic := range helpTopicList() {
			got := strings.TrimPrefix(runStatements(t, session.cli, session.out, "help "+topic), "> help "+topic+"\n")
			if strings.Contains(got, "unknown help topic") || got == general {
				t.Errorf("help %s printed %q, want a topic page", topic, got)
			}
		}
	})
}

// TestGeneralHelpDoesNotCallClusterVerbsUnserved: the general help listed
// `join` and `remove-node` under "Not available yet (no replication service is
// served)" after both became real, documented statements (the "hql-spec" user-docs page
// and replication.md). A reader trusting it would never try them on a cluster.
func TestGeneralHelpDoesNotCallClusterVerbsUnserved(t *testing.T) {
	var out bytes.Buffer
	handler := NewCommandHandler(&out, false)
	handler.handleHelp()
	help := out.String()

	if strings.Contains(help, "no replication service is served") {
		t.Error("the general help still says there is no replication service")
	}
	notAvailable := help[strings.Index(help, "Not available yet"):]
	notAvailable = notAvailable[:strings.Index(notAvailable, "\n\n")]
	for _, verb := range []string{"join", "remove-node"} {
		if strings.Contains(notAvailable, verb) {
			t.Errorf("the general help lists %s under %q", verb, "Not available yet")
		}
		if !strings.Contains(help, "\n  "+verb+" ") {
			t.Errorf("the general help does not list %s as a command", verb)
		}
	}
}

// helpUpdateOperators reads the operator names out of the "update operators:"
// block of `help operators`: the block runs to the "range bounds:" line, and an
// operator is a $name followed by its form, {...} or [...]. The examples in the
// block write "$set:" with a colon, so they are not read as headings.
func helpUpdateOperators(t *testing.T) []string {
	t.Helper()
	var out bytes.Buffer
	requireHelpTopic(t, NewCommandHandler(&out, false), "operators")
	help := out.String()
	start := strings.Index(help, "update operators:")
	end := strings.Index(help, "range bounds:")
	if start < 0 || end < start {
		t.Fatalf("help operators has no update operators block:\n%s", help)
	}
	var names []string
	for _, match := range regexp.MustCompile(`(\$[A-Za-z]+) +[{\[]`).FindAllStringSubmatch(help[start:end], -1) {
		names = append(names, match[1])
	}
	if len(names) == 0 {
		t.Fatalf("no operator found in the update operators block:\n%s", help[start:end])
	}
	return names
}

// TestHelpUpdateOperatorsMatchTheParser: the help listed six
// operators that applyUpdateOperators answered with "is not supported". It
// feeds every operator the help lists to the parser.
func TestHelpUpdateOperatorsMatchTheParser(t *testing.T) {
	for _, operator := range helpUpdateOperators(t) {
		request := &server.UpdateRequest{}
		err := applyUpdateOperators(request, map[string]interface{}{
			operator: map[string]interface{}{"field": "value"},
		})
		if err != nil && strings.Contains(err.Error(), "is not supported") {
			t.Errorf("help operators lists %s, but the update parser refuses it: %v", operator, err)
		}
	}
}

// TestHelpListsOnlyOperatorsTheParserTakes compares the sets in both
// directions: the help block, the parser's declared set and the usage text of
// `update` name the same operators, no more and no fewer.
func TestHelpListsOnlyOperatorsTheParserTakes(t *testing.T) {
	parserSet := append([]string(nil), updateOperatorNames...)
	helpSet := helpUpdateOperators(t)
	sort.Strings(parserSet)
	sort.Strings(helpSet)
	if !reflect.DeepEqual(helpSet, parserSet) {
		t.Errorf("help operators lists %v, the parser takes %v", helpSet, parserSet)
	}

	var printed bytes.Buffer
	err := NewCommandHandler(&printed, false).handleUpdate("app", "Q", nil)
	if err == nil {
		t.Fatal("update with no arguments was accepted")
	}
	for _, operator := range parserSet {
		if !strings.Contains(err.Error(), operator) {
			t.Errorf("the usage text of update does not name %s: %v", operator, err)
		}
	}
}

// helpTopicText returns what `help <topic>` prints.
func helpTopicText(t *testing.T, topic string) string {
	t.Helper()
	var out bytes.Buffer
	requireHelpTopic(t, NewCommandHandler(&out, false), topic)
	return out.String()
}

// requireHelpTopic prints a topic that must exist and fails the test if the
// handler calls it unknown.
func requireHelpTopic(t *testing.T, handler *CommandHandler, topic string) {
	t.Helper()
	if err := handler.handleHelpTopic(topic); err != nil {
		t.Fatalf("help %s: %v", topic, err)
	}
}

// TestHelpDoesNotCallAlterBucketUnavailable: the quick reference
// listed `alter bucket` under "Not available yet" while `help fields` taught it
// and the command works.
func TestHelpDoesNotCallAlterBucketUnavailable(t *testing.T) {
	var out bytes.Buffer
	NewCommandHandler(&out, false).handleHelp()
	help := out.String()

	notAvailable := help[strings.Index(help, "Not available yet"):]
	notAvailable = notAvailable[:strings.Index(notAvailable, "\n\n")]
	if strings.Contains(notAvailable, "alter bucket") {
		t.Errorf("the quick reference lists alter bucket under \"Not available yet\":\n%s", notAvailable)
	}
	if !strings.Contains(help, "alter bucket <name> {") {
		t.Error("the quick reference does not show the alter bucket form that works")
	}
}

// TestGeneralHelpDoesNotCallAlterBucketUnavailable (BUG-0188): the general
// help listed `alter bucket` under "Not available yet" after the store-field
// form had become real, so a reader trusting the help would drop and recreate
// a bucket to add a field that one statement adds. The command line must sit
// outside that block, worded as what it does.
func TestGeneralHelpDoesNotCallAlterBucketUnavailable(t *testing.T) {
	var out bytes.Buffer
	NewCommandHandler(&out, false).handleHelp()
	help := out.String()

	start := strings.Index(help, "Not available yet")
	if start < 0 {
		t.Fatal("the general help no longer has a \"Not available yet\" block")
	}
	notAvailable := help[start:]
	if end := strings.Index(notAvailable, "\n\n"); end >= 0 {
		notAvailable = notAvailable[:end]
	}
	if strings.Contains(notAvailable, "alter bucket") {
		t.Errorf("the general help lists alter bucket under \"Not available yet\":\n%s", notAvailable)
	}
	if !strings.Contains(help, "alter bucket <name> {") {
		t.Error("the general help does not list the alter bucket form that works")
	}
}

// TestHelpUserTopicTeachesEveryUserCommand: `help user` was a role
// list with no command in it. Every example it prints must also parse (see
// TestHelpStatementExamplesParse).
func TestHelpUserTopicTeachesEveryUserCommand(t *testing.T) {
	help := helpTopicText(t, "user")
	for _, fragment := range []string{
		"create user", "roles [", "alter user", "grant role", "revoke role",
		"disable", "enable", "drop user", "password", "alter user admin password",
	} {
		if !strings.Contains(help, fragment) {
			t.Errorf("help user does not contain %q:\n%s", fragment, help)
		}
	}
}

// TestHelpThemeNamesEveryThemeFileKey: nothing in the help said a
// theme file exists. The key list is read from themeFile's yaml tags, so a new
// key without help fails.
func TestHelpThemeNamesEveryThemeFileKey(t *testing.T) {
	if !slices.Contains(helpTopicList(), "theme") {
		t.Fatalf("helpTopicList %v has no theme topic", helpTopicList())
	}
	help := helpTopicText(t, "theme")
	if strings.Contains(help, "unknown help topic") {
		t.Fatalf("help theme is unknown:\n%s", help)
	}
	for _, key := range yamlTagKeys(themeFile{}) {
		if !strings.Contains(help, key) {
			t.Errorf("help theme does not name the theme file key %q", key)
		}
	}
}

// TestHelpStoreTopicsShowTheFieldForm: help btree, hash and blob
// showed only the bucket verbs, so a user with a btree store field tried
// insert, which a field refuses.
func TestHelpStoreTopicsShowTheFieldForm(t *testing.T) {
	for _, topic := range []string{"btree", "hash", "blob"} {
		help := helpTopicText(t, topic)
		if !strings.Contains(help, `["<key>"].`) || !strings.Contains(help, ".put(") {
			t.Errorf("help %s does not show the store-field form (db.<bucket>[\"<key>\"].<field>.put(...)):\n%s", topic, help)
		}
	}
}

// TestHelpPlacementTopic is the placement help page (build step 12b): it
// names spread and whole, says what unique and a filtered update need, the
// density a whole bucket keeps on one node and the blob caveat of a recreate,
// and shows the recreate sequence. Every statement it shows is parsed by
// TestHelpStatementExamplesParse, which walks helpTopicList.
func TestHelpPlacementTopic(t *testing.T) {
	if !slices.Contains(helpTopicList(), "placement") {
		t.Fatalf("helpTopicList %v has no placement topic", helpTopicList())
	}
	help := helpTopicText(t, "placement")
	for _, want := range []string{"spread", "whole", "unique", "filter", "one node", "blob payloads",
		"create bucket User in app { placement: whole, Name string, Email string unique }",
		"dump --out ./dump-User app User", "restore --dir ./dump-User"} {
		if !strings.Contains(help, want) {
			t.Errorf("help placement does not mention %q:\n%s", want, help)
		}
	}
}

// TestConfigHelpDoesNotOfferThePerNodeReplicationFactor: the server refuses
// HOARDDB_REPLICATION_FACTOR above 1 at start, so the config topic must not list it as a setting.
// Copies are a database option in the ring.
func TestConfigHelpDoesNotOfferThePerNodeReplicationFactor(t *testing.T) {
	var out bytes.Buffer
	requireHelpTopic(t, NewCommandHandler(&out, false), "config")
	if strings.Contains(out.String(), "HOARDDB_REPLICATION_FACTOR") {
		t.Errorf("config help still lists HOARDDB_REPLICATION_FACTOR:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "create database <name> { replication_factor: N }") {
		t.Errorf("config help does not say where copies are set:\n%s", out.String())
	}
}
