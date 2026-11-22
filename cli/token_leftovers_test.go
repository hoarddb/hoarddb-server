package cli

import (
	"bytes"
	"io"
	"regexp"
	"strings"
	"testing"
)

// TestNoClusterTokenSurfaceInCLIText: a start no longer mints or prints a
// cluster token and a first start writes no cluster.token (commit 4e1bc202d;
// with encryption at rest a start still seals, or refuses, a token file an
// earlier build left), so the binary's help, its quick reference, every help
// topic and tab completion must not offer a -token or --token flag, the
// get-token and rotate-token verbs, or the HOARDDB_CLUSTER_TOKEN variable. A
// user reading them would look for a token that does not exist.
func TestNoClusterTokenSurfaceInCLIText(t *testing.T) {
	forbidden := []string{"get-token", "rotate-token", "HOARDDB_CLUSTER_TOKEN"}

	var help bytes.Buffer
	RunMain(Invocation{Args: []string{"-h"}, Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: &help, Getenv: func(string) string { return "" }})

	var quick bytes.Buffer
	handler := NewCommandHandler(&quick, false)
	handler.handleHelp()

	scanned := map[string]string{
		"hoarddb -h":          help.String(),
		"quick reference":     quick.String(),
		"completion keywords": strings.Join(completionWords, " "),
	}
	// Every topic the CLI lists is scanned, and a topic that prints nothing or
	// is refused fails the test: a name that is not a topic would scan no text.
	for _, topic := range helpTopicList() {
		var topicText bytes.Buffer
		if err := NewCommandHandler(&topicText, false).handleHelpTopic(topic); err != nil {
			t.Fatalf("help %s is listed as a topic but is refused, so its text was not scanned: %v", topic, err)
		}
		if topicText.Len() == 0 {
			t.Fatalf("help %s printed nothing, so there was no text to scan", topic)
		}
		scanned["help "+topic] = topicText.String()
	}

	for name, text := range scanned {
		if spelling := clusterTokenFlag.FindString(text); spelling != "" {
			t.Errorf("%s still offers the token flag (%q):\n%s", name, strings.TrimSpace(spelling), text)
		}
		for _, word := range forbidden {
			if strings.Contains(text, word) {
				t.Errorf("%s still offers %q:\n%s", name, word, text)
			}
		}
	}
}

// clusterTokenFlag matches -token and --token as a flag of their own, so
// -auth-token and --auth-token are not hits. The removed options of join,
// remove-node, ingest and dump were spelled --token.
var clusterTokenFlag = regexp.MustCompile(`(^|[^\w-])--?token($|[^\w-])`)

// TestTheClusterTokenFlagPatternMatchesBothSpellings pins the pattern the
// scan above depends on: it once missed --token, because the character before
// "-token" there is a dash and the pattern asked for white space.
func TestTheClusterTokenFlagPatternMatchesBothSpellings(t *testing.T) {
	for text, want := range map[string]bool{
		"-token":                      true,
		"--token":                     true,
		"join n2@host:7433 --token t": true,
		"  -token string":             true,
		"[--token=<token>]":           true,
		"--auth-token":                false,
		"-auth-token <t>":             false,
		"--token-file":                false,
		"a login token":               false,
	} {
		if got := clusterTokenFlag.MatchString(text); got != want {
			t.Errorf("clusterTokenFlag matches %q: %v, want %v", text, got, want)
		}
	}
}
