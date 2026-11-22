package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The trust corpus is clients/testdata/trust-vectors/v1.json: the
// known_servers rules of clients/handbook/trust.md section 4, every expected
// value produced by hoardDB/transport.
const committedTrustCorpusPath = "../../../clients/testdata/trust-vectors/v1.json"

// committedTrustCorpus reads the committed file: a failure in the private
// tree, a named skip in the published server tree (which has no clients/).
func committedTrustCorpus(t *testing.T) []byte {
	t.Helper()
	committed, err := os.ReadFile(committedTrustCorpusPath)
	if err == nil {
		return committed
	}
	if errors.Is(err, fs.ErrNotExist) {
		if _, markerErr := os.Stat(privateTreeMarker); errors.Is(markerErr, fs.ErrNotExist) {
			t.Skipf("published server tree: clients/ is not part of it, so there is no committed %s to check", committedTrustCorpusPath)
		}
	}
	t.Fatalf("reading the committed trust corpus %s: %v; generate it with 'make trust-vectors' (hoardDB/Makefile)", committedTrustCorpusPath, err)
	return nil
}

func freshTrustCorpus(t *testing.T, commit string) []byte {
	t.Helper()
	generated, err := generateTrustCorpus(commit)
	if err != nil {
		t.Fatalf("generating the trust corpus: %v", err)
	}
	return generated
}

// TestTrustCorpusIsDeterministic: two runs give the same bytes, which is what
// lets the drift gate exist at all.
func TestTrustCorpusIsDeterministic(t *testing.T) {
	first := freshTrustCorpus(t, "0000000000000000000000000000000000000000")
	second := freshTrustCorpus(t, "0000000000000000000000000000000000000000")
	if !bytes.Equal(first, second) {
		t.Fatal("two generations of the trust corpus differ: the output must not depend on time, map order, temporary paths or randomness")
	}
}

// TestTrustCorpusMatchesCommitted fails when the committed file is not what
// the known_servers code produces today; server_commit may differ.
func TestTrustCorpusMatchesCommitted(t *testing.T) {
	committed := committedTrustCorpus(t)
	corpus, err := decodeTrustCorpus(committed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(committed, freshTrustCorpus(t, corpus.ServerCommit)) {
		t.Fatalf("%s does not match the known_servers code at this commit; regenerate with 'make trust-vectors' and review the diff: a vector that changes is a change to every driver", committedTrustCorpusPath)
	}
}

func TestTrustCorpusHeader(t *testing.T) {
	generated := freshTrustCorpus(t, "abc123")
	corpus, err := decodeTrustCorpus(generated)
	if err != nil {
		t.Fatal(err)
	}
	if corpus.Format != trustCorpusFormat || corpus.ServerCommit != "abc123" || corpus.DefaultPort != 7433 {
		t.Errorf("header: format=%q server_commit=%q default_port=%d", corpus.Format, corpus.ServerCommit, corpus.DefaultPort)
	}
	if len(corpus.Keys) != 2 || len(corpus.Groups) != 5 {
		t.Errorf("header has %d keys and %d groups, want 2 and 5", len(corpus.Keys), len(corpus.Groups))
	}
	for _, leaked := range []string{homeDirPrefix, "/tmp/", os.TempDir()} {
		if strings.Contains(string(generated), leaked) {
			t.Errorf("the corpus names a local path (%s)", leaked)
		}
	}
}

// TestTrustCorpusCoversTheRules: every input the rules single out is in the
// file, so a later edit of the case table cannot quietly drop one.
func TestTrustCorpusCoversTheRules(t *testing.T) {
	corpus, err := decodeTrustCorpus(freshTrustCorpus(t, "abc123"))
	if err != nil {
		t.Fatal(err)
	}
	byGroup := make(map[string][]trustVector)
	for _, vector := range corpus.Vectors {
		byGroup[vector.Group] = append(byGroup[vector.Group], vector)
	}
	inputs := func(group string, of func(trustVector) string) string {
		var all strings.Builder
		for _, vector := range byGroup[group] {
			all.WriteString(of(vector) + "\n--\n")
		}
		return all.String()
	}
	canonicalInputs := inputs("canonical", func(v trustVector) string { return *v.Input })
	for _, want := range []string{"DB.Internal:7433", "db.internal.:7433", "[::1]:7433", "[0:0:0:0:0:0:0:1]:7433", "[::0001]", "127.0.0.1:7433", "db.internal\n", "db:07433", "%eth0", "localhost:7433"} {
		if !strings.Contains(canonicalInputs, want) {
			t.Errorf("no canonical vector for %q", want)
		}
	}
	parseInputs := inputs("parse", func(v trustVector) string { return *v.FileText })
	for _, want := range []string{"\r\n", "\t", "\n  \t", " ", "\u0085", " ", "\f", "\v", " added ", "# ", "\n\n", "not-hex"} {
		if !strings.Contains(parseInputs, want) {
			t.Errorf("no parse vector holds %q", want)
		}
	}
	var duplicateRefused, changedLookup, refusedSave, appendCase, replaceCase bool
	for _, vector := range byGroup["parse"] {
		duplicateRefused = duplicateRefused || len(vector.ErrorContains) == 3
	}
	for _, vector := range byGroup["lookup"] {
		changedLookup = changedLookup || (vector.Expect == "changed" && vector.RecordedFingerprint != "" && vector.PresentedFingerprint != "")
	}
	for _, vector := range byGroup["save"] {
		refusedSave = refusedSave || vector.ErrorContains != nil
		appendCase = appendCase || vector.Action == "append"
		replaceCase = replaceCase || vector.Action == "replace"
	}
	if !duplicateRefused || !changedLookup || !refusedSave || !appendCase || !replaceCase {
		t.Errorf("missing: duplicate refusal %v, changed lookup %v, refused save %v, append %v, replace %v",
			duplicateRefused, changedLookup, refusedSave, appendCase, replaceCase)
	}
	owners := map[string]bool{}
	for _, vector := range byGroup["safety"] {
		owners[vector.Owner+"/"+vector.Expect] = true
	}
	for _, want := range []string{"self/accept", "self/refuse", "root/accept", "other/refuse"} {
		if !owners[want] {
			t.Errorf("no safety vector for %s", want)
		}
	}
}

// TestTrustGeneratorWritesOnlyItsFile: -trust-out writes trust-vectors/v1.json
// and nothing else, and refuses the other vector directories.
func TestTrustGeneratorWritesOnlyItsFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "trust-vectors")
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-trust-out", out}, &stdout, &stderr); err != nil {
		t.Fatalf("-trust-out: %v\n%s", err, stderr.String())
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "v1.json" {
		t.Fatalf("-trust-out wrote %v, want only v1.json", entries)
	}
	for _, forbidden := range []string{"uri-vectors", "driver-vectors", "bson-vectors"} {
		err := run([]string{"-trust-out", filepath.Join(t.TempDir(), forbidden)}, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), forbidden) {
			t.Errorf("-trust-out into %s: err = %v, want a refusal naming it", forbidden, err)
		}
	}
	err = run([]string{"-bson-out", filepath.Join(t.TempDir(), "trust-vectors")}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "trust-vectors") {
		t.Errorf("-bson-out into trust-vectors: err = %v, want a refusal naming it", err)
	}
	if err := run([]string{"-trust-out", out, "-bson-out", out}, &stdout, &stderr); err == nil {
		t.Error("-trust-out together with -bson-out was accepted")
	}
}

func TestTrustCheckFlag(t *testing.T) {
	committed := committedTrustCorpus(t)
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-trust-check", committedTrustCorpusPath}, &stdout, &stderr); err != nil {
		t.Fatalf("-trust-check on the committed file: %v", err)
	}
	altered := filepath.Join(t.TempDir(), "v1.json")
	if err := os.WriteFile(altered, bytes.Replace(committed, []byte(`"canonical":"db:7433"`), []byte(`"canonical":"db:07433"`), 1), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-trust-check", altered}, &stdout, &stderr); err == nil {
		t.Error("-trust-check accepted an altered file")
	}
}

// TestDriverVectorTrustCasesUnchanged: the known_servers cases in the frozen
// driver vectors still load as committed under the new parser, so the fix
// changes no driver-vectors/v1.json byte. trustVectors is the generator's own
// builder for them and needs no server.
func TestDriverVectorTrustCasesUnchanged(t *testing.T) {
	committed := committedVectors(t)
	var file vectorFile
	if err := json.Unmarshal(committed, &file); err != nil {
		t.Fatal(err)
	}
	want := make(map[string]string)
	for _, vector := range file.Vectors {
		if vector.Group == "trust" {
			line, err := json.Marshal(vector)
			if err != nil {
				t.Fatal(err)
			}
			want[vector.Name] = string(line)
		}
	}
	workDir, err := os.MkdirTemp(t.TempDir(), "trust-")
	if err != nil {
		t.Fatal(err)
	}
	regenerated, err := trustVectors(workDir)
	if err != nil {
		t.Fatalf("trustVectors: %v", err)
	}
	if len(regenerated) != len(want) {
		t.Fatalf("regenerated %d trust vectors, the committed file has %d", len(regenerated), len(want))
	}
	for _, vector := range regenerated {
		line, err := json.Marshal(vector)
		if err != nil {
			t.Fatal(err)
		}
		if want[vector.Name] != string(line) {
			t.Errorf("driver vector %q changed:\n got %s\nwant %s", vector.Name, line, want[vector.Name])
		}
	}
}
