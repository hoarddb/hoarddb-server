package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The trust corpus: clients/testdata/trust-vectors/v1.json. The known_servers
// rules of clients/handbook/trust.md section 4 (canonical host form, line
// grammar, lookup, saving, file safety) as data, every expected value
// produced by the server's own hoardDB/transport code, the code the CLI and
// the server's cluster store use. Only the inputs are written here; a case
// whose real outcome differs from the outcome this table states stops the
// generator, so the table cannot drift from the code either. The file is data,
// licensed Apache-2.0 under clients/; this generator imports server code and
// stays in the server's tree.
//
//	go run ./cmd/gen-testvectors -trust-out ../clients/testdata/trust-vectors
//	go run ./cmd/gen-testvectors -trust-check ../clients/testdata/trust-vectors/v1.json

// trustCorpusFormat is the file's format word. A breaking change to the shape
// is a new word and a new file, never an edit of this one.
const trustCorpusFormat = "hoarddb-trust-vectors/1"

// trustCorpusFileName is the one file -trust-out writes.
const trustCorpusFileName = "v1.json"

// trustCorpusFile is the whole file.
type trustCorpusFile struct {
	Format          string              `json:"format"`
	License         string              `json:"license"`
	ServerCommit    string              `json:"server_commit"`
	GeneratedBy     string              `json:"generated_by"`
	Rules           string              `json:"rules"`
	DefaultPort     int                 `json:"default_port"`
	LockWaitSeconds int                 `json:"lock_wait_seconds"`
	Keys            map[string]trustKey `json:"keys"`
	Groups          map[string]string   `json:"groups"`
	Vectors         []trustVector       `json:"vectors"`
}

// trustKey is one of the fixed keys the file texts are built from.
type trustKey struct {
	Hex         string `json:"hex"`
	Fingerprint string `json:"fingerprint"`
}

// trustVector is one entry; its group decides which members are set.
type trustVector struct {
	Name  string `json:"name"`
	Group string `json:"group"`
	Notes string `json:"notes,omitempty"`

	// canonical
	Input     *string `json:"input,omitempty"`
	Canonical string  `json:"canonical,omitempty"`
	Refused   bool    `json:"refused,omitempty"`

	// parse, lookup, save
	FileText *string            `json:"file_text,omitempty"`
	Entries  *map[string]string `json:"entries,omitempty"`

	// lookup and save
	Host                 string  `json:"host,omitempty"`
	PresentedKey         string  `json:"presented_key,omitempty"`
	Action               string  `json:"action,omitempty"`
	Key                  string  `json:"key,omitempty"`
	ResultText           *string `json:"result_text,omitempty"`
	RecordedFingerprint  string  `json:"recorded_fingerprint,omitempty"`
	PresentedFingerprint string  `json:"presented_fingerprint,omitempty"`

	// safety
	Target  string `json:"target,omitempty"`
	Kind    string `json:"kind,omitempty"`
	Symlink bool   `json:"symlink,omitempty"`
	Mode    string `json:"mode,omitempty"`
	Owner   string `json:"owner,omitempty"`

	// lookup: known, unknown or changed; safety: accept or refuse
	Expect        string   `json:"expect,omitempty"`
	ErrorContains []string `json:"error_contains,omitempty"`
}

// trustKeys are two fixed ed25519 keys, so equal inputs give equal bytes.
type trustKeys struct {
	a, b ed25519.PublicKey
}

func newTrustKeys() trustKeys {
	key := func(seedByte byte) ed25519.PublicKey {
		private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seedByte}, ed25519.SeedSize))
		return private.Public().(ed25519.PublicKey)
	}
	return trustKeys{a: key(0x41), b: key(0x42)}
}

// expand replaces {a} and {b} in a case's text with the keys' hex.
func (k trustKeys) expand(text string) string {
	return strings.NewReplacer("{a}", hex.EncodeToString(k.a), "{b}", hex.EncodeToString(k.b),
		"{A}", strings.ToUpper(hex.EncodeToString(k.a))).Replace(text)
}

func (k trustKeys) byName(name string) ed25519.PublicKey {
	if name == "b" {
		return k.b
	}
	return k.a
}

// ---- generation ------------------------------------------------------------

// generateTrustCorpus builds the whole file. It reads no clock and no random
// source, and every map is sorted by encoding/json, so equal inputs give equal
// bytes.
func generateTrustCorpus(commit string) ([]byte, error) {
	workDir, err := os.MkdirTemp("", "hoarddb-trust-vectors-")
	if err != nil {
		return nil, fmt.Errorf("creating a work directory: %w", err)
	}
	vectors, buildErr := buildTrustVectors(workDir)
	if removeErr := os.RemoveAll(workDir); removeErr != nil && buildErr == nil {
		buildErr = fmt.Errorf("removing %s: %w", workDir, removeErr)
	}
	if buildErr != nil {
		return nil, buildErr
	}
	keys := newTrustKeys()
	header := trustCorpusFile{
		Format:       trustCorpusFormat,
		License:      "Apache-2.0",
		ServerCommit: commit,
		GeneratedBy:  "github.com/hoarddb/hoarddb-server/cmd/gen-testvectors -trust-out (make trust-vectors), from hoardDB/transport's known_servers code",
		Rules:        "clients/handbook/trust.md section 4",
		DefaultPort:  transport.DefaultDriverPort,
		// The handbook's bound on waiting for another save's lock.
		LockWaitSeconds: 5,
		Keys: map[string]trustKey{
			"a": {Hex: hex.EncodeToString(keys.a), Fingerprint: transport.Fingerprint(keys.a)},
			"b": {Hex: hex.EncodeToString(keys.b), Fingerprint: transport.Fingerprint(keys.b)},
		},
		Groups: trustGroups(),
	}
	return encodeTrustCorpus(header, vectors)
}

func trustGroups() map[string]string {
	return map[string]string{
		"canonical": "input -> canonical, the host:port the address is recorded and looked up under; or refused: true (section 4.1)",
		"parse":     "file_text -> entries, exactly what the file loads to (host:port -> key hex, lowercase); or error_contains, every string the load refusal must name (section 4.2)",
		"lookup":    "file_text, host (as dialled) and presented_key -> expect: known, unknown, or changed with recorded_fingerprint and presented_fingerprint, both named by the refusal",
		"save":      "file_text (absent: no file yet), action (append: record a first-seen server; replace: accept a new key), host and key -> result_text, the exact file afterwards; error_contains when the save is refused (result_text is then the unchanged file) (section 4.4)",
		"safety":    "target (file or directory), kind, symlink, mode, owner (self, root or other) -> expect: accept, or refuse with error_contains, the fix the refusal names (section 4.3)",
	}
}

func buildTrustVectors(workDir string) ([]trustVector, error) {
	keys := newTrustKeys()
	var vectors []trustVector
	builders := []func() ([]trustVector, error){
		trustCanonicalVectors,
		func() ([]trustVector, error) { return trustParseVectors(workDir, keys) },
		func() ([]trustVector, error) { return trustLookupVectors(workDir, keys) },
		func() ([]trustVector, error) { return trustSaveVectors(workDir, keys) },
		func() ([]trustVector, error) { return trustSafetyVectors(workDir) },
	}
	for _, build := range builders {
		built, err := build()
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, built...)
	}
	seen := make(map[string]bool, len(vectors))
	for _, vector := range vectors {
		if seen[vector.Name] {
			return nil, fmt.Errorf("two trust vectors are named %q", vector.Name)
		}
		seen[vector.Name] = true
	}
	return vectors, nil
}

func trustCanonicalVectors() ([]trustVector, error) {
	var vectors []trustVector
	for _, tc := range trustCanonicalCases {
		input := tc.input
		vector := trustVector{Name: "canonical: " + tc.name, Group: "canonical", Input: &input, Notes: tc.notes}
		canonical, err := transport.CanonicalServerAddress(tc.input)
		switch {
		case tc.refused && err == nil:
			return nil, fmt.Errorf("canonical case %q: %q was accepted as %q, the table says it is refused", tc.name, tc.input, canonical)
		case tc.refused:
			vector.Refused = true
		case err != nil:
			return nil, fmt.Errorf("canonical case %q: %q was refused: %w", tc.name, tc.input, err)
		case canonical != tc.want:
			return nil, fmt.Errorf("canonical case %q: %q canonicalised to %q, the table says %q", tc.name, tc.input, canonical, tc.want)
		default:
			vector.Canonical = canonical
		}
		vectors = append(vectors, vector)
	}
	return vectors, nil
}

// writeCaseFile writes text as a known_servers file in its own 0700
// directory under workDir and returns the path.
func writeCaseFile(workDir, name string, text *string) (string, error) {
	dir, err := os.MkdirTemp(workDir, "case-")
	if err != nil {
		return "", fmt.Errorf("case %q: %w", name, err)
	}
	path := filepath.Join(dir, "known_servers")
	if text == nil {
		return path, nil
	}
	if err := os.WriteFile(path, []byte(*text), 0o600); err != nil {
		return "", fmt.Errorf("case %q: writing %s: %w", name, path, err)
	}
	return path, nil
}

func trustParseVectors(workDir string, keys trustKeys) ([]trustVector, error) {
	var vectors []trustVector
	for _, tc := range trustParseCases {
		text := keys.expand(tc.text)
		vector := trustVector{Name: "parse: " + tc.name, Group: "parse", FileText: &text, Notes: tc.notes}
		path, err := writeCaseFile(workDir, tc.name, &text)
		if err != nil {
			return nil, err
		}
		loaded, err := transport.NewKnownServers(path)
		if tc.errorContains != nil {
			if err == nil {
				return nil, fmt.Errorf("parse case %q loaded, the table says it is refused", tc.name)
			}
			vector.ErrorContains = expandAll(keys, tc.errorContains)
			if err := requireAll(err, vector.ErrorContains); err != nil {
				return nil, fmt.Errorf("parse case %q: %w", tc.name, err)
			}
			vectors = append(vectors, vector)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("parse case %q was refused: %w", tc.name, err)
		}
		entries := make(map[string]string)
		for host, key := range loaded.Entries() {
			entries[host] = hex.EncodeToString(key)
		}
		want := make(map[string]string, len(tc.entries))
		for host, keyName := range tc.entries {
			want[host] = hex.EncodeToString(keys.byName(keyName))
		}
		if !equalStringMaps(entries, want) {
			return nil, fmt.Errorf("parse case %q loaded %v, the table says %v", tc.name, entries, want)
		}
		vector.Entries = &entries
		vectors = append(vectors, vector)
	}
	return vectors, nil
}

func trustLookupVectors(workDir string, keys trustKeys) ([]trustVector, error) {
	text := keys.expand(trustLookupFile)
	path, err := writeCaseFile(workDir, "lookup", &text)
	if err != nil {
		return nil, err
	}
	loaded, err := transport.NewKnownServers(path)
	if err != nil {
		return nil, fmt.Errorf("loading the lookup file: %w", err)
	}
	names := map[transport.KnownServerMatch]string{
		transport.KnownServerKnown: "known", transport.KnownServerUnknown: "unknown", transport.KnownServerChanged: "changed",
	}
	var vectors []trustVector
	for _, tc := range trustLookupCases {
		presented := keys.byName(tc.key)
		match, recorded, err := loaded.Lookup(tc.host, presented)
		if err != nil {
			return nil, fmt.Errorf("lookup case %q: %w", tc.name, err)
		}
		if names[match] != tc.expect {
			return nil, fmt.Errorf("lookup case %q is %s, the table says %s", tc.name, names[match], tc.expect)
		}
		vector := trustVector{
			Name: "lookup: " + tc.name, Group: "lookup", FileText: &text, Host: tc.host,
			PresentedKey: hex.EncodeToString(presented), Expect: tc.expect, Notes: tc.notes,
		}
		if match == transport.KnownServerChanged {
			vector.RecordedFingerprint = transport.Fingerprint(recorded)
			vector.PresentedFingerprint = transport.Fingerprint(presented)
		}
		vectors = append(vectors, vector)
	}
	return vectors, nil
}

func trustSaveVectors(workDir string, keys trustKeys) ([]trustVector, error) {
	var vectors []trustVector
	for _, tc := range trustSaveCases {
		vector := trustVector{Name: "save: " + tc.name, Group: "save", Action: tc.action, Host: tc.host,
			Key: hex.EncodeToString(keys.byName(tc.key)), Notes: tc.notes}
		if tc.text != nil {
			text := keys.expand(*tc.text)
			vector.FileText = &text
		}
		path, err := writeCaseFile(workDir, tc.name, vector.FileText)
		if err != nil {
			return nil, err
		}
		store, err := transport.NewKnownServers(path)
		if err != nil {
			return nil, fmt.Errorf("save case %q: loading: %w", tc.name, err)
		}
		if tc.action == "replace" {
			err = store.Replace(tc.host, keys.byName(tc.key))
		} else {
			err = store.Trust(tc.host, keys.byName(tc.key))
		}
		switch {
		case tc.errorContains != nil && err == nil:
			return nil, fmt.Errorf("save case %q succeeded, the table says it is refused", tc.name)
		case tc.errorContains != nil:
			vector.ErrorContains = expandAll(keys, tc.errorContains)
			if err := requireAll(err, vector.ErrorContains); err != nil {
				return nil, fmt.Errorf("save case %q: %w", tc.name, err)
			}
		case err != nil:
			return nil, fmt.Errorf("save case %q was refused: %w", tc.name, err)
		}
		result, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("save case %q: reading the result: %w", tc.name, err)
		}
		resultText := string(result)
		if want := keys.expand(tc.result); resultText != want {
			return nil, fmt.Errorf("save case %q wrote %q, the table says %q", tc.name, resultText, want)
		}
		vector.ResultText = &resultText
		vectors = append(vectors, vector)
	}
	return vectors, nil
}

// trustSafetyUIDs stand in for the uids of a safety row: the decision only
// compares them.
const (
	trustSafetySelf  = 1000
	trustSafetyOther = 2000
)

func trustSafetyVectors(workDir string) ([]trustVector, error) {
	var vectors []trustVector
	for _, tc := range trustSafetyCases {
		facts, err := tc.facts()
		if err != nil {
			return nil, err
		}
		refusal := transport.TrustPathRefusal(facts)
		vector := trustVector{
			Name: "safety: " + tc.name, Group: "safety", Target: tc.target, Kind: tc.kind, Symlink: tc.symlink,
			Mode: tc.mode, Owner: tc.owner, Notes: tc.notes, Expect: "accept",
		}
		if tc.errorContains != nil {
			if refusal == nil {
				return nil, fmt.Errorf("safety case %q was accepted, the table says it is refused", tc.name)
			}
			if err := requireAll(refusal, tc.errorContains); err != nil {
				return nil, fmt.Errorf("safety case %q: %w", tc.name, err)
			}
			vector.Expect, vector.ErrorContains = "refuse", tc.errorContains
		} else if refusal != nil {
			return nil, fmt.Errorf("safety case %q was refused: %w", tc.name, refusal)
		}
		if tc.owner == "self" {
			if err := checkSafetyOnDisk(workDir, tc, refusal == nil); err != nil {
				return nil, err
			}
		}
		vectors = append(vectors, vector)
	}
	return vectors, nil
}

// checkSafetyOnDisk builds a row this process can build (its own files) and
// opens it with the real store, so the decision function and the code path
// that reads the file cannot disagree.
func checkSafetyOnDisk(workDir string, tc trustSafetyCase, accepted bool) error {
	if tc.kind == "fifo" || tc.kind == "not-a-directory" {
		return nil // the decision alone; building them proves nothing more about the rule
	}
	base, err := os.MkdirTemp(workDir, "safety-")
	if err != nil {
		return err
	}
	mode, err := parseOctalMode(tc.mode)
	if err != nil {
		return err
	}
	dir := filepath.Join(base, "hoarddb")
	if err := os.Mkdir(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "known_servers")
	switch tc.target {
	case "file":
		target := path
		if tc.symlink {
			target = filepath.Join(base, "elsewhere")
			if err := os.Symlink(target, path); err != nil {
				return err
			}
		}
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			return err
		}
		// A link's own mode is always 0777; the row's mode is the link's.
		if !tc.symlink {
			if err := os.Chmod(target, mode); err != nil {
				return err
			}
		}
	case "directory":
		if tc.symlink {
			linked := filepath.Join(base, "linked")
			if err := os.Symlink(dir, linked); err != nil {
				return err
			}
			path = filepath.Join(linked, "known_servers")
		}
		if err := os.Chmod(dir, mode); err != nil {
			return err
		}
	}
	_, loadErr := transport.NewKnownServers(path)
	if (loadErr == nil) != accepted {
		return fmt.Errorf("safety case %q: the decision says accepted=%v, but opening a real file gave %v", tc.name, accepted, loadErr)
	}
	return nil
}

func parseOctalMode(text string) (os.FileMode, error) {
	var mode uint32
	if _, err := fmt.Sscanf(text, "%o", &mode); err != nil {
		return 0, fmt.Errorf("mode %q is not octal: %w", text, err)
	}
	return os.FileMode(mode), nil
}

func expandAll(keys trustKeys, texts []string) []string {
	expanded := make([]string, 0, len(texts))
	for _, text := range texts {
		expanded = append(expanded, strings.NewReplacer(
			"{fa}", transport.Fingerprint(keys.a), "{fb}", transport.Fingerprint(keys.b)).Replace(text))
	}
	return expanded
}

func requireAll(err error, wanted []string) error {
	for _, want := range wanted {
		if !strings.Contains(err.Error(), want) {
			return fmt.Errorf("the refusal %q does not contain %q", err, want)
		}
	}
	return nil
}

func equalStringMaps(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

// ---- the file --------------------------------------------------------------

// encodeTrustCorpus writes the header indented and each vector on one line,
// as the BSON corpus does, so a diff reads one vector per line.
func encodeTrustCorpus(header trustCorpusFile, vectors []trustVector) ([]byte, error) {
	var headerText bytes.Buffer
	encoder := json.NewEncoder(&headerText)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(header); err != nil {
		return nil, fmt.Errorf("encoding the trust corpus header: %w", err)
	}
	text := headerText.String()
	const emptyVectors = "\"vectors\": null\n}\n"
	cut := strings.LastIndex(text, emptyVectors)
	if cut < 0 {
		return nil, errors.New("the trust corpus header does not end with its empty vector list")
	}
	var out bytes.Buffer
	out.WriteString(text[:cut])
	out.WriteString("\"vectors\": [\n")
	for position, vector := range vectors {
		var line bytes.Buffer
		lineEncoder := json.NewEncoder(&line)
		lineEncoder.SetEscapeHTML(false)
		if err := lineEncoder.Encode(vector); err != nil {
			return nil, fmt.Errorf("encoding vector %q: %w", vector.Name, err)
		}
		out.WriteString("    ")
		out.Write(bytes.TrimRight(line.Bytes(), "\n"))
		if position < len(vectors)-1 {
			out.WriteString(",")
		}
		out.WriteString("\n")
	}
	out.WriteString("  ]\n}\n")
	return out.Bytes(), nil
}

func decodeTrustCorpus(encoded []byte) (trustCorpusFile, error) {
	var corpus trustCorpusFile
	if err := json.Unmarshal(encoded, &corpus); err != nil {
		return trustCorpusFile{}, fmt.Errorf("decoding the trust corpus: %w", err)
	}
	return corpus, nil
}

// compareTrustCorpora lists how generated differs from committed, by vector
// name, ignoring server_commit.
func compareTrustCorpora(committed, generated []byte) ([]string, error) {
	before, err := decodeTrustCorpus(committed)
	if err != nil {
		return nil, fmt.Errorf("the committed file: %w", err)
	}
	after, err := decodeTrustCorpus(generated)
	if err != nil {
		return nil, fmt.Errorf("the generated file: %w", err)
	}
	oldByName := make(map[string]string, len(before.Vectors))
	for _, vector := range before.Vectors {
		line, err := json.Marshal(vector)
		if err != nil {
			return nil, fmt.Errorf("re-encoding committed vector %q: %w", vector.Name, err)
		}
		oldByName[vector.Name] = string(line)
	}
	var differences []string
	newNames := make(map[string]bool, len(after.Vectors))
	for _, vector := range after.Vectors {
		newNames[vector.Name] = true
		line, err := json.Marshal(vector)
		if err != nil {
			return nil, fmt.Errorf("re-encoding generated vector %q: %w", vector.Name, err)
		}
		old, existed := oldByName[vector.Name]
		switch {
		case !existed:
			differences = append(differences, fmt.Sprintf("vector %q is new", vector.Name))
		case old != string(line):
			differences = append(differences, fmt.Sprintf("vector %q changed", vector.Name))
		}
	}
	for _, vector := range before.Vectors {
		if !newNames[vector.Name] {
			differences = append(differences, fmt.Sprintf("vector %q is gone", vector.Name))
		}
	}
	if len(differences) == 0 {
		adjusted := strings.Replace(string(generated), `"server_commit": "`+after.ServerCommit+`"`, `"server_commit": "`+before.ServerCommit+`"`, 1)
		if adjusted != string(committed) {
			differences = append(differences, "the header or the layout differs: regenerate")
		}
	}
	return differences, nil
}

// runTrustCorpus is the -trust-out and -trust-check mode of the command.
func runTrustCorpus(outDir, checkPath string, stdout io.Writer) error {
	if outDir != "" {
		if err := refuseNonTrustVectorDirs(outDir); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	generated, err := generateTrustCorpus(serverCommit(ctx))
	if err != nil {
		return err
	}
	if checkPath != "" {
		committed, err := os.ReadFile(checkPath)
		if err != nil {
			return fmt.Errorf("reading the committed trust corpus: %w", err)
		}
		differences, err := compareTrustCorpora(committed, generated)
		if err != nil {
			return err
		}
		if len(differences) > 0 {
			return fmt.Errorf("%s does not match the known_servers code at this commit:\n  %s\nregenerate with 'make trust-vectors' and review the diff: a vector that changes is a change to every driver",
				checkPath, joinLines(differences))
		}
		_, err = fmt.Fprintf(stdout, "%s matches the known_servers code\n", checkPath)
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", outDir, err)
	}
	target := filepath.Join(outDir, trustCorpusFileName)
	if err := os.WriteFile(target, generated, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", target, err)
	}
	_, err = fmt.Fprintf(stdout, "wrote %s (%d bytes)\n", target, len(generated))
	return err
}

// refuseNonTrustVectorDirs keeps -trust-out from overwriting the other three
// vector files, one mistyped flag away.
func refuseNonTrustVectorDirs(outDir string) error {
	resolved, err := filepath.Abs(outDir)
	if err != nil {
		return fmt.Errorf("resolving -trust-out %q: %w", outDir, err)
	}
	for _, other := range []string{"uri-vectors", "driver-vectors", "bson-vectors"} {
		if strings.Contains(resolved, other) {
			return fmt.Errorf("refusing -trust-out %s: clients/testdata/%s is not this generator mode's file; use -trust-out clients/testdata/trust-vectors", outDir, other)
		}
	}
	return nil
}
