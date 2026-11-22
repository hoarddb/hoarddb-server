package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
)

// generateOptions configures one generation.
type generateOptions struct {
	// log receives progress lines and, on a failure, the servers' log tails.
	log io.Writer
	// keepGoing captures every exchange and reports every failure together,
	// for a maintainer updating many cases at once.
	keepGoing bool
}

// The in-process server profiles. A vector captured on a tuned profile
// carries the profile's environment in server_env, so a harness replays it
// on a server started the same way.
var profiles = map[string]profile{
	"single": {name: "single"},
	// auth is the dedicated server of the authentication refusals: the
	// failed-login limiter counts per address, so they never share a server
	// with anything else (handbook auth.md section 4). A one-second token
	// with no skew lets a capture wait one out.
	"auth": {name: "auth", env: map[string]string{"HOARDDB_TOKEN_TTL": "1", "HOARDDB_AUTH_SKEW": "0"},
		apply: func(cfg *config.Config) { cfg.Auth.TokenTTLSeconds = 1; cfg.Auth.AuthSkewSeconds = 0 }},
	// ratelimit refuses an address after one failure.
	"ratelimit": {name: "ratelimit", env: map[string]string{"HOARDDB_TCP_AUTH_MAX_FAILS": "1"},
		apply: func(cfg *config.Config) { cfg.TCP.AuthMaxFails = 1 }},
	// limits shrinks the document and blob limits so a refusal fits in the file.
	"limits": {name: "limits", env: map[string]string{"HOARDDB_MAX_DOCUMENT_BYTES": "4096", "HOARDDB_MAX_BLOB_BYTES": "1024"},
		apply: func(cfg *config.Config) { cfg.Storage.MaxDocumentBytes = 4096; cfg.Storage.MaxBlobBytes = 1024 }},
	// overload allows one connection per address.
	"overload": {name: "overload", env: map[string]string{"HOARDDB_TCP_MAX_CONNS_PER_IP": "1"},
		apply: func(cfg *config.Config) { cfg.TCP.MaxConnsPerIP = 1 }},
	// results shrinks the query result ceiling to its minimum.
	"results": {name: "results", env: map[string]string{"HOARDDB_QUERY_MAX_RESULT_BYTES": "1048576"},
		apply: func(cfg *config.Config) { cfg.Query.MaxResultBytes = 1 << 20 }},
}

// generation is one run: its servers, its work directory and its ports.
type generation struct {
	opts     generateOptions
	progress *log.Logger
	workDir  string
	ports    *portBook
	keys     map[string]nodeKey
	servers  map[string]*inProcessServer
}

// generateFile runs every capture and returns the encoded file.
func generateFile(ctx context.Context, opts generateOptions) ([]byte, error) {
	if opts.log == nil {
		opts.log = io.Discard
	}
	progress := log.New(opts.log, "", 0)
	workDir, err := os.MkdirTemp("", "hoarddb-vectors-")
	if err != nil {
		return nil, fmt.Errorf("creating the work directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(workDir); err != nil {
			progress.Printf("removing %s: %v", workDir, err)
		}
	}()

	// Parts of the server log through the default logger; keep them in the
	// work directory instead of the caller's terminal.
	defaultLog, err := os.Create(workDir + "/default.log")
	if err != nil {
		return nil, fmt.Errorf("creating the default log: %w", err)
	}
	previousDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(defaultLog, nil)))
	defer func() {
		slog.SetDefault(previousDefault)
		_ = defaultLog.Close()
	}()

	g := &generation{opts: opts, progress: progress, workDir: workDir, ports: &portBook{}, servers: make(map[string]*inProcessServer)}
	defer g.close()
	if err := g.makeKeys(); err != nil {
		return nil, err
	}

	vectors, err := frameVectors()
	if err != nil {
		return nil, err
	}
	var failures []error
	databaseName := func(number int) string { return fmt.Sprintf("v%03d", number) }
	for index, build := range exchangeCases() {
		number := index + 1
		db := databaseName(number)
		exchange := build(db)
		if exchange.retired {
			continue
		}
		progress.Printf("capturing %03d %s", number, exchange.name)
		captured, err := g.capture(ctx, number, db, exchange)
		if err != nil {
			failure := fmt.Errorf("capturing %q: %w", exchange.name, err)
			if !opts.keepGoing {
				return nil, failure
			}
			failures = append(failures, failure)
			continue
		}
		vectors = append(vectors, captured)
	}
	if len(failures) > 0 {
		return nil, errors.Join(failures...)
	}
	firstElement, err := g.captureFirstElementRefusal(ctx)
	if err != nil {
		return nil, fmt.Errorf("capturing the first-element refusal: %w", err)
	}
	decoded, err := decodeVectors(firstElement)
	if err != nil {
		return nil, err
	}
	vectors = append(vectors, decoded...)
	live, err := g.liveDecodeVectors(ctx)
	if err != nil {
		return nil, fmt.Errorf("capturing the connection refusals: %w", err)
	}
	vectors = append(vectors, live...)
	trusted, err := trustVectors(workDir)
	if err != nil {
		return nil, err
	}
	vectors = append(vectors, trusted...)
	ring, err := ringVectors()
	if err != nil {
		return nil, fmt.Errorf("building the ring vectors: %w", err)
	}
	vectors = append(vectors, ring...)

	if err := uniqueNames(vectors); err != nil {
		return nil, err
	}
	return encodeVectorFile(vectorFile{
		Format:        formatName,
		License:       "Apache-2.0",
		ServerCommit:  serverCommit(ctx),
		ServerVersion: server.Version,
		ServerEnv:     map[string]string{"HOARDDB_ROOT_USER": rootUser, "HOARDDB_ROOT_PASSWORD": rootPassword},
		Normalisations: []string{
			normaliseRequestIDs, normaliseVary, normaliseStored, normaliseAddresses,
			normaliseIdentities, normaliseDropped, normaliseMapError,
		},
		Vectors: vectors,
	})
}

// makeKeys derives every server identity of the run.
func (g *generation) makeKeys() error {
	seeds := map[string]byte{"single": 1}
	g.keys = make(map[string]nodeKey, len(seeds))
	for name, seed := range seeds {
		key, err := newNodeKey(seed)
		if err != nil {
			return err
		}
		g.keys[name] = key
	}
	return nil
}

// target is the endpoint an exchange runs on, started on first use, and what
// to do when the exchange is done with it.
func (g *generation) target(ctx context.Context, c exchangeCase) (endpoint, func(), error) {
	name := c.server
	if name == "" {
		name = "single"
	}
	chosen, known := profiles[name]
	if !known {
		return endpoint{}, nil, fmt.Errorf("no server profile %q", name)
	}
	if c.fresh {
		started, err := startInProcess(ctx, g.workDir, chosen, g.keys["single"], g.ports)
		if err != nil {
			return endpoint{}, nil, err
		}
		return started.endpoint, started.stop, nil
	}
	if running, ok := g.servers[name]; ok {
		return running.endpoint, func() {}, nil
	}
	started, err := startInProcess(ctx, g.workDir, chosen, g.keys["single"], g.ports)
	if err != nil {
		return endpoint{}, nil, err
	}
	g.servers[name] = started
	return started.endpoint, func() {}, nil
}

// service is a running in-process profile's server, for the steps a capture
// takes through the server's own API rather than the wire.
func (g *generation) service(name string) (*server.Service, error) {
	running, ok := g.servers[name]
	if !ok {
		return nil, fmt.Errorf("the %s server is not running", name)
	}
	return running.service, nil
}

func (g *generation) topologyOf(string) string { return "single" }

func (g *generation) serverEnvOf(serverName string) map[string]string {
	if chosen, ok := profiles[serverName]; ok && len(chosen.env) > 0 {
		return chosen.env
	}
	return nil
}

// replacer rewrites the run's ports and directory to fixed ones
// (normaliseAddresses).
func (g *generation) replacer() *strings.Replacer {
	pairs := []string{g.workDir, "/data"}
	actual := make([]int, 0, len(g.ports.actual))
	for port := range g.ports.actual {
		actual = append(actual, port)
	}
	slices.Sort(actual)
	for _, port := range actual {
		pairs = append(pairs, "127.0.0.1:"+strconv.Itoa(port), "127.0.0.1:"+strconv.Itoa(g.ports.actual[port]))
	}
	return strings.NewReplacer(pairs...)
}

// close stops every server the run started.
func (g *generation) close() {
	for _, running := range g.servers {
		running.stop()
	}
}

// serverCommit is the commit the server source was at, or "unknown" outside
// a git checkout.
func serverCommit(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// uniqueNames refuses two vectors with one name.
func uniqueNames(vectors []vector) error {
	seen := make(map[string]bool, len(vectors))
	for _, v := range vectors {
		if seen[v.Name] {
			return fmt.Errorf("two vectors are named %q", v.Name)
		}
		seen[v.Name] = true
	}
	return nil
}

// compareVectorFiles lists how generated differs from committed, by vector
// and field, ignoring server_commit (it names the commit the file was last
// generated at, which every later commit changes).
func compareVectorFiles(committed, generated []byte) ([]string, error) {
	before, err := decodeVectorFile(committed)
	if err != nil {
		return nil, fmt.Errorf("the committed file: %w", err)
	}
	after, err := decodeVectorFile(generated)
	if err != nil {
		return nil, fmt.Errorf("the generated file: %w", err)
	}
	var differences []string
	if before.Format != after.Format {
		differences = append(differences, fmt.Sprintf("format: %q is now %q", before.Format, after.Format))
	}
	if before.ServerVersion != after.ServerVersion {
		differences = append(differences, fmt.Sprintf("server_version: %q is now %q", before.ServerVersion, after.ServerVersion))
	}
	if !slices.Equal(before.Normalisations, after.Normalisations) {
		differences = append(differences, "normalisations: the list changed")
	}
	if fmt.Sprint(before.ServerEnv) != fmt.Sprint(after.ServerEnv) {
		differences = append(differences, fmt.Sprintf("server_env: %v is now %v", before.ServerEnv, after.ServerEnv))
	}
	beforeByName := make(map[string]vector, len(before.Vectors))
	for _, v := range before.Vectors {
		beforeByName[v.Name] = v
	}
	afterNames := make(map[string]bool, len(after.Vectors))
	for position, v := range after.Vectors {
		afterNames[v.Name] = true
		old, existed := beforeByName[v.Name]
		if !existed {
			differences = append(differences, fmt.Sprintf("vector %q is new", v.Name))
			continue
		}
		changed, err := compareVectors(old, v)
		if err != nil {
			return nil, err
		}
		differences = append(differences, changed...)
		if position < len(before.Vectors) && before.Vectors[position].Name != v.Name {
			differences = append(differences, fmt.Sprintf("vector %q moved to position %d", v.Name, position+1))
		}
	}
	for _, v := range before.Vectors {
		if !afterNames[v.Name] {
			differences = append(differences, fmt.Sprintf("vector %q is gone", v.Name))
		}
	}
	if len(differences) == 0 && string(committed) != string(withCommit(generated, before.ServerCommit, after.ServerCommit)) {
		differences = append(differences, "the files differ in their layout, not their content: regenerate")
	}
	return differences, nil
}

// withCommit puts the committed file's server_commit into a generated file,
// so the byte comparison ignores it.
func withCommit(generated []byte, committedCommit, generatedCommit string) []byte {
	return []byte(strings.Replace(string(generated), `"server_commit": "`+generatedCommit+`"`, `"server_commit": "`+committedCommit+`"`, 1))
}

// compareVectors names what changed in one vector: the request's and the
// answer's fields first, because that is the change a driver must make.
func compareVectors(before, after vector) ([]string, error) {
	var differences []string
	prefix := fmt.Sprintf("vector %q: ", after.Name)
	if before.Request != nil && after.Request != nil {
		for _, name := range added(before.Request.AllowedFields, after.Request.AllowedFields) {
			differences = append(differences, prefix+fmt.Sprintf("request type gained the field %q", name))
		}
		for _, name := range added(after.Request.AllowedFields, before.Request.AllowedFields) {
			differences = append(differences, prefix+fmt.Sprintf("request type lost the field %q", name))
		}
		differences = append(differences, compareFields(prefix+"request ", before.Request.PayloadHex, after.Request.PayloadHex)...)
	}
	if before.Response != nil && after.Response != nil {
		for _, name := range added(before.Response.Fields, after.Response.Fields) {
			differences = append(differences, prefix+fmt.Sprintf("answer type gained the field %q", name))
		}
		for _, name := range added(after.Response.Fields, before.Response.Fields) {
			differences = append(differences, prefix+fmt.Sprintf("answer type lost the field %q", name))
		}
		differences = append(differences, compareFields(prefix+"answer ", before.Response.PayloadHex, after.Response.PayloadHex)...)
	}
	if len(differences) > 0 {
		return differences, nil
	}
	beforeMembers, err := vectorMembers(before)
	if err != nil {
		return nil, err
	}
	afterMembers, err := vectorMembers(after)
	if err != nil {
		return nil, err
	}
	var changed []string
	for name := range mergedKeys(beforeMembers, afterMembers) {
		if string(beforeMembers[name]) != string(afterMembers[name]) {
			changed = append(changed, name)
		}
	}
	if len(changed) > 0 {
		slices.Sort(changed)
		differences = append(differences, prefix+"changed in "+strings.Join(changed, ", "))
	}
	return differences, nil
}

// vectorMembers is a vector's JSON members, each as its encoded value.
func vectorMembers(v vector) (map[string]json.RawMessage, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encoding vector %q: %w", v.Name, err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		return nil, fmt.Errorf("reading vector %q back: %w", v.Name, err)
	}
	return members, nil
}

// compareFields compares two hex payloads' top-level fields: gained, lost,
// and changed values.
func compareFields(prefix, beforeHex, afterHex string) []string {
	if beforeHex == afterHex {
		return nil
	}
	beforeDoc, beforeErr := decodeHexDocument(beforeHex)
	afterDoc, afterErr := decodeHexDocument(afterHex)
	if beforeErr != nil || afterErr != nil {
		return []string{prefix + "payload changed"}
	}
	var differences []string
	beforeNames, afterNames := documentNames(beforeDoc), documentNames(afterDoc)
	for _, name := range added(beforeNames, afterNames) {
		differences = append(differences, prefix+fmt.Sprintf("gained the field %q", name))
	}
	for _, name := range added(afterNames, beforeNames) {
		differences = append(differences, prefix+fmt.Sprintf("lost the field %q", name))
	}
	for _, name := range afterNames {
		if !slices.Contains(beforeNames, name) {
			continue
		}
		if beforeDoc.Lookup(name).String() != afterDoc.Lookup(name).String() {
			differences = append(differences, prefix+fmt.Sprintf("field %q changed: %s is now %s", name, beforeDoc.Lookup(name), afterDoc.Lookup(name)))
		}
	}
	if len(differences) == 0 {
		differences = append(differences, prefix+"payload bytes changed (field order or width)")
	}
	return differences
}

func decodeHexDocument(payloadHex string) (bson.Raw, error) {
	raw, err := decodeHex(payloadHex)
	if err != nil {
		return nil, err
	}
	if err := bson.Raw(raw).Validate(); err != nil {
		return nil, fmt.Errorf("not a BSON document: %w", err)
	}
	return bson.Raw(raw), nil
}

func documentNames(doc bson.Raw) []string {
	elements, err := doc.Elements()
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(elements))
	for _, element := range elements {
		names = append(names, element.Key())
	}
	return names
}

// added lists what after has and before lacks.
func added(before, after []string) []string {
	var gained []string
	for _, name := range after {
		if !slices.Contains(before, name) {
			gained = append(gained, name)
		}
	}
	return gained
}

func mergedKeys(left, right map[string]json.RawMessage) map[string]bool {
	keys := make(map[string]bool, len(left)+len(right))
	for key := range left {
		keys[key] = true
	}
	for key := range right {
		keys[key] = true
	}
	return keys
}

func joinLines(lines []string) string {
	return strings.Join(lines, "\n  ")
}

// requestsOn sends requests to a running in-process profile, each required
// to succeed: preparation that is not part of a vector.
func (g *generation) requestsOn(ctx context.Context, profileName string, requests []call) error {
	target, release, err := g.target(ctx, exchangeCase{server: profileName})
	if err != nil {
		return err
	}
	defer release()
	conn, err := dial(ctx, target)
	if err != nil {
		return err
	}
	defer conn.close()
	if err := conn.authenticate(); err != nil {
		return err
	}
	for position, request := range requests {
		payload, err := request.encode()
		if err != nil {
			return err
		}
		answer, err := conn.roundTrip(request.op, fixedRequestID(0), payload)
		if err != nil {
			return fmt.Errorf("preparation request %d (%s): %w", position+1, request.op, err)
		}
		if err := requireSetupSuccess(request.op, answer); err != nil {
			return fmt.Errorf("preparation request %d: %w", position+1, err)
		}
	}
	return nil
}
