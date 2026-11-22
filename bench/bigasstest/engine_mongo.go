package main

// The MongoDB adapter of big-ass-test.
//
// Connection. -uri is a MongoDB connection string. When it asks for TLS (tls=true) the adapter
// connects with the certificate unchecked, the same rule the scenarios seeder uses for the
// self-signed benchmark server. Params, all optional, anything else is rejected:
//
//	w=N          write concern w, default 1
//	journal=B    write concern j, default true
//	level=L      L-A (default) or L-B. At L-A the adapter refuses w below 1 and journal=false,
//	             because the record is labelled with the durability level; say level=L-B to
//	             run the weaker settings on purpose.
//
// The write concern is set explicitly, w1j by default, the one the seeder uses, so the request
// does not depend on a server default. One client is shared by every virtual user; its pool is
// sized to Config.VUsers, so each user has a connection of its own while it has a request in
// flight. The login must be allowed on the four databases <base>_<layout>: use the root login
// engines.sh creates (authSource=admin), not the bench user, which has readWrite on one database.
//
// Layout. The adapter reads and writes the collections the scenarios seeder created (one
// collection per entity and per store, never an array inside an owner's document). Their names
// are hard-coded in engine_mongo_schema.go and checked against the live database, collections
// and index key lists, the first time a layout is opened; a mismatch stops the run before the
// first request.
//
// Order of writes. A standalone mongod has no transactions, so one logical write is a
// sequence of single-document writes, one round trip each, never pipelined: first the claim
// and its acknowledgement (agent.task), then the documents, then each element in the order the
// WriteOp lists them, then one $inc per document for the counters that apply (the deltas to
// one document are merged, as the SQL adapters send one UPDATE). A failure part way leaves what
// was already written in place and reports the error; the audit treats such a write as
// uncertain.
//
// Paired stores. The seeded schema keeps one row where hoardDB keeps two copies: User.Following
// and User.Followers are one follows row, Room.Members and User.Rooms one members row, Post.Likes
// and User.Likes one likes row. The row is written once (by the Follow or Member document, or by
// the Post.Likes element), and the other side of the pair is counted and read through its own
// index.
//
// Elements. A fifo element is inserted with seq = SeqBase + Ordinal. A keyed element is put
// with a replacing upsert on (owner, key), and the write is "newly created" when the upsert
// inserted. Follow and Member documents are upserts that set the non-key fields, so a repeat replaces them. A
// Counters delta is a $inc of the document and is applied only when the element it names was
// newly created; a paired element is created exactly when its row was.
//
// Claim. agent.task finds the oldest task of the run with state 0 and sets state 1 in one
// findOneAndUpdate, then sets state 2 (the acknowledgement), then inserts the replacement.
// A count of Run.Tasks leaves out state 2, so it equals the tasks still in the queue.
// Written tasks sort by seq = SeqBase + ordinal, so a replacement from a lower-numbered virtual user is claimed before an older one from a higher-numbered user; hoardDB claims in arrival order.
//
// Reads. The feed's posts and a room's users are one $in query per page (two round trips for
// the feed), as a Mongo application reads references. Pages sort by the (owner, seq) or (owner, key) index, oldest first. A Reply counts
// one round trip per command; a read that returns more than one 10,000-document batch would
// add getMore round trips the count does not include.

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func init() { register("mongo", openMongo) }

// mongoEngine is one MongoDB server under test.
type mongoEngine struct {
	client  *mongo.Client
	uri     connstring.ConnString
	tlsOn   bool
	tlsConf *tls.Config
	vusers  int
	writeC  string
	level   string

	mu      sync.Mutex
	checked map[string]error
}

// mongoConcern is the write concern the params ask for.
type mongoConcern struct {
	w       int
	journal bool
	level   string
}

// describe is the write concern as the record shows it.
func (c mongoConcern) describe() string {
	return "w=" + strconv.Itoa(c.w) + ",j=" + strconv.FormatBool(c.journal)
}

// parseMongoParams reads the params and refuses a write concern that weakens the durability
// level it is labelled with.
func parseMongoParams(params map[string]string) (mongoConcern, error) {
	concern := mongoConcern{w: 1, journal: true, level: "L-A"}
	for name, text := range params {
		switch name {
		case "w":
			n, err := strconv.Atoi(text)
			if err != nil || n < 0 {
				return mongoConcern{}, fmt.Errorf("-param w=%q is not a write concern count. Use a whole number such as w=1", text)
			}
			concern.w = n
		case "journal":
			on, err := strconv.ParseBool(text)
			if err != nil {
				return mongoConcern{}, fmt.Errorf("-param journal=%q is not true or false. Use journal=true or journal=false", text)
			}
			concern.journal = on
		case "level":
			if text != "L-A" && text != "L-B" {
				return mongoConcern{}, fmt.Errorf("-param level=%q is not a durability level. Use level=L-A or level=L-B", text)
			}
			concern.level = text
		default:
			return mongoConcern{}, fmt.Errorf("the mongo adapter has no parameter %q. It reads w=N, journal=true|false and level=L-A|L-B", name)
		}
	}
	if concern.level == "L-A" && (concern.w < 1 || !concern.journal) {
		return mongoConcern{}, fmt.Errorf("write concern %s weakens durability level L-A (it needs w of at least 1 and journal=true). Drop the -param, or add -param level=L-B to run the weaker setting on purpose and label the run L-B", concern.describe())
	}
	return concern, nil
}

// openMongo connects and pings. Unknown params are refused.
func openMongo(ctx context.Context, cfg Config) (Engine, error) {
	concern, err := parseMongoParams(cfg.Params)
	if err != nil {
		return nil, err
	}
	writeW, journal := concern.w, concern.journal
	if cfg.URI == "" {
		return nil, errors.New("the mongo adapter needs -uri, for example mongodb://user:password@host:27017/?tls=true&authSource=admin")
	}
	parsed, err := connstring.Parse(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("parsing the MongoDB -uri: %w", err)
	}
	vusers := max(cfg.VUsers, 1)
	clientOptions := options.Client().ApplyURI(cfg.URI).
		SetMaxPoolSize(uint64(vusers) + 1).
		SetWriteConcern(&writeconcern.WriteConcern{W: writeW, Journal: &journal})
	engine := &mongoEngine{uri: *parsed, tlsOn: parsed.SSL, vusers: vusers, checked: map[string]error{},
		writeC: concern.describe(), level: concern.level}
	if engine.tlsOn {
		engine.tlsConf = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, //nolint:gosec // self-signed benchmark server, the rule the seeder uses
		}
		clientOptions.SetTLSConfig(engine.tlsConf)
	}
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB (check -uri): %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("pinging MongoDB (check -uri, its login and tls=true): %w", err)
	}
	engine.client = client
	return engine, nil
}

// Settings reads back the server's own configuration. engine_version and tls are required;
// a server that refuses an optional read (the login lacks the monitoring role) is recorded
// under <key>_error instead of failing the run.
func (e *mongoEngine) Settings(ctx context.Context) (map[string]string, error) {
	out := map[string]string{
		"write_concern":        e.writeC,
		"write_concern_level":  e.level,
		"write_concern_source": "set explicitly on the client, so the server default does not apply",
		"pool_size":            strconv.Itoa(e.vusers + 1),
	}
	var build struct {
		Version string `bson:"version"`
	}
	if err := e.client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&build); err != nil {
		return nil, fmt.Errorf("reading the MongoDB version with buildInfo: %w", err)
	}
	out["engine_version"] = build.Version
	cipher, err := e.cipher(ctx)
	if err != nil {
		return nil, err
	}
	out["tls"] = cipher
	var status struct {
		StorageEngine struct {
			Name string `bson:"name"`
		} `bson:"storageEngine"`
		WiredTiger struct {
			Cache map[string]any `bson:"cache"`
		} `bson:"wiredTiger"`
	}
	if err := e.client.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&status); err != nil {
		out["server_status_error"] = err.Error()
	} else {
		out["storage_engine"] = status.StorageEngine.Name
		if cacheMax, ok := status.WiredTiger.Cache["maximum bytes configured"]; ok {
			out["cache_bytes"] = fmt.Sprint(cacheMax)
		}
	}
	var cmdline struct {
		Parsed bson.M `bson:"parsed"`
	}
	if err := e.client.Database("admin").RunCommand(ctx, bson.D{{Key: "getCmdLineOpts", Value: 1}}).Decode(&cmdline); err != nil {
		out["cmdline_error"] = err.Error()
	} else {
		for key, value := range mongoCmdlineSettings(cmdline.Parsed) {
			out[key] = value
		}
	}
	var compat struct {
		Version struct {
			Version string `bson:"version"`
		} `bson:"featureCompatibilityVersion"`
	}
	if err := e.client.Database("admin").RunCommand(ctx, bson.D{{Key: "getParameter", Value: 1}, {Key: "featureCompatibilityVersion", Value: 1}}).Decode(&compat); err != nil {
		out["feature_compatibility_error"] = err.Error()
	} else {
		out["feature_compatibility"] = compat.Version.Version
	}
	return out, nil
}

// mongoCmdlineAllowed are the prefixes of the command-line options the record keeps:
// storage, durability, replication and TLS facts. Nothing else is copied, so no bind address,
// path or credential reaches a record.
var mongoCmdlineAllowed = []string{
	"storage.engine", "storage.journal.", "storage.syncPeriodSecs", "storage.wiredTiger.",
	"replication.", "net.tls.mode", "net.tls.disabledProtocols", "net.ssl.mode",
}

// bsonDToM copies an ordered document into a map, one level deep, as the deprecated bson.D.Map did.
func bsonDToM(ordered bson.D) bson.M {
	unordered := make(bson.M, len(ordered))
	for _, element := range ordered {
		unordered[element.Key] = element.Value
	}
	return unordered
}

// mongoCmdlineSettings flattens the parsed getCmdLineOpts document into cmdline.<dotted key>
// entries, keeping only allowlisted keys whose name does not suggest a file or a directory.
func mongoCmdlineSettings(parsed bson.M) map[string]string {
	out := map[string]string{}
	var walk func(prefix string, node bson.M)
	walk = func(prefix string, node bson.M) {
		for name, value := range node {
			key := prefix + name
			if child, ok := value.(bson.M); ok {
				walk(key+".", child)
				continue
			}
			if child, ok := value.(bson.D); ok {
				walk(key+".", bsonDToM(child))
				continue
			}
			lower := strings.ToLower(key)
			if strings.Contains(lower, "file") || strings.Contains(lower, "path") || strings.Contains(lower, "directory") {
				continue
			}
			for _, allowed := range mongoCmdlineAllowed {
				if strings.HasPrefix(key, allowed) {
					out["cmdline."+key] = fmt.Sprint(value)
					break
				}
			}
		}
	}
	walk("", parsed)
	return out
}

// cipher returns the negotiated TLS version and cipher suite, from a probe connection to the
// first host of the URI, or "off".
func (e *mongoEngine) cipher(ctx context.Context) (string, error) {
	if !e.tlsOn {
		return "off", nil
	}
	if len(e.uri.Hosts) == 0 {
		return "", errors.New("the MongoDB -uri names no host to probe for its TLS cipher")
	}
	dialer := tls.Dialer{Config: e.tlsConf, NetDialer: &net.Dialer{}}
	conn, err := dialer.DialContext(ctx, "tcp", e.uri.Hosts[0])
	if err != nil {
		return "", fmt.Errorf("probing the TLS cipher of %s: %w", e.uri.Hosts[0], err)
	}
	defer func() { _ = conn.Close() }() // the probe sent nothing worth flushing
	state := conn.(*tls.Conn).ConnectionState()
	return tls.VersionName(state.Version) + " " + tls.CipherSuiteName(state.CipherSuite), nil
}

// Session shares the engine's pool; Config.VUsers sized it so every user has a connection.
func (e *mongoEngine) Session(context.Context, int) (Session, error) {
	return &mongoSession{engine: e}, nil
}

// Close disconnects the client.
func (e *mongoEngine) Close() error { return e.client.Disconnect(context.Background()) }

type mongoSession struct{ engine *mongoEngine }

// Layout opens the operations of one layout, after checking the database once per run.
func (s *mongoSession) Layout(layout model.Layout, database string) (LayoutOps, error) {
	spec, ok := mongoLayouts[layout]
	if !ok {
		return nil, fmt.Errorf("the MongoDB adapter does not know layout %s", layout)
	}
	if err := s.engine.checkDatabase(database, spec); err != nil {
		return nil, err
	}
	return &mongoOps{db: s.engine.client.Database(database), layout: layout, spec: spec}, nil
}

func (s *mongoSession) Close() error { return nil }

// checkDatabase verifies, once per database, that every collection and index the adapter uses
// exists, and says how to seed it when not.
func (e *mongoEngine) checkDatabase(database string, spec mongoLayout) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err, done := e.checked[database]; done {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), mongoSchemaCheckTimeout)
	defer cancel()
	err := e.verifySchema(ctx, database, spec)
	e.checked[database] = err
	return err
}

// mongoSchemaCheckTimeout bounds the schema check that holds the engine's mutex.
const mongoSchemaCheckTimeout = time.Minute

func (e *mongoEngine) verifySchema(ctx context.Context, database string, spec mongoLayout) error {
	db := e.client.Database(database)
	have, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("listing the collections of database %q (check the login can read it): %w", database, err)
	}
	present := map[string]bool{}
	for _, name := range have {
		present[name] = true
	}
	required := spec.required()
	names := make([]string, 0, len(required))
	for name := range required {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !present[name] {
			return fmt.Errorf("database %q has no collection %q, so it was not seeded by this version of the scenarios binary. Seed it with: bigasstest seed -engine mongo", database, name)
		}
		if len(required[name]) == 0 {
			continue
		}
		cursor, err := db.Collection(name).Indexes().List(ctx)
		if err != nil {
			return fmt.Errorf("listing the indexes of %s.%s: %w", database, name, err)
		}
		var found [][]string
		for cursor.Next(ctx) {
			elements, err := cursor.Current.Lookup("key").Document().Elements()
			if err != nil {
				_ = cursor.Close(ctx)
				return fmt.Errorf("reading an index of %s.%s: %w", database, name, err)
			}
			keys := make([]string, len(elements))
			for i, element := range elements {
				keys[i] = element.Key()
			}
			found = append(found, keys)
		}
		if err := cursor.Close(ctx); err != nil {
			return fmt.Errorf("listing the indexes of %s.%s: %w", database, name, err)
		}
		if err := checkMongoIndexes(name, required[name], found); err != nil {
			return fmt.Errorf("database %q: %w. Seed it again with: bigasstest seed -engine mongo", database, err)
		}
	}
	return nil
}
