package main

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	mongoUsers = "users"
	mongoPosts = "posts"
)

// mongoEngine drives MongoDB with the schema a competent developer would use:
// a users collection keyed by _id, and one posts collection with a compound
// {author:1, seq:-1} index that serves "newest N of an author" as an index
// range walk with no sort stage.
type mongoEngine struct {
	cfg     *config
	client  *mongo.Client
	users   *mongo.Collection
	posts   *mongo.Collection
	concern *writeconcern.WriteConcern
	// concernSource is concernFromURI or concernFromFlag.
	concernSource string
	facts         map[string]string
	// ids mints the time-ordered _id of feed entries, notifications, comments
	// and jobs.
	ids *timeOrderedIDs
}

// mongoWriteConcern maps the -mongo-wc values onto explicit concerns. w1j is
// spelled {w:1, j:true} rather than the driver's bare Journaled() so the
// request on the wire does not depend on a server default.
func mongoWriteConcern(name string) (*writeconcern.WriteConcern, error) {
	switch name {
	case "w1":
		return writeconcern.W1(), nil
	case "w1j":
		journaled := true
		return &writeconcern.WriteConcern{W: 1, Journal: &journaled}, nil
	case "majority":
		return writeconcern.Majority(), nil
	default:
		return nil, fmt.Errorf("unknown -mongo-wc %q: use w1, w1j or majority", name)
	}
}

func newMongoEngine(ctx context.Context, cfg *config) (*mongoEngine, error) {
	concern, source, err := resolveMongoConcern(cfg)
	if err != nil {
		return nil, err
	}
	preference, preferenceSource, err := resolveMongoReadPref(cfg)
	if err != nil {
		return nil, err
	}
	engine, err := connectMongoEngine(ctx, cfg, mongoClientOptions(cfg, concern, preference), concern, source)
	if err != nil {
		return nil, err
	}
	engine.facts["read_preference"] = preference.Mode().String()
	engine.facts["read_preference_source"] = preferenceSource
	return engine, nil
}

// Where a run's read preference came from, recorded with the run.
const (
	readPrefFromURI     = "uri"
	readPrefFromFlag    = "flag"
	readPrefFromRouting = "routing"
)

// resolveMongoReadPref picks the read preference as it picks the write concern:
// one named in the connection string wins, then -mongo-read-pref, and failing
// both the routing decides, primary under RP-A and nearest under RP-B (spec 3.5).
func resolveMongoReadPref(cfg *config) (*readpref.ReadPref, string, error) {
	if fromURI := options.Client().ApplyURI(cfg.MongoURI).ReadPreference; fromURI != nil {
		return fromURI, readPrefFromURI, nil
	}
	if cfg.MongoReadPref != "" {
		mode, err := readpref.ModeFromString(cfg.MongoReadPref)
		if err != nil {
			return nil, "", fmt.Errorf("-mongo-read-pref %q: %w; use primary, primaryPreferred, secondary, secondaryPreferred or nearest", cfg.MongoReadPref, err)
		}
		preference, err := readpref.New(mode)
		if err != nil {
			return nil, "", fmt.Errorf("-mongo-read-pref %q: %w", cfg.MongoReadPref, err)
		}
		return preference, readPrefFromFlag, nil
	}
	if cfg.Routing == routingB {
		return readpref.Nearest(), readPrefFromRouting, nil
	}
	return readpref.Primary(), readPrefFromRouting, nil
}

// Where a run's write concern came from, recorded with the run.
const (
	concernFromURI  = "uri"
	concernFromFlag = "flag"
)

// resolveMongoConcern picks the write concern: the one in the connection string
// (w= or journal=, as engines.sh writes it for the level under test) wins, so a
// client built from the string cannot run weaker than the harness stated;
// -mongo-wc applies only when the string carries none.
func resolveMongoConcern(cfg *config) (*writeconcern.WriteConcern, string, error) {
	if fromURI := options.Client().ApplyURI(cfg.MongoURI).WriteConcern; fromURI != nil {
		return fromURI, concernFromURI, nil
	}
	concern, err := mongoWriteConcern(cfg.MongoWC)
	return concern, concernFromFlag, err
}

// describeConcern spells a write concern for the run's record.
func describeConcern(concern *writeconcern.WriteConcern) string {
	journaled := concern.Journal != nil && *concern.Journal
	return fmt.Sprintf("w=%v,j=%v", concern.W, journaled)
}

// mongoClientOptions is the client the runs use. A pool of exactly -conc
// connections: each worker has one operation in flight, so the pool never
// queues and never exceeds the hoardDB socket count.
func mongoClientOptions(cfg *config, concern *writeconcern.WriteConcern, preference *readpref.ReadPref) *options.ClientOptions {
	clientOptions := options.Client().ApplyURI(cfg.MongoURI).
		SetMaxPoolSize(uint64(cfg.Conc)).SetMinPoolSize(uint64(cfg.Conc)).
		SetWriteConcern(concern).SetReadPreference(preference)
	if cfg.MongoTLS {
		clientOptions.SetTLSConfig(competitorTLSConfig())
	}
	if cfg.MongoNode != "" {
		// One member, no discovery: a read must come from that member, or the
		// convergence check would be told what the primary holds.
		clientOptions.SetHosts([]string{cfg.MongoNode}).SetDirect(true)
		clientOptions.ReplicaSet = nil
	}
	return clientOptions
}

// connectMongoEngine connects with clientOptions and reads the server's facts.
func connectMongoEngine(ctx context.Context, cfg *config, clientOptions *options.ClientOptions, concern *writeconcern.WriteConcern, source string) (*mongoEngine, error) {
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB at %s: %w", redactedHost(cfg.MongoURI), err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("pinging MongoDB (%s; check -mongo-uri): %w", redactedHost(cfg.MongoURI), err)
	}
	database := client.Database(cfg.Database)
	engine := &mongoEngine{
		cfg: cfg, client: client, concern: concern, concernSource: source, ids: newTimeOrderedIDs(),
		users: database.Collection(mongoUsers), posts: database.Collection(mongoPosts),
	}
	if engine.facts, err = engine.readFacts(ctx); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	return engine, nil
}

// readFacts records what decides when an acknowledged write is on disk.
func (m *mongoEngine) readFacts(ctx context.Context) (map[string]string, error) {
	admin := m.client.Database("admin")
	var buildInfo struct {
		Version string `bson:"version"`
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err != nil {
		return nil, fmt.Errorf("reading MongoDB buildInfo: %w", err)
	}
	var commitInterval bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "getParameter", Value: 1}, {Key: "journalCommitInterval", Value: 1}}).Decode(&commitInterval); err != nil {
		return nil, fmt.Errorf("reading MongoDB journalCommitInterval: %w", err)
	}
	var status struct {
		StorageEngine struct {
			Name string `bson:"name"`
		} `bson:"storageEngine"`
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&status); err != nil {
		return nil, fmt.Errorf("reading MongoDB serverStatus: %w", err)
	}
	return map[string]string{
		"version":               buildInfo.Version,
		"storage_engine":        status.StorageEngine.Name,
		"write_concern":         describeConcern(m.concern),
		"write_concern_source":  m.concernSource,
		"tls":                   fmt.Sprint(m.cfg.MongoTLS),
		"journalCommitInterval": fmt.Sprint(commitInterval["journalCommitInterval"]),
	}, nil
}

// target names the servers the run talks to: the one member of a convergence
// probe, else every host of the connection string.
func (m *mongoEngine) target() string {
	if m.cfg.MongoNode != "" {
		return m.cfg.MongoNode
	}
	return redactedHost(m.cfg.MongoURI)
}
func (m *mongoEngine) durability() map[string]string { return m.facts }
func (m *mongoEngine) desynced(error) bool           { return false }

func (m *mongoEngine) close() { _ = m.client.Disconnect(context.Background()) }

func (m *mongoEngine) newSession() (engineSession, error) { return &mongoSession{engine: m}, nil }

// setup creates both collections and the posts index. It refuses a database
// that already holds them, as the hoardDB engine does.
func (m *mongoEngine) setup(ctx context.Context) (schemaInfo, error) {
	database := m.client.Database(m.cfg.Database)
	existing, err := database.ListCollectionNames(ctx, bson.D{{Key: "name", Value: bson.D{{Key: "$in", Value: bson.A{mongoUsers, mongoPosts}}}}})
	if err != nil {
		return schemaInfo{}, fmt.Errorf("listing collections of %q: %w", m.cfg.Database, err)
	}
	if len(existing) > 0 {
		return schemaInfo{}, fmt.Errorf("database %q already holds %v; pass -attach to reuse it or drop it first", m.cfg.Database, existing)
	}
	for _, name := range []string{mongoUsers, mongoPosts} {
		if err := database.CreateCollection(ctx, name); err != nil {
			return schemaInfo{}, fmt.Errorf("creating collection %q: %w", name, err)
		}
	}
	// Unique, as the SQL primary key is: both engines pay for the same
	// guarantee that (author, seq) names one post.
	index := mongo.IndexModel{Keys: bson.D{{Key: "author", Value: 1}, {Key: "seq", Value: -1}}, Options: options.Index().SetUnique(true)}
	if _, err := m.posts.Indexes().CreateOne(ctx, index); err != nil {
		return schemaInfo{}, fmt.Errorf("creating index on %s: %w", mongoPosts, err)
	}
	return schemaInfo{
		Layout:  "single-collection",
		Detail:  []string{"users: _id = user id (string)", "posts: unique index {author: 1, seq: -1}, plus the implicit _id index"},
		Buckets: 2,
	}, nil
}

// attach reuses collections an earlier run created.
func (m *mongoEngine) attach(context.Context) (schemaInfo, error) {
	return schemaInfo{Layout: "single-collection", Detail: []string{"existing users and posts collections"}, Buckets: 2}, nil
}

type mongoSession struct {
	engine *mongoEngine
}

func (s *mongoSession) close() {}

func (s *mongoSession) opContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, s.engine.cfg.OpTimeout)
}

func (s *mongoSession) putProfile(parent context.Context, id string, profile map[string]interface{}) error {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	// The hoardDB profile carries its key twice ("_id_" and "userid"); here
	// the key is _id and "userid" stays, so the document keeps the same fields.
	document := make(map[string]interface{}, len(profile))
	for field, value := range profile {
		document[field] = value
	}
	delete(document, "_id_")
	document["_id"] = id
	if _, err := s.engine.users.InsertOne(ctx, document); err != nil {
		return fmt.Errorf("writing profile: %w", err)
	}
	return nil
}

func (s *mongoSession) writePost(parent context.Context, _ string, post map[string]interface{}) error {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	if _, err := s.engine.posts.InsertOne(ctx, post); err != nil {
		return fmt.Errorf("inserting post: %w", err)
	}
	return nil
}

func (s *mongoSession) attachState(parent context.Context, id string) (int64, int64, error) {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	profiles, err := s.engine.users.CountDocuments(ctx, bson.D{{Key: "_id", Value: id}})
	if err != nil {
		return 0, 0, fmt.Errorf("checking profile: %w", err)
	}
	if profiles == 0 {
		return 0, 0, fmt.Errorf("profile missing; -users is larger than the dataset or the database is not seeded")
	}
	total, err := s.countPosts(ctx, id)
	if err != nil {
		return 0, 0, err
	}
	var nextSeq int64
	if total > 0 {
		var newest postView
		newestQuery := options.FindOne().SetSort(bson.D{{Key: "seq", Value: -1}})
		if err := s.engine.posts.FindOne(ctx, bson.D{{Key: "author", Value: id}}, newestQuery).Decode(&newest); err != nil {
			return 0, 0, fmt.Errorf("reading newest post: %w", err)
		}
		nextSeq = newest.Seq + 1
	}
	return total, nextSeq, nil
}

func (s *mongoSession) countPosts(ctx context.Context, id string) (int64, error) {
	count, err := s.engine.posts.CountDocuments(ctx, bson.D{{Key: "author", Value: id}})
	if err != nil {
		return 0, fmt.Errorf("counting posts of %s: %w", id, err)
	}
	return count, nil
}

// readTimeline runs find({author}).sort({seq:-1}).limit(n) and a findOne on
// the profile. A sampled read brackets the page with two counts, both outside
// the clock, because without a multi-document transaction the page and a
// count cannot share a snapshot.
func (s *mongoSession) readTimeline(parent context.Context, request readRequest) (timelinePage, error) {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	var countBefore int64
	if request.Sampled {
		var err error
		if countBefore, err = s.countPosts(ctx, request.Author); err != nil {
			return timelinePage{}, err
		}
		request.Clock.start = time.Now()
	}
	findOptions := options.Find().SetSort(bson.D{{Key: "seq", Value: -1}}).SetLimit(int64(request.Limit))
	cursor, err := s.engine.posts.Find(ctx, bson.D{{Key: "author", Value: request.Author}}, findOptions)
	if err != nil {
		return timelinePage{}, fmt.Errorf("finding timeline %s: %w", request.Author, err)
	}
	defer func() { _ = cursor.Close(ctx) }() // a close failure cannot change a read that already completed
	var documents []bson.Raw
	for cursor.Next(ctx) {
		documents = append(documents, bytes.Clone(cursor.Current))
	}
	if err := cursor.Err(); err != nil {
		return timelinePage{}, fmt.Errorf("reading timeline %s: %w", request.Author, err)
	}
	profile, err := s.engine.users.FindOne(ctx, bson.D{{Key: "_id", Value: request.Author}}).Raw()
	if err != nil {
		return timelinePage{}, fmt.Errorf("finding profile %s: %w", request.Author, err)
	}
	if !request.Sampled {
		return timelinePage{}, nil
	}
	request.Clock.stop = time.Now()
	return s.verificationPage(ctx, request.Author, countBefore, documents, profile)
}

func (s *mongoSession) verificationPage(ctx context.Context, id string, countBefore int64, documents []bson.Raw, profile bson.Raw) (timelinePage, error) {
	countAfter, err := s.countPosts(ctx, id)
	if err != nil {
		return timelinePage{}, err
	}
	posts := make([]postView, len(documents))
	for i, document := range documents {
		if err := bson.Unmarshal(document, &posts[i]); err != nil {
			return timelinePage{}, fmt.Errorf("timeline %s: decoding post %d: %w", id, i, err)
		}
	}
	var owner struct {
		UserID string `bson:"userid"`
	}
	if err := bson.Unmarshal(profile, &owner); err != nil {
		return timelinePage{}, fmt.Errorf("profile %s: decoding: %w", id, err)
	}
	return timelinePage{Posts: posts, TotalLow: countBefore, TotalHigh: countAfter, ProfileOwner: owner.UserID}, nil
}

// redactedHost returns host[:port] of a connection string, dropping any
// credentials and options so the report never carries a password.
func redactedHost(uri string) string {
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Host == "" {
		return "(unparsable uri)"
	}
	return parsed.Host
}
