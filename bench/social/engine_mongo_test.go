package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// commandLog records the commands one test database receives: their names,
// their write concerns, and how many were in flight at the same moment.
type commandLog struct {
	mu            sync.Mutex
	database      string
	names         []string
	writeConcerns []bson.Raw
	inFlight      int
	peakInFlight  int
}

func (l *commandLog) monitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(_ context.Context, started *event.CommandStartedEvent) {
			if started.DatabaseName != l.database {
				return
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			l.names = append(l.names, started.CommandName)
			if concern, err := started.Command.LookupErr("writeConcern"); err == nil {
				if document, ok := concern.DocumentOK(); ok {
					l.writeConcerns = append(l.writeConcerns, slices.Clone(document))
				}
			}
			l.inFlight++
			l.peakInFlight = max(l.peakInFlight, l.inFlight)
		},
		Succeeded: func(_ context.Context, finished *event.CommandSucceededEvent) {
			l.finish(finished.CommandFinishedEvent)
		},
		Failed: func(_ context.Context, finished *event.CommandFailedEvent) { l.finish(finished.CommandFinishedEvent) },
	}
}

func (l *commandLog) finish(finished event.CommandFinishedEvent) {
	if finished.DatabaseName != l.database {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inFlight--
}

// mark returns how many commands have been logged so far.
func (l *commandLog) mark() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.names)
}

// since returns the command names logged after mark.
func (l *commandLog) since(mark int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.names[mark:])
}

// liveMongoConfig names a scratch database on the server SOCIAL_LIVE_MONGO_URI
// points at, or skips the test.
func liveMongoConfig(t *testing.T) *config {
	t.Helper()
	uri := os.Getenv("SOCIAL_LIVE_MONGO_URI")
	if uri == "" {
		t.Skip("set SOCIAL_LIVE_MONGO_URI to run against a local mongod (TLS and a login, as scripts/certification/engines.sh starts it)")
	}
	return &config{
		Engine: engineMongo, MongoURI: uri, MongoWC: "w1j", Database: fmt.Sprintf("social_live_%d", time.Now().UnixNano()),
		Users: tinySpec.Users, Conc: 4, OpTimeout: 30 * time.Second,
	}
}

// withWriteConcern returns uri with its w and journal parameters replaced:
// an empty value removes the parameter.
func withWriteConcern(t *testing.T, uri, w, journal string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("parsing the live URI: %v", err)
	}
	query := parsed.Query()
	for name, value := range map[string]string{"w": w, "journal": journal} {
		query.Del(name)
		if value != "" {
			query.Set(name, value)
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// newMongoWorld connects with the production client options plus a command
// monitor, and drops the scratch database when the test ends.
func newMongoWorld(t *testing.T, seeded bool) (*socialWorld, *commandLog) {
	t.Helper()
	return newMongoWorldWith(t, seeded, func(cfg *config) {})
}

// newMongoWorldWith is newMongoWorld after adjust has changed the config.
func newMongoWorldWith(t *testing.T, seeded bool, adjust func(*config)) (*socialWorld, *commandLog) {
	t.Helper()
	cfg := liveMongoConfig(t)
	adjust(cfg)
	log := &commandLog{database: cfg.Database}
	concern, source, err := resolveMongoConcern(cfg)
	if err != nil {
		t.Fatal(err)
	}
	preference, _, err := resolveMongoReadPref(cfg)
	if err != nil {
		t.Fatalf("resolving the read preference: %v", err)
	}
	engine, err := connectMongoEngine(context.Background(), cfg, mongoClientOptions(cfg, concern, preference).SetMonitor(log.monitor()), concern, source)
	if err != nil {
		t.Fatalf("connecting to the local mongod: %v", err)
	}
	t.Cleanup(func() {
		if err := engine.client.Database(cfg.Database).Drop(context.Background()); err != nil {
			t.Logf("dropping %s: %v", cfg.Database, err)
		}
		engine.close()
	})
	world := &socialWorld{social: engine}
	if seeded {
		world.seed(t)
	}
	return world, log
}

func TestMongoOperationContract(t *testing.T) {
	world, _ := newMongoWorld(t, true)
	checkOperationContract(t, world)
}

// TestMongoCanonicalUserMatchesGraph: the seeded MongoDB dataset hashes to
// what the generator says it holds, as hoardDB's does (P4).
// TestMongoMergedTimelineIsTheTrueNewest is the M1 ruling on MongoDB.
func TestMongoMergedTimelineIsTheTrueNewest(t *testing.T) {
	world, _ := newMongoWorld(t, true)
	checkMergedTimelineIsTrueNewest(t, world)
}

func TestMongoCanonicalUserMatchesGraph(t *testing.T) {
	world, _ := newMongoWorld(t, true)
	checkCanonicalMatchesGraph(t, world)
}

// expectedMongoIndexes is spec 6.2's index column by the names MongoDB gives
// them. The likes collection has no index besides _id.
var expectedMongoIndexes = map[string][]string{
	mongoUsers:         {"_id_"},
	mongoPosts:         {"_id_", "author_1_seq_-1"},
	mongoLikes:         {"_id_"},
	mongoComments:      {"_id_", "post_1__id_1"},
	mongoFollows:       {"_id_", "follower_1_followee_1", "followee_1_follower_1"},
	mongoFeed:          {"_id_", "user_1__id_-1"},
	mongoNotifications: {"_id_", "user_1__id_1"},
	mongoMedia:         {"_id_"},
	mongoJobs:          {"_id_", "outbox_1_lease_until_1__id_1"},
}

// TestMongoSchemaMatchesSpec: the collections and indexes of spec 6.2 exactly,
// and no index on likes.
func TestMongoSchemaMatchesSpec(t *testing.T) {
	declared := map[string][]string{}
	for _, name := range mongoSocialCollections {
		declared[name] = []string{"_id_"}
	}
	for _, spec := range mongoSocialIndexes {
		name := ""
		for _, key := range spec.Keys {
			name += fmt.Sprintf("%s_%v_", key.Key, key.Value)
		}
		declared[spec.Collection] = append(declared[spec.Collection], name[:len(name)-1])
	}
	for name, want := range expectedMongoIndexes {
		if !slices.Equal(declared[name], want) {
			t.Errorf("collection %s declares indexes %v; spec 6.2 wants %v", name, declared[name], want)
		}
	}
	if len(declared) != len(expectedMongoIndexes) {
		t.Errorf("declared %d collections, spec 6.2 has %d", len(declared), len(expectedMongoIndexes))
	}

	t.Run("live", func(t *testing.T) {
		world, _ := newMongoWorld(t, false)
		if _, err := world.social.setupSocial(context.Background(), tinySpec); err != nil {
			t.Fatalf("setupSocial: %v", err)
		}
		engine := world.social.(*mongoEngine)
		for name, want := range expectedMongoIndexes {
			specs, err := engine.collection(name).Indexes().ListSpecifications(context.Background())
			if err != nil {
				t.Fatalf("listing indexes of %s: %v", name, err)
			}
			var got []string
			for _, index := range specs {
				got = append(got, index.Name)
			}
			slices.Sort(got)
			wantSorted := slices.Sorted(slices.Values(want))
			if !slices.Equal(got, wantSorted) {
				t.Errorf("%s has indexes %v; want %v", name, got, wantSorted)
			}
		}
	})
}

// TestMongoSeedKeepsOneLikePerTriple: a like is the unique (post author, post
// sequence, liker); the seed keeps every comment and every self-like, and the
// per-post counter is the unique count (P4).
func TestMongoSeedKeepsOneLikePerTriple(t *testing.T) {
	world, _ := newMongoWorld(t, true)
	engine := world.social.(*mongoEngine)
	ctx := context.Background()
	likeCounts, commentCounts := seededReactionCounts(world.graph)
	var wantLikes, wantComments, counterLikes int64
	for i := range likeCounts {
		wantLikes += int64(likeCounts[i])
		wantComments += int64(commentCounts[i])
	}
	if wantLikes >= int64(tinySpec.Likes) {
		t.Fatalf("the tiny graph holds no repeated like (%d unique of %d), so it cannot show the dedupe", wantLikes, tinySpec.Likes)
	}
	gotLikes, err := engine.collection(mongoLikes).CountDocuments(ctx, bson.D{})
	if err != nil || gotLikes != wantLikes {
		t.Errorf("likes collection holds %d, %v; want %d unique", gotLikes, err, wantLikes)
	}
	gotComments, err := engine.collection(mongoComments).CountDocuments(ctx, bson.D{})
	if err != nil || gotComments != wantComments || gotComments != int64(tinySpec.Comments) {
		t.Errorf("comments collection holds %d, %v; want all %d", gotComments, err, tinySpec.Comments)
	}
	cursor, err := engine.collection(mongoPosts).Find(ctx, bson.D{})
	if err != nil {
		t.Fatal(err)
	}
	var posts []countedPost
	if err := cursor.All(ctx, &posts); err != nil {
		t.Fatal(err)
	}
	for _, post := range posts {
		counterLikes += post.Likes
	}
	if counterLikes != wantLikes {
		t.Errorf("the posts' likes_n add up to %d; want the unique count %d", counterLikes, wantLikes)
	}
}

func duplicateKeyError() error {
	return mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: mongoDuplicateKey, Message: "E11000 duplicate key"}}}}
}

// TestIgnoreDuplicateKeysToleratesNothingElse: a duplicate key is the engine
// deduping; every other failure of an insertMany still fails the seed.
func TestIgnoreDuplicateKeysToleratesNothingElse(t *testing.T) {
	other := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Code: mongoDuplicateKey}}, {WriteError: mongo.WriteError{Code: 121, Message: "document failed validation"}},
	}}
	concern := mongo.BulkWriteException{WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "waiting for replication timed out"}}
	cases := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{"no error", nil, false},
		{"only duplicate keys", duplicateKeyError(), false},
		{"a duplicate and another write error", other, true},
		{"a write concern failure", concern, true},
		{"a network error", errors.New("connection reset"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ignoreDuplicateKeys(tc.err); (got != nil) != tc.wantErr {
				t.Errorf("ignoreDuplicateKeys = %v; want an error: %v", got, tc.wantErr)
			}
		})
	}
}

// TestMongoDuplicateLikeAddsNothingButStillNotifies: the same user liking the
// same post twice is one like (P4): the second call succeeds, likes_n does not
// move, and the author is still notified, as hoardDB's idempotent put plus
// inbox push does.
func TestMongoDuplicateLikeAddsNothingButStillNotifies(t *testing.T) {
	world, _ := newMongoWorld(t, true)
	engine := world.social.(*mongoEngine)
	ctx := context.Background()
	ops := world.ops(t)
	const liker, author, seq = 40, 3, 1
	notifications := func() int64 {
		count, err := engine.collection(mongoNotifications).CountDocuments(ctx, bson.D{{Key: "user", Value: userID(author)}})
		if err != nil {
			t.Fatal(err)
		}
		return count
	}
	if err := ops.likePost(ctx, liker, author, seq); err != nil {
		t.Fatalf("first like: %v", err)
	}
	afterFirst, err := ops.postPage(ctx, author, seq)
	if err != nil {
		t.Fatal(err)
	}
	noticesAfterFirst := notifications()
	if err := ops.likePost(ctx, liker, author, seq); err != nil {
		t.Fatalf("the same like a second time returned an error: %v", err)
	}
	afterSecond, err := ops.postPage(ctx, author, seq)
	if err != nil {
		t.Fatal(err)
	}
	if afterSecond.Likes != afterFirst.Likes {
		t.Errorf("likes_n went from %d to %d on a repeated like; want it unchanged", afterFirst.Likes, afterSecond.Likes)
	}
	if got := notifications(); got != noticesAfterFirst+1 {
		t.Errorf("the author has %d notifications after the repeat, %d before; want one more", got, noticesAfterFirst)
	}
}

// TestMongoRequestsPerOperationAreCounted: the count the operations report is
// the number of commands the server received, and each is the "Req" column of
// spec 6.3.
func TestMongoRequestsPerOperationAreCounted(t *testing.T) {
	// W1 is counted as -fanout bulk sends it; single mode is in fanout_test.go.
	world, log := newMongoWorldWith(t, true, func(cfg *config) { cfg.Fanout = fanoutBulk })
	ctx := context.Background()
	ops := world.ops(t)
	author := world.busiestAuthor()
	followers := len(world.followersOf(author))
	actor, followee := notFollowing(world.graph)
	followees := []int{}
	for _, followed := range world.graph.following(3) {
		followees = append(followees, int(followed))
	}
	post := postRecord{Author: userID(author), Seq: 100, Text: "t", Created: time.Now().UTC()}
	cases := []struct {
		name string
		want int64
		run  func() error
	}{
		{"R1", 1, func() error { _, err := ops.homeTimeline(ctx, 3, 20); return err }},
		{"R2", 1, func() error { _, err := ops.profile(ctx, 3); return err }},
		{"R3", 1, func() error { _, err := ops.profileTimeline(ctx, 3, 20); return err }},
		{"R4", 1, func() error { _, err := ops.postPage(ctx, 3, 1); return err }},
		{"R5", 1, func() error { _, err := ops.followCheck(ctx, 3, 4); return err }},
		{"R6", 1, func() error { _, err := ops.followingList(ctx, 3, 50); return err }},
		{"R7", 1, func() error { _, err := ops.avatar(ctx, 0); return err }},
		{"N1", 2, func() error { _, err := ops.readNotifications(ctx, 3, 10); return err }},
		{"W1", 2 + min(int64(followers), 1), func() error { _, err := ops.publishPost(ctx, post, noLedger{}); return err }},
		{"W2", 3, func() error { return ops.likePost(ctx, 4, 3, 1) }},
		{"W3", 4, func() error { return ops.follow(ctx, actor, followee) }},
		{"W4", 3, func() error { return ops.unfollow(ctx, actor, followee) }},
		{"W5", 3, func() error { return ops.commentOnPost(ctx, 4, 3, 1, "c") }},
		{"W6", 1, func() error { return ops.uploadAvatar(ctx, 0, []byte("a")) }},
		{"M1", 1, func() error { _, err := ops.mergedTimeline(ctx, followees); return err }},
		{"Q1", 4, func() error { _, err := ops.queueRound(ctx, 1); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before, mark := ops.requests(), log.mark()
			if err := tc.run(); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := ops.requests() - before; got != tc.want {
				t.Errorf("%s reports %d requests, want %d", tc.name, got, tc.want)
			}
			if sent := log.since(mark); int64(len(sent)) != tc.want {
				t.Errorf("%s sent %d commands %v; the count it reports must be the commands on the wire", tc.name, len(sent), sent)
			}
		})
	}
}

// notFollowing is a pair (actor, followee) with no edge in the seeded graph.
func notFollowing(graph *socialGraph) (actor, followee int) {
	actor = 4
	followee = 5
	for slices.Contains(graph.following(actor), int32(followee)) || followee == actor {
		followee++
	}
	return actor, followee
}

// TestMongoSerialSubOperations: no operation has two commands in flight.
func TestMongoSerialSubOperations(t *testing.T) {
	world, log := newMongoWorld(t, true)
	ctx := context.Background()
	ops := world.ops(t)
	author := world.busiestAuthor()
	actor, followee := notFollowing(world.graph)
	post := postRecord{Author: userID(author), Seq: 100, Text: "t", Created: time.Now().UTC()}
	log.mu.Lock()
	log.peakInFlight = 0 // seeding uses several connections on purpose
	log.mu.Unlock()
	steps := map[string]func() error{
		"N1": func() error { _, err := ops.readNotifications(ctx, 3, 10); return err },
		"W1": func() error { _, err := ops.publishPost(ctx, post, noLedger{}); return err },
		"W2": func() error { return ops.likePost(ctx, 4, 3, 1) },
		"W3": func() error { return ops.follow(ctx, actor, followee) },
		"W4": func() error { return ops.unfollow(ctx, actor, followee) },
		"W5": func() error { return ops.commentOnPost(ctx, 4, 3, 1, "c") },
		"Q1": func() error { _, err := ops.queueRound(ctx, 1); return err },
	}
	for name, step := range steps {
		if err := step(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.peakInFlight != 1 {
		t.Errorf("up to %d commands were in flight at once; the serial default allows 1", log.peakInFlight)
	}
}

// TestMongoN1IsTwoCommandsNotAtomic: N1 is a find and then a delete, two
// commands with nothing tying them together (spec 6.3), unlike hoardDB's pop.
func TestMongoN1IsTwoCommandsNotAtomic(t *testing.T) {
	world, log := newMongoWorld(t, true)
	ops := world.ops(t)
	mark := log.mark()
	if taken, err := ops.readNotifications(context.Background(), 3, 10); err != nil || taken == 0 {
		t.Fatalf("N1 took %d, %v; want the seeded notifications", taken, err)
	}
	if got := log.since(mark); !slices.Equal(got, []string{"find", "delete"}) {
		t.Errorf("N1 sent %v; want [find delete], with no transaction around them", got)
	}
}

// TestMongoDefaultsToJournaledWrites: with no -mongo-wc the writes carry
// {w:1, j:true} on the wire (L-A, spec 3.3), not a bare w:1.
func TestMongoDefaultsToJournaledWrites(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "mongo"})
	if err != nil {
		t.Fatal(err)
	}
	concern, err := mongoWriteConcern(cfg.MongoWC)
	if err != nil || concern.W != 1 || concern.Journal == nil || !*concern.Journal {
		t.Fatalf("the default -mongo-wc %q is %+v, %v; want w:1 with j:true", cfg.MongoWC, concern, err)
	}

	t.Run("on the wire", func(t *testing.T) {
		world, log := newMongoWorldWith(t, false, func(cfg *config) { cfg.MongoURI = withWriteConcern(t, cfg.MongoURI, "", "") })
		if source := world.social.(*mongoEngine).concernSource; source != concernFromFlag {
			t.Fatalf("write concern source %q; a URI with none must leave it to the flag", source)
		}
		lastWriteConcern(t, world, log, 1, "", true)
	})
}

// lastWriteConcern uploads an avatar and checks the write concern its command
// carried on the wire. wantW is a number (wantName empty) or a name.
func lastWriteConcern(t *testing.T, world *socialWorld, log *commandLog, wantW int32, wantName string, wantJournal bool) {
	t.Helper()
	if err := world.ops(t).uploadAvatar(context.Background(), 1, []byte("a")); err != nil {
		t.Fatalf("W6: %v", err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.writeConcerns) == 0 {
		t.Fatal("no command carried a writeConcern")
	}
	last := log.writeConcerns[len(log.writeConcerns)-1]
	journal, ok := last.Lookup("j").BooleanOK()
	if !ok || journal != wantJournal {
		t.Errorf("the update carried write concern %s; want j:%v", last, wantJournal)
	}
	if wantName != "" {
		if name, ok := last.Lookup("w").StringValueOK(); !ok || name != wantName {
			t.Errorf("the update carried write concern %s; want w:%q", last, wantName)
		}
	} else if number, ok := last.Lookup("w").AsInt64OK(); !ok || number != int64(wantW) {
		t.Errorf("the update carried write concern %s; want w:%d", last, wantW)
	}
}

// TestMongoURIWriteConcernWins: the write concern in the connection string
// (engines.sh writes the level's) beats -mongo-wc, and the engine says which
// source it used.
func TestMongoURIWriteConcernWins(t *testing.T) {
	world, log := newMongoWorldWith(t, false, func(cfg *config) {
		cfg.MongoURI = withWriteConcern(t, cfg.MongoURI, "majority", "true")
		cfg.MongoWC = "w1" // the flag would be weaker; the URI must win
	})
	engine := world.social.(*mongoEngine)
	if engine.concernSource != concernFromURI || engine.facts["write_concern_source"] != concernFromURI || engine.facts["write_concern"] != "w=majority,j=true" {
		t.Errorf("source %q, facts %v; want the URI's w=majority,j=true", engine.concernSource, engine.facts)
	}
	lastWriteConcern(t, world, log, 0, "majority", true)
}

// TestMongoClientOptionsUseExactlyConcConnections: the pool never queues and
// never exceeds the hoardDB socket count.
func TestMongoClientOptionsUseExactlyConcConnections(t *testing.T) {
	concern, err := mongoWriteConcern("w1j")
	if err != nil {
		t.Fatal(err)
	}
	clientOptions := mongoClientOptions(&config{MongoURI: "mongodb://localhost", Conc: 3}, concern, readpref.Primary())
	if *clientOptions.MaxPoolSize != 3 || *clientOptions.MinPoolSize != 3 {
		t.Errorf("pool %d..%d; want exactly 3", *clientOptions.MinPoolSize, *clientOptions.MaxPoolSize)
	}
}

// TestResolveMongoConcern: the connection string's write concern wins over
// -mongo-wc, and -mongo-wc applies when the string carries none.
func TestResolveMongoConcern(t *testing.T) {
	cases := []struct {
		name, uri, flag, wantSource, wantSpelled string
	}{
		{"majority in the URI beats the default flag", "mongodb://h/?w=majority&journal=true", "w1j", concernFromURI, "w=majority,j=true"},
		{"w1 without journal in the URI beats a journaled flag", "mongodb://h/?w=1", "w1j", concernFromURI, "w=1,j=false"},
		{"no concern in the URI leaves it to the flag", "mongodb://h/?tls=true", "w1j", concernFromFlag, "w=1,j=true"},
		{"the relaxed flag applies when the URI is silent", "mongodb://h/", "w1", concernFromFlag, "w=1,j=false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			concern, source, err := resolveMongoConcern(&config{MongoURI: tc.uri, MongoWC: tc.flag})
			if err != nil || source != tc.wantSource || describeConcern(concern) != tc.wantSpelled {
				t.Errorf("resolved %v from %q, %v; want %s from %s", concern, source, err, tc.wantSpelled, tc.wantSource)
			}
		})
	}
}
