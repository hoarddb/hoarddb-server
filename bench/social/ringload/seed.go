package ringload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// postKeyBase makes the key of a post in the author's Posts btreeStore fall as the sequence
// number rises, so the newest post sorts first (the legacy harness does the same).
const postKeyBase = 2147483647

// PostKey is the key of post seq in the author's Posts store; it falls as seq rises.
func PostKey(seq int) string { return fmt.Sprintf("%010d", postKeyBase-seq) }

// PostID is the id of a post document: "<author>:<seq>".
func PostID(author, seq int) string { return fmt.Sprintf("%d:%d", author, seq) }

const (
	queueDepth        = 256
	seededCommentText = "seeded comment"
)

// Stats is what a finished seed reports.
type Stats struct {
	Database string  `json:"database"`
	Dataset  string  `json:"dataset"`
	Users    int     `json:"users"`
	Posts    int     `json:"posts"`
	Buckets  int     `json:"buckets"`
	Writes   int64   `json:"writes"`
	Seconds  float64 `json:"seed_s"`
}

// seeder writes the dataset through a set of connections.
type seeder struct {
	graph    *socialGraph
	clients  []*ringclient.Client
	database string
	workers  int
	progress *progress
	next     atomic.Uint64
	// outbox says to create and fill the Outbox bucket.
	outbox bool
	// schemaCreated says createSchema already ran.
	schemaCreated bool
}

// Options is what Seed needs.
type Options struct {
	// Spec is the dataset to generate; LookupDataset names the real ones.
	Spec Spec
	// Database is the database to create and fill; it must not exist.
	Database string
	// Clients are the connections the workers share; at least one.
	Clients []*ringclient.Client
	// Workers is how many writes are in flight; 0 means 32.
	Workers int
	// Progress receives a progress line about every two seconds; nil discards them.
	Progress io.Writer
	// SchemaCreated says the caller already ran CreateSchema (to time the setup apart from the
	// seed), so Seed writes data only.
	SchemaCreated bool
	// Outbox also creates the Outbox bucket and its OutboxDocs documents, which the harness'
	// queue operation Q1 needs.
	Outbox bool
}

// Seed creates the database and the buckets and writes the whole dataset, using the
// generator of this package (the one copy the harness' ring engine and the ringseed command
// share). It refuses a database that exists.
func Seed(ctx context.Context, options Options) (Stats, error) {
	if len(options.Clients) == 0 {
		return Stats{}, errors.New("ringload.Seed was given no connections. Dial at least one ringclient.Client and pass it in Options.Clients")
	}
	workers := options.Workers
	if workers == 0 {
		workers = 32
	}
	out := options.Progress
	if out == nil {
		out = io.Discard
	}
	seeding := &seeder{graph: newSocialGraph(options.Spec, DatasetSeed), clients: options.Clients, database: options.Database,
		workers: workers, progress: newProgress(out), outbox: options.Outbox, schemaCreated: options.SchemaCreated}
	return seedAll(ctx, seeding)
}

// CreateSchema creates the database and its buckets (the Outbox bucket only when outbox is
// set) without writing any data. A database that exists is refused with the way out.
func CreateSchema(ctx context.Context, client *ringclient.Client, database string, outbox bool) error {
	return createSchema(ctx, client, database, outbox)
}

// pick returns a client round-robin, so the workers spread over the connections.
func (s *seeder) pick() *ringclient.Client {
	return s.clients[int(s.next.Add(1))%len(s.clients)]
}

func (s *seeder) users(c *ringclient.Client) *ringclient.Bucket {
	return c.Bucket(s.database, UserBucket)
}
func (s *seeder) posts(c *ringclient.Client) *ringclient.Bucket {
	return c.Bucket(s.database, PostBucket)
}

// createSchema creates the database and the declared buckets. A database that exists is
// refused with the way out, because seeding into it would collide with or duplicate what is
// there.
func createSchema(ctx context.Context, client *ringclient.Client, database string, outbox bool) error {
	if _, err := client.CreateDatabase(ctx, database, 0); err != nil {
		var refusal *ringclient.Error
		if errors.As(err, &refusal) && refusal.Code == cluster.CodeConflict {
			return fmt.Errorf("database %q already exists on this server, so it may already hold the dataset and seeding again would collide with it. "+
				"Seed a fresh database with -database <name>, or stop the server and start it on an empty data directory: %w", database, err)
		}
		return fmt.Errorf("creating database %q: %w", database, err)
	}
	declared := declaredBuckets()
	for _, name := range bucketOrder {
		if name == OutboxBucket && !outbox {
			continue
		}
		if _, err := client.CreateBucket(ctx, database, name, declared[name]); err != nil {
			return fmt.Errorf("creating bucket %q in database %q: %w", name, database, err)
		}
	}
	return nil
}

// applied turns a write that was not acknowledged as applied into an error.
func applied(doing string, result ringclient.WriteResult, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", doing, err)
	}
	if !result.Applied() {
		return fmt.Errorf("%s: the server's write outcome was %q, not applied", doing, result.Outcome)
	}
	return nil
}

// seedAll creates the schema and writes every user, post, edge, feed entry, notification,
// like and comment. It returns the first error.
func seedAll(ctx context.Context, s *seeder) (Stats, error) {
	started := time.Now()
	if !s.schemaCreated {
		if err := createSchema(ctx, s.clients[0], s.database, s.outbox); err != nil {
			return Stats{}, err
		}
	}
	offsets, followers := followerLists(s.graph)
	s.progress.phase("users, their posts, follows, feeds and inboxes")
	err := pool(ctx, s.workers, s.graph.users(), func(ctx context.Context, user graphUser) error {
		return s.seedUser(ctx, user, followers[offsets[user.Rank]:offsets[user.Rank+1]])
	}, s.progress)
	if err != nil {
		return Stats{}, fmt.Errorf("seeding users: %w", err)
	}
	if s.outbox {
		s.progress.phase("outbox documents")
		if err := pool(ctx, s.workers, intRange(OutboxDocs), s.seedOutbox, s.progress); err != nil {
			return Stats{}, fmt.Errorf("seeding the outbox documents: %w", err)
		}
	}
	s.progress.phase("likes")
	if err := pool(ctx, s.workers, s.graph.likes(), s.seedLike, s.progress); err != nil {
		return Stats{}, fmt.Errorf("seeding likes: %w", err)
	}
	s.progress.phase("comments")
	if err := pool(ctx, s.workers, s.commentThreads(), s.seedThread, s.progress); err != nil {
		return Stats{}, fmt.Errorf("seeding comments: %w", err)
	}
	if err := s.verifyCounts(ctx); err != nil {
		return Stats{}, err
	}
	return Stats{Database: s.database, Dataset: s.graph.spec.Name, Users: s.graph.spec.Users,
		Posts: s.graph.spec.Users * s.graph.postsPerUser, Buckets: len(bucketOrder) - 1 + btoi(s.outbox),
		Writes: s.progress.total.Load(), Seconds: time.Since(started).Seconds()}, nil
}

// verifyCounts reads the document counts back, so a seed that lost writes does not report success.
func (s *seeder) verifyCounts(ctx context.Context) error {
	client := s.clients[0]
	wantPosts := int64(s.graph.spec.Users * s.graph.postsPerUser)
	wanted := map[string]int64{UserBucket: int64(s.graph.spec.Users), PostBucket: wantPosts}
	if s.outbox {
		wanted[OutboxBucket] = OutboxDocs
	}
	for bucket, want := range wanted {
		got, err := client.Bucket(s.database, bucket).Count(ctx, nil)
		if err != nil {
			return fmt.Errorf("counting bucket %s: %w", bucket, err)
		}
		if got != want {
			return fmt.Errorf("bucket %s holds %d documents after the seed, want %d", bucket, got, want)
		}
	}
	return nil
}

// seedUser writes one user's document, posts and every store of the user. One worker does a
// user from start to finish, so the pushes onto the user's own stores keep their order.
func (s *seeder) seedUser(ctx context.Context, user graphUser, followerRanks []int32) error {
	client := s.pick()
	users, posts := s.users(client), s.posts(client)
	profile := user.Profile
	id := profile.ID
	write := func(doing string, result ringclient.WriteResult, err error) error {
		s.progress.add(1)
		return applied(doing, result, err)
	}
	doc := ringclient.Doc{"Name": profile.Name, "Bio": profile.Bio, "Joined": profile.Joined,
		"FollowerCount": profile.Followers, "FollowingCount": profile.Following}
	if avatar, photo := s.graph.mediaOf(user.Rank); avatar != nil || photo != nil {
		if avatar != nil {
			doc["Avatar"] = avatar
		}
		if photo != nil {
			doc["Photo"] = photo
		}
	}
	result, err := users.Insert(ctx, id, doc)
	if err := write(fmt.Sprintf("storing user %d", user.Rank), result, err); err != nil {
		return err
	}
	for seq := range s.graph.postsPerUser {
		post := s.graph.post(user.Rank, seq)
		ref := PostID(user.Rank, seq)
		result, err := posts.Insert(ctx, ref, ringclient.Doc{"Author": id, "Seq": int64(seq), "Text": post.Text,
			"Created": post.Created, "Tags": post.Tags, "ReplyTo": post.ReplyTo})
		if err := write("storing post "+ref, result, err); err != nil {
			return err
		}
		result, err = users.Put(ctx, id, UserPosts, PostKey(seq), bson.D{{Key: "post", Value: ref}, {Key: "seq", Value: int64(seq)}, {Key: "created", Value: post.Created}})
		if err := write("indexing post "+ref, result, err); err != nil {
			return err
		}
	}
	// An edge carries the id of the user at its other end. A list returns the keys too now, but
	// the parity hash and the seed tests read the edge, so the shape is left as it was.
	for _, followee := range user.Following {
		edge := bson.D{{Key: "user", Value: UserID(int(followee))}, {Key: "since", Value: graphEpoch}}
		result, err := users.Put(ctx, id, UserFollowing, UserID(int(followee)), edge)
		if err := write(fmt.Sprintf("seeding who user %d follows", user.Rank), result, err); err != nil {
			return err
		}
	}
	for _, follower := range followerRanks {
		edge := bson.D{{Key: "user", Value: UserID(int(follower))}, {Key: "since", Value: graphEpoch}}
		result, err := users.Put(ctx, id, UserFollowers, UserID(int(follower)), edge)
		if err := write(fmt.Sprintf("seeding the followers of user %d", user.Rank), result, err); err != nil {
			return err
		}
	}
	for _, ref := range user.Feed { // oldest first, so the newest is on top of the lifoStore
		post := s.graph.post(int(ref.Author), int(ref.Seq))
		entry := bson.D{{Key: "author", Value: UserID(post.Author)}, {Key: "seq", Value: int64(post.Seq)}, {Key: "text", Value: post.Text},
			{Key: "created", Value: post.Created}, {Key: "tags", Value: post.Tags}, {Key: "reply_to", Value: post.ReplyTo}}
		result, err := users.Push(ctx, id, UserFeed, entry)
		if err := write(fmt.Sprintf("seeding the feed of user %d", user.Rank), result, err); err != nil {
			return err
		}
	}
	for _, note := range user.Inbox {
		entry := bson.D{{Key: "type", Value: note.Kind}, {Key: "actor", Value: UserID(note.Actor)}, {Key: "target", Value: id}, {Key: "ts", Value: graphEpoch}}
		result, err := users.Push(ctx, id, UserInbox, entry)
		if err := write(fmt.Sprintf("seeding the inbox of user %d", user.Rank), result, err); err != nil {
			return err
		}
	}
	return nil
}

// intRange streams 0 up to but not including count.
func intRange(count int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for index := range count {
			if !yield(index) {
				return
			}
		}
	}
}

// seedOutbox writes outbox document index, the target of the harness' queue operation Q1.
func (s *seeder) seedOutbox(ctx context.Context, index int) error {
	id := strconv.Itoa(index)
	var result ringclient.WriteResult
	err := retryRefused(ctx, refusedRetries, refusedRetryWait, func() error {
		var err error
		result, err = s.pick().Bucket(s.database, OutboxBucket).Insert(ctx, id, ringclient.Doc{"Name": "outbox " + id})
		return err
	})
	s.progress.add(1)
	return applied("storing outbox document "+id, result, err)
}

// refusedRetries and refusedRetryWait bound the repeat of a refused seeding write: the
// first write to a bucket on a node with a cold declaration cache can be refused
// DECL_UNAVAILABLE while the owner is busy (BUG-0770), and a 35-minute seed must not end on it.
const (
	refusedRetries   = 5
	refusedRetryWait = 200 * time.Millisecond
)

// retryRefused runs write, and runs it again (up to retries more times, after wait) while
// the server refused it with "Nothing was written", which every refusal that is safe to
// repeat says. Any other error, including one for a write that may have been applied, is
// returned at once. This is bench code: the product tells its callers to retry, and the
// seeder is the one caller in the tree that did not (BUG-0770).
func retryRefused(ctx context.Context, retries int, wait time.Duration, write func() error) error {
	err := write()
	for attempt := 0; err != nil && attempt < retries && strings.Contains(err.Error(), "Nothing was written"); attempt++ {
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
		err = write()
	}
	return err
}

func (s *seeder) seedLike(ctx context.Context, like reaction) error {
	ref := PostID(like.PostAuthor, like.PostSeq)
	liker := UserID(like.Actor)
	result, err := s.posts(s.pick()).Put(ctx, ref, PostLikes, liker, bson.D{{Key: "liker", Value: liker}, {Key: "ts", Value: graphEpoch}})
	s.progress.add(1)
	return applied("seeding a like on post "+ref, result, err)
}

// commentThread is every seeded comment of one post, in generation order.
type commentThread struct {
	post   string
	actors []int
}

// commentThreads groups the comments by post, so one worker pushes a post's thread in order
// and the result is the same on every run.
func (s *seeder) commentThreads() iter.Seq[commentThread] {
	return func(yield func(commentThread) bool) {
		index := map[string]int{}
		var threads []commentThread
		for comment := range s.graph.comments() {
			ref := PostID(comment.PostAuthor, comment.PostSeq)
			at, seen := index[ref]
			if !seen {
				at = len(threads)
				index[ref] = at
				threads = append(threads, commentThread{post: ref})
			}
			threads[at].actors = append(threads[at].actors, comment.Actor)
		}
		for _, thread := range threads {
			if !yield(thread) {
				return
			}
		}
	}
}

func (s *seeder) seedThread(ctx context.Context, thread commentThread) error {
	posts := s.posts(s.pick())
	for _, actor := range thread.actors {
		entry := bson.D{{Key: "author", Value: UserID(actor)}, {Key: "text", Value: seededCommentText}, {Key: "ts", Value: graphEpoch}}
		result, err := posts.Push(ctx, thread.post, PostComments, entry)
		s.progress.add(1)
		if err := applied("seeding a comment on post "+thread.post, result, err); err != nil {
			return err
		}
	}
	return nil
}

// followerLists inverts the follow graph: user u is followed by followers[offsets[u]:offsets[u+1]].
func followerLists(graph *socialGraph) (offsets, followers []int32) {
	offsets = make([]int32, graph.spec.Users+1)
	for _, followee := range graph.followees {
		offsets[followee+1]++
	}
	for user := range graph.spec.Users {
		offsets[user+1] += offsets[user]
	}
	followers = make([]int32, len(graph.followees))
	fill := make([]int32, graph.spec.Users)
	for follower := range graph.spec.Users {
		for _, followee := range graph.following(follower) {
			followers[offsets[followee]+fill[followee]] = int32(follower)
			fill[followee]++
		}
	}
	return offsets, followers
}

// pool feeds items to workers and returns the first error. The first failure cancels the
// context the workers run under and the producer stops, so no goroutine outlives the call.
func pool[T any](ctx context.Context, workers int, items iter.Seq[T], do func(context.Context, T) error, progress *progress) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan T, queueDepth)
	var group sync.WaitGroup
	var first atomic.Pointer[error]
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for item := range queue {
				if first.Load() != nil {
					continue // drain, so the producer never blocks
				}
				if err := do(ctx, item); err != nil {
					first.CompareAndSwap(nil, &err)
					cancel()
				}
			}
		}()
	}
produce:
	for item := range items {
		select {
		case queue <- item:
		case <-ctx.Done():
			break produce
		}
	}
	close(queue)
	group.Wait()
	progress.flush()
	if failure := first.Load(); failure != nil {
		return *failure
	}
	return ctx.Err()
}

// progress prints the running write count and rate to out about every two seconds.
type progress struct {
	out   io.Writer
	total atomic.Int64
	start time.Time
	last  atomic.Int64
	name  atomic.Value
}

func newProgress(out io.Writer) *progress {
	return &progress{out: out, start: time.Now()}
}

func (p *progress) phase(name string) {
	p.name.Store(name)
	say(p.out, "seeding %s\n", name)
}

func (p *progress) add(n int64) {
	total := p.total.Add(n)
	now := time.Since(p.start).Milliseconds()
	last := p.last.Load()
	if now-last >= 2000 && p.last.CompareAndSwap(last, now) {
		say(p.out, "  %d writes, %.0f writes/s\n", total, float64(total)/(float64(now)/1000))
	}
}

func (p *progress) flush() {
	total := p.total.Load()
	seconds := time.Since(p.start).Seconds()
	say(p.out, "  %d writes so far, %.0f writes/s\n", total, float64(total)/max(seconds, 0.001))
}

// btoi is 1 for true.
func btoi(flag bool) int {
	if flag {
		return 1
	}
	return 0
}

// say writes a progress line. A failed write to a terminal or a closed pipe is not worth stopping a seed for.
func say(out io.Writer, format string, args ...any) { _, _ = fmt.Fprintf(out, format, args...) }
