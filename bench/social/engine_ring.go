package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringUser is the login the load client uses on a ring node. The legacy engine logs in as
// the same bootstrap administrator, with -password.
const ringUser = "admin"

// ringSeedConnections is how many connections the seeder's workers share. A ring connection
// carries many requests at once, so a few are enough.
const ringSeedConnections = 4

// storeRef addresses one store field of one document.
type storeRef struct {
	bucket, id, field string
}

// ringAPI is the part of a ring connection the social operations use: one method per ring
// verb, each one request, so an operation's request count is the number of these calls.
// *ringConn is the production implementation; the unit tests substitute an in-memory model.
// It is safe for concurrent use, because the ring wire matches replies to requests by id.
type ringAPI interface {
	// get reads a document's scalar fields; a missing document wraps ringclient.ErrNotFound.
	get(ctx context.Context, bucket, id string) (bson.Raw, error)
	// getFields is get returning only _id_ and the named scalar fields (wire class get_fields):
	// it reads no store state, where get reads every store field's state to print its marker.
	getFields(ctx context.Context, bucket, id string, fields []string) (bson.Raw, error)
	// getMany reads the documents ids of one bucket as RING_GET_MANY does: one answer per id in
	// order, each as get when fields is nil and as getFields when it is not. ids beyond one
	// frame's limit go as further frames on the same call. A page of one bucket is one request.
	getMany(ctx context.Context, bucket string, ids []string, fields []string) ([]ringclient.GetAnswer, error)
	insert(ctx context.Context, bucket, id string, doc ringclient.Doc) error
	upsert(ctx context.Context, bucket, id string, doc ringclient.Doc) error
	push(ctx context.Context, store storeRef, element any) error
	// pushWithID is push under a request id taken from nextRequestID, so a retry of a push whose
	// outcome is unknown is answered from the owner's dedupe window and never applies twice.
	pushWithID(ctx context.Context, store storeRef, element any, id [16]byte) error
	// sendBatch sends one push of element onto each store as a single RING_BATCH frame under id,
	// and returns one answer per store in order: nil when applied (or deduped), the refusal
	// otherwise. The error is a failure of the whole frame. Sending again under the same id
	// resends the whole frame.
	sendBatch(ctx context.Context, stores []storeRef, element any, id [16]byte) ([]error, error)
	// nextRequestID returns a request id no other request of this connection uses.
	nextRequestID() [16]byte
	put(ctx context.Context, store storeRef, key string, element any) error
	// take removes the element under key; an absent key wraps ringclient.ErrNotFound.
	take(ctx context.Context, store storeRef, key string) error
	// storeGet reads the element under key of a hash or btree store; an absent key wraps
	// ringclient.ErrNotFound.
	storeGet(ctx context.Context, store storeRef, key string) (bson.Raw, error)
	peek(ctx context.Context, store storeRef, count int) ([]bson.Raw, error)
	list(ctx context.Context, store storeRef, limit int, after []byte) (ringclient.ElementPage, error)
	count(ctx context.Context, store storeRef) (int64, error)
	claim(ctx context.Context, store storeRef, count int) (ringclient.Lease, error)
	ack(ctx context.Context, store storeRef, lease [16]byte) error
	// version is the node's version, as DescribeNode reports it.
	version(ctx context.Context) (string, error)
	// requests is how many requests this connection has sent so far.
	requests() int64
	close()
}

// ringConn is one authenticated ring connection and the database it works in.
type ringConn struct {
	client   *ringclient.Client
	database string
	sent     atomic.Int64
}

func (c *ringConn) requests() int64 { return c.sent.Load() }

func (c *ringConn) close() { _ = c.client.Close() }

func (c *ringConn) version(ctx context.Context) (string, error) {
	c.sent.Add(1)
	node, err := c.client.DescribeNode(ctx)
	return node.Version, err
}

func (c *ringConn) bucket(name string) *ringclient.Bucket {
	c.sent.Add(1)
	return c.client.Bucket(c.database, name)
}

// applied turns a write the server did not acknowledge as applied into an error.
func applied(result ringclient.WriteResult, err error) error {
	if err != nil {
		return err
	}
	if !result.Applied() {
		return fmt.Errorf("the server's write outcome was %q, not applied", result.Outcome)
	}
	return nil
}

func (c *ringConn) get(ctx context.Context, bucket, id string) (bson.Raw, error) {
	return c.bucket(bucket).Get(ctx, id)
}

func (c *ringConn) getFields(ctx context.Context, bucket, id string, fields []string) (bson.Raw, error) {
	return c.bucket(bucket).GetFields(ctx, id, fields)
}

func (c *ringConn) getMany(ctx context.Context, bucket string, ids []string, fields []string) ([]ringclient.GetAnswer, error) {
	return c.bucket(bucket).GetMany(ctx, ids, fields)
}

func (c *ringConn) insert(ctx context.Context, bucket, id string, doc ringclient.Doc) error {
	result, err := c.bucket(bucket).Insert(ctx, id, doc)
	return applied(result, err)
}

func (c *ringConn) upsert(ctx context.Context, bucket, id string, doc ringclient.Doc) error {
	result, err := c.bucket(bucket).Upsert(ctx, id, doc)
	return applied(result, err)
}

func (c *ringConn) push(ctx context.Context, store storeRef, element any) error {
	result, err := c.bucket(store.bucket).Push(ctx, store.id, store.field, element)
	return applied(result, err)
}

func (c *ringConn) pushWithID(ctx context.Context, store storeRef, element any, id [16]byte) error {
	result, err := c.bucket(store.bucket).Push(ctx, store.id, store.field, element, ringclient.WithRequestID(id))
	return applied(result, err)
}

func (c *ringConn) nextRequestID() [16]byte { return c.client.NextRequestID() }

func (c *ringConn) put(ctx context.Context, store storeRef, key string, element any) error {
	result, err := c.bucket(store.bucket).Put(ctx, store.id, store.field, key, element)
	return applied(result, err)
}

func (c *ringConn) take(ctx context.Context, store storeRef, key string) error {
	_, err := c.bucket(store.bucket).Take(ctx, store.id, store.field, key)
	return err
}

func (c *ringConn) storeGet(ctx context.Context, store storeRef, key string) (bson.Raw, error) {
	return c.bucket(store.bucket).StoreGet(ctx, store.id, store.field, key)
}

func (c *ringConn) peek(ctx context.Context, store storeRef, count int) ([]bson.Raw, error) {
	return c.bucket(store.bucket).Peek(ctx, store.id, store.field, count)
}

func (c *ringConn) list(ctx context.Context, store storeRef, limit int, after []byte) (ringclient.ElementPage, error) {
	return c.bucket(store.bucket).StoreList(ctx, store.id, store.field, limit, after)
}

func (c *ringConn) count(ctx context.Context, store storeRef) (int64, error) {
	return c.bucket(store.bucket).StoreCount(ctx, store.id, store.field)
}

func (c *ringConn) claim(ctx context.Context, store storeRef, count int) (ringclient.Lease, error) {
	return c.bucket(store.bucket).Claim(ctx, store.id, store.field, count)
}

func (c *ringConn) ack(ctx context.Context, store storeRef, lease [16]byte) error {
	_, err := c.bucket(store.bucket).Ack(ctx, store.id, store.field, lease)
	return err
}

// ringEngine drives hoardDB through the entry-ring wire (ringclient): the entity-centric
// layout of bench/social/ringload, two buckets (User, Post) and the Outbox of operation Q1,
// whatever the dataset size.
type ringEngine struct {
	cfg *config
	// dial opens one connection to the node; nil dials it over TLS. Tests substitute a model.
	dial func(ctx context.Context) (ringAPI, error)
	// progressOut receives the seed's progress lines; nil writes to stderr.
	progressOut io.Writer
	// batch is the frame sender's totals for the leg, made by the first newSocialOps of a
	// -ring-batch run; close prints it.
	batch     *batchTotals
	batchOnce sync.Once
}

func newRingEngine(cfg *config) *ringEngine { return &ringEngine{cfg: cfg} }

// ringOptions is how the load client reaches the node: the certificate is unchecked, as
// for the legacy engine, unless -ring-fingerprint pins it.
func (r *ringEngine) ringOptions() ringclient.Options {
	options := ringclient.Options{Addr: r.cfg.Addr, User: ringUser, Password: r.cfg.Password, DialTimeout: r.cfg.OpTimeout, RequestTimeout: r.cfg.OpTimeout}
	if r.cfg.RingFingerprint != "" {
		options.Fingerprint = r.cfg.RingFingerprint
	} else {
		options.Insecure = true
	}
	return options
}

// dialClient opens one authenticated ringclient connection through the login gate, so a
// leg that dials -depth connections at once does not trip the node's limit on logins in flight.
func (r *ringEngine) dialClient(ctx context.Context) (*ringclient.Client, error) {
	var client *ringclient.Client
	err := logins.run(func() error {
		var dialErr error
		client, dialErr = ringclient.Dial(ctx, r.ringOptions())
		var refusal *ringclient.Error
		if errors.As(dialErr, &refusal) && refusal.Code == cluster.CodeUnavailable {
			return fmt.Errorf("%w: %w", errLoginRateLimited, dialErr)
		}
		return dialErr
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to the ring node %s: %w", r.cfg.Addr, err)
	}
	return client, nil
}

// open returns one connection for the social operations.
func (r *ringEngine) open(ctx context.Context) (ringAPI, error) {
	if r.dial != nil {
		return r.dial(ctx)
	}
	client, err := r.dialClient(ctx)
	if err != nil {
		return nil, err
	}
	return &ringConn{client: client, database: r.cfg.Database}, nil
}

func (r *ringEngine) target() string { return r.cfg.Addr }
func (r *ringEngine) close() {
	if r.batch == nil || r.batch.snapshot().frames == 0 {
		return
	}
	text := r.batch.summary()
	if r.progressOut == nil {
		fmt.Fprint(os.Stderr, text)
		return
	}
	if _, err := io.WriteString(r.progressOut, text); err != nil {
		fmt.Fprintf(os.Stderr, "writing the ring-batch summary: %v\n", err)
	}
}

func (r *ringEngine) desynced(err error) bool { return isDesynced(err) }

// setup, attach and newSession belong to the older timeline workload, which has one bucket per
// user and a LIFO timeline each: not the layout this engine measures.
func (r *ringEngine) setup(context.Context) (schemaInfo, error) {
	return schemaInfo{}, errRingTimelineWorkload
}

func (r *ringEngine) attach(context.Context) (schemaInfo, error) {
	return schemaInfo{}, errRingTimelineWorkload
}

func (r *ringEngine) newSession() (engineSession, error) { return nil, errRingTimelineWorkload }

var errRingTimelineWorkload = errors.New("-engine ring runs the social operations only, not the older timeline workload. Pass -mix rh, wh, sk or op:<ID> (for example -mode seed -dataset S-10k, then -mode run -mix rh)")

// setupSocial creates the database and the buckets; the dataset size does not change how many.
func (r *ringEngine) setupSocial(ctx context.Context, _ datasetSpec) (schemaInfo, error) {
	client, err := r.dialClient(ctx)
	if err != nil {
		return schemaInfo{}, err
	}
	defer func() { _ = client.Close() }()
	if err := ringload.CreateSchema(ctx, client, r.cfg.Database, true); err != nil {
		return schemaInfo{}, err
	}
	return r.schemaInfo(), nil
}

// schemaInfo describes the buckets setupSocial creates and a seed fills.
func (r *ringEngine) schemaInfo() schemaInfo {
	return schemaInfo{
		Layout: "entity-documents",
		Detail: []string{
			"bucket User { Name, Bio, Joined, FollowerCount, FollowingCount, Avatar binary, Photo binary, Posts btreeStore, Feed lifoStore, Following hashStore, Followers hashStore, Inbox fifoStore }",
			"bucket Post { Author, Seq, Text, Created, Tags, ReplyTo, Likes hashStore, Comments fifoStore }",
			fmt.Sprintf("bucket Outbox { Name, Jobs fifoStore }, %d documents for operation Q1", ringload.OutboxDocs),
		},
		Buckets: 3,
	}
}

// seedSocial writes the dataset through ringload, the one generator the ringseed command
// shares. The harness' own graph says what the dataset is; ringload regenerates the same
// data from the same spec and seed, which TestRingSeedParity and TestRingLoadSeedIsTheHarnessSeed pin.
func (r *ringEngine) seedSocial(ctx context.Context, graph *socialGraph, conc int) error {
	clients := make([]*ringclient.Client, 0, ringSeedConnections)
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	for range ringSeedConnections {
		client, err := r.dialClient(ctx)
		if err != nil {
			return err
		}
		clients = append(clients, client)
	}
	progress := r.progressOut
	if progress == nil {
		progress = os.Stderr
	}
	_, err := ringload.Seed(ctx, ringload.Options{
		Spec: ringload.Spec(graph.spec), Database: r.cfg.Database, Clients: clients, Workers: max(conc, 1),
		Progress: progress, Outbox: true, SchemaCreated: true,
	})
	return err
}

// durability reports the same launch facts as the legacy engine (the replication factor, the
// write concern, the unsafe-ack switch and the WAL preallocation) from the files the harness
// saved, plus the node's version.
func (r *ringEngine) durability() map[string]string {
	facts := launchDurability(r.cfg, r.nodeVersion())
	facts["wire"] = "entry-ring"
	return facts
}

// nodeVersion asks the node for its version. The answer is a fact to record, so a node that
// cannot be asked is recorded as such.
func (r *ringEngine) nodeVersion() string {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.OpTimeout)
	defer cancel()
	api, err := r.open(ctx)
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	defer api.close()
	version, err := api.version(ctx)
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	return version
}
