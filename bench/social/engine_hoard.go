package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

const (
	usersBucket    = "users"
	timelinePrefix = "tl_"
)

func userID(rank int) string          { return fmt.Sprintf("%d", rank) }
func timelineBucket(id string) string { return timelinePrefix + id }

// hoardStoreDeclarations is the store-field part of the spec 6.2 User bucket,
// in the form the create request carries it. Over the wire there is no DDL
// text, so the `--` comment hazard of the command line cannot arise.
func hoardStoreDeclarations() []interface{} {
	return []interface{}{
		map[string]interface{}{"name": "Posts", "engine": "btreeStore", "fields": []interface{}{
			map[string]interface{}{"name": "Likes", "engine": "hashStore"},
			map[string]interface{}{"name": "Comments", "engine": "fifoStore"},
		}},
		map[string]interface{}{"name": "Feed", "engine": "lifoStore"},
		map[string]interface{}{"name": "Following", "engine": "hashStore"},
		map[string]interface{}{"name": "Followers", "engine": "hashStore"},
		map[string]interface{}{"name": "Inbox", "engine": "fifoStore"},
		map[string]interface{}{"name": "Media", "engine": "blobStore"},
	}
}

// hoardEngine drives hoardDB over its TCP+TLS wire protocol: a hash bucket of
// profiles and one LIFO bucket per user.
type hoardEngine struct {
	cfg *config
	// dial opens a connection to addr for the social operations; nil dials it
	// over TLS. Tests substitute an in-memory server.
	dial func(addr string) (wireCaller, func(), error)
	// progressOut receives the seed's progress lines; nil writes to stderr.
	progressOut io.Writer

	routerOnce sync.Once
	router     *roundRobin

	placeOnce sync.Once
	place     placement
}

// placement is where the run puts its users, from -placement.
func (h *hoardEngine) placement() placement {
	h.placeOnce.Do(func() { h.place = placementOfConfig(h.cfg) })
	return h.place
}

// opsOn is the social operations handle over caller, routed by the run's placement.
func (h *hoardEngine) opsOn(caller wireCaller) hoardOps {
	return hoardOps{caller: caller, database: h.cfg.Database, place: h.placement(), batch: h.cfg.HoardBatch}
}

func newHoardEngine(cfg *config) *hoardEngine { return &hoardEngine{cfg: cfg} }

// nodes are the addresses the run connects to: -addr's list, or the one
// address a test or the older timeline workload set.
func (h *hoardEngine) nodes() []string {
	if len(h.cfg.Addrs) > 0 {
		return h.cfg.Addrs
	}
	return []string{h.cfg.Addr}
}

// nextNode is the node the next connection goes to: the first under RP-A, each
// in turn under RP-B.
func (h *hoardEngine) nextNode() string {
	h.routerOnce.Do(func() { h.router = newRoundRobin(len(h.nodes()), h.cfg.Routing) })
	return h.nodes()[h.router.pick()]
}

func (h *hoardEngine) target() string { return strings.Join(h.nodes(), ",") }
func (h *hoardEngine) close()         {}

func (h *hoardEngine) desynced(err error) bool { return isDesynced(err) }

func (h *hoardEngine) newSession() (engineSession, error) {
	conn, err := dialWire(h.nextNode(), h.cfg.Password, h.cfg.OpTimeout)
	if err != nil {
		return nil, err
	}
	return &hoardSession{conn: conn, database: h.cfg.Database, concern: h.cfg.WriteConcern}, nil
}

// setup creates the database, the users bucket and one LIFO timeline per user.
func (h *hoardEngine) setup(ctx context.Context) (schemaInfo, error) {
	cfg := h.cfg
	admin, err := dialWire(h.nodes()[0], cfg.Password, cfg.OpTimeout)
	if err != nil {
		return schemaInfo{}, err
	}
	defer admin.close()
	var created server.CreateDatabaseResponse
	if err := admin.call(transport.OpCreateDB, server.CreateDatabaseRequest{Name: cfg.Database}, &created); err != nil {
		return schemaInfo{}, fmt.Errorf("creating database %q: %w", cfg.Database, err)
	}
	if !created.Success {
		return schemaInfo{}, fmt.Errorf("creating database %q: %s; if it is left over from an earlier run, pass -attach to reuse it or delete it first", cfg.Database, created.Message)
	}
	if err := createBucket(admin, cfg.Database, usersBucket, server.BucketTypeHash); err != nil {
		return schemaInfo{}, err
	}
	open := func() (*wireConn, func(), error) {
		conn, err := dialWire(h.nextNode(), cfg.Password, cfg.OpTimeout)
		if err != nil {
			return nil, nil, err
		}
		return conn, conn.close, nil
	}
	err = forEachRank(cfg, "creating timeline for", open, func(conn *wireConn, rank int) error {
		return createBucket(conn, cfg.Database, timelineBucket(userID(rank)), server.BucketTypeLIFO)
	})
	if err != nil {
		return schemaInfo{}, err
	}
	return schemaInfo{
		Layout:            "per-user-buckets",
		Detail:            []string{"hash bucket " + usersBucket, "one LIFO bucket " + timelinePrefix + "<userid> per user"},
		Buckets:           cfg.Users + 1,
		PerBucketCreation: true,
	}, nil
}

// attach reuses buckets an earlier run created.
func (h *hoardEngine) attach(context.Context) (schemaInfo, error) {
	return schemaInfo{Layout: "per-user-buckets", Detail: []string{"existing buckets"}, Buckets: h.cfg.Users + 1}, nil
}

func createBucket(conn *wireConn, database, name string, kind server.BucketType) error {
	var created server.CreateBucketResponse
	if err := conn.call(transport.OpCreateBucket, server.CreateBucketRequest{Database: database, Name: name, Type: kind}, &created); err != nil {
		return fmt.Errorf("creating bucket %q: %w", name, err)
	}
	if !created.Success {
		return fmt.Errorf("creating bucket %q: %s", name, created.Message)
	}
	return nil
}

// hoardSession is one authenticated socket.
type hoardSession struct {
	conn     *wireConn
	database string
	concern  string // the run's write concern, empty for the node's default
}

func (s *hoardSession) close() { s.conn.close() }

func (s *hoardSession) putProfile(_ context.Context, id string, profile map[string]interface{}) error {
	var stored server.InsertResponse
	request := server.InsertRequest{Database: s.database, Bucket: usersBucket, Key: id, Data: profile, WriteConcern: s.concern}
	if err := s.conn.call(transport.OpUpsert, request, &stored); err != nil {
		return fmt.Errorf("writing profile: %w", err)
	}
	if !stored.Success {
		return fmt.Errorf("writing profile: %s", stored.Message)
	}
	return nil
}

func (s *hoardSession) writePost(_ context.Context, id string, post map[string]interface{}) error {
	var pushed server.PushResponse
	request := server.PushRequest{Database: s.database, Bucket: timelineBucket(id), Data: post, WriteConcern: s.concern}
	if err := s.conn.call(transport.OpLIFOPush, request, &pushed); err != nil {
		return fmt.Errorf("pushing post: %w", err)
	}
	if !pushed.Success {
		return fmt.Errorf("pushing post: %s", pushed.Message)
	}
	return nil
}

// attachPageSize is how many posts attachState reads per scan. The projection
// keeps each one to a sequence number, so a page is small at any size.
const attachPageSize = 500

// attachState reads a timeline's size and the next free sequence number. It
// reads the whole timeline, not its top post: concurrent pushes to one
// timeline may land in either order, so after a burst the top post need not
// carry the highest sequence number, and resuming from it would reuse one.
func (s *hoardSession) attachState(_ context.Context, id string) (int64, int64, error) {
	var total, nextSeq int64
	for skip := 0; ; skip += attachPageSize {
		var page server.ScanResponse
		request := server.ScanRequest{
			Database: s.database, Bucket: timelineBucket(id),
			Skip: skip, Limit: attachPageSize, Projection: map[string]int{"seq": 1},
		}
		if err := s.conn.call(transport.OpScan, request, &page); err != nil {
			return 0, 0, err
		}
		if !page.Found {
			return 0, 0, fmt.Errorf("timeline missing (%s); -users is larger than the dataset or the database is not seeded", page.Message)
		}
		total = page.Total
		posts := make([]postView, len(page.Documents))
		for i, document := range page.Documents {
			if err := bson.Unmarshal(document, &posts[i]); err != nil {
				return 0, 0, fmt.Errorf("decoding post %d of the timeline: %w", skip+i, err)
			}
		}
		nextSeq = max(nextSeq, nextSeqAfter(posts))
		if !page.HasMore {
			return total, nextSeq, nil
		}
	}
}

// readTimeline scans the newest posts and gets the profile. Decoding for
// verification happens after the clock stops.
func (s *hoardSession) readTimeline(_ context.Context, request readRequest) (timelinePage, error) {
	var page server.ScanResponse
	scan := server.ScanRequest{Database: s.database, Bucket: timelineBucket(request.Author), Limit: request.Limit}
	if err := s.conn.call(transport.OpScan, scan, &page); err != nil {
		return timelinePage{}, err
	}
	var profile server.FindResponse
	if err := s.conn.call(transport.OpGet, server.FindRequest{Database: s.database, Bucket: usersBucket, Key: request.Author}, &profile); err != nil {
		return timelinePage{}, err
	}
	if !page.Found {
		return timelinePage{}, fmt.Errorf("timeline %s missing: %s", request.Author, page.Message)
	}
	if !profile.Found {
		return timelinePage{}, fmt.Errorf("profile %s missing: %s", request.Author, profile.Message)
	}
	if !request.Sampled {
		return timelinePage{}, nil
	}
	request.Clock.stop = time.Now()
	posts := make([]postView, len(page.Documents))
	for i, document := range page.Documents {
		if err := bson.Unmarshal(document, &posts[i]); err != nil {
			return timelinePage{}, fmt.Errorf("timeline %s: decoding post %d: %w", request.Author, i, err)
		}
	}
	var owner struct {
		UserID string `bson:"userid"`
	}
	if err := bson.Unmarshal(profile.Data, &owner); err != nil {
		return timelinePage{}, fmt.Errorf("profile %s: decoding: %w", request.Author, err)
	}
	return timelinePage{Posts: posts, TotalLow: page.Total, TotalHigh: page.Total, ProfileOwner: owner.UserID}, nil
}
