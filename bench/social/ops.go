package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The social operations of spec 6.3, as one worker sees them. Every
// operation's sub-operations are sent one after another on the worker's own
// connection (Q-A: serial by default); the pipelined variant is a later step.

const (
	// userBucketName is the one bucket of the primary placement form H-1 and
	// the stem of the shard buckets User_NN of H-64.
	userBucketName = "User"
	// postKeyBase makes a post's key descending in its sequence number, so the
	// first page of a btree store's list, which runs in ascending key order, is
	// the newest posts (spec 6.2).
	postKeyBase = 2147483647
	// outboxBuckets is the number of bucket-level FIFOs the queue operation Q1
	// spreads over (spec 6.3).
	outboxBuckets = 1000
	// feedPageSize, followingPageSize and the others are the page sizes of
	// spec 6.3, all inside the limits of spec 6.2a.
	feedPageSize      = 20
	profilePageSize   = 20
	followingPageSize = 50
	inboxPopSize      = 10
	mergedPostsTotal  = 20
	queueClaimSize    = 10
	queueLeaseMs      = 30_000
	// followerPageSize is the page of Followers.list; the follower cap of the
	// seed is 500, so one page serves a seeded author.
	followerPageSize = 1000
)

// errNoAvatar is returned by avatar when the user has none.
var errNoAvatar = errors.New("user has no avatar")

// wireCaller is the part of a hoardDB connection the operations use: one
// request out, one reply in. *wireConn is the production implementation; the
// tests substitute an in-memory server.
type wireCaller interface {
	call(op transport.Opcode, request, response interface{}) error
	// requests is how many requests this connection has sent so far; an
	// operation's request count is the difference around it.
	requests() int64
}

// postRecord is a post as stored in Posts and copied into Feed. Feed entries
// are copies of the post document (fan-out on write, spec 6.3).
type postRecord struct {
	// ID is the id the server minted for the stored element; a request never
	// carries it, and the canonical form drops it.
	ID      string    `bson:"_id_,omitempty"`
	Author  string    `bson:"author"`
	Seq     int64     `bson:"seq"`
	Text    string    `bson:"text"`
	Created time.Time `bson:"created"`
	Tags    []string  `bson:"tags"`
	ReplyTo string    `bson:"reply_to"`
}

// document is the post as a request body.
func (p postRecord) document() map[string]interface{} {
	return map[string]interface{}{
		"author": p.Author, "seq": p.Seq, "text": p.Text,
		"created": p.Created, "tags": p.Tags, "reply_to": p.ReplyTo,
	}
}

// userRecord is the User document: the scalar fields of spec 6.2.
type userRecord struct {
	UserID         string    `bson:"userid"`
	Name           string    `bson:"name"`
	Bio            string    `bson:"bio"`
	Joined         time.Time `bson:"joined"`
	FollowerCount  int64     `bson:"followers_n"`
	FollowingCount int64     `bson:"following_n"`
}

// userDocument is the profile as a request body.
func userDocument(profile userProfile) map[string]interface{} {
	return map[string]interface{}{
		"userid": profile.ID, "name": profile.Name, "bio": profile.Bio, "joined": profile.Joined,
		"followers_n": profile.Followers, "following_n": profile.Following,
	}
}

// postPage is operation R4's answer: the post with its like and comment counts.
type postPage struct {
	Found    bool
	Post     postRecord
	Likes    int64
	Comments int64
}

// socialOps is one worker's handle on the sixteen operations of spec 6.3. A
// method returns what a verifier needs to check the answer; the caller times
// the call and verifies afterwards, so checking is never charged.
type socialOps interface {
	homeTimeline(ctx context.Context, actor, limit int) ([]postRecord, error)                        // R1
	profile(ctx context.Context, target int) (userRecord, error)                                     // R2
	profileTimeline(ctx context.Context, target, limit int) ([]postRecord, error)                    // R3
	postPage(ctx context.Context, author int, seq int64) (postPage, error)                           // R4
	followCheck(ctx context.Context, actor, target int) (bool, error)                                // R5
	followingList(ctx context.Context, actor, limit int) ([]string, error)                           // R6
	avatar(ctx context.Context, target int) ([]byte, error)                                          // R7
	readNotifications(ctx context.Context, actor, limit int) (int, error)                            // N1
	publishPost(ctx context.Context, post postRecord, ledger deliveryLedger) (fanout int, err error) // W1
	likePost(ctx context.Context, liker, author int, seq int64) error                                // W2
	follow(ctx context.Context, actor, target int) error                                             // W3
	unfollow(ctx context.Context, actor, target int) error                                           // W4
	commentOnPost(ctx context.Context, commenter, author int, seq int64, text string) error          // W5
	uploadAvatar(ctx context.Context, user int, content []byte) error                                // W6
	mergedTimeline(ctx context.Context, followees []int) ([]postRecord, error)                       // M1
	queueRound(ctx context.Context, outbox int) (claimed int, err error)                             // Q1
	// canonicalUser reads everything an engine holds for a user, in canonical
	// form (spec P4).
	canonicalUser(ctx context.Context, user int) (canonicalUser, error)
	// requests is how many requests this handle has sent so far.
	requests() int64
	close()
}

// deliveryLedger is told about each feed copy a post's fan-out sends, so the
// bookkeeping knows what a follower's feed may hold: a copy is issued before
// it is sent and delivered once the server acknowledged it.
type deliveryLedger interface {
	sending(follower int)
	delivered(follower int)
}

// noLedger ignores every delivery.
type noLedger struct{}

func (noLedger) sending(int)   {}
func (noLedger) delivered(int) {}

// socialEngine is an engine that implements the social operations. All five
// engines do, and a -mix leg runs against any of them.
type socialEngine interface {
	engine
	// setupSocial creates the database and every bucket the operations use.
	setupSocial(ctx context.Context, spec datasetSpec) (schemaInfo, error)
	// seedSocial writes the dataset of graph, conc connections at a time.
	seedSocial(ctx context.Context, graph *socialGraph, conc int) error
	newSocialOps() (socialOps, error)
}

var (
	_ socialEngine = (*hoardEngine)(nil)
	_ socialEngine = (*mongoEngine)(nil)
	_ socialEngine = (*mysqlEngine)(nil)
	_ socialEngine = (*postgresEngine)(nil)
	_ socialEngine = (*cockroachEngine)(nil)
	_ socialEngine = (*ringEngine)(nil)
)

// postKey is a post's key in Posts: a zero-padded descending sequence.
func postKey(seq int64) string { return fmt.Sprintf("%010d", postKeyBase-seq) }

// outboxBucket names the bucket-level FIFO Q1 uses.
func outboxBucket(index int) string { return "Outbox_" + strconv.Itoa(index) }

// predecessorKey is the key just below lo, for a fixed-width decimal key. An
// inclusive lower bound on a btree store's list is a list that starts after
// this key, because `after` is exclusive and accepts a key that does not exist
// (spec 6.2a). The lowest key has no predecessor and the empty string, which
// starts the list at the beginning, is returned for it.
func predecessorKey(lo string) (string, error) {
	number, err := strconv.ParseInt(lo, 10, 64)
	if err != nil {
		return "", fmt.Errorf("lower bound %q is not a fixed-width decimal key, so its predecessor cannot be formed: %w", lo, err)
	}
	if number == 0 {
		return "", nil
	}
	return fmt.Sprintf("%0*d", len(lo), number-1), nil
}

// listRange reads the elements of a btree store field whose keys lie in
// [lo, hi]. The field verbs have no range read and no upper bound: the read is
// a list after the predecessor of lo, page by page until a key passes hi, with
// the rest trimmed. It returns the entries and how many requests it took.
func listRange(caller wireCaller, base server.FieldRequest, lo, hi string, pageSize int) ([]server.FieldListEntry, error) {
	after, err := predecessorKey(lo)
	if err != nil {
		return nil, err
	}
	var inRange []server.FieldListEntry
	for {
		request := server.FieldListRequest{FieldRequest: base, Limit: pageSize}
		if after != "" {
			valueType, value, err := bson.MarshalValue(after)
			if err != nil {
				return nil, fmt.Errorf("encoding the list cursor %q: %w", after, err)
			}
			request.After = bson.RawValue{Type: valueType, Value: value}
		}
		var page server.FieldListResponse
		if err := caller.call(transport.OpFieldList, request, &page); err != nil {
			return nil, fmt.Errorf("listing %s after %q: %w", base.Field, after, err)
		}
		for _, entry := range page.Entries {
			if entry.ElementKey > hi {
				return inRange, nil
			}
			inRange = append(inRange, entry)
			after = entry.ElementKey
		}
		if !page.HasMore || len(page.Entries) == 0 {
			return inRange, nil
		}
	}
}

// heapPushRequest is a push onto a heap store field with the element's urgency
// as its priority. The heap orders by the request's Priority member and by
// nothing else; an urgency that is only a document field is ignored for
// ordering, silently (spec 6.2a, verified on the frozen build).
func heapPushRequest(target server.FieldRequest, urgency int64, document map[string]interface{}) server.FieldRequest {
	target.Priority = urgency
	target.Data = document
	return target
}

// pushByUrgency pushes document onto a heap store field at its urgency.
func pushByUrgency(caller wireCaller, target server.FieldRequest, urgency int64, document map[string]interface{}) error {
	var pushed server.FieldResponse
	if err := caller.call(transport.OpFieldPush, heapPushRequest(target, urgency, document), &pushed); err != nil {
		return fmt.Errorf("pushing onto %s at urgency %d: %w", target.Field, urgency, err)
	}
	if !pushed.Success {
		return fmt.Errorf("pushing onto %s at urgency %d: %s", target.Field, urgency, pushed.Message)
	}
	return nil
}

// peekMostUrgent reads the element a pop of the heap store field would take.
func peekMostUrgent(caller wireCaller, target server.FieldRequest) (bson.Raw, error) {
	var peeked server.FieldResponse
	if err := caller.call(transport.OpFieldPeek, server.FieldPeekRequest{FieldRequest: target, Limit: 1}, &peeked); err != nil {
		return nil, fmt.Errorf("peeking %s: %w", target.Field, err)
	}
	if len(peeked.Items) == 0 {
		return nil, fmt.Errorf("peeking %s: the heap is empty", target.Field)
	}
	return peeked.Items[0].Element, nil
}
