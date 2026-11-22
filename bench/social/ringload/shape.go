package ringload

import (
	"strings"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The shape this seeder declares (rule 11 of AGENTS.md: data lives with what it describes).
// There are two buckets whatever the dataset size; a user and a post are documents, and the
// collections that belong to them are store fields of those documents.
//
// Why a Post bucket exists: a post has likes and a comment thread of its own, and a ring
// store field lives on a document, not inside another store's element (the legacy nested
// L3 stores have no ring form). Making the post a document is the way it carries its own
// Likes and Comments. The author's Posts store holds only a reference to each post, so the
// text is stored once.
const (
	// UserBucket holds one document per user; the document id is the user rank as text.
	UserBucket = "User"
	// PostBucket holds one document per post; the document id is "<author>:<seq>".
	PostBucket = "Post"
	// OutboxBucket holds the OutboxDocs documents the harness's queue operation Q1 pushes jobs
	// onto; it exists only when the load asks for it (Options.Outbox), because it is a
	// benchmark artifact and not part of the social data.
	OutboxBucket = "Outbox"
	// OutboxDocs is how many outbox documents Q1 spreads over (the legacy harness' outboxBuckets).
	OutboxDocs = 1000
)

// Store field names, written once so the declaration and the writes cannot drift.
const (
	UserPosts     = "Posts"
	UserFeed      = "Feed"
	UserFollowing = "Following"
	UserFollowers = "Followers"
	UserInbox     = "Inbox"
	PostLikes     = "Likes"
	PostComments  = "Comments"
	OutboxJobs    = "Jobs"
)

// declaredBuckets returns the buckets to create, by name.
//
//	create bucket User { Name string, Bio string, Joined datetime, FollowerCount int, FollowingCount int,
//	    Avatar binary, Photo binary,
//	    Posts btreeStore, Feed lifoStore, Following hashStore, Followers hashStore, Inbox fifoStore };
//	create bucket Post { Author string, Seq int, Text string, Created datetime, Tags array, ReplyTo string,
//	    Likes hashStore, Comments fifoStore };
//
// The legacy User kept avatars and photos in a Media blobStore. The ring wire has no verb that
// writes a blobStore yet (SPEC-GAP RS-2), so the two blobs are binary scalar fields of the user
// document instead: one read returns them with the profile, and replacing one rewrites the
// document's scalars. Outbox is declared only when asked for:
//
//	create bucket Outbox { Name string, Jobs fifoStore };
func declaredBuckets() map[string][]ringclient.FieldDecl {
	return map[string][]ringclient.FieldDecl{
		UserBucket: {
			{Name: "Name", Type: "string"}, {Name: "Bio", Type: "string"}, {Name: "Joined", Type: "datetime"},
			{Name: "FollowerCount", Type: "int"}, {Name: "FollowingCount", Type: "int"},
			{Name: "Avatar", Type: "binary"}, {Name: "Photo", Type: "binary"},
			{Name: UserPosts, Type: "btreeStore"}, {Name: UserFeed, Type: "lifoStore"},
			{Name: UserFollowing, Type: "hashStore"}, {Name: UserFollowers, Type: "hashStore"}, {Name: UserInbox, Type: "fifoStore"},
		},
		PostBucket: {
			{Name: "Author", Type: "string"}, {Name: "Seq", Type: "int"}, {Name: "Text", Type: "string"},
			{Name: "Created", Type: "datetime"}, {Name: "Tags", Type: "array"}, {Name: "ReplyTo", Type: "string"},
			{Name: PostLikes, Type: "hashStore"}, {Name: PostComments, Type: "fifoStore"},
		},
		OutboxBucket: {{Name: "Name", Type: "string"}, {Name: OutboxJobs, Type: "fifoStore"}},
	}
}

// ScalarFields returns the names of the scalar (non-store) fields a bucket declares, in
// declaration order: the names a projected read may ask for.
func ScalarFields(bucket string) []string {
	var names []string
	for _, decl := range declaredBuckets()[bucket] {
		if !strings.HasSuffix(decl.Type, "Store") {
			names = append(names, decl.Name)
		}
	}
	return names
}

// bucketOrder is the order the buckets are created in, so a run is repeatable.
var bucketOrder = []string{UserBucket, PostBucket, OutboxBucket}
