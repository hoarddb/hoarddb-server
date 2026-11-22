package main

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// column is one column of a SQL table and one field of a MongoDB document. From says where
// the value comes from: "_id" (the document id), "_owner" (the id of the document a store
// element belongs to), "_key" (the element key), "_seq" (its position in its store), or the
// name of a field or counter of the model. A column with no From takes Fixed.
type column struct {
	Name, SQLType, From string
	Fixed               any
}

// index is a secondary key.
type index struct {
	Name   string
	Cols   []string
	Unique bool
}

// table is the SQL table and the MongoDB collection one source of the model is loaded into.
// Source is a bucket name ("User") for its documents or "Bucket.Store" for the elements of a
// store. Why says which read each key serves; it is printed above the CREATE TABLE.
type table struct {
	Name, Source, Why string
	Cols              []column
	PK                []string
	Keys              []index
}

// pairedStores are the stores a relational or document schema holds once, with a key on
// each side, instead of twice. hoardDB keeps both copies in the entity (that is the point of
// the layout); a competent SQL or MongoDB user keeps one row and indexes it both ways. The
// map is store -> the source whose table holds it; counts are checked on that source.
func pairedStores(layout model.Layout) map[string]string {
	switch layout {
	case model.Social:
		return map[string]string{"User.Following": "Follow", "User.Followers": "Follow", "User.Likes": "Post.Likes"}
	case model.Chat:
		return map[string]string{"Room.Members": "Member", "User.Rooms": "Member"}
	case model.Video:
		return map[string]string{"Viewer.Subscriptions": "Channel.Subscribers"}
	}
	return nil
}

func idCol(name string) column {
	return column{Name: name, SQLType: "VARCHAR(64) NOT NULL", From: "_id"}
}
func owner(name string) column {
	return column{Name: name, SQLType: "VARCHAR(64) NOT NULL", From: "_owner"}
}
func seq() column { return column{Name: "seq", SQLType: "BIGINT NOT NULL", From: "_seq"} }
func key(name, size string) column {
	return column{Name: name, SQLType: "VARCHAR(" + size + ") NOT NULL", From: "_key"}
}
func str(name, from string) column {
	return column{Name: name, SQLType: "VARCHAR(255) NOT NULL", From: from}
}
func text(name, from string) column { return column{Name: name, SQLType: "TEXT NOT NULL", From: from} }
func num(name, from string) column  { return column{Name: name, SQLType: "BIGINT NOT NULL", From: from} }
func idRef(name, from string) column {
	return column{Name: name, SQLType: "VARCHAR(64) NOT NULL", From: from}
}

// tables returns the tables of a layout in creation order.
func tables(layout model.Layout) []table {
	switch layout {
	case model.Social:
		return socialTables
	case model.Chat:
		return chatTables
	case model.Ledger:
		return ledgerTables
	case model.Video:
		return videoTables
	}
	return agentTables
}

var socialTables = []table{
	{Name: "users", Source: "User", Why: "a profile is read by id; the two counters are denormalised because a profile page shows them and counting the follows table per view would scan it",
		Cols: []column{idCol("id"), str("name", "Name"), str("handle", "Handle"), num("joined_at", "JoinedAt"), text("bio", "Bio"), num("followers_n", "followers_n"), num("following_n", "following_n")},
		PK:   []string{"id"}},
	{Name: "posts", Source: "Post", Why: "a post is read by id (the fetch-through after a feed read); the like and comment counters are denormalised for the same reason as the user counters",
		Cols: []column{idCol("id"), idRef("author", "Author"), text("text", "Text"), num("at", "At"), num("likes_n", "likes_n"), num("comments_n", "comments_n")},
		PK:   []string{"id"}},
	{Name: "social_groups", Source: "Group", Why: "a group is read by id",
		Cols: []column{idCol("id"), str("name", "Name"), idRef("owner", "Owner")},
		PK:   []string{"id"}},
	{Name: "follows", Source: "Follow", Why: "one row per follow edge replaces User.Following and User.Followers; the primary key answers who a user follows, follows_by_followee answers who follows v (the read-followers operation) without scanning every edge",
		Cols: []column{idRef("follower_id", "From"), idRef("followee_id", "To"), num("at", "At")},
		PK:   []string{"follower_id", "followee_id"}, Keys: []index{{Name: "follows_by_followee", Cols: []string{"followee_id", "follower_id"}}}},
	{Name: "feed", Source: "User.Feed", Why: "a user's feed page is the newest rows of one user: the (user_id, seq) key makes it a backward range scan with no sort",
		Cols: []column{owner("user_id"), seq(), idRef("post_id", "post"), num("at", "at")},
		PK:   []string{"user_id", "seq"}},
	{Name: "inbox", Source: "User.Inbox", Why: "the fan-out target: a follower's inbox page is a range scan on (user_id, seq), as for the feed",
		Cols: []column{owner("user_id"), seq(), idRef("post_id", "post"), idRef("author", "author"), num("at", "at")},
		PK:   []string{"user_id", "seq"}},
	{Name: "notifications", Source: "User.Notifications", Why: "newest first per user: backward scan of (user_id, seq)",
		Cols: []column{owner("user_id"), seq(), str("kind", "kind"), idRef("actor", "actor"), idRef("post_id", "post"), num("at", "at")},
		PK:   []string{"user_id", "seq"}},
	{Name: "photos", Source: "User.Photos", Why: "newest first per user: backward scan of (user_id, seq); the bytes are not stored, only the size, as in the hoardDB element",
		Cols: []column{owner("user_id"), seq(), str("name", "name"), num("bytes", "bytes")},
		PK:   []string{"user_id", "seq"}},
	{Name: "friends", Source: "User.Friends", Why: "a keyed set per user: the primary key (user_id, friend_id) answers membership and the list",
		Cols: []column{owner("user_id"), key("friend_id", "64"), num("since", "since")},
		PK:   []string{"user_id", "friend_id"}},
	{Name: "likes", Source: "Post.Likes", Why: "one row per (post, user) replaces Post.Likes and User.Likes; the primary key answers did u like p and the likes of p, likes_by_user answers the likes of u",
		Cols: []column{owner("post_id"), key("user_id", "64"), num("at", "at")},
		PK:   []string{"post_id", "user_id"}, Keys: []index{{Name: "likes_by_user", Cols: []string{"user_id", "post_id"}}}},
	{Name: "settings", Source: "User.Settings", Why: "keyed overwrite per user: the primary key (user_id, setting) makes an upsert one row",
		Cols: []column{owner("user_id"), key("setting", "64"), str("value", "value")},
		PK:   []string{"user_id", "setting"}},
	{Name: "drafts", Source: "User.Drafts", Why: "a priority queue per user: drafts_by_priority orders a user's drafts for the pop; nothing is seeded because the ring client cannot push a heap entry",
		Cols: []column{owner("user_id"), key("draft_id", "64"), num("priority", "priority"), text("text", "text")},
		PK:   []string{"user_id", "draft_id"}, Keys: []index{{Name: "drafts_by_priority", Cols: []string{"user_id", "priority", "draft_id"}}}},
	{Name: "comments", Source: "Post.Comments", Why: "a post's thread in order: range scan of (post_id, seq)",
		Cols: []column{owner("post_id"), seq(), idRef("author", "author"), text("text", "text"), num("at", "at")},
		PK:   []string{"post_id", "seq"}},
	{Name: "group_members", Source: "Group.Members", Why: "the member set of a group, read for the wall fan-out: primary key (group_id, user_id)",
		Cols: []column{owner("group_id"), key("user_id", "64"), str("role", "role")},
		PK:   []string{"group_id", "user_id"}},
	{Name: "wall", Source: "Group.Wall", Why: "a group's wall in order: range scan of (group_id, seq)",
		Cols: []column{owner("group_id"), seq(), idRef("author", "author"), text("text", "text"), num("at", "at")},
		PK:   []string{"group_id", "seq"}},
}

var chatTables = []table{
	{Name: "rooms", Source: "Room", Why: "a room is read by id; member and message counters are denormalised because a room list shows them",
		Cols: []column{idCol("id"), str("name", "Name"), num("member_n", "member_n"), num("message_n", "message_n")},
		PK:   []string{"id"}},
	{Name: "users", Source: "User", Why: "a user is read by id",
		Cols: []column{idCol("id"), str("name", "Name")},
		PK:   []string{"id"}},
	{Name: "members", Source: "Member", Why: "one row per join replaces Room.Members, User.Rooms and the Member bucket; the primary key lists a room's members, members_by_user lists a user's rooms",
		Cols: []column{idRef("room_id", "Room"), idRef("user_id", "User"), num("since", "Since")},
		PK:   []string{"room_id", "user_id"}, Keys: []index{{Name: "members_by_user", Cols: []string{"user_id", "room_id"}}}},
	{Name: "messages", Source: "Room.Messages", Why: "a room's last page is the newest rows of one room: the (room_id, seq) key makes it a backward range scan",
		Cols: []column{owner("room_id"), seq(), idRef("author", "author"), text("text", "text"), num("at", "at")},
		PK:   []string{"room_id", "seq"}},
}

var ledgerTables = []table{
	{Name: "accounts", Source: "Account", Why: "an account is read by id; the expense sum and counts are denormalised because the page read shows the sum and recomputing it scans every expense",
		Cols: []column{idCol("id"), str("name", "Name"), {Name: "currency", SQLType: "CHAR(3) NOT NULL", From: "Currency"}, num("expense_n", "expense_n"), num("expense_total", "expense_total"), num("invoice_n", "invoice_n"), num("invoice_total", "invoice_total")},
		PK:   []string{"id"}},
	{Name: "expenses", Source: "Account.Expenses", Why: "the expenses page of an account in time order: the key <time>-<id> sorts by time, so the (account_id, expense_key) primary key serves the page and the range read",
		Cols: []column{owner("account_id"), key("expense_key", "32"), num("amount", "amount"), str("category", "category"), {Name: "currency", SQLType: "CHAR(3) NOT NULL", From: "currency"}},
		PK:   []string{"account_id", "expense_key"}},
	{Name: "invoices", Source: "Account.Invoices", Why: "an invoice is put and read by number within its account: primary key (account_id, number)",
		Cols: []column{owner("account_id"), key("number", "32"), num("amount", "amount"), str("customer", "customer"), num("due", "due")},
		PK:   []string{"account_id", "number"}},
}

var agentTables = []table{
	{Name: "users", Source: "User", Why: "a user is read by id; the memory counter is denormalised for a profile view",
		Cols: []column{idCol("id"), str("name", "Name"), num("memory_n", "memory_n")},
		PK:   []string{"id"}},
	{Name: "sessions", Source: "Session", Why: "a session is read by id; the message counter is denormalised for a session list",
		Cols: []column{idCol("id"), idRef("owner", "Owner"), num("message_n", "message_n")},
		PK:   []string{"id"}},
	{Name: "session_messages", Source: "Session.Messages", Why: "a turn appends and a read takes the newest rows of one session: backward range scan of (session_id, seq)",
		Cols: []column{owner("session_id"), seq(), str("role", "role"), text("text", "text"), num("at", "at")},
		PK:   []string{"session_id", "seq"}},
	{Name: "runs", Source: "Run", Why: "a run is read by id",
		Cols: []column{idCol("id"), idRef("session", "Session"), num("task_n", "task_n")},
		PK:   []string{"id"}},
	{Name: "scratch", Source: "Run.Scratch", Why: "a keyed overwrite per run: primary key (run_id, scratch_key) makes a step's put one row",
		Cols: []column{owner("run_id"), key("scratch_key", "64"), text("value", "value")},
		PK:   []string{"run_id", "scratch_key"}},
	{Name: "tasks", Source: "Run.Tasks", Why: "a queue per run: a claim takes the oldest ready rows, so tasks_ready (run_id, state, seq) finds them without a scan; state is 0 ready, 1 leased, 2 acked and lease_until is the lease deadline, the columns a SQL queue needs that hoardDB's fifoStore keeps itself",
		Cols: []column{owner("run_id"), seq(), str("task", "task"), text("payload", "payload"),
			{Name: "state", SQLType: "TINYINT NOT NULL", Fixed: int64(0)}, {Name: "lease_until", SQLType: "BIGINT NOT NULL", Fixed: int64(0)}},
		PK: []string{"run_id", "seq"}, Keys: []index{{Name: "tasks_ready", Cols: []string{"run_id", "state", "seq"}}}},
	{Name: "memories", Source: "User.Memories", Why: "a keyed set per user listed by key: primary key (user_id, memory_id); the 64-value vector is a 256-byte BLOB with no index because no verb searches by vector yet",
		Cols: []column{owner("user_id"), key("memory_id", "64"), text("text", "text"), {Name: "vec", SQLType: "BLOB NOT NULL", From: "vec"}},
		PK:   []string{"user_id", "memory_id"}},
}

var videoTables = []table{
	{Name: "videos", Source: "Video", Why: "a video is read by id (the watch page); only the path to its bytes is stored, as in the hoardDB document; views is the hot counter, an integer column updated in place; the comment and like counters are denormalised because the page shows them and counting the child tables per view would scan them",
		Cols: []column{idCol("id"), idRef("owner", "Owner"), str("title", "Title"), str("path", "Path"), num("duration", "Duration"), num("views", "Views"), num("comments_n", "comments_n"), num("likes_n", "likes_n")},
		PK:   []string{"id"}},
	{Name: "channels", Source: "Channel", Why: "a channel is read by id; the subscriber counter is denormalised for the channel header",
		Cols: []column{idCol("id"), str("name", "Name"), num("subscriber_n", "subscriber_n")},
		PK:   []string{"id"}},
	{Name: "viewers", Source: "Viewer", Why: "a viewer is read by id",
		Cols: []column{idCol("id"), str("name", "Name")},
		PK:   []string{"id"}},
	{Name: "comments", Source: "Video.Comments", Why: "a video's thread in order: range scan of (video_id, seq)",
		Cols: []column{owner("video_id"), seq(), idRef("author", "author"), text("text", "text"), num("at", "at")},
		PK:   []string{"video_id", "seq"}},
	{Name: "likes", Source: "Video.Likes", Why: "did viewer w like video v, and who liked v, is the primary key (video_id, viewer_id); likes_by_viewer is the key the other way, for the videos a viewer liked",
		Cols: []column{owner("video_id"), key("viewer_id", "64"), num("at", "at")},
		PK:   []string{"video_id", "viewer_id"}, Keys: []index{{Name: "likes_by_viewer", Cols: []string{"viewer_id", "video_id"}}}},
	{Name: "uploads", Source: "Channel.Uploads", Why: "a channel's page lists newest uploads first: the (channel_id, seq) key makes it a backward range scan; uploads_by_at (channel_id, at) serves the same page by time when uploads arrive out of order, and an ascending key is read backward at the same cost as a descending one",
		Cols: []column{owner("channel_id"), seq(), idRef("video_id", "video"), num("at", "at")},
		PK:   []string{"channel_id", "seq"}, Keys: []index{{Name: "uploads_by_at", Cols: []string{"channel_id", "at"}}}},
	{Name: "subscriptions", Source: "Channel.Subscribers", Why: "one row per subscription replaces Channel.Subscribers and Viewer.Subscriptions; the primary key (channel_id, viewer_id) lists a channel's subscribers and answers is v subscribed, subscriptions_by_viewer lists the channels of a viewer",
		Cols: []column{owner("channel_id"), key("viewer_id", "64"), num("since", "since")},
		PK:   []string{"channel_id", "viewer_id"}, Keys: []index{{Name: "subscriptions_by_viewer", Cols: []string{"viewer_id", "channel_id"}}}},
	{Name: "progress", Source: "Viewer.Progress", Why: "resume position is put and read by video within a viewer: primary key (viewer_id, video_id) makes the overwrite one row and the continue-watching list a range scan",
		Cols: []column{owner("viewer_id"), key("video_id", "64"), num("position", "position"), num("watched_at", "watched_at")},
		PK:   []string{"viewer_id", "video_id"}},
	{Name: "history", Source: "Viewer.History", Why: "a viewer's history in order: range scan of (viewer_id, seq), newest by reading backward",
		Cols: []column{owner("viewer_id"), seq(), idRef("video_id", "video"), num("at", "at")},
		PK:   []string{"viewer_id", "seq"}},
	{Name: "playlists", Source: "Viewer.Playlists", Why: "a playlist is read in position order: primary key (viewer_id, playlist, position) lists a viewer's playlists and walks one in order; the hoardDB key is the playlist name and the zero-padded position joined by a slash",
		Cols: []column{owner("viewer_id"), {Name: "playlist", SQLType: "VARCHAR(64) NOT NULL", From: "_key:playlist"}, {Name: "position", SQLType: "BIGINT NOT NULL", From: "_key:position"}, idRef("video_id", "video"), num("added", "added")},
		PK:   []string{"viewer_id", "playlist", "position"}},
}

// expectedRows is what each source table must hold: the model's count of that source.
func expectedRows(layout model.Layout, counts model.Counts) (map[string]int64, error) {
	out := map[string]int64{}
	for _, spec := range tables(layout) {
		count, ok := counts[spec.Source]
		if !ok {
			return nil, fmt.Errorf("table %s names source %q, which layout %s does not have", spec.Name, spec.Source, layout)
		}
		out[spec.Name] = count
	}
	return out, nil
}
