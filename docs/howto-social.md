# How-to: a social network

A social network has users, and each user has a feed, an inbox, a set of
followers and a set of people they follow. Posts have comments and likes of
their own, groups have members and a wall, and every follow and every like is
read from both sides. This is the case the idea of data living with what it
describes was first drawn for: a person's feed is that person's own queue, not a
row in a table of everyone's feeds.

## The application and its reads

| Read or write | How often | Shape |
| --- | --- | --- |
| Publish a post | constantly | the post, the author's feed, and a few followers' inboxes |
| Comment on a post | often | the post's comments, and a notification to the author |
| Like a post | often | the post's likes and the user's likes |
| Follow a user | occasionally | the user's following, the other's followers, and the relationship |
| Post on a group's wall | occasionally | the group's wall, with the group's members read for fan-out |
| Save a draft, change a setting | occasionally | one keyed write on one user |
| Read a feed | every time the app opens | one user's feed, then each post by id |
| Read an inbox | every time the app opens | one user's inbox |
| Who follows v | on profile views | the follows whose target is v |

Everything on the hot path is about one user, one post or one group. Only the
last read crosses the kind.

## The layouts this application usually gets

**A relational schema.** `users`, `posts`, `comments (post_id, ...)`, `likes
(post_id, user_id)`, `follows (from_id, to_id)` with an index on each column, and
a feed built by a join of posts with follows at read time, or a `timeline` table
written by fan-out. Everything is correct and everything is a table shared by
every user. What goes wrong is the feed: the join over all follows is the
expensive read, and the fan-out table is the one that grows fastest.

**A document store.** `users`, `posts`, `comments`, `likes` and `follows`
collections with indexes on the foreign fields, or comments and likes embedded in
the post, which fails for any post that does well because the array grows without
bound. The feed is an aggregation over follows or a fan-out collection. It leaves
both sides of each follow and like to the indexes, and the cost of a feed read
to the number of follows.

**An in-memory data structure server.** A list per user for the feed and another
for the inbox, a set per user for followers and another for following, a set per
post for likes and a list per post for comments. The structures match the reads
exactly. What it lacks is a schema, a way to ask for every follow whose target is
v without a third structure, and a way to say that a user's lists belong to that
user.

## The hoardDB layout

```
create bucket User   in social { Name string, Handle string, JoinedAt int, Bio string,
                                 Feed fifoStore, Inbox fifoStore, Notifications lifoStore, Photos lifoStore,
                                 Friends hashStore, Following hashStore, Followers hashStore, Likes hashStore,
                                 Settings hashStore, Drafts heapStore }
create bucket Post   in social { Author string, Text string, At int, Comments fifoStore, Likes hashStore }
create bucket Group  in social { Name string, Owner string, Members hashStore, Wall fifoStore }
create bucket Follow in social { From string, To string, At int }
```

Four buckets. A user carries their own queues and sets, a post carries its own
comments and likes, a group its members and wall, and `Follow` is the
relationship itself.

**Why posts are their own bucket.** A post has comments and likes of its own, and
they are read with the post, not with its author. If the post lived inside the
author's document, a popular post would make its author's document the busiest
thing on the node. As its own entity, a post's comments and likes travel with
that post.

**Why `Feed` holds references.** The user's `Feed` is a FIFO of `{post, at}`: a
pointer to the post, not a copy of it. Reading a feed is a page of the user's
`Feed`, then one `get` per post id. A post is stored once, so an edit or a delete
is one write.

**Why `Inbox` is filled by fan-out.** When a user publishes, the application
pushes a reference into each follower's `Inbox`. Those pushes are the
application's writes, one per follower. In this layout that is at most a handful
of followers per post. A very large fan-out, one post to millions of inboxes, is
the case this layout does not serve.

**Why likes and follows are two-store pairs.** A like is `Post.Likes` and
`User.Likes`; a follow is `User.Following` and `User.Followers`. Each is a hash
keyed by the other side's id, so "who liked this post" and "what has this user
liked" are each one keyed read. The price is that the application writes both
sides.

**Why `Follow` is a bucket as well.** "Who follows v" is answered by the
followers hash on v for the hot path. `Follow` is the Membership-style bucket for
the read that crosses the kind: every follow whose `To` is v, as documents, with
when it happened, served by `list` with an equality filter.

**Why `Notifications` is a LIFO, `Drafts` a heap and `Settings` a hash.**
Notifications are read newest first, so a LIFO. `Drafts` is declared as a heap so
a draft can carry a priority and the highest comes out first; the seed declares
it and does not fill it. `Settings` is a hash by setting name, and a second
`put` of the same key replaces the first: the last write wins.

## What changes in the application

- Publishing is a sequence the application owns: the post, the author's feed, then
  each follower's inbox. If a later write fails, the application retries it with
  the same request id.
- Liking and following are two writes, and following is three if `Follow` is used.
  The application keeps them equal.
- Reading a feed is a page of the feed, then the posts of the page by id. From the
  Go client that is one call, [`GetMany`](#reading-many-documents-by-id); the shell
  has one `get` per id.
- An inbox read is one page of one user's inbox. There is no join.
- Settings are overwritten by key. Nothing needs a transaction.

## Reading many documents by id

The posts of a feed page are many documents you already know the ids of. The Go
client reads them in one frame instead of one `Get` each:

```go
page, err := client.Bucket("scenarios_social", "User").StoreList(ctx, "u7", "Feed", 20, nil)
// ... ids is the "post" member of each element of page.Elements ...
answers, err := client.Bucket("scenarios_social", "Post").GetMany(ctx, ids, nil)
for i, answer := range answers {
	switch {
	case answer.Err != nil: // that key was refused; the others are answered
	case !answer.Found: // the post is gone: a missing document is data, not an error
	default: // answer.Doc is the post
	}
}
```

The complete program is `ExampleBucket_GetMany` in the `ringclient` package, and a test
that runs the same read against a real server is `TestLiveGetManyReadsAFeedOfPostsInOneFrame`.
`Bucket.GetMany(ctx, ids, fields)` reads one bucket; `Client.GetMany(ctx, keys)` takes a
`Key` per document and may mix buckets and databases, a whole document or only named
fields (`Fields`), and `Local: true` per key.

Three things matter:

- **One frame, up to 255 keys.** More than 255 ids go as several frames sent at once and
  come back in the order you gave. The reply is bounded at 16 MiB: a document whose answer
  does not fit what is left is that key's `RESULT_TOO_LARGE` error, and the message says to
  ask for it again in a smaller frame or with `GetFields`. A document larger than 16 MiB
  on its own is read with `Get`.
- **Each key is read at its own current state, not as one snapshot.** Two posts in one
  call may be read a moment apart, and a post deleted since the feed was written is
  simply not there.
- **A missing document is `Found` false, not an error.** The call itself fails only for
  the connection or a refusal that names no key; a key that is refused (a bucket you may not
  read, an owner that is down, a bad `Fields` list) is that key's `Err` and the rest are
  answered. A frame's `WithWriteDeadline(ms)` bounds every key: keys not read when it is spent
  answer `UNAVAILABLE`, and reads change nothing, so you resend them.

The shell has no many-documents-by-id statement: `db.Post["p0"].get()` reads one, and
`help get` says so. To read a page of one bucket by a filter, use `list`.

## Cost and limits

**What a document costs.** A user with a dozen stores is one document: a few keys
in the node's one shared store for each store that holds something, with no file,
no descriptor and no goroutine of its own. Empty stores cost almost nothing. A post
costs one document of its own. The server's work to publish does not grow with the
number of users.

**What this layout does not serve.**

- A very large fan-out. Pushing one post into every follower's inbox is one write
  per follower, which is fine for a handful and wrong for an account with an
  audience.
- Ranking or search across the kind: the most-liked posts, a text search of every
  post, trending anything.
- A feed that interleaves by relevance. The feed is in arrival order, and sorting it
  is the application's.
- Likes and follows kept equal by the engine. Each pair is separate writes; a
  crash between them leaves the application to repair it.
- A feed read with the posts joined in one call. `GetMany` reads the posts of a page
  in one call, but the feed page is a separate read first, and the order is the feed's.

If one of those is on your hot path, this is not the layout for it, and the
[Limitations](limitations.md) page is the honest next read.

## Try it

Start a server and seed the layout as described on the [How-to](howto.md) page,
with `-layout social`. The transcripts below were captured against that data:
forty users, ten groups, two hundred posts, and a follow graph in which the
busiest user has eight followers. The seeded text is random words, so the shape is
real and the prose is not. `Drafts` is declared and the seed does not fill it.

A user's follows, from both sides and from the relationship bucket.

```
[h]oardDB [scenarios_social]> use scenarios_social
Switched to database 'scenarios_social'
[h]oardDB [scenarios_social]> db.User["u7"].Followers.list()
u14  {"at": 1700003780, "user": "u14"}
u15  {"at": 1700003840, "user": "u15"}
u16  {"at": 1700003900, "user": "u16"}
u17  {"at": 1700003960, "user": "u17"}
u18  {"at": 1700004020, "user": "u18"}
u21  {"at": 1700004200, "user": "u21"}
u24  {"at": 1700004380, "user": "u24"}
u32  {"at": 1700004860, "user": "u32"}
[h]oardDB [scenarios_social]> db.User["u21"].Following.list()
u16  {"at": 1700007980, "user": "u16"}
u24  {"at": 1700011340, "user": "u24"}
u32  {"at": 1700014700, "user": "u32"}
u6  {"at": 1700003780, "user": "u6"}
u7  {"at": 1700004200, "user": "u7"}
[h]oardDB [scenarios_social]> db.Follow.list({filter: {To: "u7"}, limit: 5})
{"At": 1700004200, "From": "u21", "To": "u7", "_id_": "u21>u7"}
{"At": 1700003960, "From": "u17", "To": "u7", "_id_": "u17>u7"}
{"At": 1700003840, "From": "u15", "To": "u7", "_id_": "u15>u7"}
{"At": 1700004860, "From": "u32", "To": "u7", "_id_": "u32>u7"}
{"At": 1700004380, "From": "u24", "To": "u7", "_id_": "u24>u7"}
more: db.Follow.list({filter: {To: "u7"}, limit: 5, cursor: "010000000000000001c797d0a84b08e15c0fa109507532343e75370000c7ac8826"})
```

Reading a feed by hand: a page of references, then the post by id.

```
[h]oardDB [scenarios_social]> db.User["u7"].Feed.list({limit: 2})
{"at": 1700002400, "post": "p0"}
{"at": 1700006600, "post": "p70"}
more: db.User["u7"].Feed.list({limit: 2, cursor: "0000000000000001"})
[h]oardDB [scenarios_social]> db.Post["p0"].get()
{"At": 1700002400, "Author": "u7", "Comments": "fifoStore (3 elements)", "Likes": "hashStore (1 element)", "Text": "pejh you xp kn hxbsubcgj qg fbmwgdaon hddknj wnhnekddq lxz vzme mdx cmbpbgbah dlsqq", "_id_": "p0"}
[h]oardDB [scenarios_social]> db.User["u7"].Inbox.peek(2)
{"at": 1700003300, "author": "u31", "post": "p15"}
{"at": 1700003780, "author": "u31", "post": "p23"}
```

Publishing is the post, the author's feed and the followers' inboxes. Here `u7`
publishes and two followers get it. The application writes each push.

```
[h]oardDB [scenarios_social]> db.Post.insert({_id_: "p900", Author: "u7", Text: "first post from the how-to", At: 1700600000})
OK  id=p900
[h]oardDB [scenarios_social]> db.User["u7"].Feed.push({post: "p900", at: 1700600000})
OK  pushed: Feed now holds 5 elements
[h]oardDB [scenarios_social]> db.User["u14"].Inbox.push({post: "p900", author: "u7", at: 1700600000})
OK  pushed: Inbox now holds 11 elements
[h]oardDB [scenarios_social]> db.User["u15"].Inbox.push({post: "p900", author: "u7", at: 1700600000})
OK  pushed: Inbox now holds 11 elements
```

A comment is on the post, with a notification to the author, which is a LIFO.

```
[h]oardDB [scenarios_social]> db.Post["p900"].Comments.push({author: "u14", text: "welcome", at: 1700600100})
OK  pushed: Comments now holds 1 element
[h]oardDB [scenarios_social]> db.User["u7"].Notifications.push({kind: "comment", post: "p900", actor: "u14", at: 1700600100})
OK  pushed: Notifications now holds 9 elements
```

A like is a pair of writes, one on each side.

```
[h]oardDB [scenarios_social]> db.Post["p900"].Likes.put("u15", {user: "u15", at: 1700600200})
OK  put "u15": Likes now holds 1 element
[h]oardDB [scenarios_social]> db.User["u15"].Likes.put("p900", {post: "p900", at: 1700600200})
OK  put "p900": Likes now holds 10 elements
[h]oardDB [scenarios_social]> db.User["u15"].Likes.get("p900")
{"at": 1700600200, "post": "p900"}
```

A follow is two store writes and the relationship document.

```
[h]oardDB [scenarios_social]> db.User["u30"].Following.put("u7", {user: "u7", at: 1700600300})
OK  put "u7": Following now holds 3 elements
[h]oardDB [scenarios_social]> db.User["u7"].Followers.put("u30", {user: "u30", at: 1700600300})
OK  put "u30": Followers now holds 9 elements
[h]oardDB [scenarios_social]> db.Follow.insert({_id_: "u30>u7", From: "u30", To: "u7", At: 1700600300})
OK  id=u30>u7
```

Groups and settings. A setting is overwritten by key, and the last write wins.

```
[h]oardDB [scenarios_social]> db.Group["g6"].Members.list()
u10  {"role": "member", "user": "u10"}
u21  {"role": "member", "user": "u21"}
u25  {"role": "admin", "user": "u25"}
u26  {"role": "admin", "user": "u26"}
[h]oardDB [scenarios_social]> db.Group["g6"].Wall.count()
5
[h]oardDB [scenarios_social]> db.User["u7"].Settings.put("theme", {value: "light"})
OK  put "theme": Settings now holds 3 elements
[h]oardDB [scenarios_social]> db.User["u7"].Settings.get("theme")
{"value": "light"}
[h]oardDB [scenarios_social]> db.User["u7"].Drafts.count()
0
```
