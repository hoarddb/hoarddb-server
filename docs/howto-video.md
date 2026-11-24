# How-to: a video library

A video site keeps the video files somewhere built for serving bytes: a content
network, an object store, a shared volume behind the web servers. What it keeps
in the database is everything around the file: who owns it, its title and length,
where the file is, how many times it has been watched, its comments and likes,
who subscribes to which channel, and where each viewer stopped watching. The
same shape serves a podcast catalogue, a course library and a photo archive.

## The application and its reads

| Read or write | How often | Shape |
| --- | --- | --- |
| The watch page: the video, a page of its comments, the viewer's resume position | every play | three keyed reads on two entities |
| Count a view | every play | one hot counter on one video |
| Post a comment, like a video | often | one write on the video, one on the viewer for a like |
| The channel page: uploads newest first | often | one channel's list |
| Subscribe to a channel | sometimes | two writes, one per side |
| The viewer's subscriptions, history and playlists | on login, on the home page | keyed reads on one viewer |
| Resume where the viewer stopped | every play | one key on one viewer |

Every hot read is about one video, one channel or one viewer. The bytes of the
video are never part of any of them.

## The layouts this application usually gets

**A relational schema.** `videos (id, owner_id, title, path, duration, views)`,
`comments (video_id, seq, author_id, text, at)` keyed on `(video_id, seq)`,
`likes (video_id, viewer_id)` with a key both ways, `subscriptions (channel_id,
viewer_id)` with a key both ways, `progress (viewer_id, video_id, position)`,
`history (viewer_id, seq, video_id)`, `playlist_items (viewer_id, playlist,
position, video_id)`. The watch page is three queries on three tables. The view
counter is an `UPDATE ... SET views = views + 1` on the hottest rows in the
database, and the lock on those rows is where this schema spends its evenings.

**A document store.** One collection per entity, with comments in their own
collection indexed on `(video_id, seq)` rather than embedded, because a popular
video's comments would grow one document past any sane size. The counter is an
atomic increment on the video document, which is the right primitive; the
subscription pair and the progress table are separate collections with the
indexes the reads need.

**An in-memory data structure server.** A hash per video, a list per video for
comments, a set per video for likes, a sorted set per channel for uploads, a hash
per viewer for progress keyed by video id, and an integer key per video for the
counter with its own increment. The structures match the reads exactly, and the
file paths and counters are precisely the data a team does not want to lose when
the server restarts.

## The hoardDB layout

```
create bucket Video    in media { Owner string, Title string, Path string, Duration int, Views int, Comments fifoStore, Likes hashStore }
create bucket Channel  in media { Name string, Uploads lifoStore, Subscribers hashStore }
create bucket Viewer   in media { Name string, Subscriptions hashStore, Progress hashStore, History fifoStore, Playlists btreeStore }
```

**Why `Path` is a string and the bytes are elsewhere.** hoardDB has a blob store,
and it is the wrong tool for a two-gigabyte file that a web server should hand to
the kernel with a range request. The document says where the file is; the servers
that already know how to stream a file do the streaming. The database holds the
truth about the file, not the file.

**Why `Comments` is a FIFO and `Likes` a hash, on the video.** A comment thread is
read in arrival order under the video it belongs to; a like is looked up and
removed by the viewer who gave it. Both live on the video document, so the watch
page's comment page is one read of one entity with no other video's comments in
the way.

**Why `Views` is a plain field.** The view count is the hottest write in the
application and the one that other layouts solve with a server-side increment.
Today the application reads the document and writes it back with the new count,
which is correct for one writer and loses increments under many. An atomic
increment on the server is not available yet; this page says so because
a page that hid it would be wrong at the first popular video.

**Why `Uploads` is a LIFO on the channel.** The channel page shows the newest
upload first. A LIFO returns the most recent element first with `peek` and
`list`, so the page is one read with no sort.

**Why subscriptions are written on both sides.** `Channel.Subscribers` answers
"who subscribes to this channel" and `Viewer.Subscriptions` answers "which
channels does this viewer follow". Each is one keyed read on its own entity, and
the application writes both when a viewer subscribes, retrying the second with
the same request id if it fails.

**Why `Progress` is a hash on the viewer keyed by video id.** Resume position is
per viewer per video, and the read is always "this viewer, this video". A hash on
the viewer keyed by the video's id makes it one keyed read on the entity that
owns the watching, not the entity being watched.

**Why `Playlists` is a btree.** A playlist is an ordered list of videos, and its
key is the playlist name followed by a zero-padded position, so one playlist's
items come back in order from one range of one store. This is the layout's one
use of a btree for order rather than for time.

## What changes in the application

- The watch page is three keyed reads: the video, a page of its comments, the
  viewer's progress. No join, and no comments table shared by every video.
- Serving the file is the web server's job, as it was; only the path lookup
  moves.
- A subscription is two writes the application sequences. A crash between them
  is repaired by the retry, and both sides carry the subscription date.
- The view counter is read and written back by the application until the
  server-side increment ships, so a very popular video should count views in the
  application's own aggregation until then.
- Progress and history are written on the viewer, which is the entity that
  changes most and reads least.

## Cost and limits

**What a document costs.** A video with its comments and likes is one document
and two stores: a few keys in the node's one shared store, no file, no descriptor
and no goroutine of its own. Serving a watch page reads that one video's
document and its stores, not the other videos in the library.

**What this layout does not serve.**

- Search by title or description across every video. Titles live on their
  videos; a search across all of them is a scan of every document.
- A trending or most-viewed ranking across the kind. That is a sort of every
  video by a field, which no store here provides; an application keeps its own
  ranking from the counters it reads.
- A single hot counter under thousands of concurrent viewers, until the atomic
  increment ships.
- A comment thread that is itself the product, with threads of thousands of
  replies read by rank. That is a different layout, with each thread its own
  entity.

If one of those is on your hot path, this is not the layout for it, and the
[Limitations](limitations.md) page is the honest next read.

## Try it

Start a server and seed the layout as described on the [How-to](howto.md) page,
with `hoarddb seed video`. The transcript below was captured against that data:
twenty channels with five uploads each, forty viewers, a hundred videos with
eight comments and six likes each, and progress, history and playlists for every
viewer.

The watch page is three keyed reads: the video, a page of its comments, the
viewer's resume position. Here the viewer has not watched this video yet, and the
miss says what to run next.

```
[h]oardDB [media]> use scenarios_video
Switched to database 'scenarios_video'
[h]oardDB [scenarios_video]> db.Video["v17"].get()
{"Comments": "fifoStore (8 elements)", "Duration": 4739, "Likes": "hashStore (6 elements)", "Owner": "c3", "Path": "library/2025/06/v017.mp4", "Title": "bnnb ulmco tt ofweupcvr", "Views": 80632, "_id_": "v17"}
[h]oardDB [scenarios_video]> db.Video["v17"].Comments.peek(2)
{"at": 1700061260, "author": "w1", "text": "mqoesyy fdi tytkxcs mr ihhsgu yrodlvcfs"}
{"at": 1700061320, "author": "w31", "text": "xcv wosrv yhm fm vdnvixprz cwpdd"}
[h]oardDB [scenarios_video]> db.Viewer["w5"].Progress.get("v17")
CLI error: NOT_FOUND: Progress of "w5" in scenarios_video.Viewer has no key "v17". Run 'db.Viewer["w5"].Progress.list()' to see what it holds, or 'db.Viewer["w5"].Progress.put("v17", {…})' to add one
```

`Path` is where the file is; the web server reads it from there. The document
shows each store as its type and element count, and the stores are read on the
document. The seeded titles and comments are random words, so the shape is real
and the prose is not.

Watching: the viewer's position is a put on the viewer keyed by the video, a
comment is a push on the video, a like is a put keyed by the viewer, and the view
count is read and written back, which is the honest form until a server-side
increment exists.

```
[h]oardDB [scenarios_video]> db.Viewer["w5"].Progress.put("v17", {position: 600, watched_at: 1700400000})
OK  put "v17": Progress now holds 11 elements
[h]oardDB [scenarios_video]> db.Viewer["w5"].Progress.get("v17")
{"position": 600, "watched_at": 1700400000}
[h]oardDB [scenarios_video]> db.Video["v17"].Comments.push({author: "w5", text: "the part at ten minutes is the one to watch", at: 1700400060})
OK  pushed: Comments now holds 9 elements
[h]oardDB [scenarios_video]> db.Video["v17"].Likes.put("w5", {at: 1700400070})
OK  put "w5": Likes now holds 6 elements
[h]oardDB [scenarios_video]> db.Video.update({_id_: "v17", $set: {Views: 80633}})
OK  updated id=v17
[h]oardDB [scenarios_video]> db.Video["v17"].get({fields: ["Title", "Views"]})
{"Title": "bnnb ulmco tt ofweupcvr", "Views": 80633, "_id_": "v17"}
```

The like did not raise the count: this viewer had already liked the video, and a
`put` under the same key replaces the element, so a viewer counts once however
many times they click. The view count went from 80632 to 80633 because one
process read it and wrote it back; two processes doing that at once would lose
one, which is exactly the case a server-side increment is for.

The channel page and the subscription written on both sides.

```
[h]oardDB [scenarios_video]> db.Channel["c3"].get()
{"Name": "zmkbbcmk cfpubln", "Subscribers": "hashStore (5 elements)", "Uploads": "lifoStore (5 elements)", "_id_": "c3"}
[h]oardDB [scenarios_video]> db.Channel["c3"].Uploads.peek(2)
{"at": 1700068400, "video": "v19"}
{"at": 1700064800, "video": "v18"}
[h]oardDB [scenarios_video]> db.Channel["c3"].Subscribers.put("w5", {since: 1700400080})
OK  put "w5": Subscribers now holds 6 elements
[h]oardDB [scenarios_video]> db.Viewer["w5"].Subscriptions.put("c3", {since: 1700400080})
OK  put "c3": Subscriptions now holds 5 elements
[h]oardDB [scenarios_video]> db.Viewer["w5"].Subscriptions.list()
c12  {"since": 1700003720}
c14  {"since": 1700003840}
c15  {"since": 1700003900}
c19  {"since": 1700004140}
c3  {"since": 1700400080}
```

The LIFO gives the newest upload first with no sort. The subscription is two
writes, each on its own entity, each carrying the date, and either side answers
its question with one keyed read.

The viewer's home page: progress, a playlist in order, and the history count.

```
[h]oardDB [scenarios_video]> db.Viewer["w5"].Progress.list({limit: 3})
v17  {"position": 600, "watched_at": 1700400000}
v21  {"position": 1844, "watched_at": 1700128227}
v32  {"position": 3117, "watched_at": 1700187916}
more: db.Viewer["w5"].Progress.list({limit: 3, cursor: "507633320000"})
[h]oardDB [scenarios_video]> db.Viewer["w5"].Playlists.list({limit: 5})
favorites/0000  {"added": 1700390000, "video": "v80"}
favorites/0001  {"added": 1700390060, "video": "v2"}
favorites/0002  {"added": 1700390120, "video": "v8"}
favorites/0003  {"added": 1700390180, "video": "v10"}
favorites/0004  {"added": 1700390240, "video": "v20"}
more: db.Viewer["w5"].Playlists.list({limit: 5, cursor: "506661766f72697465732f303030340000"})
[h]oardDB [scenarios_video]> db.Viewer["w5"].History.count()
20
```

The playlist key is the playlist's name, then a zero-padded position, so one
playlist's items come back in order from one range of the btree, and the next
playlist follows it.
