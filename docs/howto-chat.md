# How-to: chat rooms

A chat application has rooms, users, and a membership that is read from both
sides: "who is in this room" and "which rooms is this user in". Each room also
has a list of messages that grows all day. Rooms and users are the entities, and
the messages belong to the room, so this is a clear case for a layout where each
kind of data lives on the thing it describes.

## The application and its reads

| Read or write | How often | Shape |
| --- | --- | --- |
| Post a message to a room | constantly | one write, in arrival order |
| Read a page of a room's messages | every time a room is opened | a page of one room's messages |
| Show the members' names beside the page | every time a room is opened | one lookup per member of one room |
| List the rooms a user is in | on login and in the sidebar | one user's rooms |
| Join a room | occasionally | writes on the room, the user and the relationship |
| Who is in room r, with when they joined | admin screens | the memberships whose room is r |

Everything on the hot path is about one room or one user. Only the last read
crosses the kind, and it crosses it by one field.

## The layouts this application usually gets

**A relational schema.** Four tables: `rooms`, `users`, `memberships (room_id,
user_id, joined_at)` with a primary key on the pair and a second index on
`user_id`, and `messages (room_id, seq, author_id, text, at)` with a key on
`(room_id, seq)` so a room's page is a range scan. "A room's members" and "a
user's rooms" are both index reads on the one membership table, and the page with
names is a join. It is correct and it is the usual answer. What goes wrong is
scale of one table: every room's messages share one index, so the hottest room
and the quietest room compete for the same pages.

**A document store.** A `rooms` collection and a `users` collection, with
membership either embedded as an array on each side (two arrays to keep equal,
and arrays that grow without bound) or held in a `memberships` collection with
two indexes. Messages go in their own collection indexed on `(room, seq)`,
because a document holding every message of a room grows without limit. It
works, with the same shared index across rooms, and with the array version it
leaves keeping both sides equal to the application.

**An in-memory data structure server.** A list per room for messages, a set per
room for members, and a set per user for rooms. The structures match the reads,
which is why teams reach for it. What it lacks is a schema, a way to ask "which
memberships are in room r, with their join times" without maintaining a third
structure by hand, and durability by default.

## The hoardDB layout

```
create bucket Room    in chat { Name string, Messages fifoStore, Members hashStore }
create bucket User    in chat { Name string, Rooms hashStore }
create bucket Member  in chat { Room string, User string, Since int }
```

Three buckets. A room carries its own messages and its own members, a user
carries their own rooms, and the third bucket is the relationship itself.

**Why `Messages` is a FIFO on the room.** The read is "this room's messages, in
order". A FIFO keeps them in arrival order, `push` appends one, `peek` or `list`
reads a page of that room, and `count` says how many there are. A client that
wants the newest messages first would declare a `lifoStore` instead; the choice
is the read's, not the engine's.

**Why membership is written on both sides.** `Room.Members` is a hash keyed by
user id, and `User.Rooms` is a hash keyed by room id. "A room's members" is one
keyed read of one room, and "a user's rooms" is one keyed read of one user. Neither
needs a scan of the other kind. The price is that a join writes both sides, and
the application is the one that keeps them equal.

**Why there is also a `Member` bucket.** The one read that crosses the kind is
"who is in room r, with when they joined", or an admin listing of every
membership. A bucket whose documents are the relationship answers it with `list`
and an equality filter on `Room` or on `User`. It is the right tool for a read that
runs on an admin screen. It is not what the room page uses, because the room page
has `Members` on the room.

**Why names come from keyed gets.** The page shows the members' names, which live
on `User`. The application reads the member ids from the room and then does one
keyed `get` per member id. A one-call form that returns a page with the names
already joined is not available yet.

## What changes in the application

- A message is one `push` to the room; reading the page is one `peek` or
  `list`. There is no messages table to share with other rooms.
- Joining is three writes: `Room.Members`, `User.Rooms` and `Member`. The
  application issues them in that order. If a later write fails, it retries the
  later writes with the same request id until they land, so a retry never writes
  a second membership.
- A room's members and a user's rooms are each one read of one entity, with no
  join and no index to choose.
- Names are one `get` per member id. For a room of a few dozen people that is
  a few dozen small reads, and the application can cache them.
- Admin listings use the `Member` bucket. Nothing on the message path touches it.

## Cost and limits

**What a document costs.** A room with its messages and members is one document
with two stores: a few keys in the node's one shared store, no file, no
descriptor and no goroutine of its own. The `Member` bucket adds one small
document per membership. The server's work to post a message does not grow with
the number of rooms.

**What this layout does not serve.**

- A text search across every room's messages. Messages live on their room; a
  search across all of them is a scan of every room.
- "The most active rooms", or any ranking across the kind by the size of a store.
- Membership kept equal by the engine. The two sides and the `Member` document are
  separate writes; a crash between them leaves the application to repair it.
- A members' page with names in one call. That is not available yet.
- A very large room. A room's members are a hash on one document, which is
  right for rooms of thousands and wrong for a room that is a whole audience.

If one of those is on your hot path, this is not the layout for it, and the
[Limitations](limitations.md) page is the honest next read.

## Try it

Start a server and seed the layout as described on the [How-to](howto.md) page,
with `-layout chat`. The transcripts below were captured against that data:
twenty rooms of twenty messages each, with the users and their memberships
seeded on every side. The seeded text is random words, so the shape is real and
the prose is not.

A room: its members, a page of its messages, and the user's rooms.

```
[h]oardDB [scenarios_chat]> use scenarios_chat
Switched to database 'scenarios_chat'
[h]oardDB [scenarios_chat]> db.Room["r11"].get()
{"Members": "hashStore (8 elements)", "Messages": "fifoStore (20 elements)", "Name": "stpap qjskb", "_id_": "r11"}
[h]oardDB [scenarios_chat]> db.Room["r11"].Members.list()
u10  {"since": 1700066600, "user": "u10"}
u15  {"since": 1700066900, "user": "u15"}
u21  {"since": 1700067260, "user": "u21"}
u22  {"since": 1700067320, "user": "u22"}
u3  {"since": 1700066180, "user": "u3"}
u36  {"since": 1700068160, "user": "u36"}
u5  {"since": 1700066300, "user": "u5"}
u7  {"since": 1700066420, "user": "u7"}
[h]oardDB [scenarios_chat]> db.Room["r11"].Messages.count()
20
[h]oardDB [scenarios_chat]> db.Room["r11"].Messages.list({limit: 2})
{"at": 1700330000, "author": "u3", "text": "hrih en vtmstwv ivcbdb qkby ai jrkc aqas niu nmibtwds"}
{"at": 1700330030, "author": "u3", "text": "ykponkdjv pio aodsppk kmvkddjl aswu ubp"}
more: db.Room["r11"].Messages.list({limit: 2, cursor: "0000000000000001"})
[h]oardDB [scenarios_chat]> db.User["u36"].Rooms.list()
r11  {"room": "r11", "since": 1700068160}
r2  {"room": "r2", "since": 1700014160}
r9  {"room": "r9", "since": 1700056160}
```

The page lists a FIFO from the front, so it reads the oldest messages first; the
`more:` line is the statement that reads the next page. The members' names are
one keyed `get` per id from the room's member list.

```
[h]oardDB [scenarios_chat]> db.User["u10"].get()
{"Name": "agzerar euqkhec", "Rooms": "hashStore (4 elements)", "_id_": "u10"}
[h]oardDB [scenarios_chat]> db.User["u15"].get()
{"Name": "pzalfxg xcp", "Rooms": "hashStore (3 elements)", "_id_": "u15"}
```

A join is three writes. User `u2` joins room `r7`, and the application writes the
room's side, the user's side and the relationship, then reads both sides back.

```
[h]oardDB [scenarios_chat]> db.Room["r7"].Members.put("u2", {user: "u2", since: 1700400100})
OK  put "u2": Members now holds 8 elements
[h]oardDB [scenarios_chat]> db.User["u2"].Rooms.put("r7", {room: "r7", since: 1700400100})
OK  put "r7": Rooms now holds 2 elements
[h]oardDB [scenarios_chat]> db.Member.insert({_id_: "r7/u2", Room: "r7", User: "u2", Since: 1700400100})
OK  id=r7/u2
[h]oardDB [scenarios_chat]> db.User["u2"].Rooms.list()
r0  {"room": "r0", "since": 1700000120}
r7  {"room": "r7", "since": 1700400100}
```

The `Member` document's id is the room and user joined, so a retried insert is
refused instead of written twice:

```
[h]oardDB [scenarios_chat]> db.Member.insert({_id_: "r7/u2", Room: "r7", User: "u2", Since: 1700400100})
CLI error: DOCUMENT_EXISTS: document "r7/u2" is already in scenarios_chat.Member, and insert never replaces. Run 'db.Member.upsert({_id_: "r7/u2", …})' to replace it, or 'db.Member["r7/u2"].get()' to read what is there
```

A `put` on a hash with the same key is an overwrite, so the two store writes are
safe to repeat as well.

The one read that crosses the kind: every membership of a room, and every room a
user is in, by filter on the relationship bucket.

```
[h]oardDB [scenarios_chat]> db.Member.list({filter: {Room: "r7"}, limit: 10})
{"Room": "r7", "Since": 1700042000, "User": "u0", "_id_": "r7/u0"}
{"Room": "r7", "Since": 1700043080, "User": "u18", "_id_": "r7/u18"}
{"Room": "r7", "Since": 1700043020, "User": "u17", "_id_": "r7/u17"}
{"Room": "r7", "Since": 1700043860, "User": "u31", "_id_": "r7/u31"}
{"Room": "r7", "Since": 1700042840, "User": "u14", "_id_": "r7/u14"}
{"Room": "r7", "Since": 1700042780, "User": "u13", "_id_": "r7/u13"}
{"Room": "r7", "Since": 1700043380, "User": "u23", "_id_": "r7/u23"}
{"Room": "r7", "Since": 1700400100, "User": "u2", "_id_": "r7/u2"}
[h]oardDB [scenarios_chat]> db.Member.list({filter: {User: "u36"}, limit: 5})
{"Room": "r11", "Since": 1700068160, "User": "u36", "_id_": "r11/u36"}
{"Room": "r9", "Since": 1700056160, "User": "u36", "_id_": "r9/u36"}
{"Room": "r2", "Since": 1700014160, "User": "u36", "_id_": "r2/u36"}
```

The filter runs on the nodes that hold the bucket's slots and reads every document
there, because there are no indexes yet, so it is for admin screens and not for
the message path.

Last, a message from `u1`, who had just joined room `r5` with the same three writes.

```
[h]oardDB [scenarios_chat]> db.Room["r5"].Messages.push({author: "u1", text: "hello, I just joined", at: 1700400010})
OK  pushed: Messages now holds 21 elements
```
