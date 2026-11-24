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
with `hoarddb seed chat`. The transcripts below were captured against that data:
145 rooms of one hundred messages each, with forty users and their memberships
seeded on every side. The seeded text is random words, so the shape is real and
the prose is not.

A room: its members, a page of its messages, and the user's rooms.

```
[h]oardDB [scenarios_chat]> use scenarios_chat
Switched to database 'scenarios_chat'
[h]oardDB [scenarios_chat]> db.Room["r11"].get()
{"Members": "hashStore (8 elements)", "Messages": "fifoStore (100 elements)", "Name": "stpap qjskb", "_id_": "r11"}
[h]oardDB [scenarios_chat]> db.Room["r11"].Members.list()
u0  {"since": 1700066000, "user": "u0"}
u13  {"since": 1700066780, "user": "u13"}
u14  {"since": 1700066840, "user": "u14"}
u19  {"since": 1700067140, "user": "u19"}
u20  {"since": 1700067200, "user": "u20"}
u30  {"since": 1700067800, "user": "u30"}
u35  {"since": 1700068100, "user": "u35"}
u5  {"since": 1700066300, "user": "u5"}
[h]oardDB [scenarios_chat]> db.Room["r11"].Messages.count()
100
[h]oardDB [scenarios_chat]> db.Room["r11"].Messages.list({limit: 2})
{"at": 1700330000, "author": "u20", "text": "hrih en vtmstwv ivcbdb qkby ai jrkc aqas niu nmibtwds"}
{"at": 1700330030, "author": "u20", "text": "ykponkdjv pio aodsppk kmvkddjl aswu ubp"}
more: db.Room["r11"].Messages.list({limit: 2, cursor: "0000000000000001"})
[h]oardDB [scenarios_chat]> db.User["u36"].Rooms.list()
r1  {"room": "r1", "since": 1700008160}
r102  {"room": "r102", "since": 1700614160}
r109  {"room": "r109", "since": 1700656160}
r116  {"room": "r116", "since": 1700698160}
r125  {"room": "r125", "since": 1700752160}
r133  {"room": "r133", "since": 1700800160}
r139  {"room": "r139", "since": 1700836160}
r14  {"room": "r14", "since": 1700086160}
r22  {"room": "r22", "since": 1700134160}
r29  {"room": "r29", "since": 1700176160}
r36  {"room": "r36", "since": 1700218160}
r43  {"room": "r43", "since": 1700260160}
r50  {"room": "r50", "since": 1700302160}
r57  {"room": "r57", "since": 1700344160}
r64  {"room": "r64", "since": 1700386160}
r7  {"room": "r7", "since": 1700044160}
r71  {"room": "r71", "since": 1700428160}
r79  {"room": "r79", "since": 1700476160}
r87  {"room": "r87", "since": 1700524160}
r94  {"room": "r94", "since": 1700566160}
```

The page lists a FIFO from the front, so it reads the oldest messages first; the
`more:` line is the statement that reads the next page. The members' names are
one keyed `get` per id from the room's member list.

```
[h]oardDB [scenarios_chat]> db.User["u10"].get()
{"Name": "agzerar euqkhec", "Rooms": "hashStore (20 elements)", "_id_": "u10"}
[h]oardDB [scenarios_chat]> db.User["u15"].get()
{"Name": "pzalfxg xcp", "Rooms": "hashStore (20 elements)", "_id_": "u15"}
```

A join is three writes. User `u2` joins room `r7`, and the application writes the
room's side, the user's side and the relationship, then reads both sides back.

```
[h]oardDB [scenarios_chat]> db.Room["r7"].Members.put("u2", {user: "u2", since: 1700400100})
OK  put "u2": Members now holds 8 elements
[h]oardDB [scenarios_chat]> db.User["u2"].Rooms.put("r7", {room: "r7", since: 1700400100})
OK  put "r7": Rooms now holds 21 elements
[h]oardDB [scenarios_chat]> db.Member.insert({_id_: "r7/u2", Room: "r7", User: "u2", Since: 1700400100})
OK  id=r7/u2
[h]oardDB [scenarios_chat]> db.User["u2"].Rooms.list()
r100  {"room": "r100", "since": 1700600120}
r107  {"room": "r107", "since": 1700642120}
r114  {"room": "r114", "since": 1700684120}
r123  {"room": "r123", "since": 1700738120}
r13  {"room": "r13", "since": 1700078120}
r130  {"room": "r130", "since": 1700780120}
r138  {"room": "r138", "since": 1700828120}
r144  {"room": "r144", "since": 1700864120}
r20  {"room": "r20", "since": 1700120120}
r28  {"room": "r28", "since": 1700168120}
r34  {"room": "r34", "since": 1700204120}
r41  {"room": "r41", "since": 1700246120}
r49  {"room": "r49", "since": 1700294120}
r55  {"room": "r55", "since": 1700330120}
r6  {"room": "r6", "since": 1700036120}
r63  {"room": "r63", "since": 1700378120}
r7  {"room": "r7", "since": 1700400100}
r70  {"room": "r70", "since": 1700420120}
r76  {"room": "r76", "since": 1700456120}
r85  {"room": "r85", "since": 1700510120}
r92  {"room": "r92", "since": 1700552120}
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
{"Room": "r7", "Since": 1700043080, "User": "u18", "_id_": "r7/u18"}
{"Room": "r7", "Since": 1700044160, "User": "u36", "_id_": "r7/u36"}
{"Room": "r7", "Since": 1700042420, "User": "u7", "_id_": "r7/u7"}
{"Room": "r7", "Since": 1700042360, "User": "u6", "_id_": "r7/u6"}
{"Room": "r7", "Since": 1700043440, "User": "u24", "_id_": "r7/u24"}
{"Room": "r7", "Since": 1700042480, "User": "u8", "_id_": "r7/u8"}
{"Room": "r7", "Since": 1700043380, "User": "u23", "_id_": "r7/u23"}
{"Room": "r7", "Since": 1700400100, "User": "u2", "_id_": "r7/u2"}
[h]oardDB [scenarios_chat]> db.Member.list({filter: {User: "u36"}, limit: 5})
{"Room": "r79", "Since": 1700476160, "User": "u36", "_id_": "r79/u36"}
{"Room": "r116", "Since": 1700698160, "User": "u36", "_id_": "r116/u36"}
{"Room": "r139", "Since": 1700836160, "User": "u36", "_id_": "r139/u36"}
{"Room": "r36", "Since": 1700218160, "User": "u36", "_id_": "r36/u36"}
{"Room": "r43", "Since": 1700260160, "User": "u36", "_id_": "r43/u36"}
more: db.Member.list({filter: {User: "u36"}, limit: 5, cursor: "010000000000000001f71c16461de345c504ca0a507234332f753336000013be80e2"})
```

The filter runs on the nodes that hold the bucket's slots and reads every document
there, because there are no indexes yet, so it is for admin screens and not for
the message path.

Last, a message from `u1`, who had just joined room `r5` with the same three writes.

```
[h]oardDB [scenarios_chat]> db.Room["r5"].Messages.push({author: "u1", text: "hello, I just joined", at: 1700400010})
OK  pushed: Messages now holds 101 elements
```
