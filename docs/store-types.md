# Store Types

hoardDB has six stores, and you pick the one that matches how you read. A bucket
declares fields, and a field can be a store of any of the six types, so every
document keeps its own queues, maps and files:

```text
create bucket User { Name string, Photos lifoStore, Purchases fifoStore, Friends hashStore,
    Recipes btreeStore, Books heapStore, Data blobStore }
```

A store is written through its document, `db.<bucket>["<id>"].<field>.<verb>()`,
never through the bucket: `insert` and `upsert` set a document's scalar fields
and nothing else.

## Quick reference

The word you write is the store's name followed by `Store`: **`fifoStore`**,
**`lifoStore`**, **`hashStore`**, **`btreeStore`**, **`heapStore`** and
**`blobStore`**. The first column is what goes after the field name.

| Write | What it is | Reads back | Use it for | Verbs that run |
|-------|-----------|------------|------------|----------------|
| `fifoStore` | a queue (FIFO) | the oldest element first | queues, logs, event streams | `push` `pop` `peek` `list` `count` `sum` `describe` `nearest` `claim` `ack` `release` `extend` `get` |
| `lifoStore` | a stack (LIFO) | the newest element first | recent items, undo, a feed | `push` `pop` `peek` `list` `count` `sum` `describe` `nearest` `claim` `ack` `release` `extend` `get` |
| `hashStore` | a map | an element by its key | profiles, settings, sessions | `put` `get` `take` `inc` `list` `count` `sum` `describe` `nearest` |
| `btreeStore` | an ordered map | elements by key, in key order | leaderboards, sorted data, time series | `put` `get` `take` `inc` `list` `count` `sum` `describe` `nearest` |
| `heapStore` | a priority queue | elements by priority | task queues, alerts, scheduling | `pop` `peek` `list` `count` `sum` `describe` `nearest` `get` |
| `blobStore` | large values and files | the bytes you stored | files, images, binary assets | `count` |

Two stores do not run every verb yet: a `heapStore` has no `push` statement in the
CLI (the Go client's `Bucket.PushHeap` writes one) and a `blobStore` has no `put`
([Limitations](limitations.md#not-available-yet)). `help types` in a
session always lists the verbs that run on the build you have, and `help <verb>`
(`help push`, `help claim`) prints a statement you can run as it stands.

Spell the word exactly. `fifo`, `FIFO`, `heap` and `queue` are names of the
structures, not words the grammar knows, and `FifoStore` is refused for its
capital letter; the refusal names the spelling that works.

[Getting started](getting-started.md) walks through each store, and [Data types](data-types.md)
covers the scalar fields that sit beside them.

A document's own scalar fields are read with `get` and `list`; a bucket's stores are
reached through its documents, so `list({filter: …})` reads every document of the
bucket and does not look inside a store. There is no bucket type, no
per-bucket placement and no indexes: a bucket declares fields, and each field is a
scalar or a store of one of the six kinds.

## Sums and counts a store keeps for you

A store can keep a sum or a count of its elements current, so you read the number in one step instead of adding the elements up on every page. You declare which ones when you create the bucket, after the store's type:

```
create bucket Property in app { Name string,
    Expenses btreeStore with (sum Amount, sum Amount by Currency, count by Category),
    Tickets  fifoStore  with (count by Status) }
```

- `sum Amount` is the sum of the `Amount` field of every element in the store.
- `sum Amount by Currency` is one sum and one count per value of `Currency`.
- `count by Category` is one count per value of `Category`.

A path may be dotted, up to three keys deep (`Amount.Cents`). A `by` value may take at most 1024 bytes as a key (about 1020 ASCII characters; a string in a script that takes three bytes a character holds about 340); a longer one is refused before anything is written, naming the path. A store keeps at most 8 aggregates, and every element must have the fields they name: a push or put of an element with no number at the summed path, or with a decimal there, is refused before anything is written, naming the path. Keep money in integer minor units (cents): integers add exactly, while doubles drift by the usual floating-point error under many adds and removes, and a store that ever held a double reports its sum as a double from then on. The absolute values of the elements one aggregate covers add up to at most 2^62 (about 4.6e18); an add that would pass it is refused, so a sum can always be taken back down, whatever order the elements leave in.

Read them from the document that owns the store:

```
db.<bucket>["<id>"].<store>.sum("<Field>")
db.<bucket>["<id>"].<store>.sum("<Field>", {by: "<Field>"})
db.<bucket>["<id>"].<store>.count({by: "<Field>"})
db.<bucket>["<id>"].<store>.describe()
```

For the Property bucket above, `sum("Amount")` on the Expenses store of property `p1` answers the total, `sum("Amount", {by: "Currency"})` one line per currency, and `count({by: "Category"})` one line per category.

A sum or a count includes the elements a `claim` has leased and an `ack` has not yet removed; it drops them when they are acked or dead-lettered. A put that replaces a key subtracts the old element and adds the new one. `min` and `max` are not kept: a `btreeStore` keyed by the value answers first and last for free. An average is the sum divided by `count()`.

An aggregate read works from any node: a node that does not hold the document asks the node that does. An aggregate is declared when the bucket is created.

## Counters by key: inc

A `hashStore` or `btreeStore` can hold a counter for each key, such as hits per hour for an app or mentions per channel for a user. `inc` adds to the numbers of the element under a key, and creates the element when the key is not there yet, in one step at the node that owns the document, so two callers adding at once never lose an add:

```
create bucket App { Name string, Hits hashStore, Daily btreeStore }
db.<bucket>["<id>"].<store>.inc("<key>", {<path>: <number>, …})
db.<bucket>["<id>"].<store>.inc("<key>", {<path>: <number>, …}, {return: "after"})
```

For the App bucket above and its document `a1`, `inc` on the `Hits` store with the key `2026-11-05T14` and the amounts `t: 1, s: 30` creates the hour `{t: 1, s: 30}`. The same call again with `t: 1, s: 12` leaves `{t: 2, s: 42}`, and adding `{return: "after"}` prints the element as it then stands. A dotted path such as `"paid.cents"` makes the document inside. `help inc` prints a statement you can run as it stands.

- Each path is a field name, or names joined by dots, up to three levels deep. Each amount is an int or a double (a Go client sends an `int`, `int32`, `int64` or `float64`). An absent path starts from zero. Two ints stay an int and widen to a long when the sum leaves an int; a double anywhere makes the result a double.
- A counter may go negative: `inc` has no floor and no ceiling.
- A retry of an `inc` with the same request id is safe only while the server still remembers that id, the last `replication.dedupe_window` writes of the pair (8192 by default). After that the retry is a new `inc` and adds again, so read the element before you resend an old request.
- A path that holds a string, an int that would pass the largest long, and a decimal are refused before anything is written, naming the path. Keep money in integer minor units (cents).
- A btree key is the string you give, so a time written the same width each time (`2026-11-05T14`) keeps the counters in time order and `list` reads them in that order.
- If the store keeps [sums and counts](#sums-and-counts-a-store-keeps-for-you), they follow the new numbers in the same write. An `inc` that would create an element with no number at a summed path is refused, naming the path.
- `inc` is for the keyed stores only. A `fifoStore`, `lifoStore` or `heapStore` element has no key to name; to add to a number inside a document, use `update` with `$inc`.

## FIFO (First In, First Out)

A queue. Elements are pushed at the tail and popped from the head.

### When to use

- Message queues
- Event streams and log ingestion
- Comment threads (chronological)
- Time-series data
- Job queues (first in, first out)

### Operations

```text
create bucket Post { Caption string, Comments fifoStore }
db.Post.insert({_id_: "p1", Caption: "Sunset"})
db.Post["p1"].Comments.push({Author: "Bob", Text: "Nice photo!"})
db.Post["p1"].Comments.push({Author: "Eve", Text: "Lovely light"})
db.Post["p1"].Comments.push({Author: "Sam", Text: "Where was this?"})
db.Post["p1"].Comments.push({Author: "Ann", Text: "Thanks for sharing"})
db.Post["p1"].Comments.peek()
db.Post["p1"].Comments.pop()
db.Post["p1"].Comments.pop(3)
db.Post["p1"].Comments.count()
```

A store is written through a document that exists, so insert it first. `peek` reads the oldest element and leaves it in place; `pop` removes and returns it,
and `pop(3)` removes the next three, oldest first, in one write. `peek` and `pop`
take a count of 1 to 100. A `pop` of several must fit about
1 KiB together, or it is refused with the count that fits. A pop's answer is the
only copy of what it removed: if your program can lose the answer, pop fewer at a
time, or use `claim`.

### Leases

`pop` removes an element before you have done the work. To consume a queue
without losing work, `claim` leases the next element (or the next n) to one
worker and leaves it in the store; `ack` removes it for good once the work is
done; `release` gives it back at once; `extend` moves the lease's deadline. An
element whose lease ends before it is acked comes back to the queue, so a lost
answer is a redelivery and not a loss.

```text
db.Post["p1"].Comments.claim(1)
db.Post["p1"].Comments.ack("<lease>")
```

`claim` prints the lease id on its first line. There is no list of elements that
keep failing: an element that is never acked is handed out again each time its
lease ends ([Limitations](limitations.md#not-available-yet)).

---

## Documents and queue elements that expire

A bucket can say when its documents leave, and a `fifoStore` or `lifoStore` field can say when its elements do. You name the field that holds the time; hoardDB never invents one and never guesses a duration.

```
create bucket Session { UserId objectid, ExpiresAt datetime, expire at ExpiresAt }
create bucket Device  { Name string, Log fifoStore expire at "ts" }
alter  bucket Session { set expire at ExpiresAt }
alter  bucket Session { drop expire at }
```

- **The field holds the expiry instant, not the creation time.** A document whose field is at or before now is already gone: `get`, `list` and `count` skip it at once, and the owner removes it soon after. `expire at CreatedAt` with a field that defaults to now removes the document the moment it is written.
- **A document without the field never expires**, so one bucket can hold permanent and expiring documents.
- **A queue element needs a datetime at its path** (one to three dot-separated keys, such as `"ts"` or `"meta.at"`). A push without one is refused and nothing is written.
- `pop`, `peek`, `claim`, `list` and `count` on an expiring queue skip expired elements. A pop or claim removes at most 64 expired elements per call; the retention tick removes the rest, and a read never shows an expired element either way.
- A pop or claim that finds more than 64 expired elements ahead of the live ones may answer empty while live elements are queued: ask again, the retention tick clears the backlog. `count` on an expiring queue reads every element, so it costs as much as the queue is long.
- **Nodes compare against their own clocks**, so a document can become invisible a few milliseconds apart on two nodes; the removal itself does not depend on any clock.
- There is no duration form (`expire after 30d`): write the time yourself, for example now plus 30 minutes in your program, and write it into the field. A read of an expired document answers `NOT_FOUND`.
- `describe bucket` shows the `Expires:` block, and `help expire` repeats this page.

---

## LIFO (Last In, First Out)

A stack. Elements are pushed and popped from the same end, so the newest comes
out first. It takes the same verbs as a FIFO, including `claim` and `ack`.

### When to use

- Undo stacks
- Recent items (newest first)
- Browser history
- Photo feeds (newest first)

### Operations

```text
create bucket User { Name string, Photos lifoStore }
db.User.insert({_id_: "ada", Name: "Ada"})
db.User["ada"].Photos.push({File: "beach.jpg"})
db.User["ada"].Photos.push({File: "sunset.jpg"})
db.User["ada"].Photos.peek()
db.User["ada"].Photos.pop()
```

`peek` shows the newest element and leaves it in place; `pop` removes and returns it.

| Operation | FIFO | LIFO |
|-----------|------|------|
| `push` | adds at the tail | adds at the tail (identical) |
| `pop`, `peek` | the oldest element | the newest element |

---

## Hash (key-value)

A map. Each element sits under a key you choose.

### When to use

- Profiles and settings
- Session data
- Any exact key lookup
- Metadata

### Operations

```text
create bucket User { Name string, Friends hashStore }
db.User.insert({_id_: "ada", Name: "Ada"})
db.User["ada"].Friends.put("grace", {since: 2024})
db.User["ada"].Friends.get("grace")
db.User["ada"].Friends.take("grace")
db.User["ada"].Friends.list({limit: 10})
db.User["ada"].Friends.count()
```

`put` replaces any element already under the key, and `take` removes the element and returns it. `list` prints each line with its key first, and ends a page that is not the last
with a `more:` line: the same statement with the cursor filled in.
[Counters by key](#counters-by-key-inc) adds to the numbers of an element in one write.

---

## B+Tree (sorted key-value)

A map whose elements read back in key order.

### When to use

- Leaderboards and rankings
- Alphabetical or numerical listings
- Time series keyed by time
- Any data you read in order

### Operations

```text
create bucket Player { Name string, Scores btreeStore }
db.Player.insert({_id_: "ada", Name: "Ada"})
db.Player["ada"].Scores.put("0100", {Game: "chess"})
db.Player["ada"].Scores.put("0085", {Game: "go"})
db.Player["ada"].Scores.put("0092", {Game: "draughts"})
db.Player["ada"].Scores.list()
db.Player["ada"].Scores.get("0092")
```

`list` reads the elements in key order. A key is the string you give, even one that looks like a number, so `10` sorts
before `2`. Write numeric keys with leading zeros to one width (`0002`, `0010`),
and a time as `2026-11-05T14`, to get numeric or time order.

---

## Heap (priority queue)

A priority queue: the highest-priority element is the next one out.

A `heapStore` field can be declared and read, and the Go client adds to it:
`users.PushHeap(ctx, "ada", "Books", "dune", 9, element)` pushes `element` under entry
id `dune` with priority 9, alone or queued on a batch. An element is identified by its priority and entry id
together: pushing the same pair again overwrites it, and pushing the same entry id at
another priority adds a second element, which `get` by entry id answers with the higher
priority first. The element and its arguments may be 1 MiB at most. The CLI has no `push`
statement for a heap, so from a session the store stays empty until a program fills
it. It answers `peek`, `pop`, `list`, `count` and `get`
([Limitations](limitations.md#not-available-yet)).

---

## Blob (large values and files)

A `blobStore` field is for bytes hoardDB owns. It can be declared and it answers
`count`; writing to it is not available yet, and the refusal says so. To keep files
today, store a pointer document and leave the bytes on storage you already run:
[Large file stores](large-files.md).

---

## Where the data lives

Every bucket, of every store type, and every store field on every document, is a
range of keys in the one store of the node that holds it. A bucket has no directory,
no files and no process of its own, so creating another costs its catalog entry and
the data you put in it, and no bucket count is refused: a node's memory and disk
are the bound. [Limits](limits.md) says more.

```text
<data_dir>/
└── ring/     # every database, bucket, document and store field on this node
```

---

## Levels and decision rules

A bucket declares its fields, and a field can itself be a store of any of the six
types, **on each document**. Choosing a store type is therefore two choices: which
fields the bucket has, and which stores each of its documents carries.

### Levels

- **L1**: a bucket. Like a SQL table or a document-database collection. Its documents
  have an `_id_` of their own, so you can look any of them up directly.
- **L2**: a store field of one document. `Comments fifoStore` on `Post` gives every
  post its own comments queue, and `user.Friends` is that user's own hash. It is
  addressed through the document, `db.Post["p1"].Comments`, and it is deleted with
  the document.

### Decision rules

| Data | Where it goes | Reason |
|------|---------------|--------|
| User's first name | a field of the `User` document | Fixed size, read with the document |
| User's friends | `Friends hashStore` on `User` | Belongs to one user, looked up by friend id |
| User's posts | **a `Post` bucket** | A post has an identity of its own: you list and look up posts across users |
| Post's comments | `Comments fifoStore` on `Post` | Belongs to one post, read in order |
| Post's photos | `Photos blobStore` on `Post` | Belongs to one post |
| Post's caption | a field of the `Post` document | Fixed size, read with the document |

### The critical rule

**If you will look a thing up or list it on its own, across its owners, it is a
bucket. If it only ever exists as part of one document, it is a store field of that
document.**

If you have a dump of your data, [`hoarddb plan`](plan.md) applies this rule to it and
shows you the result.

A store field is reached through its document's id, so `list({filter: …})` does not
look inside it. The comments under one post belong in that post. Many small stores
are what the design is for: one queue per post, one hash per user.

```text
L1  User                          <- bucket
    └── L2  Friends (hashStore)   <- one per user: db.User["jane"].Friends

L1  Post                          <- its own bucket, one document per post
    ├── L2  Comments (fifoStore)  <- one per post: db.Post["p1"].Comments
    └── L2  Photos (blobStore)    <- one per post: db.Post["p1"].Photos
```

### Example: PhotoJournal schema

Two entities, `User` and `Post`, each a bucket, and the parts that belong to one
document are store fields of it:

```text
create database PhotoJournal
use PhotoJournal
create bucket User { FirstName string, Email string, Friends hashStore }
create bucket Post { Author string, Caption string, Photos blobStore, Comments fifoStore }
```

Each user has a `Friends` hash of their own, and each post has its own `Comments`:

```text
[h]oardDB [PhotoJournal]> db.User.insert({_id_: "jane", FirstName: "Jane", Email: "jane@example.com"})
OK  id=jane
[h]oardDB [PhotoJournal]> db.User.insert({_id_: "sam", FirstName: "Sam", Email: "sam@example.com"})
OK  id=sam
[h]oardDB [PhotoJournal]> db.User["jane"].Friends.put("sam", {since: 2021})
OK  put "sam": Friends now holds 1 element
[h]oardDB [PhotoJournal]> db.User["jane"].Friends.get("sam")
{"since": 2021}
[h]oardDB [PhotoJournal]> db.Post.insert({_id_: "p1", Author: "jane", Caption: "Sunset over the hills"})
OK  id=p1
[h]oardDB [PhotoJournal]> db.Post["p1"].Comments.push({Author: "sam", Text: "Lovely light"})
OK  pushed: Comments now holds 1 element
[h]oardDB [PhotoJournal]> db.Post["p1"].Comments.push({Author: "jane", Text: "Thanks"})
OK  pushed: Comments now holds 2 elements
[h]oardDB [PhotoJournal]> db.Post["p1"].Comments.pop()
{"Author": "sam", "Text": "Lovely light"}
[h]oardDB [PhotoJournal]> db.Post["p1"].Comments.count()
1
```

`describe bucket Post` shows the fields in the order you declared them:

```text
[h]oardDB [PhotoJournal]> describe bucket Post
BUCKET PhotoJournal.Post
---
Id:                  0000000000000007
State:               active
Declaration version: 1
Fields:              (in declaration order)
  Author       string
  Caption      string
  Photos       blobStore
  Comments     fifoStore
```

`Photos blobStore` is declared the same way as the other stores, but it cannot be
written to yet, so a post that needs a photo keeps its path or key in a field.

**Referential design:** `Post` is its own bucket, so a post points at its author with
the author's user id, and the application resolves it with a `get` on `User`. A
document's queue or table is declared on it, not kept in a separate bucket that points
back. Keep a separate bucket only for something that is its own entity, such as `Post`,
or a dedicated queue of alerts that you read across all users.
