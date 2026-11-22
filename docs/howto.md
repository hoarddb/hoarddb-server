# How-to: from your schema to hoardDB

These pages take an application you already know how to build, show the three
layouts it usually gets, and then show the hoardDB layout and the reason for each
choice. Each ends with a seeded data set you can load in a minute and the
statements that walk its reads, so nothing on these pages is only described.

## The method, in five steps

1. **Find the entities.** The things the data is about: a user, a room, an
   account, a session. Each becomes a bucket, and each document in it is one
   entity.
2. **List the reads.** Not the tables: the questions the application asks on its
   hot path. "The last twenty messages of this room." "This user's rooms."
   "The sum of this account's expenses this month."
3. **Give each read a store on the entity it belongs to.** Newest first is a
   LIFO. Oldest first, or a queue of work, is a FIFO. Looked up by key is a hash.
   Ordered by key is a btree. Highest priority first is a heap. Bytes are a
   blob. The store lives on the document whose read it serves.
4. **Decide what crosses an entity.** A read that starts from one entity and
   needs another ("the names of this room's members") is a keyed read per id.
   A read that must find entities by a field ("every room this user is in") is
   either a store the application writes on both sides, or a small bucket whose
   documents are the relationship itself, listed with a filter.
5. **Say what the layout does not do.** Every page here has that section. A
   layout that cannot answer an arbitrary filter across a whole kind is not a
   failure of the layout; it is the trade that buys one owner per entity.

The rule behind all five: data lives with what it describes. A user's feed is
that user's own queue, not a row in a table of everyone's feeds. You choose the
structure; hoardDB never chooses it for you.

## The worked examples

| Page | Read it when |
| --- | --- |
| [Agent memory](howto-agent-memory.md) | Your application keeps sessions, scratch state, task queues and per-user memories for an agent or an assistant |
| [Chat rooms](howto-chat.md) | Your application has rooms, members on both sides, and a newest-first message list per room |
| [A ledger](howto-ledger.md) | Your application keeps accounts with expenses and invoices under them, and reads sums |
| [A social network](howto-social.md) | Your application has users with feeds, followers on both sides, posts with comments and likes, and fan-out |
| [A video library](howto-video.md) | Your application streams files from storage you already run and keeps the owner, the path, the counters, the comments and each viewer's progress in the database |

Each page follows the same outline: the application and its reads; the three
layouts it usually gets, described by kind; the hoardDB layout and why; what
changes in the application; the cost and the limits; and a seeded walkthrough.

## Loading the example data

Every page's walkthrough uses the same seed tool, which loads one layout into a
running server with a fixed seed, so what you see matches the page. From the
repository's `github.com/hoarddb/hoarddb-server/` directory, with a server listening on `127.0.0.1:7433`
and a root password in `HOARDDB_ROOT_PASSWORD`:

```bash
go run ./bench/scenarios -engine hoarddb -layout agent -addr 127.0.0.1:7433 -insecure
```

`-layout` takes `social`, `chat`, `ledger`, `agent`, `video` or `all`; each layout goes
into its own database, named `scenarios_<layout>`. The tool refuses a database
that already exists and says how to start over. `-insecure` is for a server you
started yourself on this machine; otherwise pass `-fingerprint` with the value the
server printed at start.
