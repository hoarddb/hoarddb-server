# Limits

Every size and count that hoardDB enforces, with its default, how to change it
and what you see when you reach it. This page is the numbers;
[Limitations](limitations.md) is the list of things hoardDB does not do, and
explains the reasoning behind several of these.

A limit that an operator can change is a setting of **one node**; keep these
settings the same on every node of a cluster.

## How big can one thing be

These are the sizes and counts a node enforces on a write or a read.

| What | Limit | You can set | When you reach it |
| --- | --- | --- | --- |
| One protocol frame (one request, with its key, bucket name and envelope) | 32 MiB (33,554,432 bytes) | fixed | The client library refuses the request before sending it, with an error that names the 32 MiB limit and says to split it, and the connection stays usable. A client that sends one anyway gets an `INVALID_ARGUMENT` reply that names the limit |
| One request sent before the connection has logged in | 64 KiB (65,536 bytes) | fixed | The server replies `INVALID_ARGUMENT`, naming the limit and saying to send AUTH first and wait for `AUTH_OK`, and closes the connection |
| What an update operator may leave in one field (`push`, `addToSet`) | 16 MiB | fixed | The update is refused with a message that names the field and the limit, and nothing is changed. Keep the array shorter, or store the elements in a store field |
| One batch | 255 writes | fixed | The batch is refused with `INVALID_ARGUMENT` and nothing in it is written. Split it into batches of 255 or fewer |
| The answers of one batch | 16 MiB kept whole | fixed | Answers past the budget are not kept whole; the error answers are always kept |
| One page of `list` | 100 documents unless you ask for another number, at most 10,000, and at most 16 MiB of documents | the `limit` option, up to 10,000 | The page ends early, and the cursor it returns continues from there |
| One statement typed in the CLI | 64 KiB per line | fixed | `CLI error: read input: bufio.Scanner: token too long` |
| A database name, or a bucket name | 255 bytes each | fixed | The create is refused before anything is written, with an error that names the limit |
| A bucket's size, or the documents in it | no limit | | The disk is the bound |

A ring node does not read `storage.max_document_bytes` (`HOARDDB_MAX_DOCUMENT_BYTES`)
or `storage.max_blob_bytes` (`HOARDDB_MAX_BLOB_BYTES`). Setting them changes
nothing. A document has no limit of its own: it must fit in one frame,
so a document and everything else in its request together stay under 32 MiB.
A blob store cannot be written yet ([Limitations](limitations.md)).

**Why a name has a limit.** A node keeps one record per bucket, under a key made
of the database name and the bucket name, and its store refuses a key over 65,000
bytes. 255 bytes each, counted in UTF-8 bytes (so fewer characters if a name uses
non-Latin letters), is the limit you already know from file names. If you want to
keep a long identifier, store it as a field of a document and name the bucket by a
short id.

**What to do instead of raising a limit.**

- A list that keeps growing belongs in a nested store field, whose elements are
  stored separately and do not count towards the parent document.
- A file that is large belongs on storage you run, with a pointer document in
  hoardDB. [Large file stores](large-files.md) is the pattern.

## How many buckets

A bucket is a key range in the one store a node keeps for all its buckets. It
costs no file, no file descriptor, no memory mapping and no goroutine of its own,
and it adds one small entry to the node's in-memory catalog. Adding a bucket is
adding keys, not opening anything.

**hoardDB sets no limit on the number of buckets.** It keeps data with what it
describes, so a database is made of many small buckets, and no count is refused.
What bounds a node is its memory and its disk: every bucket adds its catalog entry
and the data you put in it, so the number a node holds depends on your hardware and
on what the buckets contain. Try the count you need on your own hardware before you
rely on it.

To see the buckets a database has, run `show buckets in <db>` or
`describe database <db>`. hoardDB keeps no metric per bucket: the metrics are
totals for the node, and per-bucket detail is a command you run
(`describe bucket`).

## The memtable

The memtable is the write buffer in front of the node's one store. It is a
per-node setting: every bucket shares it, so it does not grow with the number of
buckets.

**A new node picks its size once**, from the memory it is allowed (its container
or cgroup limit, or the machine's RAM), and keeps it on every restart:

| Memory limit | Memtable chosen | Largest atomic batch (15%) | Memory a heavy write burst can reach |
| --- | --- | --- | --- |
| below 3.5 GiB | 32 MiB | about 5 MB (5,033,164 bytes) | about 460 MB |
| 3.5 GiB to below 7 GiB | 64 MiB | about 10 MB (10,066,329 bytes) | about 725 MB |
| 7 GiB and above | 128 MiB | about 20 MB (20,132,659 bytes) | about 1.25 GB |

The automatic choice never goes above 128 MiB. The last column is a prediction for
one client loading in batches, about 200 MB plus 8.2 MB for every MiB of
memtable; many concurrent writers need more.

**To choose it yourself**, set `storage.memtable_mb` (`HOARDDB_MEMTABLE_MB`) to
a value from 32 to 1024 MiB. Sizes of 256 MiB and above are only ever set this
way. A value outside the range stops the server at start:

```text
storage.memtable_mb is 8, below the minimum of 32. The store's largest atomic
batch is 15% of the memtable, and under 32 MiB it drops below 5 MB, which the
ingest batch budget and batch splitting assume. Set storage.memtable_mb (or
HOARDDB_MEMTABLE_MB) to a value from 32 to 1024, or remove it to run at the size
this node's store chose at its first start
```

A bigger memtable stalls writes less and holds up to three of them in memory,
each needing about 1.3 times its size, so 1024 MiB is already about 3.9 GB. It is
fixed at start. A node that crashed at a larger size cannot be opened at a smaller
one until it has been started once at the old size and stopped cleanly.
[Configuration](configuration.md#storage-memtable) shows how to read the size
a node chose, and why.

## Connections and requests

| What | Default | Range | Setting |
| --- | --- | --- | --- |
| Concurrent client connections per node | 1,000 | | `HOARDDB_MAX_CONNS` |
| Concurrent connections from one address, counted from the moment a connection is accepted, before TLS | 64 | `0` turns the cap off | `HOARDDB_TCP_MAX_CONNS_PER_IP` |
| Connections kept for other nodes | 64 | | `HOARDDB_MAX_PEER_CONNS` |
| Idle time before a connection is closed, counted from its last answer with nothing in flight | 300 seconds | | `HOARDDB_TCP_IDLE_TIMEOUT` |
| Time to read one frame | 30 seconds | | `HOARDDB_TCP_FRAME_TIMEOUT` |
| Requests a client may have in flight on one connection | 4 | 1 to 64 | `HOARDDB_TCP_PIPELINE_FRAMES` |
| Bytes of those requests | 1 MiB | 1 MiB to 16 MiB (a smaller value is raised to 1 MiB, with a warning) | `HOARDDB_TCP_PIPELINE_BYTES` |
| Queries running at once | twice the CPU count | 4 to 256 | `HOARDDB_QUERY_CONCURRENCY` |
| Failed logins in a row before an address is refused; also how many logins from one address are checked at once | 20 | `0` turns the limit off | `HOARDDB_TCP_AUTH_MAX_FAILS` |
| Logins from one address waiting for those being checked | four times `HOARDDB_TCP_AUTH_MAX_FAILS` (80), each for up to 5 seconds | fixed | follows `HOARDDB_TCP_AUTH_MAX_FAILS` |
| Shortest user password, checked by the CLI on `create user` and `alter user ... password` | 12 characters | 1 to 1024 | `HOARDDB_PASSWORD_MIN_LENGTH` |

The client budget (`HOARDDB_MAX_CONNS`) and the node-to-node budget are
separate, so a flood of clients cannot starve replication. A connection over a
cap is answered with `OVERLOADED`, naming the cap and its setting, and closed.
While 64 such refusals are already in progress on a port, a further refused
connection is closed without the message: retry after a short wait. A connection carries
one request at a time unless the client reads ahead within the window above;
a client that wants concurrency opens another connection. A query past the
concurrency limit waits; it is not refused.

A login still being checked holds one of the `HOARDDB_TCP_AUTH_MAX_FAILS` places
for its address until its password is checked, because it may yet fail. A login
that finds every place held this way waits for one to come free and is then let
in, so a burst of correct logins larger than the limit (an application starting
a big connection pool, a load generator) succeeds, a little slower. A login that
waits more than 5 seconds, or arrives while 80 others from its address already
wait, is answered `RATE_LIMITED` with "too many logins from this address are
being checked at once", and its connection is closed. Its password was not
checked and it does not count as a failed login: reconnect and retry, or raise
`HOARDDB_TCP_AUTH_MAX_FAILS`. Raising it also lets more wrong passwords from one
address be tried before the address is refused.

A value outside a range with a stated clamp is brought to the nearest end of it
when the server loads its configuration. The sizes in the first table are the
other kind: outside their range, the server refuses to start.

## Checking a limit against your data

Before you rely on a limit, try it at its edge on your own data. The sizes and
counts above are what the build enforces, and they were read from its
configuration, not measured on your workload: your hardware, your kernel's
file-descriptor and memory-mapping limits, and the shape of your documents decide
where a node becomes slow before any of them is reached.
