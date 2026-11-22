# scenarios: one data set, six engines, five layouts

`scenarios` loads the same deterministic data set into hoardDB, MySQL, PostgreSQL, MongoDB, Redis or Kvrocks for one of the five
application layouts of the private test-scenarios specification (section 2), so a scenario or a
benchmark starts from identical data on every engine. The text comes from one seeded generator
(`model/`), so it is byte-identical on all six (performance-certification specification 3.1); the
schema on each engine is the one a competent user of that product would choose (6.2), with the
choice justified in a comment above every table (`-ddl` prints them).

## What it loads (scale 1)

`-scale N` multiplies every count. A follow graph keeps its shape at every scale (busiest user 8
followers, median 2).

| layout | documents | store elements |
| --- | --- | --- |
| `social` | 40 `User`, 1600 `Post` (40 per author), 10 `Group`, 74 `Follow` | `User`: Feed 1600 (40 each), Inbox 2960 (at least 40 each), Notifications 2412, Photos 80, Friends 120, Following 74, Followers 74, Likes 2357, Settings 120, Drafts 0; `Post`: Comments 2412, Likes 2357; `Group`: Members 40, Wall 50 |
| `chat` | 145 `Room`, 40 `User`, 803 `Member` (3 to 8 per room, every user in at least 20 rooms) | `Room`: Messages 14500 (100 per room), Members 803; `User`: Rooms 803 |
| `ledger` | 30 `Account` | `Account`: Expenses 3000 (100 each), Invoices 1200 (40 each) |
| `agent` | 40 `User`, 60 `Session`, 60 `Run` | `Session`: Messages 2400 (40 each); `Run`: Scratch 360 (6 each), Tasks 600 (10 each); `User`: Memories 8000 (200 each) |
| `video` | 20 `Channel`, 40 `Viewer`, 100 `Video` (5 uploads per channel) | `Video`: Comments 800 (8 each), Likes 600 (6 each); `Channel`: Uploads 100, Subscribers 160; `Viewer`: Subscriptions 160 (4 each, the other side of Subscribers), Progress 400 (10 each), History 800 (20 each), Playlists 400 (2 lists of 5 each) |

Each layout goes into a database of its own, `<database>_<layout>` (`scenarios_social` with the
defaults), because three layouts declare a `User` bucket with different fields. A memory is a text and a
64-value float32 little-endian vector: BSON binary subtype 0 on hoardDB and MongoDB, a 256-byte BLOB
on MySQL, a BYTEA on PostgreSQL, the raw 256-byte string in a hash field on Redis and Kvrocks.

A `Video` stores no bytes, only `Path` (for example `library/2026/11/v017.mp4`), the place they live on
external storage, and `Views`, the hot counter, a plain number between 0 and 100000. A subscription is
written on both sides with the same time (`Channel.Subscribers` and `Viewer.Subscriptions`). A playlist
element key is `<playlist>/<position>`, the position zero-padded to four digits (`watch-later/0003`), so
the btreeStore walks a list in order.

Two things the layouts document lists are not seeded, and the command says so rather than
pretending: the `Drafts` heapStore of `User` is declared and left empty, because `ringclient` has
no push that takes a priority. The fan-out inboxes are not capped: every user follows at least one author, and every author writes 40 posts, so every inbox holds at least 40 entries (a page of 20 and a page offset of 20 still fit).

## The five schemas, side by side (chat)

Redis and Kvrocks share one scheme, so five texts cover six engines. hoardDB declares the stores
inside the entity; a join is written to `Room.Members`, `User.Rooms` and `Member`:

    create database scenarios_chat { replication_factor: 1 };
    create bucket Room in scenarios_chat { Name string, Messages fifoStore, Members hashStore };
    create bucket User in scenarios_chat { Name string, Rooms hashStore };
    create bucket Member in scenarios_chat { Room string, User string, Since int };

MySQL has no store, so each store is a child table keyed by its owner, and the two membership
stores plus the `Member` bucket are one `members` table with a key on each side. No foreign keys;
counters (`member_n`, `message_n`) are denormalised because a room list shows them:

    CREATE TABLE members (
      room_id VARCHAR(64) NOT NULL, user_id VARCHAR(64) NOT NULL, since BIGINT NOT NULL,
      PRIMARY KEY (room_id, user_id),
      KEY members_by_user (user_id, room_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    CREATE TABLE messages (
      room_id VARCHAR(64) NOT NULL, seq BIGINT NOT NULL, author VARCHAR(64) NOT NULL,
      text TEXT NOT NULL, at BIGINT NOT NULL,
      PRIMARY KEY (room_id, seq)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

PostgreSQL takes the same tables with its own types (`TEXT` for every string, `SMALLINT` for the
one-byte queue state, `BYTEA` for the vector) and the secondary keys as separate `CREATE INDEX`
statements. The agent layout's `memories.vec` is a plain `BYTEA`; the DDL comments where a pgvector
`vector(64)` column would go, and the extension is not required:

    CREATE TABLE members (
      room_id TEXT NOT NULL, user_id TEXT NOT NULL, since BIGINT NOT NULL,
      PRIMARY KEY (room_id, user_id)
    );
    CREATE INDEX members_by_user ON members (user_id, room_id);
    CREATE TABLE messages (
      room_id TEXT NOT NULL, seq BIGINT NOT NULL, author TEXT NOT NULL,
      text TEXT NOT NULL, at BIGINT NOT NULL,
      PRIMARY KEY (room_id, seq)
    );

MongoDB takes the same collections and never an array inside the owner's document (an unbounded array
passes the 16 MB limit and rewrites the document on every push):

    db.createCollection("members");
    db.members.createIndex({ room_id: 1, user_id: 1 }, { unique: true });
    db.members.createIndex({ user_id: 1, room_id: 1 });
    db.createCollection("messages");
    db.messages.createIndex({ room_id: 1, seq: 1 }, { unique: true });

Redis has no DDL, so its schema is the key families, printed with the read each serves. Each layout is a
key prefix (`scenarios_chat`) inside one logical database; a document is a hash, a `fifoStore` is a list
(`RPUSH`; a `lifoStore` is `LPUSH`), a `hashStore` is a hash, a `btreeStore` is a sorted set of score 0
with a second hash for the bodies, a `heapStore` is a sorted set by priority, and each side of a
membership pair is a set:

    scenarios_chat:Room:<id>             hash of the scalar fields and the counters
    scenarios_chat:Room:<id>:Messages    list, RPUSH appends a JSON body
    scenarios_chat:Room:<id>:Members     set of user ids   (the pair's other side:)
    scenarios_chat:User:<id>:Rooms       set of room ids

Kvrocks is that scheme on RocksDB, unchanged.

Print all of it, for any layout, with no server running:

    go run ./bench/scenarios -ddl -layout chat

The paired stores a SQL or document schema holds once are `User.Following` and `User.Followers`
(table `follows`), `Post.Likes` and `User.Likes` (table `likes`), `Room.Members` and `User.Rooms`
(table `members`), `Channel.Subscribers` and `Viewer.Subscriptions` (table `subscriptions`). That is why MySQL and MongoDB write fewer rows than hoardDB writes documents and
elements for `social` and `chat` (2111 against 2581, and 574 against 802) and for `video` (3420 against 3580); `ledger` and `agent` write
the same number on all of them (1530 and 3220). Redis and Kvrocks keep both sides of a pair as sets, so
they write what hoardDB writes (2581, 802 and 3580), with no `Member`, `follows` or `subscriptions` row to hold the time of a join.

## Run it (from `github.com/hoarddb/hoarddb-server/`)

hoardDB. Start a server on a scratch directory (it prints its address and certificate fingerprint),
then seed. `-insecure` is for a server you started yourself on this machine; otherwise pass
`-fingerprint`:

    HOARDDB_DATA_DIR=<scratch dir> HOARDDB_ROOT_USER=admin HOARDDB_ROOT_PASSWORD=<12+ characters> hoarddb-server
    HOARDDB_ROOT_PASSWORD=<same password> go run ./bench/scenarios -engine hoarddb -addr <host:port> -insecure -layout social

MySQL. The DSN names the server; any database in it is ignored and the layout's database is created.
TLS is on with the certificate unchecked (`tls=skip-verify`) unless the DSN names a `tls=` value or
`-mysql-tls=false`:

    go run ./bench/scenarios -engine mysql -mysql-dsn 'user:password@tcp(<host>:3306)/' -layout social

MongoDB. TLS is on with the certificate unchecked unless `-mongo-tls=false`; writes use write concern
`{w:1, j:true}`:

    go run ./bench/scenarios -engine mongo -mongo-uri 'mongodb://<host>:27017' -layout social

PostgreSQL. The DSN is a URL or a key=value string; its database is only the one the connection starts in
(`CREATE DATABASE` needs one). TLS is on with the certificate unchecked (`sslmode=require`) unless the DSN
names a verifying `sslmode` or `-postgres-tls=false`:

    go run ./bench/scenarios -engine postgres -postgres-dsn 'postgres://user:password@<host>:5432/postgres' -layout social

Redis. Each layout is a key prefix, `<database>_<layout>`, in the logical database the URL names. TLS is on
with the certificate unchecked unless the URL says `rediss://` (then the certificate is verified) or
`-redis-tls=false`:

    go run ./bench/scenarios -engine redis -redis-url 'redis://<host>:6379/0' -redis-tls=false -layout social

Kvrocks. The Redis seeder against Kvrocks's own port (6666 by default); `-redis-tls` applies:

    go run ./bench/scenarios -engine kvrocks -kvrocks-url 'redis://<host>:6666/0' -redis-tls=false -layout social

To run MongoDB locally, use the `mongo:8.2` image. On a host with a Linux kernel at 6.19 or newer the
MongoDB 8.0 image refuses to start by its own check (verified on this host on 2026-11-06); the
certification VMs run an older kernel and keep the 8.0 pin.

Flags: `-engine hoarddb|mysql|postgres|mongo|redis|kvrocks`, `-layout social|chat|ledger|agent|video|all` (default `all`), `-scale`
(1), `-seed` (1), `-database` (`scenarios`), `-workers` (16), `-connections` (4, hoardDB only), `-rf`
(1, hoardDB only), `-ddl`, `-mysql-dsn`/`-mysql-tls`, `-postgres-dsn`/`-postgres-tls`, `-mongo-uri`/`-mongo-tls`,
`-redis-url`/`-redis-tls`, `-kvrocks-url`. hoardDB reads `-addr` from `HOARDDB_ADDR`, `-user` from `HOARDDB_ROOT_USER`
(default `admin`) and `-password` from `HOARDDB_ROOT_PASSWORD`, as `ringseed` does.

After the load every engine counts what it holds and refuses to report success on a mismatch:
`Count` per bucket and `StoreCount` per store of every document on hoardDB, `COUNT(*)` per table on
MySQL and PostgreSQL, `CountDocuments` per collection on MongoDB, and on Redis and Kvrocks a `SCAN` for
the document hashes of each bucket plus `HLEN`, `LLEN`, `ZCARD` or `SCARD` per store of every document.

## The JSON line

One line per layout on stdout when its load and count check are done (progress goes to stderr):

    {"engine":"mysql","layout":"chat","database":"scenarios_chat","scale":1,"seed":1,"counts":{"Member":114,"Room":20,"Room.Members":114,"Room.Messages":400,"User":40,"User.Rooms":114},"writes":574,"seed_s":0.4}

`counts` is what the generator produced, by bucket and by `Bucket.Store`, the same on every engine.
`writes` is what this engine wrote: documents plus elements on hoardDB, rows or documents on MySQL and
MongoDB (smaller where stores are paired, see above). `seed_s` is the wall time of that layout,
schema creation and count check included.

Seeding is **not** resumable. Running it again against a database that exists (on Redis and Kvrocks, a
key prefix that holds any key, found by `SCAN`) is refused, with the
way out in the message: seed a fresh database with `-database <name>`, or drop the database (or
start hoardDB on an empty data directory). A failed seed leaves a partly filled database behind;
drop it before trying again.

## Tests

`go test ./bench/scenarios/...` needs no server for the generator (golden fingerprint, determinism,
shape), the schema text, the flag refusals and the row mapping. One hoardDB end-to-end test starts a
server in the process and seeds every layout. `TestSeedMySQL`, `TestSeedPostgres`, `TestSeedMongo`,
`TestSeedRedis` and `TestSeedKvrocks` skip unless `MYSQL_DSN` (for example `user:password@tcp(<host>:3306)/`),
`POSTGRES_DSN` (for example `postgres://user:password@<host>:5432/postgres?sslmode=disable`), `MONGO_URI`,
`REDIS_URL` (`redis://<host>:6379/0`) and `KVROCKS_URL` (`redis://<host>:6666/0`) are set; they create
databases or key prefixes with a time-based name and drop them afterwards. The command plan of the Redis
seeder is tested without a server.

If the generator changes on purpose, the golden fingerprints in `model/model_test.go` change with it,
and every data set seeded before is no longer the one a new run compares against.

## On the certification VMs

`scripts/certification/scenarios.sh` wraps this tool the way `social.sh` wraps `bench/social`: it finds
the engine's address from the inventory (hoardDB's client port, or `engines.sh uri` for MongoDB and
MySQL; for PostgreSQL, Redis and Kvrocks, which `engines.sh` does not manage, the environment variables
`CERT_POSTGRES_DSN`, `CERT_REDIS_URL` and `CERT_KVROCKS_URL`), runs one seed per layout, and prints `CHECK` and `RECORD` lines.

    scripts/certification/scenarios.sh ddl chat
    scripts/certification/scenarios.sh seed hoarddb all VM1
    scripts/certification/scenarios.sh seed mysql all VM3
    CERT_REDIS_URL=redis://<host>:6379/0 scripts/certification/scenarios.sh seed redis all
    scripts/certification/scenarios.sh parity all --all

`parity` compares the document counts per entity, the scale and the seed across the engines that have
been seeded; store-element counts are not compared, because the relational engines hold each paired
fact once. The load mixes of the layouts are not part of this tool.
