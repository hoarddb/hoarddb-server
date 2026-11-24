# The first v1.0-written BSON corpus

What this is: every value of the v1.0 server's own codec that frames as one BSON document and was
written to Badger while the named tests and two dataset generators ran, deduplicated by SHA-256
and capped (bson-codec-spec.md 9.1.6, bson-codec-build.md step 1d). Generated documents only; no
secrets and no real data. The property-shaped dataset is synthetic (`bench/propertyshape`).

main merge-base: fb8649179789b14db8116d089e2c9ab52766839e
next: 6be108fe30cc8b9b548e9c27ba471a32d520df7c
capture tree: 19f2de3f93c154e67c99ee90ffa793306d20f80a
records: 50000
gzip bytes: 1381154

`main merge-base` is `git merge-base main next`: the `main` commit v1.0's codec matches. `next` is
the branch tip the step was cut from. `capture tree` is the commit of branch `bson/step-1d-rest`
the capture ran on (the tap at 3fabefe0 and the tools below it); the capture script's merge phase
was then re-run with one fix to its input glob, which is in the commit that adds this corpus.
The tree is still on v1.0's own codec (vendor `go.mongodb.org/mongo-driver` v1.17.10).

## Commands

The capture is **private-only**: `bench/social` and `bench/propertyshape` below are the benchmark
harness of the private repository, which is not in the published tree, so `capture.sh` cannot be re-run
from it. The corpus and this record are published for the oracle tests that read them.

All from `hoardDB/`, `GOWORK=off`, by `bash internal/bsonoracle/testdata/v1corpus/capture.sh`
(raw records went to a scratch directory, `$BSONCORPUS_OUT` per phase):

```
go test -tags bsoncorpus -count=1 ./store/...
go test -tags bsoncorpus -count=1 ./storage/...
go test -tags bsoncorpus -count=1 -run 'Test.*(Hash|Btree|Heap|Queue|Fifo|Lifo|Blob|Ingest|Restore|Migrat|Dead|StoreField|Update)' ./server/
go test -tags bsoncorpus -count=1 -run 'Test.*Ingest' ./cli/...
go build -tags bsoncorpus -o <scratch>/hoarddb-server ./cmd/server      # started on a scratch data directory
go run ./bench/social -addr <addr> -users 200 -seed-posts 10 -conc 4 -warmup 1s -duration 5s
go run ./bench/propertyshape -out <scratch>/shape                        # synthetic, seed 1, scale 1
hoarddb ingest <scratch>/shape --into-db propshape -address <addr> -insecure
go run ./internal/bsonoracle/corpuscapture -in <the phase directories> -out records.jsonl.gz > stats.json
```

The four test runs exited 0. The capture read 1266926 tapped values, left out 0 keys outside the
shared layout and 53 values over 256 KiB, found 613839 distinct values, and kept 50000
(the 50,000 cap binds; the gzip is far under 8 MiB). A class with fewer records than an equal share
keeps all of them; the larger classes share the rest and take the smallest SHA-256 first.

## Classes

A class names the record by its key prefix (slot, bucket id, namespace, kind byte). `depthLimit` is
`server/nesting_depth.go:recordDepthLimit` for the site; `delivery-dead` takes the heap's 102
because a key does not name its store type; no other class is deeper than 100.

| class | distinct values | kept |
| --- | --- | --- |
| blobmeta | 55 | 55 |
| delivery-dead | 903 | 903 |
| documents | 122512 | 11989 |
| fieldstore | 76 | 76 |
| node-blobregistry | 7 | 7 |
| node-catalog | 43608 | 11989 |
| node-materialize | 54 | 54 |
| node-reclaim | 94 | 94 |
| node-replication-a | 11 | 11 |
| node-replication-d | 6 | 6 |
| node-replication-g | 3 | 3 |
| node-replication-h | 12 | 12 |
| node-replication-m | 169 | 169 |
| node-replication-n | 10 | 10 |
| node-replication-p | 627 | 627 |
| node-replication-s | 2 | 2 |
| node-replication-v | 3 | 3 |
| node-replication-x | 12 | 12 |
| queue | 247047 | 11989 |
| state | 198628 | 11989 |
