module github.com/hoarddb/hoarddb-server

go 1.26.9

require (
	github.com/buraksezer/consistent v1.1.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/dgraph-io/badger/v4 v4.9.6
	github.com/google/uuid v1.6.0
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/raft v1.8.0
	github.com/hashicorp/raft-boltdb/v2 v2.4.2
	github.com/hoarddb/hoarddb-bson v0.8.1
	github.com/hoarddb/hoarddb-bson/oracle v0.2.0
	github.com/prometheus/client_golang v1.24.1
	github.com/prometheus/client_model v0.6.3
	go.etcd.io/bbolt v1.4.1
	go.mongodb.org/mongo-driver v1.17.10
	golang.org/x/crypto v0.57.0
	golang.org/x/sys v0.48.0
	golang.org/x/tools v0.49.0
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/dgraph-io/ristretto/v2 v2.2.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.7.0 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/common v0.71.0 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
)

// Badger v4.9.6 with DB.Sync also syncing the immutable memtables.
// Recorded decision: docs/specs/badger-sync-patch-decision.md. Drop this line
// once upstream ships the change.
replace github.com/dgraph-io/badger/v4 => ./third_party/badger
