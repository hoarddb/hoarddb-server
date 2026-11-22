package main

import (
	"errors"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// config is every knob of the run; it is echoed into the JSON report. The flags
// the shell harness calls are listed, with an example each, in HARNESS.md, and
// TestHarnessFlagContract holds that file and this one to each other.
type config struct {
	Engine string `json:"engine"`
	Addr   string `json:"addr,omitempty"`
	// Addrs is Addr as a list: one entry per hoardDB node.
	Addrs []string `json:"addrs,omitempty"`
	// Routing is a (RP-A, one entry node) or b (RP-B, reads spread over every member).
	Routing string `json:"routing,omitempty"`
	// WriteConcern is sent on every hoardDB mutation: one, majority or all; empty keeps the node's default.
	WriteConcern string `json:"write_concern,omitempty"`
	// MongoReadPref names the MongoDB read preference; empty derives it from Routing.
	MongoReadPref string `json:"mongo_read_pref,omitempty"`
	// MongoNode, when set, connects directly to this one member of the replica
	// set instead of discovering it, for the convergence check.
	MongoNode string `json:"-"`
	// Level is the durability level the run claims: L-A, L-B, L-R or L-R-all (spec 3.3).
	Level string `json:"level,omitempty"`
	// HoardEnv and HoardLog are the launch environment the harness set and the
	// server's log, which hold the facts hoardDB cannot report at runtime.
	HoardEnv string `json:"hoard_env,omitempty"`
	HoardLog string `json:"hoard_log,omitempty"`
	// MongoTLS and MySQLTLS make the load client connect to the competitors
	// over TLS with the certificate unchecked, as it does to hoardDB.
	MongoTLS bool `json:"mongo_tls"`
	MySQLTLS bool `json:"mysql_tls"`
	// CockroachTLS asks for an encrypted connection to CockroachDB, certificate
	// unchecked. It is off by default because the recipe's single node is
	// insecure; a leg without it is the record-only TLS-off leg.
	CockroachTLS bool `json:"cockroach_tls,omitempty"`
	// PostgresTLS asks for an encrypted connection to PostgreSQL, certificate
	// unchecked, as MongoTLS and MySQLTLS do; it is on by default.
	PostgresTLS bool `json:"postgres_tls"`
	// SyncMS is the witness reference: the latency in milliseconds of the cheapest
	// data-only flush on the server's disk (pwrite plus fdatasync to a preallocated
	// block, sync_datasync_ms_p50 of -mode syncprobe), which a durable depth-1
	// write is compared with.
	SyncMS float64 `json:"sync_ms,omitempty"`
	// SyncOverwriteMS and SyncAppendMS are the O_DSYNC overwrite and O_DSYNC
	// append figures; recorded beside SyncMS and never the reference.
	SyncOverwriteMS float64 `json:"sync_overwrite_ms,omitempty"`
	SyncAppendMS    float64 `json:"sync_append_ms,omitempty"`
	// WitnessP50MS is the depth-1 write median the witness measured, carried
	// into the leg record with its ratio to SyncMS.
	WitnessP50MS float64 `json:"witness_p50_ms,omitempty"`
	// SyncDir is where -mode syncprobe measures.
	SyncDir string `json:"-"`
	// Config, Copies, Placement, Shards and Variant label the leg record: the
	// configuration (C1 to C5b), the copies of the data kept, the hoardDB
	// placement form (h1, h64, h256 or hu) and its shard count, and a name for a
	// record-only variant.
	Config    string `json:"config,omitempty"`
	Copies    int    `json:"copies,omitempty"`
	Placement string `json:"placement,omitempty"`
	Shards    int    `json:"shards,omitempty"`
	Variant   string `json:"variant,omitempty"`
	// RingFingerprint pins the ring node's certificate (-engine ring); empty leaves it unchecked,
	// as the hoardDB client does for a self-signed node.
	RingFingerprint string `json:"-"`
	// RingSerial sends the independent requests of one ring operation one after another, as the
	// other engines do, instead of in flight together on the connection (-engine ring).
	RingSerial bool `json:"ring_serial,omitempty"`
	// RingNoProject restores whole-document reads (get) where the ring leg now asks only for the
	// scalars an operation uses (get_fields), so figures taken before the projection can be
	// reproduced.
	RingNoProject bool `json:"ring_no_project,omitempty"`
	// RingFanoutParallel sends the feed pushes of a publish (W1) in flight together on the
	// connection, at most RingFanoutWidth at a time, instead of one after another (-engine ring).
	RingFanoutParallel bool `json:"ring_fanout_parallel,omitempty"`
	// RingFanoutWidth bounds the pushes in flight at once for one publish.
	RingFanoutWidth int `json:"ring_fanout_width,omitempty"`
	// RingBatch sends the feed pushes of a publish (W1) as RING_BATCH frames of RingBatchSize
	// operations instead of one RING_KEYED each (-engine ring).
	RingBatch bool `json:"ring_batch,omitempty"`
	// RingBatchSize is the operations in one frame, 1 to 255.
	RingBatchSize int `json:"ring_batch_size,omitempty"`
	// HoardBatch sends each hoardDB operation's sub-operations as one BATCH
	// request (batch-request spec §7.2). It forces Variant to "batch", so a
	// batched run is never recorded as the comparison of record.
	HoardBatch bool `json:"-"`
	// Fanout is single or bulk: whether the MongoDB and MySQL engines write
	// each follower's feed entry with its own request, as hoardDB does, or
	// with one bulk write.
	Fanout string `json:"fanout,omitempty"`
	// Loop is closed or open; empty follows -rate (open when it is positive).
	Loop string `json:"loop,omitempty"`
	// WarmConverged says the warm-up before this leg converged; false records warm=false.
	WarmConverged bool `json:"warm_converged"`
	// WarmShort, WarmWindow, WarmMin and WarmMax shape -mode warm.
	WarmShort  bool          `json:"-"`
	WarmWindow time.Duration `json:"-"`
	WarmMin    time.Duration `json:"-"`
	WarmMax    time.Duration `json:"-"`
	// SampleUsers is how many users the parity hash and the convergence check read.
	SampleUsers int `json:"sample_users,omitempty"`
	// ConvergeTimeout and ConvergePoll bound -mode converge.
	ConvergeTimeout time.Duration `json:"-"`
	ConvergePoll    time.Duration `json:"-"`
	// MySQLReadRouting is primary or roundrobin; empty derives it from Routing.
	MySQLReadRouting string        `json:"mysql_read_routing,omitempty"`
	MongoURI         string        `json:"-"` // may carry credentials; the report records a redacted target
	MongoWC          string        `json:"mongo_wc,omitempty"`
	MySQLDSN         string        `json:"-"`
	CockroachDSN     string        `json:"-"` // may carry credentials; the report records a redacted target
	PostgresDSN      string        `json:"-"` // may carry credentials; the report records a redacted target
	Password         string        `json:"-"`
	Database         string        `json:"database"`
	Users            int           `json:"users"`
	SeedPosts        int           `json:"seed_posts"`
	PostMin          int           `json:"post_min_bytes"`
	PostMax          int           `json:"post_max_bytes"`
	ReadPct          int           `json:"read_pct"`
	ReadPosts        int           `json:"read_posts"`
	ZipfS            float64       `json:"zipf_s"`
	Conc             int           `json:"conc"`
	Duration         time.Duration `json:"-"`
	Warmup           time.Duration `json:"-"`
	OpTimeout        time.Duration `json:"-"`
	// LoginConcurrency bounds the logins this process has in flight at once.
	LoginConcurrency int     `json:"login_concurrency,omitempty"`
	Attach           bool    `json:"attach"`
	VerifyPct        float64 `json:"verify_pct"`
	RandSeed         uint64  `json:"rand_seed"`
	// Rate is the offered rate in operations per second across all client VMs;
	// 0 keeps the closed loop. Each client VM offers Rate/ClientVMs.
	Rate float64 `json:"rate,omitempty"`
	// ClientVMs splits Rate and Conc evenly across this many client VMs; this
	// process is number ClientIndex of them. Conc then holds this process's share
	// and TotalConc the whole.
	ClientVMs   int `json:"client_vms,omitempty"`
	ClientIndex int `json:"client_index,omitempty"`
	TotalConc   int `json:"total_conc,omitempty"`
	// Mix is the -mix value (rh, wh, sk or op:<ID>); empty keeps the original -read-pct timeline workload.
	Mix string `json:"mix,omitempty"`
	// Dataset names the spec 6.4 dataset a -mix run uses: S-10k, S-100k or S-1M.
	Dataset string `json:"dataset,omitempty"`
	// Repeat is the N of the leg's back-to-back repeats, recorded as is.
	Repeat int `json:"repeat,omitempty"`
	// VerifySafety selects the safety form of the read checks, for replicated
	// routing where a replica may legitimately be behind (spec 6.6).
	VerifySafety bool `json:"verify_safety,omitempty"`
	// Out is the file the -mix leg record is appended to; empty writes stdout.
	Out string `json:"-"`
	// SeriesOut, when set, is the file a -mix leg writes its per-second operation series to.
	SeriesOut string `json:"-"`
	// HistPrefix, when set, names the leg's histogram files: <prefix>-read.hist and <prefix>-write.hist.
	HistPrefix string `json:"-"`
	// LedgerOut is where a -mix leg writes its acknowledged-write ledger; LedgerFile
	// is the ledger -mode ledgercheck reads, and LedgerOwner limits that check to
	// the authors one node owns.
	LedgerOut, LedgerFile, LedgerOwner string `json:"-"`
	// WALDir is the copy of a node's wal directory -mode walcheck reads.
	WALDir string `json:"-"`
	// Summarize switches the process from running a leg to printing the reading
	// tables of the leg-record files in Files; CSV prints them as CSV.
	Mode      string      `json:"-"`
	Soak      soakOptions `json:"-"`
	Summarize bool        `json:"-"`
	CSV       bool        `json:"-"`
	Files     []string    `json:"-"`
}

// The two ways a competitor engine can fan a publish out to its followers.
const (
	// variantBatch is the record-only variant -hoard-batch records.
	variantBatch = "batch"
	fanoutSingle = "single"
	fanoutBulk   = "bulk"
)

// bulkFanout reports whether the run keeps the competitors' bulk writes; an
// empty Fanout is single, the mode of every run unless -fanout bulk says so.
func (c *config) bulkFanout() bool { return c.Fanout == fanoutBulk }

// secondsFlag reads a positive number of seconds, fractions allowed, into a Duration.
type secondsFlag struct{ target *time.Duration }

func (s secondsFlag) String() string {
	if s.target == nil {
		return ""
	}
	return strconv.FormatFloat(s.target.Seconds(), 'f', -1, 64)
}

func (s secondsFlag) Set(text string) error {
	seconds, err := strconv.ParseFloat(text, 64)
	if err != nil || seconds <= 0 {
		return fmt.Errorf("%q is not a positive number of seconds", text)
	}
	*s.target = time.Duration(seconds * float64(time.Second))
	return nil
}

// newFlagSet declares every flag of the tool on cfg. -help prints these, so the
// text here is what an operator reads at 3am.
func newFlagSet(cfg *config) *flag.FlagSet {
	cfg.Duration = 60 * time.Second
	flags := flag.NewFlagSet("social", flag.ContinueOnError)
	flags.StringVar(&cfg.Mode, "mode", "", "what to run: empty runs a leg (with -mix) or the old timeline workload; seed creates the schema and seeds -dataset; warm runs the -mix unmeasured until throughput settles; run measures one leg on the seeded data; parity-hash hashes a sample of users in canonical form; converge waits for every node to return what the first one does; witness times depth-1 writes against -sync-ms and checks hoardDB's launch files against -level; tlscheck proves the load client's connections are encrypted; clientcpu measures the client's CPU per operation at 64 and 128 workers; soak runs a resumable soak in windows; driftreport prints the drift report of a soak directory; merge-hist merges the .hist files named after the flags (one per client VM) and prints the percentiles of their union")
	flags.StringVar(&cfg.Engine, "engine", engineHoardDB, "database under test: hoarddb, mongo, mysql, postgres, cockroach or ring (hoardDB over the entry-ring wire, documents with store fields)")
	flags.StringVar(&cfg.Addr, "addr", "127.0.0.1:7433", "hoardDB driver listener address; several nodes of one cluster are separated by commas (-engine hoarddb)")
	flags.StringVar(&cfg.Password, "password", "bench-root-password", "root password (bench convention)")
	flags.StringVar(&cfg.Database, "db", "social", "database name")
	flags.StringVar(&cfg.Routing, "routing", routingA, "read routing: a sends every connection to one node (RP-A), b spreads reads over all nodes (RP-B)")
	flags.StringVar(&cfg.WriteConcern, "write-concern", "", "hoardDB write concern sent on every mutation: one, majority or all; empty keeps the node's default (-engine hoarddb; MongoDB has -mongo-wc)")
	flags.StringVar(&cfg.Level, "level", "", "durability level this run claims: L-A (durable ack, one node), L-B (relaxed), L-R (durable and replicated) or L-R-all; hoardDB's launch files and the witness are checked against it")
	flags.StringVar(&cfg.HoardEnv, "hoard-env", "", "file with the environment hoardDB was launched with (KEY=VALUE lines): its replication factor, write concern and unsafe-ack switch are recorded and checked against -level (-engine hoarddb)")
	flags.StringVar(&cfg.HoardLog, "hoard-log", "", "hoardDB's server log: the start-up WARN line about acknowledging before the sync must be absent at L-A and present at L-B (-engine hoarddb)")
	flags.StringVar(&cfg.MongoURI, "mongo-uri", "mongodb://localhost:27017", "MongoDB connection string (-engine mongo); pass the address of the server under test, or all members of the replica set")
	flags.StringVar(&cfg.MongoWC, "mongo-wc", "w1j", "MongoDB write concern: w1j (w:1, j:true: the durable default), w1 (journal flushed every 100 ms; the relaxed level) or majority (-engine mongo)")
	flags.StringVar(&cfg.MongoReadPref, "mongo-read-pref", "", "MongoDB read preference: primary, primaryPreferred, secondary, secondaryPreferred or nearest; empty follows -routing (a reads the primary, b the nearest member). A readPreference in -mongo-uri wins (-engine mongo)")
	flags.BoolVar(&cfg.MongoTLS, "mongo-tls", true, "connect to MongoDB over TLS with the certificate unchecked, as the hoardDB client does; -mongo-tls=false is for the record-only TLS-off leg (-engine mongo)")
	flags.StringVar(&cfg.MySQLDSN, "mysql-dsn", "bench:bench@tcp(localhost:3306)/social", "MySQL DSN naming the database (-engine mysql); pass the address of the server under test. Several servers, the source first, are separated by commas (percent-encode a comma inside a DSN)")
	flags.StringVar(&cfg.MySQLReadRouting, "mysql-read-routing", "", "MySQL reads: primary (the source only) or roundrobin (every server in turn); empty follows -routing (-engine mysql)")
	flags.BoolVar(&cfg.MySQLTLS, "mysql-tls", true, "connect to MySQL over TLS (tls=skip-verify) unless the DSN names a tls= value itself; -mysql-tls=false is for the record-only TLS-off leg (-engine mysql)")
	flags.StringVar(&cfg.CockroachDSN, "cockroach-dsn", "postgresql://root@localhost:26257/social", "CockroachDB URL naming the database (-engine cockroach); pass the address of the single node under test. The database is created when missing")
	flags.StringVar(&cfg.PostgresDSN, "postgres-dsn", "postgresql://bench@localhost:5432/social", "PostgreSQL URL naming the database (-engine postgres); pass the address of the server under test, with the password in the URL or in PGPASSWORD. The database is created when missing")
	flags.BoolVar(&cfg.PostgresTLS, "postgres-tls", true, "connect to PostgreSQL over TLS (sslmode=require, certificate unchecked) unless the URL names an sslmode itself; -postgres-tls=false is for the record-only TLS-off leg (-engine postgres)")
	flags.BoolVar(&cfg.CockroachTLS, "cockroach-tls", false, "connect to CockroachDB over TLS (sslmode=require, certificate unchecked) unless the URL names an sslmode itself; off by default because the recipe's single node is insecure, so a default leg is the record-only TLS-off leg (-engine cockroach)")
	flags.StringVar(&cfg.RingFingerprint, "ring-fingerprint", "", "pin the ring node's certificate to this fingerprint (the server prints it at start); empty leaves the certificate unchecked, as for hoardDB (-engine ring)")
	flags.BoolVar(&cfg.RingSerial, "ring-serial", false, "send the independent requests of one ring operation (the post documents of a timeline, a profile's counts) one after another, as the other engines do, instead of in flight together on the connection; recorded as ring_serial (-engine ring)")
	flags.BoolVar(&cfg.RingNoProject, "ring-no-project", false, "read whole documents (get, which reads every store field's state) where the ring leg by default projects only the scalars an operation uses (get_fields); reproduces figures taken before the projection; recorded as ring_no_project (-engine ring)")
	flags.BoolVar(&cfg.RingFanoutParallel, "ring-fanout-parallel", false, "push a publish's feed copies to the followers in flight together on the connection (at most -ring-fanout-width at a time, each under its own request id, retried alone when it fails) instead of one after another, so the server's WAL group commit can share fsyncs; recorded as ring_fanout_parallel (-engine ring)")
	flags.IntVar(&cfg.RingFanoutWidth, "ring-fanout-width", defaultRingFanoutWidth, "most feed pushes of one publish in flight at once with -ring-fanout-parallel (-engine ring)")
	flags.BoolVar(&cfg.RingBatch, "ring-batch", false, "send a publish's feed copies as RING_BATCH frames (up to -ring-batch-size pushes per request) instead of one request per follower; a frame with a not-attempted operation is resent whole under its request id; recorded as ring_batch (-engine ring)")
	flags.IntVar(&cfg.RingBatchSize, "ring-batch-size", defaultRingBatchSize, "pushes per RING_BATCH frame with -ring-batch, 1 to 255 (-engine ring)")
	flags.StringVar(&cfg.Mix, "mix", "", "operation mix: rh, wh, sk or op:<ID> (for example op:R3), run on the -dataset; one perf-record/2 line goes to -out or stdout")
	flags.StringVar(&cfg.SeriesOut, "series-out", "", "write the leg's operations per second to this file, one JSON line per second counted from the start of the measured window (warm-up seconds are negative), even when the leg ends in an error; needs -mix")
	flags.StringVar(&cfg.Dataset, "dataset", "", "S-10k, S-100k or S-1M (default S-10k), for -mix and the modes seed, warm, run, parity-hash and converge")
	flags.StringVar(&cfg.Placement, "placement", "h1", "hoardDB placement form the data is laid out in and recorded with the leg: h1 (one User bucket), h64 (User_00 to User_63, a user in xxHash64(id) mod 64) or hu (one User_<id> bucket per user, so the ring spreads users); h256 is phase 2 (S41) and refused; MongoDB, MySQL, PostgreSQL and CockroachDB take h1 only")
	flags.IntVar(&cfg.Shards, "shards", 0, "shard count recorded with the leg; 0 takes it from -placement")
	flags.StringVar(&cfg.Loop, "loop", "", "closed (every worker waits for its reply) or open (Poisson arrivals at -rate); empty is open when -rate is positive")
	flags.IntVar(&cfg.LoginConcurrency, "login-concurrency", defaultLoginConcurrency, "logins this process has in flight at once; the sockets of a -depth burst wait their turn, because a default hoardDB node refuses about a hundred logins in flight from one address; a login refused as RATE_LIMITED is retried with backoff")
	flags.IntVar(&cfg.Conc, "depth", 32, "operations in flight: closed loop workers, or the pool for an open loop; hoardDB gives each its own socket, MongoDB, MySQL, PostgreSQL and CockroachDB a pool of exactly this many connections")
	flags.Var(secondsFlag{&cfg.Duration}, "seconds", "measured run length in seconds")
	flags.DurationVar(&cfg.Warmup, "warmup", 10*time.Second, "unmeasured warmup before the measured run")
	flags.DurationVar(&cfg.OpTimeout, "op-timeout", 30*time.Second, "per-request timeout")
	flags.Float64Var(&cfg.Rate, "rate", 0, "open loop: offered operations per second in total, Poisson arrivals, latency counted from each arrival's scheduled time; 0 keeps the closed loop")
	flags.IntVar(&cfg.ClientVMs, "client-vms", 1, "split -rate and -depth evenly across this many client VMs; merge their .hist files afterwards")
	flags.IntVar(&cfg.ClientIndex, "client-index", 0, "which client VM this is, from 0 to -client-vms minus 1")
	flags.IntVar(&cfg.Repeat, "repeat", 0, "which back-to-back repeat of the leg this is, recorded in the leg record")
	flags.Uint64Var(&cfg.RandSeed, "seed", 1, "seed for the workload generators")
	flags.StringVar(&cfg.Variant, "variant", "", "name of a record-only variant, recorded in the leg's engine settings (for example cache-raised)")
	flags.BoolVar(&cfg.HoardBatch, "hoard-batch", false, "send each hoardDB operation's sub-operations as one BATCH request (publish, like, follow, unfollow, comment, post page): a record-only variant, recorded as variant batch and refused beside any other -variant (-engine hoarddb)")
	flags.StringVar(&cfg.Fanout, "fanout", fanoutSingle, "how MongoDB, MySQL, PostgreSQL and CockroachDB write the feed entries of a publish: single (one write per follower, one after another, as hoardDB does; the default) or bulk (one bulk write); recorded in the leg record as fanout (-engine mongo, mysql, postgres or cockroach)")
	flags.StringVar(&cfg.Config, "config", "", "configuration the leg belongs to, C1 to C5b; empty takes C1, C2 or C3 from the node count")
	flags.IntVar(&cfg.Copies, "copies", 0, "copies of the data kept, recorded with the leg; 0 means one for a single node and the node count otherwise")
	flags.BoolVar(&cfg.VerifySafety, "verify-safety", false, "check reads in the safety form (no phantom, no duplicate, right order per author, count between the pre-run floor and the issue count), for replicated routing where a replica may be behind")
	flags.Float64Var(&cfg.VerifyPct, "verify-pct", 1, "percentage of reads to verify (at least 1)")
	flags.StringVar(&cfg.Out, "out", "", "append the leg record (or the mode's JSON line) to this file instead of printing it")
	flags.StringVar(&cfg.LedgerOut, "ledger-out", "", "write the acknowledged-write ledger of this leg to this file when it ends: per author the highest acknowledged post and the one in flight, for -mode ledgercheck (-engine hoarddb)")
	flags.StringVar(&cfg.LedgerFile, "ledger", "", "with -mode ledgercheck: the ledger file written by -ledger-out")
	flags.StringVar(&cfg.LedgerOwner, "ledger-owner", "", "with -mode ledgercheck: check only the authors whose bucket this node owns (the node id recorded in the ledger), for a node that answers reads for the buckets it owns only")
	flags.StringVar(&cfg.WALDir, "wal-dir", "", "with -mode walcheck: a copy of a stopped node's wal directory (plain, not sealed); the ledger's acknowledged posts must all be in it")
	flags.StringVar(&cfg.HistPrefix, "hist-prefix", "", "write the read and write latency histograms to <prefix>-read.hist and <prefix>-write.hist")
	flags.BoolVar(&cfg.Attach, "attach", false, "reuse an existing seeded dataset instead of creating and seeding one (a -mix run without -mode; the modes run, warm, parity-hash and converge always attach)")
	flags.BoolVar(&cfg.WarmConverged, "warm-converged", true, "with -mode run: whether the warm-up before this leg converged; false records warm=false in the leg record")
	flags.BoolVar(&cfg.WarmShort, "warm-short", false, "with -mode warm: a short warm-up of 5 minutes, used after a restart or a reseed")
	flags.DurationVar(&cfg.WarmWindow, "warm-window", 60*time.Second, "with -mode warm: length of one throughput window")
	flags.DurationVar(&cfg.WarmMin, "warm-min", 5*time.Minute, "with -mode warm: shortest warm-up")
	flags.DurationVar(&cfg.WarmMax, "warm-max", 15*time.Minute, "with -mode warm: longest warm-up; a warm-up that has not settled by then records warm=false and goes on")
	flags.IntVar(&cfg.SampleUsers, "sample", 1000, "users the parity hash and the convergence check read")
	flags.DurationVar(&cfg.ConvergeTimeout, "converge-timeout", 120*time.Second, "with -mode converge: how long a node may take to catch up")
	flags.DurationVar(&cfg.ConvergePoll, "converge-poll", 2*time.Second, "with -mode converge: pause between comparisons")
	flags.Float64Var(&cfg.SyncMS, "sync-ms", 0, "with -mode witness or run: the reference, the latency in milliseconds of the cheapest data-only flush on the server's disk (pwrite plus fdatasync to a preallocated block; -mode syncprobe prints it as sync_datasync_ms_p50), not an O_DSYNC figure")
	flags.Float64Var(&cfg.SyncOverwriteMS, "sync-overwrite-ms", 0, "with -mode witness or run: the latency of one O_DSYNC overwrite of a preallocated block (sync_overwrite_ms_p50 of -mode syncprobe), recorded beside -sync-ms with its ratio; it is never the reference")
	flags.Float64Var(&cfg.SyncAppendMS, "sync-append-ms", 0, "with -mode witness or run: the latency of one O_DSYNC write that extends a file (sync_append_ms_p50 of -mode syncprobe), recorded beside -sync-ms with its ratio; it is never the reference")
	flags.Float64Var(&cfg.WitnessP50MS, "witness-p50-ms", 0, "with -mode run: the depth-1 write median the witness printed (depth1_write_p50_ms), recorded in the leg with its ratio to -sync-ms")
	flags.StringVar(&cfg.SyncDir, "sync-dir", "", "with -mode syncprobe: a directory on the disk under test (the server's data directory)")
	flags.IntVar(&cfg.Users, "users", 10000, "users of the old timeline workload, one profile and one LIFO timeline bucket each")
	flags.IntVar(&cfg.SeedPosts, "seed-posts", 20, "posts seeded into each timeline before measuring (old timeline workload)")
	flags.IntVar(&cfg.PostMin, "post-min", 280, "minimum post text length in bytes (old timeline workload)")
	flags.IntVar(&cfg.PostMax, "post-max", 1800, "maximum post text length in bytes (old timeline workload)")
	flags.IntVar(&cfg.ReadPct, "read-pct", 80, "percentage of operations that are reads (old timeline workload)")
	flags.IntVar(&cfg.ReadPosts, "read-posts", 20, "posts fetched per read (old timeline workload)")
	flags.Float64Var(&cfg.ZipfS, "zipf-s", 1.1, "Zipf exponent for user choice (must be > 1)")
	registerSoakFlags(flags, &cfg.Soak)
	flags.BoolVar(&cfg.Summarize, "summarize", false, "print the reading tables of the perf-record/2 files named after the flags, instead of running a leg")
	flags.BoolVar(&cfg.CSV, "csv", false, "with -summarize, print CSV (one row per group: the median of the repeats and its range)")
	return flags
}

// helpText is what -help prints: every flag with its default.
func helpText() string {
	var text strings.Builder
	flags := newFlagSet(&config{})
	flags.SetOutput(&text)
	flags.PrintDefaults()
	return text.String()
}

func parseFlags(args []string) (*config, error) {
	cfg := &config{}
	flags := newFlagSet(cfg)
	if err := flags.Parse(args); err != nil {
		return nil, fmt.Errorf("parsing flags: %w", err)
	}
	if cfg.Summarize {
		cfg.Files = flags.Args()
		if len(cfg.Files) == 0 {
			return nil, errors.New("-summarize needs at least one leg-record file, for example: social -summarize leg1.json leg2.json")
		}
		return cfg, nil
	}
	if cfg.Mode == modeMergeHist {
		cfg.Files = flags.Args()
		if len(cfg.Files) == 0 {
			return nil, errors.New("-mode merge-hist needs the .hist files to merge, for example: social -mode merge-hist -out merged.json c1-rh-read.hist c2-rh-read.hist")
		}
		return cfg, nil
	}
	if cfg.Mode == modeLedgerCheck && cfg.LedgerFile == "" {
		return nil, errors.New("-mode ledgercheck needs -ledger, the file a leg wrote with -ledger-out, for example: social -mode ledgercheck -ledger ledger.json -addr <node>")
	}
	if cfg.Mode == modeWALCheck && (cfg.LedgerFile == "" || cfg.WALDir == "") {
		return nil, errors.New("-mode walcheck needs -ledger (the file a leg wrote with -ledger-out) and -wal-dir (a copy of a stopped node's wal directory), for example: social -mode walcheck -ledger ledger.json -wal-dir ./wal-copy")
	}
	if cfg.LoginConcurrency < 1 {
		return nil, fmt.Errorf("-login-concurrency %d must be at least 1", cfg.LoginConcurrency)
	}
	if cfg.Engine != engineMongo {
		cfg.MongoWC = "" // only MongoDB has a write-concern flag
	}
	if cfg.Engine != engineHoardDB && cfg.Engine != engineRing {
		cfg.Addr = "" // the report must not echo a default address that was never dialled
	}
	if cfg.Engine != engineRing {
		cfg.RingSerial, cfg.RingFingerprint, cfg.RingNoProject = false, "", false
		cfg.RingFanoutParallel, cfg.RingFanoutWidth = false, 0
		cfg.RingBatch, cfg.RingBatchSize = false, 0
	}
	if err := cfg.resolveRouting(); err != nil {
		return nil, err
	}
	if err := cfg.resolveLabels(); err != nil {
		return nil, err
	}
	if err := cfg.validateClients(); err != nil {
		return nil, err
	}
	if cfg.ClientVMs > 1 {
		cfg.TotalConc = cfg.Conc
		_, cfg.Conc = clientShare(cfg.Rate, cfg.TotalConc, cfg.ClientVMs, cfg.ClientIndex)
	}
	return cfg, cfg.validate()
}

// resolveRouting validates the multi-node flags and drops the ones another
// engine owns, so the report never echoes a setting that was not in force.
func (c *config) resolveRouting() error {
	var err error
	if c.Routing, err = parseRouting(c.Routing); err != nil {
		return err
	}
	if c.WriteConcern, err = parseWriteConcern(c.WriteConcern); err != nil {
		return err
	}
	if c.Engine != engineHoardDB && c.WriteConcern != "" {
		return fmt.Errorf("-write-concern applies to -engine hoarddb; for MongoDB use -mongo-wc or w=/journal= in -mongo-uri, MySQL's, PostgreSQL's and CockroachDB's durability is the server's setting, and the ring wire's is the node's (see \"Ring leg\" in HARNESS.md)")
	}
	switch c.Engine {
	case engineHoardDB:
		if c.Addrs, err = parseAddrList(c.Addr); err != nil {
			return err
		}
	case engineRing:
		if c.Addrs, err = parseAddrList(c.Addr); err != nil {
			return err
		}
		if len(c.Addrs) != 1 {
			return fmt.Errorf("-engine ring drives one node and -addr names %d. Pass the one address, for example -addr <host>:<port>; the ring routing across nodes is not built in this harness", len(c.Addrs))
		}
		if c.Routing == routingB {
			return errors.New("-routing b spreads reads over several nodes and -engine ring drives one. Drop -routing b (a is the default)")
		}
	case engineMongo:
		if _, _, err = resolveMongoReadPref(c); err != nil {
			return err
		}
	case engineCockroach:
		if _, err = c.cockroachConnectionString(); err != nil {
			return err
		}
	case enginePostgres:
		if _, err = c.postgresConnectionString(); err != nil {
			return err
		}
	case engineMySQL:
		if c.MySQLReadRouting != "" && c.MySQLReadRouting != "primary" && c.MySQLReadRouting != "roundrobin" {
			return fmt.Errorf("-mysql-read-routing %q is not primary or roundrobin", c.MySQLReadRouting)
		}
	}
	if c.Fanout != fanoutSingle && c.Fanout != fanoutBulk {
		return fmt.Errorf("-fanout %q is not single or bulk", c.Fanout)
	}
	if c.HoardBatch {
		if c.Engine != engineHoardDB {
			return fmt.Errorf("-hoard-batch applies to -engine hoarddb: MongoDB, MySQL, PostgreSQL and CockroachDB send their bulk writes under -fanout bulk")
		}
		if c.Variant != "" && c.Variant != variantBatch {
			return fmt.Errorf("-hoard-batch records the leg as variant %q, so it cannot run beside -variant %q; drop -variant", variantBatch, c.Variant)
		}
		c.Variant = variantBatch
	}
	if c.Engine == engineHoardDB && c.bulkFanout() {
		return fmt.Errorf("-fanout bulk applies to -engine mongo, mysql, postgres or cockroach; hoardDB has no bulk write and always sends one request per follower")
	}
	if c.Engine == engineRing && c.bulkFanout() {
		return fmt.Errorf("-fanout bulk applies to -engine mongo, mysql, postgres or cockroach; the ring wire has no bulk write and the publish sends one request per follower")
	}
	if c.Engine == engineRing && c.RingBatch {
		if c.RingFanoutParallel {
			return errors.New("-ring-batch and -ring-fanout-parallel both change how a publish sends its feed copies; pick one")
		}
		if c.RingBatchSize < 1 {
			return fmt.Errorf("-ring-batch-size %d must be at least 1; the default is %d", c.RingBatchSize, defaultRingBatchSize)
		}
		if c.RingBatchSize > maxRingBatchSize {
			return fmt.Errorf("-ring-batch-size %d is more than a frame holds: at most %d. Use -ring-batch-size %d or fewer", c.RingBatchSize, maxRingBatchSize, maxRingBatchSize)
		}
	}
	if c.Level, err = parseLevel(c.Level); err != nil {
		return err
	}
	if c.Engine != engineMongo {
		c.MongoReadPref = ""
	}
	if c.Engine != engineMySQL {
		c.MySQLReadRouting = ""
	}
	return nil
}

// validateClients checks the split before it is applied, while Conc is still the total.
func (c *config) validateClients() error {
	switch {
	case c.Rate < 0:
		return errors.New("-rate must not be negative")
	case c.ClientVMs < 1:
		return errors.New("-client-vms must be at least 1")
	case c.ClientIndex < 0 || c.ClientIndex >= c.ClientVMs:
		return fmt.Errorf("-client-index %d is outside 0..%d", c.ClientIndex, c.ClientVMs-1)
	case c.Conc < c.ClientVMs:
		return fmt.Errorf("-depth %d cannot be split across %d client VMs: each needs at least one worker", c.Conc, c.ClientVMs)
	}
	return nil
}

// offeredRate is this process's share of the offered rate, 0 for a closed loop.
func (c *config) offeredRate() float64 {
	return c.Rate / float64(max(c.ClientVMs, 1))
}

func (c *config) validate() error {
	switch {
	case c.Engine != engineHoardDB && c.Engine != engineMongo && c.Engine != engineMySQL && c.Engine != enginePostgres && c.Engine != engineCockroach && c.Engine != engineRing:
		return fmt.Errorf("-engine %q is not one of hoarddb, mongo, mysql, postgres, cockroach, ring", c.Engine)
	case c.Engine == engineMongo && c.MongoWC != "w1" && c.MongoWC != "w1j" && c.MongoWC != "majority":
		return fmt.Errorf("-mongo-wc %q is not one of w1, w1j, majority", c.MongoWC)
	case c.Users < 2:
		return errors.New("-users must be at least 2")
	case c.PostMin < 1 || c.PostMax < c.PostMin:
		return errors.New("-post-min must be positive and no larger than -post-max")
	case c.ReadPct < 0 || c.ReadPct > 100:
		return errors.New("-read-pct must be between 0 and 100")
	case c.ZipfS <= 1:
		return errors.New("-zipf-s must be greater than 1 (math/rand/v2's Zipf is undefined at or below 1)")
	case c.Conc < 1:
		return errors.New("-depth must be at least 1")
	case c.VerifyPct < 1 || c.VerifyPct > 100:
		return errors.New("-verify-pct must be between 1 and 100: an unverified run proves nothing")
	case c.ReadPosts < 1:
		return errors.New("-read-posts must be at least 1")
	case c.Duration <= 0:
		return errors.New("-seconds must be positive")
	case c.Mode != "" && !slices.Contains(knownModes(), c.Mode):
		return modeError(c.Mode)
	case c.SampleUsers < 1:
		return errors.New("-sample must be at least 1")
	}
	if c.needsMix() && c.Mix == "" {
		return fmt.Errorf("-mode %s needs -mix (rh, wh, sk or op:<ID>)", c.Mode)
	}
	if c.SeriesOut != "" && c.Mix == "" {
		return errors.New("-series-out needs -mix: the series belongs to one -mix leg (for example -mix wh -series-out series.jsonl)")
	}
	if c.Mix != "" {
		if _, err := newMixScheduler(c.Mix, c.RandSeed); err != nil {
			return fmt.Errorf("-mix: %w", err)
		}
	}
	if c.Mix != "" || c.needsDataset() {
		if c.Dataset == "" {
			c.Dataset = "S-10k"
		}
		if _, err := lookupDataset(c.Dataset); err != nil {
			return fmt.Errorf("-dataset: %w", err)
		}
	}
	return nil
}

// needsMix says whether the mode runs operations of a mix.
func (c *config) needsMix() bool { return c.Mode == modeWarm || c.Mode == modeRun }

// needsDataset says whether the mode works on a seeded dataset.
func (c *config) needsDataset() bool {
	return slices.Contains([]string{modeSeed, modeWarm, modeRun, modeParityHash, modeConverge}, c.Mode)
}

// placementForms are the hoardDB placement forms of spec 6.2 as the leg record
// names them (spec 9.2), and the shard count each implies. hu has one bucket
// per user, so its shard count is the dataset's user count.
var placementForms = map[string]struct {
	label  string
	shards int
	// built says the engine can lay the form out; the others are phase 2
	// (spec 12.2) and are refused until they are.
	built bool
}{
	"h1":   {"single", 1, true},
	"h64":  {"shard-64", 64, true},
	"h256": {"shard-256", 256, false},
	"hu":   {"per-entity", 0, true},
}

// placementLabel is the record's placement and shards for a placement flag
// and, for hu, the dataset's user count.
func placementLabel(placement string, shards, users int) (label string, count int, err error) {
	form, known := placementForms[strings.ToLower(placement)]
	if !known {
		return "", 0, fmt.Errorf("-placement %q is not h1, h64, h256 or hu", placement)
	}
	count = form.shards
	if strings.EqualFold(placement, "hu") {
		count = users
	}
	if shards != 0 && shards != count {
		return "", 0, fmt.Errorf("-shards %d contradicts -placement %s, which has %d", shards, placement, count)
	}
	return form.label, count, nil
}

// resolveLabels checks the flags that only label the leg record, and refuses a
// label the engine cannot honour: a record that says shard-64 over data laid
// out as H-1 would put two different things in one group.
func (c *config) resolveLabels() error {
	c.Placement = strings.ToLower(c.Placement)
	form, known := placementForms[c.Placement]
	if !known {
		return fmt.Errorf("-placement %q is not h1, h64, h256 or hu", c.Placement)
	}
	if form.shards != 0 && c.Shards != 0 && c.Shards != form.shards {
		return fmt.Errorf("-shards %d contradicts -placement %s, which has %d", c.Shards, c.Placement, form.shards)
	}
	if !form.built {
		return fmt.Errorf("-placement %s is a phase 2 form (build step S41) and is not built yet; run with -placement h1 or h64, or the record would name a layout the data does not have", c.Placement)
	}
	if c.Engine != engineHoardDB && c.Placement != "h1" {
		return fmt.Errorf("-placement %s: %s has one form, its own single layout, which -placement h1 names; the sharded forms are hoardDB's, where a bucket is the unit of placement", c.Placement, c.Engine)
	}
	switch c.Loop {
	case "", "closed", "open":
	default:
		return fmt.Errorf("-loop %q is not closed or open", c.Loop)
	}
	switch {
	case c.Loop == "open" && c.Rate <= 0:
		return errors.New("-loop open needs -rate, the offered operations per second")
	case c.Loop == "closed" && c.Rate > 0:
		return errors.New("-rate is set, which is an open loop; pass -loop open, or drop -rate for a closed loop")
	case c.Copies < 0:
		return errors.New("-copies must not be negative")
	}
	return nil
}
