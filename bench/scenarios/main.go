// Command scenarios loads the same deterministic data set into hoardDB, MySQL, PostgreSQL, MongoDB, Redis or Kvrocks for
// one of five application layouts (social, chat, ledger, agent memory, video), so a scenario or a
// benchmark starts from identical data on every engine. See README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// config is what the flags say.
type config struct {
	Engine, Layout, Database string
	Scale                    int
	Seed                     uint64
	DDL                      bool
	Workers, Connections     int

	// Variant is the layout variant: variantCommon, or variantNative for the hoardDB-native copies.
	Variant string

	Addr, User, Password, Fingerprint string
	Insecure                          bool
	RF                                int

	MySQLDSN string
	MySQLTLS bool
	MongoURI string
	MongoTLS bool

	PostgresDSN string
	PostgresTLS bool

	RedisURL   string
	RedisTLS   bool
	KvrocksURL string
}

// The layout variants. Common is the layout every engine is seeded with. Native (hoardDB only)
// stores a copy of the post's author and text in each User.Feed element and of the member's
// name in each Room.Members element; see model/native.go.
const (
	variantCommon = "common"
	variantNative = "native"
)

// layoutAll is the -layout value that seeds every layout, each into a database of its own.
const layoutAll = "all"

func parseFlags(args []string, stderr io.Writer) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("scenarios", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.Engine, "engine", "", "hoarddb, mysql, postgres, mongo, redis or kvrocks")
	flags.StringVar(&cfg.Layout, "layout", layoutAll, "social, chat, ledger, agent, video or all (each layout goes into its own database, <database>_<layout>)")
	flags.StringVar(&cfg.Variant, "layout-variant", variantCommon, "common (every engine), or native (hoarddb only): User.Feed elements also carry the post's author and text, Room.Members elements the member's name; add +agg (hoarddb only) to declare the ledger's sum and count aggregates, and +deep<N> (every engine) for N expenses per ledger account, for example common+agg+deep10000")
	flags.IntVar(&cfg.Scale, "scale", 1, "multiplies every count of the layouts (1 is the size the test scenarios and layouts spec states)")
	flags.Uint64Var(&cfg.Seed, "seed", 1, "seed of the generator; the same seed and scale give byte-identical data on every engine")
	flags.StringVar(&cfg.Database, "database", "scenarios", "base name of the database to create and fill; a layout is seeded into <database>_<layout>")
	flags.BoolVar(&cfg.DDL, "ddl", false, "print the schema text of every engine of the chosen layouts to stdout and exit without connecting")
	flags.IntVar(&cfg.Workers, "workers", 16, "writes in flight (hoardDB: one worker per document and its stores; MySQL and MongoDB: batches in flight)")
	flags.IntVar(&cfg.Connections, "connections", 4, "hoardDB connections the workers share")

	flags.StringVar(&cfg.Addr, "addr", os.Getenv("HOARDDB_ADDR"), "hoardDB: host:port of the server's TCP listener (env HOARDDB_ADDR)")
	flags.StringVar(&cfg.User, "user", envOr("HOARDDB_ROOT_USER", "admin"), "hoardDB: bootstrap admin user (env HOARDDB_ROOT_USER)")
	flags.StringVar(&cfg.Password, "password", os.Getenv("HOARDDB_ROOT_PASSWORD"), "hoardDB: its password (env HOARDDB_ROOT_PASSWORD)")
	flags.StringVar(&cfg.Fingerprint, "fingerprint", "", "hoardDB: pin the server certificate to this fingerprint (the server prints it at start)")
	flags.BoolVar(&cfg.Insecure, "insecure", false, "hoardDB: skip certificate checks; for a server you started yourself on this machine, local development only")
	flags.IntVar(&cfg.RF, "rf", 1, "hoardDB: replication factor of the databases it creates")

	flags.StringVar(&cfg.MySQLDSN, "mysql-dsn", "bench:bench@tcp(localhost:3306)/", "MySQL: DSN of the server; any database name in it is ignored, the layout's database is created")
	flags.BoolVar(&cfg.MySQLTLS, "mysql-tls", true, "MySQL: connect over TLS with the certificate unchecked (tls=skip-verify) unless the DSN names a tls= value itself")
	flags.StringVar(&cfg.PostgresDSN, "postgres-dsn", "postgres://bench:bench@localhost:5432/postgres", "PostgreSQL: URL or key=value DSN of the server; its database is only the one the connection starts in (CREATE DATABASE needs one), the layout's database is created")
	flags.BoolVar(&cfg.PostgresTLS, "postgres-tls", true, "PostgreSQL: connect over TLS with the certificate unchecked (sslmode=require) unless the DSN names a verifying sslmode itself")
	flags.StringVar(&cfg.RedisURL, "redis-url", "redis://localhost:6379/0", "Redis: URL of the server (rediss:// for a TLS server whose certificate is verified); the -database flag becomes the key prefix, each layout <database>_<layout>")
	flags.BoolVar(&cfg.RedisTLS, "redis-tls", true, "Redis and Kvrocks: connect over TLS with the certificate unchecked unless the URL says rediss://")
	flags.StringVar(&cfg.KvrocksURL, "kvrocks-url", "redis://localhost:6666/0", "Kvrocks: URL of the server (Kvrocks listens on 6666 by default); the Redis key scheme on RocksDB, -redis-tls applies")
	flags.StringVar(&cfg.MongoURI, "mongo-uri", "mongodb://localhost:27017", "MongoDB: connection string")
	flags.BoolVar(&cfg.MongoTLS, "mongo-tls", true, "MongoDB: connect over TLS with the certificate unchecked")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	return cfg, cfg.check()
}

func envOr(name, fallback string) string {
	if configured := os.Getenv(name); configured != "" {
		return configured
	}
	return fallback
}

// layouts resolves -layout to the layouts to seed.
func (c config) layouts() ([]model.Layout, error) {
	if c.Layout == layoutAll {
		return model.Layouts, nil
	}
	layout, err := model.ParseLayout(c.Layout)
	if err != nil {
		return nil, err
	}
	return []model.Layout{layout}, nil
}

// check refuses a configuration that cannot work, naming the fix.
func (c config) check() error {
	if _, err := c.layouts(); err != nil {
		return err
	}
	variant, variantErr := model.ParseVariant(c.Variant)
	switch {
	case variantErr != nil:
		return fmt.Errorf("-layout-variant: %w", variantErr)
	case c.Scale < 1:
		return errors.New("-scale must be 1 or more")
	case c.Database == "":
		return errors.New("-database is empty. Pass the base name of the database to create, for example -database scenarios")
	case c.DDL:
		return nil
	case c.Workers < 1 || c.Connections < 1:
		return errors.New("-workers and -connections must be 1 or more")
	}
	if variant.Native && c.Engine != "hoarddb" && c.Engine != "" {
		return fmt.Errorf("engine %s has no native layout variant yet; drop -layout-variant, or seed hoarddb", c.Engine)
	}
	if variant.Aggregates && c.Engine != "hoarddb" && c.Engine != "" {
		return fmt.Errorf("engine %s has no +agg layout variant (declared aggregates are a hoarddb feature; the SQL and Mongo arms keep the ledger totals in their own columns); drop +agg from -layout-variant, or seed hoarddb", c.Engine)
	}
	switch c.Engine {
	case "hoarddb":
		switch {
		case c.Addr == "":
			return errors.New("-addr is empty. Pass the host:port hoarddb-server listens on for drivers (or set HOARDDB_ADDR), for example -addr <host>:<port>")
		case c.Password == "":
			return errors.New("no password. Set HOARDDB_ROOT_PASSWORD to the bootstrap admin's password, or pass -password")
		case c.Fingerprint == "" && !c.Insecure:
			return errors.New("neither -fingerprint nor -insecure. Pass -fingerprint <value the server printed at start>, or -insecure for a server you started yourself on this machine")
		case c.RF < 1:
			return errors.New("-rf must be 1 or more")
		}
	case "mysql":
		if c.MySQLDSN == "" {
			return errors.New("-mysql-dsn is empty. Pass user:password@tcp(host:3306)/ for the server to seed")
		}
	case "postgres":
		if c.PostgresDSN == "" {
			return errors.New("-postgres-dsn is empty. Pass postgres://user:password@host:5432/postgres for the server to seed")
		}
	case "redis":
		if c.RedisURL == "" {
			return errors.New("-redis-url is empty. Pass redis://host:6379/0 for the server to seed")
		}
	case "kvrocks":
		if c.KvrocksURL == "" {
			return errors.New("-kvrocks-url is empty. Pass redis://host:6666/0 for the Kvrocks server to seed")
		}
	case "mongo":
		if c.MongoURI == "" {
			return errors.New("-mongo-uri is empty. Pass mongodb://host:27017 for the server to seed")
		}
	case "":
		return errors.New("-engine is empty. Pass -engine hoarddb, mysql, postgres, mongo, redis or kvrocks (or -ddl to print the schemas without a server)")
	default:
		return fmt.Errorf("unknown -engine %q. Use hoarddb, mysql, postgres, mongo, redis or kvrocks", c.Engine)
	}
	return nil
}

// seedReport is the one JSON line a finished seed prints, one per layout.
type seedReport struct {
	Engine   string       `json:"engine"`
	Layout   string       `json:"layout"`
	Database string       `json:"database"`
	Scale    int          `json:"scale"`
	Seed     uint64       `json:"seed"`
	Counts   model.Counts `json:"counts"`
	Writes   int64        `json:"writes"`
	Seconds  float64      `json:"seed_s"`
	// Variant is the layout variant that was loaded. NativeFingerprint is the model's hash of
	// the embedded copies of a native load (model.Native.Fingerprint), empty for a common one
	// and for a layout with no embedded copy.
	Variant           string `json:"variant"`
	NativeFingerprint string `json:"native_fingerprint,omitempty"`
}

// seeder loads one generated layout into one engine and verifies the counts it wrote.
type seeder interface {
	// seed creates the layout's database, loads it, verifies the counts and returns how many
	// documents, elements or rows it wrote. It refuses a database that exists.
	seed(ctx context.Context, gen *model.Generator, database string, progress *progress) (int64, error)
	close()
}

func openSeeder(ctx context.Context, cfg config) (seeder, error) {
	switch cfg.Engine {
	case "hoarddb":
		return newHoardSeeder(ctx, cfg)
	case "mysql":
		return newMySQLSeeder(ctx, cfg)
	case "postgres":
		return newPostgresSeeder(ctx, cfg)
	case "redis":
		return newRedisSeeder(ctx, "Redis", cfg.RedisURL, "-redis-url", cfg)
	case "kvrocks":
		return newRedisSeeder(ctx, "Kvrocks", cfg.KvrocksURL, "-kvrocks-url", cfg)
	case "mongo":
		return newMongoSeeder(ctx, cfg)
	default:
		return nil, fmt.Errorf("engine %q has no seeder", cfg.Engine)
	}
}

func run(ctx context.Context, cfg config, stdout, stderr io.Writer) error {
	layouts, err := cfg.layouts()
	if err != nil {
		return err
	}
	variant, err := model.ParseVariant(cfg.Variant)
	if err != nil {
		return fmt.Errorf("-layout-variant: %w", err)
	}
	if cfg.DDL {
		for _, layout := range layouts {
			gen, err := model.NewWith(layout, cfg.Seed, cfg.Scale, variant.Options())
			if err != nil {
				return err
			}
			say(stdout, "%s\n", ddlText(gen, layoutDatabase(cfg.Database, layout), cfg.RF))
		}
		return nil
	}
	engine, err := openSeeder(ctx, cfg)
	if err != nil {
		return err
	}
	defer engine.close()
	for _, layout := range layouts {
		gen, err := model.NewWith(layout, cfg.Seed, cfg.Scale, variant.Options())
		if err != nil {
			return err
		}
		database := layoutDatabase(cfg.Database, layout)
		say(stderr, "generating %s at scale %d, seed %d: loading %s into %s\n", layout, cfg.Scale, cfg.Seed, cfg.Engine, database)
		started := time.Now()
		writes, err := engine.seed(ctx, gen, database, newProgress(stderr))
		if err != nil {
			return fmt.Errorf("seeding layout %s: %w", layout, err)
		}
		report := seedReport{Engine: cfg.Engine, Layout: string(layout), Database: database, Scale: cfg.Scale, Seed: cfg.Seed,
			Counts: gen.Counts(), Writes: writes, Seconds: time.Since(started).Seconds(), Variant: variant.String()}
		if variant.Native {
			report.NativeFingerprint = gen.Native().Fingerprint()
		}
		line, err := json.Marshal(report)
		if err != nil {
			return fmt.Errorf("encoding the report: %w", err)
		}
		say(stdout, "%s\n", line)
	}
	return nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "scenarios:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "scenarios: FAILED:", err)
		stop()
		os.Exit(1)
	}
}

// say writes a progress line. A failed write to a terminal or a closed pipe is not worth stopping a seed for.
func say(out io.Writer, format string, args ...any) { _, _ = fmt.Fprintf(out, format, args...) }
