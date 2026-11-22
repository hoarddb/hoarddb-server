// Command ringseed loads the social data set into a running hoarddb-server in the
// entity-centric shape: one User bucket and one Post bucket, where every user and post is a
// document and its collections are store fields. See README.md.
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

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// seedConfig is what the flags say.
type seedConfig struct {
	Addr, User, Password, Fingerprint, Database, Dataset string
	Insecure                                             bool
	Workers, Connections                                 int
}

func parseFlags(args []string, stderr io.Writer) (seedConfig, error) {
	var cfg seedConfig
	flags := flag.NewFlagSet("ringseed", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.Addr, "addr", os.Getenv("HOARDDB_ADDR"), "host:port of the server's TCP listener (env HOARDDB_ADDR)")
	flags.StringVar(&cfg.User, "user", envOr("HOARDDB_ROOT_USER", "admin"), "bootstrap admin user (env HOARDDB_ROOT_USER)")
	flags.StringVar(&cfg.Password, "password", os.Getenv("HOARDDB_ROOT_PASSWORD"), "its password (env HOARDDB_ROOT_PASSWORD)")
	flags.StringVar(&cfg.Fingerprint, "fingerprint", "", "pin the server certificate to this fingerprint (the server prints it at start)")
	flags.BoolVar(&cfg.Insecure, "insecure", false, "skip certificate checks; for a server you started yourself on this machine, local development only")
	flags.StringVar(&cfg.Dataset, "dataset", "S-10k", "S-10k (10,000 users, 100,000 posts) or S-100k")
	flags.StringVar(&cfg.Database, "database", "social", "database to create and fill")
	flags.IntVar(&cfg.Workers, "workers", 32, "writes in flight")
	flags.IntVar(&cfg.Connections, "connections", 4, "connections the workers share")
	if err := flags.Parse(args); err != nil {
		return seedConfig{}, err
	}
	return cfg, cfg.check()
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func (c seedConfig) check() error {
	switch {
	case c.Addr == "":
		return errors.New("-addr is empty. Pass the host:port hoarddb-server listens on for drivers (or set HOARDDB_ADDR), for example -addr <host>:<port>")
	case c.Password == "":
		return errors.New("no password. Set HOARDDB_ROOT_PASSWORD to the bootstrap admin's password, or pass -password")
	case c.Fingerprint == "" && !c.Insecure:
		return errors.New("neither -fingerprint nor -insecure. Pass -fingerprint <value the server printed at start>, or -insecure for a server you started yourself on this machine")
	case c.Workers < 1 || c.Connections < 1:
		return errors.New("-workers and -connections must be 1 or more")
	}
	if _, err := ringload.LookupDataset(c.Dataset); err != nil {
		return err
	}
	if c.Dataset == "S-1M" {
		return errors.New("-dataset S-1M is not offered here; use S-10k or S-100k")
	}
	return nil
}

func run(ctx context.Context, cfg seedConfig, stdout, stderr io.Writer) error {
	spec, err := ringload.LookupDataset(cfg.Dataset)
	if err != nil {
		return err
	}
	options := ringclient.Options{Addr: cfg.Addr, User: cfg.User, Password: cfg.Password, Fingerprint: cfg.Fingerprint, Insecure: cfg.Insecure}
	clients := make([]*ringclient.Client, 0, cfg.Connections)
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	for range cfg.Connections {
		client, err := ringclient.Dial(ctx, options)
		if err != nil {
			return err
		}
		clients = append(clients, client)
	}
	say(stderr, "generating %s: %d users, %d posts\n", spec.Name, spec.Users, spec.Posts)
	stats, err := ringload.Seed(ctx, ringload.Options{Spec: spec, Database: cfg.Database, Clients: clients, Workers: cfg.Workers, Progress: stderr})
	if err != nil {
		return err
	}
	line, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("encoding the result: %w", err)
	}
	say(stdout, "%s\n", line)
	say(stderr, "done in %s: database %q holds %d users and %d posts in %d buckets\n",
		(time.Duration(stats.Seconds * float64(time.Second))).Round(time.Second), stats.Database, stats.Users, stats.Posts, stats.Buckets)
	return nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "ringseed:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "ringseed: FAILED:", err)
		stop()
		os.Exit(1)
	}
}

// say writes a progress line. A failed write to a terminal or a closed pipe is not worth stopping a seed for.
func say(out io.Writer, format string, args ...any) { _, _ = fmt.Fprintf(out, format, args...) }
