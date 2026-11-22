package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

// Engine names, as -engine spells them.
const (
	engineHoarddb   = "hoarddb"
	engineCockroach = "cockroach"
	engineMySQL     = "mysql"
	enginePostgres  = "postgres"
	engineMongo     = "mongo"
)

// Environment variables that carry the address and credentials, so none has
// to appear in a script, a shell history or a process listing.
const (
	envAddr        = "WIREMICRO_ADDR"
	envUser        = "WIREMICRO_USER"
	envPassword    = "WIREMICRO_PASSWORD"
	envDSN         = "WIREMICRO_DSN"
	envFingerprint = "WIREMICRO_HOARD_FINGERPRINT"
)

const (
	defaultOps     = 20000
	defaultWorkers = 16
	defaultPayload = 256
	defaultPreload = 1000
	// maxPayload keeps one document well under every engine's document limit.
	maxPayload = 1 << 20
)

// config is the validated command line.
type config struct {
	Engine string
	// Addr is host:port of the server under test.
	Addr string
	// DSN, when set, replaces the connection string the tool would build from
	// Addr, User and Password (mysql, postgres, cockroach and mongo only).
	DSN      string
	User     string
	Password string
	// TLS asks a competitor for an encrypted connection with the certificate
	// unchecked; hoardDB is always TLS.
	TLS bool
	// Name is the database name; hoardDB gets Name_<RunID> because it has no
	// drop, the others drop and recreate their table.
	Name  string
	RunID string

	Ops     int
	Workers int
	Payload int
	Preload int
	Smoke   bool
	Timeout time.Duration

	Out           string
	DatadirFSNote string

	// Phases lists the phases to run, in the order of allPhases; empty means all.
	Phases []string
	// Ledger is a file that receives one fsynced line per acknowledged insert
	// (hoardDB only); VerifyLedger reads such a file back and reports missing ids.
	Ledger       string
	VerifyLedger string
	// ledger is the open Ledger file, set by execute.
	ledger *ackLedger

	// HoardEnv is the file the hoardDB server was launched with.
	HoardEnv string
	// HoardTableSync is HOARDDB_BADGER_TABLE_SYNC as the operator states it.
	HoardTableSync string
	// HoardFingerprint pins the server certificate; empty trusts any, as the
	// other bench tools do against a self-signed node.
	HoardFingerprint string
}

// parseConfig reads and validates the command line. getenv supplies the
// environment so a test needs none.
func parseConfig(args []string, getenv func(string) string) (*config, error) {
	return parseConfigTo(args, getenv, io.Discard)
}

// parseConfigTo is parseConfig with the flag package's usage text going to out.
func parseConfigTo(args []string, getenv func(string) string, out io.Writer) (*config, error) {
	cfg := &config{}
	fs := flag.NewFlagSet("wiremicro", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&cfg.Engine, "engine", "", "hoarddb, cockroach, mysql, postgres or mongo (required)")
	fs.StringVar(&cfg.Addr, "addr", getenv(envAddr), "host:port of the server ($"+envAddr+")")
	fs.StringVar(&cfg.DSN, "dsn", getenv(envDSN), "full connection string for mysql, postgres, cockroach or mongo; replaces -addr/-user/-password ($"+envDSN+")")
	fs.StringVar(&cfg.User, "user", getenv(envUser), "user name ($"+envUser+"); defaults: hoarddb admin, mysql and cockroach root, postgres postgres")
	fs.StringVar(&cfg.Password, "password", getenv(envPassword), "password; prefer $"+envPassword+" so it stays out of the process list")
	fs.BoolVar(&cfg.TLS, "tls", false, "connect to a competitor over TLS with the certificate unchecked (hoardDB is always TLS)")
	fs.StringVar(&cfg.Name, "name", "wiremicro", "database name")
	fs.StringVar(&cfg.RunID, "run-id", time.Now().UTC().Format("20060102t150405"), "suffix that keeps a hoardDB database name unique per run")
	fs.IntVar(&cfg.Ops, "ops", defaultOps, "operations per phase (at least 20000 unless -smoke)")
	fs.IntVar(&cfg.Workers, "workers", defaultWorkers, "connections in the parallel phases")
	fs.IntVar(&cfg.Payload, "payload", defaultPayload, "payload field size in bytes")
	fs.IntVar(&cfg.Preload, "preload", defaultPreload, "documents preloaded for the get phases")
	fs.BoolVar(&cfg.Smoke, "smoke", false, "allow fewer than 20000 operations (a wiring check, never a result)")
	fs.DurationVar(&cfg.Timeout, "timeout", 30*time.Second, "per-operation timeout")
	phases := fs.String("phases", "", "comma list of phases to run: "+strings.Join(allPhases, ", ")+" (default all; the get phases preload first)")
	fs.StringVar(&cfg.Ledger, "ledger", "", "hoarddb: append every acknowledged insert id to this file, fsynced, on the client side")
	fs.StringVar(&cfg.VerifyLedger, "verify-ledger", "", "hoarddb: read back every id of this ledger file and report the missing ones; writes nothing")
	fs.StringVar(&cfg.Out, "out", "", "JSON result file (default wiremicro-<engine>-<run-id>.json)")
	fs.StringVar(&cfg.DatadirFSNote, "datadir-fs-note", "", "free text: filesystem and disk of the server's data directory, for example 'ext4 on virtio disk'")
	fs.StringVar(&cfg.HoardEnv, "hoard-env", "", "hoarddb: the KEY=VALUE file the server was launched with (read for HOARDDB_UNSAFE_ACK_BEFORE_SYNC)")
	fs.StringVar(&cfg.HoardTableSync, "hoard-table-sync", "", "hoarddb: HOARDDB_BADGER_TABLE_SYNC the server runs with, stepwise or msync; every hoardDB run states it")
	fs.StringVar(&cfg.HoardFingerprint, "hoard-fingerprint", getenv(envFingerprint), "hoarddb: pin the server certificate by SHA-256 fingerprint ($"+envFingerprint+")")
	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("reading flags: %w", err)
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q: every setting is a flag, see -h", fs.Arg(0))
	}
	var err error
	if cfg.Phases, err = parsePhases(*phases); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Out == "" {
		cfg.Out = fmt.Sprintf("wiremicro-%s-%s.json", cfg.Engine, cfg.RunID)
	}
	return cfg, nil
}

func (c *config) validate() error {
	switch c.Engine {
	case "":
		return errors.New("-engine is required: one of hoarddb, cockroach, mysql, postgres or mongo")
	case engineHoarddb, engineCockroach, engineMySQL, enginePostgres, engineMongo:
	default:
		return fmt.Errorf("-engine %q is not known: use hoarddb, cockroach, mysql, postgres or mongo", c.Engine)
	}
	if c.Addr == "" && c.DSN == "" {
		return fmt.Errorf("no server address: pass -addr host:port or set %s (or -dsn / %s for a competitor)", envAddr, envDSN)
	}
	if c.Engine == engineHoarddb && c.DSN != "" {
		return errors.New("-dsn does not apply to hoarddb: pass -addr")
	}
	if (c.Ledger != "" || c.VerifyLedger != "") && c.Engine != engineHoarddb {
		return errors.New("-ledger and -verify-ledger apply to -engine hoarddb only")
	}
	if c.Ledger != "" && c.VerifyLedger != "" {
		return errors.New("-ledger and -verify-ledger are two runs: pass one")
	}
	if c.Workers < 1 {
		return fmt.Errorf("-workers is %d: it must be at least 1", c.Workers)
	}
	if c.Ops < defaultOps && !c.Smoke {
		return fmt.Errorf("-ops is %d: a result needs at least %d operations per phase; add -smoke for a wiring check that is not a result", c.Ops, defaultOps)
	}
	if c.Ops < c.Workers {
		return fmt.Errorf("-ops is %d but -workers is %d: every worker needs at least one operation, so raise -ops or lower -workers", c.Ops, c.Workers)
	}
	if c.Payload < 1 || c.Payload > maxPayload {
		return fmt.Errorf("-payload is %d: it must be between 1 and %d bytes", c.Payload, maxPayload)
	}
	if c.Preload < 1 {
		return fmt.Errorf("-preload is %d: the get phases need at least one preloaded document", c.Preload)
	}
	if c.Engine == engineHoarddb {
		return c.validateHoarddb()
	}
	return nil
}

func (c *config) validateHoarddb() error {
	if c.HoardEnv == "" {
		return errors.New("-hoard-env is required for hoarddb: the server reports none of its durability settings, so name the file it was launched with (an empty file means all defaults)")
	}
	switch c.HoardTableSync {
	case "stepwise", "msync":
	case "":
		return errors.New("-hoard-table-sync is required for hoarddb: state HOARDDB_BADGER_TABLE_SYNC the server runs with, stepwise or msync (stepwise first)")
	default:
		return fmt.Errorf("-hoard-table-sync %q is not a mode: use stepwise or msync", c.HoardTableSync)
	}
	if c.Password == "" {
		return fmt.Errorf("no hoardDB password: set %s to the bootstrap user's password", envPassword)
	}
	if c.User == "" {
		c.User = "admin"
	}
	return nil
}

// describe is the one-line run description printed at start. It never
// includes the password or a connection string, which can hold one.
func (c *config) describe() string {
	parts := []string{
		"engine=" + c.Engine,
		fmt.Sprintf("ops=%d", c.Ops),
		fmt.Sprintf("workers=%d", c.Workers),
		fmt.Sprintf("payload=%dB", c.Payload),
		fmt.Sprintf("preload=%d", c.Preload),
	}
	if c.Smoke {
		parts = append(parts, "SMOKE (not a result)")
	}
	return strings.Join(parts, " ")
}

// allPhases are the phase names in run order.
var allPhases = []string{"insert_serial", "insert_parallel", "get_serial", "get_parallel"}

// parsePhases reads the -phases list; empty means every phase. The result is in
// run order whatever the order given.
func parsePhases(list string) ([]string, error) {
	if strings.TrimSpace(list) == "" {
		return nil, nil
	}
	wanted := map[string]bool{}
	for _, name := range strings.Split(list, ",") {
		name = strings.TrimSpace(name)
		if !slices.Contains(allPhases, name) {
			return nil, fmt.Errorf("-phases has %q, which is not a phase: use some of %s", name, strings.Join(allPhases, ", "))
		}
		wanted[name] = true
	}
	var ordered []string
	for _, name := range allPhases {
		if wanted[name] {
			ordered = append(ordered, name)
		}
	}
	return ordered, nil
}

// runs reports whether the phase is selected.
func (c *config) runs(phase string) bool {
	return len(c.Phases) == 0 || slices.Contains(c.Phases, phase)
}
