package main

// The "ring" adapter: hoardDB's entity-centric form, the four layouts exactly as the
// scenarios binary seeds them (a bucket per entity kind, the entity's own stores as fields),
// served through ringclient. The h1 form (one plain document per SQL row) is deferred.
//
// Connection (-uri and -param):
//
//	-uri host:port          the node's TCP listener (a leading hoarddb:// is accepted)
//	-param user=NAME        bootstrap admin; default env HOARDDB_ROOT_USER, then "admin"
//	-param password=TEXT    default env HOARDDB_ROOT_PASSWORD (prefer the env var, a flag shows in ps)
//	-param fingerprint=SHA256:...   pin the certificate the server printed at start
//	-param insecure=true    skip certificate checks, for a server started on this machine
//	-param multiget=on|off  read the documents a page names (social.feed's posts, chat.room_page's
//	                        members) with get-many frames instead of one Get each; default off
//	-param native=on|off    read the hoardDB-native layout: the data was seeded with scenarios
//	                        -layout-variant native, so a feed page is one StoreList (each User.Feed
//	                        element also holds the post's author and text) and a room page is two
//	                        (each Room.Members element also holds the member's name); default off.
//	                        Refused together with multiget=on. The layout is probed when a session
//	                        opens it: a server of the other variant is refused with the way out
//	-param aggregates=on|off  the ledger was seeded with its sum and count aggregates declared
//	                        (scenarios -layout-variant common+agg): the audit then also reads the
//	                        engine's maintained expense sum of every account written to and compares
//	                        it with the elements. The seed is probed when a session opens the ledger;
//	                        default off
//	-param batch=1          send the several writes of one operation (a post's upsert and pushes, a
//	                        group post, a chat message, any WriteOp with more than one document or
//	                        element) as ONE batch frame; default 0, one round trip per write
//
// One of fingerprint and insecure is required, as for the scenarios binary. Unknown keys are
// refused. Each virtual user owns one ringclient connection; requests on it are sequential.
//
// How the operations map (one round trip per sub-operation, never pipelined, never retried):
//
//   - A page is StoreList from the first element, so every page reads oldest first. A store is
//     read in full by paging 1000 at a time, one round trip per page.
//   - With multiget=on the documents of social.feed and chat.room_page are read with one get-many
//     call (the ringclient splits more than 255 ids into frames and sends them together), and
//     RoundTrips counts the frames: 2 for a feed page, 3 for a room page. A document that is gone
//     fails the operation, as a lone Get does.
//   - social.feed reads User[e].Feed and then one Post Get per element; social.followers reads
//     the entity's own User[e].Followers store; social.follow_scan lists Follow documents
//     filtered on To, which is O(documents) on this engine and is record-only.
//   - chat.room_page reads Messages (first page), Members in full, and a User Get per member.
//   - With batch=1 a write of several documents and elements goes as one ringclient batch frame
//     (up to 255 operations; writes only, so a PreRead or the claim of agent.task stays a separate
//     round trip, and a write of a single element stays a plain request). RoundTrips then counts
//     frames. A batch is not a transaction: the operations are applied or refused one by one, and
//     one refusal fails the write. ringclient may resend a whole frame once after a stale-header
//     refusal, as it does for a lone request; that stays inside the library, in both modes.
//   - A write applies Documents first (Upsert, so repeating a write is harmless), then Elements
//     in order: Push for a fifo or lifo store, Put by key for a hash or btree store. A write
//     that the owner does not acknowledge as "applied" is an error. PreRead stores are read in
//     full before the first write. Counters are ignored: the engine derives them.
//   - agent.task is Claim (count 1), Ack of the lease, then the replacement push. An empty queue
//     applies nothing and returns no elements.
//
// Questions the contract left open, settled by reading ringclient and the server's ring
// handler and by TestRingEmptyClaimAndReplacingWritesAreApplied on a real server: a Claim on an
// empty queue is a replicated write that reports "applied" with no elements, and a Put or Upsert
// over an existing key also reports "applied", so neither is an error and neither adds to the
// audit's count. ringclient itself re-describes the bucket and sends a request once more after
// a stale-header refusal (Bucket.keyed); that is inside the library and not visible here.
// Written tasks are claimed in arrival order, where the SQL and Mongo adapters order them by
// virtual user and then arrival (seq = SeqBase + ordinal).
//
// Settings reports the version and topology the node describes about itself. The driver cannot
// read the server's HOARDDB_* environment or its durability level, so Settings says so and the
// wrapper must record the server's environment in the facts file. The cipher is read with a
// separate handshake that sends no credentials.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

func init() { register("ring", openRing) }

// ringConfig is what the ring adapter reads from Config.
type ringConfig struct {
	options ringclient.Options
	// batch sends the several writes of one operation as one batch frame (-param batch=1).
	batch bool
	// multiget reads the documents of a page with one get-many frame (-param multiget=on).
	multiget bool
	// native reads the copies embedded in the elements of the native layout (-param native=on).
	native bool
	// aggregates says the ledger was seeded with its aggregates declared (-param aggregates=on).
	aggregates bool
}

// parseRingConfig reads -uri and -param, refusing an unknown key and a missing credential
// with the way out.
func parseRingConfig(cfg Config, getenv func(string) string) (ringConfig, error) {
	addr := strings.TrimPrefix(cfg.URI, "hoarddb://")
	if addr == "" {
		return ringConfig{}, fmt.Errorf("ring: -uri is empty: pass the host:port hoarddb-server listens on for drivers, for example -uri 127.0.0.1:7433")
	}
	options := ringclient.Options{Addr: addr, User: getenv("HOARDDB_ROOT_USER"), Password: getenv("HOARDDB_ROOT_PASSWORD")}
	if options.User == "" {
		options.User = "admin"
	}
	known := []string{"user", "password", "fingerprint", "insecure", "batch", "multiget", "native", "aggregates"}
	batch, multiget, native, aggregates := false, false, false, false
	for key, value := range cfg.Params {
		switch key {
		case "user":
			options.User = value
		case "password":
			options.Password = value
		case "fingerprint":
			options.Fingerprint = value
		case "insecure":
			insecure, err := strconv.ParseBool(value)
			if err != nil {
				return ringConfig{}, fmt.Errorf("ring: -param insecure=%q is not true or false: %w", value, err)
			}
			options.Insecure = insecure
		case "batch":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return ringConfig{}, fmt.Errorf("ring: -param batch=%q is not true or false (1 sends the several writes of one operation as one batch frame, 0 sends them one at a time): %w", value, err)
			}
			batch = parsed
		case "multiget":
			switch value {
			case "on":
				multiget = true
			case "off":
				multiget = false
			default:
				return ringConfig{}, fmt.Errorf("ring: -param multiget=%q is not on or off. Use on to read the documents of a page with one get-many frame, off to read them one at a time", value)
			}
		case "native":
			switch value {
			case "on":
				native = true
			case "off":
				native = false
			default:
				return ringConfig{}, fmt.Errorf("ring: -param native=%q is not on or off. Use on to read the copies embedded in User.Feed and Room.Members elements (the data must be seeded with -layout-variant native), off for the common layout", value)
			}
		case "aggregates":
			switch value {
			case "on":
				aggregates = true
			case "off":
				aggregates = false
			default:
				return ringConfig{}, fmt.Errorf("ring: -param aggregates=%q is not on or off. Use on when the data was seeded with -layout-variant common+agg (the ledger declares its sums and counts and the audit reads them back), off for the common layout", value)
			}
		default:
			return ringConfig{}, fmt.Errorf("ring: unknown -param %q. The ring adapter reads %s", key, strings.Join(known, ", "))
		}
	}
	switch {
	case native && multiget:
		return ringConfig{}, fmt.Errorf("ring: native=on already returns the documents a page names, so multiget=on would measure the same thing under another label. Use one of them")
	case options.Password == "":
		return ringConfig{}, fmt.Errorf("ring: no password. Set HOARDDB_ROOT_PASSWORD to the bootstrap admin's password, or pass -param password=<text>")
	case options.Fingerprint == "" && !options.Insecure:
		return ringConfig{}, fmt.Errorf("ring: neither fingerprint nor insecure. Pass -param fingerprint=<value the server printed at start>, or -param insecure=true for a server you started yourself on this machine")
	}
	return ringConfig{options: options, batch: batch, multiget: multiget, native: native, aggregates: aggregates}, nil
}

// ringEngine is one hoarddb-server node.
type ringEngine struct {
	options  ringclient.Options
	admin    *ringclient.Client
	batch    bool
	multiget bool
	native   bool
	// aggregates says the ledger was seeded with its aggregates declared (-param aggregates=on).
	aggregates bool
}

func openRing(ctx context.Context, cfg Config) (Engine, error) {
	parsed, err := parseRingConfig(cfg, os.Getenv)
	if err != nil {
		return nil, err
	}
	admin, err := ringclient.Dial(ctx, parsed.options)
	if err != nil {
		return nil, fmt.Errorf("connecting to hoarddb-server at %s: %w", parsed.options.Addr, err)
	}
	return &ringEngine{options: parsed.options, admin: admin, batch: parsed.batch, multiget: parsed.multiget, native: parsed.native, aggregates: parsed.aggregates}, nil
}

// Settings reads back what the node says about itself.
func (e *ringEngine) Settings(ctx context.Context) (map[string]string, error) {
	node, err := e.admin.DescribeNode(ctx)
	if err != nil {
		return nil, fmt.Errorf("describing the node at %s: %w", e.options.Addr, err)
	}
	trust := "certificate pinned by fingerprint"
	if e.options.Insecure {
		trust = "certificate not verified"
	}
	cipher, err := e.cipher(ctx)
	if err != nil {
		return nil, err
	}
	settings := map[string]string{
		"engine_version": node.Version,
		"tls":            cipher,
		"tls_trust":      trust,
		"layout":         "ring, entity-centric (stores are fields of the entity)",
		"batch":          ringBatchMode(e.batch),
		"multiget":       ringMultiGetMode(e.multiget),
		"native":         ringNativeMode(e.native),
		"aggregates":     ringAggregatesMode(e.aggregates),
		"node_id":        node.NodeID,
		"member_state":   node.MemberState,
		"members":        strconv.Itoa(int(node.Members)),
		"voters":         strconv.Itoa(int(node.Voters)),
		"default_rf":     strconv.Itoa(int(node.DefaultRF)),
		"serving":        node.Serving,
		"durability":     "server default; not readable over the driver, record the server's HOARDDB_* environment in the facts file",
	}
	if host := node.Host; host != nil {
		settings["host_cpus"] = strconv.Itoa(int(host.CPUs))
		settings["host_mem_total_bytes"] = strconv.FormatInt(host.MemTotal, 10)
		if host.Process != nil {
			settings["process_mem_limit_bytes"] = strconv.FormatInt(host.Process.Limit, 10)
		}
	}
	return settings, nil
}

// cipher handshakes once without credentials to read the negotiated suite. A failed probe is
// an error: a record must not carry a guessed TLS fact.
func (e *ringEngine) cipher(ctx context.Context) (string, error) {
	dialer := tls.Dialer{NetDialer: &net.Dialer{}, Config: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}} //nolint:gosec // reads the suite only; no credential is sent
	conn, err := dialer.DialContext(ctx, "tcp", e.options.Addr)
	if err != nil {
		return "", fmt.Errorf("probing the TLS cipher of %s: %w", e.options.Addr, err)
	}
	defer func() { _ = conn.Close() }() // the probe sent nothing worth flushing
	state := conn.(*tls.Conn).ConnectionState()
	return tls.VersionName(state.Version) + " " + tls.CipherSuiteName(state.CipherSuite), nil
}

// Session dials one connection for a virtual user.
func (e *ringEngine) Session(ctx context.Context, index int) (Session, error) {
	client, err := ringclient.Dial(ctx, e.options)
	if err != nil {
		return nil, fmt.Errorf("virtual user %d connecting to %s: %w", index, e.options.Addr, err)
	}
	return &ringSession{client: client, batch: e.batch, multiget: e.multiget, native: e.native, aggregates: e.aggregates}, nil
}

// Close closes the engine's own connection.
func (e *ringEngine) Close() error { return e.admin.Close() }

// ringSession is one virtual user's connection.
type ringSession struct {
	client   *ringclient.Client
	batch    bool
	multiget bool
	native   bool
	// aggregates is -param aggregates=on: the probe expects the ledger's declared aggregates.
	aggregates bool
}

// Layout returns the operations on one layout's database.
func (s *ringSession) Layout(layout model.Layout, database string) (LayoutOps, error) {
	if _, ok := schemaKinds[layout]; !ok {
		return nil, fmt.Errorf("ring: layout %s is not one of the benchmark layouts", layout)
	}
	reads := clientReads{client: s.client, database: database}
	probeCtx, cancel := context.WithTimeout(context.Background(), ringProbeTimeout)
	defer cancel()
	if err := probeVariant(probeCtx, reads, layout, s.native); err != nil {
		return nil, err
	}
	if err := probeAggregates(probeCtx, reads, layout, s.aggregates); err != nil {
		return nil, err
	}
	return &ringOps{client: s.client, reads: reads, layout: layout, database: database, batch: s.batch, multiget: s.multiget, native: s.native, aggregates: s.aggregates}, nil
}

// Close closes the connection.
func (s *ringSession) Close() error { return s.client.Close() }
