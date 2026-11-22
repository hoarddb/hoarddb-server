package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/redis/go-redis/v9"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

const (
	// redisUnitBatch is how many units one pipeline carries; redisScanPage is the COUNT hint of
	// the SCANs that verify and refuse.
	redisUnitBatch = 50
	redisScanPage  = 1000
	// redisCountBatch is how many owners one verification pipeline asks about.
	redisCountBatch = 500
)

// redisSeeder loads a layout as the application would keep it in Redis, one key per document and
// per store field. The same seeder serves Kvrocks, which speaks the same protocol.
type redisSeeder struct {
	client  *redis.Client
	workers int
	// label names the engine in messages: "Redis" or "Kvrocks".
	label string
}

// redisOptions parses a Redis URL and applies the TLS flag. With the flag on the connection is TLS
// with the certificate unchecked unless the URL says rediss://, which keeps the verifying
// configuration the URL parser builds; with the flag off a rediss:// URL is refused rather than
// silently downgraded.
func redisOptions(rawURL string, useTLS bool, urlFlag string, workers int) (*redis.Options, error) {
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", urlFlag, err)
	}
	switch {
	case useTLS && opts.TLSConfig == nil:
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} //nolint:gosec // self-signed certificates, the same rule as the hoardDB client (spec 3.1)
	case !useTLS && opts.TLSConfig != nil:
		return nil, fmt.Errorf("%s names rediss:// while -redis-tls is off; use redis:// in the URL, or drop -redis-tls=false", urlFlag)
	}
	opts.PoolSize = workers
	return opts, nil
}

func newRedisSeeder(ctx context.Context, label, rawURL, urlFlag string, cfg config) (*redisSeeder, error) {
	opts, err := redisOptions(rawURL, cfg.RedisTLS, urlFlag, cfg.Workers)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connecting to %s at %s (check %s and -redis-tls): %w", label, opts.Addr, urlFlag, err)
	}
	return &redisSeeder{client: client, workers: cfg.Workers, label: label}, nil
}

func (s *redisSeeder) close() { _ = s.client.Close() }

// redisSetStores are the stores kept as plain sets: both sides of a membership pair, which an
// application holds as two sets and a SQL schema holds as one row with a key on each side.
func redisSetStores(layout model.Layout) map[string]bool {
	sets := map[string]bool{}
	for store, holder := range pairedStores(layout) {
		sets[store] = true
		if strings.Contains(holder, ".") {
			sets[holder] = true
		}
	}
	return sets
}

// redisPlan turns units into Redis commands for one layout.
type redisPlan struct {
	database string
	// kinds maps "Bucket.Store" to its type word (fifoStore, hashStore, ...).
	kinds map[string]string
	sets  map[string]bool
}

func newRedisPlan(gen *model.Generator, database string) redisPlan {
	kinds := map[string]string{}
	for _, bucket := range gen.Schema() {
		for _, field := range bucket.Fields {
			if field.IsStore() {
				kinds[bucket.Name+"."+field.Name] = field.Type
			}
		}
	}
	return redisPlan{database: database, kinds: kinds, sets: redisSetStores(gen.Layout())}
}

// docKey is the hash of a document; storeKey is the key of one of its store fields.
func (p redisPlan) docKey(bucket, id string) string { return p.database + ":" + bucket + ":" + id }
func (p redisPlan) storeKey(bucket, id, store string) string {
	return p.docKey(bucket, id) + ":" + store
}

// bodyJSON renders the scalar fields of an element as one JSON object, in model order. Binary
// fields are left out: they go to a hash of their own, as raw bytes.
func bodyJSON(fields []model.Field) (string, error) {
	var out strings.Builder
	out.WriteByte('{')
	for _, field := range fields {
		if _, binary := field.Value.([]byte); binary {
			continue
		}
		name, err := json.Marshal(field.Name)
		if err != nil {
			return "", fmt.Errorf("encoding field name %q: %w", field.Name, err)
		}
		cell, err := json.Marshal(field.Value)
		if err != nil {
			return "", fmt.Errorf("encoding field %s: %w", field.Name, err)
		}
		if out.Len() > 1 {
			out.WriteByte(',')
		}
		out.Write(name)
		out.WriteByte(':')
		out.Write(cell)
	}
	out.WriteByte('}')
	return out.String(), nil
}

// command is one Redis command: the verb, then its arguments.
type command []any

// commandsOf returns the commands that write a unit: its document hash, then its elements in push order.
func (p redisPlan) commandsOf(unit model.Unit) ([]command, error) {
	doc := unit.Doc
	hash := command{"HSET", p.docKey(doc.Bucket, doc.ID)}
	for _, field := range append(append([]model.Field{}, doc.Fields...), doc.Counters...) {
		hash = append(hash, field.Name, field.Value)
	}
	commands := []command{hash}
	for _, element := range unit.Elements {
		more, err := p.elementCommands(element)
		if err != nil {
			return nil, err
		}
		commands = append(commands, more...)
	}
	return commands, nil
}

func (p redisPlan) elementCommands(element model.Element) ([]command, error) {
	key := p.storeKey(element.Bucket, element.Owner, element.Store)
	body, err := bodyJSON(element.Body)
	if err != nil {
		return nil, fmt.Errorf("%s of %s: %w", element.StoreName(), element.Owner, err)
	}
	var commands []command
	switch kind := p.kinds[element.StoreName()]; {
	case p.sets[element.StoreName()]:
		commands = append(commands, command{"SADD", key, element.Key})
	case kind == "fifoStore":
		commands = append(commands, command{"RPUSH", key, body})
	case kind == "lifoStore":
		commands = append(commands, command{"LPUSH", key, body})
	case kind == "hashStore":
		commands = append(commands, command{"HSET", key, element.Key, body})
	case kind == "btreeStore":
		commands = append(commands, command{"ZADD", key, 0, element.Key}, command{"HSET", key + ":bodies", element.Key, body})
	case kind == "heapStore":
		priority, err := heapPriority(element)
		if err != nil {
			return nil, err
		}
		commands = append(commands, command{"ZADD", key, priority, element.Key})
	default:
		return nil, fmt.Errorf("store %s has type %q, which the Redis seeder does not know", element.StoreName(), kind)
	}
	for _, field := range element.Body {
		raw, binary := field.Value.([]byte)
		if !binary {
			continue
		}
		if element.Key == "" {
			return nil, fmt.Errorf("%s of %s has a binary field %s but no element key to file it under", element.StoreName(), element.Owner, field.Name)
		}
		commands = append(commands, command{"HSET", key + ":" + field.Name, element.Key, raw})
	}
	return commands, nil
}

// heapPriority is the score of a heap element, taken from its priority field.
func heapPriority(element model.Element) (int64, error) {
	for _, field := range element.Body {
		if priority, ok := field.Value.(int64); ok && field.Name == "priority" {
			return priority, nil
		}
	}
	return 0, fmt.Errorf("%s of %s has no integer priority field to score it by", element.StoreName(), element.Owner)
}

// escapeGlob quotes the characters MATCH treats as a pattern.
func escapeGlob(literal string) string {
	return strings.NewReplacer(`\`, `\\`, "*", `\*`, "?", `\?`, "[", `\[`, "]", `\]`).Replace(literal)
}

// scanKeys calls visit for every key matching pattern; visit returns false to stop early.
func (s *redisSeeder) scanKeys(ctx context.Context, pattern string, visit func(string) bool) error {
	var cursor uint64
	for {
		keys, next, err := s.client.Scan(ctx, cursor, pattern, redisScanPage).Result()
		if err != nil {
			return fmt.Errorf("scanning %s for %q: %w", s.label, pattern, err)
		}
		for _, key := range keys {
			if !visit(key) {
				return nil
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// refuseExisting fails when any key lives under the layout's prefix.
func (s *redisSeeder) refuseExisting(ctx context.Context, database string) error {
	var found string
	if err := s.scanKeys(ctx, escapeGlob(database)+":*", func(key string) bool { found = key; return false }); err != nil {
		return err
	}
	if found != "" {
		return fmt.Errorf("keys under the prefix %q already exist on this server (for example %q), so it may already hold this layout and seeding again would mix with it. "+
			"Seed a fresh prefix with -database <name>, or delete the keys with: redis-cli --scan --pattern '%s:*' | xargs -n 500 redis-cli del", database, found, database)
	}
	return nil
}

// seed loads the units in pipelined batches and verifies the counts.
func (s *redisSeeder) seed(ctx context.Context, gen *model.Generator, database string, progress *progress) (int64, error) {
	if err := s.refuseExisting(ctx, database); err != nil {
		return 0, err
	}
	plan := newRedisPlan(gen, database)
	progress.phase("keys")
	var written atomic.Int64
	err := pool(ctx, s.workers, chunks(gen.Units(), redisUnitBatch), func(ctx context.Context, batch []model.Unit) error {
		pipe := s.client.Pipeline()
		var logical int64
		for _, unit := range batch {
			commands, err := plan.commandsOf(unit)
			if err != nil {
				return err
			}
			for _, one := range commands {
				pipe.Do(ctx, one...)
			}
			logical += 1 + int64(len(unit.Elements))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("writing %d units: %w", len(batch), err)
		}
		written.Add(logical)
		progress.add(logical)
		return nil
	}, progress)
	if err != nil {
		return 0, fmt.Errorf("writing %s: %w", gen.Layout(), err)
	}
	if err := s.verify(ctx, gen, plan); err != nil {
		return 0, err
	}
	return written.Load(), nil
}

// countVerb is the command that counts the members of a store's key.
func (p redisPlan) countVerb(store string) string {
	switch {
	case p.sets[store]:
		return "SCARD"
	case p.kinds[store] == "fifoStore" || p.kinds[store] == "lifoStore":
		return "LLEN"
	case p.kinds[store] == "hashStore":
		return "HLEN"
	}
	return "ZCARD"
}

// verify counts the document hashes of every bucket by SCAN and the members of every store
// field with HLEN, LLEN, ZCARD or SCARD, so a seed that lost writes does not report success.
func (s *redisSeeder) verify(ctx context.Context, gen *model.Generator, plan redisPlan) error {
	want := gen.Counts()
	for _, bucket := range gen.Schema() {
		prefix := plan.database + ":" + bucket.Name + ":"
		var documents int64
		err := s.scanKeys(ctx, escapeGlob(prefix)+"*", func(key string) bool {
			if !strings.Contains(key[len(prefix):], ":") { // a store key has a further colon
				documents++
			}
			return true
		})
		if err != nil {
			return err
		}
		if documents != want[bucket.Name] {
			return fmt.Errorf("%s holds %d %s documents under %q after the seed, want %d", s.label, documents, bucket.Name, prefix, want[bucket.Name])
		}
		for _, field := range bucket.Fields {
			if !field.IsStore() {
				continue
			}
			if err := s.verifyStore(ctx, gen, plan, bucket.Name, field.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// verifyStore sums the count of one store field over every owner and compares it with the model.
// A btreeStore also has its body hash counted, since the sorted set alone would not show a lost body.
func (s *redisSeeder) verifyStore(ctx context.Context, gen *model.Generator, plan redisPlan, bucket, store string) error {
	name := bucket + "." + store
	var owners []string
	for doc := range gen.Documents(bucket) {
		owners = append(owners, doc.ID)
	}
	var held, bodies int64
	for batch := range chunks(slices.Values(owners), redisCountBatch) {
		pipe := s.client.Pipeline()
		counts := make([]*redis.Cmd, len(batch))
		bodyCounts := make([]*redis.Cmd, len(batch))
		for i, owner := range batch {
			key := plan.storeKey(bucket, owner, store)
			counts[i] = pipe.Do(ctx, plan.countVerb(name), key)
			if plan.kinds[name] == "btreeStore" {
				bodyCounts[i] = pipe.Do(ctx, "HLEN", key+":bodies")
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("counting %s on %s: %w", name, s.label, err)
		}
		for i := range batch {
			n, err := counts[i].Int64()
			if err != nil {
				return fmt.Errorf("reading the count of %s: %w", name, err)
			}
			held += n
			if bodyCounts[i] != nil {
				n, err := bodyCounts[i].Int64()
				if err != nil {
					return fmt.Errorf("reading the body count of %s: %w", name, err)
				}
				bodies += n
			}
		}
	}
	if want := gen.Counts()[name]; held != want {
		return fmt.Errorf("store %s holds %d members after the seed on %s, want %d", name, held, s.label, want)
	}
	if plan.kinds[name] == "btreeStore" && bodies != held {
		return fmt.Errorf("store %s has %d keys but %d bodies on %s", name, held, bodies, s.label)
	}
	return nil
}
