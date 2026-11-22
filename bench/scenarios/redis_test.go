package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// TestRedisPlanWritesEveryDocumentAndElement needs no server: the commands built for a layout
// hold one document hash per document and one write per element, by the verb its store type uses.
func TestRedisPlanWritesEveryDocumentAndElement(t *testing.T) {
	for _, layout := range model.Layouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		plan := newRedisPlan(gen, "scn_"+string(layout))
		verbs := map[string]int64{}
		for unit := range gen.Units() {
			commands, err := plan.commandsOf(unit)
			if err != nil {
				t.Fatalf("%s: %v", layout, err)
			}
			for _, one := range commands {
				verbs[one[0].(string)]++
			}
		}
		var documents, elements, binary int64
		for name, count := range gen.Counts() {
			if strings.Contains(name, ".") {
				elements += count
			} else {
				documents += count
			}
		}
		if layout == model.Agent {
			binary = gen.Counts()["User.Memories"]
		}
		btree := gen.Counts()["Account.Expenses"] + gen.Counts()["Viewer.Playlists"]
		// ZADD and the bodies' HSET both write a btree element; a binary field adds one HSET.
		want := documents + elements + btree + binary
		var got int64
		for _, n := range verbs {
			got += n
		}
		if got != want {
			t.Errorf("%s: %d commands, want %d (documents %d, elements %d, btree bodies %d, vectors %d): %v", layout, got, want, documents, elements, btree, binary, verbs)
		}
	}
}

func TestRedisPlanKeysAndTypes(t *testing.T) {
	gen, err := model.New(model.Social, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	plan := newRedisPlan(gen, "db")
	for store, want := range map[string]string{
		"User.Feed": "LLEN", "User.Notifications": "LLEN", "User.Friends": "HLEN", "User.Settings": "HLEN",
		"User.Following": "SCARD", "User.Followers": "SCARD", "User.Likes": "SCARD", "Post.Likes": "SCARD", "Group.Members": "HLEN", "User.Drafts": "ZCARD",
	} {
		if got := plan.countVerb(store); got != want {
			t.Errorf("%s counts with %s, want %s", store, got, want)
		}
	}
	if got := plan.storeKey("User", "u1", "Feed"); got != "db:User:u1:Feed" {
		t.Errorf("store key %q", got)
	}
	commands, err := plan.elementCommands(model.Element{Bucket: "User", Owner: "u1", Store: "Notifications", Body: []model.Field{{Name: "kind", Value: "comment"}, {Name: "at", Value: int64(3)}}})
	if err != nil || fmt.Sprint(commands) != `[[LPUSH db:User:u1:Notifications {"kind":"comment","at":3}]]` {
		t.Errorf("a lifoStore element must be LPUSHed as JSON: %v %v", commands, err)
	}
	if _, err := plan.elementCommands(model.Element{Bucket: "User", Owner: "u1", Store: "Nothing"}); err == nil {
		t.Error("an undeclared store must be refused")
	}
	ledger, err := model.New(model.Ledger, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	commands, err = newRedisPlan(ledger, "db").elementCommands(model.Element{Bucket: "Account", Owner: "a1", Store: "Expenses", Key: "k1", Body: []model.Field{{Name: "amount", Value: int64(5)}}})
	if err != nil || fmt.Sprint(commands) != `[[ZADD db:Account:a1:Expenses 0 k1] [HSET db:Account:a1:Expenses:bodies k1 {"amount":5}]]` {
		t.Errorf("a btreeStore element is a score-0 member plus a body: %v %v", commands, err)
	}
}

func TestRedisTLSRules(t *testing.T) {
	on, err := redisOptions("redis://localhost:6379/0", true, "-redis-url", 4)
	if err != nil || on.TLSConfig == nil || !on.TLSConfig.InsecureSkipVerify || on.PoolSize != 4 {
		t.Fatalf("tls on must be unchecked TLS: %+v %v", on, err)
	}
	verified, err := redisOptions("rediss://localhost:6379/0", true, "-redis-url", 4)
	if err != nil || verified.TLSConfig == nil || verified.TLSConfig.InsecureSkipVerify {
		t.Fatalf("rediss:// keeps verification: %+v %v", verified, err)
	}
	off, err := redisOptions("redis://localhost:6379/2", false, "-redis-url", 4)
	if err != nil || off.TLSConfig != nil || off.DB != 2 {
		t.Fatalf("tls off must be plain text and keep the database number: %+v %v", off, err)
	}
	if _, err := redisOptions("rediss://localhost:6379/0", false, "-redis-url", 4); err == nil || !strings.Contains(err.Error(), "-redis-tls") {
		t.Fatalf("rediss:// against -redis-tls=false must be refused with the fix: %v", err)
	}
	if _, err := redisOptions("http://localhost", true, "-kvrocks-url", 4); err == nil || !strings.Contains(err.Error(), "-kvrocks-url") {
		t.Fatalf("a bad URL must name its flag: %v", err)
	}
}

func TestRedisDDLNamesEveryStoreFamily(t *testing.T) {
	for _, layout := range model.Layouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		text := redisDDL(gen, "scn_"+string(layout))
		for name := range gen.Counts() {
			bucket, store, isStore := strings.Cut(name, ".")
			want := "scn_" + string(layout) + ":" + bucket + ":<id>"
			if isStore {
				want += ":" + store
			}
			if !strings.Contains(text, want+" ") {
				t.Errorf("%s: no key family %s in\n%s", layout, want, text)
			}
		}
	}
	gen, err := model.New(model.Agent, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if text := redisDDL(gen, "x"); !strings.Contains(text, "raw 256-byte binary string") {
		t.Errorf("the vector family must say it is raw bytes:\n%s", text)
	}
}

func TestEscapeGlob(t *testing.T) {
	if got := escapeGlob(`a*b?c[d]\e`); got != `a\*b\?c\[d\]\\e` {
		t.Errorf("escapeGlob = %q", got)
	}
}

// seedRedisLike seeds every layout into the server at url and checks the refusal of a second
// seed, then deletes the keys it wrote.
func seedRedisLike(t *testing.T, engine, url string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	base := fmt.Sprintf("scn_%d", time.Now().UnixNano()%1_000_000_000)
	cfg := config{Engine: engine, Layout: layoutAll, Scale: 1, Seed: 1, Database: base, Workers: 4, RedisURL: url, KvrocksURL: url, RedisTLS: false}
	t.Cleanup(func() {
		opts, err := redisOptions(url, false, "-redis-url", 1)
		if err != nil {
			return
		}
		client := redis.NewClient(opts)
		defer func() { _ = client.Close() }()
		keys, _ := client.Keys(context.Background(), escapeGlob(base)+"_*").Result()
		for len(keys) > 0 {
			part := keys[:min(len(keys), 500)]
			keys = keys[len(part):]
			_ = client.Del(context.Background(), part...).Err()
		}
	})
	var stdout, stderr bytes.Buffer
	if err := run(ctx, cfg, &stdout, &stderr); err != nil {
		t.Fatalf("%v\n%s", err, stderr.String())
	}
	if lines := strings.Count(stdout.String(), "\n"); lines != len(model.Layouts) {
		t.Fatalf("want one JSON line per layout, got %d", lines)
	}
	again := run(ctx, cfg, &bytes.Buffer{}, &bytes.Buffer{})
	if again == nil || !strings.Contains(again.Error(), "-database") || !strings.Contains(again.Error(), "already exist") {
		t.Fatalf("a second seed must be refused with the way out: %v", again)
	}
}

// TestSeedRedis skips when REDIS_URL is unset.
func TestSeedRedis(t *testing.T) {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL is not set; set it to redis://host:6379/0 to run against a Redis or Valkey server")
	}
	seedRedisLike(t, "redis", url)
}

// TestSeedKvrocks skips when KVROCKS_URL is unset.
func TestSeedKvrocks(t *testing.T) {
	url := os.Getenv("KVROCKS_URL")
	if url == "" {
		t.Skip("KVROCKS_URL is not set; set it to redis://host:6666/0 to run against a Kvrocks server")
	}
	seedRedisLike(t, "kvrocks", url)
}

func TestKvrocksIsTheRedisSchemeOnAnotherPort(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "kvrocks"}, &bytes.Buffer{})
	if err != nil || cfg.KvrocksURL != "redis://localhost:6666/0" {
		t.Fatalf("kvrocks must default to its own port: %+v %v", cfg, err)
	}
	cfg.KvrocksURL = ""
	if err := cfg.check(); err == nil || !strings.Contains(err.Error(), "-kvrocks-url") {
		t.Fatalf("an empty URL must name the flag: %v", err)
	}
	var out bytes.Buffer
	ddl := config{Layout: "chat", Scale: 1, Seed: 1, Database: "scenarios", RF: 1, DDL: true}
	if err := run(context.Background(), ddl, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "-- Kvrocks\n-- Kvrocks is the Redis scheme above on RocksDB") {
		t.Errorf("the -ddl output must have a Kvrocks section:\n%s", out.String())
	}
}
