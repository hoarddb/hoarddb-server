package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	serverconfig "github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
)

const inprocPassword = "bigasstest-native-test-password"

// startRingServer runs one in-process hoarddb-server on a free local port and returns its
// address, the way bench/scenarios' tests do. It sets the admin password in the environment, which
// both the ring adapter and the scenarios binary read.
func startRingServer(t *testing.T) string {
	t.Helper()
	t.Setenv("HOARDDB_ROOT_PASSWORD", inprocPassword)
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := serverconfig.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = addr
	service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SeedRootUser("admin", inprocPassword); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Stop() })
	return addr
}

func openInprocRing(t *testing.T, ctx context.Context, addr string, params map[string]string) Engine {
	t.Helper()
	withTrust := map[string]string{"insecure": "true"}
	for key, value := range params {
		withTrust[key] = value
	}
	engine, err := openEngine(ctx, Config{Engine: "ring", URI: addr, Params: withTrust, VUsers: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

// TestContractRingNative runs the shared contract against a real in-process server with the
// native layout seeded and -param native=on, and with the common layout and native=off. In both
// every read of every operation is verified and every write audited. Then it counts the round
// trips of a feed page and a room page: native is 1 and 2 whatever the room's size, so a silent
// fallback to the two-step read cannot pass.
func TestContractRingNative(t *testing.T) {
	addr := startRingServer(t)
	for _, native := range []string{"off", "on"} {
		t.Run("native="+native, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
			defer cancel()
			engine := openInprocRing(t, ctx, addr, map[string]string{"native": native})
			settings, err := engine.Settings(ctx)
			if err != nil || settings["native"] != native {
				t.Fatalf("Settings native = %q, %v; want %q", settings["native"], err, native)
			}
			variant := map[string]string{"off": variantCommon, "on": variantNative}[native]
			database := "natin" + native
			runContract(t, contractTarget{Engine: engine, Name: "ring", Database: database, Variant: variant, SeedArgs: []string{"-addr", addr, "-insecure"}})

			session, err := engine.Session(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = session.Close() }()
			world, err := newWorld(contractSeed, contractScale)
			if err != nil {
				t.Fatal(err)
			}
			social, err := session.Layout(model.Social, layoutDatabase(database, model.Social))
			if err != nil {
				t.Fatal(err)
			}
			chat, err := session.Layout(model.Chat, layoutDatabase(database, model.Chat))
			if err != nil {
				t.Fatal(err)
			}
			feed, err := social.Read(ctx, *world.readOp(model.Social, ReadSocialFeed, world.readOwners(ReadSocialFeed)[0]))
			if err != nil {
				t.Fatal(err)
			}
			wantFeed := 1 + len(feed.Documents)
			if native == "on" {
				wantFeed = 1
			}
			if feed.RoundTrips != wantFeed || len(feed.Documents) != world.readLimit(ReadSocialFeed) {
				t.Errorf("native=%s: a feed page took %d round trips and returned %d posts, want %d trips and %d posts", native, feed.RoundTrips, len(feed.Documents), wantFeed, world.readLimit(ReadSocialFeed))
			}
			for _, room := range world.readOwners(ReadChatRoomPage)[:3] {
				page, err := chat.Read(ctx, *world.readOp(model.Chat, ReadChatRoomPage, room))
				if err != nil {
					t.Fatal(err)
				}
				wantRoom := 2 + len(page.Documents)
				if native == "on" {
					wantRoom = 2
				}
				if page.RoundTrips != wantRoom || len(page.Documents) < 3 {
					t.Errorf("native=%s: room %s took %d round trips for %d members, want %d", native, room, page.RoundTrips, len(page.Documents), wantRoom)
				}
			}
		})
	}
}

// A native adapter on a common seed, and a common adapter on a native seed, are refused when the
// layout is opened: before any request is made, naming the way out.
func TestNativeProbeRefusesAMismatchedSeed(t *testing.T) {
	addr := startRingServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	for _, tc := range []struct{ seeded, native string }{{variantCommon, "on"}, {variantNative, "off"}} {
		t.Run(fmt.Sprintf("%s seed, native=%s", tc.seeded, tc.native), func(t *testing.T) {
			engine := openInprocRing(t, ctx, addr, map[string]string{"native": tc.native})
			database := "natprobe" + tc.seeded
			target := contractTarget{Engine: engine, Name: "ring", Database: database, Variant: tc.seeded, SeedArgs: []string{"-addr", addr, "-insecure"}}
			for _, layout := range []model.Layout{model.Social, model.Chat} {
				if err := target.seed(ctx, layout); err != nil {
					t.Fatal(err)
				}
			}
			session, err := engine.Session(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = session.Close() }()
			for _, layout := range []model.Layout{model.Social, model.Chat} {
				_, err := session.Layout(layout, layoutDatabase(database, layout))
				want := "this server holds the " + tc.seeded + " layout but -param native=" + tc.native + " was given"
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("%s: Layout error = %v, want one containing %q", layout, err, want)
				}
			}
		})
	}
}
