package server

import (
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A host of real cluster members in one process (ring user store step M2b): full NewService
// nodes with their own data directories, pinned keys and loopback ports, which a test can stop
// and start again on the same directory and ports. The first id starts a new cluster; the others
// list it as their seed and wait to be joined.

// trioNode is one member: its configuration, its log, and the service while it runs.
type trioNode struct {
	id, listen, driver string
	cfg                *config.Config
	logs               *ringClusterBuffer
	// operator is what the node told its operator (cmd/server's stderr).
	operator *ringClusterBuffer
	qs       *Service
	// driverLn holds the driver port from newTrio until the node's first start, so nothing else on
	// the host can take it in between; build hands it to the service and clears it. A restart binds
	// the address again, as the node's own restart does.
	driverLn net.Listener
}

// trio is the host.
type trio struct {
	t     *testing.T
	nodes map[string]*trioNode
	ids   []string
	// budget, when set, is every node's proposal budget (the M2b.2 test option).
	budget time.Duration
}

// newTrio prepares the members named by ids (the first starts the cluster); edit, when set, changes
// a member's configuration last. Nothing is started.
func newTrio(t *testing.T, ids []string, edit func(id string, cfg *config.Config)) *trio {
	t.Helper()
	tr := &trio{t: t, nodes: map[string]*trioNode{}, ids: ids}
	fingerprints := map[string]string{}
	dirs := map[string]string{}
	for _, id := range ids {
		dirs[id] = t.TempDir()
		_, fingerprint, err := transport.MakeServerTLSConfig(filepath.Join(dirs[id], "keys"))
		if err != nil {
			t.Fatal(err)
		}
		fingerprints[id] = fingerprint
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		driver := listener.Addr().String()
		tr.nodes[id] = &trioNode{id: id, listen: ringClusterPorts(t), driver: driver, driverLn: listener, logs: &ringClusterBuffer{}, operator: &ringClusterBuffer{}}
	}
	first := ids[0]
	for _, id := range ids {
		node := tr.nodes[id]
		cfg := config.DefaultConfig()
		cfg.Storage.DataDir = dirs[id]
		cfg.Metrics.Enabled, cfg.Audit.Enabled = false, false
		cfg.Server.NodeID, cfg.Server.Listen, cfg.TCP.Listen = id, node.listen, node.driver
		cfg.Cluster.Enabled = true
		// Every member pins every other member's key, so any two can elect a leader without the
		// first.
		for _, other := range ids {
			if other != id {
				cfg.HashRing.SeedFingerprints = append(cfg.HashRing.SeedFingerprints, other+"="+fingerprints[other])
			}
		}
		if id != first {
			cfg.HashRing.SeedNodes = []string{first + "@" + tr.nodes[first].listen}
			if _, _, err := (CredentialKeys{}).InitClusterToken(cfg.Storage.DataDir); err != nil {
				t.Fatal(err)
			}
		}
		if edit != nil {
			edit(id, cfg)
		}
		node.cfg = cfg
	}
	t.Cleanup(func() {
		for _, id := range ids {
			if node := tr.nodes[id]; node.qs != nil {
				_ = node.qs.Stop()
				node.qs = nil
			}
			// A node that was never started still holds its driver port.
			if node := tr.nodes[id]; node.driverLn != nil {
				_ = node.driverLn.Close()
				node.driverLn = nil
			}
		}
	})
	return tr
}

// build makes id's service on its directory, given rootPassword as cmd/server gives
// HOARDDB_ROOT_PASSWORD ("" for none), without starting it.
func (tr *trio) build(id, rootPassword string) *Service {
	tr.t.Helper()
	node := tr.nodes[id]
	logger := slog.New(slog.NewTextHandler(node.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	qs, err := NewService(node.cfg, logger)
	if err != nil {
		tr.t.Fatalf("%s: NewService: %v", id, err)
	}
	qs.proposalBudget = tr.budget
	if node.driverLn != nil {
		qs.driverListener, node.driverLn = node.driverLn, nil
	}
	qs.SetOperatorOutput(node.operator)
	if err := qs.BootstrapUsersFrom("root", rootPassword, node.cfg.Auth.AuthBootstrapRoot, "HOARDDB_ROOT_PASSWORD"); err != nil {
		tr.t.Fatalf("%s: BootstrapUsersFrom: %v", id, err)
	}
	return qs
}

// start builds and starts each id at once (a member restarted with its peers down waits for them
// in Start), each with rootPassword.
func (tr *trio) start(rootPassword string, ids ...string) {
	tr.t.Helper()
	services := make([]*Service, len(ids))
	for i, id := range ids {
		services[i] = tr.build(id, rootPassword)
	}
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = services[i].Start()
		}()
	}
	wg.Wait()
	for i, id := range ids {
		if errs[i] != nil {
			tr.t.Fatalf("starting %s: %v", id, errs[i])
		}
		tr.nodes[id].qs = services[i]
	}
}

// stop stops each id.
func (tr *trio) stop(ids ...string) {
	tr.t.Helper()
	for _, id := range ids {
		node := tr.nodes[id]
		if node.qs == nil {
			continue
		}
		if err := node.qs.Stop(); err != nil {
			tr.t.Fatalf("stopping %s: %v", id, err)
		}
		node.qs = nil
	}
}

func (tr *trio) qs(id string) *Service { return tr.nodes[id].qs }

// login logs user in on id, failing the test unless it works within timeout.
func (tr *trio) login(id, user, password string, timeout time.Duration) *ringclient.Client {
	tr.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		client, err := tr.dial(id, user, password)
		if err == nil {
			tr.t.Cleanup(func() { _ = client.Close() })
			return client
		}
		if time.Now().After(deadline) {
			tr.t.Fatalf("%s could not log in on %s within %v: %v", user, id, timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// dial is one login attempt by a default client.
func (tr *trio) dial(id, user, password string) (*ringclient.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return ringclient.Dial(ctx, ringclient.Options{Addr: tr.nodes[id].driver, User: user, Password: password, Insecure: true})
}

// join joins each id (data members) through root's session and waits until each holds the
// cluster's password key and has installed what root's node has.
func (tr *trio) join(root *ringclient.Client, ids ...string) {
	tr.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, id := range ids {
		node := tr.nodes[id]
		if _, err := root.Join(ctx, id, node.listen, node.cfg.Cluster.Witness); err != nil {
			tr.t.Fatalf("join %s: %v", id, err)
		}
	}
	for _, id := range ids {
		if !tr.nodes[id].cfg.Cluster.Witness {
			tr.awaitKey(id, 30*time.Second)
		}
	}
}

// awaitKey waits until id holds the cluster's password key.
func (tr *trio) awaitKey(id string, timeout time.Duration) {
	tr.t.Helper()
	deadline := time.Now().Add(timeout)
	for tr.qs(id).ringNode.PasswordKeyStatus() != wire.PasswordKeyHeld {
		if time.Now().After(deadline) {
			tr.t.Fatalf("%s does not hold the cluster's password key after %v (status %q)", id, timeout, tr.qs(id).ringNode.PasswordKeyStatus())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// awaitInstalled waits until id's follower has installed the users up to stateSeq: a login there
// then sees every change up to it (spec 6.4: the tests wait on the watermark, never a sleep).
func (tr *trio) awaitInstalled(id string, stateSeq int64) {
	tr.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := tr.qs(id).users.awaitInstalled(ctx, stateSeq); err != nil {
		tr.t.Fatalf("%s: %v", id, err)
	}
}

// leader is the id every running member names as the leader, waiting up to timeout for one.
func (tr *trio) leader(timeout time.Duration) string {
	tr.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, id := range tr.ids {
			if qs := tr.qs(id); qs != nil && qs.ringNode.IsLeader() {
				return id
			}
		}
		if time.Now().After(deadline) {
			tr.t.Fatalf("no member leads after %v", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
