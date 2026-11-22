package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The convergence check (spec P5). After a replicated leg every node must
// return the same sampled users within 120 s, and the catch-up time is
// recorded. The first node (the hoardDB entry node, the MongoDB primary, the
// MySQL source) is the reference: it took the writes, so the others are compared
// with what it returns. The writers' own bookkeeping lives in the process that
// ran the leg, which has ended, so the reference is the owner's answer rather
// than the counters; the exact check against those is the leg's own safety
// check and the RP-A exact forms (spec 6.6).

// nodeProbe returns the digest of the sampled users as node (an index) returns them.
type nodeProbe func(ctx context.Context, node int) (sampleDigest, error)

// convergeNode is one node's part of the result.
type convergeNode struct {
	Node string `json:"node"`
	Hash string `json:"hash"`
	// ConvergedAfterS is how long after the check began the node first returned
	// what the reference did; null while it never has.
	ConvergedAfterS *float64 `json:"converged_after_s"`
	// Error is why the node could not be read at the last comparison, empty when it answered.
	Error string `json:"error,omitempty"`
	// DifferingUsers names up to five sampled users the node still returned
	// differently when the check ended, for a node that never caught up.
	DifferingUsers []int `json:"differing_users,omitempty"`
}

// convergeResult is the one JSON line -mode converge prints.
type convergeResult struct {
	Mode            string         `json:"mode"`
	Engine          string         `json:"engine"`
	Dataset         string         `json:"dataset"`
	SampleUsers     int            `json:"sample_users"`
	Polls           int            `json:"polls"`
	ConvergeSeconds float64        `json:"converge_seconds"`
	TimeoutSeconds  float64        `json:"timeout_seconds"`
	Pass            bool           `json:"pass"`
	Nodes           []convergeNode `json:"nodes"`
}

// waitForConvergence compares every node with the first until all agree or the
// timeout passes, pausing poll between rounds. It returns the nodes' state either
// way; Pass says which.
func waitForConvergence(ctx context.Context, names []string, probe nodeProbe, poll, timeout time.Duration) (convergeResult, error) {
	started := time.Now()
	result := convergeResult{TimeoutSeconds: timeout.Seconds(), Nodes: make([]convergeNode, len(names))}
	for index, name := range names {
		result.Nodes[index].Node = name
	}
	for {
		result.Polls++
		digests := make([]sampleDigest, len(names))
		for node := range names {
			var err error
			digests[node], err = probe(ctx, node)
			result.Nodes[node].Hash, result.Nodes[node].Error = digests[node].hash, ""
			switch {
			case err == nil:
			case node == 0:
				return result, fmt.Errorf("reading the sample from the reference %s: %w", names[node], err)
			default:
				// A node still recovering may refuse reads. That is behind, not a
				// reason to stop: it has until the timeout to answer.
				result.Nodes[node].Error = err.Error()
			}
		}
		elapsed := time.Since(started)
		behind := false
		for node := 1; node < len(names); node++ {
			if result.Nodes[node].Error == "" && digests[node].hash == digests[0].hash {
				if result.Nodes[node].ConvergedAfterS == nil {
					seconds := elapsed.Seconds()
					result.Nodes[node].ConvergedAfterS = &seconds
				}
				result.Nodes[node].DifferingUsers = nil
				continue
			}
			behind = true
			result.Nodes[node].ConvergedAfterS = nil // it matched once and has fallen away again
			result.Nodes[node].DifferingUsers = nil
			if result.Nodes[node].Error == "" {
				result.Nodes[node].DifferingUsers = differingUsers(digests[0], digests[node], 5)
			}
		}
		if !behind {
			zero := 0.0
			result.Nodes[0].ConvergedAfterS = &zero
			result.ConvergeSeconds, result.Pass = elapsed.Seconds(), true
			return result, nil
		}
		if elapsed >= timeout {
			result.ConvergeSeconds = elapsed.Seconds()
			return result, nil
		}
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("waiting for the nodes to converge: %w", ctx.Err())
		case <-time.After(poll):
		}
	}
}

// failure names the nodes that never caught up.
func (r convergeResult) failure() string {
	var lagging []string
	for _, node := range r.Nodes[1:] {
		if node.ConvergedAfterS == nil {
			reason := fmt.Sprintf("users still different: %v", node.DifferingUsers)
			if node.Error != "" {
				reason = "cannot be read: " + node.Error
			}
			lagging = append(lagging, fmt.Sprintf("%s (%s)", node.Node, reason))
		}
	}
	return fmt.Sprintf("after %.0f s %d of %d nodes still return something other than %s: %s", r.ConvergeSeconds, len(lagging), len(r.Nodes)-1, r.Nodes[0].Node, strings.Join(lagging, "; "))
}

// convergeEngines opens one engine per node, the reference first. Each is told
// to talk to its node alone: reads that spread over the cluster would hide a
// node that is behind.
func convergeEngines(ctx context.Context, cfg *config) ([]socialEngine, func(), error) {
	nodeConfigs, err := nodeConfigs(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	var engines []socialEngine
	closeAll := func() {
		for _, eng := range engines {
			eng.close()
		}
	}
	for _, nodeConfig := range nodeConfigs {
		eng, err := newEngine(ctx, nodeConfig)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("opening a node for the convergence check: %w", err)
		}
		social, ok := eng.(socialEngine)
		if !ok {
			eng.close()
			closeAll()
			return nil, nil, fmt.Errorf("-engine %s does not implement the social operations", cfg.Engine)
		}
		engines = append(engines, social)
	}
	return engines, closeAll, nil
}

// nodeConfigs is cfg once per node, each pointing at that node only.
func nodeConfigs(ctx context.Context, cfg *config) ([]*config, error) {
	var configs []*config
	switch cfg.Engine {
	case engineHoardDB:
		for _, addr := range cfg.Addrs {
			node := *cfg
			node.Addr, node.Addrs, node.Routing = addr, []string{addr}, routingA
			configs = append(configs, &node)
		}
	case engineMongo:
		members, err := mongoMembers(ctx, cfg)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			node := *cfg
			node.MongoNode, node.MongoReadPref, node.Routing = member, "nearest", routingA
			configs = append(configs, &node)
		}
	case engineRing:
		return nil, errors.New("-mode converge is not built for -engine ring: the ring leg drives one node. Run -mode parity-hash against each node instead and compare sample_hash")
	case engineCockroach, enginePostgres:
		node := *cfg
		node.Routing = routingA
		configs = append(configs, &node)
	case engineMySQL:
		for _, dsn := range cfg.mysqlDSNs() {
			node := *cfg
			node.MySQLDSN, node.MySQLReadRouting, node.Routing = dsn, "primary", routingA
			configs = append(configs, &node)
		}
	}
	return configs, nil
}

// mongoMembers lists the data-bearing members of the replica set, the primary
// first, from the server's own hello answer: the URI may name them in any order
// and an arbiter holds no data to compare.
func mongoMembers(ctx context.Context, cfg *config) ([]string, error) {
	engine, err := newMongoEngine(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("asking MongoDB for its replica set members: %w", err)
	}
	defer engine.close()
	var hello struct {
		Primary string   `bson:"primary"`
		Hosts   []string `bson:"hosts"`
	}
	if err := engine.client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return nil, fmt.Errorf("running hello: %w", err)
	}
	if len(hello.Hosts) == 0 {
		return []string{hello.Primary}, nil // a standalone server names no set
	}
	members := []string{hello.Primary}
	for _, host := range hello.Hosts {
		if host != hello.Primary {
			members = append(members, host)
		}
	}
	return members, nil
}

// runConverge is -mode converge: it waits until every node returns the sampled
// users as the first does, prints the result, and fails when one never does.
func runConverge(ctx context.Context, cfg *config, out io.Writer) error {
	spec, err := lookupDataset(cfg.Dataset)
	if err != nil {
		return err
	}
	engines, closeAll, err := convergeEngines(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeAll()
	return convergeOn(ctx, cfg, spec, engines, out)
}

// convergeOn is runConverge for engines that are already open, the reference first.
func convergeOn(ctx context.Context, cfg *config, spec datasetSpec, engines []socialEngine, out io.Writer) error {
	ranks := sampleUsers(spec.Users, cfg.SampleUsers, datasetSeed)
	names := make([]string, len(engines))
	for index, eng := range engines {
		names[index] = eng.target()
	}
	probe := func(ctx context.Context, node int) (sampleDigest, error) {
		return readSample(ctx, engines[node], ranks, cfg.Conc)
	}
	result, err := waitForConvergence(ctx, names, probe, cfg.ConvergePoll, cfg.ConvergeTimeout)
	if err != nil {
		return err
	}
	result.Mode, result.Engine, result.Dataset, result.SampleUsers = modeConverge, cfg.Engine, spec.Name, len(ranks)
	line, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encoding the convergence result: %w", err)
	}
	if _, err := fmt.Fprintln(out, string(line)); err != nil {
		return fmt.Errorf("writing the convergence result: %w", err)
	}
	if !result.Pass {
		return fmt.Errorf("P5 convergence failed: %s", result.failure())
	}
	return nil
}
