package main

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode"

	"go.mongodb.org/mongo-driver/mongo/options"
)

// Read routing (spec 3.5). RP-A sends every client connection to one member:
// hoardDB node 1, the MongoDB primary, the MySQL source. RP-B spreads reads
// over all members: hoardDB round-robins its connections over every node,
// MongoDB reads from the nearest member, and the MySQL engine keeps one pool
// per server and sends reads to them in turn. Writes go where the product
// sends them in both.
const (
	routingA = "a"
	routingB = "b"
)

// parseRouting accepts -routing a or b in any case.
func parseRouting(name string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case routingA:
		return routingA, nil
	case routingB:
		return routingB, nil
	default:
		return "", fmt.Errorf("-routing %q is not a or b: a sends every connection to one node (RP-A), b spreads reads over all of them (RP-B)", name)
	}
}

// routingLabel is the routing as the leg record names it.
func routingLabel(routing string) string {
	if routing == routingB {
		return "RP-B"
	}
	return "RP-A"
}

// parseAddrList splits a comma-separated host:port list. Every entry must name
// a port and none may repeat: a duplicate would silently give one node a
// double share of the connections and make RP-B unlike what the leg records.
func parseAddrList(list string) ([]string, error) {
	if strings.TrimSpace(list) == "" {
		return nil, errors.New("-addr is empty; pass host:port, or several separated by commas for a cluster, for example -addr node1:7433,node2:7433")
	}
	var addrs []string
	seen := map[string]bool{}
	for _, entry := range strings.Split(list, ",") {
		addr := strings.TrimFunc(entry, unicode.IsSpace)
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("-addr entry %q is not host:port (%v); write each node as host:port and separate nodes with commas", entry, err)
		}
		if seen[addr] {
			return nil, fmt.Errorf("-addr names %s twice; list each node once", addr)
		}
		seen[addr] = true
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// roundRobin picks which of n members serves the next connection or read.
// Under RP-A it always picks member 0; under RP-B it takes them in turn, so a
// leg spreads evenly whatever the worker count.
type roundRobin struct {
	members int
	spread  bool
	turn    atomic.Uint64
}

func newRoundRobin(members int, routing string) *roundRobin {
	return &roundRobin{members: members, spread: routing == routingB}
}

// pick is the index of the member the next connection or read goes to.
func (r *roundRobin) pick() int {
	if !r.spread || r.members < 2 {
		return 0
	}
	return int((r.turn.Add(1) - 1) % uint64(r.members))
}

// nodeCount is how many servers the run talks to: the nodes of -addr, the
// hosts of the MongoDB connection string or the servers of -mysql-dsn.
func (c *config) nodeCount() int {
	switch c.Engine {
	case engineMongo:
		return max(len(options.Client().ApplyURI(c.MongoURI).Hosts), 1)
	case engineMySQL:
		return len(c.mysqlDSNs())
	default:
		return max(len(c.Addrs), 1)
	}
}

// configByNodes names the configuration a bare node count implies. Five nodes
// are C5a or C5b and a MongoDB set with an arbiter has one member more than it
// has copies, so those are said with -config.
var configByNodes = map[int]string{1: "C1", 2: "C2", 3: "C3"}

// labelRecord writes what the run's flags say into the leg record, so a table
// never has to guess whether a figure was one node or five, read from the primary
// or from every member, over one bucket or sharded. The placement label is
// refused earlier for a layout the engines cannot build; here it is only named.
func labelRecord(record *legRecord, cfg *config, spec datasetSpec) error {
	placement := cfg.Placement
	if placement == "" {
		placement = "h1"
	}
	var err error
	if record.Placement, record.Shards, err = placementLabel(placement, cfg.Shards, spec.Users); err != nil {
		return err
	}
	if cfg.Engine == engineRing {
		// The ring layout is not H-1: a record that said "single" would group it with the
		// legacy one-bucket form, which holds the same data in a different structure.
		record.Placement, record.Shards = "entity-documents", 0
		record.EngineSettings["ring_serial"] = strconv.FormatBool(cfg.RingSerial)
		record.EngineSettings["ring_no_project"] = strconv.FormatBool(cfg.RingNoProject)
		record.EngineSettings["ring_fanout_parallel"] = strconv.FormatBool(cfg.RingFanoutParallel)
		if cfg.RingFanoutParallel {
			record.EngineSettings["ring_fanout_width"] = strconv.Itoa(cfg.RingFanoutWidth)
		}
		record.EngineSettings["ring_batch"] = strconv.FormatBool(cfg.RingBatch)
		if cfg.RingBatch {
			record.EngineSettings["ring_batch_size"] = strconv.Itoa(cfg.RingBatchSize)
		}
	}
	record.Nodes = cfg.nodeCount()
	record.Routing = routingLabel(cfg.Routing)
	record.Level = cfg.Level
	if cfg.WriteConcern != "" {
		record.WriteConcern = cfg.WriteConcern
	}
	record.Fanout = fanoutSingle
	if cfg.bulkFanout() {
		record.Fanout = fanoutBulk
	}
	record.Copies = cfg.Copies
	if record.Copies == 0 {
		record.Copies = record.Nodes
	}
	record.Config = cfg.Config
	if record.Config == "" {
		var known bool
		if record.Config, known = configByNodes[record.Nodes]; !known {
			return fmt.Errorf("%d nodes are C5a or C5b, or a set with an arbiter: say which with -config", record.Nodes)
		}
	}
	if cfg.Variant != "" {
		record.EngineSettings["variant"] = cfg.Variant
	}
	return nil
}
