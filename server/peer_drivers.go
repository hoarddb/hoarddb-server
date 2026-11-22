package server

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
)

// Where a client reaches a cluster member (BUG-0709).
//
// A member's ring address is the identity its peers dial for membership; the
// internode listener is the next port, and nothing on it answers a client. A
// client connects to the member's driver listener (tcp.listen), which is local
// configuration the member set does not carry. Each node therefore says its own
// driver address on its own gossip record (Peer.Driver), and every node keeps
// what its peers said in a table of one short string per member.
//
// Like Peer.Store, the word is a node's own: a receiver never takes one
// node's word about another, and the table is memory only. A restarted node
// relearns every peer's address with its first gossip round, and until then
// says the address is not known rather than printing the ring address.
//
// Cost (AGENTS.md rule 11): one address per cluster member in memory and one
// per gossip message, none per bucket or store, no metric, and a membership
// change touches only the member it adds or removes.

// peerDriverTable is what each peer has said about where clients reach it.
type peerDriverTable struct {
	mu        sync.RWMutex
	addresses map[string]string
}

func newPeerDriverTable() *peerDriverTable {
	return &peerDriverTable{addresses: make(map[string]string)}
}

// record stores the address a peer gave for itself. An empty address clears
// the entry: a peer that stops saying it (an older binary after a downgrade)
// must not keep a stale one.
func (t *peerDriverTable) record(id, address string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if address == "" {
		delete(t.addresses, id)
		return
	}
	t.addresses[id] = address
}

// address returns the driver address a peer gave for itself, or "".
func (t *peerDriverTable) address(id string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.addresses[id]
}

// retain forgets every peer outside members, so the table is bounded by the
// member count however many nodes have come and gone.
func (t *peerDriverTable) retain(members []ring.Node) {
	keep := make(map[string]struct{}, len(members))
	for _, member := range members {
		keep[member.ID] = struct{}{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.addresses {
		if _, member := keep[id]; !member {
			delete(t.addresses, id)
		}
	}
}

// driverAddress is the address a client can connect to on this node: its
// tcp.listen, with a wildcard host (0.0.0.0, ::, or none) replaced by the host
// of ringAddr. The ring host is what the cluster already dials this node by,
// so it is the one name known to reach it; there is no separate advertise
// setting. A node on a wildcard whose ring address is also loopback (a
// one-machine cluster) is therefore shown on 127.0.0.1. It returns "" when
// tcp.listen is unset or malformed.
func (s *Server) driverAddress(ringAddr string) string {
	host, port, err := net.SplitHostPort(strings.TrimSpace(s.config.TCP.Listen))
	if err != nil || port == "" {
		return ""
	}
	if isWildcardHost(host) {
		host = "127.0.0.1"
		if ringHost, _, err := net.SplitHostPort(ringAddr); err == nil && !isWildcardHost(ringHost) {
			host = ringHost
		}
	}
	return net.JoinHostPort(host, port)
}

func isWildcardHost(host string) bool {
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return true
	}
	return false
}

// ownDriverAddress is the address clients use to reach this node.
func (s *Server) ownDriverAddress() string {
	if s.cluster != nil {
		return s.driverAddress(s.cluster.self.Addr)
	}
	return s.driverAddress(normalizeAddr(s.config.Server.Listen))
}

// clientAddressOfRemote decides what a node may tell its operator about where
// to reach another member: the driver address, or no address and the reason.
//
// A driver listener on loopback is the default (SEC-1). On a member whose ring
// host is not loopback, that address dials the asking machine itself, so it is
// withheld rather than printed as a command: printing it would be the dead end
// BUG-0709 is about, with a different number. The note names the fix.
func clientAddressOfRemote(driver, ringAddr string) (address, note string) {
	if driver == "" {
		return "", "its client address has not reached this node yet (retry in a few seconds), or tcp.listen is unset or malformed on that node (check it there)"
	}
	if config.ListensOnLoopbackOnly(driver) && !config.ListensOnLoopbackOnly(ringAddr) {
		_, port, err := net.SplitHostPort(driver)
		if err != nil || port == "" {
			port = "7433"
		}
		return "", fmt.Sprintf("it accepts clients on loopback only (%s), which no other machine can reach; "+
			"on that node set tcp.listen to 0.0.0.0:%s, or to an address clients can reach, and restart it", driver, port)
	}
	return driver, ""
}

// masterClientAddress is where an operator is told to reach the master, or no
// address and why. This node's own listener is trusted as configured: a client
// already connected to it needs no warning about loopback.
func (s *Server) masterClientAddress(master ring.Node) (address, note string) {
	if s.cluster == nil || !s.cluster.Enabled() || master.ID == s.cluster.self.ID {
		own := s.ownDriverAddress()
		if own == "" {
			return "", "tcp.listen is unset or malformed on this node; set it to host:port, for example 0.0.0.0:7433"
		}
		return own, ""
	}
	return clientAddressOfRemote(s.cluster.peerDrivers.address(master.ID), master.Addr)
}

// notMasterDirections is the master and what to do about it, for the
// not-master refusal.
func notMasterDirections(master ring.Node, address, note string) string {
	if address != "" {
		return fmt.Sprintf("master:    %s at %s\n       run the same command against %s, or use 'master' to find it", master.ID, address, address)
	}
	return fmt.Sprintf("master:    %s, but this node has no address to give for it: %s\n       run the same command against that node once it has a reachable address, or use 'master' to find it", master.ID, note)
}

// recordPeerDriver stores what a peer said about where clients reach it, on
// the same authenticated exchange as its store claim. This node's own entry is
// never stored: it is computed from its configuration.
func (c *Cluster) recordPeerDriver(peer ring.Node, address string) {
	if peer.ID == "" || peer.ID == c.self.ID {
		return
	}
	c.peerDrivers.record(peer.ID, address)
}
