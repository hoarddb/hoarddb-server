package config

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ListensOnLoopbackOnly reports whether a listen address binds loopback and
// so cannot be reached from another machine. A wildcard, an empty host or a
// name other than "localhost" is not loopback: a name may resolve to any
// interface, so only the literal loopback forms are trusted.
func ListensOnLoopbackOnly(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	parsed := net.ParseIP(host)
	return parsed != nil && parsed.IsLoopback()
}

// ListensOnWildcard reports whether a listen address binds every interface.
// The ring advertises such a node as 127.0.0.1 on the same port, which never
// equals the address its seed entry lists.
func ListensOnWildcard(address string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return false
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return true
	}
	return false
}

// firstRemoteSeed returns the first configured seed whose host is not
// loopback, or "" when every seed is on this machine. A seed is "host:port" or
// "id@host:port".
func firstRemoteSeed(seedNodes []string) string {
	for _, spec := range seedNodes {
		address := seedAddress(spec)
		if address != "" && !ListensOnLoopbackOnly(address) {
			return address
		}
	}
	return ""
}

// validateClusterReachability refuses a node that would tell peers on other
// machines to dial it on loopback. selfNode advertises Server.Listen as the
// node's ring address, and the internode port is derived from it, so a node
// left on the loopback default while its seeds are elsewhere cannot be reached
// by the peers it joins: every symptom is a timeout and none names the cause.
// A cluster whose seeds are all loopback is a cluster on one machine and is
// left alone.
func (c *Config) validateClusterReachability() error {
	remoteSeed := firstRemoteSeed(c.HashRing.SeedNodes)
	if remoteSeed == "" {
		return nil
	}
	if ListensOnLoopbackOnly(c.Server.Listen) {
		origin := "loopback only"
		if c.Server.Listen == DefaultConfig().Server.Listen {
			origin = "loopback only, the default"
		}
		return fmt.Errorf(`this node listens on %s (%s) but its seed %s is on another machine
       peers would be told to reach this node at %s, which only this machine can do
       set HOARDDB_LISTEN to this machine's real address, the one the other nodes dial, for example
         HOARDDB_LISTEN=192.0.2.1:4433
       and list that same address in HOARDDB_SEED_NODES on every node
       (listening on every interface (0.0.0.0) also works, but only together with HOARDDB_NODE_ID and
       id@address seeds such as node1@192.0.2.1:4433, or the node advertises 127.0.0.1 to its peers)
       to accept drivers from other machines as well, set HOARDDB_TCP_LISTEN=0.0.0.0:7433`,
			c.Server.Listen, origin, remoteSeed, c.Server.Listen)
	}
	if c.TCP.InternodeListen != "" && ListensOnLoopbackOnly(c.TCP.InternodeListen) {
		return fmt.Errorf(`the internode listener is %s (loopback only) but the seed %s is on another machine
       peers dial the internode port, so no other machine can reach this node
       set HOARDDB_TCP_INTERNODE_LISTEN to an address other nodes can reach (for example 0.0.0.0:4434), or unset it to derive it from HOARDDB_LISTEN`,
			c.TCP.InternodeListen, remoteSeed)
	}
	return nil
}

// MembershipFileName is the node's saved member set in its data directory.
// server/membership.go owns its format; config only checks that it exists.
const MembershipFileName = "membership.json"

// validateListenMatchesSeeds refuses the seed lists that give this node a
// second ring entry it never answers on (BUG-0319): a wildcard listener with
// a plain host:port seed on another machine, or with the hostname default id
// (neither can ever be matched to its own entry), and a concrete listener
// with a loopback seed on its own port. A wildcard node with an explicit
// HOARDDB_NODE_ID and only id@address seeds is accepted whether or not it is
// listed: that is a node being added with 'join'. A node with a saved member
// set is left to it, since that set, not the seeds, decides its membership.
func (c *Config) validateListenMatchesSeeds() error {
	if c.holdsSavedMembership() {
		return nil
	}
	listen := strings.TrimSpace(c.Server.Listen)
	// A listen address without a port names no seed entry; binding it fails
	// with its own error.
	_, listenPort, err := net.SplitHostPort(listen)
	if err != nil {
		return nil
	}
	if ListensOnWildcard(listen) {
		return c.validateWildcardSeeds(listen, net.JoinHostPort("127.0.0.1", listenPort))
	}
	if ListensOnLoopbackOnly(listen) {
		return nil
	}
	for _, spec := range c.HashRing.SeedNodes {
		address := seedAddress(spec)
		_, seedPort, err := net.SplitHostPort(address)
		if err != nil || seedPort != listenPort || !ListensOnLoopbackOnly(address) {
			continue
		}
		return fmt.Errorf(`HOARDDB_SEED_NODES lists %s, a loopback address on this node's own port, but this node listens on %s
       peers know this node as %s, so the loopback entry would be a second ring member for it that no peer can reach
       list this node in HOARDDB_SEED_NODES on every node as %s, the address it listens on, instead of %s`,
			address, listen, listen, listen, address)
	}
	return nil
}

// holdsSavedMembership reports whether the data directory holds a saved
// member set. A file that cannot be checked counts as present: the server
// reads it next and reports that failure itself.
func (c *Config) holdsSavedMembership() bool {
	_, err := os.Stat(filepath.Join(c.Storage.DataDir, MembershipFileName))
	return !errors.Is(err, fs.ErrNotExist)
}

// validateWildcardSeeds refuses a wildcard listener whose own seed entry can
// never be matched: a plain host:port seed on another machine (the node is
// advertised as 127.0.0.1, never that address), or a remote seed list with no
// explicit node id (the hostname default is never matched by id, BUG-0598).
func (c *Config) validateWildcardSeeds(listen, advertised string) error {
	explicitID := strings.TrimSpace(c.Server.NodeID)
	if c.Server.NodeIDDefaulted {
		explicitID = ""
	}
	entry := "<node-id>@<address>"
	if explicitID != "" {
		entry = explicitID + "@<address>"
	}
	fix := fmt.Sprintf(`set HOARDDB_NODE_ID and list every node as <node-id>@<address> in HOARDDB_SEED_NODES on every node, this one as %s, for example
         HOARDDB_NODE_ID=node1 HOARDDB_SEED_NODES=node1@192.0.2.1:4433,node2@192.0.2.2:4433
       or set HOARDDB_LISTEN to this machine's real address, the one HOARDDB_SEED_NODES lists for it`, entry)
	for _, spec := range c.HashRing.SeedNodes {
		spec = strings.TrimSpace(spec)
		if spec == "" || strings.Contains(spec, "@") || ListensOnLoopbackOnly(spec) {
			continue
		}
		return fmt.Errorf(`this node listens on %s (every interface), which peers are told is %s, and HOARDDB_SEED_NODES lists %s by address only
       an address-only seed can never be this node's own entry, so if it names this node the ring holds this node twice:
       at that address, and on loopback as a member no peer can reach
       %s`, listen, advertised, spec, fix)
	}
	remoteSeed := firstRemoteSeed(c.HashRing.SeedNodes)
	if explicitID == "" && remoteSeed != "" {
		return fmt.Errorf(`this node listens on %s (every interface) with seeds on other machines such as %s, and HOARDDB_NODE_ID is not set
       the host name default is never matched to a seed entry, so this node can never find its own entry and would join the ring
       a second time, on loopback (%s), as a member no peer can reach
       %s`, listen, remoteSeed, advertised, fix)
	}
	return nil
}

// seedAddress returns the address part of a "host:port" or "id@host:port" seed.
func seedAddress(spec string) string {
	spec = strings.TrimSpace(spec)
	if _, address, hasID := strings.Cut(spec, "@"); hasID {
		return strings.TrimSpace(address)
	}
	return spec
}

// validateAdvertise checks tcp.advertise, the address a cluster member publishes for drivers. The
// cluster applies a stricter rule when it commits the address (the same host as server.listen);
// this catches a malformed value at start-up, with the fix. The host is checked when the node
// starts in a cluster, against its cluster address.
func validateAdvertise(advertise string) error {
	if advertise == "" {
		return nil
	}
	host, port, err := net.SplitHostPort(advertise)
	number, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || host == "" || portErr != nil || number == 0 || strings.ContainsAny(advertise, "@/% \t\r\n\x00") {
		return fmt.Errorf(`tcp.advertise (HOARDDB_TCP_ADVERTISE) is %q, but it must be host:port with a port from 1 to 65535
       write the address drivers use to reach this node, for example db1.example:7433, with no scheme, user name or spaces
       or remove it to publish tcp.listen`, advertise)
	}
	return nil
}
