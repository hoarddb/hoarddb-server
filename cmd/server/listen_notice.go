package main

import (
	"fmt"
	"log/slog"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// internodeListenAddr is the address the internode listener binds: the
// explicit override, else the ring address with its port plus one.
func internodeListenAddr(cfg *config.Config) string {
	if cfg.TCP.InternodeListen != "" {
		return cfg.TCP.InternodeListen
	}
	derived, err := transport.InternodePeerAddr(cfg.Server.Listen)
	if err != nil {
		// The listener start reports the malformed address with its own
		// error; the start-up line only has to not hide it.
		return fmt.Sprintf("(not derivable from %q)", cfg.Server.Listen)
	}
	return derived
}

// startupListenAttrs are the log attributes that say where the node listens.
func startupListenAttrs(cfg *config.Config) []any {
	return []any{
		"client_listen", cfg.TCP.Listen,
		"internode_listen", internodeListenAddr(cfg),
		"ring_address", cfg.Server.Listen,
	}
}

// loopbackNotice explains how to open up a node that only this machine can
// reach, or returns "" when its client listener is already reachable. It is
// the start-up half of SEC-1: a remote client otherwise sees only
// "connection refused" from the network stack, which names nothing.
func loopbackNotice(cfg *config.Config) string {
	if !config.ListensOnLoopbackOnly(cfg.TCP.Listen) {
		return ""
	}
	return fmt.Sprintf("this node listens on this machine only (clients on %s). "+
		"To accept clients from other machines set HOARDDB_TCP_LISTEN=0.0.0.0:7433; "+
		"to join other machines in a cluster set HOARDDB_LISTEN to this node's address (for example 192.0.2.1:4433), which also moves the internode port. "+
		"See the security page of the user docs", cfg.TCP.Listen)
}

// logListenAddresses writes the start-up line for the node's listeners and,
// for a loopback-only node, how to open it up.
func logListenAddresses(logger *slog.Logger, cfg *config.Config) {
	logger.Info("listen addresses", startupListenAttrs(cfg)...)
	if notice := loopbackNotice(cfg); notice != "" {
		logger.Info(notice)
	}
}
