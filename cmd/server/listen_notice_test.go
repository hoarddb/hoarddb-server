package main

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestStartupListenAttrsNameEveryListener: the start-up line says where the
// node listens (SEC-1), because a client on another machine otherwise sees
// only "connection refused" from the network stack.
func TestStartupListenAttrsNameEveryListener(t *testing.T) {
	cfg := config.DefaultConfig()
	attrs := startupListenAttrs(cfg)
	got := map[string]string{}
	for index := 0; index+1 < len(attrs); index += 2 {
		got[attrs[index].(string)] = attrs[index+1].(string)
	}
	want := map[string]string{
		"client_listen":    "127.0.0.1:7433",
		"internode_listen": "127.0.0.1:4434",
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %q, want %q (attrs %v)", key, got[key], value, attrs)
		}
	}
}

func TestLoopbackNoticeNamesTheFix(t *testing.T) {
	notice := loopbackNotice(config.DefaultConfig())
	for _, want := range []string{"this machine only", "HOARDDB_TCP_LISTEN", "HOARDDB_LISTEN"} {
		if !strings.Contains(notice, want) {
			t.Errorf("notice %q does not mention %q", notice, want)
		}
	}
}

func TestNoLoopbackNoticeWhenClientsCanReachTheNode(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.TCP.Listen = "0.0.0.0:7433"
	if notice := loopbackNotice(cfg); notice != "" {
		t.Errorf("notice = %q for a node listening on every interface, want none", notice)
	}
}
