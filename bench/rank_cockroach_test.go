package main

import (
	"strings"
	"testing"
)

// TestRankCockroachStatements: the rank speaks the Postgres wire, so its
// statements carry numbered placeholders and an upsert the server accepts, and
// the leg is named after the sync policy the server reports.
func TestRankCockroachStatements(t *testing.T) {
	table := rankCockroachDatabase + ".rank_hash"
	for name, query := range map[string]string{
		"upsert": rankCockroachUpsert(table),
		"read":   rankCockroachRead(table),
		"scan":   rankCockroachScan(table),
	} {
		if strings.Contains(query, "?") || !strings.Contains(query, "$1") {
			t.Errorf("%s is %q; want numbered placeholders", name, query)
		}
	}
	if upsert := rankCockroachUpsert(table); !strings.Contains(upsert, "ON CONFLICT (id) DO UPDATE SET seq = excluded.seq, payload = excluded.payload") {
		t.Errorf("upsert is %q; want an ON CONFLICT (id) DO UPDATE", upsert)
	}
	if scan := rankCockroachScan(table); !strings.Contains(scan, "BETWEEN $1 AND $2 ORDER BY id") {
		t.Errorf("scan is %q", scan)
	}
}

// TestRankCockroachLegNames: a server with the log sync disabled is labelled so
// it can never pass for the durable comparison.
func TestRankCockroachLegNames(t *testing.T) {
	durable := rankCockroachLeg(rankLeg{}, "CockroachDB CCL v25.2.0", "false")
	if durable.Engine != "CockroachDB" || !strings.Contains(durable.Durability, "disable_synchronization_unsafe=false") {
		t.Errorf("durable leg = %+v", durable)
	}
	if unsafe := rankCockroachLeg(rankLeg{}, "v", "true"); unsafe.Engine != "CockroachDB no-fsync" {
		t.Errorf("unsafe leg named %q; want CockroachDB no-fsync", unsafe.Engine)
	}
}
