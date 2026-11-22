package main

import (
	"strings"
	"testing"
)

func TestRankPostgresStatements(t *testing.T) {
	table := rankPostgresSchema + ".rank_hash"
	for name, query := range map[string]string{
		"upsert": rankPostgresUpsert(table),
		"read":   rankPostgresRead(table),
		"scan":   rankPostgresScan(table),
	} {
		if strings.Contains(query, "?") || !strings.Contains(query, "$1") {
			t.Errorf("%s is %q; want numbered placeholders", name, query)
		}
	}
	upsert := rankPostgresUpsert(table)
	if !strings.Contains(upsert, "ON CONFLICT (id) DO UPDATE SET seq = excluded.seq, payload = excluded.payload") ||
		!strings.Contains(upsert, "RETURNING (xmax = 0)") {
		t.Errorf("upsert is %q; want ON CONFLICT and a created flag", upsert)
	}
	if scan := rankPostgresScan(table); !strings.Contains(scan, "BETWEEN $1 AND $2 ORDER BY id") {
		t.Errorf("scan is %q", scan)
	}
}

func TestRankPostgresLegNames(t *testing.T) {
	durable := rankPostgresLeg(rankLeg{}, "17.2", "on", "on")
	if durable.Engine != "PostgreSQL" || !strings.Contains(durable.Durability, "synchronous_commit=on") {
		t.Errorf("durable leg = %+v", durable)
	}
	for _, relaxed := range [][2]string{{"off", "on"}, {"on", "off"}} {
		if leg := rankPostgresLeg(rankLeg{}, "17", relaxed[0], relaxed[1]); leg.Engine != "PostgreSQL no-fsync" {
			t.Errorf("%v named %q; want PostgreSQL no-fsync", relaxed, leg.Engine)
		}
	}
}

func TestRankSummaryKnowsPostgres(t *testing.T) {
	columns := rankColumns(map[string]bool{"PostgreSQL": true, "MySQL": true, "PostgreSQL no-fsync": true})
	want := []string{"MySQL", "PostgreSQL no-fsync", "PostgreSQL"}
	if strings.Join(columns, ",") != strings.Join(want, ",") {
		t.Errorf("columns %v, want %v", columns, want)
	}
}
