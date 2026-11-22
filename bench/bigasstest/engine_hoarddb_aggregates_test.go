package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// fakeDescriber answers the aggregates of one bucket, as DescribeBucket does.
type fakeDescriber struct {
	infos []ringclient.AggregateInfo
	err   error
	calls []string
}

func (d *fakeDescriber) Aggregates(_ context.Context, bucket string) ([]ringclient.AggregateInfo, error) {
	d.calls = append(d.calls, bucket)
	return d.infos, d.err
}

func ledgerAggregateInfos(state string) []ringclient.AggregateInfo {
	var infos []ringclient.AggregateInfo
	for store, list := range model.LedgerAggregates() {
		for _, decl := range list {
			infos = append(infos, ringclient.AggregateInfo{Store: strings.TrimPrefix(store, "Account."), Kind: decl.Kind, Field: decl.Field, By: decl.By, State: state})
		}
	}
	return infos
}

func TestRingConfigReadsTheAggregatesParam(t *testing.T) {
	env := envOf(map[string]string{"HOARDDB_ROOT_PASSWORD": "secret"})
	for param, want := range map[string]bool{"on": true, "off": false} {
		got, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "aggregates": param}}, env)
		if err != nil || got.aggregates != want {
			t.Errorf("aggregates=%s: %+v, %v, want %v", param, got, err, want)
		}
	}
	got, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true"}}, env)
	if err != nil || got.aggregates {
		t.Errorf("the default must be off: %+v, %v", got, err)
	}
	_, err = parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "aggregates": "yes"}}, env)
	if err == nil || !strings.Contains(err.Error(), `aggregates="yes"`) || !strings.Contains(err.Error(), "common+agg") {
		t.Errorf("a bad word must be refused naming the seed variant, got %v", err)
	}
}

func TestRingModeWordForAggregates(t *testing.T) {
	if ringAggregatesMode(true) != "on" || ringAggregatesMode(false) != "off" {
		t.Error("the mode words are on and off")
	}
}

// The open-time probe: the Account bucket must declare the model's aggregates, all ready, exactly
// when -param aggregates=on, so a run on the wrong seed is refused before any request is made.
func TestProbeAggregates(t *testing.T) {
	cases := map[string]struct {
		layout    model.Layout
		infos     []ringclient.AggregateInfo
		err       error
		aggregate bool
		want      string
	}{
		"declared and on":            {model.Ledger, ledgerAggregateInfos("ready"), nil, true, ""},
		"none and off":               {model.Ledger, nil, nil, false, ""},
		"none but on":                {model.Ledger, nil, nil, true, "declares 0 aggregates but -param aggregates=on was given: this server holds the common ledger. Reseed with -layout-variant common+agg"},
		"declared but off":           {model.Ledger, ledgerAggregateInfos("ready"), nil, false, "declares 4 aggregates but -param aggregates=off was given: this server holds the common+agg ledger. Reseed with -layout-variant common"},
		"still building":             {model.Ledger, ledgerAggregateInfos("building"), nil, true, "is building"},
		"describe failed":            {model.Ledger, nil, errors.New("boom"), true, "boom"},
		"social has none, on":        {model.Social, nil, nil, true, ""},
		"social has none, off":       {model.Social, nil, nil, false, ""},
		"ledger partly declared, on": {model.Ledger, ledgerAggregateInfos("ready")[:2], nil, true, "declares 2 aggregates"},
	}
	for name, tc := range cases {
		describer := &fakeDescriber{infos: tc.infos, err: tc.err}
		err := probeAggregates(context.Background(), describer, tc.layout, tc.aggregate)
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: got %v, want %q", name, err, tc.want)
		}
		if tc.layout != model.Ledger && len(describer.calls) != 0 {
			t.Errorf("%s: a layout with no aggregates must not be described, calls %v", name, describer.calls)
		}
	}
}

func aggregatesRun(t *testing.T, only string, mutate func(*fakeEngine)) *Record {
	t.Helper()
	p, _ := fakeRun(t, func(f *fakeEngine) {
		f.aggregates = true
		if mutate != nil {
			mutate(f)
		}
	})
	mix, err := newMix(benchLayouts, only)
	if err != nil {
		t.Fatal(err)
	}
	p.Mix = mix
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestRunnerPassesTheMixOnTheAggregatesFake(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.aggregates = true })
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Failed || !rec.AuditOK {
		t.Fatalf("failed=%v audit=%q verify=%d errors=%d", rec.Failed, rec.AuditError, rec.VerifyFailures, rec.Errors)
	}
	if rec.Aggregates != "on" || rec.Settings["aggregates"] != "on" {
		t.Fatalf("record aggregates %q, settings %v", rec.Aggregates, rec.Settings)
	}
	if !strings.Contains(rec.legName(), "-agg") {
		t.Errorf("the leg name %q lacks -agg", rec.legName())
	}
}

// A maintained sum that lost a write passes every read (no mix read asks for it) and is caught
// only by the audit.
func TestAuditCatchesALostMaintainedSum(t *testing.T) {
	rec := aggregatesRun(t, string(WriteLedgerExpense), func(f *fakeEngine) { f.corruptSum = true })
	if rec.Overall.Requests == 0 {
		t.Fatal("no expense was written")
	}
	if rec.VerifyFailures != 0 {
		t.Fatalf("the reads should not see the maintained sum, got %d verify failures", rec.VerifyFailures)
	}
	if rec.AuditOK || !rec.Failed || !strings.Contains(rec.AuditError, "maintained sum") {
		t.Fatalf("audit ok=%v failed=%v error=%q, want a failed audit naming the maintained sum", rec.AuditOK, rec.Failed, rec.AuditError)
	}
}

func TestAuditPassesAMaintainedSum(t *testing.T) {
	if rec := aggregatesRun(t, string(WriteLedgerExpense), nil); !rec.AuditOK || rec.Failed {
		t.Errorf("audit ok=%v failed=%v error=%q", rec.AuditOK, rec.Failed, rec.AuditError)
	}
}
