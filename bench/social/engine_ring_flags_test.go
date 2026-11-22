package main

import (
	"strconv"
	"strings"
	"testing"
)

func TestRingEngineFlagsParseAndRecordTheRingSettings(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "ring", "-addr", "node-a:7433", "-ring-serial", "-ring-fingerprint", "0a1b"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != engineRing || cfg.Addr != "node-a:7433" || len(cfg.Addrs) != 1 || !cfg.RingSerial || cfg.RingFingerprint != "0a1b" {
		t.Errorf("ring flags parsed to %+v", cfg)
	}
	engine, err := newEngine(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, isRing := engine.(*ringEngine); !isRing || engine.target() != "node-a:7433" {
		t.Errorf("newEngine(ring) = %T, target %q", engine, engine.target())
	}
	options := engine.(*ringEngine).ringOptions()
	if options.Fingerprint != "0a1b" || options.Insecure || options.User != ringUser {
		t.Errorf("a pinned run must not also skip the certificate check: %+v", options)
	}
	unpinned := newRingEngine(&config{Addr: "node-a:7433", Password: "pw"}).ringOptions()
	if !unpinned.Insecure || unpinned.Fingerprint != "" {
		t.Errorf("without -ring-fingerprint the certificate is unchecked, as for the other hoardDB engine: %+v", unpinned)
	}
}

func TestRingFlagsAreDroppedOnOtherEngines(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "mysql", "-ring-serial", "-ring-fingerprint", "0a1b"})
	if err != nil || cfg.RingSerial || cfg.RingFingerprint != "" {
		t.Errorf("a ring flag leaked into a %s run: %+v %v", cfg.Engine, cfg, err)
	}
}

// TestRingEngineRefusesWhatItCannotDoAndNamesTheFix: each refusal names the flag to use instead.
func TestRingEngineRefusesWhatItCannotDoAndNamesTheFix(t *testing.T) {
	for name, test := range map[string]struct {
		args []string
		want string
	}{
		"two nodes":     {[]string{"-engine", "ring", "-addr", "a:1,b:1"}, "drives one node"},
		"routing b":     {[]string{"-engine", "ring", "-routing", "b"}, "Drop -routing b"},
		"bulk fanout":   {[]string{"-engine", "ring", "-fanout", "bulk"}, "one request per follower"},
		"batch":         {[]string{"-engine", "ring", "-hoard-batch"}, "-engine hoarddb"},
		"concern":       {[]string{"-engine", "ring", "-write-concern", "one"}, "Ring leg"},
		"placement h64": {[]string{"-engine", "ring", "-placement", "h64"}, "-placement h1"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseFlags(test.args); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Errorf("parseFlags(%v) = %v, want a refusal containing %q", test.args, err, test.want)
			}
		})
	}
}

func TestRingEngineRefusesTheTimelineWorkloadAndConvergeWithTheFix(t *testing.T) {
	engine := newRingEngine(&config{Engine: engineRing})
	if _, err := engine.setup(t.Context()); err == nil || !strings.Contains(err.Error(), "-mix") {
		t.Errorf("the timeline workload's setup must name -mix: %v", err)
	}
	if _, err := engine.newSession(); err == nil || !strings.Contains(err.Error(), "-mix") {
		t.Errorf("the timeline workload's session must name -mix: %v", err)
	}
	if _, err := nodeConfigs(t.Context(), &config{Engine: engineRing}); err == nil || !strings.Contains(err.Error(), "parity-hash") {
		t.Errorf("-mode converge on the ring must name parity-hash: %v", err)
	}
}

func TestRingLegIsLabelledAsItsOwnPlacement(t *testing.T) {
	cfg := &config{Engine: engineRing, Addrs: []string{"a:1"}, Placement: "h1", RingSerial: true, Routing: routingA}
	record := newLegRecord()
	if err := labelRecord(record, cfg, tinySpec); err != nil {
		t.Fatal(err)
	}
	if record.Placement != "entity-documents" || record.Shards != 0 || record.EngineSettings["ring_serial"] != "true" {
		t.Errorf("record placement %q shards %d ring_serial %q", record.Placement, record.Shards, record.EngineSettings["ring_serial"])
	}
}

// TestRingNoProjectFlagIsParsedRecordedAndDroppedElsewhere: the default projects; the flag turns
// whole-document reads back on and the record says which the leg did.
func TestRingNoProjectFlagIsParsedRecordedAndDroppedElsewhere(t *testing.T) {
	for _, test := range []struct {
		args []string
		want bool
	}{{[]string{"-engine", "ring"}, false}, {[]string{"-engine", "ring", "-ring-no-project"}, true}} {
		cfg, err := parseFlags(test.args)
		if err != nil || cfg.RingNoProject != test.want {
			t.Fatalf("%v parsed to RingNoProject=%v %v, want %v", test.args, cfg.RingNoProject, err, test.want)
		}
		record := &legRecord{EngineSettings: map[string]string{}}
		if err := labelRecord(record, cfg, datasetSpec{Users: 10}); err != nil {
			t.Fatal(err)
		}
		if record.EngineSettings["ring_no_project"] != strconv.FormatBool(test.want) {
			t.Errorf("%v recorded ring_no_project=%q", test.args, record.EngineSettings["ring_no_project"])
		}
		engine, _ := ringModelEngine(cfg, newRingModel())
		built, err := engine.newSocialOps()
		if err != nil || built.(*ringOps).wholeDocuments != test.want {
			t.Errorf("%v built ops %+v %v, want wholeDocuments=%v", test.args, built, err, test.want)
		}
	}
	if cfg, err := parseFlags([]string{"-engine", "mysql", "-ring-no-project"}); err != nil || cfg.RingNoProject {
		t.Errorf("-ring-no-project leaked into a %s run: %+v %v", cfg.Engine, cfg, err)
	}
}
