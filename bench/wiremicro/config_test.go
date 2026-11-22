package main

import (
	"strings"
	"testing"
)

func envOf(pairs map[string]string) func(string) string {
	return func(name string) string { return pairs[name] }
}

func TestDefaultsMatchTheBrief(t *testing.T) {
	cfg, err := parseConfig([]string{"-engine", "mysql", "-addr", "db:3306"}, envOf(map[string]string{"WIREMICRO_PASSWORD": "pw"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Ops != 20000 || cfg.Workers != 16 || cfg.Payload != 256 || cfg.Preload != 1000 {
		t.Fatalf("defaults ops=%d workers=%d payload=%d preload=%d", cfg.Ops, cfg.Workers, cfg.Payload, cfg.Preload)
	}
}

func TestAddressAndCredentialsComeFromTheEnvironment(t *testing.T) {
	env := envOf(map[string]string{"WIREMICRO_ADDR": "host:1", "WIREMICRO_USER": "u", "WIREMICRO_PASSWORD": "p"})
	cfg, err := parseConfig([]string{"-engine", "cockroach"}, env)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "host:1" || cfg.User != "u" || cfg.Password != "p" {
		t.Fatalf("got %q %q %q", cfg.Addr, cfg.User, cfg.Password)
	}
	flagged, err := parseConfig([]string{"-engine", "cockroach", "-addr", "other:2"}, env)
	if err != nil || flagged.Addr != "other:2" {
		t.Fatalf("a flag must beat the environment, got %q (%v)", flagged.Addr, err)
	}
}

func TestInvalidFlagsAreRefusedWithTheFix(t *testing.T) {
	good := []string{"-engine", "mongo", "-addr", "h:1"}
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no engine", []string{"-addr", "h:1"}, "-engine"},
		{"unknown engine", []string{"-engine", "redis", "-addr", "h:1"}, "hoarddb, cockroach, mysql, postgres or mongo"},
		{"no address", []string{"-engine", "mongo"}, "WIREMICRO_ADDR"},
		{"too few ops", append(append([]string{}, good...), "-ops", "100"), "-smoke"},
		{"zero workers", append(append([]string{}, good...), "-workers", "0"), "-workers"},
		{"ops below workers", append(append([]string{}, good...), "-smoke", "-ops", "4", "-workers", "16"), "-ops"},
		{"empty payload", append(append([]string{}, good...), "-payload", "0"), "-payload"},
		{"huge payload", append(append([]string{}, good...), "-payload", "99999999"), "-payload"},
		{"no preload", append(append([]string{}, good...), "-preload", "0"), "-preload"},
		{"stray argument", append(append([]string{}, good...), "extra"), "extra"},
	}
	for _, c := range cases {
		_, err := parseConfig(c.args, envOf(nil))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want it to mention %q", c.name, err, c.want)
		}
	}
}

func TestSmokeLowersTheOpsFloor(t *testing.T) {
	cfg, err := parseConfig([]string{"-engine", "mongo", "-addr", "h:1", "-smoke", "-ops", "64", "-workers", "4"}, envOf(nil))
	if err != nil || cfg.Ops != 64 {
		t.Fatalf("cfg %+v err %v", cfg, err)
	}
}

func TestHoarddbMustStateItsLaunchEnvironmentTableSyncAndPassword(t *testing.T) {
	base := []string{"-engine", "hoarddb", "-addr", "h:1"}
	pw := envOf(map[string]string{"WIREMICRO_PASSWORD": "p"})
	cases := []struct {
		name string
		args []string
		env  func(string) string
		want string
	}{
		{"no env file", append(append([]string{}, base...), "-hoard-table-sync", "stepwise"), pw, "-hoard-env"},
		{"no table sync", append(append([]string{}, base...), "-hoard-env", "e"), pw, "-hoard-table-sync"},
		{"bad table sync", append(append([]string{}, base...), "-hoard-env", "e", "-hoard-table-sync", "fast"), pw, "stepwise or msync"},
		{"no password", append(append([]string{}, base...), "-hoard-env", "e", "-hoard-table-sync", "stepwise"), envOf(nil), "WIREMICRO_PASSWORD"},
	}
	for _, c := range cases {
		if _, err := parseConfig(c.args, c.env); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want it to mention %q", c.name, err, c.want)
		}
	}
	cfg, err := parseConfig(append(append([]string{}, base...), "-hoard-env", "e", "-hoard-table-sync", "stepwise"), pw)
	if err != nil || cfg.HoardTableSync != "stepwise" || cfg.User != "admin" {
		t.Fatalf("cfg %+v err %v", cfg, err)
	}
}

func TestFlagsNeverPrintThePassword(t *testing.T) {
	cfg, err := parseConfig([]string{"-engine", "mysql", "-addr", "h:1", "-password", "s3cret"}, envOf(nil))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cfg.describe(), "s3cret") {
		t.Fatalf("describe leaks the password: %s", cfg.describe())
	}
}
