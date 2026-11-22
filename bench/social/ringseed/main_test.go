package main

import (
	"strings"
	"testing"
)

func TestFlagsRefuseWhatCannotWorkAndNameTheFix(t *testing.T) {
	good := seedConfig{Addr: "host:1", Password: "pw", Insecure: true, Dataset: "S-10k", Workers: 1, Connections: 1}
	if err := good.check(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*seedConfig){
		"addr":     func(c *seedConfig) { c.Addr = "" },
		"password": func(c *seedConfig) { c.Password = "" },
		"trust":    func(c *seedConfig) { c.Insecure = false },
		"dataset":  func(c *seedConfig) { c.Dataset = "S-1M" },
		"workers":  func(c *seedConfig) { c.Workers = 0 },
	} {
		bad := good
		mutate(&bad)
		if err := bad.check(); err == nil {
			t.Errorf("%s: a config that cannot work was accepted", name)
		}
	}
	missing := good
	missing.Insecure = false
	if err := missing.check(); err == nil || !strings.Contains(err.Error(), "-fingerprint") || !strings.Contains(err.Error(), "-insecure") {
		t.Errorf("the trust refusal must name both flags: %v", err)
	}
}
