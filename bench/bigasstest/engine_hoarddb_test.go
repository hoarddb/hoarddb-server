package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

func envOf(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func TestParseRingConfigReadsParamsAndEnv(t *testing.T) {
	cfg := Config{Engine: "ring", URI: "hoarddb://host:7433", Params: map[string]string{"fingerprint": "SHA256:abc"}}
	got, err := parseRingConfig(cfg, envOf(map[string]string{"HOARDDB_ROOT_PASSWORD": "secret"}))
	if err != nil {
		t.Fatal(err)
	}
	options := got.options
	if options.Addr != "host:7433" || options.User != "admin" || options.Password != "secret" || options.Fingerprint != "SHA256:abc" || options.Insecure {
		t.Fatalf("unexpected options %+v", options)
	}
}

func TestParseRingConfigRefusals(t *testing.T) {
	env := envOf(map[string]string{"HOARDDB_ROOT_PASSWORD": "secret"})
	cases := map[string]struct {
		cfg  Config
		env  func(string) string
		want string
	}{
		"no address":      {Config{Params: map[string]string{"insecure": "true"}}, env, "-uri is empty"},
		"unknown key":     {Config{URI: "h:1", Params: map[string]string{"colour": "red"}}, env, `unknown -param "colour"`},
		"no password":     {Config{URI: "h:1", Params: map[string]string{"insecure": "true"}}, envOf(nil), "no password"},
		"no trust":        {Config{URI: "h:1"}, env, "neither fingerprint nor insecure"},
		"bad insecure":    {Config{URI: "h:1", Params: map[string]string{"insecure": "maybe"}}, env, "not true or false"},
		"password in url": {Config{URI: "h:1", Params: map[string]string{"insecure": "1", "password": ""}}, envOf(nil), "no password"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseRingConfig(tc.cfg, tc.env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestRingScalarsExcludeStores(t *testing.T) {
	for layout, buckets := range ringScalars {
		for bucket, names := range buckets {
			for _, name := range names {
				if _, isStore := schemaKinds[layout][bucket+"."+name]; isStore {
					t.Fatalf("%s.%s.%s is a store but is listed as a scalar", layout, bucket, name)
				}
			}
		}
	}
	if got := ringScalars["social"]["Post"]; len(got) != 3 || got[0] != "Author" {
		t.Fatalf("Post scalars %v", got)
	}
}

func TestRingIsRegistered(t *testing.T) {
	if _, ok := adapters["ring"]; !ok {
		t.Fatal("the ring adapter is not registered")
	}
}

// TestRingCipherProbeFailureIsAnError keeps "tls: unknown" out of a record: a failed probe
// makes Settings fail.
func TestRingCipherProbeFailureIsAnError(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	engine := &ringEngine{options: ringclient.Options{Addr: addr}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cipher, err := engine.cipher(ctx)
	if err == nil {
		t.Fatalf("a probe of a closed port returned cipher %q and no error", cipher)
	}
	if strings.Contains(cipher, "unknown") {
		t.Errorf("cipher = %q", cipher)
	}
}
