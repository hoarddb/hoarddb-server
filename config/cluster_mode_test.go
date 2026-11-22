package config

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// clusterModeEnvNames are the environment variables of the cluster.* keys,
// cleared before a test so a host that sets them does not change the
// zero-config start under test.
var clusterModeEnvNames = []string{
	envClusterEnabled,
	envClusterListen,
	envClusterAdvertise,
	envClusterWitness,
	envClusterAllowMembership,
}

func clearClusterModeEnv(t *testing.T) {
	t.Helper()
	for _, name := range clusterModeEnvNames {
		t.Setenv(name, "")
	}
}

// TestClusterModeEnvConstantsMatchTags checks that each environment name the
// refusals and the docs use is the one its struct tag carries, so the constant
// and the binding cannot drift apart.
func TestClusterModeEnvConstantsMatchTags(t *testing.T) {
	clusterType := reflect.TypeOf(ClusterConfig{})
	for field, want := range map[string]string{
		"Enabled":         envClusterEnabled,
		"Listen":          envClusterListen,
		"Advertise":       envClusterAdvertise,
		"Witness":         envClusterWitness,
		"AllowMembership": envClusterAllowMembership,
	} {
		structField, ok := clusterType.FieldByName(field)
		if !ok {
			t.Fatalf("ClusterConfig has no field %s", field)
		}
		if got := structField.Tag.Get("env"); got != want {
			t.Errorf("ClusterConfig.%s env tag %q, the constant says %q", field, got, want)
		}
	}
}

// TestClusterModeKeysDefaultToOff checks that a start with no cluster settings
// leaves every cluster mode key at its zero value, so the node is the one-voter
// ring node it was before these keys existed.
func TestClusterModeKeysDefaultToOff(t *testing.T) {
	clearClusterModeEnv(t)
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cluster := cfg.Cluster
	if cluster.Enabled || cluster.Listen != "" || cluster.Advertise != "" || cluster.Witness {
		t.Errorf("cluster mode keys with no settings = enabled:%v listen:%q advertise:%q witness:%v, want all off and empty",
			cluster.Enabled, cluster.Listen, cluster.Advertise, cluster.Witness)
	}
}

// TestClusterModeEnvOverridesReachTheKeys checks each cluster mode variable
// reaches its key, the string keys included.
func TestClusterModeEnvOverridesReachTheKeys(t *testing.T) {
	clearClusterModeEnv(t)
	t.Setenv("HOARDDB_NODE_ID", "node-a")
	t.Setenv("HOARDDB_CLUSTER_ENABLED", "true")
	t.Setenv("HOARDDB_CLUSTER_LISTEN", "127.0.0.1:4443")
	t.Setenv("HOARDDB_CLUSTER_ADVERTISE", "192.0.2.1:4443")
	t.Setenv("HOARDDB_CLUSTER_WITNESS", "true")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cluster := cfg.Cluster
	if !cluster.Enabled || cluster.Listen != "127.0.0.1:4443" || cluster.Advertise != "192.0.2.1:4443" ||
		!cluster.Witness {
		t.Errorf("environment did not reach the cluster mode keys: enabled:%v listen:%q advertise:%q witness:%v",
			cluster.Enabled, cluster.Listen, cluster.Advertise, cluster.Witness)
	}
}

// TestClusterModeAcceptedSettingsLoad checks the settings the checks must let
// through: a wildcard listen with an advertise address, a loopback listen with
// no advertise, and a wildcard listen while cluster mode is off.
func TestClusterModeAcceptedSettingsLoad(t *testing.T) {
	clearClusterModeEnv(t)
	cases := map[string]string{
		"wildcard listen with advertise": "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  listen: \"0.0.0.0:4443\"\n  advertise: \"192.0.2.1:4443\"\n",
		"loopback listen, no advertise":  "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  listen: \"127.0.0.1:4443\"\n",
		"wildcard listen, cluster off":   "cluster:\n  listen: \"0.0.0.0:4443\"\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeYAML(t, body)); err != nil {
				t.Errorf("Load(%q) = %v, want no error", body, err)
			}
		})
	}
}

// TestClusterModeRefusalsNameTheFix checks each start-up refusal of cluster
// mode quotes the key it is about and the exact fix.
func TestClusterModeRefusalsNameTheFix(t *testing.T) {
	clearClusterModeEnv(t)
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "defaulted node id",
			body: "cluster:\n  enabled: true\n",
			want: "cluster.enabled is true but server.node_id is not set",
		},
		{
			name: "wildcard listen without advertise",
			body: "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  listen: \"0.0.0.0:4443\"\n",
			want: "cluster.advertise is not set",
		},
		{
			name: "wildcard server.listen without advertise",
			body: "server:\n  node_id: node-a\n  listen: \"0.0.0.0:4433\"\ncluster:\n  enabled: true\n",
			want: "cluster.advertise is not set",
		},
		{
			name: "listen without a port",
			body: "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  listen: \"4443\"\n",
			want: "cluster.listen is \"4443\", which is not host:port",
		},
		{
			name: "node id with a colon",
			body: "server:\n  node_id: \"node-a:4433\"\ncluster:\n  enabled: true\n",
			want: "server.node_id is \"node-a:4433\"",
		},
		{
			name: "listen on the driver port",
			body: "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  listen: \"127.0.0.1:7433\"\n",
			want: "cluster.listen is 127.0.0.1:7433, the port tcp.listen",
		},
		{
			name: "listen on the legacy internode port",
			body: "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  listen: \"127.0.0.1:4434\"\n",
			want: "cluster.listen is 127.0.0.1:4434, the port the node-to-node listener",
		},
		{
			name: "advertise without a port, cluster off",
			body: "cluster:\n  advertise: \"192.0.2.1\"\n",
			want: "cluster.advertise is \"192.0.2.1\", which is not host:port",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeYAML(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Load error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestAllowMembershipWithoutClusterIsRefusedAtStart (M2): an explicit true with no cluster has
// nothing to allow, and the refusal names the key and the fix; false, unset, and true with a
// cluster all load.
func TestAllowMembershipWithoutClusterIsRefusedAtStart(t *testing.T) {
	clearClusterModeEnv(t)
	t.Setenv(envClusterAllowMembership, "")
	wantM2 := "cluster.allow_membership is true but cluster.enabled is false: there is no cluster to change the membership of. " +
		"Set 'cluster.enabled: true' (HOARDDB_CLUSTER_ENABLED=1) as well, or remove cluster.allow_membership."
	if _, err := Load(writeYAML(t, "cluster:\n  allow_membership: true\n")); err == nil || err.Error() != wantM2 {
		t.Errorf("allow_membership true with the cluster off = %v\nwant %q", err, wantM2)
	}
	t.Setenv(envClusterAllowMembership, "1")
	if _, err := Load(""); err == nil || err.Error() != wantM2 {
		t.Errorf("HOARDDB_CLUSTER_ALLOW_MEMBERSHIP=1 with the cluster off = %v\nwant %q", err, wantM2)
	}
	t.Setenv(envClusterAllowMembership, "")
	for name, body := range map[string]string{
		"false, cluster off": "cluster:\n  allow_membership: false\n",
		"unset, cluster off": "server:\n  node_id: node-a\n",
		"true, cluster on":   "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  allow_membership: true\n",
	} {
		if _, err := Load(writeYAML(t, body)); err != nil {
			t.Errorf("%s: Load = %v, want no error", name, err)
		}
	}
}

// TestAllowMembershipDefaultsToTrueInClusterMode: unset reads as allowed, an explicit false locks,
// and the environment variable reaches the key.
func TestAllowMembershipDefaultsToTrueInClusterMode(t *testing.T) {
	clearClusterModeEnv(t)
	t.Setenv(envClusterAllowMembership, "")
	t.Setenv("HOARDDB_NODE_ID", "node-a")
	t.Setenv(envClusterEnabled, "true")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Cluster.MembershipAllowed() {
		t.Fatal("membership is locked when cluster.allow_membership is unset; the default is true")
	}
	t.Setenv(envClusterAllowMembership, "false")
	locked, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if locked.Cluster.MembershipAllowed() || locked.Cluster.AllowMembership == nil {
		t.Fatalf("HOARDDB_CLUSTER_ALLOW_MEMBERSHIP=false left membership allowed: %+v", locked.Cluster.AllowMembership)
	}
	t.Setenv(envClusterAllowMembership, "maybe")
	if _, err := Load(""); err == nil || !strings.Contains(err.Error(), "HOARDDB_CLUSTER_ALLOW_MEMBERSHIP=\"maybe\" is not true or false") {
		t.Errorf("a bad value = %v; it must be refused naming the variable", err)
	}
}

// TestClusterAdvertiseRefusesAZone checks that cluster.advertise, the address peers dial, refuses
// a zoned address (an interface of this host, which no other machine can dial) and names the
// key, the zone and the fix. Loopback, routable and named addresses still load.
// [BREAK: validateClusterMode skips the advertise zone check].
func TestClusterAdvertiseRefusesAZone(t *testing.T) {
	clearClusterModeEnv(t)
	head := "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  listen: \"127.0.0.1:4443\"\n  advertise: "
	refused := map[string]string{
		"[fe80::1%eth0]:4433":   `cluster.advertise is "[fe80::1%eth0]:4433", which names a network zone (%eth0)`,
		"[fe80::1%25eth0]:4433": `cluster.advertise is "[fe80::1%25eth0]:4433", which names a network zone (%25eth0)`,
	}
	for value, want := range refused {
		_, err := Load(writeYAML(t, head+"\""+value+"\"\n"))
		if err == nil || !strings.Contains(err.Error(), want) ||
			!strings.Contains(err.Error(), "cluster.advertise: <this node's address>:<port>") ||
			!strings.Contains(err.Error(), "HOARDDB_CLUSTER_ADVERTISE=") {
			t.Errorf("advertise %q: Load = %v, want a zone refusal containing %q and the fix", value, err, want)
		}
	}
	for _, value := range []string{"[::1]:4433", "192.0.2.1:4433", "[2001:db8::1]:4433", "node-a:4433"} {
		if _, err := Load(writeYAML(t, head+"\""+value+"\"\n")); err != nil {
			t.Errorf("advertise %q: Load = %v, want no error", value, err)
		}
	}
}

// TestClusterListenRefusesAZone checks that cluster.listen refuses a zoned address whatever
// cluster.advertise says. With advertise unset the listen address is the one peers are told to
// dial, so a zone there is a dial failure; with advertise set the zone is only a bind address, but
// one rule is easier to state than the narrow one. Loopback, routable and named listens still load.
// [BREAK: validateClusterMode replaces advertiseZone(c.Cluster.Listen) with advertiseZone("")].
func TestClusterListenRefusesAZone(t *testing.T) {
	clearClusterModeEnv(t)
	zoned := map[string]string{
		"[fe80::1%eth0]:4433":   `cluster.listen is "[fe80::1%eth0]:4433", which names a network zone (%eth0)`,
		"[fe80::1%25eth0]:4433": `cluster.listen is "[fe80::1%25eth0]:4433", which names a network zone (%25eth0)`,
	}
	for _, advertise := range []string{"", "advertise: \"192.0.2.1:4443\"\n"} {
		head := "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  %s\n  " + advertise
		for value, want := range zoned {
			body := fmt.Sprintf(head, "listen: \""+value+"\"")
			_, err := Load(writeYAML(t, body))
			if err == nil || !strings.Contains(err.Error(), want) ||
				!strings.Contains(err.Error(), "cluster.listen: <this node's address>:<port>") ||
				!strings.Contains(err.Error(), "HOARDDB_CLUSTER_LISTEN=") {
				t.Errorf("listen %q with advertise %q: Load = %v, want a zone refusal containing %q and the fix",
					value, advertise, err, want)
			}
		}
	}
	for _, value := range []string{"127.0.0.1:4443", "[::1]:4443", "192.0.2.1:4443", "[2001:db8::1]:4443", "node-a:4443"} {
		body := "server:\n  node_id: node-a\ncluster:\n  enabled: true\n  listen: \"" + value + "\"\n"
		if _, err := Load(writeYAML(t, body)); err != nil {
			t.Errorf("listen %q: Load = %v, want no error", value, err)
		}
	}
}

// TestServerListenZoneIsRefusedWhenItIsThePeerListener: with cluster.listen unset the peer
// listener binds server.listen and, with cluster.advertise unset, peers are told to dial it, so a
// zoned server.listen is refused naming server.listen (BUG-0799). It is no business of a node that
// is not in cluster mode, nor of one that advertises a real address.
func TestServerListenZoneIsRefusedWhenItIsThePeerListener(t *testing.T) {
	clearClusterModeEnv(t)
	zoned := "[fe80::1%eth0]:4433"
	_, err := Load(writeYAML(t, "server:\n  node_id: node-a\n  listen: \""+zoned+"\"\ncluster:\n  enabled: true\n"))
	if err == nil || !strings.Contains(err.Error(), `server.listen is "`+zoned+`"`) || !strings.Contains(err.Error(), "network zone (%eth0)") ||
		!strings.Contains(err.Error(), "HOARDDB_LISTEN=") || !strings.Contains(err.Error(), "cluster.advertise") {
		t.Errorf("zoned server.listen, cluster.listen and advertise unset: Load = %v, want a refusal naming server.listen and the fix", err)
	}
	if _, err := Load(writeYAML(t, "server:\n  node_id: node-a\n  listen: \""+zoned+"\"\ncluster:\n  enabled: true\n  advertise: \"192.0.2.1:4443\"\n")); err != nil {
		t.Errorf("zoned server.listen with a real cluster.advertise: Load = %v, want no error", err)
	}
	if _, err := Load(writeYAML(t, "server:\n  node_id: node-a\n  listen: \""+zoned+"\"\n")); err != nil {
		t.Errorf("zoned server.listen with cluster mode off: Load = %v, want no error", err)
	}
}
