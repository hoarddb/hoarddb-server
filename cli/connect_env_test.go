package cli

import "testing"

// A client with the server's old replication settings in its environment still resolves its
// connection: the refusal of the old replicated mode belongs to a server start, and a developer
// who has HOARDDB_REPLICATION_FACTOR exported for a server must still be able to run the CLI.
func TestAClientWithTheOldReplicationFactorSetStillResolvesItsConnection(t *testing.T) {
	// config.Load reads the process environment, so the settings go there.
	t.Setenv("HOARDDB_REPLICATION_FACTOR", "3")
	_, err := resolveConnection(ConnectSpec{
		Address: "127.0.0.1:7433",
		Getenv:  func(string) string { return "" },
	})
	if err != nil {
		t.Fatalf("resolveConnection refused a client because of server settings: %v", err)
	}
}
