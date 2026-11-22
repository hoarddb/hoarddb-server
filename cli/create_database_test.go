package cli

import (
	"strings"
	"testing"
)

// TestCreateDatabaseWithReplicationFactorIsRefused pins the refusal. The clause
// was parsed and dropped before any handler saw it, so the command reported
// success and the database got the node-wide factor — a durability promise the
// server never made. The refusal is permanent: the factor belongs to the
// bucket, not the database (the bucket-replication-spec record §2.2), so the
// message names the ring form, a database option in a ring session, and says nothing about a per-node factor.
func TestCreateDatabaseWithReplicationFactorIsRefused(t *testing.T) {
	cases := []string{
		"create database MyApp with replication_factor 3",
		"create database MyApp with replication_factor 3;",
		"create db MyApp with replication_factor 3",
		"create database MyApp with REPLICATION_FACTOR 3",
		// The number is not what makes it wrong, and a truncated clause must
		// not fall through to the silent accept it is replacing.
		"create database MyApp with replication_factor",
	}

	for _, line := range cases {
		t.Run(line, func(t *testing.T) {
			cmd, err := ParseCommand(line)
			if err == nil {
				t.Fatalf("ParseCommand(%q) returned no error (Method=%q Database=%q); the clause is accepted and discarded", line, cmd.Method, cmd.Database)
			}
			message := err.Error()
			if !strings.Contains(message, "create database MyApp { replication_factor: N }") {
				t.Fatalf("error %q does not name the form that carries the factor", message)
			}
			if !strings.Contains(message, "not a setting of this session") {
				t.Fatalf("error %q does not say why the clause is refused", message)
			}
		})
	}
}

// TestCreateDatabaseRefusalDoesNotPromiseADatabaseFactor is the other half of
// the refusal: a per-database factor is out of scope for the design, so the
// message may not read as "not yet". A message that promises it would be the
// same rule-10 failure in a different tense.
func TestCreateDatabaseRefusalDoesNotPromiseADatabaseFactor(t *testing.T) {
	_, err := ParseCommand("create database MyApp with replication_factor 3")
	if err == nil {
		t.Fatal("create database … with replication_factor parsed; it must be refused")
	}
	for _, promise := range []string{"coming soon", "not supported yet", "in a future release", "will be supported"} {
		if strings.Contains(strings.ToLower(err.Error()), promise) {
			t.Fatalf("error %q promises a database-level factor with %q; the refusal is permanent", err, promise)
		}
	}
}

// TestCreateDatabaseStillParses guards the blast radius: refusing one clause
// must not cost the verb it is attached to.
func TestCreateDatabaseStillParses(t *testing.T) {
	for _, line := range []string{"create database MyApp", "create database MyApp;", "create db MyApp"} {
		cmd, err := ParseCommand(line)
		if err != nil {
			t.Fatalf("ParseCommand(%q): %v", line, err)
		}
		if cmd.Method != "create_database" {
			t.Fatalf("ParseCommand(%q) Method = %q, want create_database", line, cmd.Method)
		}
		if cmd.Database != "MyApp" {
			t.Fatalf("ParseCommand(%q) Database = %q, want MyApp", line, cmd.Database)
		}
	}
}

// TestCreateBucketReplicationFactorStillParses is the scope fence. The
// bucket-level clause is GAP-23's build, not this fix: it must keep reaching
// parseBucketSchema exactly as it does today.
func TestCreateBucketReplicationFactorStillParses(t *testing.T) {
	cmd, err := ParseCommand("create bucket Users { type: hash, replication_factor: 3 }")
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	schema, err := parseBucketSchema(cmd, "app")
	if err != nil {
		t.Fatalf("parseBucketSchema: %v", err)
	}
	if string(schema.Type) != "hash" {
		t.Fatalf("type = %q, want hash", schema.Type)
	}
	if schema.ReplicationFactor != 3 {
		t.Fatalf("ReplicationFactor = %d, want 3", schema.ReplicationFactor)
	}
}

// TestCreateDatabaseRefusalDoesNotClaimTheBucketFactorIsIgnored is BUG-0559.
// The per-bucket clause the refusal points at has been honoured by the server
// since GAP-23 Part 1; a refusal that says it is "not honoured yet" sends the
// operator away from the one form that works.
func TestCreateDatabaseRefusalDoesNotClaimTheBucketFactorIsIgnored(t *testing.T) {
	_, err := ParseCommand("create database MyApp with replication_factor 3")
	if err == nil {
		t.Fatal("create database … with replication_factor parsed; it must be refused")
	}
	for _, falsehood := range []string{"does not honour", "not honoured", "does not honor"} {
		if strings.Contains(err.Error(), falsehood) {
			t.Fatalf("error %q still says the per-bucket factor is not honoured (%q)", err, falsehood)
		}
	}
}
