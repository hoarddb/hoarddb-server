package cli

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestU80ShowDatabasesPrintsTheV1Layout: the title, the rule and the table of v1.0, minus
// the BUCKETS column the ring reply cannot supply (a count per database would be O(buckets)).
func TestU80ShowDatabasesPrintsTheV1Layout(t *testing.T) {
	session, _ := u52Session()
	got, err := u52Run(session, "show databases")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "DATABASES", "---", "(no databases yet — create one with 'create database <name>')")
	if _, err := u52Run(session, "create database app", "create database shop { replication_factor: 2 }"); err != nil {
		t.Fatal(err)
	}
	got, err = u52Run(session, "show databases")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "DATABASES", "---",
		"NAME  ID                REPLICATION",
		"app   0000000000000010  rf 1",
		"shop  0000000000000011  rf 2")
}

func TestU80ShowDatabasesPagesWithTheMoreHint(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database a", "create database b", "create database c"); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, "show databases limit 2")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "DATABASES", "---", "NAME  ID                REPLICATION",
		"a     0000000000000010  rf 1", "b     0000000000000011  rf 1",
		"more: show databases limit 2 cursor 02")
	got, err = u52Run(session, "show databases limit 2 cursor 02")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "DATABASES", "---", "NAME  ID                REPLICATION", "c     0000000000000012  rf 1")
}

func TestU80DescribeDatabasePrintsFactsAndTheBucketPage(t *testing.T) {
	session, _ := u52Session()
	got, err := u52Run(session, "create database app", "describe database app")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, "DATABASE app\n---\nId:                 0000000000000010\nReplication factor: 1\nStorage:            disk\nBuckets:            0\n") {
		t.Fatalf("empty database:\n%s", got)
	}
	got, err = u52Run(session, "create bucket User in app { Name string }", "create bucket Post in app { Title string }", "describe database app limit 1")
	if err != nil {
		t.Fatal(err)
	}
	want := "DATABASE app\n---\nId:                 0000000000000010\nReplication factor: 1\nStorage:            disk\nBuckets:            1 on this page, more follow\n\n" +
		"NAME  ID                STATE\nUser  1f4a2c9b7de30015  active\nmore: describe database app limit 1 cursor 01\n"
	if !strings.HasSuffix(got, want) {
		t.Fatalf("paged:\n%s\nwant suffix:\n%s", got, want)
	}
}

func TestU80DescribeDatabaseRefusalKeepsTheServerCode(t *testing.T) {
	session, _ := u52Session()
	_, err := u52Run(session, "describe database nope")
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "PERMISSION_DENIED" || failed.Exit != cluster.CodePermissionDenied.Exit() {
		t.Fatalf("describe database nope: %v", err)
	}
}

// TestU80CompletionAsksTheServerForDatabases replaces the session-only list (SPEC-GAP U52-4):
// a database another session made is offered, from the 5-second cache.
func TestU80CompletionAsksTheServerForDatabases(t *testing.T) {
	session, fake := u52Session()
	fake.databases["elsewhere"] = 1
	names := newRingServerNames(session)
	clock := time.Unix(1000, 0)
	names.now = func() time.Time { return clock }
	if got := CompleteWith(names, "use ", ""); strings.Join(got, ",") != "elsewhere" {
		t.Fatalf("databases: %v", got)
	}
	fake.databases["later"] = 1
	before := len(fake.calls)
	if got := CompleteWith(names, "use ", ""); strings.Join(got, ",") != "elsewhere" || len(fake.calls) != before {
		t.Fatalf("a lookup inside the TTL asked again or changed: %v %v", got, fake.calls)
	}
	clock = clock.Add(ringNameTTL + time.Second)
	if got := CompleteWith(names, "use ", ""); strings.Join(got, ",") != "elsewhere,later" {
		t.Fatalf("after the TTL: %v", got)
	}
	fake.failures["ShowDatabases"] = &ringclient.Error{Code: cluster.CodeUnavailable, Message: "down"}
	clock = clock.Add(time.Hour)
	if got := CompleteWith(names, "use ", ""); len(got) != 0 {
		t.Fatalf("a failed lookup must offer nothing, got %v", got)
	}
}
