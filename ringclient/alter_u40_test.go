package ringclient_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// u40Pair is two clients of one live server, each with its own connection and header cache.
func u40Pair(t *testing.T) (a, b *ringclient.Client, ctx context.Context) {
	t.Helper()
	addr := startLiveServer(t)
	ctx = liveContext(t)
	dial := func() *ringclient.Client {
		client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "root", Password: "live-test-password", Insecure: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	return dial(), dial(), ctx
}

func u40Fields(info ringclient.BucketInfo) []string {
	names := make([]string, 0, len(info.Fields))
	for _, field := range info.Fields {
		names = append(names, field.Name+" "+field.Type)
	}
	return names
}

// TestLiveAlterBucketAddsAndDropsFields: the first-five-minutes alter against the real server.
// A second client that cached the header before the alter learns the new field by itself, a
// write naming a dropped field is FIELD_TYPE, and the same alter twice changes nothing.
func TestLiveAlterBucketAddsAndDropsFields(t *testing.T) {
	alterer, other, ctx := u40Pair(t)
	if _, err := alterer.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := alterer.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}, {Name: "Age", Type: "int"}}); err != nil {
		t.Fatal(err)
	}
	users := other.Bucket("app", "User")
	if _, err := users.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada", "Age": int64(36)}); err != nil {
		t.Fatal(err)
	}

	info, err := alterer.AlterBucket(ctx, "app", "User", ringclient.AlterChange{Add: []ringclient.FieldDecl{{Name: "Email", Type: "string"}, {Name: "Orders", Type: "fifoStore"}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(u40Fields(info)); got != "[_id_ any Name string Age int Email string Orders fifoStore]" {
		t.Fatalf("fields after the add = %s", got)
	}
	if info.DeclVersion != 2 {
		t.Fatalf("decl_version = %d, want 2", info.DeclVersion)
	}
	// The other client's cached header knows neither field; it learns them and writes.
	if _, err := users.Insert(ctx, "grace", ringclient.Doc{"Name": "Grace", "Email": "grace@example.org"}); err != nil {
		t.Fatalf("a client with an older header writes the new field: %v", err)
	}
	if _, err := users.Push(ctx, "grace", "Orders", item("book")); err != nil {
		t.Fatalf("a client with an older header pushes to the new store: %v", err)
	}
	got, err := users.Get(ctx, "ada")
	if err != nil || got.Lookup("Email").Type != 0 {
		t.Fatalf("an existing document must not gain the new field: %v %v", got, err)
	}

	again, err := alterer.AlterBucket(ctx, "app", "User", ringclient.AlterChange{Add: []ringclient.FieldDecl{{Name: "Email", Type: "string"}}})
	if err != nil || again.DeclVersion != info.DeclVersion {
		t.Fatalf("the same alter twice: %+v %v, want the same declaration", again, err)
	}

	dropped, err := alterer.AlterBucket(ctx, "app", "User", ringclient.AlterChange{Drop: []string{"Age"}})
	if err != nil || dropped.DeclVersion != 3 || strings.Contains(fmt.Sprint(u40Fields(dropped)), "Age") {
		t.Fatalf("drop: %+v %v", dropped, err)
	}
	// The other client still names Age: the owner refuses it, or the client learns first and refuses itself.
	_, err = users.Insert(ctx, "linus", ringclient.Doc{"Name": "Linus", "Age": int64(50)})
	if err == nil {
		t.Fatal("a write naming a dropped field was accepted")
	}
	var refusal *ringclient.Error
	if errors.As(err, &refusal) && refusal.Code != cluster.CodeFieldType {
		t.Fatalf("a write naming a dropped field = %v, want FIELD_TYPE or a client-side refusal", err)
	}
	if _, err := users.Insert(ctx, "linus", ringclient.Doc{"Name": "Linus"}); err != nil {
		t.Fatalf("a write that avoids the dropped field after the refusal: %v", err)
	}
	if _, err := alterer.AlterBucket(ctx, "app", "User", ringclient.AlterChange{Add: []ringclient.FieldDecl{{Name: "Name", Type: "int"}}}); err == nil ||
		!strings.Contains(err.Error(), "a declared type never changes") {
		t.Fatalf("a type change = %v, want the refusal naming the fix", err)
	}
}

// TestLiveWritersRaceAnAlter: eight writers insert 40 documents each while another client
// adds and then drops fields in a loop. Every insert is acknowledged exactly once, every
// document is there afterwards, and every writer, whatever header it held, converges.
func TestLiveWritersRaceAnAlter(t *testing.T) {
	addr := startLiveServer(t)
	ctx := liveContext(t)
	dial := func() *ringclient.Client {
		client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "root", Password: "live-test-password", Insecure: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	admin := dial()
	if _, err := admin.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	const writers, each = 8, 40
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		client := dial()
		go func() {
			defer wg.Done()
			users := client.Bucket("app", "User")
			for i := range each {
				id := fmt.Sprintf("w%d-%d", w, i)
				requestID := client.NextRequestID()
				for attempt := 0; ; attempt++ {
					_, err := users.Insert(ctx, id, ringclient.Doc{"Name": id}, ringclient.WithRequestID(requestID))
					if err == nil {
						break
					}
					var refusal *ringclient.Error
					// The client relearns a stale header once; two alters can land inside that
					// one retry, so the caller repeats its write under the same request id.
					if attempt == 8 || !errors.As(err, &refusal) || refusal.Code != cluster.CodeSchemaNotCurrent {
						t.Errorf("insert %s: %v", id, err)
						return
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := range 6 {
			name := fmt.Sprintf("Extra%d", round)
			if _, err := admin.AlterBucket(ctx, "app", "User", ringclient.AlterChange{Add: []ringclient.FieldDecl{{Name: name, Type: "string"}}}); err != nil {
				t.Errorf("add %s: %v", name, err)
				return
			}
			if _, err := admin.AlterBucket(ctx, "app", "User", ringclient.AlterChange{Drop: []string{name}}); err != nil {
				t.Errorf("drop %s: %v", name, err)
				return
			}
		}
	}()
	wg.Wait()

	reader := dial().Bucket("app", "User")
	for w := range writers {
		for i := range each {
			id := fmt.Sprintf("w%d-%d", w, i)
			doc, err := reader.Get(ctx, id)
			if err != nil || doc.Lookup("Name").StringValue() != id {
				t.Fatalf("document %s after the race: %v %v", id, doc, err)
			}
		}
	}
	count, err := reader.Count(ctx, nil)
	if err != nil || count != writers*each {
		t.Fatalf("count = %d, %v, want %d: a write was lost or applied twice", count, err, writers*each)
	}
}

// TestLiveAStaleClientRefreshesOnSchemaNotCurrent: a client holds the header from before an
// alter; once the server has used the new declaration for anyone, the stale write meets
// SCHEMA_NOT_CURRENT inside the server, and the client learns the header again and sends the
// write once more under the same request id. The caller sees one successful insert.
func TestLiveAStaleClientRefreshesOnSchemaNotCurrent(t *testing.T) {
	alterer, stale, ctx := u40Pair(t)
	if _, err := alterer.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := alterer.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	old := stale.Bucket("app", "User")
	if _, err := old.Insert(ctx, "first", ringclient.Doc{"Name": "First"}); err != nil {
		t.Fatal(err)
	}
	before, err := old.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := alterer.AlterBucket(ctx, "app", "User", ringclient.AlterChange{Add: []ringclient.FieldDecl{{Name: "Email", Type: "string"}}}); err != nil {
		t.Fatal(err)
	}
	// Any use by anyone makes the server fetch the new declaration.
	if _, err := alterer.Bucket("app", "User").Insert(ctx, "second", ringclient.Doc{"Name": "Second", "Email": "e"}); err != nil {
		t.Fatal(err)
	}
	var id [16]byte
	id[0] = 40
	written, err := old.Insert(ctx, "third", ringclient.Doc{"Name": "Third"}, ringclient.WithRequestID(id))
	if err != nil || !written.Applied() {
		t.Fatalf("the stale client's insert = %+v, %v: it must learn the new header and succeed", written, err)
	}
	after, err := old.Info(ctx)
	if err != nil || after.DeclVersion <= before.DeclVersion {
		t.Fatalf("the stale client kept its header: version %d then %d, %v", before.DeclVersion, after.DeclVersion, err)
	}
	if got, err := old.Get(ctx, "third"); err != nil || got.Lookup("Name").StringValue() != "Third" {
		t.Fatalf("get third = %v, %v", got, err)
	}
}
