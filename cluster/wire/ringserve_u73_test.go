package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// u73StaleNames answers the reverse-index read of one bucket id with a name the id no
// longer belongs to, as an orphaned reverse document does after a bucket is recreated.
type u73StaleNames struct {
	DeclarationReader
	encID []byte
	name  string
}

func (r *u73StaleNames) ReadBucketName(ctx context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error) {
	if bytes.Equal(read.EncodedID, r.encID) {
		return r.name, true, nil
	}
	return r.DeclarationReader.ReadBucketName(ctx, owner, read)
}

// TestU73AStaleReverseIndexEntryDoesNotRouteToARecreatedBucket: [GUARD the fresh lookup
// checks the declaration still carries the requested id].
func TestU73AStaleReverseIndexEntryDoesNotRouteToARecreatedBucket(t *testing.T) {
	env := u66Start(t)
	staleID := env.ref.entry.Decl.BucketID ^ 0x7373
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], staleID)
	encID, err := store.EncodeComponent(raw[:])
	if err != nil {
		t.Fatal(err)
	}
	env.node.fetcher.cfg.Reader = &u73StaleNames{DeclarationReader: env.node.fetcher.cfg.Reader, encID: encID, name: "Box"}
	c := env.login(t)
	stale := env.read(t, RingClassGet, "b1", "", 0)
	stale.Read.BucketID = staleID
	u66Refusal(t, c.keyed(stale), cluster.CodePermissionDenied)
	write := env.doc(t, cluster.OpInsert, "u73-new", "x")
	write.Op.BucketID = staleID
	u66Refusal(t, c.keyed(write), cluster.CodePermissionDenied)
	if got := u66Reply(t, c.keyed(env.read(t, RingClassGet, "u73-new", "", 0))); got.Found == nil || *got.Found {
		t.Fatalf("a request naming a stale bucket id wrote into the bucket that now has the name: %+v", got)
	}
	// The live id still works, so the check is on the id and not on the name.
	if got := u66Reply(t, c.keyed(env.read(t, RingClassGet, "b1", "", 0))); got.Found == nil || !*got.Found {
		t.Fatalf("the live bucket id stopped working: %+v", got)
	}
}

// TestU73TheUnavailableTextNamesNoNodeAndNoDatabase: [GUARD the lookup error is logged,
// never sent].
func TestU73TheUnavailableTextNamesNoNodeAndNoDatabase(t *testing.T) {
	app, _, _ := u70Start(t)
	other, err := app.node.CreateDatabase(context.Background(), "zzbroken")
	if err != nil {
		t.Fatal(err)
	}
	app.node.topology.Refresh()
	u69WatchNames(app).broken[other] = true
	unknown := app.read(t, RingClassGet, "b1", "", 0)
	unknown.Read.BucketID ^= 0x5A5A5A5A
	reader := u70Login(t, app, "reader", "read@app")
	refusal := u66Refusal(t, reader.keyed(unknown), cluster.CodeUnavailable)
	for _, leak := range []string{"zzbroken", app.node.cfg.NodeID, "is down", "reading the name", "bucket index"} {
		if strings.Contains(refusal.Message, leak) {
			t.Errorf("the unavailable text carries %q: %s", leak, refusal.Message)
		}
	}
	if !strings.Contains(refusal.Message, "Retry") {
		t.Errorf("the unavailable text does not say to retry: %s", refusal.Message)
	}
}
