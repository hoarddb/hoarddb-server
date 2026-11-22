package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// indexOptions builds a create's Options with one index declaration.
func oneIndexOption(element interface{}) map[string]interface{} {
	return map[string]interface{}{"indices": []interface{}{element}}
}

// indexDeclarations are every shape an index name or index field takes in a
// create: a bare field name, an object with a name, an object with a field,
// and an object with a fields list.
func indexDeclarations() map[string]interface{} {
	return map[string]interface{}{
		"string form":         badName,
		"object form name":    map[string]interface{}{"field": "x", "name": badName},
		"object form field":   map[string]interface{}{"field": badName},
		"object form fields":  map[string]interface{}{"fields": []interface{}{"x", badName}},
		"object name, fields": map[string]interface{}{"fields": []interface{}{"x", "y"}, "name": badName},
	}
}

// TestCreateRefusesControlCharactersInIndexDeclarations: an index's name and
// its fields reach clients in the bucket's statistics, so they are held to the
// same rule as the names around them, in both declaration forms, on a node
// that does not route and on a cluster owner.
func TestCreateRefusesControlCharactersInIndexDeclarations(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		s := newTestServer(t)
		handler := NewHandler(s)
		handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		for label, element := range indexDeclarations() {
			t.Run(label, func(t *testing.T) {
				frame, err := handler(context.Background(), transport.NewFrame(transport.OpCreateBucket, mustMarshalBSON(t,
					CreateBucketRequest{Database: "app", Name: "indexed", Type: BucketTypeHash, Options: oneIndexOption(element)})))
				if err != nil {
					t.Fatal(err)
				}
				requireControlRefusal(t, "an index declaration", refusalText(t, frame), "Index", "indices:")
			})
		}
	})
	t.Run("cluster owner", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		waitForFeatureConvergence(t, tc)
		wal := tc.nodes[0].server.core.wal
		loggedBefore := wal.LastSequence()
		for label, element := range indexDeclarations() {
			t.Run(label, func(t *testing.T) {
				name := bucketOwnedBy(t, tc, 0, "idx-")
				requireControlRefusal(t, "an index declaration", refusalText(t, tc.frame(0, transport.OpCreateBucket,
					CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash, Options: oneIndexOption(element)})), "Index", "indices:")
				requireNoBucketAnywhere(t, tc, testDatabase, name)
			})
		}
		if logged := wal.LastSequence(); logged != loggedBefore {
			t.Errorf("the owner's log moved from %d to %d for refused index names", loggedBefore, logged)
		}
	})
}

// TestPathIndexRefusalEscapesTheName: the refusal of an index on a nested path
// names the field in its sentence, and must not repeat a control character raw.
func TestPathIndexRefusalEscapesTheName(t *testing.T) {
	text := (&pathIndexError{field: "Addr.a\x1b[2Jb"}).Error()
	for _, character := range text {
		if character == 0x1b {
			t.Fatalf("the refusal repeats an escape character raw: %q", text)
		}
	}
}
