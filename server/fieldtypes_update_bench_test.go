package server

import (
	"context"
	"maps"
	"testing"
)

// The update checks' cost (rule 11; build doc TF-7): what an update pays for
// them on a bucket that declares nothing, and on one that declares five of a
// 20-field document's fields, three of which the update writes.

// benchUpdate sets Name, increments Age and raises Score: two static checks
// and one result check on the typed bucket of enforcementServer.
func benchUpdate(bucket string) UpdateRequest {
	return UpdateRequest{Database: "app", Bucket: bucket, Key: "doc",
		Set: map[string]interface{}{"Name": "Grace Hopper"}, Inc: map[string]int64{"Age": 1}, Max: map[string]interface{}{"Score": 10.0}}
}

var updateCheckSink error

// BenchmarkUpdateFieldTypeCheck is the checks an update's apply runs, on the
// bucket it holds, for one document: preparing them from the parsed request
// (once per update), the static half before the operators and the result
// half after. The parse and the operators are not in it: every update pays
// them, declared or not.
func BenchmarkUpdateFieldTypeCheck(b *testing.B) {
	s := enforcementServer(b)
	ctx := context.Background()
	for _, name := range []string{"plain", "typed"} {
		bucket := servedBucketTB(b, s, "app", name)
		parsed, err := parseUpdate(benchUpdate(name))
		if err != nil {
			b.Fatalf("parsing the update: %v", err)
		}
		document := benchDocument(b)
		if err := parsed.apply(document, "doc"); err != nil {
			b.Fatalf("applying the update: %v", err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				typing, err := typeUpdate(bucket.declaredFieldTypes(), parsed)
				if err == nil {
					err = typing.before(ctx, document, "doc")
				}
				if err == nil {
					err = typing.after(ctx, document, "doc")
				}
				updateCheckSink = err
			}
		})
	}
}

// BenchmarkUpdateDeclaredVsUndeclared is a whole update of the 20-field
// document in the undeclared and the typed bucket: the checks against
// everything else an update does (read, decode, operators, encode, write).
func BenchmarkUpdateDeclaredVsUndeclared(b *testing.B) {
	s := enforcementServer(b)
	for _, name := range []string{"plain", "typed"} {
		resp, err := s.Insert(InsertRequest{Database: "app", Bucket: name, Key: "doc", Data: benchDocument(b)})
		if err != nil || !resp.Success {
			b.Fatalf("insert: %+v %v", resp, err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				update, err := s.Update(benchUpdate(name))
				if err != nil || !update.Success {
					b.Fatalf("update: %+v %v", update, err)
				}
			}
		})
	}
}

// TestUndeclaredBucketPaysNoUpdateCheckAllocation (spec §9.1): on a bucket
// that declares nothing, the update checks are the declaration's load and
// nil tests, and allocate nothing.
func TestUndeclaredBucketPaysNoUpdateCheckAllocation(t *testing.T) {
	s := enforcementServer(t)
	ctx := context.Background()
	plain := servedBucket(t, s, "app", "plain")
	parsed, err := parseUpdate(benchUpdate("plain"))
	if err != nil {
		t.Fatalf("parsing the update: %v", err)
	}
	document := maps.Clone(benchDocument(t))
	if allocations := minAllocs(7, 1000, func() {
		typing, err := typeUpdate(plain.declaredFieldTypes(), parsed)
		if err == nil {
			err = typing.before(ctx, document, "doc")
		}
		if err == nil {
			err = typing.after(ctx, document, "doc")
		}
		updateCheckSink = err
	}); allocations != 0 {
		t.Errorf("the update checks on an undeclared bucket allocated %v times per update, want 0", allocations)
	}
}
