package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// wrongTypeCase is one verb sent to the store types that do not answer it.
type wrongTypeCase struct {
	verb  string
	types []BucketType
	call  func(s *Server, bucket string) error
}

// typesOtherThan lists the store types that are not in answering.
func typesOtherThan(answering ...BucketType) []BucketType {
	var refusing []BucketType
	for _, candidate := range matrixTypes {
		answers := false
		for _, kept := range answering {
			answers = answers || candidate == kept
		}
		if !answers {
			refusing = append(refusing, candidate)
		}
	}
	return refusing
}

// TestWrongTypeVerbIsInvalidArgument is BUG-0515: a verb the bucket's store
// type does not answer was a bare fmt.Errorf, so codeFor fell through to
// INTERNAL ("report it to the maintainers") for the caller's own mistake. Each
// pair must carry ErrWrongBucketType and classify as INVALID_ARGUMENT.
func TestWrongTypeVerbIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	matchN := map[string]interface{}{"n": int32(1)}
	cases := []wrongTypeCase{
		{"BTREE_RANGE", typesOtherThan(BucketTypeBTree), func(s *Server, b string) error {
			_, err := s.RangeQueryCtx(ctx, BTreeRangeRequest{Database: "app", Bucket: b})
			return err
		}},
		{"BTREE_FIRST", typesOtherThan(BucketTypeBTree), func(s *Server, b string) error {
			_, err := s.Edge("app", b, false)
			return err
		}},
		{"BTREE_LAST", typesOtherThan(BucketTypeBTree), func(s *Server, b string) error {
			_, err := s.Edge("app", b, true)
			return err
		}},
		{"BLOB_READ", typesOtherThan(BucketTypeBlob), func(s *Server, b string) error {
			_, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: b, Key: "k1"})
			return err
		}},
		{"BLOB_WRITE", typesOtherThan(BucketTypeBlob), func(s *Server, b string) error {
			_, err := s.BlobWriteCtx(ctx, BlobWriteRequest{Database: "app", Bucket: b, Key: "k1", Data: []byte("x")})
			return err
		}},
		{"UPDATE", typesOtherThan(BucketTypeHash, BucketTypeBTree), func(s *Server, b string) error {
			_, err := s.UpdateCtx(ctx, UpdateRequest{Database: "app", Bucket: b, Key: "k1", Set: map[string]interface{}{"n": int32(9)}})
			return err
		}},
		{"UPDATE multi", []BucketType{BucketTypeBlob}, func(s *Server, b string) error {
			_, err := s.UpdateCtx(ctx, UpdateRequest{Database: "app", Bucket: b, Multi: true, Filter: matchN, Set: map[string]interface{}{"n": int32(9)}})
			return err
		}},
		{"DISTINCT", []BucketType{BucketTypeBlob}, func(s *Server, b string) error {
			_, err := s.DistinctCtx(ctx, DistinctRequest{Database: "app", Bucket: b, Field: "n"})
			return err
		}},
		{"COUNT with a filter", []BucketType{BucketTypeBlob}, func(s *Server, b string) error {
			_, err := s.CountCtx(ctx, CountRequest{Database: "app", Bucket: b, Filter: matchN})
			return err
		}},
		{"SEARCH", []BucketType{BucketTypeBlob}, func(s *Server, b string) error {
			_, err := s.SearchCtx(ctx, SearchRequest{Database: "app", Bucket: b, Filter: matchN})
			return err
		}},
	}

	s := newTestServer(t)
	made := 0
	for _, tc := range cases {
		for _, bucketType := range tc.types {
			t.Run(tc.verb+" on "+string(bucketType), func(t *testing.T) {
				made++
				bucket := fmt.Sprintf("wt%d", made)
				createTestBucket(t, s, "app", bucket, bucketType)
				err := tc.call(s, bucket)
				if err == nil {
					t.Fatalf("%s on a %s bucket answered no error, want a refusal", tc.verb, bucketType)
				}
				if !errors.Is(err, ErrWrongBucketType) {
					t.Errorf("%s on a %s bucket: %q does not carry ErrWrongBucketType", tc.verb, bucketType, err)
				}
				if got := codeFor(err); got != transport.ErrInvalidArgument {
					t.Errorf("%s on a %s bucket: code %v, want INVALID_ARGUMENT (message %q)", tc.verb, bucketType, got, err)
				}
			})
		}
	}
}
