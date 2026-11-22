package wire

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// ErrBucketDropping is a refetch that found the bucket's declaration in the dropping state:
// a write to it is refused BUCKET_DROPPING, not SCHEMA_NOT_CURRENT (conv 8 check 4).
var ErrBucketDropping = errors.New("bucket is being dropped")

// refetchCode is the refusal a failed Refetch stands for: a bucket the ring does not know
// is NOT_FOUND and a dropping one BUCKET_DROPPING, each of which the developer fixes
// differently from an unreachable ring (DECL_UNAVAILABLE).
func refetchCode(err error) cluster.Code {
	switch {
	case errors.Is(err, ErrBucketNotFound):
		return cluster.CodeNotFound
	case errors.Is(err, ErrBucketDropping):
		return cluster.CodeBucketDropping
	}
	return cluster.CodeDeclUnavailable
}

// CacheSchema is the Schema the owner path reads: the declaration cache behind a Fetcher. The
// owner's section calls View, which does no I/O; the one transparent re-admission calls
// Refetch outside the section, which is the fill that makes a cold owner fetch instead of
// refusing a forwarded write (conv 8 check 4).
type CacheSchema struct{ fetcher *Fetcher }

// NewCacheSchema returns the Schema over fetcher.
func NewCacheSchema(fetcher *Fetcher) *CacheSchema { return &CacheSchema{fetcher: fetcher} }

var _ Schema = (*CacheSchema)(nil)

// View reads the cache without I/O. A stale entry is returned as it is, with its old Seen, so
// the fence can tell "the database moved" from "never fetched"; nothing is ever Missing,
// because an absent bucket is not cached (conv 20).
func (s *CacheSchema) View(db, bucketID uint64) SchemaView {
	published := s.fetcher.cfg.Published(db)
	if bucketID == machine.SystemBucketID {
		return SchemaView{Present: true, Decl: machine.SystemDeclaration(), Seen: published, Published: published}
	}
	entry, ok := s.fetcher.peekByID(db, bucketID)
	if !ok {
		return SchemaView{Published: published}
	}
	return SchemaView{Present: true, Decl: entry.Decl, Name: entry.Name, Seen: entry.Seen, Published: published}
}

// Refetch fills the cache for a bucket id from the ring. A stale entry already knows its name;
// a cold one reads the reverse-index document first. A bucket that does not exist caches
// nothing and returns ErrBucketNotFound; one that is dropping returns ErrBucketDropping,
// so the owner path can tell them from a failed read, which is ErrDeclUnavailable.
func (s *CacheSchema) Refetch(ctx context.Context, db, bucketID uint64) error {
	if bucketID == machine.SystemBucketID {
		return nil
	}
	published := s.fetcher.cfg.Published(db)
	name := ""
	if entry, ok := s.fetcher.peekByID(db, bucketID); ok {
		name = entry.Name
	} else {
		found := false
		var err error
		if name, found, err = s.fetcher.readName(ctx, db, bucketID); err != nil {
			return fmt.Errorf("refetching bucket id %d: %w", bucketID, err)
		} else if !found {
			return fmt.Errorf("bucket id %d is not declared in database %d: %w", bucketID, db, ErrBucketNotFound)
		}
	}
	entry, err := s.fetcher.Lookup(ctx, db, name, published)
	if err != nil {
		return fmt.Errorf("refetching bucket %q: %w", name, err)
	}
	if entry.Decl.State == machine.StateDropping {
		return fmt.Errorf("bucket %q is being dropped: %w", name, ErrBucketDropping)
	}
	return nil
}
