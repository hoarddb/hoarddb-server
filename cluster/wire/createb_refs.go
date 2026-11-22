package wire

import (
	"context"
	"errors"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// selfReference stands, in a create's references, for the id of the bucket being created.
const selfReference uint64 = 0

// resolveReferences finds, for each field declared 'of' a bucket, the id of that bucket, so
// the declaration can carry it. The target is looked up in the creating database unless it
// is written "<Database>.<Bucket>". A reference holds the target's _id_, which is text in
// this build, so only a string field or a hashStore or btreeStore (whose element keys are
// text) may carry one. Nothing is checked about the documents themselves.
func (n *Node) resolveReferences(ctx context.Context, db, bucket string, fields []Field) (map[string]uint64, error) {
	var references map[string]uint64
	for _, field := range fields {
		if field.Of == "" {
			continue
		}
		targetDB, target, qualified := strings.Cut(field.Of, ".")
		if !qualified {
			targetDB, target = db, targetDB
		}
		args := map[string]string{"field": field.Name, "bucket": bucket, "db": db, "target": target, "tdb": targetDB, "type": field.Type}
		if field.Type != "string" && field.Type != "hashStore" && field.Type != "btreeStore" {
			return nil, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondOfTypeWord, args)}
		}
		if targetDB == db && target == bucket {
			// The bucket being created: its id is drawn at the claim, so the declaration takes it from there.
			if references == nil {
				references = map[string]uint64{}
			}
			references[field.Name] = selfReference
			continue
		}
		far, err := n.bucket(ctx, targetDB, target)
		if errors.Is(err, ErrBucketNotFound) {
			return nil, &cluster.Error{Code: cluster.CodeNotFound, Message: cluster.CodeNotFound.Text(cluster.CondOfBucketNotFound, args)}
		}
		if err != nil {
			return nil, err
		}
		if references == nil {
			references = map[string]uint64{}
		}
		references[field.Name] = far.entry.Decl.BucketID
	}
	return references, nil
}
