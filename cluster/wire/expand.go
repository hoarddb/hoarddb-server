package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
)

// maxExpandReads bounds the distinct far documents one call reads and holds, so the memory of
// an expansion is the page and not the product of a 10,000-row limit and eight fields.
const maxExpandReads = 1000

// ExpandStats are the node-wide counters of fetch-through: calls, distinct far reads, far
// documents that were missing, and where each far read was served. There is no per-bucket
// series; a bucket never adds a counter.
type ExpandStats struct {
	Calls, Reads, Missing, ServedLocal, ServedOwner atomic.Int64
}

// ExpandStats returns the node's fetch-through counters.
func (n *Node) ExpandStats() *ExpandStats { return &n.expandStats }

// Reasons an expanded reference is not a document.
const (
	expandReasonMissing = "missing"
	expandReasonIDType  = "id_type"
)

// expandFetcher reads one far document from the owner of its slot. The ring server reads
// through Node.readDocument; a test swaps it to count reads or to fail an owner.
type expandFetcher func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error)

// expandSpec is a RingExpandSpec resolved against its far bucket.
type expandSpec struct {
	label   string
	far     bucketRef
	project []uint16
	where   bson.Raw
}

// scalarNames lists the declared scalars of table that a default expansion returns: every
// field that is not a store, without _id_ (a projection always returns _id_).
func scalarNames(table machine.FieldTable) []string {
	var names []string
	for _, entry := range table.Entries {
		if !isStoreType(entry.Type) && entry.Name != idFieldName {
			names = append(names, entry.Name)
		}
	}
	return names
}

// declaredReference is the bucket id a field of near declares itself of. The declaration
// stores references in its field 13; a field with none has to be named with its bucket.
func declaredReference(near bucketRef, field string) (uint64, bool) {
	entry, ok := entryNamed(near.table, field)
	if !ok {
		return 0, false
	}
	return near.entry.Decl.ReferenceOf(entry.ID)
}

// readExpanded is the default far read: through the owner's gate, like any keyed get.
func (n *Node) readExpanded(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
	return n.readDocumentFrom(ctx, ref, encID, project, local)
}

// resolveSpec finds the far bucket of one expansion and refuses what a call may not do:
// expand through a dotted field (depth two), expand a bucket the session may not read, or
// expand a field no declaration and no option names a bucket for.
func (s *ringServer) resolveSpec(ctx context.Context, near bucketRef, spec RingExpandSpec) (expandSpec, error) {
	if strings.Contains(spec.Field, ".") {
		return expandSpec{}, expandDepthRefusal(spec.Field)
	}
	var far bucketRef
	var err error
	switch {
	case spec.Bucket != "":
		database := spec.Database
		if database == "" {
			database = near.db
		}
		if err := s.authz.Authorize(ctx, RingAccess{Database: database, Bucket: spec.Bucket, Class: RingClassGet}); err != nil {
			return expandSpec{}, ringDenied()
		}
		far, err = s.namedBucket(ctx, database, spec.Bucket)
	default:
		bucketID, declared := declaredReference(near, spec.Field)
		if !declared {
			return expandSpec{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondExpandNoBucket, map[string]string{
				"field": spec.Field, "bucket": near.name, "db": near.db,
			})}
		}
		far, err = s.node.bucketByID(ctx, bucketID, func(database, bucket string) error {
			return s.authz.Authorize(ctx, RingAccess{Database: database, Bucket: bucket, Class: RingClassGet})
		})
	}
	if err != nil {
		return expandSpec{}, err
	}
	names := spec.Fields
	if names == nil {
		names = scalarNames(far.table)
	}
	if err := checkExpandWhere(far, names, spec.Where); err != nil {
		return expandSpec{}, err
	}
	project, err := far.projection(names)
	if err != nil {
		return expandSpec{}, err
	}
	return expandSpec{label: spec.Field, far: far, project: project, where: spec.Where}, nil
}

// expandFieldRefusal says why field of near cannot hold a far id, or "": it is not declared, or
// it is a store (a store's element keys are expanded by listing the store).
func expandFieldRefusal(near bucketRef, field string) string {
	entry, ok := entryNamed(near.table, field)
	switch {
	case !ok:
		return fmt.Sprintf("%s.%s declares no field of that name", near.db, near.name)
	case isStoreType(entry.Type):
		return fmt.Sprintf("it is a %s, which holds elements and not an id", entry.Type)
	}
	return ""
}

// expandDepthRefusal is the answer to an expand through a dotted field: one level only.
func expandDepthRefusal(field string) error {
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondExpandDepth, map[string]string{"bucket": field})}
}

// checkExpandWhere refuses a where key that the expansion cannot match: the far document the
// where reads is the projected one, so a key outside names (or a field the far bucket does not
// declare as a scalar) would drop every row without saying why.
func checkExpandWhere(far bucketRef, names []string, where bson.Raw) error {
	if len(where) == 0 {
		return nil
	}
	elements, err := where.Elements()
	if err != nil {
		return ringRefusal("the where of an expand is not a document: %v. Send {Field: value}", err)
	}
	for _, element := range elements {
		key := element.Key()
		if key == idFieldName || slices.Contains(names, key) {
			continue
		}
		why := fmt.Sprintf("%s.%s declares no scalar field of that name", far.db, far.name)
		if entry, ok := entryNamed(far.table, key); ok && !isStoreType(entry.Type) {
			why = "the fields of the expansion leave it out, so the document the where reads does not have it"
		}
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondExpandWhereField, map[string]string{
			"field": key, "why": why, "db": far.db, "bucket": far.name,
		})}
	}
	return nil
}

// checkExpandNearFields refuses an expand over a field of near that cannot hold a far id (not
// declared, or a store), and over one the call's own fields list removed: the field's value is
// the far id, and a row without it would expand to nothing.
func checkExpandNearFields(near bucketRef, fields []string, specs []RingExpandSpec) error {
	for _, spec := range specs {
		if spec.Field == "" {
			continue
		}
		if strings.Contains(spec.Field, ".") {
			return expandDepthRefusal(spec.Field)
		}
		if why := expandFieldRefusal(near, spec.Field); why != "" {
			return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondExpandFieldType, map[string]string{
				"field": spec.Field, "db": near.db, "bucket": near.name, "why": why,
			})}
		}
		if fields == nil || spec.Field == idFieldName || slices.Contains(fields, spec.Field) {
			continue
		}
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondExpandFieldProjected, map[string]string{
			"field": spec.Field, "db": near.db, "bucket": near.name,
		})}
	}
	return nil
}

// expandAnswer is what one reference of one row turned into.
type expandAnswer struct {
	body   []byte
	found  bool
	reason string
}

// expandRows reads the far documents the rows point at and returns one answer document per
// kept row, in row order, and for each answer the index of its input row. ids[i][j] is row
// i's id for spec j; "" is a null or absent reference and expands to nothing. Every far
// bucket is resolved and authorised before any read, distinct ids are grouped by the owner
// of their slot, each group is read concurrently, and the first owner that does not answer
// fails the whole call: a page with silently unexpanded rows would look complete.
func (s *ringServer) expandRows(ctx context.Context, near bucketRef, specs []RingExpandSpec, ids [][]string, local bool) (answers [][]byte, kept []int, filtered int, served string, err error) {
	resolved := make([]expandSpec, len(specs))
	for j, spec := range specs {
		if resolved[j], err = s.resolveSpec(ctx, near, spec); err != nil {
			return nil, nil, 0, "", err
		}
	}
	found, served, err := s.fetchExpansions(ctx, resolved, ids, local)
	if err != nil {
		return nil, nil, 0, "", err
	}
	for i := range ids {
		row := make(bson.D, 0, len(specs))
		keep := true
		for j, spec := range resolved {
			if ids[i][j] == "" {
				// Spec 3.2 step 1: a null or absent reference expands to nothing. Under where it
				// has no far document to match, so the row is filtered.
				if len(spec.where) > 0 {
					keep = false
				}
				continue
			}
			answer, referenced := found[j][ids[i][j]]
			if !referenced {
				answer = expandAnswer{reason: expandReasonMissing}
			}
			if !answer.found {
				s.node.expandStats.Missing.Add(1)
			}
			if len(spec.where) > 0 && (!answer.found || !whereMatches(spec.where, answer.body)) {
				keep = false
			}
			row = append(row, bson.E{Key: spec.label, Value: expandedValue(answer)})
		}
		if !keep {
			filtered++
			continue
		}
		raw, err := bson.Marshal(row)
		if err != nil {
			return nil, nil, 0, "", fmt.Errorf("encoding the expansion of row %d: %w", i, err)
		}
		answers, kept = append(answers, raw), append(kept, i)
	}
	return answers, kept, filtered, served, nil
}

// expandedValue is the document one reference becomes: {expanded: true, doc} or
// {expanded: false, reason}.
func expandedValue(answer expandAnswer) bson.D {
	if answer.found {
		return bson.D{{Key: "expanded", Value: true}, {Key: "doc", Value: bson.Raw(answer.body)}}
	}
	return bson.D{{Key: "expanded", Value: false}, {Key: "reason", Value: answer.reason}}
}

// expandTarget is one (spec, id) cell of the page that a far read answers.
type expandTarget struct {
	spec int
	id   string
}

// farReadKey names one far read: the same bucket, id and projection is read once and answers
// every spec that asked for it, so Author and Editor of one message both of User share it.
type farReadKey struct {
	bucketID   uint64
	id         string
	projection string
}

// fetchExpansions reads every distinct far id of every spec once and returns, per spec, the
// answer of each id. An id that does not encode is an id_type answer and reads nothing.
func (s *ringServer) fetchExpansions(ctx context.Context, specs []expandSpec, ids [][]string, local bool) ([]map[string]expandAnswer, string, error) {
	found := make([]map[string]expandAnswer, len(specs))
	var refs []expandRef
	byRead := map[farReadKey]int{}
	for j, spec := range specs {
		found[j] = map[string]expandAnswer{}
		for _, row := range ids {
			id := row[j]
			if _, done := found[j][id]; done || id == "" {
				continue
			}
			encID, err := store.EncodeComponent(id)
			if err != nil {
				found[j][id] = expandAnswer{reason: expandReasonIDType}
				continue
			}
			found[j][id] = expandAnswer{reason: expandReasonMissing}
			key := farReadKey{spec.far.entry.Decl.BucketID, string(encID), fmt.Sprint(spec.project)}
			at, shared := byRead[key]
			if !shared {
				at = len(refs)
				byRead[key] = at
				refs = append(refs, expandRef{ref: spec.far, encID: encID, slot: spec.far.slot(encID), project: spec.project, local: local, at: len(refs)})
			}
			refs[at].targets = append(refs[at].targets, expandTarget{j, id})
		}
	}
	if len(refs) > maxExpandReads {
		return nil, "", &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondExpandTooManyReads, map[string]string{
			"n": strconv.Itoa(len(refs)), "max": strconv.Itoa(maxExpandReads),
		})}
	}
	ownerOf := s.node.ownerOfSlot
	if s.expandOwner != nil {
		ownerOf = s.expandOwner
	}
	fetch := s.expandFetch
	if fetch == nil {
		fetch = s.node.readExpanded
	}

	caller := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var firstErr error
	servedAll := true
	// readMany answers every read on its own; the policy of an expansion is on top of it: the
	// first error other than a missing document fails the call and cancels the reads left.
	readMany(ctx, refs, readManyPlan{
		ownerOf: ownerOf,
		fetch: func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
			s.node.expandStats.Reads.Add(1)
			return fetch(ctx, ref, encID, project, local)
		},
		settle: func(at int, result manyResult) manyResult {
			ref := refs[at]
			var coded *cluster.Error
			switch err := result.err; {
			case err == nil:
				for _, target := range ref.targets {
					found[target.spec][target.id] = expandAnswer{body: result.doc.Body, found: true}
				}
				if result.doc.Served == servedLocal {
					s.node.expandStats.ServedLocal.Add(1)
				} else {
					s.node.expandStats.ServedOwner.Add(1)
				}
				servedAll = servedAll && result.doc.Served == servedLocal
			case errors.As(err, &coded) && coded.Code == cluster.CodeNotFound:
				// A document that is gone, tombstoned or expired is data, not a failure.
			case firstErr == nil:
				firstErr = fmt.Errorf("expanding %s of %s.%s from %q: %w", specs[ref.targets[0].spec].label, ref.ref.db, ref.ref.name, ownerOf(ref.ref, ref.slot), err)
				cancel()
			}
			return manyResult{read: true}
		},
	})
	if firstErr != nil {
		return nil, "", firstErr
	}
	// A caller that ended between reads leaves ids unread, and a read that ignores its context
	// answers anyway: without this the unread ids would come back as missing documents.
	if err := caller.Err(); err != nil {
		return nil, "", fmt.Errorf("expanding: the call ended before every far read was made: %w", err)
	}
	// One value per call: local only when every far read was answered by a copy here.
	served := ""
	switch {
	case len(refs) == 0:
	case servedAll:
		served = servedLocal
	default:
		served = servedOwner
	}
	return found, served, nil
}

// whereMatches reports whether every equality of where holds on body.
func whereMatches(where bson.Raw, body []byte) bool {
	elements, err := where.Elements()
	if err != nil {
		return false
	}
	for _, element := range elements {
		got, err := bson.Raw(body).LookupErr(element.Key())
		if err != nil || !sameScalar(got, element.Value()) {
			return false
		}
	}
	return true
}

// sameScalar compares two BSON values: equal bytes and type, or two numbers of equal value.
func sameScalar(a, b bson.RawValue) bool {
	if a.Type == b.Type {
		return bytes.Equal(a.Value, b.Value)
	}
	left, leftNumber := numberOf(a)
	right, rightNumber := numberOf(b)
	return leftNumber && rightNumber && left == right
}

func numberOf(scalar bson.RawValue) (float64, bool) {
	switch scalar.Type {
	case bson.TypeInt32:
		return float64(scalar.Int32()), true
	case bson.TypeInt64:
		return float64(scalar.Int64()), true
	case bson.TypeDouble:
		return scalar.Double(), true
	}
	return 0, false
}
