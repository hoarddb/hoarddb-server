package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// pgPreparedDoc is a document table with the SQL of its statements.
type pgPreparedDoc struct {
	pgDoc
	name      string
	insertSQL string
	countSQL  string
}

func prepareDoc(name string, d pgDoc) *pgPreparedDoc {
	var cols []string
	if d.idCol != "" {
		cols = append(cols, d.idCol)
	}
	for _, f := range d.fields {
		cols = append(cols, f.col)
	}
	params := pgPlaceholders(1, len(cols))
	for _, counter := range d.counters {
		cols = append(cols, counter)
		params += ", 0"
	}
	insert := "INSERT INTO " + pgIdent(d.table) + " (" + pgIdents(cols) + ") VALUES (" + params + ")"
	if d.upsert {
		var updates []string
		for _, f := range d.fields {
			if !contains(d.pk, f.col) {
				updates = append(updates, pgIdent(f.col)+" = EXCLUDED."+pgIdent(f.col))
			}
		}
		insert += " ON CONFLICT (" + pgIdents(d.pk) + ") DO UPDATE SET " + strings.Join(updates, ", ") + " RETURNING (xmax = 0)"
	}
	return &pgPreparedDoc{pgDoc: d, name: name, insertSQL: insert, countSQL: "SELECT count(*) FROM " + pgIdent(d.table)}
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

const (
	pgClaimSQL = `SELECT "seq", "task", "payload" FROM "tasks" WHERE "run_id" = $1 AND "state" = 0 ORDER BY "seq" LIMIT 1 FOR UPDATE SKIP LOCKED`
	pgLeaseSQL = `UPDATE "tasks" SET "state" = 1, "lease_until" = $3 WHERE "run_id" = $1 AND "seq" = $2`
	pgAckSQL   = `UPDATE "tasks" SET "state" = 2 WHERE "run_id" = $1 AND "seq" = $2`
	// pgLeaseSeconds is the lease a claim takes; the benchmark acknowledges at once.
	pgLeaseSeconds = 30
	pgRollbackWait = 5 * time.Second
)

// pgValue is a model value as pgx takes it: text, a 64-bit integer or bytes.
func pgValue(value any) (any, error) {
	switch v := value.(type) {
	case string, []byte, int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	}
	return nil, fmt.Errorf("value %v of type %T is not text, an integer or bytes", value, value)
}

func fieldOf(fields []model.Field, name string) (any, bool) {
	for _, f := range fields {
		if f.Name == name {
			return f.Value, true
		}
	}
	return nil, false
}

// elementArgs returns the parameters of the store's INSERT for an element, in column order,
// and the identity of its row. The body must be exactly the model's fields for the store: the
// schema has no room for a field more or a field less.
func (p *pgPrepared) elementArgs(e model.Element, ordinal int64) ([]any, string, error) {
	if len(e.Body) != len(p.body) {
		return nil, "", fmt.Errorf("element of %s.%s has %d fields, the store has %d", p.bucket, p.name, len(e.Body), len(p.body))
	}
	args := []any{e.Owner}
	if p.seqKeyed() {
		args = append(args, SeqBase+ordinal)
	} else {
		args = append(args, e.Key)
	}
	for _, b := range p.body {
		value, ok := fieldOf(e.Body, b.field)
		if !ok {
			return nil, "", fmt.Errorf("element of %s.%s has no field %q", p.bucket, p.name, b.field)
		}
		switch b.col {
		case p.keyCol:
			if value != e.Key {
				return nil, "", fmt.Errorf("element of %s.%s: field %q is %v but the key is %q", p.bucket, p.name, b.field, value, e.Key)
			}
		case p.ownerCol:
			if value != e.Owner {
				return nil, "", fmt.Errorf("element of %s.%s: field %q is %v but the owner is %q", p.bucket, p.name, b.field, value, e.Owner)
			}
		default:
			converted, err := pgValue(value)
			if err != nil {
				return nil, "", fmt.Errorf("element of %s.%s field %q: %w", p.bucket, p.name, b.field, err)
			}
			args = append(args, converted)
		}
	}
	identity := p.table
	for _, col := range p.pk {
		switch col {
		case p.ownerCol:
			identity += "\x00" + e.Owner
		case p.keyCol:
			identity += "\x00" + e.Key
		default:
			identity += fmt.Sprintf("\x00%d", ordinal)
		}
	}
	return args, identity, nil
}

// docArgs returns the parameters of the document's INSERT and the identity of its row.
func (d *pgPreparedDoc) docArgs(doc model.Document) ([]any, string, error) {
	if len(doc.Fields) != len(d.fields) {
		return nil, "", fmt.Errorf("document %s/%s has %d fields, the bucket has %d", doc.Bucket, doc.ID, len(doc.Fields), len(d.fields))
	}
	var args []any
	if d.idCol != "" {
		args = append(args, doc.ID)
	}
	byCol := map[string]any{}
	for _, f := range d.fields {
		value, ok := fieldOf(doc.Fields, f.field)
		if !ok {
			return nil, "", fmt.Errorf("document %s/%s has no field %q", doc.Bucket, doc.ID, f.field)
		}
		converted, err := pgValue(value)
		if err != nil {
			return nil, "", fmt.Errorf("document %s/%s field %q: %w", doc.Bucket, doc.ID, f.field, err)
		}
		args = append(args, converted)
		byCol[f.col] = converted
	}
	identity := d.table
	for _, col := range d.pk {
		if col == d.idCol {
			identity += "\x00" + doc.ID
			continue
		}
		identity += fmt.Sprintf("\x00%v", byCol[col])
	}
	return args, identity, nil
}

// Write runs one logical write as one transaction.
func (l *pgLayout) Write(ctx context.Context, op WriteOp) (Reply, error) {
	if op.Layout != l.layout {
		return Reply{}, fmt.Errorf("write %s is for layout %s, this database holds %s", op.Kind, op.Layout, l.layout)
	}
	conn, err := l.db.pool.Acquire(ctx)
	if err != nil {
		return Reply{}, fmt.Errorf("write %s %s: acquiring a connection: %w", op.Kind, op.ID, err)
	}
	defer conn.Release()
	var reply Reply
	for _, ref := range op.PreRead {
		if _, err := l.readStore(ctx, conn.Conn(), ref.Bucket, ref.Store, ref.Owner, 0); err != nil {
			return Reply{}, fmt.Errorf("write %s %s: %w", op.Kind, op.ID, err)
		}
		reply.RoundTrips++
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return Reply{}, fmt.Errorf("write %s %s: beginning: %w", op.Kind, op.ID, err)
	}
	reply.RoundTrips++
	apply, err := l.applyWrite(ctx, tx, op, &reply)
	if err != nil {
		return Reply{}, rollbackWith(tx, fmt.Errorf("write %s %s: %w", op.Kind, op.ID, err))
	}
	if !apply {
		if err := tx.Rollback(ctx); err != nil {
			return Reply{}, fmt.Errorf("write %s %s: ending the empty claim: %w", op.Kind, op.ID, err)
		}
		reply.RoundTrips++
		return reply, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return Reply{}, fmt.Errorf("write %s %s: committing: %w", op.Kind, op.ID, err)
	}
	reply.RoundTrips++
	reply.Commits = 1
	return reply, nil
}

// rollbackWith ends a failed transaction and returns the cause, with the rollback's own
// failure joined to it. The rollback does not depend on the request's context, which may be
// the very thing that ended.
func rollbackWith(tx pgx.Tx, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), pgRollbackWait)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return errors.Join(cause, fmt.Errorf("rolling back: %w", err))
	}
	return cause
}

// applyWrite runs the statements of a write in tx. It returns false when a claim found no
// task and nothing was applied.
func (l *pgLayout) applyWrite(ctx context.Context, tx pgx.Tx, op WriteOp, reply *Reply) (bool, error) {
	if op.Claim != nil {
		task, found, err := l.claim(ctx, tx, op.Claim.Run, reply)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		reply.Elements = []model.Element{task}
	}
	created := make([]bool, len(op.Elements))
	// rows remembers whether a row was new, so the second side of a pair (Following and
	// Followers) and a document that names the same row (Follow, Member) do not write it again.
	rows := map[string]bool{}
	for i, e := range op.Elements {
		p, ok := l.stores[e.Bucket+"."+e.Store]
		if !ok {
			return false, fmt.Errorf("layout %s has no store %s.%s", l.layout, e.Bucket, e.Store)
		}
		args, identity, err := p.elementArgs(e, op.Ordinal)
		if err != nil {
			return false, err
		}
		if p.seqKeyed() {
			if _, err := tx.Exec(ctx, p.insertSQL, args...); err != nil {
				return false, fmt.Errorf("pushing onto %s.%s of %s: %w", e.Bucket, e.Store, e.Owner, err)
			}
			reply.RoundTrips++
			created[i] = true
			continue
		}
		if prior, seen := rows[identity]; seen {
			created[i] = prior
			continue
		}
		var inserted bool
		if err := tx.QueryRow(ctx, p.insertSQL, args...).Scan(&inserted); err != nil {
			return false, fmt.Errorf("putting %s into %s.%s of %s: %w", e.Key, e.Bucket, e.Store, e.Owner, err)
		}
		reply.RoundTrips++
		rows[identity] = inserted
		created[i] = inserted
	}
	for _, doc := range op.Documents {
		d, ok := l.docs[doc.Bucket]
		if !ok {
			return false, fmt.Errorf("layout %s has no bucket %s", l.layout, doc.Bucket)
		}
		args, identity, err := d.docArgs(doc)
		if err != nil {
			return false, err
		}
		if !d.upsert {
			if _, err := tx.Exec(ctx, d.insertSQL, args...); err != nil {
				return false, fmt.Errorf("inserting %s/%s: %w", doc.Bucket, doc.ID, err)
			}
			reply.RoundTrips++
			continue
		}
		if _, seen := rows[identity]; seen {
			continue
		}
		var inserted bool
		if err := tx.QueryRow(ctx, d.insertSQL, args...).Scan(&inserted); err != nil {
			return false, fmt.Errorf("upserting %s/%s: %w", doc.Bucket, doc.ID, err)
		}
		reply.RoundTrips++
		rows[identity] = inserted
	}
	return true, l.applyCounters(ctx, tx, op, created, reply)
}

// pgCounterRow is the counters of one document that a write changes.
type pgCounterRow struct {
	table, id string
	deltas    map[string]int64
}

// applyCounters adds the counter deltas of a write, one UPDATE per document, in table and id
// order. A delta applies only when the seeded schema declares the counter and the element it
// names was newly created.
func (l *pgLayout) applyCounters(ctx context.Context, tx pgx.Tx, op WriteOp, created []bool, reply *Reply) error {
	byDocument := map[string]*pgCounterRow{}
	for _, delta := range op.Counters {
		d, ok := l.docs[delta.Bucket]
		if !ok || d.idCol == "" {
			return fmt.Errorf("counter %s of %s/%s: layout %s has no such document table", delta.Field, delta.Bucket, delta.ID, l.layout)
		}
		if !contains(d.counters, delta.Field) {
			continue
		}
		if delta.Element >= len(created) {
			return fmt.Errorf("counter %s of %s/%s names element %d of a write with %d", delta.Field, delta.Bucket, delta.ID, delta.Element, len(created))
		}
		if delta.Element >= 0 && !created[delta.Element] {
			continue
		}
		key := d.table + "\x00" + delta.ID
		row := byDocument[key]
		if row == nil {
			row = &pgCounterRow{table: d.table, id: delta.ID, deltas: map[string]int64{}}
			byDocument[key] = row
		}
		row.deltas[delta.Field] += delta.Delta
	}
	ordered := make([]*pgCounterRow, 0, len(byDocument))
	for _, row := range byDocument {
		ordered = append(ordered, row)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].table != ordered[j].table {
			return ordered[i].table < ordered[j].table
		}
		return ordered[i].id < ordered[j].id
	})
	for _, row := range ordered {
		fields := make([]string, 0, len(row.deltas))
		for field := range row.deltas {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		sets := make([]string, len(fields))
		args := make([]any, 0, len(fields)+1)
		for i, field := range fields {
			sets[i] = fmt.Sprintf("%s = %s + $%d", pgIdent(field), pgIdent(field), i+1)
			args = append(args, row.deltas[field])
		}
		args = append(args, row.id)
		tag, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d", pgIdent(row.table), strings.Join(sets, ", "), pgIdent("id"), len(args)), args...)
		if err != nil {
			return fmt.Errorf("updating the counters of %s %s: %w", row.table, row.id, err)
		}
		reply.RoundTrips++
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("updating the counters of %s %s changed %d rows, want 1: the document is not in the table", row.table, row.id, tag.RowsAffected())
		}
	}
	return nil
}

// claim takes the oldest ready task of a run, leases it and acknowledges it. found is false
// when the queue has no ready task.
func (l *pgLayout) claim(ctx context.Context, tx pgx.Tx, run string, reply *Reply) (model.Element, bool, error) {
	var seq int64
	var task, payload string
	err := tx.QueryRow(ctx, pgClaimSQL, run).Scan(&seq, &task, &payload)
	reply.RoundTrips++
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Element{}, false, nil
	}
	if err != nil {
		return model.Element{}, false, fmt.Errorf("claiming a task of %s: %w", run, err)
	}
	if _, err := tx.Exec(ctx, pgLeaseSQL, run, seq, time.Now().Unix()+pgLeaseSeconds); err != nil {
		return model.Element{}, false, fmt.Errorf("leasing task %d of %s: %w", seq, run, err)
	}
	reply.RoundTrips++
	if _, err := tx.Exec(ctx, pgAckSQL, run, seq); err != nil {
		return model.Element{}, false, fmt.Errorf("acknowledging task %d of %s: %w", seq, run, err)
	}
	reply.RoundTrips++
	claimed := model.Element{Bucket: "Run", Owner: run, Store: "Tasks", Seq: seq,
		Body: []model.Field{{Name: "task", Value: task}, {Name: "payload", Value: payload}}}
	return claimed, true, nil
}

// Count answers the post-run audit in one batch: it runs outside the measured window.
func (l *pgLayout) Count(ctx context.Context, refs []StoreRef) (map[StoreRef]int64, error) {
	conn, err := l.db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("count: acquiring a connection: %w", err)
	}
	defer conn.Release()
	batch := &pgx.Batch{}
	for _, ref := range refs {
		if ref.Owner == "" && ref.Store == "" {
			d, ok := l.docs[ref.Bucket]
			if !ok {
				return nil, fmt.Errorf("count: layout %s has no bucket %s", l.layout, ref.Bucket)
			}
			batch.Queue(d.countSQL)
			continue
		}
		p, ok := l.stores[ref.Bucket+"."+ref.Store]
		if !ok {
			return nil, fmt.Errorf("count: layout %s has no store %s.%s", l.layout, ref.Bucket, ref.Store)
		}
		batch.Queue(p.countSQL, ref.Owner)
	}
	results := conn.SendBatch(ctx, batch)
	counts := make(map[StoreRef]int64, len(refs))
	for _, ref := range refs {
		var n int64
		if err := results.QueryRow().Scan(&n); err != nil {
			return nil, errors.Join(fmt.Errorf("count of %v: %w", ref, err), results.Close())
		}
		counts[ref] = n
	}
	if err := results.Close(); err != nil {
		return nil, fmt.Errorf("count: closing the batch: %w", err)
	}
	return counts, nil
}
