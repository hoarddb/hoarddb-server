package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// mysqlTx is one attempt of a write transaction. It counts the statements it sends.
type mysqlTx struct {
	ctx   context.Context
	tx    *sql.Tx
	trips int
}

func (t *mysqlTx) exec(query string, args ...any) (sql.Result, error) {
	t.trips++
	return t.tx.ExecContext(t.ctx, query, args...)
}

// Write runs one write operation as one transaction, after its PreRead selects.
func (o *mysqlOps) Write(ctx context.Context, op WriteOp) (Reply, error) {
	var reply Reply
	for _, ref := range op.PreRead {
		if _, err := o.readStore(ctx, ref.Bucket+"."+ref.Store, ref.Owner, 0); err != nil {
			return reply, fmt.Errorf("write %s: reading %s/%s/%s first: %w", op.Kind, ref.Bucket, ref.Owner, ref.Store, err)
		}
		reply.RoundTrips++
	}
	var claimed []model.Element
	var committed bool
	retries, err := retryDeadlocks(func() error {
		trips, got, done, err := o.attempt(ctx, op)
		reply.RoundTrips += trips // every statement of a rerun is work the engine did
		claimed, committed = got, done
		return err
	})
	reply.Retries = retries
	if err != nil {
		return reply, fmt.Errorf("write %s %s: %w", op.Kind, op.ID, err)
	}
	reply.Elements = claimed
	if committed {
		reply.Commits = 1
		reply.RoundTrips++ // COMMIT
	}
	return reply, nil
}

// mysqlDeadlockAttempts bounds how often one write transaction is run: the first try and the
// reruns after a deadlock.
const mysqlDeadlockAttempts = 4

// isMySQLDeadlock reports a deadlock victim, error 1213, the error MySQL says to retry.
func isMySQLDeadlock(err error) bool {
	var driverErr *mysqlDriver.MySQLError
	return errors.As(err, &driverErr) && driverErr.Number == 1213
}

// retryDeadlocks runs one write transaction again when it was the victim of a deadlock, at most
// mysqlDeadlockAttempts runs in all, and returns how many reruns there were so the caller counts
// them in Reply.Retries (the runner labels a record that has any). No other error is retried.
func retryDeadlocks(run func() error) (retries int, err error) {
	for attempt := 1; ; attempt++ {
		err = run()
		if err == nil || !isMySQLDeadlock(err) {
			return retries, err
		}
		if attempt == mysqlDeadlockAttempts {
			return retries, fmt.Errorf("deadlock victim on every one of %d attempts (retried %d times): %w", attempt, retries, err)
		}
		retries++
	}
}

// attempt runs the transaction once; a deadlock victim comes back as an error. trips counts the statements sent, BEGIN included and
// COMMIT not (the caller adds it when the commit succeeded). committed is false when nothing
// was applied (a claim that found nothing).
func (o *mysqlOps) attempt(ctx context.Context, op WriteOp) (trips int, claimed []model.Element, committed bool, err error) {
	sqlTx, err := o.conn.BeginTx(ctx, nil)
	if err != nil {
		return 1, nil, false, fmt.Errorf("beginning the transaction: %w", err)
	}
	tx := &mysqlTx{ctx: ctx, tx: sqlTx, trips: 1}
	finish := func(err error) (int, []model.Element, bool, error) {
		_ = sqlTx.Rollback() // the error being returned is the cause; a failed rollback only closes the connection
		return tx.trips, nil, false, err
	}
	if op.Claim != nil {
		task, found, err := o.claim(tx, op.Claim.Run)
		if err != nil {
			return finish(err)
		}
		if !found {
			return finish(nil)
		}
		claimed = []model.Element{task}
	}
	created := make([]bool, len(op.Elements))
	rows := map[string]bool{}
	if err := o.writeDocuments(tx, op, rows); err != nil {
		return finish(err)
	}
	if err := o.writeElements(tx, op, rows, created); err != nil {
		return finish(err)
	}
	if err := o.writeCounters(tx, op, created); err != nil {
		return finish(err)
	}
	if err := sqlTx.Commit(); err != nil {
		return tx.trips + 1, nil, false, fmt.Errorf("committing: %w", err)
	}
	return tx.trips, claimed, true, nil
}

// claim takes the oldest ready task of a run, leases it and acknowledges it.
func (o *mysqlOps) claim(tx *mysqlTx, run string) (model.Element, bool, error) {
	tasks := o.lay.qdb + ".`tasks`"
	tx.trips++
	var seq int64
	var task, payload string
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT `seq`, `task`, `payload` FROM "+tasks+
		" WHERE `run_id` = ? AND `state` = 0 ORDER BY `seq` LIMIT 1 FOR UPDATE SKIP LOCKED", run).Scan(&seq, &task, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Element{}, false, nil
	}
	if err != nil {
		return model.Element{}, false, fmt.Errorf("claiming a task of %s: %w", run, err)
	}
	if _, err := tx.exec("UPDATE "+tasks+" SET `state` = 1, `lease_until` = UNIX_TIMESTAMP() + 30 WHERE `run_id` = ? AND `seq` = ?", run, seq); err != nil {
		return model.Element{}, false, fmt.Errorf("leasing task %d of %s: %w", seq, run, err)
	}
	if _, err := tx.exec("UPDATE "+tasks+" SET `state` = 2 WHERE `run_id` = ? AND `seq` = ?", run, seq); err != nil {
		return model.Element{}, false, fmt.Errorf("acknowledging task %d of %s: %w", seq, run, err)
	}
	return model.Element{Bucket: "Run", Owner: run, Store: "Tasks",
		Body: []model.Field{{Name: "task", Value: task}, {Name: "payload", Value: payload}}}, true, nil
}

// mysqlFieldValue finds a body field by name.
func mysqlFieldValue(fields []model.Field, name string) (any, bool) {
	for _, f := range fields {
		if f.Name == name {
			return f.Value, true
		}
	}
	return nil, false
}

// writeDocuments inserts the documents of a write. rows records, by row identity, whether the
// row was created, so a paired element naming the same row does not write it twice.
func (o *mysqlOps) writeDocuments(tx *mysqlTx, op WriteOp, rows map[string]bool) error {
	for _, doc := range op.Documents {
		bd := o.lay.docs[doc.Bucket]
		if bd == nil || bd.write == nil {
			return fmt.Errorf("layout %s: the MySQL adapter does not write %s documents", o.lay.layout, doc.Bucket)
		}
		if len(doc.Fields) != len(bd.write.fields) {
			return fmt.Errorf("document %s/%s has %d fields, the table %s takes %d", doc.Bucket, doc.ID, len(doc.Fields), bd.table, len(bd.write.fields))
		}
		var args []any
		if bd.idCol != "" {
			args = append(args, doc.ID)
		}
		for _, f := range bd.write.fields {
			value, ok := mysqlFieldValue(doc.Fields, f.field)
			if !ok {
				return fmt.Errorf("document %s/%s has no field %s", doc.Bucket, doc.ID, f.field)
			}
			args = append(args, value)
		}
		identity := bd.table + "\x00" + doc.ID
		if len(bd.pkFields) > 0 {
			parts := []string{bd.table}
			for _, name := range bd.pkFields {
				value, ok := mysqlFieldValue(doc.Fields, name)
				if !ok {
					return fmt.Errorf("document %s/%s has no key field %s", doc.Bucket, doc.ID, name)
				}
				parts = append(parts, fmt.Sprint(value))
			}
			identity = strings.Join(parts, "\x00")
		}
		result, err := tx.exec(bd.insertSQL, args...)
		if err != nil {
			return fmt.Errorf("inserting %s %s: %w", doc.Bucket, doc.ID, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inserting %s %s: %w", doc.Bucket, doc.ID, err)
		}
		rows[identity] = affected == 1
	}
	return nil
}

// writeElements applies the elements in order: a push inserts seq = SeqBase + Ordinal, a put
// is an upsert on the key. created[i] is true when element i made its row.
func (o *mysqlOps) writeElements(tx *mysqlTx, op WriteOp, rows map[string]bool, created []bool) error {
	for i, element := range op.Elements {
		name := element.Bucket + "." + element.Store
		bs := o.lay.stores[name]
		if bs == nil {
			return fmt.Errorf("layout %s has no MySQL mapping for store %s", o.lay.layout, name)
		}
		if len(element.Body) != len(bs.body) {
			return fmt.Errorf("element of %s has %d fields, the table %s takes %d", name, len(element.Body), bs.storeMap.table, len(bs.body))
		}
		if !bs.pushed() {
			if made, seen := rows[bs.rowKey(element.Owner, element.Key)]; seen {
				created[i] = made
				continue
			}
		}
		args := []any{element.Owner}
		if bs.pushed() {
			args = append(args, SeqBase+op.Ordinal)
		} else {
			args = append(args, element.Key)
		}
		for _, b := range bs.body {
			if b.col == bs.keyCol {
				continue
			}
			value, ok := mysqlFieldValue(element.Body, b.field)
			if !ok {
				return fmt.Errorf("element of %s/%s has no field %s", name, element.Owner, b.field)
			}
			args = append(args, value)
		}
		result, err := tx.exec(bs.insertSQL, args...)
		if err != nil {
			return fmt.Errorf("writing %s of %s: %w", name, element.Owner, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("writing %s of %s: %w", name, element.Owner, err)
		}
		created[i] = bs.pushed() || affected == 1
		if !bs.pushed() {
			rows[bs.rowKey(element.Owner, element.Key)] = created[i]
		}
	}
	return nil
}

// mysqlCounterRow is the deltas of one document.
type mysqlCounterRow struct {
	bucket, id string
	fields     []string
	delta      map[string]int64
}

// counterRows folds the deltas a write applies into one row per document, ordered by table
// and id so two transactions that touch the same documents lock them in the same order.
func (l *mysqlLayout) counterRows(op WriteOp, created []bool) ([]*mysqlCounterRow, error) {
	byDoc := map[string]*mysqlCounterRow{}
	for _, d := range op.Counters {
		if d.Element >= len(created) {
			return nil, fmt.Errorf("counter %s.%s names element %d of a write with %d", d.Bucket, d.Field, d.Element, len(created))
		}
		if d.Element >= 0 && !created[d.Element] {
			continue
		}
		bd := l.docs[d.Bucket]
		if bd == nil || bd.idCol == "" {
			return nil, fmt.Errorf("layout %s: counter on %s, which has no document table with an id", l.layout, d.Bucket)
		}
		if !l.declared[bd.table][d.Field] {
			continue
		}
		key := bd.table + "\x00" + d.ID
		row := byDoc[key]
		if row == nil {
			row = &mysqlCounterRow{bucket: d.Bucket, id: d.ID, delta: map[string]int64{}}
			byDoc[key] = row
		}
		if _, seen := row.delta[d.Field]; !seen {
			row.fields = append(row.fields, d.Field)
		}
		row.delta[d.Field] += d.Delta
	}
	ordered := make([]*mysqlCounterRow, 0, len(byDoc))
	for _, row := range byDoc {
		ordered = append(ordered, row)
	}
	sort.Slice(ordered, func(i, j int) bool {
		ti, tj := l.docs[ordered[i].bucket].table, l.docs[ordered[j].bucket].table
		if ti != tj {
			return ti < tj
		}
		return ordered[i].id < ordered[j].id
	})
	return ordered, nil
}

// writeCounters adds the counter deltas the live schema declares, one UPDATE per document.
func (o *mysqlOps) writeCounters(tx *mysqlTx, op WriteOp, created []bool) error {
	order, err := o.lay.counterRows(op, created)
	if err != nil {
		return err
	}
	for _, row := range order {
		bd := o.lay.docs[row.bucket]
		sets := make([]string, len(row.fields))
		args := make([]any, 0, len(row.fields)+1)
		for i, field := range row.fields {
			sets[i] = quoteIdent(field) + " = " + quoteIdent(field) + " + ?"
			args = append(args, row.delta[field])
		}
		args = append(args, row.id)
		result, err := tx.exec("UPDATE "+o.lay.qdb+"."+quoteIdent(bd.table)+" SET "+strings.Join(sets, ", ")+" WHERE "+quoteIdent(bd.idCol)+" = ?", args...)
		if err != nil {
			return fmt.Errorf("updating the counters of %s %s: %w", row.bucket, row.id, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("updating the counters of %s %s: %w", row.bucket, row.id, err)
		}
		if affected == 0 {
			return fmt.Errorf("the counters of %s %s were not updated: no such document in table %s", row.bucket, row.id, bd.table)
		}
	}
	return nil
}

// Count returns the rows of each store, or of each bucket's table when Owner and Store are empty.
func (o *mysqlOps) Count(ctx context.Context, refs []StoreRef) (map[StoreRef]int64, error) {
	out := make(map[StoreRef]int64, len(refs))
	for _, ref := range refs {
		var count int64
		var err error
		if ref.Store == "" {
			bd := o.lay.docs[ref.Bucket]
			if bd == nil {
				return nil, fmt.Errorf("layout %s has no MySQL table for documents of %s", o.lay.layout, ref.Bucket)
			}
			err = o.conn.QueryRowContext(ctx, bd.countSQL).Scan(&count)
		} else {
			bs, serr := o.store(ref.Bucket + "." + ref.Store)
			if serr != nil {
				return nil, serr
			}
			err = o.conn.QueryRowContext(ctx, bs.countSQL, ref.Owner).Scan(&count)
		}
		if err != nil {
			return nil, fmt.Errorf("counting %s/%s/%s: %w", ref.Bucket, ref.Owner, ref.Store, err)
		}
		out[ref] = count
	}
	return out, nil
}
