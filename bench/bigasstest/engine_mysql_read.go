package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// mysqlLayout is one layout in one database: the statements built from the hard-coded maps
// and the live column sets. It is immutable once built, so sessions share it.
type mysqlLayout struct {
	layout model.Layout
	qdb    string
	// declared is the live column set of each table.
	declared map[string]map[string]bool
	stores   map[string]*boundStore
	docs     map[string]*boundDoc

	feedSQL, followScanSQL, roomMembersSQL string
	// feedFromSQL is the feed read from a seq: the range form of an offset.
	feedFromSQL string
}

// boundStore is a storeMap with its statements.
type boundStore struct {
	storeMap
	bucket, store string
	readSQL       string
	// fromSQL is the read of a page that starts inside the store: the range seq >= ? of a
	// pushed store, or key > ? of a keyed one, on the primary key, so skipped rows are not scanned.
	fromSQL   string
	countSQL  string
	insertSQL string
}

// boundDoc is a docMap with its statements.
type boundDoc struct {
	docMap
	bucket    string
	countSQL  string
	insertSQL string
	// pkFields are the document fields that make up the primary key of a document without an id column.
	pkFields []string
}

func (b *boundStore) table(qdb string) string { return qdb + "." + quoteIdent(b.storeMap.table) }

func newMySQLLayout(l model.Layout, database string, declared map[string]map[string]bool) *mysqlLayout {
	lay := &mysqlLayout{layout: l, qdb: quoteIdent(database), declared: declared,
		stores: map[string]*boundStore{}, docs: map[string]*boundDoc{}}
	for name, sm := range mysqlStores[l] {
		bucket, store, _ := strings.Cut(name, ".")
		lay.stores[name] = bindStore(lay.qdb, bucket, store, sm)
	}
	for bucket, dm := range mysqlDocs[l] {
		lay.docs[bucket] = bindDoc(lay.qdb, bucket, dm)
	}
	t := func(name string) string { return lay.qdb + "." + quoteIdent(name) }
	switch l {
	case model.Social:
		lay.feedSQL = "SELECT f.`post_id`, f.`at`, p.`author`, p.`text`, p.`at` FROM " + t("feed") + " f JOIN " + t("posts") +
			" p ON p.`id` = f.`post_id` WHERE f.`user_id` = ? ORDER BY f.`seq`"
		lay.feedFromSQL = "SELECT f.`post_id`, f.`at`, p.`author`, p.`text`, p.`at` FROM " + t("feed") + " f JOIN " + t("posts") +
			" p ON p.`id` = f.`post_id` WHERE f.`user_id` = ? AND f.`seq` >= ? ORDER BY f.`seq`"
		lay.followScanSQL = "SELECT `follower_id`, `at` FROM " + t("follows") + " WHERE `followee_id` = ? ORDER BY `follower_id`"
	case model.Chat:
		lay.roomMembersSQL = "SELECT m.`user_id`, m.`since`, u.`name` FROM " + t("members") + " m JOIN " + t("users") +
			" u ON u.`id` = m.`user_id` WHERE m.`room_id` = ? ORDER BY m.`user_id`"
	}
	return lay
}

func bindStore(qdb, bucket, store string, sm storeMap) *boundStore {
	bs := &boundStore{storeMap: sm, bucket: bucket, store: store}
	table := bs.table(qdb)
	order := "`seq`"
	selects := []string{}
	if !sm.pushed() {
		order = quoteIdent(sm.keyCol)
		selects = append(selects, quoteIdent(sm.keyCol))
	}
	for _, b := range sm.body {
		selects = append(selects, quoteIdent(b.col))
	}
	bs.readSQL = "SELECT " + strings.Join(selects, ", ") + " FROM " + table + " WHERE " + quoteIdent(sm.ownerCol) + " = ? ORDER BY " + order
	from := "`seq` >= ?"
	if !sm.pushed() {
		from = quoteIdent(sm.keyCol) + " > ?"
	}
	bs.fromSQL = "SELECT " + strings.Join(selects, ", ") + " FROM " + table + " WHERE " + quoteIdent(sm.ownerCol) + " = ? AND " + from + " ORDER BY " + order
	bs.countSQL = "SELECT COUNT(*) FROM " + table + " WHERE " + quoteIdent(sm.ownerCol) + " = ?"
	if sm.liveOnly != "" {
		bs.countSQL += " AND " + sm.liveOnly
	}
	cols := []string{quoteIdent(sm.ownerCol)}
	marks := []string{"?"}
	var updates []string
	if sm.pushed() {
		cols, marks = append(cols, "`seq`"), append(marks, "?")
	} else {
		cols, marks = append(cols, quoteIdent(sm.keyCol)), append(marks, "?")
	}
	for _, b := range sm.body {
		if b.col == sm.keyCol {
			continue
		}
		cols, marks = append(cols, quoteIdent(b.col)), append(marks, "?")
		updates = append(updates, quoteIdent(b.col)+" = VALUES("+quoteIdent(b.col)+")")
	}
	for _, col := range sm.fixed {
		cols, marks = append(cols, quoteIdent(col)), append(marks, "0")
	}
	bs.insertSQL = "INSERT INTO " + table + " (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(marks, ", ") + ")"
	if !sm.pushed() {
		if len(updates) == 0 {
			updates = []string{quoteIdent(sm.ownerCol) + " = " + quoteIdent(sm.ownerCol)}
		}
		bs.insertSQL += " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ", ")
	}
	return bs
}

func bindDoc(qdb, bucket string, dm docMap) *boundDoc {
	bd := &boundDoc{docMap: dm, bucket: bucket}
	table := qdb + "." + quoteIdent(dm.table)
	bd.countSQL = "SELECT COUNT(*) FROM " + table
	if dm.write == nil {
		return bd
	}
	var cols, marks, updates []string
	if dm.idCol != "" {
		cols, marks = append(cols, quoteIdent(dm.idCol)), append(marks, "?")
	}
	for _, f := range dm.write.fields {
		cols, marks = append(cols, quoteIdent(f.col)), append(marks, "?")
	}
	for _, col := range dm.write.zero {
		cols, marks = append(cols, quoteIdent(col)), append(marks, "0")
	}
	for _, col := range dm.write.upsert {
		updates = append(updates, quoteIdent(col)+" = VALUES("+quoteIdent(col)+")")
	}
	if len(updates) == 0 {
		updates = []string{cols[0] + " = " + cols[0]}
	}
	bd.insertSQL = "INSERT INTO " + table + " (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(marks, ", ") + ") ON DUPLICATE KEY UPDATE " + strings.Join(updates, ", ")
	for _, col := range dm.write.pk {
		for _, f := range dm.write.fields {
			if f.col == col {
				bd.pkFields = append(bd.pkFields, f.field)
			}
		}
	}
	return bd
}

// mysqlOps runs the operations of one layout on one pinned connection.
type mysqlOps struct {
	engine *mysqlEngine
	conn   *sql.Conn
	lay    *mysqlLayout
}

// scanTargets returns pointers to scan into, one per kind, and a function reading them back
// as field values.
func scanTargets(kinds []colKind) ([]any, func() []any) {
	dest := make([]any, len(kinds))
	texts := make([]string, len(kinds))
	ints := make([]int64, len(kinds))
	blobs := make([][]byte, len(kinds))
	for i, kind := range kinds {
		switch kind {
		case kindInt:
			dest[i] = &ints[i]
		case kindBytes:
			dest[i] = &blobs[i]
		default:
			dest[i] = &texts[i]
		}
	}
	return dest, func() []any {
		values := make([]any, len(kinds))
		for i, kind := range kinds {
			switch kind {
			case kindInt:
				values[i] = ints[i]
			case kindBytes:
				values[i] = append([]byte(nil), blobs[i]...)
			default:
				values[i] = texts[i]
			}
		}
		return values
	}
}

func (o *mysqlOps) store(name string) (*boundStore, error) {
	bs := o.lay.stores[name]
	if bs == nil {
		return nil, fmt.Errorf("layout %s has no MySQL mapping for store %s", o.lay.layout, name)
	}
	return bs, nil
}

func withLimit(query string, limit int) string {
	if limit > 0 {
		return query + " LIMIT " + strconv.Itoa(limit)
	}
	return query
}

// readStore reads a store of one owner, oldest first (by key for a keyed store).
func (o *mysqlOps) readStore(ctx context.Context, name, owner string, limit int) ([]model.Element, error) {
	return o.readStorePage(ctx, name, owner, limit, ReadOp{})
}

// readStorePage is readStore for a page that may start inside the store. This adapter reads an
// offset as a range on the primary key and never as OFFSET, so the skipped rows are not
// scanned: seq >= Offset for a pushed store (seeded seq counts 0, 1, 2, ...), key > AfterKey
// for a keyed one.
func (o *mysqlOps) readStorePage(ctx context.Context, name, owner string, limit int, start ReadOp) ([]model.Element, error) {
	bs, err := o.store(name)
	if err != nil {
		return nil, err
	}
	query, args := bs.readSQL, []any{owner}
	if start.Offset > 0 {
		query = bs.fromSQL
		if bs.pushed() {
			args = append(args, int64(start.Offset))
		} else {
			if start.AfterKey == "" {
				return nil, fmt.Errorf("reading %s of %s at offset %d: the store is keyed and the read gave no AfterKey", name, owner, start.Offset)
			}
			args = append(args, start.AfterKey)
		}
	}
	rows, err := o.conn.QueryContext(ctx, withLimit(query, limit), args...)
	if err != nil {
		return nil, fmt.Errorf("reading %s of %s: %w", name, owner, err)
	}
	defer func() { _ = rows.Close() }()
	kinds := make([]colKind, 0, len(bs.body)+1)
	if !bs.pushed() {
		kinds = append(kinds, kindText)
	}
	for _, b := range bs.body {
		kinds = append(kinds, b.kind)
	}
	dest, values := scanTargets(kinds)
	var out []model.Element
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("reading %s of %s: %w", name, owner, err)
		}
		got := values()
		element := model.Element{Bucket: bs.bucket, Owner: owner, Store: bs.store}
		if !bs.pushed() {
			key, ok := got[0].(string)
			if !ok {
				return nil, fmt.Errorf("reading %s of %s: the key column holds a %T, want text", name, owner, got[0])
			}
			element.Key = key
			got = got[1:]
		}
		element.Body = make([]model.Field, len(bs.body))
		for i, b := range bs.body {
			element.Body[i] = model.Field{Name: b.field, Value: got[i]}
		}
		out = append(out, element)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading %s of %s: %w", name, owner, err)
	}
	return out, nil
}

// ReadStore implements StoreReader: every element of one store of one document.
func (o *mysqlOps) ReadStore(ctx context.Context, ref StoreRef) ([]model.Element, error) {
	return o.readStore(ctx, ref.Bucket+"."+ref.Store, ref.Owner, 0)
}

// mysqlCounterColumns lists the counter columns (names ending _n or _total) of a table, in
// table order, from the hard-coded column list.
func mysqlCounterColumns(layout model.Layout, table string) []string {
	var counters []string
	for _, column := range mysqlTables[layout][table] {
		if strings.HasSuffix(column, "_n") || strings.HasSuffix(column, "_total") {
			counters = append(counters, column)
		}
	}
	return counters
}

// ReadCounters implements CounterReader: the counter columns of one document, by column name.
func (o *mysqlOps) ReadCounters(ctx context.Context, bucket, id string) (map[string]int64, error) {
	bd := o.lay.docs[bucket]
	if bd == nil || bd.idCol == "" {
		return nil, fmt.Errorf("layout %s: %s has no document table with an id to read counters from", o.lay.layout, bucket)
	}
	columns := mysqlCounterColumns(o.lay.layout, bd.table)
	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s declares no counter columns", bd.table)
	}
	quoted := make([]string, len(columns))
	values := make([]int64, len(columns))
	dest := make([]any, len(columns))
	for i, column := range columns {
		quoted[i], dest[i] = quoteIdent(column), &values[i]
	}
	err := o.conn.QueryRowContext(ctx, "SELECT "+strings.Join(quoted, ", ")+" FROM "+o.lay.qdb+"."+quoteIdent(bd.table)+
		" WHERE "+quoteIdent(bd.idCol)+" = ?", id).Scan(dest...)
	if err != nil {
		return nil, fmt.Errorf("reading the counters of %s %s: %w", bucket, id, err)
	}
	out := make(map[string]int64, len(columns))
	for i, column := range columns {
		out[column] = values[i]
	}
	return out, nil
}

// Read runs one read operation.
func (o *mysqlOps) Read(ctx context.Context, op ReadOp) (Reply, error) {
	switch op.Kind {
	case ReadSocialFeed:
		return o.readFeed(ctx, op)
	case ReadSocialInbox:
		return o.readPlain(ctx, "User.Inbox", op)
	case ReadSocialFollowers:
		return o.readPlain(ctx, "User.Followers", op)
	case ReadSocialFollowScan:
		return o.readFollowScan(ctx, op)
	case ReadChatRoomPage:
		return o.readRoomPage(ctx, op)
	case ReadChatUserRooms:
		return o.readPlain(ctx, "User.Rooms", op)
	case ReadLedgerPage, ReadLedgerAll:
		return o.readPlain(ctx, "Account.Expenses", op)
	case ReadLedgerTotals:
		return o.readTotals(ctx, op)
	case ReadLedgerInvoices:
		return o.readPlain(ctx, "Account.Invoices", op)
	case ReadAgentMemories:
		return o.readPlain(ctx, "User.Memories", op)
	case ReadAgentSession:
		return o.readPlain(ctx, "Session.Messages", op)
	}
	return Reply{}, fmt.Errorf("mysql adapter: unknown read kind %s", op.Kind)
}

func (o *mysqlOps) readPlain(ctx context.Context, name string, op ReadOp) (Reply, error) {
	elements, err := o.readStorePage(ctx, name, op.Entity, op.Limit, op)
	if err != nil {
		return Reply{}, err
	}
	return Reply{Elements: elements, RoundTrips: 1}, nil
}

// readFeed is the feed page and the post of each element in one join.
func (o *mysqlOps) readFeed(ctx context.Context, op ReadOp) (Reply, error) {
	query, args := o.lay.feedSQL, []any{op.Entity}
	if op.Offset > 0 {
		query, args = o.lay.feedFromSQL, append(args, int64(op.Offset))
	}
	rows, err := o.conn.QueryContext(ctx, withLimit(query, op.Limit), args...)
	if err != nil {
		return Reply{}, fmt.Errorf("reading the feed of %s: %w", op.Entity, err)
	}
	defer func() { _ = rows.Close() }()
	dest, values := scanTargets([]colKind{kindText, kindInt, kindText, kindText, kindInt})
	var reply Reply
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return Reply{}, fmt.Errorf("reading the feed of %s: %w", op.Entity, err)
		}
		got := values()
		reply.Elements = append(reply.Elements, model.Element{Bucket: "User", Owner: op.Entity, Store: "Feed",
			Body: []model.Field{{Name: "post", Value: got[0]}, {Name: "at", Value: got[1]}}})
		reply.Documents = append(reply.Documents, model.Document{Bucket: "Post", ID: got[0].(string),
			Fields: []model.Field{{Name: "Author", Value: got[2]}, {Name: "Text", Value: got[3]}, {Name: "At", Value: got[4]}}})
	}
	if err := rows.Err(); err != nil {
		return Reply{}, fmt.Errorf("reading the feed of %s: %w", op.Entity, err)
	}
	reply.RoundTrips = 1
	return reply, nil
}

// readFollowScan lists the Follow documents whose To is the entity.
func (o *mysqlOps) readFollowScan(ctx context.Context, op ReadOp) (Reply, error) {
	rows, err := o.conn.QueryContext(ctx, withLimit(o.lay.followScanSQL, op.Limit), op.Entity)
	if err != nil {
		return Reply{}, fmt.Errorf("scanning the follows of %s: %w", op.Entity, err)
	}
	defer func() { _ = rows.Close() }()
	dest, values := scanTargets([]colKind{kindText, kindInt})
	var reply Reply
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return Reply{}, fmt.Errorf("scanning the follows of %s: %w", op.Entity, err)
		}
		got := values()
		from := got[0].(string)
		reply.Documents = append(reply.Documents, model.Document{Bucket: "Follow", ID: from + ">" + op.Entity,
			Fields: []model.Field{{Name: "From", Value: from}, {Name: "To", Value: op.Entity}, {Name: "At", Value: got[1]}}})
	}
	if err := rows.Err(); err != nil {
		return Reply{}, fmt.Errorf("scanning the follows of %s: %w", op.Entity, err)
	}
	reply.RoundTrips = 1
	return reply, nil
}

// readRoomPage is the first page of messages, then every member joined with the user's name.
func (o *mysqlOps) readRoomPage(ctx context.Context, op ReadOp) (Reply, error) {
	messages, err := o.readStorePage(ctx, "Room.Messages", op.Entity, op.Limit, op)
	if err != nil {
		return Reply{}, err
	}
	rows, err := o.conn.QueryContext(ctx, withLimit(o.lay.roomMembersSQL, op.MemberLimit), op.Entity)
	if err != nil {
		return Reply{}, fmt.Errorf("reading the members of %s: %w", op.Entity, err)
	}
	defer func() { _ = rows.Close() }()
	dest, values := scanTargets([]colKind{kindText, kindInt, kindText})
	reply := Reply{Elements: messages, RoundTrips: 2}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return Reply{}, fmt.Errorf("reading the members of %s: %w", op.Entity, err)
		}
		got := values()
		user := got[0].(string)
		reply.Elements = append(reply.Elements, model.Element{Bucket: "Room", Owner: op.Entity, Store: "Members", Key: user,
			Body: []model.Field{{Name: "user", Value: user}, {Name: "since", Value: got[1]}}})
		reply.Documents = append(reply.Documents, model.Document{Bucket: "User", ID: user, Fields: []model.Field{{Name: "Name", Value: got[2]}}})
	}
	if err := rows.Err(); err != nil {
		return Reply{}, fmt.Errorf("reading the members of %s: %w", op.Entity, err)
	}
	return reply, nil
}

// mysqlTotalsSQL is the query a SQL application writes for "what did this account spend by
// category": a GROUP BY over the expenses of one account, not a maintained summary row.
func mysqlTotalsSQL(qdb string, m storeMap) (string, error) {
	category, amount := "", ""
	for _, b := range m.body {
		switch b.field {
		case "category":
			category = b.col
		case "amount":
			amount = b.col
		}
	}
	if category == "" || amount == "" {
		return "", fmt.Errorf("the store %s has no category and amount columns to group", m.table)
	}
	return "SELECT " + quoteIdent(category) + ", CAST(SUM(" + quoteIdent(amount) + ") AS SIGNED), COUNT(*) FROM " + qdb + "." + quoteIdent(m.table) +
		" WHERE " + quoteIdent(m.ownerCol) + " = ? GROUP BY " + quoteIdent(category), nil
}

// readTotals runs ledger.totals_by_category: one GROUP BY, the rows sorted by category here.
func (o *mysqlOps) readTotals(ctx context.Context, op ReadOp) (Reply, error) {
	bs, err := o.store("Account.Expenses")
	if err != nil {
		return Reply{}, err
	}
	query, err := mysqlTotalsSQL(o.lay.qdb, bs.storeMap)
	if err != nil {
		return Reply{}, err
	}
	rows, err := o.conn.QueryContext(ctx, query, op.Entity)
	if err != nil {
		return Reply{}, fmt.Errorf("totals of %s by category: %w", op.Entity, err)
	}
	defer func() { _ = rows.Close() }()
	var totals []CategoryTotal
	for rows.Next() {
		var row CategoryTotal
		if err := rows.Scan(&row.Category, &row.Total, &row.Count); err != nil {
			return Reply{}, fmt.Errorf("scanning the totals of %s: %w", op.Entity, err)
		}
		totals = append(totals, row)
	}
	if err := rows.Err(); err != nil {
		return Reply{}, fmt.Errorf("totals of %s by category: %w", op.Entity, err)
	}
	sort.Slice(totals, func(i, j int) bool { return totals[i].Category < totals[j].Category })
	return Reply{Totals: totals, RoundTrips: 1}, nil
}
