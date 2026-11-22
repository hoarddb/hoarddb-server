package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// pgQuerier is what a read runs on: a connection or a transaction.
type pgQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// pgLayout runs the operations of one layout on its database.
type pgLayout struct {
	db     *pgDatabase
	layout model.Layout
	stores map[string]*pgPrepared
	docs   map[string]*pgPreparedDoc
}

// pgPrepared is a store with the SQL text of its statements, built once.
type pgPrepared struct {
	pgStore
	bucket, name string
	// sel are the selected columns: seq or key first, then the body columns.
	sel               []string
	index             map[string]int
	kinds             map[string]byte
	pageSQL, limitSQL string
	// fromLimitSQL is a page that starts inside the store: the range seq >= $2 of a pushed store
	// or key > $2 of a keyed one, then LIMIT $3. The range runs on the primary key, so the
	// skipped rows are not scanned.
	fromLimitSQL string
	insertSQL    string
	countSQL     string
}

func newPostgresLayout(db *pgDatabase, layout model.Layout) *pgLayout {
	l := &pgLayout{db: db, layout: layout, stores: map[string]*pgPrepared{}, docs: map[string]*pgPreparedDoc{}}
	for name, s := range pgMaps[layout].stores {
		l.stores[name] = prepareStore(layout, name, s, db.collateC)
	}
	for name, d := range pgMaps[layout].docs {
		l.docs[name] = prepareDoc(name, d)
	}
	return l
}

func prepareStore(layout model.Layout, name string, s pgStore, collateC bool) *pgPrepared {
	bucket, store, _ := strings.Cut(name, ".")
	p := &pgPrepared{pgStore: s, bucket: bucket, name: store, index: map[string]int{}, kinds: map[string]byte{"seq": pgInt}}
	if s.seqKeyed() {
		p.sel = []string{"seq"}
	} else {
		p.sel = []string{s.keyCol}
		p.kinds[s.keyCol] = pgText
	}
	for _, b := range s.bodyCols() {
		p.sel = append(p.sel, b.col)
		p.kinds[b.col] = b.kind
	}
	for i, col := range p.sel {
		p.index[col] = i
	}
	kind, _ := KindOf(layout, bucket, store) // the map test proves every store is in the model
	var order string
	switch {
	case s.seqKeyed():
		order = " ORDER BY seq"
	case kind.Keyed() && collateC:
		order = " ORDER BY " + pgIdent(s.keyCol)
	case kind.Keyed():
		order = " ORDER BY " + pgIdent(s.keyCol) + ` COLLATE "C"`
	}
	where := pgIdent(s.ownerCol) + " = $1"
	if s.live != "" {
		where += " AND " + s.live
	}
	base := "SELECT " + pgIdents(p.sel) + " FROM " + pgIdent(s.table) + " WHERE " + where + order
	p.pageSQL = base
	p.limitSQL = base + " LIMIT $2"
	from := "seq >= $2"
	if !s.seqKeyed() {
		from = pgIdent(s.keyCol) + ` COLLATE "C" > $2`
		if collateC {
			from = pgIdent(s.keyCol) + " > $2"
		}
	}
	p.fromLimitSQL = "SELECT " + pgIdents(p.sel) + " FROM " + pgIdent(s.table) + " WHERE " + where + " AND " + from + order + " LIMIT $3"
	p.countSQL = "SELECT count(*) FROM " + pgIdent(s.table) + " WHERE " + where
	p.insertSQL = insertStoreSQL(s)
	return p
}

// insertStoreSQL is the INSERT of one element. A keyed store is a put that replaces and
// reports whether the row is new; a fifo or lifo row is always new.
func insertStoreSQL(s pgStore) string {
	cols := []string{s.ownerCol}
	if s.seqKeyed() {
		cols = append(cols, "seq")
	} else {
		cols = append(cols, s.keyCol)
	}
	for _, b := range s.bodyCols() {
		cols = append(cols, b.col)
	}
	params := pgPlaceholders(1, len(cols))
	for _, f := range s.extra {
		cols = append(cols, f.col)
		params += fmt.Sprintf(", %d", f.value)
	}
	sql := "INSERT INTO " + pgIdent(s.table) + " (" + pgIdents(cols) + ") VALUES (" + params + ")"
	if s.seqKeyed() {
		return sql
	}
	var updates []string
	for _, b := range s.bodyCols() {
		updates = append(updates, pgIdent(b.col)+" = EXCLUDED."+pgIdent(b.col))
	}
	return sql + " ON CONFLICT (" + pgIdents(s.pk) + ") DO UPDATE SET " + strings.Join(updates, ", ") + " RETURNING (xmax = 0)"
}

// holders makes the scan destinations of one row of the store's select list.
func (p *pgPrepared) holders() []any {
	out := make([]any, len(p.sel))
	for i, col := range p.sel {
		switch p.kinds[col] {
		case pgInt:
			out[i] = new(int64)
		case pgBytes:
			out[i] = new([]byte)
		default:
			out[i] = new(string)
		}
	}
	return out
}

// element builds the model element of a scanned row.
func (p *pgPrepared) element(owner string, held []any) model.Element {
	e := model.Element{Bucket: p.bucket, Owner: owner, Store: p.name}
	if p.seqKeyed() {
		e.Seq = *held[0].(*int64)
	} else {
		e.Key = *held[0].(*string)
	}
	e.Body = make([]model.Field, len(p.body))
	for i, b := range p.body {
		switch b.col {
		case p.keyCol:
			e.Body[i] = model.Field{Name: b.field, Value: e.Key}
		case p.ownerCol:
			e.Body[i] = model.Field{Name: b.field, Value: owner}
		default:
			e.Body[i] = model.Field{Name: b.field, Value: deref(held[p.index[b.col]])}
		}
	}
	return e
}

func deref(holder any) any {
	switch v := holder.(type) {
	case *int64:
		return *v
	case *[]byte:
		return *v
	}
	return *holder.(*string)
}

// readStore reads the elements of one store of one owner, oldest first. limit 0 reads all.
func (l *pgLayout) readStore(ctx context.Context, q pgQuerier, bucket, store, owner string, limit int) ([]model.Element, error) {
	return l.readStorePage(ctx, q, bucket, store, owner, limit, ReadOp{})
}

// readStorePage is readStore for a page that may start inside the store. This adapter reads an
// offset as a range on the primary key and never as OFFSET, so the skipped rows are not
// scanned: seq >= Offset for a pushed store (seeded seq counts 0, 1, 2, ...), key > AfterKey
// for a keyed one.
func (l *pgLayout) readStorePage(ctx context.Context, q pgQuerier, bucket, store, owner string, limit int, start ReadOp) ([]model.Element, error) {
	p, ok := l.stores[bucket+"."+store]
	if !ok {
		return nil, fmt.Errorf("layout %s has no store %s.%s", l.layout, bucket, store)
	}
	var (
		rows pgx.Rows
		err  error
	)
	if start.Offset > 0 {
		if limit < 1 {
			return nil, fmt.Errorf("reading %s.%s of %s at offset %d: a read at an offset needs a limit", bucket, store, owner, start.Offset)
		}
		var from any = int64(start.Offset)
		if !p.seqKeyed() {
			if start.AfterKey == "" {
				return nil, fmt.Errorf("reading %s.%s of %s at offset %d: the store is keyed and the read gave no AfterKey", bucket, store, owner, start.Offset)
			}
			from = start.AfterKey
		}
		rows, err = q.Query(ctx, p.fromLimitSQL, owner, from, limit)
	} else if limit > 0 {
		rows, err = q.Query(ctx, p.limitSQL, owner, limit)
	} else {
		rows, err = q.Query(ctx, p.pageSQL, owner)
	}
	if err != nil {
		return nil, fmt.Errorf("reading %s.%s of %s: %w", bucket, store, owner, err)
	}
	defer rows.Close()
	var out []model.Element
	for rows.Next() {
		held := p.holders()
		if err := rows.Scan(held...); err != nil {
			return nil, fmt.Errorf("scanning %s.%s of %s: %w", bucket, store, owner, err)
		}
		out = append(out, p.element(owner, held))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading %s.%s of %s: %w", bucket, store, owner, err)
	}
	return out, nil
}

// Read runs one read operation.
func (l *pgLayout) Read(ctx context.Context, op ReadOp) (Reply, error) {
	if op.Layout != l.layout {
		return Reply{}, fmt.Errorf("read %s is for layout %s, this database holds %s", op.Kind, op.Layout, l.layout)
	}
	conn, err := l.db.pool.Acquire(ctx)
	if err != nil {
		return Reply{}, fmt.Errorf("read %s: acquiring a connection: %w", op.Kind, err)
	}
	defer conn.Release()
	c := conn.Conn()
	switch op.Kind {
	case ReadSocialFeed:
		return l.readFeed(ctx, c, op)
	case ReadSocialInbox:
		return l.readPage(ctx, c, op, "User", "Inbox")
	case ReadSocialFollowers:
		return l.readPage(ctx, c, op, "User", "Followers")
	case ReadSocialFollowScan:
		return l.readFollowScan(ctx, c, op)
	case ReadChatRoomPage:
		return l.readRoomPage(ctx, c, op)
	case ReadChatUserRooms:
		return l.readPage(ctx, c, op, "User", "Rooms")
	case ReadLedgerPage, ReadLedgerAll:
		return l.readPage(ctx, c, op, "Account", "Expenses")
	case ReadLedgerTotals:
		return l.readTotals(ctx, c, op)
	case ReadLedgerInvoices:
		return l.readPage(ctx, c, op, "Account", "Invoices")
	case ReadAgentMemories:
		return l.readPage(ctx, c, op, "User", "Memories")
	case ReadAgentSession:
		return l.readPage(ctx, c, op, "Session", "Messages")
	}
	return Reply{}, fmt.Errorf("the postgres adapter has no read operation %q", op.Kind)
}

func (l *pgLayout) readPage(ctx context.Context, c *pgx.Conn, op ReadOp, bucket, store string) (Reply, error) {
	elements, err := l.readStorePage(ctx, c, bucket, store, op.Entity, op.Limit, op)
	if err != nil {
		return Reply{}, err
	}
	return Reply{Elements: elements, RoundTrips: 1}, nil
}

// The feed and member joins take a limit as $2; 0 means every row, because LIMIT NULL is no limit.
const (
	pgFeedSQL        = `SELECT f."seq", f."post_id", f."at", p."author", p."text", p."at" FROM "feed" f JOIN "posts" p ON p."id" = f."post_id" WHERE f."user_id" = $1 ORDER BY f."seq" LIMIT NULLIF($2::int, 0)`
	pgFeedFromSQL    = `SELECT f."seq", f."post_id", f."at", p."author", p."text", p."at" FROM "feed" f JOIN "posts" p ON p."id" = f."post_id" WHERE f."user_id" = $1 AND f."seq" >= $3 ORDER BY f."seq" LIMIT NULLIF($2::int, 0)`
	pgRoomMembersSQL = `SELECT m."user_id", m."since", u."name" FROM "members" m JOIN "users" u ON u."id" = m."user_id" WHERE m."room_id" = $1 ORDER BY m."user_id" COLLATE "C" LIMIT NULLIF($2::int, 0)`
)

// ReadStore implements StoreReader: every element of one store of one document.
func (l *pgLayout) ReadStore(ctx context.Context, ref StoreRef) ([]model.Element, error) {
	return l.readStore(ctx, l.db.pool, ref.Bucket, ref.Store, ref.Owner, 0)
}

// ReadCounters implements CounterReader: the counter columns of one document, by column name.
func (l *pgLayout) ReadCounters(ctx context.Context, bucket, id string) (map[string]int64, error) {
	d, ok := l.docs[bucket]
	if !ok || d.idCol == "" || len(d.counters) == 0 {
		return nil, fmt.Errorf("layout %s: %s has no document table with counters to read", l.layout, bucket)
	}
	values := make([]int64, len(d.counters))
	dest := make([]any, len(values))
	for i := range values {
		dest[i] = &values[i]
	}
	err := l.db.pool.QueryRow(ctx, "SELECT "+pgIdents(d.counters)+" FROM "+pgIdent(d.table)+" WHERE "+pgIdent(d.idCol)+" = $1", id).Scan(dest...)
	if err != nil {
		return nil, fmt.Errorf("reading the counters of %s %s: %w", bucket, id, err)
	}
	out := make(map[string]int64, len(values))
	for i, name := range d.counters {
		out[name] = values[i]
	}
	return out, nil
}

// readFeed is one join: the feed page of a user with the post of each element.
func (l *pgLayout) readFeed(ctx context.Context, c *pgx.Conn, op ReadOp) (Reply, error) {
	query, args := pgFeedSQL, []any{op.Entity, op.Limit}
	if op.Offset > 0 {
		query, args = pgFeedFromSQL, append(args, int64(op.Offset))
	}
	rows, err := c.Query(ctx, query, args...)
	if err != nil {
		return Reply{}, fmt.Errorf("reading the feed of %s: %w", op.Entity, err)
	}
	defer rows.Close()
	reply := Reply{RoundTrips: 1}
	for rows.Next() {
		var seq, feedAt, postAt int64
		var postID, author, text string
		if err := rows.Scan(&seq, &postID, &feedAt, &author, &text, &postAt); err != nil {
			return Reply{}, fmt.Errorf("scanning the feed of %s: %w", op.Entity, err)
		}
		reply.Elements = append(reply.Elements, model.Element{Bucket: "User", Owner: op.Entity, Store: "Feed", Seq: seq,
			Body: []model.Field{{Name: "post", Value: postID}, {Name: "at", Value: feedAt}}})
		reply.Documents = append(reply.Documents, model.Document{Bucket: "Post", ID: postID,
			Fields: []model.Field{{Name: "Author", Value: author}, {Name: "Text", Value: text}, {Name: "At", Value: postAt}}})
	}
	if err := rows.Err(); err != nil {
		return Reply{}, fmt.Errorf("reading the feed of %s: %w", op.Entity, err)
	}
	return reply, nil
}

// readFollowScan reads every Follow document whose To is the user, through follows_by_followee.
func (l *pgLayout) readFollowScan(ctx context.Context, c *pgx.Conn, op ReadOp) (Reply, error) {
	rows, err := c.Query(ctx, `SELECT "follower_id", "at" FROM "follows" WHERE "followee_id" = $1`, op.Entity)
	if err != nil {
		return Reply{}, fmt.Errorf("scanning the follows of %s: %w", op.Entity, err)
	}
	defer rows.Close()
	reply := Reply{RoundTrips: 1}
	for rows.Next() {
		var from string
		var at int64
		if err := rows.Scan(&from, &at); err != nil {
			return Reply{}, fmt.Errorf("scanning the follows of %s: %w", op.Entity, err)
		}
		reply.Documents = append(reply.Documents, model.Document{Bucket: "Follow", ID: from + ">" + op.Entity,
			Fields: []model.Field{{Name: "From", Value: from}, {Name: "To", Value: op.Entity}, {Name: "At", Value: at}}})
	}
	if err := rows.Err(); err != nil {
		return Reply{}, fmt.Errorf("scanning the follows of %s: %w", op.Entity, err)
	}
	return reply, nil
}

// readRoomPage is two statements: the first messages of the room, then its members joined
// with their names.
func (l *pgLayout) readRoomPage(ctx context.Context, c *pgx.Conn, op ReadOp) (Reply, error) {
	messages, err := l.readStorePage(ctx, c, "Room", "Messages", op.Entity, op.Limit, op)
	if err != nil {
		return Reply{}, err
	}
	reply := Reply{Elements: messages, RoundTrips: 2}
	rows, err := c.Query(ctx, pgRoomMembersSQL, op.Entity, op.MemberLimit)
	if err != nil {
		return Reply{}, fmt.Errorf("reading the members of %s: %w", op.Entity, err)
	}
	defer rows.Close()
	for rows.Next() {
		var user, name string
		var since int64
		if err := rows.Scan(&user, &since, &name); err != nil {
			return Reply{}, fmt.Errorf("scanning the members of %s: %w", op.Entity, err)
		}
		reply.Elements = append(reply.Elements, model.Element{Bucket: "Room", Owner: op.Entity, Store: "Members", Key: user,
			Body: []model.Field{{Name: "user", Value: user}, {Name: "since", Value: since}}})
		reply.Documents = append(reply.Documents, model.Document{Bucket: "User", ID: user, Fields: []model.Field{{Name: "Name", Value: name}}})
	}
	if err := rows.Err(); err != nil {
		return Reply{}, fmt.Errorf("reading the members of %s: %w", op.Entity, err)
	}
	return reply, nil
}

// pgTotalsSQL is the query a SQL application writes for "what did this account spend by
// category": a GROUP BY over the expenses of one account, not a maintained summary row.
func pgTotalsSQL(p pgStore) (string, error) {
	category, amount := "", ""
	for _, b := range p.body {
		switch b.field {
		case "category":
			category = b.col
		case "amount":
			amount = b.col
		}
	}
	if category == "" || amount == "" {
		return "", fmt.Errorf("the store %s has no category and amount columns to group, only %v", p.table, p.body)
	}
	return "SELECT " + pgIdent(category) + ", SUM(" + pgIdent(amount) + ")::bigint, COUNT(*) FROM " + pgIdent(p.table) +
		" WHERE " + pgIdent(p.ownerCol) + " = $1 GROUP BY " + pgIdent(category), nil
}

// readTotals runs ledger.totals_by_category: one GROUP BY, the rows sorted by category here.
func (l *pgLayout) readTotals(ctx context.Context, c *pgx.Conn, op ReadOp) (Reply, error) {
	query, err := pgTotalsSQL(l.stores["Account.Expenses"].pgStore)
	if err != nil {
		return Reply{}, err
	}
	rows, err := c.Query(ctx, query, op.Entity)
	if err != nil {
		return Reply{}, fmt.Errorf("totals of %s by category: %w", op.Entity, err)
	}
	defer rows.Close()
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
