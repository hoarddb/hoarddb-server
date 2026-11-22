package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// mongoFullBatch is the batch size of a read of every element, large enough that one command
// returns the whole store.
const mongoFullBatch = 10000

// mongoOps runs the operations of one layout against its database.
type mongoOps struct {
	db     *mongo.Database
	layout model.Layout
	spec   mongoLayout
}

// Read runs one read of the table at the top of adapter.go.
func (o *mongoOps) Read(ctx context.Context, op ReadOp) (Reply, error) {
	switch op.Kind {
	case ReadSocialFeed:
		elements, err := o.readStorePage(ctx, "User", op.Entity, "Feed", op.Limit, op)
		if err != nil {
			return Reply{}, err
		}
		ids := make([]string, len(elements))
		for i, element := range elements {
			ids[i] = bodyString(element, "post")
		}
		docs, trips, err := o.readDocuments(ctx, "Post", ids)
		if err != nil {
			return Reply{}, err
		}
		return Reply{Elements: elements, Documents: docs, RoundTrips: 1 + trips}, nil
	case ReadSocialInbox:
		return o.storeReply(ctx, "User", op.Entity, "Inbox", op.Limit, op)
	case ReadSocialFollowers:
		return o.storeReply(ctx, "User", op.Entity, "Followers", op.Limit, op)
	case ReadSocialFollowScan:
		return o.followScan(ctx, op.Entity)
	case ReadChatRoomPage:
		return o.roomPage(ctx, op)
	case ReadChatUserRooms:
		return o.storeReply(ctx, "User", op.Entity, "Rooms", op.Limit, op)
	case ReadLedgerPage, ReadLedgerAll:
		return o.storeReply(ctx, "Account", op.Entity, "Expenses", op.Limit, op)
	case ReadLedgerTotals:
		return o.readTotals(ctx, op)
	case ReadLedgerInvoices:
		return o.storeReply(ctx, "Account", op.Entity, "Invoices", op.Limit, op)
	case ReadAgentMemories:
		return o.storeReply(ctx, "User", op.Entity, "Memories", op.Limit, op)
	case ReadAgentSession:
		return o.storeReply(ctx, "Session", op.Entity, "Messages", op.Limit, op)
	}
	return Reply{}, fmt.Errorf("the MongoDB adapter has no read %q", op.Kind)
}

func (o *mongoOps) storeReply(ctx context.Context, bucket, owner, store string, limit int, op ReadOp) (Reply, error) {
	elements, err := o.readStorePage(ctx, bucket, owner, store, limit, op)
	if err != nil {
		return Reply{}, err
	}
	return Reply{Elements: elements, RoundTrips: 1}, nil
}

// roomPage reads the first messages and every member of a room, then the name of each member.
func (o *mongoOps) roomPage(ctx context.Context, op ReadOp) (Reply, error) {
	messages, err := o.readStorePage(ctx, "Room", op.Entity, "Messages", op.Limit, op)
	if err != nil {
		return Reply{}, err
	}
	members, err := o.readStore(ctx, "Room", op.Entity, "Members", op.MemberLimit)
	if err != nil {
		return Reply{}, err
	}
	ids := make([]string, len(members))
	for i, member := range members {
		ids[i] = member.Key
	}
	docs, trips, err := o.readDocuments(ctx, "User", ids)
	if err != nil {
		return Reply{}, err
	}
	return Reply{Elements: append(messages, members...), Documents: docs, RoundTrips: 2 + trips}, nil
}

// readDocuments reads the documents with the given ids in one $in query, as a Mongo
// application reads a page of references, and returns them in the order of ids. A missing
// document is left out, which the shared verifier reports. No ids cost no command.
func (o *mongoOps) readDocuments(ctx context.Context, bucket string, ids []string) (docs []model.Document, trips int, err error) {
	if len(ids) == 0 {
		return nil, 0, nil
	}
	spec, err := o.spec.docOf(bucket)
	if err != nil {
		return nil, 0, err
	}
	cursor, err := o.db.Collection(spec.Coll).Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetBatchSize(mongoFullBatch))
	if err != nil {
		return nil, 1, fmt.Errorf("reading %d %s documents: %w", len(ids), bucket, err)
	}
	defer func() { _ = cursor.Close(ctx) }() // the rows are read; a close failure loses nothing
	found := make(map[string]model.Document, len(ids))
	for cursor.Next(ctx) {
		doc, err := spec.document(cursor.Current, bucket)
		if err != nil {
			return nil, 1, err
		}
		found[doc.ID] = doc
	}
	if err := cursor.Err(); err != nil {
		return nil, 1, fmt.Errorf("reading %d %s documents: %w", len(ids), bucket, err)
	}
	return orderByIDs(ids, found), 1, nil
}

// orderByIDs returns the found documents in the order of ids, skipping an id that was not found.
func orderByIDs(ids []string, found map[string]model.Document) []model.Document {
	ordered := make([]model.Document, 0, len(ids))
	for _, id := range ids {
		if doc, ok := found[id]; ok {
			ordered = append(ordered, doc)
		}
	}
	return ordered
}

// followScan returns every Follow document whose To is the user, through follows_by_followee.
func (o *mongoOps) followScan(ctx context.Context, user string) (Reply, error) {
	spec, err := o.spec.docOf("Follow")
	if err != nil {
		return Reply{}, err
	}
	cursor, err := o.db.Collection(spec.Coll).Find(ctx, bson.D{{Key: "followee_id", Value: user}},
		options.Find().SetBatchSize(mongoFullBatch))
	if err != nil {
		return Reply{}, fmt.Errorf("scanning follows of %s: %w", user, err)
	}
	defer func() { _ = cursor.Close(ctx) }() // the rows are read; a close failure loses nothing
	reply := Reply{RoundTrips: 1}
	for cursor.Next(ctx) {
		doc, err := spec.document(cursor.Current, "Follow")
		if err != nil {
			return Reply{}, err
		}
		reply.Documents = append(reply.Documents, doc)
	}
	if err := cursor.Err(); err != nil {
		return Reply{}, fmt.Errorf("scanning follows of %s: %w", user, err)
	}
	return reply, nil
}

// ReadStore implements StoreReader: every element of one store of one document.
func (o *mongoOps) ReadStore(ctx context.Context, ref StoreRef) ([]model.Element, error) {
	return o.readStore(ctx, ref.Bucket, ref.Owner, ref.Store, 0)
}

// ReadCounters implements CounterReader: the counter fields of one document, by field name.
func (o *mongoOps) ReadCounters(ctx context.Context, bucket, id string) (map[string]int64, error) {
	spec, err := o.spec.docOf(bucket)
	if err != nil {
		return nil, err
	}
	if len(spec.Counters) == 0 {
		return nil, fmt.Errorf("collection %s declares no counters", spec.Coll)
	}
	projection := bson.D{{Key: "_id", Value: 0}}
	for _, name := range spec.Counters {
		projection = append(projection, bson.E{Key: name, Value: 1})
	}
	raw, err := o.db.Collection(spec.Coll).FindOne(ctx, bson.D{{Key: "_id", Value: id}}, options.FindOne().SetProjection(projection)).Raw()
	if err != nil {
		return nil, fmt.Errorf("reading the counters of %s %s: %w", bucket, id, err)
	}
	out := make(map[string]int64, len(spec.Counters))
	for _, name := range spec.Counters {
		decoded, err := decodeScalar(raw, name)
		if err != nil {
			return nil, err
		}
		out[name] = toInt64(decoded)
	}
	return out, nil
}

// readStore reads the elements of one store of one owner, oldest first, at most limit of them
// (every element when limit is 0).
func (o *mongoOps) readStore(ctx context.Context, bucket, owner, store string, limit int) ([]model.Element, error) {
	return o.readStorePage(ctx, bucket, owner, store, limit, ReadOp{})
}

// readStorePage is readStore for a page that may start inside the store. This adapter reads an
// offset as a range on the index the sort already uses, so the skipped elements are never
// scanned: a fifo or lifo store (seeded seq counts 0, 1, 2, ...) takes seq >= Offset, a btree
// store takes the key above AfterKey. It never uses skip().
func (o *mongoOps) readStorePage(ctx context.Context, bucket, owner, store string, limit int, start ReadOp) ([]model.Element, error) {
	spec, err := o.spec.storeOf(bucket, store)
	if err != nil {
		return nil, err
	}
	filter := bson.D{{Key: spec.Owner, Value: owner}}
	if start.Offset > 0 {
		switch {
		case spec.Seq != "":
			filter = append(filter, bson.E{Key: spec.Seq, Value: bson.D{{Key: "$gte", Value: int64(start.Offset)}}})
		case spec.Order != "" && start.AfterKey != "":
			filter = append(filter, bson.E{Key: spec.Order, Value: bson.D{{Key: "$gt", Value: start.AfterKey}}})
		default:
			return nil, fmt.Errorf("reading %s.%s of %s at offset %d: the store has no seq and the read gave no AfterKey", bucket, store, owner, start.Offset)
		}
	}
	findOptions := options.Find().SetProjection(spec.projection()).SetBatchSize(mongoFullBatch)
	if limit > 0 {
		findOptions.SetLimit(int64(limit)).SetBatchSize(int32(limit))
	}
	switch {
	case spec.Order != "":
		findOptions.SetSort(bson.D{{Key: spec.Order, Value: 1}})
	case spec.Key != "":
		// A page of a hash store is the first keys, so every run reads the same page.
		findOptions.SetSort(bson.D{{Key: spec.Key, Value: 1}})
	}
	cursor, err := o.db.Collection(spec.Coll).Find(ctx, filter, findOptions)
	if err != nil {
		return nil, fmt.Errorf("reading %s %s.%s of %s: %w", spec.Coll, bucket, store, owner, err)
	}
	defer func() { _ = cursor.Close(ctx) }() // the rows are read; a close failure loses nothing
	var elements []model.Element
	for cursor.Next(ctx) {
		element, err := spec.element(cursor.Current, bucket, owner, store)
		if err != nil {
			return nil, err
		}
		elements = append(elements, element)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("reading %s %s.%s of %s: %w", spec.Coll, bucket, store, owner, err)
	}
	return elements, nil
}

// Write applies one write as a sequence of single-document commands, in the order the header of
// engine_mongo.go gives.
func (o *mongoOps) Write(ctx context.Context, op WriteOp) (Reply, error) {
	var reply Reply
	for _, ref := range op.PreRead {
		if _, err := o.readStore(ctx, ref.Bucket, ref.Owner, ref.Store, 0); err != nil {
			return reply, err
		}
		reply.RoundTrips++
	}
	if op.Claim != nil {
		claimed, found, trips, err := o.claim(ctx, op.Claim.Run)
		reply.RoundTrips += trips
		if err != nil {
			return reply, err
		}
		if !found {
			return reply, nil
		}
		reply.Elements = []model.Element{claimed}
	}
	created := map[string]bool{}
	for _, doc := range op.Documents {
		newly, err := o.writeDocument(ctx, doc)
		if err != nil {
			return reply, err
		}
		created["doc:"+doc.Bucket] = newly
		reply.RoundTrips++
	}
	createdAt := make([]bool, len(op.Elements))
	for i, element := range op.Elements {
		newly, trips, err := o.writeElement(ctx, op, element, created)
		reply.RoundTrips += trips
		if err != nil {
			return reply, err
		}
		createdAt[i] = newly
	}
	updates, err := o.counterUpdates(op.Counters, createdAt)
	if err != nil {
		return reply, err
	}
	for _, update := range updates {
		if err := o.addCounters(ctx, update); err != nil {
			return reply, err
		}
		reply.RoundTrips++
	}
	return reply, nil
}

// claim takes the oldest ready task of a run (state 0 to 1) and acknowledges it (state 2).
func (o *mongoOps) claim(ctx context.Context, run string) (claimed model.Element, found bool, trips int, err error) {
	spec, err := o.spec.storeOf("Run", "Tasks")
	if err != nil {
		return model.Element{}, false, 0, err
	}
	tasks := o.db.Collection(spec.Coll)
	raw, err := tasks.FindOneAndUpdate(ctx,
		bson.D{{Key: spec.Owner, Value: run}, {Key: spec.Live, Value: 0}},
		bson.D{{Key: "$set", Value: bson.D{{Key: spec.Live, Value: 1}}}},
		options.FindOneAndUpdate().SetSort(bson.D{{Key: spec.Seq, Value: 1}}).SetProjection(spec.projection()),
	).Raw()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return model.Element{}, false, 1, nil
	}
	if err != nil {
		return model.Element{}, false, 1, fmt.Errorf("claiming a task of %s: %w", run, err)
	}
	claimed, err = spec.element(raw, "Run", run, "Tasks")
	if err != nil {
		return model.Element{}, false, 1, err
	}
	ack, err := tasks.UpdateOne(ctx,
		bson.D{{Key: spec.Owner, Value: run}, {Key: spec.Seq, Value: claimed.Seq}},
		bson.D{{Key: "$set", Value: bson.D{{Key: spec.Live, Value: 2}}}})
	if err != nil {
		return model.Element{}, false, 2, fmt.Errorf("acknowledging task %d of %s: %w", claimed.Seq, run, err)
	}
	if ack.MatchedCount != 1 {
		return model.Element{}, false, 2, fmt.Errorf("acknowledging task %d of %s matched %d rows, want 1", claimed.Seq, run, ack.MatchedCount)
	}
	return claimed, true, 2, nil
}

// writeDocument inserts a document, or for a document with a compound key upserts it, replacing
// the non-key fields of an existing row as the other engines do. newly reports whether a row was created.
func (o *mongoOps) writeDocument(ctx context.Context, doc model.Document) (newly bool, err error) {
	spec, err := o.spec.docOf(doc.Bucket)
	if err != nil {
		return false, err
	}
	if err := checkFields(doc.Fields, spec.Fields, "document "+doc.Bucket+"/"+doc.ID); err != nil {
		return false, err
	}
	collection := o.db.Collection(spec.Coll)
	if spec.IDSeparator == "" {
		row := bson.D{{Key: "_id", Value: doc.ID}}
		for _, field := range spec.Fields {
			row = append(row, bson.E{Key: field.Col, Value: fieldValue(doc.Fields, field.Name)})
		}
		for _, counter := range spec.Counters {
			row = append(row, bson.E{Key: counter, Value: int64(0)})
		}
		if _, err := collection.InsertOne(ctx, row); err != nil {
			return false, fmt.Errorf("inserting %s %s: %w", doc.Bucket, doc.ID, err)
		}
		return true, nil
	}
	var filter, onInsert bson.D
	for _, field := range spec.Fields {
		entry := bson.E{Key: field.Col, Value: fieldValue(doc.Fields, field.Name)}
		if field.Key {
			filter = append(filter, entry)
		} else {
			onInsert = append(onInsert, entry)
		}
	}
	res, err := collection.UpdateOne(ctx, filter, bson.D{{Key: "$set", Value: onInsert}}, options.Update().SetUpsert(true))
	if err != nil {
		return false, fmt.Errorf("upserting %s %s: %w", doc.Bucket, doc.ID, err)
	}
	return res.UpsertedCount == 1, nil
}

// writeElement writes one element of a write and reports whether it was newly created and how
// many commands it took. An element of a paired store costs none: its row was written by the
// document or element it pairs with, and is newly created exactly when that row was.
func (o *mongoOps) writeElement(ctx context.Context, op WriteOp, element model.Element, created map[string]bool) (newly bool, trips int, err error) {
	spec, err := o.spec.storeOf(element.Bucket, element.Store)
	if err != nil {
		return false, 0, err
	}
	name := element.StoreName()
	if spec.Via != "" {
		newly, ok := created[spec.Via]
		if !ok {
			return false, 0, fmt.Errorf("element of %s is stored in the row of %s, which write %s did not carry before it", name, spec.Via, op.ID)
		}
		return newly, 0, nil
	}
	if err := checkFields(element.Body, spec.Body, "element of "+name); err != nil {
		return false, 0, err
	}
	row := bson.D{{Key: spec.Owner, Value: element.Owner}}
	if spec.Key != "" {
		row = append(row, bson.E{Key: spec.Key, Value: element.Key})
	} else {
		row = append(row, bson.E{Key: spec.Seq, Value: SeqBase + op.Ordinal})
	}
	for _, field := range spec.Body {
		if field.Col != "" {
			row = append(row, bson.E{Key: field.Col, Value: fieldValue(element.Body, field.Name)})
		}
	}
	for _, fixed := range spec.Fixed {
		row = append(row, bson.E{Key: fixed.Col, Value: fixed.Value})
	}
	collection := o.db.Collection(spec.Coll)
	if spec.Key == "" {
		if _, err := collection.InsertOne(ctx, row); err != nil {
			return false, 1, fmt.Errorf("pushing onto %s of %s: %w", name, element.Owner, err)
		}
		return true, 1, nil
	}
	filter := bson.D{{Key: spec.Owner, Value: element.Owner}, {Key: spec.Key, Value: element.Key}}
	res, err := collection.ReplaceOne(ctx, filter, row, options.Replace().SetUpsert(true))
	if err != nil {
		return false, 1, fmt.Errorf("putting %s of %s: %w", name, element.Owner, err)
	}
	newly = res.UpsertedCount == 1
	created["store:"+name] = newly
	return newly, 1, nil
}

// mongoCounterUpdate is the merged deltas of one document.
type mongoCounterUpdate struct {
	bucket, id string
	inc        bson.D
}

// counterUpdates folds the deltas a write applies into one update per document, in first-seen
// order, so Mongo does one command per document as the SQL adapters do one UPDATE. A delta
// that names an element applies only when that element was newly created.
func (o *mongoOps) counterUpdates(deltas []CounterDelta, created []bool) ([]mongoCounterUpdate, error) {
	var updates []*mongoCounterUpdate
	byDocument := map[string]*mongoCounterUpdate{}
	for _, counter := range deltas {
		if counter.Element >= len(created) {
			return nil, fmt.Errorf("counter %s of %s %s names element %d of a write with %d", counter.Field, counter.Bucket, counter.ID, counter.Element, len(created))
		}
		spec, err := o.spec.docOf(counter.Bucket)
		if err != nil {
			return nil, err
		}
		declared := false
		for _, name := range spec.Counters {
			declared = declared || name == counter.Field
		}
		if !declared {
			return nil, fmt.Errorf("collection %s declares no counter %q", spec.Coll, counter.Field)
		}
		if counter.Element >= 0 && !created[counter.Element] {
			continue
		}
		key := counter.Bucket + "\x00" + counter.ID
		update := byDocument[key]
		if update == nil {
			update = &mongoCounterUpdate{bucket: counter.Bucket, id: counter.ID}
			byDocument[key] = update
			updates = append(updates, update)
		}
		merged := false
		for i := range update.inc {
			if update.inc[i].Key == counter.Field {
				update.inc[i].Value = update.inc[i].Value.(int64) + counter.Delta
				merged = true
			}
		}
		if !merged {
			update.inc = append(update.inc, bson.E{Key: counter.Field, Value: counter.Delta})
		}
	}
	out := make([]mongoCounterUpdate, len(updates))
	for i, update := range updates {
		out[i] = *update
	}
	return out, nil
}

// addCounters sends the merged $inc of one document.
func (o *mongoOps) addCounters(ctx context.Context, update mongoCounterUpdate) error {
	spec, err := o.spec.docOf(update.bucket)
	if err != nil {
		return err
	}
	res, err := o.db.Collection(spec.Coll).UpdateOne(ctx, bson.D{{Key: "_id", Value: update.id}}, bson.D{{Key: "$inc", Value: update.inc}})
	if err != nil {
		return fmt.Errorf("adding to the counters of %s %s: %w", update.bucket, update.id, err)
	}
	if res.MatchedCount != 1 {
		return fmt.Errorf("the counters of %s %s matched %d documents, want 1", update.bucket, update.id, res.MatchedCount)
	}
	return nil
}

// Count returns the size of each store or, for a reference with no store, of the bucket's
// documents. An acknowledged task is not counted.
func (o *mongoOps) Count(ctx context.Context, refs []StoreRef) (map[StoreRef]int64, error) {
	counts := make(map[StoreRef]int64, len(refs))
	collections := map[string]int64{}
	for _, ref := range refs {
		if ref.Store == "" {
			spec, err := o.spec.docOf(ref.Bucket)
			if err != nil {
				return nil, err
			}
			total, done := collections[spec.Coll]
			if !done {
				var err error
				if total, err = o.db.Collection(spec.Coll).CountDocuments(ctx, bson.D{}); err != nil {
					return nil, fmt.Errorf("counting %s: %w", spec.Coll, err)
				}
				collections[spec.Coll] = total
			}
			counts[ref] = total
			continue
		}
		spec, err := o.spec.storeOf(ref.Bucket, ref.Store)
		if err != nil {
			return nil, err
		}
		filter := bson.D{{Key: spec.Owner, Value: ref.Owner}}
		if spec.Live != "" {
			filter = append(filter, bson.E{Key: spec.Live, Value: bson.D{{Key: "$lt", Value: 2}}})
		}
		total, err := o.db.Collection(spec.Coll).CountDocuments(ctx, filter)
		if err != nil {
			return nil, fmt.Errorf("counting %s of %s in %s: %w", ref.Store, ref.Owner, spec.Coll, err)
		}
		counts[ref] = total
	}
	return counts, nil
}

// projection names the columns an element needs and leaves _id out.
func (s mongoStore) projection() bson.D {
	projection := bson.D{{Key: "_id", Value: 0}, {Key: s.Owner, Value: 1}}
	if s.Key != "" {
		projection = append(projection, bson.E{Key: s.Key, Value: 1})
	}
	if s.Seq != "" {
		projection = append(projection, bson.E{Key: s.Seq, Value: 1})
	}
	for _, field := range s.Body {
		if field.Col != "" {
			projection = append(projection, bson.E{Key: field.Col, Value: 1})
		}
	}
	return projection
}

// element decodes one row of the store's collection into an element.
func (s mongoStore) element(raw bson.Raw, bucket, owner, store string) (model.Element, error) {
	element := model.Element{Bucket: bucket, Owner: owner, Store: store}
	if s.Key != "" {
		key, err := decodeScalar(raw, s.Key)
		if err != nil {
			return model.Element{}, err
		}
		text, ok := key.(string)
		if !ok {
			return model.Element{}, fmt.Errorf("column %q holds a %T, want text", s.Key, key)
		}
		element.Key = text
	}
	if s.Seq != "" {
		seq, err := decodeScalar(raw, s.Seq)
		if err != nil {
			return model.Element{}, err
		}
		element.Seq = toInt64(seq)
	}
	element.Body = make([]model.Field, 0, len(s.Body))
	for _, field := range s.Body {
		if field.Col == "" {
			element.Body = append(element.Body, model.Field{Name: field.Name, Value: element.Key})
			continue
		}
		decoded, err := decodeScalar(raw, field.Col)
		if err != nil {
			return model.Element{}, err
		}
		element.Body = append(element.Body, model.Field{Name: field.Name, Value: decoded})
	}
	return element, nil
}

// document decodes one row of the bucket's collection into a document.
func (d mongoDocument) document(raw bson.Raw, bucket string) (model.Document, error) {
	doc := model.Document{Bucket: bucket, Fields: make([]model.Field, 0, len(d.Fields))}
	var idParts []string
	for _, field := range d.Fields {
		decoded, err := decodeScalar(raw, field.Col)
		if err != nil {
			return model.Document{}, err
		}
		doc.Fields = append(doc.Fields, model.Field{Name: field.Name, Value: decoded})
		if field.Key {
			idParts = append(idParts, fmt.Sprint(decoded))
		}
	}
	if d.IDSeparator == "" {
		id, err := decodeScalar(raw, "_id")
		if err != nil {
			return model.Document{}, err
		}
		text, ok := id.(string)
		if !ok {
			return model.Document{}, fmt.Errorf("column _id holds a %T, want text", id)
		}
		doc.ID = text
	} else {
		doc.ID = strings.Join(idParts, d.IDSeparator)
	}
	for _, name := range d.Counters {
		decoded, err := decodeScalar(raw, name)
		if err != nil {
			return model.Document{}, err
		}
		doc.Counters = append(doc.Counters, model.Field{Name: name, Value: decoded})
	}
	return doc, nil
}

// decodeScalar reads one column as a string, an int64 or the bytes of a binary.
func decodeScalar(raw bson.Raw, column string) (any, error) {
	found, err := raw.LookupErr(column)
	if err != nil {
		return nil, fmt.Errorf("a stored row has no column %q (the collection does not match the seeded schema): %w", column, err)
	}
	switch found.Type {
	case bson.TypeString:
		return found.StringValue(), nil
	case bson.TypeInt32:
		return int64(found.Int32()), nil
	case bson.TypeInt64:
		return found.Int64(), nil
	case bson.TypeBinary:
		_, stored := found.Binary()
		return append([]byte(nil), stored...), nil
	}
	return nil, fmt.Errorf("column %q holds a %s, want a string, an integer or a binary", column, found.Type)
}

// fieldValue returns the named field of a body, converted to what BSON stores: bytes become a
// binary of subtype 0, as the seeder wrote vectors.
func fieldValue(fields []model.Field, name string) any {
	for _, field := range fields {
		if field.Name == name {
			if packed, ok := field.Value.([]byte); ok {
				return primitive.Binary{Subtype: 0, Data: packed}
			}
			return field.Value
		}
	}
	return nil
}

// checkFields makes sure a body carries every field the collection stores and no other, so the
// adapter neither invents nor drops a field.
func checkFields(have []model.Field, want []mongoField, what string) error {
	for _, field := range want {
		if field.Col == "" {
			continue
		}
		if fieldValue(have, field.Name) == nil {
			return fmt.Errorf("%s has no field %q, which the collection stores", what, field.Name)
		}
	}
	for _, field := range have {
		known := false
		for _, candidate := range want {
			known = known || candidate.Name == field.Name
		}
		if !known {
			return fmt.Errorf("%s carries field %q, which the seeded collection has no column for", what, field.Name)
		}
	}
	return nil
}

// mongoTotalsPipeline is the aggregation a MongoDB application writes for "what did this account
// spend by category": a $match on the account and a $group by category, not a maintained summary document.
func mongoTotalsPipeline(spec mongoStore, owner string) (mongo.Pipeline, error) {
	category, amount := "", ""
	for _, f := range spec.Body {
		switch f.Name {
		case "category":
			category = f.Col
		case "amount":
			amount = f.Col
		}
	}
	if category == "" || amount == "" {
		return nil, fmt.Errorf("the collection %s has no category and amount fields to group", spec.Coll)
	}
	return mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: spec.Owner, Value: owner}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$" + category}, {Key: "total", Value: bson.D{{Key: "$sum", Value: "$" + amount}}}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
	}, nil
}

// readTotals runs ledger.totals_by_category: one aggregation, the rows sorted by category here.
func (o *mongoOps) readTotals(ctx context.Context, op ReadOp) (Reply, error) {
	spec, err := o.spec.storeOf("Account", "Expenses")
	if err != nil {
		return Reply{}, err
	}
	pipeline, err := mongoTotalsPipeline(spec, op.Entity)
	if err != nil {
		return Reply{}, err
	}
	cursor, err := o.db.Collection(spec.Coll).Aggregate(ctx, pipeline)
	if err != nil {
		return Reply{}, fmt.Errorf("totals of %s by category: %w", op.Entity, err)
	}
	defer func() { _ = cursor.Close(ctx) }() // the rows are read; a close failure loses nothing
	var totals []CategoryTotal
	for cursor.Next(ctx) {
		var row struct {
			Category string `bson:"_id"`
			Total    int64  `bson:"total"`
			Count    int64  `bson:"n"`
		}
		if err := cursor.Decode(&row); err != nil {
			return Reply{}, fmt.Errorf("decoding the totals of %s: %w", op.Entity, err)
		}
		totals = append(totals, CategoryTotal{Category: row.Category, Total: row.Total, Count: row.Count})
	}
	if err := cursor.Err(); err != nil {
		return Reply{}, fmt.Errorf("totals of %s by category: %w", op.Entity, err)
	}
	sort.Slice(totals, func(i, j int) bool { return totals[i].Category < totals[j].Category })
	return Reply{Totals: totals, RoundTrips: 1}, nil
}
