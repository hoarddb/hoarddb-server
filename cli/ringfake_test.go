package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// u52Fake is an in-memory ring: databases, buckets, documents and fifo, lifo
// and hash stores, enough to check every statement's output and error text.
type u52Fake struct {
	databases map[string]int32
	buckets   map[string]*u52Bucket
	order     []string // "db.bucket" in creation order
	failures  map[string]error
	calls     []string
	// node is what DescribeNode answers; the zero value is a healthy one-node cluster.
	node ringclient.NodeDescription
	// seeding is what SeedingStatus answers; seedingQueries and seedingActions record what it was sent.
	seeding        []ringclient.SeedingRow
	seedingQueries []ringclient.SeedingQuery
	seedingActions []string
	// seedingStuck makes SeedingStatus answer more with no rows, as a broken server could.
	seedingStuck bool
	// membership is what Join and Promote answer; the calls are recorded in calls.
	membership ringclient.MembershipResult
	// view is what ClusterView answers; nil is a one-member view of the default node.
	view *ringclient.Topology
	// storages is each database's storage word; memoryUsed and memoryCap are what the node's memory store reports.
	storages   map[string]string
	memoryUsed int64
	memoryCap  int64
	// users is the user store the user statements change; nil until one runs.
	users *u52Users
}

type u52Bucket struct {
	info   ringclient.BucketInfo
	ids    []string
	docs   map[string]ringclient.Doc
	stores map[string][]bson.Raw
	keyed  map[string]map[string]bson.Raw
	leases map[[16]byte][]bson.Raw
	// nextID is the next field id an alter gives; an id is never reused.
	nextID uint16
	// updates records every update statement the bucket was sent.
	updates []ringclient.Update
	// optionCounts is how many call options each Update and Delete was sent.
	optionCounts []int
}

func newU52Fake() *u52Fake {
	return &u52Fake{databases: map[string]int32{}, buckets: map[string]*u52Bucket{}, failures: map[string]error{}, storages: map[string]string{}}
}

func (f *u52Fake) fail(method string) error {
	f.calls = append(f.calls, method)
	return f.failures[method]
}

// CreateDatabaseWithStorage is the request a create database with a storage word makes.
func (f *u52Fake) CreateDatabaseWithStorage(_ context.Context, name string, rf int32, storage string) (ringclient.DatabaseInfo, error) {
	if err := f.fail("CreateDatabaseWithStorage"); err != nil {
		return ringclient.DatabaseInfo{}, err
	}
	return f.createDatabase(name, rf, storage)
}

// createDatabase keeps the fake's rules: an empty storage word is disk, and a database name is taken once.
func (f *u52Fake) createDatabase(name string, rf int32, storage string) (ringclient.DatabaseInfo, error) {
	if _, ok := f.databases[name]; ok {
		return ringclient.DatabaseInfo{}, &ringclient.Error{Code: cluster.CodeConflict,
			Message: fmt.Sprintf("database %q already exists. Run 'use %s' to select it", name, name)}
	}
	if rf == 0 {
		rf = 1
	}
	if storage == "" {
		storage = "disk"
	}
	f.databases[name] = rf
	f.storages[name] = storage
	return ringclient.DatabaseInfo{ID: int64(len(f.databases)), RF: rf, Storage: storage}, nil
}

func (f *u52Fake) CreateBucket(ctx context.Context, database, name string, fields []ringclient.FieldDecl) (ringclient.BucketInfo, error) {
	return f.CreateBucketExpiring(ctx, database, name, "", fields)
}

// CreateBucketExpiring records the expiry the statement named, as the server's declaration would hold it.
func (f *u52Fake) CreateBucketExpiring(_ context.Context, database, name, expireField string, fields []ringclient.FieldDecl) (ringclient.BucketInfo, error) {
	if err := f.fail("CreateBucket"); err != nil {
		return ringclient.BucketInfo{}, err
	}
	if _, ok := f.databases[database]; !ok {
		return ringclient.BucketInfo{}, &ringclient.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf("database %q not found", database)}
	}
	key := database + "." + name
	if _, ok := f.buckets[key]; ok {
		return ringclient.BucketInfo{}, &ringclient.Error{Code: cluster.CodeBucketExists,
			Message: fmt.Sprintf("bucket %q already exists in database %q. Run 'describe bucket %s in %s' to see its fields", name, database, name, database)}
	}
	info := ringclient.BucketInfo{Database: database, Name: name, ID: 0x1f4a2c9b7de30015, State: "active", DeclVersion: 1,
		Fields: []ringclient.FieldInfo{{ID: 0, Name: "_id_", Type: "any"}}}
	for i, field := range fields {
		info.Fields = append(info.Fields, ringclient.FieldInfo{ID: uint16(i + 1), Name: field.Name, Type: field.Type, Of: field.Of})
		if field.ExpirePath != "" {
			info.ExpireStores = append(info.ExpireStores, ringclient.ExpireStoreInfo{Field: field.Name, Path: field.ExpirePath})
		}
		for _, aggregate := range field.Aggregates {
			info.Aggregates = append(info.Aggregates, ringclient.AggregateInfo{Store: field.Name, ID: uint8(len(info.Aggregates) + 1), Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By, State: "ready"})
		}
	}
	info.ExpireField = expireField
	f.buckets[key] = &u52Bucket{info: info, docs: map[string]ringclient.Doc{}, stores: map[string][]bson.Raw{},
		keyed: map[string]map[string]bson.Raw{}, leases: map[[16]byte][]bson.Raw{}}
	f.order = append(f.order, key)
	return info, nil
}

// AlterBucket mirrors the server: drops retire an id, adds take the next id, a name already
// declared with its type is left alone, and the declaration version moves only when the table does.
func (f *u52Fake) AlterBucket(_ context.Context, database, name string, change ringclient.AlterChange) (ringclient.BucketInfo, error) {
	if err := f.fail("AlterBucket"); err != nil {
		return ringclient.BucketInfo{}, err
	}
	bucket, err := f.bucketOf(database, name)
	if err != nil {
		return ringclient.BucketInfo{}, err
	}
	if bucket.nextID == 0 {
		bucket.nextID = uint16(len(bucket.info.Fields))
	}
	fields := append([]ringclient.FieldInfo(nil), bucket.info.Fields...)
	moved := false
	for _, drop := range change.Drop {
		for i, field := range fields {
			if field.Name == drop {
				fields = append(fields[:i], fields[i+1:]...)
				moved = true
				break
			}
		}
	}
	for _, add := range change.Add {
		declared := false
		for _, field := range fields {
			if field.Name == add.Name {
				if field.Type != add.Type {
					return ringclient.BucketInfo{}, &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
						"field %q in %s.%s is declared %s and this statement declares it %s; a declared type never changes", add.Name, database, name, field.Type, add.Type)}
				}
				declared = true
			}
		}
		if !declared {
			fields = append(fields, ringclient.FieldInfo{ID: bucket.nextID, Name: add.Name, Type: add.Type})
			bucket.nextID++
			moved = true
		}
	}
	if change.SetExpire != "" && bucket.info.ExpireField != change.SetExpire {
		bucket.info.ExpireField = change.SetExpire
		moved = true
	}
	if change.DropExpire && bucket.info.ExpireField != "" {
		bucket.info.ExpireField = ""
		moved = true
	}
	if moved {
		bucket.info.Fields = fields
		bucket.info.DeclVersion++
	}
	return bucket.info, nil
}

// DropBucket forgets the bucket; one that is not there is NOT_FOUND, as a finished drop is on the server.
func (f *u52Fake) DropBucket(_ context.Context, database, name string) error {
	if err := f.fail("DropBucket"); err != nil {
		return err
	}
	if _, err := f.bucketOf(database, name); err != nil {
		return err
	}
	delete(f.buckets, database+"."+name)
	for i, key := range f.order {
		if key == database+"."+name {
			f.order = append(f.order[:i], f.order[i+1:]...)
			break
		}
	}
	return nil
}

func (f *u52Fake) bucketOf(database, name string) (*u52Bucket, error) {
	if bucket, ok := f.buckets[database+"."+name]; ok {
		return bucket, nil
	}
	return nil, &ringclient.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf(
		"bucket %q not found in database %q: the declaration fetch returned no document under that name in %s._buckets. "+
			"Run 'show buckets in %s' to list what exists, or 'create bucket %s { <Field> string }' to create it", name, database, database, database, name)}
}

func (f *u52Fake) DescribeBucket(_ context.Context, database, name string) (ringclient.BucketInfo, error) {
	if err := f.fail("DescribeBucket"); err != nil {
		return ringclient.BucketInfo{}, err
	}
	bucket, err := f.bucketOf(database, name)
	if err != nil {
		return ringclient.BucketInfo{}, err
	}
	return bucket.info, nil
}

func (f *u52Fake) ShowBuckets(_ context.Context, database string, page ringclient.PageRequest) (ringclient.BucketPage, error) {
	if err := f.fail("ShowBuckets"); err != nil {
		return ringclient.BucketPage{}, err
	}
	if _, ok := f.databases[database]; !ok {
		return ringclient.BucketPage{}, &ringclient.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf("database %q not found. Run 'create database %s'", database, database)}
	}
	var all []ringclient.BucketSummary
	for _, key := range f.order {
		if bucket := f.buckets[key]; bucket.info.Database == database {
			all = append(all, ringclient.BucketSummary{Name: bucket.info.Name, ID: bucket.info.ID, State: bucket.info.State})
		}
	}
	start := 0
	if len(page.Cursor) == 1 {
		start = int(page.Cursor[0])
	}
	limit := int(page.Limit)
	if limit == 0 {
		limit = 100
	}
	end := min(start+limit, len(all))
	out := ringclient.BucketPage{Buckets: all[start:end]}
	if end < len(all) {
		out.More, out.Cursor = true, []byte{byte(end)}
	}
	return out, nil
}

// DescribeNode answers with the fake's node, or the failure injected under "DescribeNode".
func (f *u52Fake) DescribeNode(context.Context) (ringclient.NodeDescription, error) {
	if err := f.fail("DescribeNode"); err != nil {
		return ringclient.NodeDescription{}, err
	}
	if f.node.NodeID == "" {
		return ringclient.NodeDescription{NodeID: "node-a", Version: "1.1.0-dev", UptimeMS: 312000, MemberState: "active", Members: 1, Voters: 1,
			Epoch: 1, Leader: "node-a", DefaultRF: 1, Serving: "ok"}, nil
	}
	return f.node, nil
}

// ClusterView answers with the fake's view, or the failure injected under "ClusterView".
func (f *u52Fake) ClusterView(context.Context) (*ringclient.Topology, error) {
	if err := f.fail("ClusterView"); err != nil {
		return nil, err
	}
	if f.view == nil {
		return &ringclient.Topology{Epoch: 1, StateSeq: 3, Members: []ringclient.Member{{ID: "node-a", Addr: "192.0.2.10:7433", State: "active"}}}, nil
	}
	return f.view, nil
}

// ShowDatabases lists the fake's databases by name, paged like the server: the cursor is the index to resume at.
func (f *u52Fake) ShowDatabases(_ context.Context, page ringclient.PageRequest) (ringclient.DatabasePage, error) {
	if err := f.fail("ShowDatabases"); err != nil {
		return ringclient.DatabasePage{}, err
	}
	names := make([]string, 0, len(f.databases))
	for name := range f.databases {
		names = append(names, name)
	}
	sort.Strings(names)
	start := 0
	if len(page.Cursor) == 1 {
		start = int(page.Cursor[0])
	}
	limit := int(page.Limit)
	if limit == 0 {
		limit = 100
	}
	end := min(start+limit, len(names))
	out := ringclient.DatabasePage{}
	for i, name := range names[start:end] {
		out.Databases = append(out.Databases, ringclient.DatabaseSummary{Name: name, ID: int64(0x10 + start + i), RF: f.databases[name]})
	}
	if end < len(names) {
		out.More, out.Cursor = true, []byte{byte(end)}
	}
	return out, nil
}

// DescribeDatabase reads one fake database with its buckets paged as ShowBuckets pages them.
func (f *u52Fake) DescribeDatabase(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.DatabaseDescription, error) {
	if err := f.fail("DescribeDatabase"); err != nil {
		return ringclient.DatabaseDescription{}, err
	}
	factor, ok := f.databases[database]
	if !ok {
		return ringclient.DatabaseDescription{}, &ringclient.Error{Code: cluster.CodePermissionDenied, Message: "this session cannot run that command"}
	}
	buckets, err := f.ShowBuckets(ctx, database, page)
	if err != nil {
		return ringclient.DatabaseDescription{}, err
	}
	return ringclient.DatabaseDescription{Name: database, ID: 0x10, RF: factor, Storage: f.storages[database],
		MemoryBytes: f.memoryUsed, MemoryCapBytes: f.memoryCap,
		Buckets: buckets.Buckets, More: buckets.More, Cursor: buckets.Cursor}, nil
}

func (f *u52Fake) Bucket(database, name string) RingBucket {
	return &u52Handle{fake: f, database: database, name: name}
}

type u52Handle struct {
	fake           *u52Fake
	database, name string
}

func (h *u52Handle) get() (*u52Bucket, error) { return h.fake.bucketOf(h.database, h.name) }

func (h *u52Handle) Info(ctx context.Context) (ringclient.BucketInfo, error) {
	if err := h.fake.fail("Info"); err != nil {
		return ringclient.BucketInfo{}, err
	}
	return h.fake.DescribeBucket(ctx, h.database, h.name)
}

func (h *u52Handle) write(id string, doc ringclient.Doc, replace bool) (ringclient.WriteResult, error) {
	bucket, err := h.get()
	if err != nil {
		return ringclient.WriteResult{}, err
	}
	if _, exists := bucket.docs[id]; exists {
		if !replace {
			return ringclient.WriteResult{}, &ringclient.Error{Code: cluster.CodeDocumentExists, Message: fmt.Sprintf(
				"document %q is already in %s.%s, and insert never replaces. Run 'db.%s.upsert({_id_: %q, …})' to replace it, or 'db.%s[%q].get()' to read what is there",
				id, h.database, h.name, h.name, id, h.name, id)}
		}
	} else {
		bucket.ids = append(bucket.ids, id)
	}
	bucket.docs[id] = doc
	return ringclient.WriteResult{Outcome: "applied", Copies: 1}, nil
}

func (h *u52Handle) Insert(_ context.Context, id string, doc ringclient.Doc, _ ...ringclient.CallOption) (ringclient.WriteResult, error) {
	if err := h.fake.fail("Insert"); err != nil {
		return ringclient.WriteResult{}, err
	}
	return h.write(id, doc, false)
}

func (h *u52Handle) Upsert(_ context.Context, id string, doc ringclient.Doc, _ ...ringclient.CallOption) (ringclient.WriteResult, error) {
	if err := h.fake.fail("Upsert"); err != nil {
		return ringclient.WriteResult{}, err
	}
	return h.write(id, doc, true)
}

// Update records the statement and applies its set and unset, which is all the CLI tests read
// back; the operators themselves are the server's and are tested there.
func (h *u52Handle) Update(_ context.Context, id string, change ringclient.Update, options ...ringclient.CallOption) (ringclient.WriteResult, error) {
	if err := h.fake.fail("Update"); err != nil {
		return ringclient.WriteResult{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.WriteResult{}, err
	}
	bucket.optionCounts = append(bucket.optionCounts, len(options))
	if refused, err := h.refuse(id, options); err != nil || refused.NotApplied {
		return refused, err
	}
	doc, ok := bucket.docs[id]
	if !ok {
		return ringclient.WriteResult{}, &ringclient.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf("document %q does not exist in %s.%s, so it cannot be updated", id, h.database, h.name)}
	}
	bucket.updates = append(bucket.updates, change)
	for _, element := range change.Set {
		doc[element.Key] = element.Value
	}
	for _, name := range change.Unset {
		delete(doc, name)
	}
	return ringclient.WriteResult{Outcome: "applied", Copies: 1}, nil
}

func (h *u52Handle) Delete(_ context.Context, id string, options ...ringclient.CallOption) (ringclient.WriteResult, error) {
	if err := h.fake.fail("Delete"); err != nil {
		return ringclient.WriteResult{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.WriteResult{}, err
	}
	bucket.optionCounts = append(bucket.optionCounts, len(options))
	if refused, err := h.refuse(id, options); err != nil || refused.NotApplied {
		return refused, err
	}
	delete(bucket.docs, id)
	for i, existing := range bucket.ids {
		if existing == id {
			bucket.ids = append(bucket.ids[:i], bucket.ids[i+1:]...)
			break
		}
	}
	return ringclient.WriteResult{Outcome: "applied", Copies: 1}, nil
}

// refuse answers what a guard on "refuse-me" gets: nothing written, and the current
// document back with the first field that differed.
func (h *u52Handle) refuse(id string, options []ringclient.CallOption) (ringclient.WriteResult, error) {
	if id != "refuse-me" || len(options) == 0 {
		return ringclient.WriteResult{}, nil
	}
	current, err := bson.Marshal(bson.D{{Key: "_id_", Value: id}, {Key: "Status", Value: "cancelled"}})
	if err != nil {
		return ringclient.WriteResult{}, err
	}
	return ringclient.WriteResult{Outcome: "not_applied", NotApplied: true, Reason: "guard", GuardField: "Status", Document: current}, nil
}

func (h *u52Handle) render(bucket *u52Bucket, id string) bson.Raw {
	doc := bson.D{{Key: "_id_", Value: id}}
	for _, field := range bucket.info.Fields[1:] {
		if value, ok := bucket.docs[id][field.Name]; ok && !strings.HasSuffix(field.Type, "Store") {
			doc = append(doc, bson.E{Key: field.Name, Value: value})
		}
	}
	for _, field := range bucket.info.Fields[1:] {
		if !strings.HasSuffix(field.Type, "Store") {
			continue
		}
		count := len(bucket.stores[storeKey(id, field.Name)]) + len(bucket.keyed[storeKey(id, field.Name)])
		noun := "elements"
		if count == 1 {
			noun = "element"
		}
		doc = append(doc, bson.E{Key: field.Name, Value: fmt.Sprintf("%s (%d %s)", field.Type, count, noun)})
	}
	raw, _ := bson.Marshal(doc)
	return raw
}

func (h *u52Handle) Get(_ context.Context, id string) (bson.Raw, error) {
	if err := h.fake.fail("Get"); err != nil {
		return nil, err
	}
	bucket, err := h.get()
	if err != nil {
		return nil, err
	}
	if _, ok := bucket.docs[id]; !ok {
		return nil, fmt.Errorf("ringclient: no document %q in bucket %s.%s: %w", id, h.database, h.name, ringclient.ErrNotFound)
	}
	return h.render(bucket, id), nil
}

// projected is the body of id holding _id_ and the named declared scalar fields in
// declaration order, and a FIELD_TYPE refusal for a name the bucket does not declare.
func (h *u52Handle) projected(bucket *u52Bucket, id string, fields []string) (bson.Raw, error) {
	keep := map[string]bool{}
	for _, name := range fields {
		declared := name == "_id_"
		for _, field := range bucket.info.Fields[1:] {
			declared = declared || field.Name == name && !strings.HasSuffix(field.Type, "Store")
		}
		if !declared {
			return nil, &ringclient.Error{Code: cluster.CodeFieldType, Message: fmt.Sprintf(
				"field %q is not declared in %s.%s. Run 'describe bucket %s in %s'", name, h.database, h.name, h.name, h.database)}
		}
		keep[name] = true
	}
	doc := bson.D{{Key: "_id_", Value: id}}
	for _, field := range bucket.info.Fields[1:] {
		if value, ok := bucket.docs[id][field.Name]; ok && keep[field.Name] {
			doc = append(doc, bson.E{Key: field.Name, Value: value})
		}
	}
	raw, err := bson.Marshal(doc)
	return raw, err
}

func (h *u52Handle) GetFields(_ context.Context, id string, fields []string) (bson.Raw, error) {
	if err := h.fake.fail("GetFields"); err != nil {
		return nil, err
	}
	bucket, err := h.get()
	if err != nil {
		return nil, err
	}
	if _, ok := bucket.docs[id]; !ok {
		return nil, fmt.Errorf("ringclient: no document %q in bucket %s.%s: %w", id, h.database, h.name, ringclient.ErrNotFound)
	}
	return h.projected(bucket, id, fields)
}

func storeKey(id, field string) string { return id + "/" + field }

func (h *u52Handle) Push(_ context.Context, id, field string, element any, _ ...ringclient.CallOption) (ringclient.WriteResult, error) {
	if err := h.fake.fail("Push"); err != nil {
		return ringclient.WriteResult{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.WriteResult{}, err
	}
	raw, err := bson.Marshal(element)
	if err != nil {
		return ringclient.WriteResult{}, err
	}
	bucket.stores[storeKey(id, field)] = append(bucket.stores[storeKey(id, field)], raw)
	return ringclient.WriteResult{Outcome: "applied", Copies: 1}, nil
}

func (h *u52Handle) Put(_ context.Context, id, field, key string, element any, _ ...ringclient.CallOption) (ringclient.WriteResult, error) {
	if err := h.fake.fail("Put"); err != nil {
		return ringclient.WriteResult{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.WriteResult{}, err
	}
	raw, err := bson.Marshal(element)
	if err != nil {
		return ringclient.WriteResult{}, err
	}
	if bucket.keyed[storeKey(id, field)] == nil {
		bucket.keyed[storeKey(id, field)] = map[string]bson.Raw{}
	}
	bucket.keyed[storeKey(id, field)][key] = raw
	return ringclient.WriteResult{Outcome: "applied", Copies: 1}, nil
}

// Inc adds the increments to the flat numeric fields of the element under key, creating it, as the owner does for a plain path.
func (h *u52Handle) Inc(ctx context.Context, id, field, key string, increments map[string]any, options ...ringclient.CallOption) (ringclient.WriteResult, error) {
	_, result, err := h.IncAfter(ctx, id, field, key, increments, options...)
	return result, err
}

func (h *u52Handle) IncAfter(_ context.Context, id, field, key string, increments map[string]any, _ ...ringclient.CallOption) (bson.Raw, ringclient.WriteResult, error) {
	if err := h.fake.fail("Inc"); err != nil {
		return nil, ringclient.WriteResult{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return nil, ringclient.WriteResult{}, err
	}
	if bucket.keyed[storeKey(id, field)] == nil {
		bucket.keyed[storeKey(id, field)] = map[string]bson.Raw{}
	}
	var current bson.D
	if old, ok := bucket.keyed[storeKey(id, field)][key]; ok {
		if err := bson.Unmarshal(old, &current); err != nil {
			return nil, ringclient.WriteResult{}, err
		}
	}
	for path, amount := range increments {
		var added int64
		switch number := amount.(type) {
		case int32:
			added = int64(number)
		case int64:
			added = number
		}
		found := false
		for i := range current {
			if current[i].Key == path {
				current[i].Value = current[i].Value.(int64) + added
				found = true
			}
		}
		if !found {
			current = append(current, bson.E{Key: path, Value: added})
		}
	}
	raw, err := bson.Marshal(current)
	if err != nil {
		return nil, ringclient.WriteResult{}, err
	}
	bucket.keyed[storeKey(id, field)][key] = raw
	return raw, ringclient.WriteResult{Outcome: "applied", Copies: 1}, nil
}

func (h *u52Handle) Pop(_ context.Context, id, field string, _ ...ringclient.CallOption) (bson.Raw, error) {
	if err := h.fake.fail("Pop"); err != nil {
		return nil, err
	}
	bucket, err := h.get()
	if err != nil {
		return nil, err
	}
	elements := bucket.stores[storeKey(id, field)]
	if len(elements) == 0 {
		return nil, fmt.Errorf("ringclient: pop of field %q found nothing: %w", field, ringclient.ErrNotFound)
	}
	bucket.stores[storeKey(id, field)] = elements[1:]
	return elements[0], nil
}

func (h *u52Handle) Take(_ context.Context, id, field, key string, _ ...ringclient.CallOption) (bson.Raw, error) {
	if err := h.fake.fail("Take"); err != nil {
		return nil, err
	}
	bucket, err := h.get()
	if err != nil {
		return nil, err
	}
	element, ok := bucket.keyed[storeKey(id, field)][key]
	if !ok {
		return nil, fmt.Errorf("ringclient: take of %q found nothing: %w", key, ringclient.ErrNotFound)
	}
	delete(bucket.keyed[storeKey(id, field)], key)
	return element, nil
}

func (h *u52Handle) PeekKeyed(_ context.Context, id, field string, count int) (ringclient.ElementPage, error) {
	if err := h.fake.fail("Peek"); err != nil {
		return ringclient.ElementPage{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.ElementPage{}, err
	}
	elements := bucket.stores[storeKey(id, field)]
	return ringclient.ElementPage{Elements: elements[:min(count, len(elements))]}, nil
}

func (h *u52Handle) PopN(_ context.Context, id, field string, n int, _ ...ringclient.CallOption) ([]bson.Raw, error) {
	if err := h.fake.fail("PopN"); err != nil {
		return nil, err
	}
	bucket, err := h.get()
	if err != nil {
		return nil, err
	}
	elements := bucket.stores[storeKey(id, field)]
	taken := elements[:min(n, len(elements))]
	bucket.stores[storeKey(id, field)] = elements[len(taken):]
	return taken, nil
}

func (h *u52Handle) StoreGet(_ context.Context, id, field, key string) (bson.Raw, error) {
	if err := h.fake.fail("StoreGet"); err != nil {
		return nil, err
	}
	bucket, err := h.get()
	if err != nil {
		return nil, err
	}
	element, ok := bucket.keyed[storeKey(id, field)][key]
	if !ok {
		return nil, fmt.Errorf("ringclient: get of %q found nothing: %w", key, ringclient.ErrNotFound)
	}
	return element, nil
}

func (h *u52Handle) StoreList(_ context.Context, id, field string, limit int, after []byte) (ringclient.ElementPage, error) {
	if err := h.fake.fail("StoreList"); err != nil {
		return ringclient.ElementPage{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.ElementPage{}, err
	}
	elements := bucket.stores[storeKey(id, field)]
	start := 0
	if len(after) == 1 {
		start = int(after[0])
	}
	if limit == 0 {
		limit = 100
	}
	end := min(start+limit, len(elements))
	page := ringclient.ElementPage{Elements: elements[start:end]}
	if end < len(elements) {
		page.More, page.Cursor = true, []byte{byte(end)}
	}
	return page, nil
}

func (h *u52Handle) StoreCount(_ context.Context, id, field string) (int64, error) {
	if err := h.fake.fail("StoreCount"); err != nil {
		return 0, err
	}
	bucket, err := h.get()
	if err != nil {
		return 0, err
	}
	return int64(len(bucket.stores[storeKey(id, field)]) + len(bucket.keyed[storeKey(id, field)])), nil
}

func (h *u52Handle) LeaseGet(_ context.Context, id, field string, lease [16]byte) (ringclient.LeaseRead, error) {
	if err := h.fake.fail("LeaseGet"); err != nil {
		return ringclient.LeaseRead{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.LeaseRead{}, err
	}
	elements, ok := bucket.leases[lease]
	return ringclient.LeaseRead{Found: ok, Elements: elements}, nil
}

func (h *u52Handle) Claim(_ context.Context, id, field string, count int, _ ...ringclient.CallOption) (ringclient.Lease, error) {
	if err := h.fake.fail("Claim"); err != nil {
		return ringclient.Lease{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.Lease{}, err
	}
	elements := bucket.stores[storeKey(id, field)]
	if len(elements) == 0 {
		return ringclient.Lease{Write: ringclient.WriteResult{Outcome: "applied", Copies: 1}}, nil
	}
	taken := elements[:min(count, len(elements))]
	lease := [16]byte{0xaa, 0xbb, 15: 0x01}
	bucket.leases[lease] = taken
	return ringclient.Lease{ID: lease, Elements: taken, Write: ringclient.WriteResult{Outcome: "applied", Copies: 1}}, nil
}

func (h *u52Handle) change(id, field string, lease [16]byte, ack bool) (ringclient.LeaseChange, error) {
	bucket, err := h.get()
	if err != nil {
		return ringclient.LeaseChange{}, err
	}
	_, live := bucket.leases[lease]
	if live && ack {
		elements := bucket.stores[storeKey(id, field)]
		bucket.stores[storeKey(id, field)] = elements[len(bucket.leases[lease]):]
	}
	delete(bucket.leases, lease)
	return ringclient.LeaseChange{Write: ringclient.WriteResult{Outcome: "applied", Copies: 1}, Live: live}, nil
}

func (h *u52Handle) Ack(_ context.Context, id, field string, lease [16]byte, _ ...ringclient.CallOption) (ringclient.LeaseChange, error) {
	if err := h.fake.fail("Ack"); err != nil {
		return ringclient.LeaseChange{}, err
	}
	return h.change(id, field, lease, true)
}

func (h *u52Handle) Release(_ context.Context, id, field string, lease [16]byte, _ ...ringclient.CallOption) (ringclient.LeaseChange, error) {
	if err := h.fake.fail("Release"); err != nil {
		return ringclient.LeaseChange{}, err
	}
	return h.change(id, field, lease, false)
}

func (h *u52Handle) Extend(_ context.Context, id, field string, lease [16]byte, _ int64, _ ...ringclient.CallOption) (ringclient.LeaseChange, error) {
	if err := h.fake.fail("Extend"); err != nil {
		return ringclient.LeaseChange{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.LeaseChange{}, err
	}
	_, live := bucket.leases[lease]
	return ringclient.LeaseChange{Write: ringclient.WriteResult{Outcome: "applied", Copies: 1}, Live: live}, nil
}

func (h *u52Handle) List(_ context.Context, filter any, page ringclient.PageRequest) (ringclient.EntryPage, error) {
	if err := h.fake.fail("List"); err != nil {
		return ringclient.EntryPage{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.EntryPage{}, err
	}
	var matched []string
	for _, id := range bucket.ids {
		if u52Matches(bucket.docs[id], filter) {
			matched = append(matched, id)
		}
	}
	start := 0
	if len(page.Cursor) == 1 {
		start = int(page.Cursor[0])
	}
	limit := int(page.Limit)
	if limit == 0 {
		limit = 100
	}
	end := min(start+limit, len(matched))
	out := ringclient.EntryPage{}
	for _, id := range matched[start:end] {
		body := h.render(bucket, id)
		if page.Fields != nil {
			if body, err = h.projected(bucket, id, page.Fields); err != nil {
				return ringclient.EntryPage{}, err
			}
		}
		out.Entries = append(out.Entries, ringclient.Entry{ID: id, Body: body})
	}
	if end < len(matched) {
		out.More, out.Cursor = true, []byte{byte(end)}
	}
	return out, nil
}

func u52Matches(doc ringclient.Doc, filter any) bool {
	want, ok := filter.(bson.D)
	if !ok {
		return true
	}
	for _, entry := range want {
		if fmt.Sprint(doc[entry.Key]) != fmt.Sprint(entry.Value) {
			return false
		}
	}
	return true
}

func (h *u52Handle) Count(ctx context.Context, filter any) (int64, error) {
	if err := h.fake.fail("Count"); err != nil {
		return 0, err
	}
	page, err := h.List(ctx, filter, ringclient.PageRequest{Limit: 1000})
	return int64(len(page.Entries)), err
}

// u52Run runs statements in order on one session and returns everything printed.
func u52Run(session *RingSession, statements ...string) (string, error) {
	var out strings.Builder
	for _, statement := range statements {
		printed, err := session.Execute(context.Background(), statement)
		for _, line := range printed.Lines {
			out.WriteString(line + "\n")
		}
		if err != nil {
			return out.String(), err
		}
	}
	return out.String(), nil
}

func (f *u52Fake) Join(_ context.Context, id, addr string, witness bool) (ringclient.MembershipResult, error) {
	f.calls = append(f.calls, fmt.Sprintf("join %s@%s witness=%t", id, addr, witness))
	return f.membership, f.fail("Join")
}

func (f *u52Fake) Promote(_ context.Context, id string) (ringclient.MembershipResult, error) {
	f.calls = append(f.calls, "promote "+id)
	return f.membership, f.fail("Promote")
}

func (f *u52Fake) ReplaceNode(_ context.Context, id, addr string, acceptLoss bool) (ringclient.MembershipResult, error) {
	f.calls = append(f.calls, fmt.Sprintf("replace-node %s@%s accept_loss=%t", id, addr, acceptLoss))
	return f.membership, f.fail("ReplaceNode")
}

// Nearest answers with the first K elements of the field, keyed ones in key order, each at score 1: the
// fake compares no vectors, so the scores only show which elements the bucket was asked for.
func (h *u52Handle) Nearest(_ context.Context, id, field string, query ringclient.NearestQuery) (ringclient.NearestResult, error) {
	if err := h.fake.fail("Nearest"); err != nil {
		return ringclient.NearestResult{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.NearestResult{}, err
	}
	k := query.K
	if k == 0 {
		k = 10
	}
	var result ringclient.NearestResult
	if keyed := bucket.keyed[storeKey(id, field)]; len(keyed) > 0 {
		keys := make([]string, 0, len(keyed))
		for key := range keyed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			result.Elements = append(result.Elements, keyed[key])
			result.Keys = append(result.Keys, key)
		}
	} else {
		result.Elements = bucket.stores[storeKey(id, field)]
	}
	result.Scanned = len(result.Elements)
	result.Elements = result.Elements[:min(k, len(result.Elements))]
	if len(result.Keys) > 0 {
		result.Keys = result.Keys[:len(result.Elements)]
	}
	for range result.Elements {
		result.Scores = append(result.Scores, 1)
	}
	return result, nil
}

// Aggregate answers from the elements the fake holds: a sum or count per by value over the
// field's elements, refusing an aggregate the bucket did not declare as the server does.
func (h *u52Handle) Aggregate(_ context.Context, id, field string, query ringclient.AggregateQuery) (ringclient.AggregatePage, error) {
	if err := h.fake.fail("Aggregate"); err != nil {
		return ringclient.AggregatePage{}, err
	}
	bucket, err := h.get()
	if err != nil {
		return ringclient.AggregatePage{}, err
	}
	var kept []ringclient.AggregateInfo
	for _, aggregate := range bucket.info.Aggregates {
		if aggregate.Store == field {
			kept = append(kept, aggregate)
		}
	}
	elements := append([][]byte(nil), rawBytes(bucket.stores[storeKey(id, field)])...)
	for _, element := range bucket.keyed[storeKey(id, field)] {
		elements = append(elements, element)
	}
	if query.Kind == "describe" {
		listed := bson.A{}
		for _, aggregate := range kept {
			listed = append(listed, bson.D{{Key: "id", Value: int32(aggregate.ID)}, {Key: "aggregate", Value: ringAggregateText(AggregateDecl{Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By})}, {Key: "state", Value: "ready"}})
		}
		document, err := bson.Marshal(bson.D{{Key: "aggregates", Value: listed}})
		return ringclient.AggregatePage{Document: document}, err
	}
	found := false
	for _, aggregate := range kept {
		found = found || aggregate.Kind == query.Kind && aggregate.Field == query.Field && aggregate.By == query.By
	}
	if !found {
		return ringclient.AggregatePage{}, &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"AGGREGATE_NOT_DECLARED: %s.%s.%s keeps no %q", bucket.info.Database, bucket.info.Name, field, query.Kind)}
	}
	sums, counts := map[string]int64{}, map[string]int64{}
	var order []string
	var total int64
	for _, element := range elements {
		raw := bson.Raw(element)
		amount := int64(0)
		if query.Field != "" {
			amount, _ = raw.Lookup(query.Field).Int64OK()
			if amount == 0 {
				if narrow, ok := raw.Lookup(query.Field).Int32OK(); ok {
					amount = int64(narrow)
				}
			}
		}
		total += amount
		if query.By != "" {
			by := raw.Lookup(query.By).StringValue()
			if _, seen := counts[by]; !seen {
				order = append(order, by)
			}
			sums[by] += amount
			counts[by]++
		}
	}
	if query.Kind == "sum" {
		document, err := bson.Marshal(bson.D{{Key: "sum", Value: total}})
		return ringclient.AggregatePage{Document: document}, err
	}
	sort.Strings(order)
	values := bson.A{}
	for _, by := range order {
		row := bson.D{{Key: "by", Value: by}}
		if query.Kind == "sum by" {
			row = append(row, bson.E{Key: "sum", Value: sums[by]})
		}
		values = append(values, append(row, bson.E{Key: "n", Value: counts[by]}))
	}
	document, err := bson.Marshal(bson.D{{Key: "values", Value: values}})
	return ringclient.AggregatePage{Document: document}, err
}

// rawBytes converts the fake's stored elements to plain byte slices.
func rawBytes(elements []bson.Raw) [][]byte {
	out := make([][]byte, len(elements))
	for i, element := range elements {
		out[i] = element
	}
	return out
}
