package wire

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file serves RING_ADMIN (entry-ring-admin-wire.md): the commands that create, alter and
// describe a database and a bucket. Each stands for a legacy opcode and is authorized as that
// opcode is (server/ring_authz.go); the work is Node.CreateDatabase, Node.CreateBucket and a
// list over the _buckets system bucket.

func (s *ringServer) admin(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	req, err := DecodeRingAdmin(frame.Payload)
	if err != nil {
		return ringFailure(frame, err)
	}
	var reply RingAdminReply
	if isUserCmd(req.Cmd) {
		reply, err = s.serveUsers(ctx, frame.RequestID, req)
	} else {
		reply, err = s.serveAdmin(ctx, req)
	}
	if err != nil {
		return ringFailure(frame, err)
	}
	payload, err := EncodeRingAdminReply(reply)
	if err != nil {
		return ringFailure(frame, err)
	}
	return transport.NewFrameWithID(transport.OpRingReply, frame.RequestID, payload), nil
}

// adminDenied is the PERMISSION_DENIED answer for an admin command the session may not run
// on the database it named. A database that does not exist and one the session has no role on
// get this one text, which names neither.
func adminDenied() error {
	return &cluster.Error{Code: cluster.CodePermissionDenied, Message: "this session cannot run that command: the database does not exist on this node, " +
		"or the user holds no role on it that allows the command. Nothing was done. Creating a database or a bucket needs the db-manager role on the database; " +
		"describing or listing buckets needs reader. Ask an administrator to run 'grant role <db-manager or reader>@<database> to <user>' if the user should have access"}
}

// referenceDenied is the PERMISSION_DENIED answer for a field declared 'of' a bucket the
// session may not read. A bucket that does not exist and one the session has no role on get
// this one text, so declaring a reference cannot be used to learn what exists.
func referenceDenied() error {
	return &cluster.Error{Code: cluster.CodePermissionDenied, Message: "this session cannot declare a reference to that bucket: it does not exist on this node, " +
		"or the user holds no role on its database that allows reading it. Nothing was created. Declaring a field 'of' a bucket needs read access to that bucket's database. " +
		"Ask an administrator to run 'grant role reader@<database> to <user>' if the user should have access, or declare the field without 'of'"}
}

// authorizeReferences asks, for every field declared 'of' a bucket, whether the session may
// read the target, before the target is looked up.
func (s *ringServer) authorizeReferences(ctx context.Context, database, creating string, fields []RingAdminField) error {
	for _, field := range fields {
		if field.Of == "" {
			continue
		}
		targetDB, target, qualified := strings.Cut(field.Of, ".")
		if !qualified {
			targetDB, target = database, targetDB
		}
		if targetDB == database && target == creating {
			continue // the bucket being created: the create grant covers it
		}
		if err := s.authz.Authorize(ctx, RingAccess{Database: targetDB, Bucket: target, Class: RingClassGet}); err != nil {
			return referenceDenied()
		}
	}
	return nil
}

// ringNodeDenied is the answer to a session the authorizer refuses outright for describe_node
// (no identity, or an internode session): one text, no facts.
func ringNodeDenied() error {
	return &cluster.Error{Code: cluster.CodePermissionDenied, Message: "this session cannot describe the node: the connection is not " +
		"authenticated as a user. Nothing was done. Connect with a user name and password ('hoarddb -u <user>'), then ask again"}
}

// admitAdmin makes the two asks of an admin command before anything is looked up: whether the
// session holds the class on any database, which refuses a session with no grants before the
// name is read, then whether it holds the class on the database as sent.
func (s *ringServer) admitAdmin(ctx context.Context, class RingClass, database, bucket string) error {
	if err := s.authz.Authorize(ctx, RingAccess{Class: class}); err != nil {
		return adminDenied()
	}
	if err := s.authz.Authorize(ctx, RingAccess{Database: database, Bucket: bucket, Class: class}); err != nil {
		return adminDenied()
	}
	return nil
}

// implicitIDField is the document id field every bucket has as entry 1 of its table.
const implicitIDField = "_id_"

func (s *ringServer) serveAdmin(ctx context.Context, req RingAdminRequest) (RingAdminReply, error) {
	switch req.Cmd {
	case AdminCreateDatabase:
		if err := s.admitAdmin(ctx, RingClassCreateDatabase, req.Name, ""); err != nil {
			return RingAdminReply{}, err
		}
		return s.createDatabase(ctx, req)
	case AdminCreateBucket:
		if err := s.admitAdmin(ctx, RingClassCreateBucket, req.Database, req.Name); err != nil {
			return RingAdminReply{}, err
		}
		if err := s.authorizeReferences(ctx, req.Database, req.Name, req.Fields); err != nil {
			return RingAdminReply{}, err
		}
		fields := make([]Field, len(req.Fields))
		for i, field := range req.Fields {
			if field.Name == implicitIDField {
				return RingAdminReply{}, refuse(cluster.CodeInvalidArgument, "create_bucket: field %d is named %q, which every bucket already has as its document id and cannot be declared again. Run 'create bucket %s in %s { … }' without a %s field",
					i, implicitIDField, req.Name, req.Database, implicitIDField)
			}
			fields[i] = Field(field)
		}
		spec := ExpireSpec{BucketField: req.ExpireField, StorePaths: req.ExpirePaths}
		if _, err := s.node.CreateBucketDeclared(ctx, req.Database, req.Name, spec, fields, req.Aggregates); err != nil {
			return RingAdminReply{}, err
		}
		return s.describe(ctx, req.Cmd, req.Database, req.Name)
	case AdminAlterBucket:
		if err := s.admitAdmin(ctx, RingClassAlterBucket, req.Database, req.Bucket); err != nil {
			return RingAdminReply{}, err
		}
		change := AlterChange{Drop: req.Drop, Add: make([]FieldEntry, len(req.Add)), SetExpire: req.ExpireField, DropExpire: req.DropExpire}
		for i, field := range req.Add {
			if field.Of != "" {
				return RingAdminReply{}, refuse(cluster.CodeInvalidArgument, "alter_bucket: field %q is added 'of %s', and alter cannot declare a reference yet, so it would be added without it. Nothing was changed. Declare the reference when the bucket is created ('create bucket <bucket> in <db> { <Field> string of <bucket> }'), or add the field without 'of' and name the bucket in expand",
					field.Name, field.Of)
			}
			change.Add[i] = FieldEntry{Name: field.Name, Type: field.Type}
		}
		if _, err := s.node.AlterBucket(ctx, req.Database, req.Bucket, change); err != nil {
			return RingAdminReply{}, err
		}
		return s.describe(ctx, req.Cmd, req.Database, req.Bucket)
	case AdminDropBucket:
		if err := s.admitAdmin(ctx, RingClassDropBucket, req.Database, req.Bucket); err != nil {
			return RingAdminReply{}, err
		}
		id, err := s.node.StartDropBucketExpecting(ctx, req.Database, req.Bucket, req.ExpectBucketID)
		if err != nil {
			return RingAdminReply{}, err
		}
		return RingAdminReply{Cmd: req.Cmd, BucketID: id, State: "dropping"}, nil
	case AdminDescribeBucket:
		if err := s.admitAdmin(ctx, RingClassDescribeBucket, req.Database, req.Bucket); err != nil {
			return RingAdminReply{}, err
		}
		return s.describe(ctx, req.Cmd, req.Database, req.Bucket)
	case AdminShowDatabases:
		return s.showDatabases(ctx, req)
	case AdminDescribeNode:
		if err := s.authz.Authorize(ctx, RingAccess{Class: RingClassDescribeNode}); err != nil {
			return RingAdminReply{}, ringNodeDenied()
		}
		node := s.node.describeNode(s.facts)
		return RingAdminReply{Cmd: req.Cmd, Node: &node}, nil
	case AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode:
		return s.serveMembership(ctx, req)
	case AdminSeedingStatus, AdminSeedingRetry, AdminSeedingForget:
		return s.serveSeeding(ctx, req)
	case AdminDescribeDatabase:
		if err := s.admitAdmin(ctx, RingClassDescribeDatabase, req.Database, ""); err != nil {
			return RingAdminReply{}, err
		}
		return s.describeDatabase(ctx, req)
	}
	if err := s.admitAdmin(ctx, RingClassShowBuckets, req.Database, ""); err != nil {
		return RingAdminReply{}, err
	}
	return s.showBuckets(ctx, req)
}

// databasePageLimit resolves the limit of a show_databases page: absent or 0 is the default,
// above the page ceiling is refused with the ceiling named.
func databasePageLimit(limit *int32) (int, error) {
	switch {
	case limit == nil || *limit == 0:
		return defaultPageLimit, nil
	case int(*limit) > maxPageLimit:
		return 0, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"show_databases: limit is %d and a page holds at most %d. Send a limit of %d or less and ask for the next page with the cursor", *limit, maxPageLimit, maxPageLimit)}
	}
	return int(*limit), nil
}

// showDatabases lists one page of the databases the session may see, in name order. It makes
// no class-only ask: a session that may see none gets an empty list, as the legacy show
// databases gives it, and learns nothing about the others. Every database after the cursor is
// asked about, so a forged cursor can only move the start; the work is the databases after
// the cursor until the page and one more visible database are found, in memory, with no I/O
// and no look at a bucket. The cursor is the last name returned, so it never names a database
// the session cannot see.
func (s *ringServer) showDatabases(ctx context.Context, req RingAdminRequest) (RingAdminReply, error) {
	want, err := databasePageLimit(req.Limit)
	if err != nil {
		return RingAdminReply{}, err
	}
	databases := s.node.topology.Refresh().Databases
	start := 0
	if len(req.Cursor) > 0 {
		var found bool
		start, found = slices.BinarySearchFunc(databases, string(req.Cursor), func(row cluster.Database, name string) int { return cmp.Compare(row.Name, name) })
		if found {
			start++
		}
	}
	reply := RingAdminReply{Cmd: req.Cmd, Databases: make([]RingAdminDatabase, 0, min(want, len(databases)-start))}
	for _, row := range databases[start:] {
		if s.authz.Authorize(ctx, RingAccess{Database: row.Name, Class: RingClassShowDatabases}) != nil {
			continue
		}
		if len(reply.Databases) == want {
			reply.More = true
			reply.Cursor = []byte(reply.Databases[want-1].Name)
			break
		}
		reply.Databases = append(reply.Databases, RingAdminDatabase{Name: row.Name, DatabaseID: row.ID, RF: row.RF})
	}
	return reply, nil
}

// describeDatabase answers with the database's id and replication factor and one page of its
// buckets, paged as show_buckets pages them. A database that does not exist is NOT_FOUND here
// only for a session already allowed to run the command on every database it could name:
// every other session was refused before this point with the one denial text.
func (s *ringServer) describeDatabase(ctx context.Context, req RingAdminRequest) (RingAdminReply, error) {
	id, err := s.node.database(req.Database)
	if err != nil {
		return RingAdminReply{}, err
	}
	info, ok := s.node.topology.Database(id)
	if !ok {
		return RingAdminReply{}, fmt.Errorf("database %q has id %d but no row in the committed state", req.Database, id)
	}
	reply, err := s.showBuckets(ctx, req)
	if err != nil {
		return RingAdminReply{}, err
	}
	reply.Name, reply.DatabaseID, reply.RF = req.Database, int64(id), int32(info.RF)
	reply.Storage = storageWordOf(info.Memory)
	return reply, nil
}

// createDatabase creates a database with the replication factor the request names, 1 when it
// names none. The committed state machine holds the rule (queries section 5): it refuses a
// factor the cluster's data members cannot hold with RF_UNSUPPORTED, and a refused command
// commits nothing. Creates are serialized so two clients cannot both read the same next
// database id.
func (s *ringServer) createDatabase(ctx context.Context, req RingAdminRequest) (RingAdminReply, error) {
	if req.Storage == AdminStorageMemory {
		// Authority for every client: there is no in-memory store, so the database would live on
		// disk. Refused before the admin lock and before any state is read.
		return RingAdminReply{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.MemoryStorageRefusal(req.Name)}
	}
	rf := int32(defaultReplicationFactor)
	if req.RF != nil {
		rf = *req.RF
	}
	if req.Storage == AdminStorageMemory && rf > 1 {
		// The state machine refuses this too, with the same text; answering here names the fix
		// before the create takes the admin lock or reads the committed state.
		return RingAdminReply{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.MemoryRFRefusal}
	}
	s.adminMu.Lock()
	defer s.adminMu.Unlock()
	id, err := s.node.CreateDatabaseWithStorage(ctx, req.Name, int(rf), req.Storage)
	if err != nil {
		return RingAdminReply{}, s.databaseRefusal(err, int(rf))
	}
	return committedCreateReply(s.node.topology.Refresh(), req, int64(id), rf)
}

// committedCreateReply answers a create from the committed row of the database, not from the
// request. A create that timed out and was retried can find a database of its name that another
// create made with other copies or storage; that is refused with the fix instead of reporting the
// options the caller asked for. Drop is not built in ring mode, so the only fix is another name.
func committedCreateReply(state cluster.ClusterState, req RingAdminRequest, id int64, rf int32) (RingAdminReply, error) {
	i := slices.IndexFunc(state.Databases, func(row cluster.Database) bool { return row.Name == req.Name })
	if i < 0 {
		return RingAdminReply{}, fmt.Errorf("database %q was created as id %d but has no row in the committed state", req.Name, id)
	}
	row := state.Databases[i]
	wantStorage := storageWordOf(req.Storage == AdminStorageMemory)
	if row.RF != rf || row.StorageWord() != wantStorage {
		if row.ID == id {
			// The row is this create's own: it committed, and the options moved afterwards.
			return RingAdminReply{}, &cluster.Error{Code: cluster.CodeConflict, Message: fmt.Sprintf(
				"database %q was created as id %d, and its committed row now shows replication factor %d and storage %s, not the replication factor %d and storage %s you asked for: it was changed after the create committed. Run 'describe database %s' to see it as it is now",
				req.Name, id, row.RF, row.StorageWord(), rf, wantStorage, req.Name)}
		}
		return RingAdminReply{}, &cluster.Error{Code: cluster.CodeConflict, Message: fmt.Sprintf(
			"database %q already exists with replication factor %d and storage %s, not the replication factor %d and storage %s you asked for. Nothing was changed. Run 'show databases' to see what exists, then create yours under another name (drop database is not available yet, so an existing database cannot be removed)",
			req.Name, row.RF, row.StorageWord(), rf, wantStorage)}
	}
	return RingAdminReply{Cmd: req.Cmd, DatabaseID: row.ID, RF: row.RF, Storage: row.StorageWord()}, nil
}

// databaseRefusal adds the RF_UNSUPPORTED detail to the state machine's refusal. The machine's
// text already names the voter and data-member counts it refused on; the detail carries the
// same counts from the committed view.
func (s *ringServer) databaseRefusal(err error, rf int) error {
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeRFUnsupported {
		return err
	}
	view := s.node.topology.current()
	return &ringDetailed{err: coded, detail: cluster.Detail{Rf: rf, Voters: view.voters, DataMembers: len(view.table.Members())}}
}

// expiryDescribeLimit bounds the pending count describe prints: past it the CLI shows "10000+".
const expiryDescribeLimit = 10_000

// describe answers with the declaration in force: the values a RING_KEYED header carries and
// the field table.
func (s *ringServer) describe(ctx context.Context, cmd, database, bucket string) (RingAdminReply, error) {
	ref, err := s.namedBucket(ctx, database, bucket)
	if err != nil {
		return RingAdminReply{}, err
	}
	fields := make([]RingAdminReplyField, 0, len(ref.table.Entries))
	for _, entry := range ref.table.Entries {
		fields = append(fields, RingAdminReplyField{ID: entry.ID, Name: entry.Name, Type: entry.Type, Of: s.referenceText(ctx, ref, entry.ID)})
	}
	reply := RingAdminReply{Cmd: cmd, BucketID: ref.entry.Decl.BucketID, State: ref.entry.Decl.State,
		DeclVersion: ref.version, SchemaVersionSeen: ref.entry.Seen, Fields: fields, Aggregates: replyAggregates(ref.table, ref.entry.Decl.Aggregates)}
	// The expiry as it reads at the published version, with names for the ids the declaration holds.
	expire := machine.EffectiveExpire(ref.entry.Decl, s.node.topology.Published(ref.dbID))
	nameOf := func(id uint16) string {
		if entry, found := entryWithID(ref.table, id); found {
			return entry.Name
		}
		return ""
	}
	reply.ExpireField = nameOf(expire.BucketField)
	for _, expiring := range expire.Stores {
		reply.ExpireStores = append(reply.ExpireStores, RingAdminReplyExpire{Field: nameOf(expiring.FieldID), Path: expiring.Path})
	}
	if cmd == AdminDescribeBucket && s.node.expiry != nil && (expire.BucketField != 0 || len(expire.Stores) != 0) {
		pending, more, err := s.node.expiry.BucketBacklog(ref.dbID, ref.entry.Decl.BucketID, expiryDescribeLimit)
		if err != nil {
			return RingAdminReply{}, fmt.Errorf("counting the expired items of bucket %q in database %q: %w. Run 'describe bucket %s in %s' again, or see the node log", bucket, database, err, bucket, database)
		}
		reply.ExpirePending, reply.ExpirePendingMore = int64(pending), more
	}
	return reply, nil
}

// referenceNamer is the namer of a write refusal: the same name describe prints, which is
// "<unavailable>" for a target the session may not describe.
func (s *ringServer) referenceNamer(ctx context.Context, ref bucketRef) referenceNamer {
	return func(fieldID uint16) string { return s.referenceText(ctx, ref, fieldID) }
}

// referenceUnavailable is what describe prints for a reference whose bucket it may not name.
const referenceUnavailable = "<unavailable>"

// referenceText is how describe names the bucket a field is declared of: "<Bucket>", or
// "<Database>.<Bucket>" when it lives in another database, or "<unavailable>" when the bucket
// is gone or the session may not describe it: the two cannot be told apart, and the id is not
// printed, so a session never learns a name or an id it has no grant on.
func (s *ringServer) referenceText(ctx context.Context, ref bucketRef, fieldID uint16) string {
	bucketID, declared := ref.entry.Decl.ReferenceOf(fieldID)
	if !declared {
		return ""
	}
	far, err := s.node.bucketByID(ctx, bucketID, func(database, bucket string) error {
		return s.authz.Authorize(ctx, RingAccess{Database: database, Bucket: bucket, Class: RingClassDescribeBucket})
	})
	if err != nil {
		return referenceUnavailable
	}
	if far.db != ref.db {
		return far.db + "." + far.name
	}
	return far.name
}

// showBuckets lists one page of the declared buckets by paging the _buckets system bucket
// and skipping the reverse-index documents that share it (conventions 19). A page of
// documents may hold few declarations, so it reads on until it has the limit or the bucket
// ends, then resumes after the last declaration it returned.
func (s *ringServer) showBuckets(ctx context.Context, req RingAdminRequest) (RingAdminReply, error) {
	system, err := s.namedBucket(ctx, req.Database, systemBucketName)
	if err != nil {
		return RingAdminReply{}, err
	}
	limit := 0
	if req.Limit != nil {
		limit = int(*req.Limit)
	}
	want, err := pageLimit(limit, system)
	if err != nil {
		return RingAdminReply{}, err
	}
	cursor := req.Cursor
	if len(cursor) > 0 {
		if _, err := cluster.DecodeCursor(cursor, system.dbID, system.entry.Decl.BucketID); err != nil {
			return RingAdminReply{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"the %s cursor is not one this database issued: %v. Nothing was done. Send %s again without a cursor to start from the first bucket", req.Cmd, err, req.Cmd)}
		}
	}
	listed := make([]RingAdminListed, 0, want)
	for {
		page, err := s.node.listPage(ctx, pageRequest{ref: system, limit: want, cursor: cursor, ceiling: pageBytes})
		if err != nil {
			return RingAdminReply{}, err
		}
		for i, entry := range page.Entries {
			bucket, isDeclaration, err := declaredBucket(entry.Body)
			if err != nil {
				return RingAdminReply{}, fmt.Errorf("listing the buckets of database %q: %w", req.Database, err)
			}
			if !isDeclaration {
				continue
			}
			listed = append(listed, bucket)
			if len(listed) < want {
				continue
			}
			reply := RingAdminReply{Cmd: req.Cmd, Buckets: listed}
			if reply.More = i+1 < len(page.Entries) || page.More; reply.More {
				reply.Cursor = cluster.EncodeCursor(cluster.ListCursor{DatabaseID: system.dbID, BucketID: system.entry.Decl.BucketID, Slot: entry.Slot, EncID: entry.EncID})
			}
			return reply, nil
		}
		if !page.More {
			return RingAdminReply{Cmd: req.Cmd, Buckets: listed}, nil
		}
		cursor = page.Cursor
	}
}

// declaredBucket reads one document of _buckets. A declaration has a string _id_ (the bucket's
// name); a reverse-index document has the id as its binary _id_ and is not a bucket to list.
func declaredBucket(body []byte) (RingAdminListed, bool, error) {
	doc := bson.Raw(body)
	id, err := doc.LookupErr("_id_")
	if err != nil {
		return RingAdminListed{}, false, fmt.Errorf("a _buckets document has no _id_: %w", err)
	}
	if id.Type != bsontype.String {
		return RingAdminListed{}, false, nil
	}
	name := id.StringValue()
	state, err := doc.LookupErr("state")
	if err != nil {
		return RingAdminListed{}, false, fmt.Errorf("the declaration of bucket %q has no state: %w", name, err)
	}
	bucketID, err := doc.LookupErr("bucket_id")
	if err != nil {
		return RingAdminListed{}, false, fmt.Errorf("the declaration of bucket %q has no bucket_id: %w", name, err)
	}
	_, idBytes, ok := bucketID.BinaryOK()
	stateText, stateOK := state.StringValueOK()
	if !ok || len(idBytes) != 8 || !stateOK {
		return RingAdminListed{}, false, errors.New("the declaration of bucket " + strconv.Quote(name) + " has a bucket_id that is not 8 bytes or a state that is not a string")
	}
	var raw uint64
	for _, b := range idBytes {
		raw = raw<<8 | uint64(b)
	}
	return RingAdminListed{Name: name, BucketID: raw, State: stateText}, true, nil
}

// storageWordOf is the word a database answers for its kind: "memory" or "disk". Every database answers one, so the strict reply check accepts the reply of a disk database.
func storageWordOf(memory bool) string {
	if memory {
		return AdminStorageMemory
	}
	return AdminStorageDisk
}
