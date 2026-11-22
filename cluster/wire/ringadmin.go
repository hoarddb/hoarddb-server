package wire

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// This file is the codec of RING_ADMIN (entry-ring-admin-wire.md): BSON commands that
// create, describe and list databases and buckets, and their reply. Both directions are strict
// like ringlist.go: the size is checked before the document is read, and a body is accepted
// only if it re-encodes to the same bytes.

// The cmd values of a RING_ADMIN request.
const (
	// AdminCreateDatabase creates a database.
	AdminCreateDatabase = "create_database"
	// AdminCreateBucket creates a bucket in a database.
	AdminCreateBucket = "create_bucket"
	// AdminAlterBucket adds and drops fields of a bucket (conventions 20).
	AdminAlterBucket = "alter_bucket"
	// AdminDropBucket drops a bucket and its documents (conventions 21).
	AdminDropBucket = "drop_bucket"
	// AdminDescribeBucket returns a bucket's declaration in force.
	AdminDescribeBucket = "describe_bucket"
	// AdminShowBuckets returns one page of a database's buckets.
	AdminShowBuckets = "show_buckets"
	// AdminShowDatabases returns one page of the databases the session may see.
	AdminShowDatabases = "show_databases"
	// AdminDescribeDatabase returns a database's id and replication factor and one page of its buckets.
	AdminDescribeDatabase = "describe_database"
	// AdminDescribeNode returns the facts of the node that answers: id, build, uptime, cluster
	// size, epoch, leader, default replication factor, serving state and, when it can be read, the host.
	AdminDescribeNode = "describe_node"
)

const (
	// ringAdminSmallBody bounds every command but create_bucket: names, a limit and a cursor.
	ringAdminSmallBody = 8 << 10
	// ringAdminCreateBody bounds a create_bucket and an alter_bucket: 999 fields of two 255-byte strings fit in 1 MiB.
	ringAdminCreateBody = 1 << 20
	// ringAdminDepth is the document, the fields array and one field document.
	ringAdminDepth = 4
	// ringAdminReplyDepth is one more: a show_users reply is the document, the users array, a
	// user, its roles array and one grant.
	ringAdminReplyDepth = 5
	// ringMaxFieldWord bounds a field name and a type word: the declaration stores each behind a one-byte length.
	ringMaxFieldWord = 255
	// ringMaxAdminListed bounds the buckets of one show_buckets page (the page limit ceiling).
	ringMaxAdminListed = maxPageLimit
	// ringMaxAdminState bounds a declaration state word.
	ringMaxAdminState = 32
)

// adminScalarWords are the canonical scalar words of typed-fields-spec 3.1.
var adminScalarWords = []string{"string", "int32", "int", "double", "decimal", "number", "bool", "datetime", "bsontimestamp",
	"binary", "uuid", "objectid", "object", "array", "regex", "null", "any"}

// adminStoreWords are the store words of conventions 17, spelled exactly.
var adminStoreWords = []string{"fifoStore", "lifoStore", "hashStore", "btreeStore", "heapStore", "blobStore"}

// adminSynonyms are the words typed-fields-spec 3.1 accepts besides the canonical one. The
// catalog records one spelling, so the wire refuses them and names the canonical word.
var adminSynonyms = map[string]string{
	"integer": "int", "int64": "int", "float": "double", "boolean": "bool", "date": "datetime", "timestamp": "datetime",
}

// RingAdminField is one declared field of a create_bucket: a name and a canonical type word.
type RingAdminField struct {
	Name, Type string
	// Of is "<Bucket>" or "<Database>.<Bucket>" for a field declared of another bucket's
	// ids; empty for none.
	Of string
}

// RingAdminRequest is a RING_ADMIN body. Which members are set depends on Cmd: create_database
// takes Name and RF; create_bucket takes Database, Name and Fields; describe_bucket takes
// Database and Bucket; alter_bucket takes Database, Bucket, Add and Drop; show_buckets and describe_database take Database, Limit and Cursor;
// show_databases takes Limit and Cursor. A member the command does not take must be zero.
type RingAdminRequest struct {
	// Cmd is one of the Admin constants.
	Cmd string
	// Database names the database of every command but create_database.
	Database string
	// Name is the database to create (create_database) or the bucket to create (create_bucket).
	Name string
	// Bucket is the bucket to describe.
	Bucket string
	// RF is the replication factor of a new database; nil leaves it out and asks for 1.
	RF *int32
	// Storage is the storage word of a new database, "disk" or "memory"; "" means disk. Only
	// create_database takes it.
	Storage string
	// Fields are the declared fields of a new bucket, without the implicit _id_. Non-nil
	// (possibly empty) for create_bucket, nil for the other commands.
	Fields []RingAdminField
	// Aggregates are the maintained aggregates of the new bucket's store fields; only
	// create_bucket takes it and it is left off the wire when empty.
	Aggregates []AggregateDecl
	// Add and Drop are the fields an alter_bucket adds, with their canonical type words, and
	// the names of the fields it drops. Both are non-nil (possibly empty) for alter_bucket,
	// at least one is non-empty, and both are nil for every other command.
	Add  []RingAdminField
	Drop []string
	// ExpireField is the datetime field a create_bucket expires its documents at, or the one an
	// alter_bucket sets (`set expire at`); ExpirePaths names, per fifoStore or lifoStore field of a
	// create_bucket, the element path of its datetime; DropExpire is an alter_bucket's
	// `drop expire at`. All three are zero for every other command, and an alter sets at most one
	// of ExpireField and DropExpire (EXPIRE_TWICE).
	ExpireField string
	ExpirePaths map[string]string
	DropExpire  bool
	// Slot is the slot a seeding command names: required by seeding_retry and seeding_forget,
	// optional (with Database) for seeding_status, absent for every other command.
	Slot *int32
	// Anyway is the owner's override of seeding_forget: it forgets a retired record although no
	// other member attests a copy, so this node may hold the only one. Only seeding_forget takes
	// it, and it is left off the wire when false.
	Anyway bool
	// ExpectBucketID, when not zero, is the id of the bucket a drop_bucket means: a bucket under
	// that name with another id is refused and left alone.
	ExpectBucketID uint64
	// Limit is the page size of a listing; nil leaves it out and asks for the default.
	Limit *int32
	// Cursor is the opaque resume point of the previous page of the same listing.
	Cursor []byte
	// NodeID, DataAddr and Witness are the members of the membership commands: join takes all
	// three, replace_node takes NodeID and DataAddr, the rest take NodeID only. They are zero
	// for every other command.
	NodeID, DataAddr string
	Witness          bool
	// AcceptLoss is replace_node's accept_loss (the CLI's --accept-loss, BUG-0870): replace the
	// node even though it held the only copy of some pairs, whose documents are then lost. Only
	// replace_node takes it, and it is left off the wire when false.
	AcceptLoss bool
	// User holds the members of the seven user commands (ringusers.go); nil for every other
	// command and for show_users.
	User *RingUserRequest
}

// The wire documents. Member order is the spec's table and is the canonical layout.
type adminCreateDatabaseDoc struct {
	Cmd  string `bson:"cmd"`
	Name string `bson:"name"`
	RF   *int32 `bson:"rf,omitempty"`
	// Storage is left out for a disk database, so a client that names no storage sends the
	// document every earlier build sent.
	Storage string `bson:"storage,omitempty"`
}

type adminFieldDoc struct {
	Name string `bson:"name"`
	Type string `bson:"type"`
	// Of is left out for a field with no reference, so a create without 'of' encodes as it
	// always did. The index member of the indexes slice will sit between type and of.
	Of string `bson:"of,omitempty"`
}

type adminAggregateDoc struct {
	Store string `bson:"store"`
	Kind  string `bson:"kind"`
	Field string `bson:"field,omitempty"`
	By    string `bson:"by,omitempty"`
}

type adminCreateBucketDoc struct {
	Cmd        string              `bson:"cmd"`
	Database   string              `bson:"database"`
	Name       string              `bson:"name"`
	Fields     []adminFieldDoc     `bson:"fields"`
	Aggregates []adminAggregateDoc `bson:"aggregates,omitempty"`
	// The expiry members are left out when the bucket declares none, so a create that names no
	// expiry is the document every earlier build sent. Paths are sorted by field name.
	ExpireField string               `bson:"expire_field,omitempty"`
	ExpirePaths []adminExpirePathDoc `bson:"expire_paths,omitempty"`
}

type adminExpirePathDoc struct {
	Field string `bson:"field"`
	Path  string `bson:"path"`
}

type adminAlterDoc struct {
	Cmd      string          `bson:"cmd"`
	Database string          `bson:"database"`
	Bucket   string          `bson:"bucket"`
	Add      []adminFieldDoc `bson:"add"`
	Drop     []string        `bson:"drop"`
	// Left out unless the alter changes expiry.
	ExpireField string `bson:"expire_field,omitempty"`
	DropExpire  bool   `bson:"drop_expire,omitempty"`
}

type adminDescribeDoc struct {
	Cmd      string `bson:"cmd"`
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
}

type adminDropDoc struct {
	Cmd            string `bson:"cmd"`
	Database       string `bson:"database"`
	Bucket         string `bson:"bucket"`
	ExpectBucketID string `bson:"expect_bucket_id,omitempty"`
}

type adminShowDoc struct {
	Cmd      string `bson:"cmd"`
	Database string `bson:"database"`
	Limit    *int32 `bson:"limit,omitempty"`
	Cursor   []byte `bson:"cursor,omitempty"`
}

type adminShowDatabasesDoc struct {
	Cmd    string `bson:"cmd"`
	Limit  *int32 `bson:"limit,omitempty"`
	Cursor []byte `bson:"cursor,omitempty"`
}

// adminCommands lists the cmd values, for the refusal that names them.
const adminCommands = AdminCreateDatabase + ", " + AdminCreateBucket + ", " + AdminAlterBucket + ", " + AdminDropBucket + ", " + AdminDescribeBucket + ", " + AdminShowBuckets +
	", " + AdminShowDatabases + ", " + AdminDescribeDatabase + ", " + AdminDescribeNode + ", " + AdminSeedingStatus + ", " + AdminSeedingRetry + ", " + AdminSeedingForget +
	", " + AdminJoin + ", " + AdminPromote + ", " + AdminLeave + ", " + AdminRemoveNode + ", " + AdminReplaceNode +
	", " + AdminCreateUser + ", " + AdminAlterUser + ", " + AdminDropUser + ", " + AdminGrantRole + ", " + AdminRevokeRole + ", " + AdminShowUsers + " or " + AdminDescribeUser

// checkAdminName refuses a name that is empty, over 1021 bytes, not UTF-8 or holding a
// control character. member is the request member that carried it.
func checkAdminName(cmd, member, name string) error {
	if len(name) == 0 || len(name) > ringMaxName {
		return ringRefusal("%s: %s is %d bytes and a name is 1 to %d. Send the name 'show databases' or 'show buckets' lists, or the one to create", cmd, member, len(name), ringMaxName)
	}
	if !utf8.ValidString(name) {
		return ringRefusal("%s: %s is not valid UTF-8. Send the name as UTF-8 text", cmd, member)
	}
	if at := strings.IndexFunc(name, unicode.IsControl); at >= 0 {
		return ringRefusal("%s: %s has a control character at byte %d, which no name may hold. Send a name of printable characters", cmd, member, at)
	}
	return nil
}

// checkAdminTypeWord refuses a type word the declaration does not record, naming the
// canonical word to send instead.
func checkAdminTypeWord(cmd string, index int, field RingAdminField) error {
	for _, word := range adminScalarWords {
		if field.Type == word {
			return nil
		}
	}
	for _, word := range adminStoreWords {
		if field.Type == word {
			return nil
		}
	}
	if canonical, ok := adminSynonyms[field.Type]; ok {
		return ringRefusal("%s: field %d (%s) has type %q, a synonym; the catalog records one spelling. Send the canonical word %q", cmd, index, field.Name, field.Type, canonical)
	}
	for _, word := range append(append([]string(nil), adminScalarWords...), adminStoreWords...) {
		if strings.EqualFold(field.Type, word) {
			return ringRefusal("%s: field %d (%s) has type %q, spelled wrongly. Send %q exactly", cmd, index, field.Name, field.Type, word)
		}
	}
	for _, plain := range []string{"array", "object"} {
		if strings.HasPrefix(field.Type, plain) {
			return ringRefusal("%s: field %d (%s) has type %q, a parameterized form this build does not take. Send the plain word %q", cmd, index, field.Name, field.Type, plain)
		}
	}
	return ringRefusal("%s: field %d (%s) has type %q, which is not a type word. Send one of %s or one of %s",
		cmd, index, field.Name, field.Type, strings.Join(adminScalarWords, ", "), strings.Join(adminStoreWords, ", "))
}

func checkAdminFields(fields []RingAdminField) error {
	if fields == nil {
		return ringRefusal("create_bucket: fields is missing. Send fields as an array of {name, type}, empty for a bucket with no fields")
	}
	if len(fields) > maxUserFields {
		return ringRefusal("create_bucket: fields holds %d entries and a bucket declares at most %d. Send fewer fields, or split the entity into two buckets", len(fields), maxUserFields)
	}
	return checkAdminFieldList(AdminCreateBucket, fields)
}

// checkAdminFieldList bounds the name and the type word of each field of a command.
func checkAdminFieldList(cmd string, fields []RingAdminField) error {
	for i, field := range fields {
		if len(field.Name) == 0 || len(field.Name) > ringMaxFieldWord {
			return ringRefusal("%s: field %d has a name of %d bytes and a field name is 1 to %d. Send a name in that range", cmd, i, len(field.Name), ringMaxFieldWord)
		}
		if !utf8.ValidString(field.Name) || strings.IndexFunc(field.Name, unicode.IsControl) >= 0 {
			return ringRefusal("%s: field %d has a name that is not printable UTF-8. Send a name of printable characters", cmd, i)
		}
		if err := checkAdminTypeWord(cmd, i, field); err != nil {
			return err
		}
		if err := checkAdminOf(cmd, i, field); err != nil {
			return err
		}
	}
	return nil
}

// checkAdminOf bounds the target a field is declared of: one bucket name, or a database
// name, a dot and a bucket name.
func checkAdminOf(cmd string, index int, field RingAdminField) error {
	if field.Of == "" {
		return nil
	}
	database, bucket, qualified := strings.Cut(field.Of, ".")
	if !qualified {
		bucket = database
	}
	if bucket == "" || (qualified && database == "") || strings.Contains(bucket, ".") || len(field.Of) > 2*ringMaxName+1 {
		return ringRefusal("%s: field %d (%s) is declared of %q, which is not a bucket. Send of as \"<Bucket>\" or \"<Database>.<Bucket>\"", cmd, index, field.Name, field.Of)
	}
	return nil
}

// checkAdminAlter bounds an alter_bucket's two lists: both present, neither longer than a
// table, at least one entry between them, and every dropped name a field name.
func checkAdminAlter(req RingAdminRequest) error {
	if req.Add == nil || req.Drop == nil {
		return ringRefusal("alter_bucket: add and drop are both required. Send add as an array of {name, type} and drop as an array of field names, either empty")
	}
	if req.ExpireField != "" && req.DropExpire {
		return ringRefusal("EXPIRE_TWICE: alter_bucket sets expire_field and drop_expire together, and one alter changes the expiry once. Send 'alter bucket <name> in <db> { set expire at <Field> }' or 'alter bucket <name> in <db> { drop expire at }', not both")
	}
	if len(req.Add) == 0 && len(req.Drop) == 0 && req.ExpireField == "" && !req.DropExpire {
		return ringRefusal("alter_bucket: add and drop are both empty, so there is nothing to change. Send a field to add as {name, type}, or the name of a field to drop, or expire_field or drop_expire to change the expiry")
	}
	if len(req.Add) > maxUserFields || len(req.Drop) > maxUserFields {
		return ringRefusal("alter_bucket: add holds %d entries and drop %d, and a bucket declares at most %d fields. Send fewer, or split the change into two alters", len(req.Add), len(req.Drop), maxUserFields)
	}
	if err := checkAdminFieldList(AdminAlterBucket, req.Add); err != nil {
		return err
	}
	for i, name := range req.Drop {
		if len(name) == 0 || len(name) > ringMaxFieldWord {
			return ringRefusal("alter_bucket: drop %d is a name of %d bytes and a field name is 1 to %d. Send a name that describe bucket lists", i, len(name), ringMaxFieldWord)
		}
		if !utf8.ValidString(name) || strings.IndexFunc(name, unicode.IsControl) >= 0 {
			return ringRefusal("alter_bucket: drop %d is not printable UTF-8. Send a name that describe bucket lists", i)
		}
	}
	return nil
}

// adminStray lists the members a command does not take that the request set.
func adminStray(req RingAdminRequest, takes string) error {
	if req.User != nil && !isUserCmd(req.Cmd) {
		return ringRefusal("%s takes no user member (name, expect_seq, expect_drop_seq, credential, enabled, roles, role, db), which only the user commands take. Remove them", req.Cmd)
	}
	if !isMembershipCmd(req.Cmd) {
		for member, set := range map[string]bool{"node_id": req.NodeID != "", "data_addr": req.DataAddr != "", "witness": req.Witness, "accept_loss": req.AcceptLoss} {
			if set {
				return ringRefusal("%s takes no %s, which only the membership commands take. Remove %s", req.Cmd, member, member)
			}
		}
	}
	set := map[string]bool{
		"database": req.Database != "", "name": req.Name != "", "bucket": req.Bucket != "", "rf": req.RF != nil, "storage": req.Storage != "",
		"fields": req.Fields != nil, "aggregates": len(req.Aggregates) > 0, "limit": req.Limit != nil, "cursor": len(req.Cursor) != 0,
		"expire_field": req.ExpireField != "", "expire_paths": req.ExpirePaths != nil, "drop_expire": req.DropExpire,
		"add": req.Add != nil, "drop": req.Drop != nil, "expect_bucket_id": req.ExpectBucketID != 0, "slot": req.Slot != nil, "anyway": req.Anyway,
	}
	for _, member := range []string{"database", "name", "bucket", "rf", "storage", "fields", "aggregates", "add", "drop", "expire_field", "expire_paths", "drop_expire", "slot", "anyway", "limit", "cursor", "expect_bucket_id"} {
		if set[member] && adminExpireMemberOf(req.Cmd, member) {
			continue // create_bucket and alter_bucket take these; their own checks name which
		}
		if set[member] && takes == "" {
			return ringRefusal("%s takes no member besides cmd, and this request also sets %s. Remove %s", req.Cmd, member, member)
		}
		if set[member] && !strings.Contains(" "+takes+" ", " "+member+" ") {
			return ringRefusal("%s takes only %s and cmd, and this request also sets %s. Remove %s", req.Cmd, takes, member, member)
		}
	}
	return nil
}

func checkRingAdmin(req RingAdminRequest) error {
	switch req.Cmd {
	case AdminCreateDatabase:
		if err := adminStray(req, "name rf storage"); err != nil {
			return err
		}
		if req.Storage != "" && req.Storage != AdminStorageDisk && req.Storage != AdminStorageMemory {
			return ringRefusal("create_database: storage is %q and a storage value is disk or memory. Leave storage out for disk", req.Storage)
		}
		if err := checkAdminName(req.Cmd, "name", req.Name); err != nil {
			return err
		}
		if req.RF != nil && *req.RF < 1 {
			return ringRefusal("create_database: rf is %d and a replication factor is 1 or more. Leave rf out for 1", *req.RF)
		}
	case AdminCreateBucket:
		if err := adminStray(req, "database name fields aggregates"); err != nil {
			return err
		}
		if err := checkAdminName(req.Cmd, "database", req.Database); err != nil {
			return err
		}
		if err := checkAdminName(req.Cmd, "name", req.Name); err != nil {
			return err
		}
		return checkAdminFields(req.Fields)
	case AdminAlterBucket:
		if err := adminStray(req, "database bucket add drop"); err != nil {
			return err
		}
		if err := checkAdminName(req.Cmd, "database", req.Database); err != nil {
			return err
		}
		if err := checkAdminName(req.Cmd, "bucket", req.Bucket); err != nil {
			return err
		}
		return checkAdminAlter(req)
	case AdminDescribeBucket, AdminDropBucket:
		takes := "database bucket"
		if req.Cmd == AdminDropBucket {
			takes += " expect_bucket_id"
		}
		if err := adminStray(req, takes); err != nil {
			return err
		}
		if err := checkAdminName(req.Cmd, "database", req.Database); err != nil {
			return err
		}
		return checkAdminName(req.Cmd, "bucket", req.Bucket)
	case AdminShowBuckets:
		if err := adminStray(req, "database limit cursor"); err != nil {
			return err
		}
		if err := checkAdminName(req.Cmd, "database", req.Database); err != nil {
			return err
		}
		if req.Limit != nil && *req.Limit < 0 {
			return ringRefusal("show_buckets: limit is %d and a page holds 1 or more. Leave limit out for the default of %d", *req.Limit, defaultPageLimit)
		}
		if len(req.Cursor) > ringMaxAfter {
			return ringRefusal("show_buckets: cursor is %d bytes and at most %d are taken. Send the cursor of the previous reply unchanged", len(req.Cursor), ringMaxAfter)
		}
	case AdminShowDatabases:
		if err := adminStray(req, "limit cursor"); err != nil {
			return err
		}
		if req.Limit != nil && *req.Limit < 0 {
			return ringRefusal("show_databases: limit is %d and a page holds 1 or more. Leave limit out for the default of %d", *req.Limit, defaultPageLimit)
		}
		if len(req.Cursor) > ringMaxName {
			return ringRefusal("show_databases: cursor is %d bytes and a database name is at most %d. Send the cursor of the previous reply unchanged", len(req.Cursor), ringMaxName)
		}
	case AdminDescribeDatabase:
		if err := adminStray(req, "database limit cursor"); err != nil {
			return err
		}
		if err := checkAdminName(req.Cmd, "database", req.Database); err != nil {
			return err
		}
		if req.Limit != nil && *req.Limit < 0 {
			return ringRefusal("describe_database: limit is %d and a page of buckets holds 1 or more. Leave limit out for the default of %d", *req.Limit, defaultPageLimit)
		}
		if len(req.Cursor) > ringMaxAfter {
			return ringRefusal("describe_database: cursor is %d bytes and at most %d are taken. Send the cursor of the previous reply unchanged", len(req.Cursor), ringMaxAfter)
		}
	case AdminDescribeNode:
		return adminStray(req, "")
	case AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode:
		return checkMembershipRequest(req)
	case AdminSeedingStatus, AdminSeedingRetry, AdminSeedingForget:
		return checkSeedingRequest(req)
	case AdminCreateUser, AdminAlterUser, AdminDropUser, AdminGrantRole, AdminRevokeRole, AdminShowUsers, AdminDescribeUser:
		return checkUserRequest(req)
	default:
		return ringRefusal("cmd %q is not an admin command. Send one of %s", req.Cmd, adminCommands)
	}
	return nil
}

func adminDocOf(req RingAdminRequest) any {
	switch req.Cmd {
	case AdminCreateDatabase:
		return adminCreateDatabaseDoc{Cmd: req.Cmd, Name: req.Name, RF: req.RF, Storage: req.Storage}
	case AdminCreateBucket:
		fields := make([]adminFieldDoc, 0, len(req.Fields))
		for _, field := range req.Fields {
			fields = append(fields, adminFieldDoc(field))
		}
		var aggregates []adminAggregateDoc
		for _, aggregate := range req.Aggregates {
			aggregates = append(aggregates, adminAggregateDoc(aggregate))
		}
		return adminCreateBucketDoc{Cmd: req.Cmd, Database: req.Database, Name: req.Name, Fields: fields, Aggregates: aggregates,
			ExpireField: req.ExpireField, ExpirePaths: adminExpirePathDocs(req.ExpirePaths)}
	case AdminAlterBucket:
		added := make([]adminFieldDoc, 0, len(req.Add))
		for _, field := range req.Add {
			added = append(added, adminFieldDoc(field))
		}
		dropped := req.Drop
		if dropped == nil {
			dropped = []string{}
		}
		return adminAlterDoc{Cmd: req.Cmd, Database: req.Database, Bucket: req.Bucket, Add: added, Drop: dropped,
			ExpireField: req.ExpireField, DropExpire: req.DropExpire}
	case AdminDropBucket:
		doc := adminDropDoc{Cmd: req.Cmd, Database: req.Database, Bucket: req.Bucket}
		if req.ExpectBucketID != 0 {
			doc.ExpectBucketID = adminBucketIDText(req.ExpectBucketID)
		}
		return doc
	case AdminDescribeBucket:
		return adminDescribeDoc{Cmd: req.Cmd, Database: req.Database, Bucket: req.Bucket}
	case AdminShowDatabases:
		return adminShowDatabasesDoc{Cmd: req.Cmd, Limit: req.Limit, Cursor: req.Cursor}
	case AdminDescribeNode:
		return adminNodeRequestDoc{Cmd: req.Cmd}
	case AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode:
		return membershipDocOf(req)
	case AdminSeedingStatus, AdminSeedingRetry, AdminSeedingForget:
		return seedingDocOf(req)
	}
	return adminShowDoc{Cmd: req.Cmd, Database: req.Database, Limit: req.Limit, Cursor: req.Cursor}
}

// EncodeRingAdmin returns the RING_ADMIN body for req. It refuses a request DecodeRingAdmin
// would not accept, with INVALID_ARGUMENT.
func EncodeRingAdmin(req RingAdminRequest) ([]byte, error) {
	if err := checkRingAdmin(req); err != nil {
		return nil, err
	}
	raw, err := adminRequestBytes(req)
	if err != nil {
		return nil, fmt.Errorf("encoding an admin request: %w", err)
	}
	return raw, nil
}

// adminRequestBytes is the canonical layout of a checked request: the user commands are written
// member by member (ringusers.go), every other command from its struct.
func adminRequestBytes(req RingAdminRequest) ([]byte, error) {
	if isUserCmd(req.Cmd) {
		return userRequestBytes(req), nil
	}
	return bson.Marshal(adminDocOf(req))
}

// DecodeRingAdmin reads a RING_ADMIN body written by EncodeRingAdmin. It never panics: the
// size is checked, then the document is validated (size, depth) before it is unpacked, and
// it is accepted only if it re-encodes to the same bytes, so a reordered, duplicated or extra
// member, a widened number and a written default are all refused.
func DecodeRingAdmin(raw []byte) (RingAdminRequest, error) {
	if len(raw) > ringAdminCreateBody {
		return RingAdminRequest{}, ringRefusal("an admin request is %d bytes and at most %d are taken. Send fewer fields", len(raw), ringAdminCreateBody)
	}
	if _, err := bson.Validate(raw, bson.Limits{Depth: ringAdminDepth, Size: ringAdminCreateBody}); err != nil {
		return RingAdminRequest{}, ringRefusal("an admin request is not a valid BSON document: %v", err)
	}
	cmdValue, err := bson.Raw(raw).LookupErr("cmd")
	cmd, isString := cmdValue.StringValueOK()
	if err != nil || !isString {
		return RingAdminRequest{}, ringRefusal("an admin request starts with cmd, a string. Send one of %s", adminCommands)
	}
	if cmd != AdminCreateBucket && cmd != AdminAlterBucket && len(raw) > ringAdminSmallBody {
		return RingAdminRequest{}, ringRefusal("a %s request is %d bytes and at most %d are taken. Send a name, a limit and a cursor only", cmd, len(raw), ringAdminSmallBody)
	}
	req, err := unpackAdmin(cmd, raw)
	if err != nil {
		return RingAdminRequest{}, err
	}
	if err := checkRingAdmin(req); err != nil {
		return RingAdminRequest{}, err
	}
	again, err := adminRequestBytes(req)
	if err != nil {
		return RingAdminRequest{}, ringRefusal("an admin request holds a value that cannot be read back: %v", err)
	}
	if !bytes.Equal(again, raw) {
		if isUserCmd(cmd) {
			return RingAdminRequest{}, errUserLayout(cmd)
		}
		return RingAdminRequest{}, ringRefusal("a %s request does not have the layout of entry-ring-admin-wire.md section 2 (members in order, none duplicated or extra, the stated types, absent rather than written as a default)", cmd)
	}
	return req, nil
}

func unpackAdmin(cmd string, raw []byte) (RingAdminRequest, error) {
	bad := func(err error) (RingAdminRequest, error) {
		return RingAdminRequest{}, ringRefusal("a %s request is not the document of entry-ring-admin-wire.md section 2 (the members and types of that command): %v", cmd, err)
	}
	switch cmd {
	case AdminCreateDatabase:
		var doc adminCreateDatabaseDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		return RingAdminRequest{Cmd: cmd, Name: doc.Name, RF: doc.RF, Storage: doc.Storage}, nil
	case AdminCreateBucket:
		var doc adminCreateBucketDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		fields := make([]RingAdminField, 0, len(doc.Fields))
		for _, field := range doc.Fields {
			fields = append(fields, RingAdminField(field))
		}
		var aggregates []AggregateDecl
		for _, aggregate := range doc.Aggregates {
			aggregates = append(aggregates, AggregateDecl(aggregate))
		}
		return RingAdminRequest{Cmd: cmd, Database: doc.Database, Name: doc.Name, Fields: fields, Aggregates: aggregates,
			ExpireField: doc.ExpireField, ExpirePaths: adminExpirePathMap(doc.ExpirePaths)}, nil
	case AdminAlterBucket:
		var doc adminAlterDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		added := make([]RingAdminField, 0, len(doc.Add))
		for _, field := range doc.Add {
			added = append(added, RingAdminField(field))
		}
		dropped := doc.Drop
		if dropped == nil {
			dropped = []string{}
		}
		return RingAdminRequest{Cmd: cmd, Database: doc.Database, Bucket: doc.Bucket, Add: added, Drop: dropped,
			ExpireField: doc.ExpireField, DropExpire: doc.DropExpire}, nil
	case AdminDropBucket:
		var doc adminDropDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		req := RingAdminRequest{Cmd: cmd, Database: doc.Database, Bucket: doc.Bucket}
		if doc.ExpectBucketID != "" {
			id, err := parseAdminBucketID(doc.ExpectBucketID)
			if err != nil {
				return RingAdminRequest{}, err
			}
			req.ExpectBucketID = id
		}
		return req, nil
	case AdminDescribeBucket:
		var doc adminDescribeDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		return RingAdminRequest{Cmd: cmd, Database: doc.Database, Bucket: doc.Bucket}, nil
	case AdminShowBuckets, AdminDescribeDatabase:
		var doc adminShowDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		return RingAdminRequest{Cmd: cmd, Database: doc.Database, Limit: doc.Limit, Cursor: doc.Cursor}, nil
	case AdminShowDatabases:
		var doc adminShowDatabasesDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		return RingAdminRequest{Cmd: cmd, Limit: doc.Limit, Cursor: doc.Cursor}, nil
	case AdminDescribeNode:
		members, err := bson.Raw(raw).Elements()
		if err != nil {
			return bad(err)
		}
		for _, member := range members {
			if key := member.Key(); key != "cmd" {
				return RingAdminRequest{}, ringRefusal("%s takes no member besides cmd, and this request also sets %s. Remove %s", cmd, key, key)
			}
		}
		return RingAdminRequest{Cmd: cmd}, nil
	case AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode:
		return unpackMembershipRequest(cmd, raw)
	case AdminSeedingStatus, AdminSeedingRetry, AdminSeedingForget:
		return unpackSeedingRequest(cmd, raw)
	case AdminCreateUser, AdminAlterUser, AdminDropUser, AdminGrantRole, AdminRevokeRole, AdminShowUsers, AdminDescribeUser:
		return unpackUserRequest(cmd, raw)
	}
	return RingAdminRequest{}, ringRefusal("cmd %q is not an admin command. Send one of %s", cmd, adminCommands)
}

// RingAdminReplyField is one row of a bucket's field table.
type RingAdminReplyField struct {
	// ID is the field id a RING_KEYED request names the field by.
	ID uint16
	// Name and Type are the declared name and canonical type word.
	Name, Type string
	// Of is the bucket the field is declared of, "<Bucket>", "<Database>.<Bucket>" when it
	// lives in another database, or "#<id>" when the bucket is gone; empty for none.
	Of string
}

// RingAdminReplyExpire is one expiring queue field of a bucket's declaration: the field's name
// and the element path of its datetime.
type RingAdminReplyExpire struct {
	Field, Path string
}

// RingAdminReplyAggregate is one maintained aggregate of a bucket's store field, as describe
// bucket prints it: Store is the field's name, Kind one of sum, sum by and count by, and State
// ready or building.
type RingAdminReplyAggregate struct {
	Store                  string
	ID                     uint8
	Kind, Field, By, State string
}

// RingAdminListed is one bucket of a show_buckets page.
type RingAdminListed struct {
	// Name is the bucket's name.
	Name string
	// BucketID is the id a RING_KEYED header carries.
	BucketID uint64
	// State is the declaration state: active or dropping.
	State string
}

// RingAdminReply is the RING_REPLY body of a RING_ADMIN request. Which members are set
// depends on Cmd; a member the command does not answer with must be zero.
type RingAdminReply struct {
	// Cmd echoes the request.
	Cmd string
	// DatabaseID and RF answer create_database.
	DatabaseID int64
	RF         int32
	// Storage answers create_database and describe_database: "disk" or "memory".
	Storage string
	// MemoryBytes and MemoryCapBytes answer describe_database of a memory database and are
	// zero otherwise.
	MemoryBytes, MemoryCapBytes int64
	// BucketID, State, DeclVersion, SchemaVersionSeen and Fields answer create_bucket,
	// alter_bucket and describe_bucket: the values a RING_KEYED header and its field ids need.
	BucketID          uint64
	State             string
	DeclVersion       int64
	SchemaVersionSeen int64
	Fields            []RingAdminReplyField
	// ExpireField and ExpireStores answer the same three commands with the bucket's expiry as it
	// reads now: the datetime field of the bucket form (empty when none) and each expiring queue field.
	ExpireField  string
	ExpireStores []RingAdminReplyExpire
	// ExpirePending answers describe_bucket: the documents and queue elements of the bucket
	// that are past their time and not yet removed, counted up to a bound; ExpirePendingMore
	// says the true count is at least that. Zero for every other command.
	ExpirePending     int64
	ExpirePendingMore bool
	// Aggregates answers the same three commands with the bucket's maintained aggregates;
	// nil when it declares none.
	Aggregates []RingAdminReplyAggregate
	// Buckets, More and Cursor answer show_buckets. Buckets is non-nil, possibly empty.
	Buckets []RingAdminListed
	More    bool
	Cursor  []byte
	// Name, DatabaseID, RF, Buckets, More and Cursor answer describe_database (Name is the
	// database). Databases, More and Cursor answer show_databases; Databases is non-nil.
	Name      string
	Databases []RingAdminDatabase
	// Membership answers the five membership commands and is nil for every other command.
	Membership *RingAdminMembership
	// Node answers describe_node and is nil for every other command.
	Node *RingAdminNode
	// Seeding answers the three seeding commands and is nil for every other command.
	Seeding *RingAdminSeeding
	// Users answers the seven user commands and is nil for every other command.
	Users *RingAdminUsers
}

// RingAdminDatabase is one database of a show_databases page.
type RingAdminDatabase struct {
	// Name is the database's name.
	Name string
	// DatabaseID is the id the cluster state gives the database.
	DatabaseID int64
	// RF is the replication factor in force.
	RF int32
}

type adminDropReplyDoc struct {
	Cmd      string `bson:"cmd"`
	BucketID string `bson:"bucket_id"`
	State    string `bson:"state"`
}

type adminDatabaseReplyDoc struct {
	Cmd        string `bson:"cmd"`
	DatabaseID int64  `bson:"database_id"`
	RF         int32  `bson:"rf"`
	Storage    string `bson:"storage"`
}

type adminReplyFieldDoc struct {
	ID   int32  `bson:"id"`
	Name string `bson:"name"`
	Type string `bson:"type"`
	Of   string `bson:"of,omitempty"`
}

type adminBucketReplyDoc struct {
	Cmd               string                   `bson:"cmd"`
	BucketID          string                   `bson:"bucket_id"`
	State             string                   `bson:"state"`
	DeclVersion       int64                    `bson:"decl_version"`
	SchemaVersionSeen int64                    `bson:"schema_version_seen"`
	Fields            []adminReplyFieldDoc     `bson:"fields"`
	Aggregates        []adminReplyAggregateDoc `bson:"aggregates,omitempty"`
	// The bucket's expiry as it reads now, left out when it declares none.
	ExpireField  string               `bson:"expire_field,omitempty"`
	ExpireStores []adminExpirePathDoc `bson:"expire_stores,omitempty"`
	// The entries past their time and not yet removed, left out when zero.
	ExpirePending     int64 `bson:"expire_pending,omitempty"`
	ExpirePendingMore bool  `bson:"expire_pending_more,omitempty"`
}

type adminReplyAggregateDoc struct {
	Store string `bson:"store"`
	ID    int32  `bson:"id"`
	Kind  string `bson:"kind"`
	Field string `bson:"field,omitempty"`
	By    string `bson:"by,omitempty"`
	State string `bson:"state"`
}

type adminListedDoc struct {
	Name     string `bson:"name"`
	BucketID string `bson:"bucket_id"`
	State    string `bson:"state"`
}

type adminListReplyDoc struct {
	Cmd     string           `bson:"cmd"`
	Buckets []adminListedDoc `bson:"buckets"`
	More    bool             `bson:"more,omitempty"`
	Cursor  []byte           `bson:"cursor,omitempty"`
}

type adminListedDatabaseDoc struct {
	Name       string `bson:"name"`
	DatabaseID int64  `bson:"database_id"`
	RF         int32  `bson:"rf"`
}

type adminDatabasesReplyDoc struct {
	Cmd       string                   `bson:"cmd"`
	Databases []adminListedDatabaseDoc `bson:"databases"`
	More      bool                     `bson:"more,omitempty"`
	Cursor    []byte                   `bson:"cursor,omitempty"`
}

type adminDescribeDatabaseReplyDoc struct {
	Cmd        string           `bson:"cmd"`
	Name       string           `bson:"name"`
	DatabaseID int64            `bson:"database_id"`
	RF         int32            `bson:"rf"`
	Storage    string           `bson:"storage"`
	Buckets    []adminListedDoc `bson:"buckets"`
	More       bool             `bson:"more,omitempty"`
	Cursor     []byte           `bson:"cursor,omitempty"`
	// MemoryBytes and MemoryCapBytes are present for a memory database only.
	MemoryBytes    int64 `bson:"memory_bytes,omitempty"`
	MemoryCapBytes int64 `bson:"memory_cap_bytes,omitempty"`
}

// adminBucketIDText is a bucket id as the 16 lowercase hex digits describe bucket prints.
func adminBucketIDText(id uint64) string { return fmt.Sprintf("%016x", id) }

func checkAdminState(state string) error {
	if len(state) == 0 || len(state) > ringMaxAdminState {
		return ringRefusal("a bucket state is %d bytes and must be 1 to %d", len(state), ringMaxAdminState)
	}
	return nil
}

func checkRingAdminReply(reply RingAdminReply) error {
	zeroBucket := reply.BucketID == 0 && reply.State == "" && reply.DeclVersion == 0 && reply.SchemaVersionSeen == 0 && reply.Fields == nil && reply.Aggregates == nil
	zeroList := reply.Buckets == nil && !reply.More && len(reply.Cursor) == 0
	zeroDatabase := reply.DatabaseID == 0 && reply.RF == 0 && reply.Storage == "" && reply.MemoryBytes == 0 && reply.MemoryCapBytes == 0
	zeroDatabaseList := reply.Name == "" && reply.Databases == nil
	if reply.Node != nil && reply.Cmd != AdminDescribeNode {
		return ringRefusal("a %s reply holds no node, which only a describe_node reply answers with", reply.Cmd)
	}
	if reply.Membership != nil && !isMembershipCmd(reply.Cmd) {
		return ringRefusal("a %s reply holds no membership member, which only the membership commands answer with", reply.Cmd)
	}
	if reply.Seeding != nil && !isSeedingCmd(reply.Cmd) {
		return ringRefusal("a %s reply holds no seeding member, which only the seeding commands answer with", reply.Cmd)
	}
	if reply.Users != nil && !isUserCmd(reply.Cmd) {
		return ringRefusal("a %s reply holds no user member, which only the user commands answer with", reply.Cmd)
	}
	switch reply.Cmd {
	case AdminCreateUser, AdminAlterUser, AdminDropUser, AdminGrantRole, AdminRevokeRole, AdminShowUsers, AdminDescribeUser:
		return checkUserReply(reply, reply.Node == nil && reply.Membership == nil && reply.Seeding == nil && zeroBucket && zeroList && zeroDatabase && zeroDatabaseList)
	case AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode:
		return checkMembershipReply(reply, reply.Node == nil && reply.Seeding == nil && zeroBucket && zeroList && zeroDatabase && zeroDatabaseList)
	case AdminSeedingStatus, AdminSeedingRetry, AdminSeedingForget:
		if reply.Seeding == nil {
			if reply.Cmd == AdminSeedingStatus {
				return ringRefusal("a seeding_status reply is rows, more and cursor, and nothing else, and this one has no rows")
			}
			return ringRefusal("a %s reply is database and slot, and nothing else, and this one has neither", reply.Cmd)
		}
		if reply.Node != nil || reply.Membership != nil || !zeroBucket || !zeroList || !zeroDatabase || !zeroDatabaseList {
			if reply.Cmd == AdminSeedingStatus {
				return ringRefusal("a seeding_status reply is rows, more and cursor, and nothing else")
			}
			return ringRefusal("a %s reply is database and slot, and nothing else", reply.Cmd)
		}
		return checkSeedingReply(reply.Cmd, reply.Seeding)
	case AdminDescribeNode:
		if reply.Node == nil || !zeroBucket || !zeroList || !zeroDatabase || !zeroDatabaseList {
			return ringRefusal("a describe_node reply is node_id, version, uptime_ms, member_state, members, voters, epoch, leader, default_rf, serving and optionally host, and nothing else")
		}
		return checkAdminNode(reply.Node)
	case AdminDropBucket:
		if reply.BucketID == 0 || reply.State != "dropping" || reply.DeclVersion != 0 || reply.SchemaVersionSeen != 0 || reply.Fields != nil || !zeroList || !zeroDatabase || !zeroDatabaseList {
			return ringRefusal("a drop_bucket reply is bucket_id and state \"dropping\", and nothing else: the documents are deleted in the background")
		}
	case AdminCreateDatabase:
		if reply.DatabaseID < 1 || reply.RF < 1 || !zeroBucket || !zeroList || !zeroDatabaseList || reply.MemoryBytes != 0 || reply.MemoryCapBytes != 0 {
			return ringRefusal("a create_database reply is database_id, rf and storage, the first two 1 or more, and nothing else")
		}
		if err := checkAdminStorage(reply.Storage); err != nil {
			return err
		}
	case AdminCreateBucket, AdminAlterBucket, AdminDescribeBucket:
		if err := checkAdminState(reply.State); err != nil {
			return err
		}
		if reply.DeclVersion < 0 || reply.SchemaVersionSeen < 0 || reply.Fields == nil || !zeroList || !zeroDatabase || !zeroDatabaseList {
			return ringRefusal("a %s reply is bucket_id, state, decl_version, schema_version_seen and fields, and nothing else", reply.Cmd)
		}
		if len(reply.Fields) > maxAdminFieldTable {
			return ringRefusal("a field table holds %d entries and at most %d", len(reply.Fields), maxAdminFieldTable)
		}
		for _, field := range reply.Fields {
			if len(field.Name) == 0 || len(field.Name) > ringMaxFieldWord || len(field.Type) == 0 || len(field.Type) > ringMaxFieldWord {
				return ringRefusal("field %d has a name of %d bytes and a type of %d, each 1 to %d", field.ID, len(field.Name), len(field.Type), ringMaxFieldWord)
			}
		}
	case AdminShowBuckets:
		if reply.Buckets == nil || !zeroBucket || !zeroDatabase || !zeroDatabaseList {
			return ringRefusal("a show_buckets reply is buckets, more and cursor, and nothing else")
		}
		if len(reply.Buckets) > ringMaxAdminListed {
			return ringRefusal("a show_buckets page holds %d buckets and at most %d", len(reply.Buckets), ringMaxAdminListed)
		}
		if len(reply.Cursor) > ringMaxAfter {
			return ringRefusal("cursor is %d bytes and a resume point is at most %d", len(reply.Cursor), ringMaxAfter)
		}
		return checkAdminListed(reply.Buckets)
	case AdminShowDatabases:
		if reply.Databases == nil || reply.Name != "" || reply.Buckets != nil || !zeroBucket || !zeroDatabase {
			return ringRefusal("a show_databases reply is databases, more and cursor, and nothing else")
		}
		if len(reply.Databases) > ringMaxAdminListed {
			return ringRefusal("a show_databases page holds %d databases and at most %d", len(reply.Databases), ringMaxAdminListed)
		}
		if len(reply.Cursor) > ringMaxName {
			return ringRefusal("cursor is %d bytes and a resume point is at most %d", len(reply.Cursor), ringMaxName)
		}
		for _, listed := range reply.Databases {
			if err := checkAdminDatabase(listed.Name, listed.DatabaseID, listed.RF); err != nil {
				return err
			}
		}
	case AdminDescribeDatabase:
		if reply.Databases != nil || reply.Buckets == nil || !zeroBucket {
			return ringRefusal("a describe_database reply is name, database_id, rf, storage, buckets, more and cursor, and optionally memory_bytes and memory_cap_bytes of a memory database, and nothing else")
		}
		if err := checkAdminStorage(reply.Storage); err != nil {
			return err
		}
		if reply.MemoryBytes < 0 || reply.MemoryCapBytes < 0 || (reply.Storage == AdminStorageDisk && (reply.MemoryBytes != 0 || reply.MemoryCapBytes != 0)) {
			return ringRefusal("a describe_database reply carries memory_bytes and memory_cap_bytes only for a memory database, never a negative figure, and this one is %s with %d and %d: send both as 0 for a disk database", reply.Storage, reply.MemoryBytes, reply.MemoryCapBytes)
		}
		if err := checkAdminDatabase(reply.Name, reply.DatabaseID, reply.RF); err != nil {
			return err
		}
		if len(reply.Buckets) > ringMaxAdminListed {
			return ringRefusal("a describe_database page holds %d buckets and at most %d", len(reply.Buckets), ringMaxAdminListed)
		}
		if len(reply.Cursor) > ringMaxAfter {
			return ringRefusal("cursor is %d bytes and a resume point is at most %d", len(reply.Cursor), ringMaxAfter)
		}
		return checkAdminListed(reply.Buckets)
	default:
		return ringRefusal("reply cmd %q is not an admin command (%s)", reply.Cmd, adminCommands)
	}
	return nil
}

// Storage words of the admin wire: the values a create_database request may carry and a
// create_database or describe_database reply always carries.
const (
	AdminStorageDisk   = "disk"
	AdminStorageMemory = "memory"
)

// checkAdminStorage refuses a reply that names no storage or one the wire does not define, so
// an old server answering a new client fails loudly.
func checkAdminStorage(word string) error {
	if word != AdminStorageDisk && word != AdminStorageMemory {
		return ringRefusal("a reply names storage %q, and a database is on \"disk\" or in \"memory\" only: this server and client are different builds, so run the same build on both (upgrade the older one)", word)
	}
	return nil
}

// checkAdminDatabase bounds one database row of a reply.
func checkAdminDatabase(name string, id int64, rf int32) error {
	if len(name) == 0 || len(name) > ringMaxName {
		return ringRefusal("a database name is %d bytes and must be 1 to %d", len(name), ringMaxName)
	}
	if id < 1 || rf < 1 {
		return ringRefusal("database %q has id %d and rf %d, each 1 or more", name, id, rf)
	}
	return nil
}

// checkAdminListed bounds the bucket rows of a reply.
func checkAdminListed(buckets []RingAdminListed) error {
	for _, listed := range buckets {
		if len(listed.Name) == 0 || len(listed.Name) > ringMaxName {
			return ringRefusal("a listed bucket name is %d bytes and must be 1 to %d", len(listed.Name), ringMaxName)
		}
		if err := checkAdminState(listed.State); err != nil {
			return err
		}
	}
	return nil
}

// maxAdminFieldTable is the entry cap of a field table including _id_ (conventions 19).
const maxAdminFieldTable = maxUserFields + 1

func adminReplyDocOf(reply RingAdminReply) any {
	switch reply.Cmd {
	case AdminDropBucket:
		return adminDropReplyDoc{Cmd: reply.Cmd, BucketID: adminBucketIDText(reply.BucketID), State: reply.State}
	case AdminDescribeNode:
		return adminNodeReplyDocOf(reply)
	case AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode:
		return adminMemberReplyDoc{Cmd: reply.Cmd, Epoch: reply.Membership.Epoch, StateSeq: reply.Membership.StateSeq, Result: reply.Membership.Result}
	case AdminSeedingStatus, AdminSeedingRetry, AdminSeedingForget:
		return seedingReplyDocOf(reply.Cmd, reply.Seeding)
	case AdminCreateDatabase:
		return adminDatabaseReplyDoc{Cmd: reply.Cmd, DatabaseID: reply.DatabaseID, RF: reply.RF, Storage: reply.Storage}
	case AdminCreateBucket, AdminAlterBucket, AdminDescribeBucket:
		fields := make([]adminReplyFieldDoc, 0, len(reply.Fields))
		for _, field := range reply.Fields {
			fields = append(fields, adminReplyFieldDoc{ID: int32(field.ID), Name: field.Name, Type: field.Type, Of: field.Of})
		}
		var aggregates []adminReplyAggregateDoc
		for _, aggregate := range reply.Aggregates {
			aggregates = append(aggregates, adminReplyAggregateDoc{Store: aggregate.Store, ID: int32(aggregate.ID), Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By, State: aggregate.State})
		}
		return adminBucketReplyDoc{Cmd: reply.Cmd, BucketID: adminBucketIDText(reply.BucketID), State: reply.State,
			DeclVersion: reply.DeclVersion, SchemaVersionSeen: reply.SchemaVersionSeen, Fields: fields, Aggregates: aggregates,
			ExpireField: reply.ExpireField, ExpireStores: adminExpireStoreDocs(reply.ExpireStores),
			ExpirePending: reply.ExpirePending, ExpirePendingMore: reply.ExpirePendingMore}
	}
	if reply.Cmd == AdminShowDatabases {
		databases := make([]adminListedDatabaseDoc, 0, len(reply.Databases))
		for _, database := range reply.Databases {
			databases = append(databases, adminListedDatabaseDoc(database))
		}
		return adminDatabasesReplyDoc{Cmd: reply.Cmd, Databases: databases, More: reply.More, Cursor: reply.Cursor}
	}
	listed := make([]adminListedDoc, 0, len(reply.Buckets))
	for _, bucket := range reply.Buckets {
		listed = append(listed, adminListedDoc{Name: bucket.Name, BucketID: adminBucketIDText(bucket.BucketID), State: bucket.State})
	}
	if reply.Cmd == AdminDescribeDatabase {
		return adminDescribeDatabaseReplyDoc{Cmd: reply.Cmd, Name: reply.Name, DatabaseID: reply.DatabaseID, RF: reply.RF, Storage: reply.Storage, Buckets: listed, More: reply.More, Cursor: reply.Cursor,
			MemoryBytes: reply.MemoryBytes, MemoryCapBytes: reply.MemoryCapBytes}
	}
	return adminListReplyDoc{Cmd: reply.Cmd, Buckets: listed, More: reply.More, Cursor: reply.Cursor}
}

// EncodeRingAdminReply returns the RING_REPLY body for reply. It refuses a reply
// DecodeRingAdminReply would not accept, with INVALID_ARGUMENT.
func EncodeRingAdminReply(reply RingAdminReply) ([]byte, error) {
	if err := checkRingAdminReply(reply); err != nil {
		return nil, err
	}
	raw, err := adminReplyBytes(reply)
	if err != nil {
		return nil, fmt.Errorf("encoding an admin reply: %w", err)
	}
	// A frame's length counts its opcode and request id as well as the payload.
	if room := maxSubScanBody - 1 - transport.RequestIDLen; len(raw) > room {
		if isUserCmd(reply.Cmd) {
			return nil, ringRefusal("a %s reply would be %d bytes and a frame holds at most %d: the users do not fit one reply. Describe them one at a time with describe_user", reply.Cmd, len(raw), room)
		}
		return nil, ringRefusal("an admin reply would be %d bytes and a frame holds at most %d. Ask for a smaller limit", len(raw), room)
	}
	return raw, nil
}

// adminReplyBytes is the canonical layout of a checked reply, as adminRequestBytes is of a request.
func adminReplyBytes(reply RingAdminReply) ([]byte, error) {
	if isUserCmd(reply.Cmd) {
		return userReplyBytes(reply), nil
	}
	return bson.Marshal(adminReplyDocOf(reply))
}

// DecodeRingAdminReply reads a RING_REPLY body written by EncodeRingAdminReply, with the
// rules of DecodeRingAdmin: validated before it is unpacked, and accepted only if it
// re-encodes to the same bytes.
func DecodeRingAdminReply(raw []byte) (RingAdminReply, error) {
	if _, err := bson.Validate(raw, bson.Limits{Depth: ringAdminReplyDepth, Size: maxSubScanBody}); err != nil {
		return RingAdminReply{}, ringRefusal("an admin reply is not a valid BSON document: %v", err)
	}
	cmdValue, err := bson.Raw(raw).LookupErr("cmd")
	cmd, isString := cmdValue.StringValueOK()
	if err != nil || !isString {
		return RingAdminReply{}, ringRefusal("an admin reply starts with cmd, a string")
	}
	reply, err := unpackAdminReply(cmd, raw)
	if err != nil {
		return RingAdminReply{}, err
	}
	if err := checkRingAdminReply(reply); err != nil {
		return RingAdminReply{}, err
	}
	again, err := adminReplyBytes(reply)
	if err != nil {
		return RingAdminReply{}, ringRefusal("an admin reply holds a value that cannot be read back: %v", err)
	}
	if !bytes.Equal(again, raw) {
		return RingAdminReply{}, ringRefusal("a %s reply does not have the layout of entry-ring-admin-wire.md section 3 (members in order, none duplicated or extra, the stated types, absent rather than written as a default)", cmd)
	}
	return reply, nil
}

func parseAdminBucketID(text string) (uint64, error) {
	id, err := strconv.ParseUint(text, 16, 64)
	if err != nil || adminBucketIDText(id) != text {
		return 0, ringRefusal("bucket_id %q is not 16 lowercase hex digits, for example 1f4a2c9b7de30015", text)
	}
	return id, nil
}

func unpackAdminListed(docs []adminListedDoc) ([]RingAdminListed, error) {
	listed := make([]RingAdminListed, 0, len(docs))
	for _, bucket := range docs {
		id, err := parseAdminBucketID(bucket.BucketID)
		if err != nil {
			return nil, err
		}
		listed = append(listed, RingAdminListed{Name: bucket.Name, BucketID: id, State: bucket.State})
	}
	return listed, nil
}

func unpackAdminReply(cmd string, raw []byte) (RingAdminReply, error) {
	bad := func(err error) (RingAdminReply, error) {
		return RingAdminReply{}, ringRefusal("a %s reply is not the document of entry-ring-admin-wire.md section 3: %v", cmd, err)
	}
	switch cmd {
	case AdminCreateUser, AdminAlterUser, AdminDropUser, AdminGrantRole, AdminRevokeRole, AdminShowUsers, AdminDescribeUser:
		return unpackUserReply(cmd, raw)
	case AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode:
		return unpackMembershipReply(cmd, raw)
	case AdminSeedingStatus, AdminSeedingRetry, AdminSeedingForget:
		return unpackSeedingReply(cmd, raw)
	case AdminDescribeNode:
		var doc adminNodeReplyDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		node, err := adminNodeOf(doc)
		if err != nil {
			return RingAdminReply{}, err
		}
		return RingAdminReply{Cmd: cmd, Node: node}, nil
	case AdminDropBucket:
		var doc adminDropReplyDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		id, err := parseAdminBucketID(doc.BucketID)
		if err != nil {
			return RingAdminReply{}, err
		}
		return RingAdminReply{Cmd: cmd, BucketID: id, State: doc.State}, nil
	case AdminCreateDatabase:
		var doc adminDatabaseReplyDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		return RingAdminReply{Cmd: cmd, DatabaseID: doc.DatabaseID, RF: doc.RF, Storage: doc.Storage}, nil
	case AdminCreateBucket, AdminAlterBucket, AdminDescribeBucket:
		var doc adminBucketReplyDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		id, err := parseAdminBucketID(doc.BucketID)
		if err != nil {
			return RingAdminReply{}, err
		}
		fields := make([]RingAdminReplyField, 0, len(doc.Fields))
		for _, field := range doc.Fields {
			if field.ID < 1 || field.ID > 0xFFFF {
				return RingAdminReply{}, ringRefusal("field id %d is not 1 to 65535", field.ID)
			}
			fields = append(fields, RingAdminReplyField{ID: uint16(field.ID), Name: field.Name, Type: field.Type, Of: field.Of})
		}
		var aggregates []RingAdminReplyAggregate
		for _, aggregate := range doc.Aggregates {
			if aggregate.ID < 1 || aggregate.ID > 255 {
				return RingAdminReply{}, ringRefusal("aggregate id %d is not 1 to 255", aggregate.ID)
			}
			aggregates = append(aggregates, RingAdminReplyAggregate{Store: aggregate.Store, ID: uint8(aggregate.ID), Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By, State: aggregate.State})
		}
		reply := RingAdminReply{Cmd: cmd, BucketID: id, State: doc.State, DeclVersion: doc.DeclVersion, SchemaVersionSeen: doc.SchemaVersionSeen, Fields: fields, Aggregates: aggregates, ExpireField: doc.ExpireField,
			ExpirePending: doc.ExpirePending, ExpirePendingMore: doc.ExpirePendingMore}
		if doc.ExpirePending < 0 {
			return RingAdminReply{}, ringRefusal("expire_pending %d is negative", doc.ExpirePending)
		}
		for _, store := range doc.ExpireStores {
			reply.ExpireStores = append(reply.ExpireStores, RingAdminReplyExpire(store))
		}
		return reply, nil
	case AdminShowBuckets:
		var doc adminListReplyDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		listed, err := unpackAdminListed(doc.Buckets)
		if err != nil {
			return RingAdminReply{}, err
		}
		return RingAdminReply{Cmd: cmd, Buckets: listed, More: doc.More, Cursor: doc.Cursor}, nil
	case AdminShowDatabases:
		var doc adminDatabasesReplyDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		databases := make([]RingAdminDatabase, 0, len(doc.Databases))
		for _, database := range doc.Databases {
			databases = append(databases, RingAdminDatabase(database))
		}
		return RingAdminReply{Cmd: cmd, Databases: databases, More: doc.More, Cursor: doc.Cursor}, nil
	case AdminDescribeDatabase:
		var doc adminDescribeDatabaseReplyDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		listed, err := unpackAdminListed(doc.Buckets)
		if err != nil {
			return RingAdminReply{}, err
		}
		return RingAdminReply{Cmd: cmd, Name: doc.Name, DatabaseID: doc.DatabaseID, RF: doc.RF, Storage: doc.Storage, Buckets: listed, More: doc.More, Cursor: doc.Cursor,
			MemoryBytes: doc.MemoryBytes, MemoryCapBytes: doc.MemoryCapBytes}, nil
	}
	return RingAdminReply{}, ringRefusal("reply cmd %q is not an admin command (%s)", cmd, adminCommands)
}

// adminExpirePathDocs lists a create's store expiry paths sorted by field name, so one request has one encoding.
func adminExpirePathDocs(paths map[string]string) []adminExpirePathDoc {
	if len(paths) == 0 {
		return nil
	}
	docs := make([]adminExpirePathDoc, 0, len(paths))
	for field, path := range paths {
		docs = append(docs, adminExpirePathDoc{Field: field, Path: path})
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].Field < docs[j].Field })
	return docs
}

func adminExpirePathMap(docs []adminExpirePathDoc) map[string]string {
	if len(docs) == 0 {
		return nil
	}
	paths := make(map[string]string, len(docs))
	for _, doc := range docs {
		paths[doc.Field] = doc.Path
	}
	return paths
}

func adminExpireStoreDocs(stores []RingAdminReplyExpire) []adminExpirePathDoc {
	docs := make([]adminExpirePathDoc, 0, len(stores))
	for _, store := range stores {
		docs = append(docs, adminExpirePathDoc(store))
	}
	if len(docs) == 0 {
		return nil
	}
	return docs
}

// adminExpireMemberOf reports whether member is an expiry member the command takes: expire_field and
// expire_paths on create_bucket, expire_field and drop_expire on alter_bucket. adminStray, which this
// function exempts them from, is not asked about them; the other pairings (drop_expire on a create,
// expire_paths on an alter, any of the three on another command) fall through to adminStray's refusal.
func adminExpireMemberOf(cmd, member string) bool {
	switch member {
	case "expire_field":
		return cmd == AdminCreateBucket || cmd == AdminAlterBucket
	case "expire_paths":
		return cmd == AdminCreateBucket
	case "drop_expire":
		return cmd == AdminAlterBucket
	}
	return false
}
