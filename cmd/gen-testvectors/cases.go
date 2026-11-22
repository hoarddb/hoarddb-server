package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsoncodec"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// nonClientOpcodes are the opcodes the server names that no v1 driver sends as
// a request, each with its reason. Every other opcode the server names must
// have an exchange vector (TestEveryClientOpcodeHasAVector).
var nonClientOpcodes = map[transport.Opcode]string{
	transport.OpAuthOK:         "a response opcode",
	transport.OpAuthFail:       "a response opcode",
	transport.OpGetResponse:    "a response opcode",
	transport.OpExistsResponse: "a response opcode",
	transport.OpBatchPutOK:     "a response opcode",
	transport.OpUpsertOK:       "a response opcode",
	transport.OpBatchUpsertOK:  "a response opcode",
	transport.OpBatchOK:        "a response opcode",
	// Their only vectors ran on the retired replicated-mode cluster and went with it; a driver reads the
	// ring's membership with RING_TOPOLOGY, which has its own vectors.
	transport.OpHealthCheck: "its vectors were captured on the retired replicated-mode cluster; add a ring capture when one is wanted",
	transport.OpListNodes:   "its vectors were captured on the retired replicated-mode cluster; add a ring capture when one is wanted",
	// Removed by the step that generates the batch-request group.
	transport.OpBatch:            "batch-request vectors not generated yet",
	transport.OpFIFOPopResponse:  "a response opcode",
	transport.OpLIFOPopResponse:  "a response opcode",
	transport.OpBTreeResponse:    "a response opcode",
	transport.OpHeapPopResponse:  "a response opcode",
	transport.OpBlobReadResponse: "a response opcode",
	transport.OpPong:             "a response opcode",
	// The legacy user opcodes are not a driver's: users are managed through RING_ADMIN's user
	// commands (entry-ring-admin-wire.md section 10, vectored in the ring group).
	transport.OpUserCreate:       "a legacy user opcode: drivers use RING_ADMIN create_user",
	transport.OpUserAlter:        "a legacy user opcode: drivers use RING_ADMIN alter_user",
	transport.OpUserDrop:         "a legacy user opcode: drivers use RING_ADMIN drop_user",
	transport.OpUserList:         "a legacy user opcode: drivers use RING_ADMIN show_users",
	transport.OpUserGet:          "a legacy user opcode: drivers use RING_ADMIN describe_user",
	transport.OpUserGrant:        "a legacy user opcode: drivers use RING_ADMIN grant_role",
	transport.OpUserRevoke:       "a legacy user opcode: drivers use RING_ADMIN revoke_role",
	transport.OpError:            "a response opcode",
	transport.OpGetFastResponse:  "a response opcode",
	transport.OpStreamAck:        "node-to-node traffic, refused on the driver port",
	transport.OpReplPush:         "node-to-node traffic, refused on the driver port",
	transport.OpReplAck:          "node-to-node traffic, refused on the driver port",
	transport.OpMigrateStart:     "node-to-node traffic, refused on the driver port",
	transport.OpMigrateData:      "node-to-node traffic, refused on the driver port",
	transport.OpMigrateDone:      "node-to-node traffic, refused on the driver port",
	transport.OpClusterPropose:   "node-to-node traffic, refused on the driver port",
	transport.OpVersionedGet:     "node-to-node traffic, refused on the driver port",
	transport.OpMembershipChange: "node-to-node traffic, refused on the driver port",
	transport.OpStreamOpen:       "node-to-node traffic, refused on the driver port",
	transport.OpStreamData:       "node-to-node traffic, refused on the driver port",
	transport.OpStreamEnd:        "node-to-node traffic, refused on the driver port",
	transport.OpAuthConfigPush:   "node-to-node traffic, refused on the driver port",
	transport.OpAuthConfigAck:    "node-to-node traffic, refused on the driver port",
	transport.OpClusterAdd:       "an operator trigger, outside the v1 driver API (handbook wire.md section 4)",
	transport.OpMigrate:          "an operator trigger, outside the v1 driver API (handbook wire.md section 4)",
	transport.OpClusterRemove:    "an operator trigger, outside the v1 driver API (handbook wire.md section 4)",
	transport.OpDumpStart:        "backup, root-only and outside the v1 driver API (driver-spec 4.3: not in v1)",
	transport.OpDumpChunk:        "backup, root-only and outside the v1 driver API (driver-spec 4.3: not in v1)",
	transport.OpDumpDone:         "backup, root-only and outside the v1 driver API (driver-spec 4.3: not in v1)",
	transport.OpRestoreStart:     "restore, root-only and outside the v1 driver API (driver-spec 4.3: not in v1)",
	transport.OpRestoreChunk:     "restore, root-only and outside the v1 driver API (driver-spec 4.3: not in v1)",
	transport.OpRestoreDone:      "restore, root-only and outside the v1 driver API (driver-spec 4.3: not in v1)",
	// Permanent: a restore sends it, a client frame is refused, so a driver
	// never does (claim-ack-spec 7.3).
	transport.OpRingReply:         "a response opcode",
	transport.OpClusterProbeReply: "a response opcode",
	transport.OpDeadRestore:       "restore-only, refused on a client frame; a driver never sends it",
}

// requestTypes maps each client request opcode to the server type its
// handler decodes, which is where the wire names come from.
var requestTypes = map[transport.Opcode]reflect.Type{
	transport.OpAuth:               reflect.TypeFor[server.AuthRequest](),
	transport.OpPut:                reflect.TypeFor[server.InsertRequest](),
	transport.OpUpsert:             reflect.TypeFor[server.InsertRequest](),
	transport.OpBatchUpsert:        reflect.TypeFor[server.BatchInsertRequest](),
	transport.OpBTreeInsert:        reflect.TypeFor[server.InsertRequest](),
	transport.OpGet:                reflect.TypeFor[server.FindRequest](),
	transport.OpExists:             reflect.TypeFor[server.FindRequest](),
	transport.OpBTreeLookup:        reflect.TypeFor[server.FindRequest](),
	transport.OpConsistentGet:      reflect.TypeFor[server.ConsistentGetRequest](),
	transport.OpDelete:             reflect.TypeFor[server.DeleteRequest](),
	transport.OpBatchPut:           reflect.TypeFor[server.BatchInsertRequest](),
	transport.OpFIFOPush:           reflect.TypeFor[server.PushRequest](),
	transport.OpLIFOPush:           reflect.TypeFor[server.PushRequest](),
	transport.OpHeapPush:           reflect.TypeFor[server.PushRequest](),
	transport.OpFIFOPop:            reflect.TypeFor[server.PopRequest](),
	transport.OpLIFOPop:            reflect.TypeFor[server.PopRequest](),
	transport.OpHeapPop:            reflect.TypeFor[server.PopRequest](),
	transport.OpFIFOPeek:           reflect.TypeFor[server.PeekRequest](),
	transport.OpLIFOPeek:           reflect.TypeFor[server.PeekRequest](),
	transport.OpQueueClaim:         reflect.TypeFor[server.ClaimRequest](),
	transport.OpQueueAck:           reflect.TypeFor[server.LeaseRequest](),
	transport.OpQueueRelease:       reflect.TypeFor[server.LeaseRequest](),
	transport.OpQueueExtend:        reflect.TypeFor[server.LeaseRequest](),
	transport.OpDeadList:           reflect.TypeFor[server.DeadListRequest](),
	transport.OpDeadRequeue:        reflect.TypeFor[server.DeadMoveRequest](),
	transport.OpDeadDiscard:        reflect.TypeFor[server.DeadMoveRequest](),
	transport.OpBTreeRange:         reflect.TypeFor[server.BTreeRangeRequest](),
	transport.OpBTreeFirst:         reflect.TypeFor[server.BTreeRangeRequest](),
	transport.OpBTreeLast:          reflect.TypeFor[server.BTreeRangeRequest](),
	transport.OpBlobWrite:          reflect.TypeFor[server.BlobWriteRequest](),
	transport.OpBlobRead:           reflect.TypeFor[server.BlobReadRequest](),
	transport.OpCreateDB:           reflect.TypeFor[server.CreateDatabaseRequest](),
	transport.OpDeleteDB:           reflect.TypeFor[server.DeleteDatabaseRequest](),
	transport.OpCreateBucket:       reflect.TypeFor[server.CreateBucketRequest](),
	transport.OpDeleteBucket:       reflect.TypeFor[server.DeleteBucketRequest](),
	transport.OpListBuckets:        reflect.TypeFor[server.ListBucketsRequest](),
	transport.OpListDBs:            reflect.TypeFor[server.ListDatabasesRequest](),
	transport.OpBucketStats:        reflect.TypeFor[server.BucketStatsRequest](),
	transport.OpCount:              reflect.TypeFor[server.CountRequest](),
	transport.OpUpdate:             reflect.TypeFor[server.UpdateRequest](),
	transport.OpSearch:             reflect.TypeFor[server.SearchRequest](),
	transport.OpScan:               reflect.TypeFor[server.ScanRequest](),
	transport.OpDistinct:           reflect.TypeFor[server.DistinctRequest](),
	transport.OpHealthCheck:        reflect.TypeFor[server.HealthRequest](),
	transport.OpListNodes:          reflect.TypeFor[server.NodeListRequest](),
	transport.OpFieldPush:          reflect.TypeFor[server.FieldRequest](),
	transport.OpFieldPop:           reflect.TypeFor[server.FieldPopRequest](),
	transport.OpFieldPeek:          reflect.TypeFor[server.FieldPeekRequest](),
	transport.OpFieldPut:           reflect.TypeFor[server.FieldRequest](),
	transport.OpFieldGet:           reflect.TypeFor[server.FieldRequest](),
	transport.OpFieldDelete:        reflect.TypeFor[server.FieldRequest](),
	transport.OpFieldLen:           reflect.TypeFor[server.FieldRequest](),
	transport.OpFieldList:          reflect.TypeFor[server.FieldListRequest](),
	transport.OpDeclareStoreFields: reflect.TypeFor[server.DeclareStoreFieldsRequest](),
	transport.OpBucketInfo:         reflect.TypeFor[server.BucketInfoRequest](),
	transport.OpAlterBucket:        reflect.TypeFor[server.AlterBucketRequest](),
	transport.OpUserCreate:         reflect.TypeFor[server.UserCreateRequest](),
	transport.OpUserAlter:          reflect.TypeFor[server.UserAlterRequest](),
	transport.OpUserDrop:           reflect.TypeFor[server.UserDropRequest](),
	transport.OpUserGet:            reflect.TypeFor[server.UserGetRequest](),
	transport.OpUserGrant:          reflect.TypeFor[server.UserGrantRequest](),
	transport.OpUserRevoke:         reflect.TypeFor[server.UserRevokeRequest](),
	transport.OpBatch:              reflect.TypeFor[server.BatchRequest](),
}

// ringRequestFields are the wire names of the BSON ring requests. RING_KEYED and
// CLUSTER_PROBE are binary and have none (their vectors carry payload_hex alone).
var ringRequestFields = map[transport.Opcode][]string{
	transport.OpRingList: ringFieldTable["list"], transport.OpRingCount: ringFieldTable["count"],
	transport.OpRingAdmin: ringFieldTable["admin"], transport.OpRingTopology: ringFieldTable["topology"],
}

// emptyRequests are the requests that carry no payload fields at all: PING,
// and the refusal vectors' opcodes a driver never sends.
var emptyRequests = []transport.Opcode{
	transport.OpPing, transport.OpGossip, transport.Opcode(0x05), transport.OpUserList,
}

// internalRequestFields are wire names a request type carries that a client
// never sends: node-to-node stamps a client's value is ignored in or
// overwritten, and the ones a client sending is refused (handbook wire.md
// section 3).
var internalRequestFields = []string{
	"admitted", "term", "db_gen", "generation", "generations",
	"dropped_generations", "previous_factor", "max_document_bytes",
	"incarnation", "repair_bound", "repair_read", "result_ceiling",
	"lease_now", "lease_nonce", "epoch",
}

// allowedFieldsFor is the wire names a driver may send for op: the request
// type's names, as the server's own BSON codec derives them, minus the
// internal ones.
func allowedFieldsFor(op transport.Opcode) ([]string, bool) {
	if slices.Contains(emptyRequests, op) {
		return []string{}, true
	}
	if names, ring := ringRequestFields[op]; ring {
		return names, true
	}
	requestType, known := requestTypes[op]
	if !known {
		return nil, false
	}
	var names []string
	for _, name := range wireNames(requestType) {
		if !slices.Contains(internalRequestFields, name) {
			names = append(names, name)
		}
	}
	if slices.Contains(writeConcernOpcodes, op) && !slices.Contains(names, "writeconcern") {
		names = append(names, "writeconcern")
	}
	return nonNil(names), true
}

// writeConcernOpcodes are the data mutations whose request type declares no
// write concern field but whose payload the server still reads one from:
// server/write_path.go:frameMeta decodes "writeconcern" from every replicated
// (non-read-only) frame, whatever the handler's own type is.
var writeConcernOpcodes = []transport.Opcode{
	transport.OpFIFOPop, transport.OpLIFOPop, transport.OpHeapPop,
	transport.OpDelete, transport.OpUpdate, transport.OpBlobWrite,
	transport.OpQueueClaim, transport.OpQueueAck, transport.OpQueueRelease, transport.OpQueueExtend,
	transport.OpDeadRequeue, transport.OpDeadDiscard,
}

// wireNames lists a struct's BSON names in field order through the codec's
// own struct-tag parser: an untagged field is its lowercased Go name (never
// its json tag), and an inline struct contributes its own fields.
func wireNames(structType reflect.Type) []string {
	var names []string
	for index := range structType.NumField() {
		field := structType.Field(index)
		if !field.IsExported() {
			continue
		}
		// The codec's own parser in the v1 codec the server encodes with:
		// it is what decides a wire name, and reading tags by hand is what
		// driver-spec 1.9 forbids. Deprecated only for codec 2.0.
		tags, err := bsoncodec.DefaultStructTagParser.ParseStructTags(field) //nolint:staticcheck // SA1019: deliberate, see above
		if err != nil || tags.Skip {
			continue
		}
		if tags.Inline && field.Type.Kind() == reflect.Struct {
			names = append(names, wireNames(field.Type)...)
			continue
		}
		names = append(names, tags.Name)
	}
	return names
}

// responseTypes maps each client request opcode to the server type its
// success answer is encoded from. EXISTS answers a one-field map ({exists})
// and GET_FAST a status byte, so neither has one.
var responseTypes = map[transport.Opcode]reflect.Type{
	transport.OpPut:                reflect.TypeFor[server.InsertResponse](),
	transport.OpUpsert:             reflect.TypeFor[server.InsertResponse](),
	transport.OpBatchUpsert:        reflect.TypeFor[server.BatchInsertResponse](),
	transport.OpBTreeInsert:        reflect.TypeFor[server.InsertResponse](),
	transport.OpGet:                reflect.TypeFor[server.FindResponse](),
	transport.OpBTreeLookup:        reflect.TypeFor[server.FindResponse](),
	transport.OpConsistentGet:      reflect.TypeFor[server.ConsistentGetResponse](),
	transport.OpDelete:             reflect.TypeFor[server.DeleteResponse](),
	transport.OpBatchPut:           reflect.TypeFor[server.BatchInsertResponse](),
	transport.OpFIFOPush:           reflect.TypeFor[server.PushResponse](),
	transport.OpLIFOPush:           reflect.TypeFor[server.PushResponse](),
	transport.OpHeapPush:           reflect.TypeFor[server.PushResponse](),
	transport.OpFIFOPop:            reflect.TypeFor[server.PopResponse](),
	transport.OpLIFOPop:            reflect.TypeFor[server.PopResponse](),
	transport.OpHeapPop:            reflect.TypeFor[server.PopResponse](),
	transport.OpFIFOPeek:           reflect.TypeFor[server.PeekResponse](),
	transport.OpLIFOPeek:           reflect.TypeFor[server.PeekResponse](),
	transport.OpBTreeRange:         reflect.TypeFor[server.BTreeRangeResponse](),
	transport.OpBTreeFirst:         reflect.TypeFor[server.BTreeRangeResponse](),
	transport.OpBTreeLast:          reflect.TypeFor[server.BTreeRangeResponse](),
	transport.OpBlobWrite:          reflect.TypeFor[server.BlobWriteResponse](),
	transport.OpBlobRead:           reflect.TypeFor[server.BlobReadResponse](),
	transport.OpCreateDB:           reflect.TypeFor[server.CreateDatabaseResponse](),
	transport.OpDeleteDB:           reflect.TypeFor[server.DeleteDatabaseResponse](),
	transport.OpCreateBucket:       reflect.TypeFor[server.CreateBucketResponse](),
	transport.OpDeleteBucket:       reflect.TypeFor[server.DeleteBucketResponse](),
	transport.OpListBuckets:        reflect.TypeFor[server.ListBucketsResponse](),
	transport.OpListDBs:            reflect.TypeFor[server.ListDatabasesResponse](),
	transport.OpBucketStats:        reflect.TypeFor[server.BucketStatsResponse](),
	transport.OpCount:              reflect.TypeFor[server.CountResponse](),
	transport.OpUpdate:             reflect.TypeFor[server.UpdateResponse](),
	transport.OpSearch:             reflect.TypeFor[server.SearchResponse](),
	transport.OpScan:               reflect.TypeFor[server.ScanResponse](),
	transport.OpDistinct:           reflect.TypeFor[server.DistinctResponse](),
	transport.OpHealthCheck:        reflect.TypeFor[server.HealthResponse](),
	transport.OpListNodes:          reflect.TypeFor[server.NodeListResponse](),
	transport.OpFieldPush:          reflect.TypeFor[server.FieldResponse](),
	transport.OpFieldPop:           reflect.TypeFor[server.FieldResponse](),
	transport.OpFieldPeek:          reflect.TypeFor[server.FieldResponse](),
	transport.OpFieldPut:           reflect.TypeFor[server.FieldResponse](),
	transport.OpFieldGet:           reflect.TypeFor[server.FieldResponse](),
	transport.OpFieldDelete:        reflect.TypeFor[server.FieldResponse](),
	transport.OpFieldLen:           reflect.TypeFor[server.FieldResponse](),
	transport.OpFieldList:          reflect.TypeFor[server.FieldListResponse](),
	transport.OpDeclareStoreFields: reflect.TypeFor[server.DeclareStoreFieldsResponse](),
	transport.OpBucketInfo:         reflect.TypeFor[server.BucketInfoResponse](),
	transport.OpAlterBucket:        reflect.TypeFor[server.AlterBucketResponse](),
	transport.OpQueueClaim:         reflect.TypeFor[server.ClaimResponse](),
	transport.OpQueueAck:           reflect.TypeFor[server.LeaseResponse](),
	transport.OpQueueRelease:       reflect.TypeFor[server.LeaseResponse](),
	transport.OpQueueExtend:        reflect.TypeFor[server.LeaseResponse](),
	transport.OpDeadList:           reflect.TypeFor[server.DeadListResponse](),
	transport.OpDeadRequeue:        reflect.TypeFor[server.DeadMoveResponse](),
	transport.OpDeadDiscard:        reflect.TypeFor[server.DeadMoveResponse](),
	transport.OpUserCreate:         reflect.TypeFor[server.UserMutationResponse](),
	transport.OpUserAlter:          reflect.TypeFor[server.UserMutationResponse](),
	transport.OpUserDrop:           reflect.TypeFor[server.UserMutationResponse](),
	transport.OpUserGrant:          reflect.TypeFor[server.UserMutationResponse](),
	transport.OpUserRevoke:         reflect.TypeFor[server.UserMutationResponse](),
	transport.OpUserList:           reflect.TypeFor[server.UserListResponse](),
	transport.OpUserGet:            reflect.TypeFor[server.UserGetResponse](),
	transport.OpBatch:              reflect.TypeFor[server.BatchResponse](),
}

// answerFieldsFor is every wire name an answer can carry, nested names as
// paths ("buckets[].name"): the answer type's names, so a field the server
// adds behind omitempty changes the vectors even before any capture sets it.
func answerFieldsFor(answer, request transport.Opcode) []string {
	var answerType reflect.Type
	switch answer {
	case transport.OpError:
		answerType = reflect.TypeFor[server.ErrorResponse]()
	case transport.OpAuthFail:
		answerType = reflect.TypeFor[server.AuthFailResponse]()
	case transport.OpAuthOK:
		answerType = reflect.TypeFor[server.AuthResponse]()
	case transport.OpExistsResponse:
		return []string{"exists"}
	default:
		known, ok := responseTypes[request]
		if !ok {
			return []string{}
		}
		answerType = known
	}
	return nonNil(wirePaths(answerType, ""))
}

// wirePaths is wireNames with the fields of nested structs, element structs
// of slices, and pointed-to structs, as paths. A time, a raw document, a map
// and an interface are leaves.
func wirePaths(structType reflect.Type, prefix string) []string {
	var paths []string
	for index := range structType.NumField() {
		field := structType.Field(index)
		if !field.IsExported() {
			continue
		}
		// The codec's own parser in the v1 codec the server encodes with:
		// it is what decides a wire name, and reading tags by hand is what
		// driver-spec 1.9 forbids. Deprecated only for codec 2.0.
		tags, err := bsoncodec.DefaultStructTagParser.ParseStructTags(field) //nolint:staticcheck // SA1019: deliberate, see above
		if err != nil || tags.Skip {
			continue
		}
		if tags.Inline && field.Type.Kind() == reflect.Struct {
			paths = append(paths, wirePaths(field.Type, prefix)...)
			continue
		}
		path := prefix + tags.Name
		paths = append(paths, path)
		inner, suffix := field.Type, ""
		for inner.Kind() == reflect.Pointer || inner.Kind() == reflect.Slice {
			if inner.Kind() == reflect.Slice {
				suffix = "[]"
			}
			inner = inner.Elem()
		}
		if inner.Kind() == reflect.Struct && inner != reflect.TypeFor[time.Time]() {
			paths = append(paths, wirePaths(inner, path+suffix+".")...)
		}
	}
	return paths
}

// topLevelNames lists a hex-encoded BSON document's top-level field names.
func topLevelNames(payloadHex string) ([]string, error) {
	if payloadHex == "" {
		return nil, nil
	}
	doc, err := decodeHexDocument(payloadHex)
	if err != nil {
		return nil, err
	}
	return documentNames(doc), nil
}

func decodeHex(payloadHex string) ([]byte, error) {
	raw, err := hex.DecodeString(payloadHex)
	if err != nil {
		return nil, fmt.Errorf("not hex: %w", err)
	}
	return raw, nil
}

// The request builders. Each takes the exchange's own database name.

func createDB(db string) call {
	return call{op: transport.OpCreateDB, payload: server.CreateDatabaseRequest{Name: db}}
}

func createBucket(db, bucket string, storeType server.BucketType) call {
	return call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{Database: db, Name: bucket, Type: storeType}}
}

func bucketSetup(db, bucket string, storeType server.BucketType) []call {
	return []call{createDB(db), createBucket(db, bucket, storeType)}
}

func put(db, bucket, key string, doc map[string]any) call {
	return call{op: transport.OpPut, payload: server.InsertRequest{Database: db, Bucket: bucket, Key: key, Data: doc}}
}

func push(op transport.Opcode, db, bucket string, doc map[string]any, priority int64) call {
	return call{op: op, payload: server.PushRequest{Database: db, Bucket: bucket, Data: doc, Priority: priority}}
}

func find(op transport.Opcode, db, bucket, key string) call {
	return call{op: op, payload: server.FindRequest{Database: db, Bucket: bucket, Key: key}}
}

func field(op transport.Opcode, request server.FieldRequest) call {
	return call{op: op, payload: request}
}

// ada is the document most vectors store.
func ada() map[string]any {
	return map[string]any{"Name": "Ada", "Born": int32(1815)}
}

// grace is a second document.
func grace() map[string]any {
	return map[string]any{"Name": "Grace", "Born": int32(1906)}
}

// linus is a third document.
func linus() map[string]any {
	return map[string]any{"Name": "Linus", "Born": int32(1969)}
}

// storeTypes are the six store types in the handbook's order.
var storeTypes = []server.BucketType{
	server.BucketTypeHash, server.BucketTypeBTree, server.BucketTypeFIFO,
	server.BucketTypeLIFO, server.BucketTypeHeap, server.BucketTypeBlob,
}

// userStores is the store-field declaration of the field-store vectors: one
// store of each type, as AGENTS.md rule 11's User bucket declares them.
func userStores() []any {
	return []any{
		map[string]any{"name": "Purchases", "engine": "fifo"},
		map[string]any{"name": "Photos", "engine": "lifo"},
		map[string]any{"name": "Friends", "engine": "hash"},
		map[string]any{"name": "Recipes", "engine": "btree"},
		map[string]any{"name": "Books", "engine": "heap"},
		map[string]any{"name": "Data", "engine": "blob"},
	}
}

// userSetup creates the User bucket with its store fields and the parent
// document "ada".
func userSetup(db string) []call {
	return []call{
		createDB(db),
		{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
			Database: db, Name: "User", Type: server.BucketTypeHash,
			Options: map[string]any{"stores": userStores()},
		}},
		put(db, "User", "ada", ada()),
	}
}

// exchangeCases is every live capture, in file order. Cluster cases come
// last of the original table; the cluster ones are retired slots. A case's database is fixed by its table number.
func exchangeCases() []func(db string) exchangeCase {
	var cases []func(db string) exchangeCase
	cases = append(cases, sessionCases()...)
	cases = append(cases, ddlCases()...)
	cases = append(cases, writeCases()...)
	cases = append(cases, readCases()...)
	cases = append(cases, queryCases()...)
	for _, build := range fieldStoreCases() {
		// A store-field element is stored with an _id_ the server mints (a
		// UUIDv7), whatever its element key.
		cases = append(cases, func(db string) exchangeCase {
			c := build(db)
			c.varyPaths = append(c.varyPaths, "element._id_", "entries[].element._id_", "items[].element._id_")
			c.notes = strings.TrimSpace(c.notes + " The field-store verbs are the newest wire surface: frozen, " +
				"and the likeliest to see the first revisions (driver-spec 4.3).")
			return c
		})
	}
	cases = append(cases, errorCases()...)
	cases = append(cases, limitCases()...)
	cases = append(cases, clusterCases()...)
	// Last, so every earlier exchange keeps its number, database name and request ID.
	cases = append(cases, updateOperatorCases()...)
	cases = append(cases, missingBucketWriteCases()...)
	cases = append(cases, verbMatrixCases()...)
	// Appended after that, the same rule: BUCKET_INFO is newer than every case above.
	cases = append(cases, bucketInfoCases()...)
	cases = append(cases, searchTruncationCases()...)
	// Appended after that, the same rule: pop({limit: N}) is newer than every case above.
	cases = append(cases, popLimitCases()...)
	cases = append(cases, fieldPopLimitCases()...)
	// Appended after that, the same rule: the lease verbs are newer than every case above.
	cases = append(cases, claimCases()...)
	// Appended after that, the same rule: user administration and the dead verbs' after are newer than every case above.
	cases = append(cases, userCases()...)
	cases = append(cases, deadAfterCases()...)
	// Appended after that, the same rule: the uniform wrong-type refusal (BUG-0469) is newer than every case above.
	cases = append(cases, wrongTypeRefusalCases()...)
	// Appended after that, the same rule: ALTER_BUCKET (0x8E) is newer than every case above.
	cases = append(cases, alterBucketCases()...)
	cases = append(cases, claimOverCapCases()...)
	// Appended after that, the same rule: a consistent get's missing-bucket refusal (BUG-0469) is newer than every case above.
	cases = append(cases, consistentGetMissingBucketCases()...)
	// Appended after that, the same rule: the alter of a missing bucket and the alter on a standalone node (BUG-0685).
	cases = append(cases, alterMissingCases()...)
	// Appended after that, the same rule: BATCH (0x22) is newer than every case above.
	cases = append(cases, batchCases()...)
	// Appended after that, the same rule: UPSERT and BATCH_UPSERT are newer than every case above.
	cases = append(cases, upsertCases()...)
	cases = append(cases, placementCases()...)
	// Appended after that, the same rule: insert refusing a held _id_ is newer than every case above.
	cases = append(cases, insertExistsCases()...)
	// Appended last, the same rule: the explicit-spread and no-placement unique creates came with the default of whole.
	cases = append(cases, placementDefaultCases()...)
	return cases
}

// notApplied is the answer field of a write refused before anything was
// applied, on a standalone node as on a cluster (BUG-0514).
func notApplied() map[string]any {
	return map[string]any{"write_outcome": "not_applied"}
}

// missingBucketWriteCases are writes to a bucket that does not exist: refused
// NOT_FOUND, and nothing was written (BUG-0514). Appended after every other
// group so no earlier exchange is renumbered.
func missingBucketWriteCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "put to a bucket that does not exist", group: "errors", storeType: "hash",
				setup: []call{createDB(db)}, request: put(db, "users", "ada", ada()),
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound, wantField: notApplied(),
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "fifo push to a bucket that does not exist", group: "errors", storeType: "fifo",
				setup: []call{createDB(db)}, request: push(transport.OpFIFOPush, db, "q", ada(), 0),
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound, wantField: notApplied(),
			}
		},
	}
}

func sessionCases() []func(db string) exchangeCase {
	authOK := func(username, password string) server.AuthRequest {
		return server.AuthRequest{Username: username, Password: password, ProtocolVersion: 1, Capabilities: []string{}, Client: "hoarddb-vectors/1"}
	}
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "ping before authentication", group: "session", connection: connFresh,
				notes:      "PING is served before AUTH, so it is a pre-authentication liveness probe. The PONG is answered by the transport layer with no payload.",
				request:    call{op: transport.OpPing},
				wantOpcode: transport.OpPong,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "auth with a password", group: "auth", connection: connFresh,
				notes: "capabilities is varied: the list grows with the server version, so it is not compared and a driver " +
					"must not depend on it being frozen. What is frozen is the rule that a driver ignores strings it does not " +
					"know and sends an option only when the server advertised its capability (wire.md section 6).",
				request:    call{op: transport.OpAuth, payload: authOK(rootUser, rootPassword)},
				wantOpcode: transport.OpAuthOK,
				vary:       []string{"capabilities"},
				wantField:  map[string]any{"success": true, "protocol_version": 1},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "auth with a resumption token", group: "auth", server: "auth", connection: connFresh, noReplay: true,
				notes: "The setup's AUTH mints the token this request presents on the same connection (a refresh); the " +
					"token differs every run, so the request is recorded with it normalised and is not replayed.",
				setup:         []call{{op: transport.OpAuth, payload: authOK(rootUser, rootPassword)}},
				request:       call{op: transport.OpAuth, payload: tokenAuth{username: rootUser}},
				useSetupToken: true,
				wantOpcode:    transport.OpAuthOK,
				vary:          []string{"capabilities"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "auth refused: wrong password", group: "auth", server: "auth", connection: connFresh,
				request:     call{op: transport.OpAuth, payload: authOK(rootUser, "not-the-password")},
				wantOpcode:  transport.OpAuthFail,
				wantReason:  transport.AuthFailCredentials,
				wantMessage: []string{"invalid username or password"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "auth refused: unknown user", group: "auth", server: "auth", connection: connFresh,
				request:     call{op: transport.OpAuth, payload: authOK("nobody", rootPassword)},
				wantOpcode:  transport.OpAuthFail,
				wantReason:  transport.AuthFailCredentials,
				wantMessage: []string{"invalid username or password"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "auth refused: invalid token", group: "auth", server: "auth", connection: connFresh,
				request:     call{op: transport.OpAuth, payload: server.AuthRequest{Username: rootUser, ProtocolVersion: 1, Capabilities: []string{}, Client: "hoarddb-vectors/1", Token: "not-a-token"}},
				wantOpcode:  transport.OpAuthFail,
				wantReason:  transport.AuthFailTokenInvalid,
				wantMessage: []string{"authenticate again with a password"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "auth refused: expired token", group: "auth", server: "auth", connection: connFresh, noReplay: true,
				notes: "The setup's AUTH mints a one-second token (HOARDDB_TOKEN_TTL=1, HOARDDB_AUTH_SKEW=0); the capture " +
					"waits it out and presents it. Recorded with the token normalised; not replayed.",
				setup: []call{{op: transport.OpAuth, payload: authOK(rootUser, rootPassword)}},
				before: func(ctx context.Context, g *generation, db string) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(2500 * time.Millisecond):
						return nil
					}
				},
				request:       call{op: transport.OpAuth, payload: tokenAuth{username: rootUser}},
				useSetupToken: true,
				wantOpcode:    transport.OpAuthFail,
				wantReason:    transport.AuthFailTokenExpired,
				wantMessage:   []string{"AUTH token expired"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "auth refused: protocol mismatch", group: "auth", server: "auth", connection: connFresh,
				request:     call{op: transport.OpAuth, payload: server.AuthRequest{Username: rootUser, Password: rootPassword, ProtocolVersion: 2, Capabilities: []string{}, Client: "hoarddb-vectors/1"}},
				wantOpcode:  transport.OpAuthFail,
				wantReason:  transport.AuthFailProtocolMismatch,
				wantMessage: []string{"server speaks protocol 1, client sent 2"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "auth refused: a second password on one connection", group: "auth",
				notes:       "The harness authenticated the connection with the password first; a connection has one password attempt.",
				request:     call{op: transport.OpAuth, payload: authOK(rootUser, rootPassword)},
				wantOpcode:  transport.OpAuthFail,
				wantReason:  transport.AuthFailAlreadyAuthed,
				wantMessage: []string{"connection is already authenticated"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "auth refused: rate limited", group: "auth", server: "ratelimit", connection: connFresh, noReplay: true,
				notes: "Before this request, one AUTH with a wrong password is sent from the same address on another " +
					"connection; the server refuses the address after HOARDDB_TCP_AUTH_MAX_FAILS=1 failure, checks no " +
					"credential, and closes the connection.",
				before:      wrongPasswordElsewhere,
				request:     call{op: transport.OpAuth, payload: authOK(rootUser, rootPassword)},
				wantOpcode:  transport.OpAuthFail,
				wantReason:  transport.AuthFailRateLimited,
				wantMessage: []string{"too many failed"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "request before authentication", group: "errors", connection: connFresh,
				request:     find(transport.OpGet, db, "b", "ada"),
				wantOpcode:  transport.OpError,
				wantCode:    transport.ErrNotAuthenticated,
				wantMessage: []string{"send AUTH first"},
			}
		},
	}
}

// tokenAuth is an AUTH request whose token is the setup's, filled at capture.
type tokenAuth struct {
	username string
}

func (t tokenAuth) with(token string) any {
	return server.AuthRequest{Username: t.username, ProtocolVersion: 1, Capabilities: []string{}, Client: "hoarddb-vectors/1", Token: token}
}

// wrongPasswordElsewhere spends the address's one allowed failure on another
// connection to the rate-limited server.
func wrongPasswordElsewhere(ctx context.Context, g *generation, db string) error {
	running, ok := g.servers["ratelimit"]
	if !ok {
		return fmt.Errorf("the ratelimit server is not running")
	}
	conn, err := dial(ctx, running.endpoint)
	if err != nil {
		return err
	}
	defer conn.close()
	payload, err := bson.Marshal(server.AuthRequest{Username: rootUser, Password: "not-the-password", ProtocolVersion: 1, Capabilities: []string{}})
	if err != nil {
		return fmt.Errorf("encoding AUTH: %w", err)
	}
	answer, err := conn.roundTrip(transport.OpAuth, fixedRequestID(0), payload)
	if err != nil {
		return err
	}
	if answer.Opcode != transport.OpAuthFail {
		return fmt.Errorf("a wrong password answered %s", answer.Opcode)
	}
	return nil
}

func ddlCases() []func(db string) exchangeCase {
	cases := []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create database", group: "ddl",
				request: createDB(db), wantOpcode: transport.OpCreateDB,
				wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create database that exists", group: "ddl",
				setup: []call{createDB(db)}, request: createDB(db),
				wantOpcode: transport.OpError, wantCode: transport.ErrAlreadyExists,
				wantMessage: []string{fmt.Sprintf("database %q already exists", db)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create database with a name over 255 bytes", group: "ddl",
				request:    createDB(db + strings.Repeat("n", 253)),
				wantOpcode: transport.OpCreateDB, wantField: map[string]any{"success": false},
				wantMessage: []string{"255"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "drop database", group: "ddl",
				setup:   []call{createDB(db)},
				request: call{op: transport.OpDeleteDB, payload: server.DeleteDatabaseRequest{Name: db}}, wantOpcode: transport.OpDeleteDB,
				wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "drop database that does not exist", group: "ddl",
				request:    call{op: transport.OpDeleteDB, payload: server.DeleteDatabaseRequest{Name: db}},
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
			}
		},
	}
	for _, storeType := range storeTypes {
		cases = append(cases, func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket: " + string(storeType), group: "ddl", storeType: string(storeType),
				setup: []call{createDB(db)}, request: createBucket(db, "b", storeType),
				wantOpcode: transport.OpCreateBucket, wantField: map[string]any{"success": true},
			}
		})
	}
	cases = append(cases,
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket in a database that does not exist", group: "ddl", storeType: "hash",
				notes:   "The database is created with the bucket.",
				request: createBucket(db, "b", server.BucketTypeHash), wantOpcode: transport.OpCreateBucket,
				wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket that exists with the same type", group: "ddl", storeType: "hash",
				notes:   "Idempotent by design: success true, and the message says it already exists. Not a refusal.",
				setup:   bucketSetup(db, "b", server.BucketTypeHash),
				request: createBucket(db, "b", server.BucketTypeHash), wantOpcode: transport.OpCreateBucket,
				wantField:   map[string]any{"success": true},
				wantMessage: []string{"already exists (type: hash)"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket that exists with another type", group: "ddl", storeType: "btree",
				setup:      bucketSetup(db, "b", server.BucketTypeHash),
				request:    createBucket(db, "b", server.BucketTypeBTree),
				wantOpcode: transport.OpError, wantCode: transport.ErrAlreadyExists,
				wantMessage: []string{"hash", "btree"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket of an unsupported type", group: "ddl",
				setup:   []call{createDB(db)},
				request: createBucket(db, "b", server.BucketType("queue")), wantOpcode: transport.OpCreateBucket,
				wantField: map[string]any{"success": false},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with a name over 255 bytes", group: "ddl", storeType: "hash",
				setup:   []call{createDB(db)},
				request: createBucket(db, strings.Repeat("b", 256), server.BucketTypeHash), wantOpcode: transport.OpCreateBucket,
				wantField: map[string]any{"success": false}, wantMessage: []string{"255"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with store fields", group: "ddl", storeType: "hash",
				notes: "options.stores declares per-document stores: every document of User carries its own " +
					"Purchases queue, Friends table and so on.",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "User", Type: server.BucketTypeHash, Options: map[string]any{"stores": userStores()},
				}},
				wantOpcode: transport.OpCreateBucket, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with indexes by field name", group: "ddl", storeType: "hash",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeHash, Options: map[string]any{"indices": []any{"Name"}},
				}},
				wantOpcode: transport.OpCreateBucket, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with a unique index", group: "ddl", storeType: "hash",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeHash,
					Options: map[string]any{"placement": "whole", "indices": []any{map[string]any{"fields": []any{"Email"}, "unique": true, "name": "by_email"}}},
				}},
				wantOpcode: transport.OpCreateBucket, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with typed fields", group: "ddl", storeType: "hash",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeHash,
					Options: map[string]any{
						"fields": []any{
							map[string]any{"name": "Name", "type": "string", "required": true},
							map[string]any{"name": "Born", "type": "int"},
						},
						"field_types": "strict",
					},
				}},
				wantOpcode: transport.OpCreateBucket, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with a bad typed-field declaration", group: "ddl", storeType: "hash",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeHash,
					Options: map[string]any{"fields": []any{map[string]any{"name": "Name", "type": "colour"}}},
				}},
				wantOpcode: transport.OpCreateBucket, wantField: map[string]any{"success": false},
				wantMessage: []string{"colour"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with schemajson", group: "wire-names", storeType: "hash",
				notes: "The wire name is schemajson, one word. The server accepts it and does not read it; a v1 " +
					"driver does not expose it.",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeHash, SchemaJSON: `{"type":"object"}`,
				}},
				wantOpcode: transport.OpCreateBucket, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with a replication factor", group: "ddl", storeType: "hash",
				notes:      "replication_factor needs the bucket_rf capability; on one node it is recorded and has no effect.",
				setup:      []call{createDB(db)},
				request:    call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{Database: db, Name: "b", Type: server.BucketTypeHash, ReplicationFactor: 1}},
				wantOpcode: transport.OpCreateBucket, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "declare store fields on an existing bucket", group: "ddl", storeType: "hash",
				setup: bucketSetup(db, "Post", server.BucketTypeHash),
				request: call{op: transport.OpDeclareStoreFields, payload: server.DeclareStoreFieldsRequest{
					Database: db, Name: "Post", Stores: []map[string]any{{"name": "Tags", "engine": "hash"}},
				}},
				wantOpcode: transport.OpDeclareStoreFields, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "drop bucket", group: "ddl", storeType: "hash",
				setup:      bucketSetup(db, "b", server.BucketTypeHash),
				request:    call{op: transport.OpDeleteBucket, payload: server.DeleteBucketRequest{Database: db, Name: "b"}},
				wantOpcode: transport.OpDeleteBucket, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "drop bucket that does not exist", group: "ddl",
				setup:      []call{createDB(db)},
				request:    call{op: transport.OpDeleteBucket, payload: server.DeleteBucketRequest{Database: db, Name: "b"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "list buckets", group: "ddl",
				setup: []call{
					createDB(db), createBucket(db, "events", server.BucketTypeFIFO),
					{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{Database: db, Name: "users", Type: server.BucketTypeHash, Options: map[string]any{"stores": []any{map[string]any{"name": "Friends", "engine": "hash"}}}}},
					put(db, "users", "ada", ada()),
				},
				request:    call{op: transport.OpListBuckets, payload: server.ListBucketsRequest{Database: db}},
				wantOpcode: transport.OpListBuckets, wantField: map[string]any{"found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "list buckets of a database that does not exist", group: "ddl",
				notes:      "An ERROR NOT_FOUND, although the answer type has a found field.",
				request:    call{op: transport.OpListBuckets, payload: server.ListBucketsRequest{Database: db}},
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
				wantMessage: []string{fmt.Sprintf("database %q not found", db)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "list databases", group: "ddl", fresh: true,
				notes:      "On a fresh server: the answer lists every database the node holds.",
				setup:      bucketSetup(db, "b", server.BucketTypeHash),
				request:    call{op: transport.OpListDBs, payload: server.ListDatabasesRequest{}},
				wantOpcode: transport.OpListDBs,
			}
		},
	)
	for _, storeType := range storeTypes {
		cases = append(cases, func(db string) exchangeCase {
			return exchangeCase{
				name: "bucket stats: " + string(storeType), group: "ddl", storeType: string(storeType),
				setup:      bucketSetup(db, "b", storeType),
				request:    call{op: transport.OpBucketStats, payload: server.BucketStatsRequest{Database: db, Bucket: "b"}},
				wantOpcode: transport.OpBucketStats, wantField: map[string]any{"found": true},
			}
		})
	}
	cases = append(cases,
		func(db string) exchangeCase {
			return exchangeCase{
				name: "bucket stats with store fields, indexes and typed fields", group: "ddl", storeType: "hash",
				setup: []call{
					createDB(db),
					{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
						Database: db, Name: "User", Type: server.BucketTypeHash,
						Options: map[string]any{
							"stores":      []any{map[string]any{"name": "Friends", "engine": "hash"}},
							"indices":     []any{"Name"},
							"fields":      []any{map[string]any{"name": "Name", "type": "string", "required": true}},
							"field_types": "strict",
						},
					}},
					put(db, "User", "ada", ada()),
					field(transport.OpFieldPut, server.FieldRequest{Database: db, Bucket: "User", Key: "ada", Field: "Friends", Element: "grace", Data: grace()}),
				},
				request:    call{op: transport.OpBucketStats, payload: server.BucketStatsRequest{Database: db, Bucket: "User"}},
				wantOpcode: transport.OpBucketStats, wantField: map[string]any{"found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "bucket stats of a bucket that does not exist", group: "ddl",
				setup:      []call{createDB(db)},
				notes:      "An ERROR NOT_FOUND, although the answer type has a found field.",
				request:    call{op: transport.OpBucketStats, payload: server.BucketStatsRequest{Database: db, Bucket: "b"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
				wantMessage: []string{`bucket "b" not found in database`},
			}
		},
	)
	return cases
}

func writeCases() []func(db string) exchangeCase {
	date := func(year int) primitive.DateTime {
		return primitive.NewDateTimeFromTime(time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC))
	}
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "put on a hash bucket", group: "write", storeType: "hash",
				setup: bucketSetup(db, "users", server.BucketTypeHash), request: put(db, "users", "ada", ada()),
				wantOpcode: transport.OpPut, wantField: map[string]any{"id": "ada", "success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "put on a btree bucket", group: "write", storeType: "btree",
				setup: bucketSetup(db, "b", server.BucketTypeBTree), request: put(db, "b", "ada", ada()),
				wantOpcode: transport.OpPut, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "insert without a key: the server assigns it", group: "write", storeType: "hash",
				setup: bucketSetup(db, "users", server.BucketTypeHash), request: put(db, "users", "", ada()),
				wantOpcode: transport.OpPut, varyPaths: []string{"id"}, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "put with a write concern", group: "wire-names", storeType: "hash",
				notes: "The wire name is writeconcern, one word. On one node every level has the same effect.",
				setup: bucketSetup(db, "users", server.BucketTypeHash),
				request: call{op: transport.OpPut, payload: server.InsertRequest{
					Database: db, Bucket: "users", Key: "ada", Data: ada(), WriteConcern: "majority",
				}},
				wantOpcode: transport.OpPut, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "put on one node with a write concern the server does not know", group: "write", storeType: "hash",
				notes: "A standalone node refuses an unknown write concern with INVALID_ARGUMENT and applies nothing, " +
					"as a cluster does (vector \"put on a cluster with a write concern the server does not know\"); " +
					"the answer names the words it accepts.",
				setup: bucketSetup(db, "users", server.BucketTypeHash),
				request: call{op: transport.OpPut, payload: server.InsertRequest{
					Database: db, Bucket: "users", Key: "ada", Data: ada(), WriteConcern: "quorum",
				}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "btree insert", group: "write", storeType: "btree",
				setup:      bucketSetup(db, "b", server.BucketTypeBTree),
				request:    call{op: transport.OpBTreeInsert, payload: server.InsertRequest{Database: db, Bucket: "b", Key: "ada", Data: ada()}},
				wantOpcode: transport.OpBTreeInsert, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "insert many", group: "write", storeType: "hash",
				setup: bucketSetup(db, "users", server.BucketTypeHash),
				request: call{op: transport.OpBatchPut, payload: server.BatchInsertRequest{
					Database: db, Bucket: "users",
					Data: []map[string]any{
						{"_id_": "ada", "Name": "Ada", "Born": int32(1815)},
						{"_id_": "grace", "Name": "Grace", "Born": int32(1906)},
					},
				}},
				wantOpcode: transport.OpBatchPutOK, wantField: map[string]any{"success": true, "count": 2},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "put on a fifo bucket is accepted and pushes", group: "write", storeType: "fifo",
				notes: "PUT on a fifo, lifo or heap bucket pushes (heap at priority 0); the handbook matrix (operations.md section 1) " +
					"says yes. A driver does not refuse it client-side, and a queue element cannot be read back by key.",
				setup: bucketSetup(db, "q", server.BucketTypeFIFO), request: put(db, "q", "ada", ada()),
				wantOpcode: transport.OpPut,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "update with set, unset and inc", group: "write", storeType: "hash",
				setup: append(bucketSetup(db, "users", server.BucketTypeHash),
					put(db, "users", "ada", map[string]any{"Name": "Ada", "Born": int32(1815), "Visits": int32(1), "Draft": true})),
				request: call{op: transport.OpUpdate, payload: server.UpdateRequest{
					Database: db, Bucket: "users", Key: "ada",
					Set: map[string]any{"Address.City": "London"}, Unset: []string{"Draft"}, Inc: map[string]int64{"Visits": 2},
				}},
				wantOpcode: transport.OpUpdate, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "update with addtoset", group: "wire-names", storeType: "hash",
				notes: "The wire name is addtoset, one word.",
				setup: append(bucketSetup(db, "users", server.BucketTypeHash),
					put(db, "users", "ada", map[string]any{"Name": "Ada", "Tags": []any{"maths"}})),
				request: call{op: transport.OpUpdate, payload: server.UpdateRequest{
					Database: db, Bucket: "users", Key: "ada", AddToSet: map[string]any{"Tags": "poetry"},
				}},
				wantOpcode: transport.OpUpdate, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "update of a key that does not exist", group: "write", storeType: "hash",
				setup: bucketSetup(db, "users", server.BucketTypeHash),
				request: call{op: transport.OpUpdate, payload: server.UpdateRequest{
					Database: db, Bucket: "users", Key: "nobody", Set: map[string]any{"Name": "Nobody"},
				}},
				wantOpcode: transport.OpUpdate, wantField: map[string]any{"success": false},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "update with a bad operand", group: "errors", storeType: "hash",
				setup: append(bucketSetup(db, "users", server.BucketTypeHash), put(db, "users", "ada", ada())),
				request: call{op: transport.OpUpdate, payload: server.UpdateRequest{
					Database: db, Bucket: "users", Key: "ada", Inc: map[string]int64{"Name": 1},
				}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "delete", group: "write", storeType: "hash",
				setup:      append(bucketSetup(db, "users", server.BucketTypeHash), put(db, "users", "ada", ada())),
				request:    call{op: transport.OpDelete, payload: server.DeleteRequest{Database: db, Bucket: "users", Key: "ada"}},
				wantOpcode: transport.OpDelete, wantField: map[string]any{"success": true},
				absent: []string{"spared", "skipped"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "delete of a key that does not exist", group: "write", storeType: "hash",
				notes:      "Nothing was deleted: in-band success false, write_outcome not_applied. Not an error; repeating a delete is safe.",
				setup:      bucketSetup(db, "users", server.BucketTypeHash),
				request:    call{op: transport.OpDelete, payload: server.DeleteRequest{Database: db, Bucket: "users", Key: "nobody"}},
				wantOpcode: transport.OpDelete, wantField: map[string]any{"success": false, "write_outcome": "not_applied"},
				wantMessage: []string{"document not found"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "delete with expire_if: the document has expired", group: "write", storeType: "hash",
				notes: "expire_if makes the delete conditional on a date field at or before date_before (or " +
					"a number of Unix seconds at or before seconds_before).",
				setup: append(bucketSetup(db, "sessions", server.BucketTypeHash),
					put(db, "sessions", "s1", map[string]any{"LastSeen": date(2020)})),
				request: call{op: transport.OpDelete, payload: server.DeleteRequest{
					Database: db, Bucket: "sessions", Key: "s1",
					ExpireIf: &server.ExpireCondition{Field: "LastSeen", DateBefore: date(2026), SecondsBefore: 1767225600},
				}},
				wantOpcode: transport.OpDelete, wantField: map[string]any{"success": true},
				absent: []string{"spared", "skipped"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "delete with expire_if: spared, the document has not expired", group: "write", storeType: "hash",
				notes: "spared true means the condition kept the document.",
				setup: append(bucketSetup(db, "sessions", server.BucketTypeHash),
					put(db, "sessions", "s1", map[string]any{"LastSeen": date(2030)})),
				request: call{op: transport.OpDelete, payload: server.DeleteRequest{
					Database: db, Bucket: "sessions", Key: "s1",
					ExpireIf: &server.ExpireCondition{Field: "LastSeen", DateBefore: date(2026), SecondsBefore: 1767225600},
				}},
				wantOpcode: transport.OpDelete, wantField: map[string]any{"success": true, "spared": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "fifo push", group: "write", storeType: "fifo",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    push(transport.OpFIFOPush, db, "q", ada(), 0),
				wantOpcode: transport.OpFIFOPush, wantField: map[string]any{"success": true},
				varyPaths: []string{"id"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "fifo pop", group: "write", storeType: "fifo",
				setup:      append(bucketSetup(db, "q", server.BucketTypeFIFO), push(transport.OpFIFOPush, db, "q", ada(), 0), push(transport.OpFIFOPush, db, "q", grace(), 0)),
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "q"}},
				wantOpcode: transport.OpFIFOPopResponse, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "fifo pop of an empty queue", group: "write", storeType: "fifo",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "q"}},
				wantOpcode: transport.OpFIFOPopResponse, wantField: map[string]any{"found": false},
				wantMessage: []string{"queue is empty"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "fifo peek", group: "read", storeType: "fifo",
				setup:      append(bucketSetup(db, "q", server.BucketTypeFIFO), push(transport.OpFIFOPush, db, "q", ada(), 0)),
				request:    call{op: transport.OpFIFOPeek, payload: server.PeekRequest{Database: db, Bucket: "q"}},
				wantOpcode: transport.OpFIFOPeek, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "fifo peek of an empty queue", group: "read", storeType: "fifo",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    call{op: transport.OpFIFOPeek, payload: server.PeekRequest{Database: db, Bucket: "q"}},
				wantOpcode: transport.OpFIFOPeek, wantField: map[string]any{"found": false},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "push on a lifo bucket with the fifo family", group: "write", storeType: "lifo",
				notes:      "The push opcodes are aliases; a driver sends FIFO_PUSH for every queue type.",
				setup:      bucketSetup(db, "s", server.BucketTypeLIFO),
				request:    push(transport.OpFIFOPush, db, "s", ada(), 0),
				wantOpcode: transport.OpFIFOPush, varyPaths: []string{"id"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "pop on a lifo bucket with the fifo family", group: "write", storeType: "lifo",
				setup:      append(bucketSetup(db, "s", server.BucketTypeLIFO), push(transport.OpFIFOPush, db, "s", ada(), 0), push(transport.OpFIFOPush, db, "s", grace(), 0)),
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "s"}},
				wantOpcode: transport.OpFIFOPopResponse, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "lifo push", group: "write", storeType: "lifo",
				setup:      bucketSetup(db, "s", server.BucketTypeLIFO),
				request:    push(transport.OpLIFOPush, db, "s", ada(), 0),
				wantOpcode: transport.OpLIFOPush, varyPaths: []string{"id"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "lifo pop", group: "write", storeType: "lifo",
				setup:      append(bucketSetup(db, "s", server.BucketTypeLIFO), push(transport.OpLIFOPush, db, "s", ada(), 0), push(transport.OpLIFOPush, db, "s", grace(), 0)),
				request:    call{op: transport.OpLIFOPop, payload: server.PopRequest{Database: db, Bucket: "s"}},
				wantOpcode: transport.OpLIFOPopResponse, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "lifo peek", group: "read", storeType: "lifo",
				setup:      append(bucketSetup(db, "s", server.BucketTypeLIFO), push(transport.OpLIFOPush, db, "s", ada(), 0)),
				request:    call{op: transport.OpLIFOPeek, payload: server.PeekRequest{Database: db, Bucket: "s"}},
				wantOpcode: transport.OpLIFOPeek, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "push on a heap bucket with the fifo family", group: "write", storeType: "heap",
				notes:      "priority is required on a heap bucket.",
				setup:      bucketSetup(db, "h", server.BucketTypeHeap),
				request:    push(transport.OpFIFOPush, db, "h", ada(), 5),
				wantOpcode: transport.OpFIFOPush, varyPaths: []string{"id"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "heap push", group: "write", storeType: "heap",
				setup:      bucketSetup(db, "h", server.BucketTypeHeap),
				request:    push(transport.OpHeapPush, db, "h", ada(), 5),
				wantOpcode: transport.OpHeapPush, varyPaths: []string{"id"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "heap pop", group: "write", storeType: "heap",
				setup:      append(bucketSetup(db, "h", server.BucketTypeHeap), push(transport.OpHeapPush, db, "h", ada(), 1), push(transport.OpHeapPush, db, "h", grace(), 9)),
				request:    call{op: transport.OpHeapPop, payload: server.PopRequest{Database: db, Bucket: "h"}},
				wantOpcode: transport.OpHeapPopResponse, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "pop on a heap bucket with the fifo family", group: "write", storeType: "heap",
				setup:      append(bucketSetup(db, "h", server.BucketTypeHeap), push(transport.OpFIFOPush, db, "h", ada(), 1), push(transport.OpFIFOPush, db, "h", grace(), 9)),
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "h"}},
				wantOpcode: transport.OpFIFOPopResponse, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "peek on a heap bucket with the fifo family", group: "read", storeType: "heap",
				notes:      "There is no HEAP_PEEK opcode: FIFO_PEEK on a heap bucket peeks the heap.",
				setup:      append(bucketSetup(db, "h", server.BucketTypeHeap), push(transport.OpFIFOPush, db, "h", ada(), 1)),
				request:    call{op: transport.OpFIFOPeek, payload: server.PeekRequest{Database: db, Bucket: "h"}},
				wantOpcode: transport.OpFIFOPeek, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "heap pop of an empty heap", group: "write", storeType: "heap",
				setup:      bucketSetup(db, "h", server.BucketTypeHeap),
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "h"}},
				wantOpcode: transport.OpFIFOPopResponse, wantField: map[string]any{"found": false},
				wantMessage: []string{"heap is empty"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "blob write", group: "write", storeType: "blob",
				setup: bucketSetup(db, "files", server.BucketTypeBlob),
				request: call{op: transport.OpBlobWrite, payload: server.BlobWriteRequest{
					Database: db, Bucket: "files", Key: "hello.txt", Data: []byte("hello, hoardDB"),
					Metadata: map[string]any{"ContentType": "text/plain"},
				}},
				wantOpcode: transport.OpBlobWrite, wantField: map[string]any{"success": true, "key": "hello.txt", "size": 14},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "delete on a blob bucket", group: "write", storeType: "blob",
				setup: append(bucketSetup(db, "files", server.BucketTypeBlob), call{op: transport.OpBlobWrite, payload: server.BlobWriteRequest{
					Database: db, Bucket: "files", Key: "hello.txt", Data: []byte("hello, hoardDB"),
				}}),
				request:    call{op: transport.OpDelete, payload: server.DeleteRequest{Database: db, Bucket: "files", Key: "hello.txt"}},
				wantOpcode: transport.OpDelete, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "delete on a blob bucket of a key that does not exist", group: "write", storeType: "blob",
				setup:      bucketSetup(db, "files", server.BucketTypeBlob),
				request:    call{op: transport.OpDelete, payload: server.DeleteRequest{Database: db, Bucket: "files", Key: "nothing.txt"}},
				wantOpcode: transport.OpDelete, wantField: map[string]any{"success": false, "write_outcome": "not_applied"},
				wantMessage: []string{"blob not found"},
			}
		},
	}
}

func readCases() []func(db string) exchangeCase {
	withAda := func(db, bucket string, storeType server.BucketType) []call {
		return append(bucketSetup(db, bucket, storeType), put(db, bucket, "ada", ada()))
	}
	getFast := func(db, bucket, key string) call {
		payload := []byte{byte(len(db))}
		payload = append(payload, db...)
		payload = append(payload, byte(len(bucket)))
		payload = append(payload, bucket...)
		payload = append(payload, byte(len(key)), byte(len(key)>>8))
		payload = append(payload, key...)
		return call{op: transport.OpGetFast, raw: payload}
	}
	btreeRange := func(op transport.Opcode, db string, low, high string, limit int) call {
		return call{op: op, payload: server.BTreeRangeRequest{Database: db, Bucket: "b", Low: low, High: high, Limit: limit}}
	}
	btreeSetup := func(db string) []call {
		return append(bucketSetup(db, "b", server.BucketTypeBTree),
			put(db, "b", "ada", ada()), put(db, "b", "grace", grace()),
			put(db, "b", "linus", map[string]any{"Name": "Linus", "Born": int32(1969)}))
	}
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get on a hash bucket", group: "read", storeType: "hash",
				setup: withAda(db, "users", server.BucketTypeHash), request: find(transport.OpGet, db, "users", "ada"),
				wantOpcode: transport.OpGetResponse, wantField: map[string]any{"found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get of a key that does not exist", group: "read", storeType: "hash",
				notes: "A miss is found false on a successful frame, never an error and never an empty document.",
				setup: bucketSetup(db, "users", server.BucketTypeHash), request: find(transport.OpGet, db, "users", "nobody"),
				wantOpcode: transport.OpGetResponse, wantField: map[string]any{"found": false},
				wantMessage: []string{"document not found"}, absent: []string{"data"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get on a btree bucket", group: "read", storeType: "btree",
				setup: withAda(db, "b", server.BucketTypeBTree), request: find(transport.OpGet, db, "b", "ada"),
				wantOpcode: transport.OpGetResponse, wantField: map[string]any{"found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "exists: present", group: "read", storeType: "hash",
				setup: withAda(db, "users", server.BucketTypeHash), request: find(transport.OpExists, db, "users", "ada"),
				wantOpcode: transport.OpExistsResponse, wantField: map[string]any{"exists": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "exists: absent", group: "read", storeType: "hash",
				setup: bucketSetup(db, "users", server.BucketTypeHash), request: find(transport.OpExists, db, "users", "nobody"),
				wantOpcode: transport.OpExistsResponse, wantField: map[string]any{"exists": false},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "btree lookup answers BTREE_RESPONSE", group: "read", storeType: "btree",
				notes: "BTREE_LOOKUP answers the BTREE_RESPONSE opcode with a find " +
					"response, while GET answers GET_RESPONSE.",
				setup: withAda(db, "b", server.BucketTypeBTree), request: find(transport.OpBTreeLookup, db, "b", "ada"),
				wantOpcode: transport.OpBTreeResponse, wantField: map[string]any{"found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "btree lookup of a key that does not exist", group: "read", storeType: "btree",
				setup: bucketSetup(db, "b", server.BucketTypeBTree), request: find(transport.OpBTreeLookup, db, "b", "nobody"),
				wantOpcode: transport.OpBTreeResponse, wantField: map[string]any{"found": false},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "btree range", group: "read", storeType: "btree",
				notes: "Bounds are strings; an empty side is open.",
				setup: btreeSetup(db), request: btreeRange(transport.OpBTreeRange, db, "b", "m", 10),
				wantOpcode: transport.OpBTreeResponse,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "btree first", group: "read", storeType: "btree",
				setup: btreeSetup(db), request: btreeRange(transport.OpBTreeFirst, db, "", "", 0),
				wantOpcode: transport.OpBTreeFirst,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "btree last", group: "read", storeType: "btree",
				setup: btreeSetup(db), request: btreeRange(transport.OpBTreeLast, db, "", "", 0),
				wantOpcode: transport.OpBTreeLast,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "btree first of an empty bucket", group: "read", storeType: "btree",
				setup: bucketSetup(db, "b", server.BucketTypeBTree), request: btreeRange(transport.OpBTreeFirst, db, "", "", 0),
				wantOpcode: transport.OpBTreeFirst,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "blob read", group: "read", storeType: "blob",
				notes: "blobid is one word here; the field-store verbs say blob_id.",
				setup: append(bucketSetup(db, "files", server.BucketTypeBlob), call{op: transport.OpBlobWrite, payload: server.BlobWriteRequest{
					Database: db, Bucket: "files", Key: "hello.txt", Data: []byte("hello, hoardDB"),
					Metadata: map[string]any{"ContentType": "text/plain"},
				}}),
				request:    call{op: transport.OpBlobRead, payload: server.BlobReadRequest{Database: db, Bucket: "files", Key: "hello.txt"}},
				wantOpcode: transport.OpBlobReadResponse, wantField: map[string]any{"found": true, "size": 14},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "blob read of a key that does not exist", group: "read", storeType: "blob",
				setup:      bucketSetup(db, "files", server.BucketTypeBlob),
				request:    call{op: transport.OpBlobRead, payload: server.BlobReadRequest{Database: db, Bucket: "files", Key: "nothing.txt"}},
				wantOpcode: transport.OpBlobReadResponse, wantField: map[string]any{"found": false},
				wantMessage: []string{"not found in"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "consistent get on one node", group: "read", storeType: "hash",
				notes:      "CONSISTENT_GET answers its own opcode. version is the read-repair version.",
				setup:      withAda(db, "users", server.BucketTypeHash),
				request:    call{op: transport.OpConsistentGet, payload: server.ConsistentGetRequest{Database: db, Bucket: "users", Key: "ada"}},
				wantOpcode: transport.OpConsistentGet, wantField: map[string]any{"found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get_fast: found", group: "read", storeType: "hash",
				notes: "GET_FAST is not BSON on the request: 1-byte database length, database, 1-byte bucket length, " +
					"bucket, 2-byte little-endian key length, key. The answer is a status byte (0x00 found) and the " +
					"stored document; response.payload_hex is that whole answer. Not in the v1 public API.",
				setup: withAda(db, "users", server.BucketTypeHash), request: getFast(db, "users", "ada"),
				wantOpcode: transport.OpGetFastResponse,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get_fast: not found", group: "read", storeType: "hash",
				notes: "Status byte 0x01 and nothing after it: a miss is not an error.",
				setup: bucketSetup(db, "users", server.BucketTypeHash), request: getFast(db, "users", "nobody"),
				wantOpcode: transport.OpGetFastResponse,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get_fast of a bucket that does not exist", group: "errors", storeType: "hash",
				notes: "Every GET_FAST failure is an ERROR frame and the connection stays open.",
				setup: []call{createDB(db)}, request: getFast(db, "users", "ada"),
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get_fast with a malformed payload", group: "errors",
				notes:      "INTERNAL (the authorization step cannot read the payload), and the connection stays open.",
				request:    call{op: transport.OpGetFast, raw: []byte{0x09, 'v'}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInternal,
				wantMessage: []string{"GET_FAST's payload is not BSON"},
			}
		},
	}
}

func queryCases() []func(db string) exchangeCase {
	people := func(db string, storeType server.BucketType) []call {
		setup := bucketSetup(db, "people", storeType)
		for _, person := range []map[string]any{
			{"Name": "Ada", "Born": int32(1815), "Field": "maths"},
			{"Name": "Grace", "Born": int32(1906), "Field": "computing"},
			{"Name": "Linus", "Born": int32(1969), "Field": "computing"},
		} {
			if storeType == server.BucketTypeHash || storeType == server.BucketTypeBTree {
				setup = append(setup, put(db, "people", strings.ToLower(person["Name"].(string)), person))
			} else {
				setup = append(setup, push(transport.OpFIFOPush, db, "people", person, int64(person["Born"].(int32))))
			}
		}
		return setup
	}
	cases := []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "find with filter, sort, projection, skip and limit", group: "query", storeType: "hash",
				notes: "find is SCAN: one page and the exact match count (total); the next page is a new SCAN with a larger skip.",
				setup: people(db, server.BucketTypeHash),
				request: call{op: transport.OpScan, payload: server.ScanRequest{
					Database: db, Bucket: "people", Filter: map[string]any{"Field": "computing"},
					Sort: map[string]int{"Born": -1}, Projection: map[string]int{"Name": 1}, Skip: 0, Limit: 1,
				}},
				wantOpcode: transport.OpScan, wantField: map[string]any{"found": true, "total": 2, "has_more": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "find of a bucket that does not exist", group: "query", storeType: "hash",
				notes: "SCAN of a missing bucket is in-band found false, while GET " +
					"of one is an ERROR NOT_FOUND. A driver presents both as the same not found.",
				setup:      []call{createDB(db)},
				request:    call{op: transport.OpScan, payload: server.ScanRequest{Database: db, Bucket: "people"}},
				wantOpcode: transport.OpScan, wantField: map[string]any{"found": false},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "count with a filter", group: "query", storeType: "hash",
				setup:      people(db, server.BucketTypeHash),
				request:    call{op: transport.OpCount, payload: server.CountRequest{Database: db, Bucket: "people", Filter: map[string]any{"Field": "computing"}}},
				wantOpcode: transport.OpCount, wantField: map[string]any{"count": 2},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "distinct", group: "query", storeType: "hash",
				setup:      people(db, server.BucketTypeHash),
				request:    call{op: transport.OpDistinct, payload: server.DistinctRequest{Database: db, Bucket: "people", Field: "Field"}},
				wantOpcode: transport.OpDistinct,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "search by a secondary index", group: "query", storeType: "hash",
				setup: append([]call{createDB(db), {op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "people", Type: server.BucketTypeHash, Options: map[string]any{"indices": []any{"Field"}},
				}}}, people(db, server.BucketTypeHash)[2:]...),
				request:    call{op: transport.OpSearch, payload: server.SearchRequest{Database: db, Bucket: "people", Filter: map[string]any{"Field": "computing"}, Limit: 10}},
				wantOpcode: transport.OpSearch, wantField: map[string]any{"total": 2},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "search without an index is a full scan", group: "query", storeType: "hash",
				setup:      people(db, server.BucketTypeHash),
				request:    call{op: transport.OpSearch, payload: server.SearchRequest{Database: db, Bucket: "people", Filter: map[string]any{"Field": "computing"}, Limit: 10}},
				wantOpcode: transport.OpSearch,
			}
		},
	}
	for _, storeType := range []server.BucketType{server.BucketTypeBTree, server.BucketTypeFIFO, server.BucketTypeLIFO, server.BucketTypeHeap} {
		cases = append(cases, func(db string) exchangeCase {
			c := exchangeCase{
				name: "find on a " + string(storeType) + " bucket", group: "query", storeType: string(storeType),
				notes:      "In the store's natural order.",
				setup:      people(db, storeType),
				request:    call{op: transport.OpScan, payload: server.ScanRequest{Database: db, Bucket: "people"}},
				wantOpcode: transport.OpScan, wantField: map[string]any{"found": true, "total": 3},
			}
			if storeType != server.BucketTypeBTree {
				c.varyPaths = []string{"documents[]._id_"}
			}
			return c
		})
		cases = append(cases, func(db string) exchangeCase {
			return exchangeCase{
				name: "count on a " + string(storeType) + " bucket", group: "query", storeType: string(storeType),
				setup:      people(db, storeType),
				request:    call{op: transport.OpCount, payload: server.CountRequest{Database: db, Bucket: "people"}},
				wantOpcode: transport.OpCount, wantField: map[string]any{"count": 3},
			}
		})
	}
	return cases
}

func fieldStoreCases() []func(db string) exchangeCase {
	at := func(db, fieldName string) server.FieldRequest {
		return server.FieldRequest{Database: db, Bucket: "User", Key: "ada", Field: fieldName}
	}
	withElement := func(request server.FieldRequest, element string, doc map[string]any) server.FieldRequest {
		request.Element = element
		request.Data = doc
		return request
	}
	purchase := func(item string) map[string]any { return map[string]any{"Item": item} }
	pushed := func(db string) []call {
		return append(userSetup(db),
			field(transport.OpFieldPush, withElement(at(db, "Purchases"), "", purchase("tea"))),
			field(transport.OpFieldPush, withElement(at(db, "Purchases"), "", purchase("scones"))))
	}
	friend := func(db string) []call {
		return append(userSetup(db), field(transport.OpFieldPut, withElement(at(db, "Friends"), "grace", grace())))
	}
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field push on a fifo store field", group: "store-fields", storeType: "fifo",
				notes: "key is the parent document's key; field names the store field of that document.",
				setup: userSetup(db), request: field(transport.OpFieldPush, withElement(at(db, "Purchases"), "", purchase("tea"))),
				wantOpcode: transport.OpFieldPush, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field pop on a fifo store field", group: "store-fields", storeType: "fifo",
				setup: pushed(db), request: field(transport.OpFieldPop, at(db, "Purchases")),
				wantOpcode: transport.OpFieldPop, wantField: map[string]any{"success": true, "found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field peek on a fifo store field", group: "store-fields", storeType: "fifo",
				setup: pushed(db), request: field(transport.OpFieldPeek, at(db, "Purchases")),
				wantOpcode: transport.OpFieldPeek, wantField: map[string]any{"success": true, "found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field pop on an empty fifo store field", group: "store-fields", storeType: "fifo",
				setup: userSetup(db), request: field(transport.OpFieldPop, at(db, "Purchases")),
				wantOpcode: transport.OpFieldPop,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field push on a lifo store field", group: "store-fields", storeType: "lifo",
				setup: userSetup(db), request: field(transport.OpFieldPush, withElement(at(db, "Photos"), "", map[string]any{"Title": "Lovelace"})),
				wantOpcode: transport.OpFieldPush, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			request := withElement(at(db, "Books"), "", map[string]any{"Title": "Sketch of the Analytical Engine"})
			request.Priority = 7
			return exchangeCase{
				name: "field push on a heap store field", group: "store-fields", storeType: "heap",
				setup: userSetup(db), request: field(transport.OpFieldPush, request),
				wantOpcode: transport.OpFieldPush, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field put on a hash store field", group: "store-fields", storeType: "hash",
				notes: "element is the element's key inside ada's Friends table.",
				setup: userSetup(db), request: field(transport.OpFieldPut, withElement(at(db, "Friends"), "grace", grace())),
				wantOpcode: transport.OpFieldPut, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field get on a hash store field", group: "store-fields", storeType: "hash",
				setup: friend(db), request: field(transport.OpFieldGet, withElement(at(db, "Friends"), "grace", nil)),
				wantOpcode: transport.OpFieldGet, wantField: map[string]any{"success": true, "found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field get of an element that does not exist", group: "store-fields", storeType: "hash",
				setup: userSetup(db), request: field(transport.OpFieldGet, withElement(at(db, "Friends"), "nobody", nil)),
				wantOpcode: transport.OpFieldGet, wantField: map[string]any{"found": false},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field delete on a hash store field", group: "store-fields", storeType: "hash",
				setup: friend(db), request: field(transport.OpFieldDelete, withElement(at(db, "Friends"), "grace", nil)),
				wantOpcode: transport.OpFieldDelete, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field put on a btree store field", group: "store-fields", storeType: "btree",
				setup: userSetup(db), request: field(transport.OpFieldPut, withElement(at(db, "Recipes"), "seed-cake", map[string]any{"Serves": int32(8)})),
				wantOpcode: transport.OpFieldPut, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			request := withElement(at(db, "Data"), "notes.txt", map[string]any{"ContentType": "text/plain"})
			request.Content = []byte("Notes by the translator")
			return exchangeCase{
				name: "field put on a blob store field", group: "store-fields", storeType: "blob",
				notes: "content is the element's bytes and data its metadata; blob_id has an underscore here.",
				setup: userSetup(db), request: field(transport.OpFieldPut, request),
				wantOpcode: transport.OpFieldPut, wantField: map[string]any{"success": true},
			}
		},
		func(db string) exchangeCase {
			request := withElement(at(db, "Data"), "notes.txt", map[string]any{"ContentType": "text/plain"})
			request.Content = []byte("Notes by the translator")
			return exchangeCase{
				name: "field get on a blob store field", group: "store-fields", storeType: "blob",
				setup:      append(userSetup(db), field(transport.OpFieldPut, request)),
				request:    field(transport.OpFieldGet, withElement(at(db, "Data"), "notes.txt", nil)),
				wantOpcode: transport.OpFieldGet, wantField: map[string]any{"success": true, "found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field len", group: "store-fields", storeType: "fifo",
				setup: pushed(db), request: field(transport.OpFieldLen, at(db, "Purchases")),
				wantOpcode: transport.OpFieldLen, wantField: map[string]any{"len": 2},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field list", group: "store-fields", storeType: "fifo",
				setup:      pushed(db),
				request:    call{op: transport.OpFieldList, payload: server.FieldListRequest{FieldRequest: at(db, "Purchases"), Limit: 1}},
				wantOpcode: transport.OpFieldList, wantField: map[string]any{"success": true, "has_more": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field verb on an undeclared store field", group: "store-fields", storeType: "hash",
				setup: userSetup(db), request: field(transport.OpFieldLen, at(db, "Enemies")),
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
			}
		},
		func(db string) exchangeCase {
			request := at(db, "Purchases")
			request.Key = "nobody"
			request.Data = purchase("tea")
			return exchangeCase{
				name: "field verb on a parent document that does not exist", group: "store-fields", storeType: "fifo",
				setup: userSetup(db), request: field(transport.OpFieldPush, request),
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound, wantField: notApplied(),
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "field verb the store type does not answer", group: "store-fields", storeType: "fifo",
				setup: userSetup(db), request: field(transport.OpFieldPut, withElement(at(db, "Purchases"), "x", purchase("tea"))),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
			}
		},
	}
}

func errorCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get of a bucket that does not exist", group: "errors", storeType: "hash",
				setup: []call{createDB(db)}, request: find(transport.OpGet, db, "users", "ada"),
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get of a database that does not exist", group: "errors", storeType: "hash",
				request:    find(transport.OpGet, db, "users", "ada"),
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get on a fifo bucket", group: "errors", storeType: "fifo",
				notes: "A verb the bucket's store type does not answer is an ERROR frame INVALID_ARGUMENT, the same for every verb and " +
					"type (BUG-0469; GET was in band, found false). The message ends with the verbs the type takes.",
				setup: bucketSetup(db, "q", server.BucketTypeFIFO), request: find(transport.OpGet, db, "q", "ada"),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument,
				wantMessage: []string{"get is not supported for bucket type fifo", "A fifo bucket takes:"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "pop on a hash bucket", group: "errors", storeType: "hash",
				notes: "POP is a write, so its refusal says write_outcome not_applied; otherwise the same ERROR frame as every wrong-type verb " +
					"(BUG-0469; it was in band, found false).",
				setup:      bucketSetup(db, "users", server.BucketTypeHash),
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "users"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"pop is not defined for hash buckets", "A hash bucket takes:"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "a node-to-node opcode on the driver port", group: "errors",
				notes:      "Refused by name with the request ID echoed, then the connection is closed.",
				request:    call{op: transport.OpGossip},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument,
				wantMessage: []string{"node-to-node"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "an opcode the server does not declare", group: "errors",
				notes:      "0x05 is not an opcode; it is answered INTERNAL and the connection stays open.",
				request:    call{op: transport.Opcode(0x05)},
				wantOpcode: transport.OpError, wantCode: transport.ErrInternal,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "a request payload that is not BSON", group: "errors",
				request:    call{op: transport.OpGet, raw: []byte{0x01, 0x02, 0x03}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "permission denied", group: "errors", storeType: "hash", noReplay: true,
				notes: "Sent on a connection authenticated as reader, a user holding only {role: reader, database: <this " +
					"vector's database>}. The user, the database and the hash bucket users are created by root before " +
					"the request and are not part of the setup. A refusal is a role problem, never a missing feature.",
				prepare:  createReader,
				username: "reader", password: readerPassword,
				request:    put(db, "users", "ada", ada()),
				wantOpcode: transport.OpError, wantCode: transport.ErrPermissionDenied, wantField: notApplied(),
				wantMessage: []string{`permission denied: user "reader"`},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "duplicate key on a unique index", group: "errors", storeType: "hash",
				setup: []call{createDB(db), {op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "users", Type: server.BucketTypeHash,
					Options: map[string]any{"placement": "whole", "indices": []any{map[string]any{"fields": []any{"Email"}, "unique": true}}},
				}}, put(db, "users", "ada", map[string]any{"Email": "ada@example.org"})},
				request:    put(db, "users", "countess", map[string]any{"Email": "ada@example.org"}),
				wantOpcode: transport.OpError, wantCode: transport.ErrDuplicateKey, wantField: notApplied(),
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "a mistyped document on a typed bucket", group: "errors", storeType: "hash",
				setup: []call{createDB(db), {op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "users", Type: server.BucketTypeHash,
					Options: map[string]any{"fields": []any{map[string]any{"name": "Born", "type": "int"}}, "field_types": "strict"},
				}}},
				request:    put(db, "users", "ada", map[string]any{"Born": "eighteen fifteen"}),
				wantOpcode: transport.OpError, wantCode: transport.ErrFieldType, wantField: notApplied(),
				wantMessage: []string{"Born"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "a top-level field name no path can reach", group: "errors", storeType: "hash",
				setup:      bucketSetup(db, "users", server.BucketTypeHash),
				request:    put(db, "users", "ada", map[string]any{"a.b": int32(1)}),
				wantOpcode: transport.OpError, wantCode: transport.ErrFieldType, wantField: notApplied(),
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "bson edge values round trip", group: "bson", storeType: "hash",
				notes: "datetime to the millisecond, an object id, binary subtypes 0 (generic) and 4 (uuid), int32 " +
					"against int64, a double, a decimal128, a nested document and an array, stored and read back.",
				setup:      append(bucketSetup(db, "edges", server.BucketTypeHash), put(db, "edges", "e1", edgeDocument())),
				request:    find(transport.OpGet, db, "edges", "e1"),
				wantOpcode: transport.OpGetResponse, wantField: map[string]any{"found": true},
			}
		},
	}
}

// readerPassword is the read-only user's password in the permission vector.
const readerPassword = "vectors-reader-password"

// createReader creates the vector's database and bucket as root, and the user
// "reader" with only the reader role on that database, through the server's
// own user API.
func createReader(ctx context.Context, g *generation, db string) error {
	return createReaderNamed(ctx, g, db, "reader")
}

// infoReader is the user of the BUCKET_INFO permission vector. It is not
// "reader": that user already exists by then, with its grant on another
// vector's database.
const infoReader = "info-reader"

func createInfoReader(ctx context.Context, g *generation, db string) error {
	return createReaderNamed(ctx, g, db, infoReader)
}

// createReaderNamed is createReader for a user of any name.
func createReaderNamed(ctx context.Context, g *generation, db, user string) error {
	if err := g.requestsOn(ctx, "single", bucketSetup(db, "users", server.BucketTypeHash)); err != nil {
		return err
	}
	service, err := g.service("single")
	if err != nil {
		return err
	}
	material, err := server.NewCredential(readerPassword, server.Argon2Params{})
	if err != nil {
		return fmt.Errorf("deriving the reader's credential: %w", err)
	}
	if err := service.Auth().CreateUser(user, material, []server.Grant{{Role: "reader", DB: db}}, true); err != nil {
		return fmt.Errorf("creating the reader %q: %w", user, err)
	}
	return nil
}

// edgeDocument holds the BSON values that differ most between codecs.
func edgeDocument() map[string]any {
	return map[string]any{
		"At":      primitive.NewDateTimeFromTime(time.Date(2026, 10, 5, 12, 34, 56, 789_000_000, time.UTC)),
		"Oid":     primitive.ObjectID{0x65, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09},
		"Generic": primitive.Binary{Subtype: 0x00, Data: []byte{0x00, 0x01, 0xfe, 0xff}},
		"UUID":    primitive.Binary{Subtype: 0x04, Data: []byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}},
		"I32":     int32(2147483647),
		"I64":     int64(9007199254740993),
		"Double":  1.5,
		// 12345.6789: coefficient 123456789, exponent -4, whose biased
		// exponent (6176 - 4) sits above the 49-bit coefficient's high bits.
		"Dec":    primitive.NewDecimal128(uint64(6176-4)<<49, 123456789),
		"Nested": map[string]any{"Deep": map[string]any{"Value": true}},
		"List":   []any{int32(1), "two", 3.0, nil},
	}
}

func limitCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "a query over the result ceiling", group: "limits", storeType: "hash", server: "results", noReplay: true,
				notes: "Captured with HOARDDB_QUERY_MAX_RESULT_BYTES=1048576 on a bucket \"big\" holding two documents of " +
					"700 KB each, written before the request and not recorded as setup (their size). The connection and " +
					"its other requests are unaffected.",
				prepare:    bigDocuments,
				request:    call{op: transport.OpScan, payload: server.ScanRequest{Database: db, Bucket: "big"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrResultTooLarge,
				wantMessage: []string{"1 MiB"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "a document over the document limit", group: "limits", storeType: "hash", server: "limits",
				setup:      bucketSetup(db, "docs", server.BucketTypeHash),
				request:    put(db, "docs", "big", map[string]any{"Text": strings.Repeat("x", 5000)}),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{`Document "big" is`},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "a blob over the blob limit", group: "limits", storeType: "blob", server: "limits",
				setup: bucketSetup(db, "files", server.BucketTypeBlob),
				request: call{op: transport.OpBlobWrite, payload: server.BlobWriteRequest{
					Database: db, Bucket: "files", Key: "big.bin", Data: []byte(strings.Repeat("b", 2048)),
				}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{`Blob "big.bin" is`, "this server's limit is"},
			}
		},
	}
}

// bigDocuments fills a bucket with two documents of 700 KB each through the
// server's driver port, so one SCAN answer is over a 1 MiB result ceiling.
func bigDocuments(ctx context.Context, g *generation, db string) error {
	return g.requestsOn(ctx, "results", append(bucketSetup(db, "big", server.BucketTypeHash),
		put(db, "big", "one", map[string]any{"Text": strings.Repeat("a", 700_000)}),
		put(db, "big", "two", map[string]any{"Text": strings.Repeat("b", 700_000)})))
}

// popLimitCases are the vectors of pop({limit: N}): the limit on the three
// bucket pops, its cap, and the result ceiling a pop is refused at.
func popLimitCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "fifo pop with a limit", group: "write", storeType: "fifo",
				notes:      "limit absent or 0 means 1. The two entries come back in pop order.",
				setup:      append(bucketSetup(db, "q", server.BucketTypeFIFO), push(transport.OpFIFOPush, db, "q", ada(), 0), push(transport.OpFIFOPush, db, "q", grace(), 0), push(transport.OpFIFOPush, db, "q", linus(), 0)),
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "q", Limit: 2}},
				wantOpcode: transport.OpFIFOPopResponse, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "fifo pop with a limit past the end", group: "write", storeType: "fifo",
				notes:      "Fewer than the limit is not an error: the two entries there are come back, found.",
				setup:      append(bucketSetup(db, "q", server.BucketTypeFIFO), push(transport.OpFIFOPush, db, "q", ada(), 0), push(transport.OpFIFOPush, db, "q", grace(), 0)),
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "q", Limit: 5}},
				wantOpcode: transport.OpFIFOPopResponse, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "lifo pop with a limit", group: "write", storeType: "lifo",
				setup:      append(bucketSetup(db, "s", server.BucketTypeLIFO), push(transport.OpLIFOPush, db, "s", ada(), 0), push(transport.OpLIFOPush, db, "s", grace(), 0), push(transport.OpLIFOPush, db, "s", linus(), 0)),
				request:    call{op: transport.OpLIFOPop, payload: server.PopRequest{Database: db, Bucket: "s", Limit: 2}},
				wantOpcode: transport.OpLIFOPopResponse, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "heap pop with a limit", group: "write", storeType: "heap",
				setup:      append(bucketSetup(db, "h", server.BucketTypeHeap), push(transport.OpHeapPush, db, "h", ada(), 1), push(transport.OpHeapPush, db, "h", grace(), 9), push(transport.OpHeapPush, db, "h", linus(), 5)),
				request:    call{op: transport.OpHeapPop, payload: server.PopRequest{Database: db, Bucket: "h", Limit: 2}},
				wantOpcode: transport.OpHeapPopResponse, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "pop limit above the maximum", group: "errors", storeType: "fifo",
				notes:      "A pop takes at most 100 entries; nothing is removed.",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "q", Limit: 101}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"101 is above the pop maximum of 100"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "a pop over the result ceiling", group: "limits", storeType: "fifo", server: "results", noReplay: true,
				notes: "Captured with HOARDDB_QUERY_MAX_RESULT_BYTES=1048576 on a bucket \"big\" holding two entries of " +
					"700 KB each, written before the request and not recorded as setup (their size). Nothing is removed: " +
					"the whole pop is refused, and both entries are still in the queue.",
				prepare:    bigQueueEntries,
				request:    call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: db, Bucket: "big", Limit: 2}},
				wantOpcode: transport.OpError, wantCode: transport.ErrResultTooLarge, wantField: notApplied(),
				wantMessage: []string{"so nothing was removed"},
			}
		},
	}
}

// fieldPopLimitCases is the store-field form of pop({limit: N}). It is appended
// last with the bucket pops, not placed among the other store-field cases, so
// that no earlier exchange changes its number, database name or request ID.
func fieldPopLimitCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			purchases := server.FieldRequest{Database: db, Bucket: "User", Key: "ada", Field: "Purchases"}
			push := func(item string) call {
				request := purchases
				request.Data = map[string]any{"Item": item}
				return field(transport.OpFieldPush, request)
			}
			return exchangeCase{
				name: "field pop with a limit on a fifo store field", group: "store-fields", storeType: "fifo",
				notes: "limit absent or 0 means 1. Each item is the element with its position, present also at 0. " +
					"The field-store verbs are the newest wire surface: frozen, and the likeliest to see the first revisions (driver-spec 4.3).",
				setup:      append(userSetup(db), push("tea"), push("scones"), push("jam")),
				request:    call{op: transport.OpFieldPop, payload: server.FieldPopRequest{FieldRequest: purchases, Limit: 2}},
				wantOpcode: transport.OpFieldPop, wantField: map[string]any{"success": true, "found": true},
				varyPaths: []string{"items[].element._id_"},
			}
		},
	}
}

// bigQueueEntries fills a fifo bucket with two entries of 700 KB each through
// the server's driver port, so one pop of both is over a 1 MiB result ceiling.
func bigQueueEntries(ctx context.Context, g *generation, db string) error {
	return g.requestsOn(ctx, "results", append(bucketSetup(db, "big", server.BucketTypeFIFO),
		push(transport.OpFIFOPush, db, "big", map[string]any{"Text": strings.Repeat("a", 700_000)}, 0),
		push(transport.OpFIFOPush, db, "big", map[string]any{"Text": strings.Repeat("b", 700_000)}, 0)))
}

// updateOperatorCases pin the five update operators no other vector covers
// ($push, $mul, $min, $max, $rename), one case that sends all nine, and three
// refusals. Every $mul factor is a fraction on purpose: a driver that re-encodes
// the request from its own numbers would write a whole double such as 2.0 as an
// int32 (the driver-node-build record step V).
func updateOperatorCases() []func(db string) exchangeCase {
	updateCase := func(name, group string, setupDocument map[string]any, fill func(*server.UpdateRequest)) func(db string) exchangeCase {
		return func(db string) exchangeCase {
			update := server.UpdateRequest{Database: db, Bucket: "users", Key: "ada"}
			fill(&update)
			return exchangeCase{
				name: name, group: group, storeType: "hash",
				setup:   append(bucketSetup(db, "users", server.BucketTypeHash), put(db, "users", "ada", setupDocument)),
				request: call{op: transport.OpUpdate, payload: update},
			}
		}
	}
	succeeds := func(build func(db string) exchangeCase) func(db string) exchangeCase {
		return func(db string) exchangeCase {
			c := build(db)
			c.wantOpcode, c.wantField = transport.OpUpdate, map[string]any{"success": true}
			return c
		}
	}
	refused := func(build func(db string) exchangeCase) func(db string) exchangeCase {
		return func(db string) exchangeCase {
			c := build(db)
			c.wantOpcode, c.wantCode, c.wantField = transport.OpError, transport.ErrInvalidArgument, notApplied()
			return c
		}
	}
	return []func(db string) exchangeCase{
		succeeds(updateCase("update with push", "write",
			map[string]any{"Name": "Ada", "Tags": []any{"maths"}},
			func(u *server.UpdateRequest) { u.Push = map[string]any{"Tags": "poetry", "Langs": "en"} })),
		succeeds(updateCase("update with mul", "write",
			map[string]any{"Name": "Ada", "Score": int32(10)},
			func(u *server.UpdateRequest) { u.Mul = map[string]float64{"Score": 1.5, "Missing": 0.5} })),
		succeeds(updateCase("update with min and max", "write",
			map[string]any{"Name": "Ada", "Age": int32(36), "Best": int32(10)},
			func(u *server.UpdateRequest) {
				u.Min = map[string]any{"Age": int32(30), "Floor": int32(5)}
				u.Max = map[string]any{"Best": int32(99)}
			})),
		succeeds(updateCase("update with rename", "write",
			map[string]any{"Name": "Ada", "Nick": "gh", "Address": map[string]any{"Zip": "1001"}},
			func(u *server.UpdateRequest) {
				u.Rename = map[string]string{"Nick": "Handle", "Address.Zip": "Address.PostCode"}
			})),
		succeeds(updateCase("update with rename onto a field that exists does nothing", "write",
			map[string]any{"Name": "Ada", "Nick": "gh", "Handle": "ada"},
			func(u *server.UpdateRequest) { u.Rename = map[string]string{"Nick": "Handle"} })),
		succeeds(updateCase("update with every operator", "wire-names",
			map[string]any{
				"Name": "Ada", "Draft": true, "Visits": int32(1), "Tags": []any{"maths"}, "Langs": []any{"en"},
				"Score": int32(10), "Age": int32(36), "Best": int32(10), "Nick": "gh",
			},
			func(u *server.UpdateRequest) {
				u.Set = map[string]any{"City": "London"}
				u.Unset = []string{"Draft"}
				u.Inc = map[string]int64{"Visits": 2}
				u.Push = map[string]any{"Tags": "poetry"}
				u.AddToSet = map[string]any{"Langs": "en"}
				u.Mul = map[string]float64{"Score": 1.5}
				u.Min = map[string]any{"Age": int32(30)}
				u.Max = map[string]any{"Best": int32(99)}
				u.Rename = map[string]string{"Nick": "Handle"}
			})),
		refused(updateCase("update with mul on a string is refused", "errors", ada(),
			func(u *server.UpdateRequest) { u.Mul = map[string]float64{"Name": 1.5} })),
		refused(updateCase("update with inc past the 64-bit range is refused", "errors",
			map[string]any{"Name": "Ada", "Visits": int64(9223372036854775807)},
			func(u *server.UpdateRequest) { u.Inc = map[string]int64{"Visits": 1} })),
		refused(updateCase("update with unset of an array position is refused", "errors",
			map[string]any{"Name": "Ada", "Tags": []any{"maths", "poetry"}},
			func(u *server.UpdateRequest) { u.Unset = []string{"Tags[0]"} })),
	}
}

// retiredCases are the table slots of vectors that were captured on the three-node cluster of
// the replicated mode the server no longer starts (it is refused in every mode). The vectors
// are gone from the file, but each slot stays so that every later exchange keeps its number,
// database name and request ID: a retired slot is skipped by the run (retiredCase).
func retiredCases(count int) []func(db string) exchangeCase {
	slots := make([]func(db string) exchangeCase, count)
	for index := range slots {
		slots[index] = retiredCase
	}
	return slots
}

// retiredCase is one retired table slot.
func retiredCase(string) exchangeCase { return exchangeCase{retired: true} }

// clusterCases is the ten slots of the retired three-node capture: health, list nodes, the
// write-concern puts, consistent get, the three not-yet-added vectors, the put with a node
// stopped and the list with membership history.
func clusterCases() []func(db string) exchangeCase { return retiredCases(10) }

// bucketInfoCases pin BUCKET_INFO: one bucket's declaration without a walk of
// its stores. They are appended after every other exchange so no earlier
// case's number, database name or request id moves.
func bucketInfoCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "bucket info with store fields", group: "ddl", storeType: "hash",
				notes: "Answers the bucket's row of LIST_BUCKETS under a bucket member: type, declared store field names, " +
					"replication_factor and entries. store_fields, a sibling of bucket, lists each declared store field with its engine, " +
					"in declaration order, spelled as BUCKET_STATS spells it (hash, fifo, lifo, btree, heap, blob; a declaration's Store suffix is dropped). " +
					"It never counts the stores' elements (BUCKET_STATS does).",
				setup: []call{
					createDB(db),
					{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
						Database: db, Name: "User", Type: server.BucketTypeHash,
						Options: map[string]any{"stores": []any{
							map[string]any{"name": "Friends", "engine": "hash"},
							map[string]any{"name": "Purchases", "engine": "fifoStore"},
							map[string]any{"name": "Photos", "engine": "lifoStore"},
							map[string]any{"name": "Recipes", "engine": "btreeStore"},
							map[string]any{"name": "Books", "engine": "heapStore"},
							map[string]any{"name": "Data", "engine": "blobStore"},
						}},
					}},
					put(db, "User", "ada", ada()),
					field(transport.OpFieldPut, server.FieldRequest{Database: db, Bucket: "User", Key: "ada", Field: "Friends", Element: "grace", Data: grace()}),
				},
				request:    call{op: transport.OpBucketInfo, payload: server.BucketInfoRequest{Database: db, Bucket: "User"}},
				wantOpcode: transport.OpBucketInfo, wantField: map[string]any{"found": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "bucket info of a bucket that does not exist", group: "ddl",
				setup:      []call{createDB(db)},
				notes:      "An ERROR NOT_FOUND, the answer BUCKET_STATS gives.",
				request:    call{op: transport.OpBucketInfo, payload: server.BucketInfoRequest{Database: db, Bucket: "b"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
				wantMessage: []string{`bucket "b" not found in database`},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "bucket info of a database that does not exist", group: "ddl",
				request:    call{op: transport.OpBucketInfo, payload: server.BucketInfoRequest{Database: db, Bucket: "b"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
				wantMessage: []string{fmt.Sprintf("database %q not found", db)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "bucket info outside the user's grant", group: "errors", storeType: "hash", noReplay: true,
				notes: "Sent on a connection authenticated as info-reader, who holds the reader role on this vector's database only, " +
					"for a bucket of another database: the read permission BUCKET_STATS needs, refused the same way.",
				prepare:  createInfoReader,
				username: infoReader, password: readerPassword,
				request:    call{op: transport.OpBucketInfo, payload: server.BucketInfoRequest{Database: "other-" + db, Bucket: "users"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrPermissionDenied,
				wantMessage: []string{`permission denied: user "info-reader"`},
			}
		},
		// "bucket info on a cluster" was captured on the retired three-node cluster; its slot stays.
		retiredCase,
	}
}
