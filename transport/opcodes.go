// Package transport implements hoardDB's TCP+TLS wire protocol.
//
// This file defines all opcodes and error codes used in the binary framing
// protocol. See the tcp-protocol-spec record for the full wire format
// specification.
package transport

// Protocol version. Servers reject unknown major versions.
const ProtocolVersion uint8 = ProtocolMajor

// RequestIDLen is the fixed size of the request ID field in the frame header.
// Each request ID is a 16-byte UUID v4, used for 0-RTT deduplication and
// request correlation.
const RequestIDLen = 16

// Opcode represents a single-byte operation type in the wire protocol.
//
// A write opcode is never added, and an existing one never changes its
// meaning, without a member feature: from v1.0 on, the change names a new
// feature string in the server's per-opcode member-feature table
// (server.writeOpcodeFeatures), and the build that introduces it advertises
// that string on gossip. The node that would log the write first then refuses
// it while a member that has gossiped lacks the feature, instead of logging an
// entry an older member would carry out differently: a bucket's owner for a
// bucket write, the node the client reached for database DDL. A server test
// fails for a write opcode with no entry.
type Opcode uint8

const (
	// Authentication opcodes. 0x04 (formerly OpRetryAfter, a 0-RTT-only
	// retry signal) is retired along with QUIC's 0-RTT model — TCP has no
	// early data, so nothing was ever sent this opcode.
	OpAuth     Opcode = 0x01 // Client authentication request
	OpAuthOK   Opcode = 0x02 // Authentication successful
	OpAuthFail Opcode = 0x03 // Authentication failed

	// Hash store opcodes.
	OpPut            Opcode = 0x10 // Hash store put (key + BSON value)
	OpGet            Opcode = 0x11 // Hash store get (key)
	OpGetResponse    Opcode = 0x12 // Hash store get response (BSON value)
	OpDelete         Opcode = 0x13 // Hash store delete (key)
	OpExists         Opcode = 0x14 // Hash store exists check (key)
	OpExistsResponse Opcode = 0x15 // Boolean response
	OpUpsert         Opcode = 0x16 // Hash or btree upsert: replaces the document's scalar fields, keeps its stores
	OpUpsertOK       Opcode = 0x17 // Upsert response

	// Batch opcodes.
	OpBatchPut   Opcode = 0x20 // Batch insert (array of key-value pairs)
	OpBatchPutOK Opcode = 0x21 // Batch insert response (per-key status)
	// OpBatch carries many independent store-field and keyed-update requests,
	// to any buckets, in one frame (batch-request spec §3); OpBatchOK answers
	// it with one answer per item. A batch is not a transaction.
	OpBatch   Opcode = 0x22 // Many independent requests in one frame
	OpBatchOK Opcode = 0x23 // One answer per item of a BATCH, in item order
	// OpBatchUpsert is OpBatchPut's replacing form: each document replaces the
	// scalar fields of the one stored under its _id_.
	OpBatchUpsert   Opcode = 0x24 // Batch upsert (array of documents)
	OpBatchUpsertOK Opcode = 0x25 // Batch upsert response

	// FIFO opcodes.
	OpFIFOPush        Opcode = 0x30 // FIFO store push (BSON value)
	OpFIFOPop         Opcode = 0x31 // FIFO store pop
	OpFIFOPopResponse Opcode = 0x32 // FIFO pop response (BSON value)
	OpFIFOPeek        Opcode = 0x33 // FIFO peek (no consume)

	// Lease opcodes. One set for fifo, lifo and heap buckets: the server reads
	// the type from the bucket, as it does for peek. Each answers with its own
	// opcode (claim-ack-spec 9.1).
	OpQueueClaim   Opcode = 0x34 // Claim entries under a lease
	OpQueueAck     Opcode = 0x35 // Acknowledge a lease's entries
	OpQueueRelease Opcode = 0x36 // Give a lease's entries back
	OpQueueExtend  Opcode = 0x37 // Move a lease's deadline out

	// Dead-letter opcodes (claim-ack-spec 9.1). DEAD_LIST is a read the owner
	// serves; DEAD_RESTORE is sent by a restore, never by a client.
	OpDeadList    Opcode = 0x38 // List dead entries
	OpDeadRequeue Opcode = 0x39 // Write dead entries back to the queue
	OpDeadDiscard Opcode = 0x3A // Drop dead entries
	OpDeadRestore Opcode = 0x3B // Restore one dead entry from a dump

	// LIFO opcodes.
	OpLIFOPush        Opcode = 0x40 // LIFO store push (BSON value)
	OpLIFOPop         Opcode = 0x41 // LIFO store pop
	OpLIFOPopResponse Opcode = 0x42 // LIFO pop response (BSON value)
	OpLIFOPeek        Opcode = 0x43 // LIFO peek (no consume)

	// B+tree opcodes.
	OpBTreeInsert   Opcode = 0x50 // B+tree insert (key + BSON value)
	OpBTreeLookup   Opcode = 0x51 // B+tree point lookup (key)
	OpBTreeRange    Opcode = 0x52 // B+tree range scan (start + end key)
	OpBTreeResponse Opcode = 0x53 // B+tree response (BSON value or array)

	// Heap opcodes.
	OpHeapPush        Opcode = 0x60 // Heap store push (priority + BSON value)
	OpHeapPop         Opcode = 0x61 // Heap store pop (highest priority)
	OpHeapPopResponse Opcode = 0x62 // Heap pop response (BSON value)

	// Blob opcodes.
	OpBlobWrite        Opcode = 0x70 // Blob store write (BSON ID + binary data)
	OpBlobRead         Opcode = 0x71 // Blob store read (BSON ID)
	OpBlobReadResponse Opcode = 0x72 // Blob read response (binary data)

	// Database/bucket management opcodes.
	OpCreateDB     Opcode = 0x80 // Create database
	OpCreateBucket Opcode = 0x81 // Create bucket (name + type)
	OpDeleteDB     Opcode = 0x82 // Delete database
	OpDeleteBucket Opcode = 0x83 // Delete bucket
	OpListBuckets  Opcode = 0x84 // List buckets in database
	OpListDBs      Opcode = 0x85 // List databases on this node
	OpBucketStats  Opcode = 0x86 // Bucket statistics (type, entries, indexes)

	// Document/query opcodes.
	OpCount      Opcode = 0x87 // Count entries in a bucket
	OpUpdate     Opcode = 0x88 // Update a document in place ($set, $unset, $inc, $push, $addToSet, $mul, $min, $max, $rename)
	OpBTreeFirst Opcode = 0x89 // Lowest key in a B+tree bucket
	OpBTreeLast  Opcode = 0x8A // Highest key in a B+tree bucket
	OpSearch     Opcode = 0x8B // Query a bucket's secondary indexes
	OpScan       Opcode = 0x8C // Bounded scan of a bucket's documents (optional server-side filter)
	OpDistinct   Opcode = 0x8D // Distinct values of a field in a bucket (optional server-side filter)

	// OpAlterBucket changes a bucket's replication factor (alter bucket
	// Sessions { replication_factor: 5 }): bucket DDL, logged by the bucket's
	// owner and delivered on its replication stream to the bucket's sets before
	// and after the change (bucket-replication-rev8-spec §3.2, §5). It is not
	// read-only: a read-only opcode is never logged.
	OpAlterBucket Opcode = 0x8E

	// Cluster opcodes.
	OpGossip         Opcode = 0x90 // Cluster membership update
	OpReplPush       Opcode = 0x91 // Replication push (bucket + entries)
	OpReplAck        Opcode = 0x92 // Replication acknowledgment
	OpMigrateStart   Opcode = 0x93 // Bucket migration start
	OpMigrateData    Opcode = 0x94 // Bucket migration data
	OpMigrateDone    Opcode = 0x95 // Bucket migration complete
	OpClusterPropose Opcode = 0x96 // Propose a membership change (node-to-node vote)
	OpClusterAdd     Opcode = 0x97 // Operator trigger to add a node (client-facing)
	OpMigrate        Opcode = 0x98 // Operator trigger to pull a bucket to this node (client-facing)
	OpClusterRemove  Opcode = 0x99 // Operator trigger to remove a node (client-facing)
	// OpClusterProbe and OpClusterProbeReply carry the control channel of
	// conventions section 6: one control message per frame, any epoch
	// accepted (node-to-node).
	OpClusterProbe      Opcode = 0x9A
	OpClusterProbeReply Opcode = 0x9B
	OpConsistentGet     Opcode = 0x9C // Quorum read + read repair (client-facing)
	OpVersionedGet      Opcode = 0x9D // Raw read returning the read-repair version (node-to-node)
	// OpMembershipChange is one phase (prepare, accept, commit) of a
	// membership change between members (node-to-node).
	OpMembershipChange Opcode = 0x9E

	// Heartbeat opcodes.
	OpPing  Opcode = 0xA0 // Heartbeat ping
	OpPong  Opcode = 0xA1 // Heartbeat pong
	OpError Opcode = 0xA2 // Error response (code + message)

	// Observability opcodes.
	OpHealthCheck Opcode = 0xA3 // Node health and statistics
	OpListNodes   Opcode = 0xA4 // List known cluster nodes

	// Dump/restore opcodes (the dump-restore-spec record §10). Driver-facing,
	// bucket-scoped, root-gated until opRules exists (server/dump.go). Restore
	// handler bodies land in a later commit; the opcodes are reserved together
	// because the archived spec's allocation is reused verbatim.
	OpDumpStart    Opcode = 0xA5 // client -> server: begin a bucket dump; reply: manifest
	OpDumpChunk    Opcode = 0xA6 // client -> server: next chunk of a bucket dump
	OpDumpDone     Opcode = 0xA7 // client -> server: acknowledge digest; server releases the pin
	OpRestoreStart Opcode = 0xA8 // client -> server: begin a bucket restore
	OpRestoreChunk Opcode = 0xA9 // client -> server: bytes for the open restore
	OpRestoreDone  Opcode = 0xAA // client -> server: close the bucket, assert count, apply

	// Bulk stream opcodes.
	OpStreamOpen Opcode = 0xB0 // Open a bulk data stream
	OpStreamData Opcode = 0xB1 // Stream data chunk
	OpStreamAck  Opcode = 0xB3 // Stream flow-control acknowledgment (server→client)
	OpStreamEnd  Opcode = 0xB2 // Stream complete

	// Read-path optimization opcodes. See docs/read-path-optimization-spec.md.
	OpGetFast         Opcode = 0xC1 // Compact read request (database + bucket + key, binary-encoded)
	OpGetFastResponse Opcode = 0xC2 // Compact read response (status byte + BSON document)

	// User-management opcodes (the user-store-spec record 6.1). Driver
	// mutations in 0xB5-0xBB are handled before cluster bucket routing and
	// are served by the elected master; list/get are served by any node.
	OpUserCreate Opcode = 0xB5 // Create a user (name, derived credential, roles)
	OpUserAlter  Opcode = 0xB6 // Replace a user's roles/enabled state
	OpUserDrop   Opcode = 0xB7 // Drop a user (last-admin guard applies)
	OpUserList   Opcode = 0xB8 // List users (never hashes)
	OpUserGet    Opcode = 0xB9 // Describe one user
	OpUserGrant  Opcode = 0xBA // Add one {role, db} grant
	OpUserRevoke Opcode = 0xBB // Remove one {role, db} grant

	// Node-only auth-config replication (the user-store-spec record 5.3).
	OpAuthConfigPush Opcode = 0xC4 // Master to member: {term, version, config}
	OpAuthConfigAck  Opcode = 0xC5 // Member to master: {term, version}

	// Field stores (nested buckets, GAP-22): one opcode per verb, not per
	// engine — the engine is the field's declaration, and a verb it lacks is
	// refused. New opcodes and not optional members on the bucket verbs,
	// because BSON ignores unknown members: a field write sent to a server
	// without this feature would be applied to the bucket itself and
	// reported as success.
	OpFieldPush   Opcode = 0xD0 // Append to a fifo/lifo field, or insert into a heap field
	OpFieldPop    Opcode = 0xD1 // Remove the element at a fifo/lifo/heap field's cursor
	OpFieldPeek   Opcode = 0xD2 // Read the element a pop would take
	OpFieldPut    Opcode = 0xD3 // Upsert an element of a hash/btree/blob field by key
	OpFieldGet    Opcode = 0xD4 // Read an element of a hash/btree/blob field by key
	OpFieldDelete Opcode = 0xD5 // Delete an element of a hash/btree/blob field by key
	OpFieldLen    Opcode = 0xD6 // One document's element count for a field
	OpFieldList   Opcode = 0xD7 // One page of a field's elements, in engine order

	// OpDeclareStoreFields adds store fields to an existing bucket (alter
	// bucket Post { Tags hashStore }): bucket DDL, additive only.
	OpDeclareStoreFields Opcode = 0xD8

	// OpBucketInfo is one bucket's declaration: its type, its declared store
	// field names, its replication factor and its entry count, read without
	// walking a store, an index or an element (BUCKET_STATS does that). It
	// sits beside OpDeclareStoreFields because 0x8F is reserved by
	// the store-type-conversion-spec record.
	OpBucketInfo Opcode = 0xD9

	// The entry-ring client opcodes (entry-ring-client-wire.md section 1).

	// OpRingList is a ListRequest (BSON); a read.
	OpRingList Opcode = 0xDA
	// OpRingCount is a CountRequest (BSON); a read.
	OpRingCount Opcode = 0xDB
	// OpRingKeyed is a keyed request: client prologue, 40-byte header, one class form.
	OpRingKeyed Opcode = 0xDC
	// OpRingAdmin is one BSON admin command (create_database, create_bucket, describe_bucket,
	// show_buckets; entry-ring-admin-wire.md). It is not a read: the catalog changes.
	OpRingAdmin Opcode = 0xDD
	// OpRingTopology is a TopologyRequest (BSON); a read.
	OpRingTopology Opcode = 0xDE
	// OpRingReply is the success reply to all five ring requests, matched by request id.
	OpRingReply Opcode = 0xDF
	// OpRingBatch is a batch of write operations: one client prologue, a count byte, then
	// that many RING_KEYED write bodies. It is not a read: it writes. It is answered by
	// OpRingReply with one answer per operation (entry-ring-client-wire.md, "RING_BATCH").
	OpRingBatch Opcode = 0xE0
	// OpRingGetMany reads many documents in one frame: one client prologue, a count byte, then
	// that many RING_KEYED get or get_fields bodies. It is a read. It is answered by OpRingReply
	// with one answer per key in request order (entry-ring-multiget-spec.md).
	OpRingGetMany Opcode = 0xE1
)

// IsReadOnly returns true if the opcode is a read that has no side effects.
// server/write_path.go uses this (not a "mutating" predicate) to decide what
// counts as a write for replication purposes.
func (op Opcode) IsReadOnly() bool {
	switch op {
	case OpGet, OpExists, OpBTreeLookup, OpBTreeRange, OpPing, OpHealthCheck, OpListNodes,
		OpListBuckets, OpListDBs, OpBucketStats, OpBucketInfo, OpCount, OpBTreeFirst, OpBTreeLast, OpBlobRead, OpSearch, OpScan, OpGetFast,
		OpConsistentGet, OpVersionedGet, OpDistinct,
		OpDumpStart, OpDumpChunk, OpDumpDone,
		OpFIFOPeek, OpLIFOPeek, OpDeadList,
		OpFieldPeek, OpFieldGet, OpFieldLen, OpFieldList,
		OpRingList, OpRingCount, OpRingTopology, OpRingGetMany:
		return true
	default:
		return false
	}
}

// String returns a human-readable name for the opcode.
func (op Opcode) String() string {
	switch op {
	case OpAuth:
		return "AUTH"
	case OpAuthOK:
		return "AUTH_OK"
	case OpAuthFail:
		return "AUTH_FAIL"
	case OpPut:
		return "PUT"
	case OpGet:
		return "GET"
	case OpGetResponse:
		return "GET_RESPONSE"
	case OpDelete:
		return "DELETE"
	case OpExists:
		return "EXISTS"
	case OpExistsResponse:
		return "EXISTS_RESPONSE"
	case OpUpsert:
		return "UPSERT"
	case OpUpsertOK:
		return "UPSERT_OK"
	case OpBatchUpsert:
		return "BATCH_UPSERT"
	case OpBatchUpsertOK:
		return "BATCH_UPSERT_OK"
	case OpBatchPut:
		return "BATCH_PUT"
	case OpBatchPutOK:
		return "BATCH_PUT_OK"
	case OpBatch:
		return "BATCH"
	case OpBatchOK:
		return "BATCH_OK"
	case OpFIFOPush:
		return "FIFO_PUSH"
	case OpFIFOPop:
		return "FIFO_POP"
	case OpFIFOPopResponse:
		return "FIFO_POP_RESPONSE"
	case OpFIFOPeek:
		return "FIFO_PEEK"
	case OpQueueClaim:
		return "QUEUE_CLAIM"
	case OpQueueAck:
		return "QUEUE_ACK"
	case OpQueueRelease:
		return "QUEUE_RELEASE"
	case OpQueueExtend:
		return "QUEUE_EXTEND"
	case OpDeadList:
		return "DEAD_LIST"
	case OpDeadRequeue:
		return "DEAD_REQUEUE"
	case OpDeadDiscard:
		return "DEAD_DISCARD"
	case OpDeadRestore:
		return "DEAD_RESTORE"
	case OpLIFOPush:
		return "LIFO_PUSH"
	case OpLIFOPop:
		return "LIFO_POP"
	case OpLIFOPopResponse:
		return "LIFO_POP_RESPONSE"
	case OpLIFOPeek:
		return "LIFO_PEEK"
	case OpBTreeInsert:
		return "BTREE_INSERT"
	case OpBTreeLookup:
		return "BTREE_LOOKUP"
	case OpBTreeRange:
		return "BTREE_RANGE"
	case OpBTreeResponse:
		return "BTREE_RESPONSE"
	case OpHeapPush:
		return "HEAP_PUSH"
	case OpHeapPop:
		return "HEAP_POP"
	case OpHeapPopResponse:
		return "HEAP_POP_RESPONSE"
	case OpBlobWrite:
		return "BLOB_WRITE"
	case OpBlobRead:
		return "BLOB_READ"
	case OpBlobReadResponse:
		return "BLOB_READ_RESPONSE"
	case OpCreateDB:
		return "CREATE_DB"
	case OpCreateBucket:
		return "CREATE_BUCKET"
	case OpDeleteDB:
		return "DELETE_DB"
	case OpDeleteBucket:
		return "DELETE_BUCKET"
	case OpListBuckets:
		return "LIST_BUCKETS"
	case OpListDBs:
		return "LIST_DATABASES"
	case OpBucketStats:
		return "BUCKET_STATS"
	case OpBucketInfo:
		return "BUCKET_INFO"
	case OpCount:
		return "COUNT"
	case OpUpdate:
		return "UPDATE"
	case OpBTreeFirst:
		return "BTREE_FIRST"
	case OpBTreeLast:
		return "BTREE_LAST"
	case OpSearch:
		return "SEARCH"
	case OpScan:
		return "SCAN"
	case OpDistinct:
		return "DISTINCT"
	case OpAlterBucket:
		return "ALTER_BUCKET"
	case OpGossip:
		return "GOSSIP"
	case OpReplPush:
		return "REPL_PUSH"
	case OpReplAck:
		return "REPL_ACK"
	case OpMigrateStart:
		return "MIGRATE_START"
	case OpMigrateData:
		return "MIGRATE_DATA"
	case OpMigrateDone:
		return "MIGRATE_DONE"
	case OpClusterPropose:
		return "CLUSTER_PROPOSE"
	case OpClusterAdd:
		return "CLUSTER_ADD"
	case OpMigrate:
		return "MIGRATE"
	case OpClusterRemove:
		return "CLUSTER_REMOVE"
	case OpClusterProbe:
		return "CLUSTER_PROBE"
	case OpClusterProbeReply:
		return "CLUSTER_PROBE_REPLY"
	case OpConsistentGet:
		return "CONSISTENT_GET"
	case OpVersionedGet:
		return "VERSIONED_GET"
	case OpMembershipChange:
		return "MEMBERSHIP_CHANGE"
	case OpPing:
		return "PING"
	case OpPong:
		return "PONG"
	case OpHealthCheck:
		return "HEALTH_CHECK"
	case OpListNodes:
		return "LIST_NODES"
	case OpDumpStart:
		return "DUMP_START"
	case OpDumpChunk:
		return "DUMP_CHUNK"
	case OpDumpDone:
		return "DUMP_DONE"
	case OpRestoreStart:
		return "RESTORE_START"
	case OpRestoreChunk:
		return "RESTORE_CHUNK"
	case OpRestoreDone:
		return "RESTORE_DONE"
	case OpError:
		return "ERROR"
	case OpStreamOpen:
		return "STREAM_OPEN"
	case OpStreamData:
		return "STREAM_DATA"
	case OpStreamEnd:
		return "STREAM_END"
	case OpStreamAck:
		return "STREAM_ACK"
	case OpGetFast:
		return "GET_FAST"
	case OpGetFastResponse:
		return "GET_FAST_RESPONSE"
	case OpUserCreate:
		return "USER_CREATE"
	case OpUserAlter:
		return "USER_ALTER"
	case OpUserDrop:
		return "USER_DROP"
	case OpUserList:
		return "USER_LIST"
	case OpUserGet:
		return "USER_GET"
	case OpUserGrant:
		return "USER_GRANT"
	case OpUserRevoke:
		return "USER_REVOKE"
	case OpAuthConfigPush:
		return "AUTH_CONFIG_PUSH"
	case OpAuthConfigAck:
		return "AUTH_CONFIG_ACK"
	case OpFieldPush:
		return "FIELD_PUSH"
	case OpFieldPop:
		return "FIELD_POP"
	case OpFieldPeek:
		return "FIELD_PEEK"
	case OpFieldPut:
		return "FIELD_PUT"
	case OpFieldGet:
		return "FIELD_GET"
	case OpFieldDelete:
		return "FIELD_DELETE"
	case OpFieldLen:
		return "FIELD_LEN"
	case OpFieldList:
		return "FIELD_LIST"
	case OpDeclareStoreFields:
		return "DECLARE_STORE_FIELDS"
	case OpRingList:
		return "RING_LIST"
	case OpRingCount:
		return "RING_COUNT"
	case OpRingKeyed:
		return "RING_KEYED"
	case OpRingAdmin:
		return "RING_ADMIN"
	case OpRingTopology:
		return "RING_TOPOLOGY"
	case OpRingReply:
		return "RING_REPLY"
	case OpRingBatch:
		return "RING_BATCH"
	case OpRingGetMany:
		return "RING_GET_MANY"
	default:
		return "UNKNOWN"
	}
}
