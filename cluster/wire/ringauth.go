package wire

import (
	"context"
	"errors"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// RingClass is the operation class of a ring request, the value an authorizer maps to a
// grant: a write class (cluster.OpInsert to cluster.OpExtend, 0x01 to 0x09) or a read
// class (RingClassGet to RingClassCount, 0x81 to 0x86, and RingClassLeaseGet, RingClassStoreGet, RingClassGetFields and RingClassNearest, 0x87 to 0x8A). The
// two ranges do not overlap.
type RingClass = byte

// The classes of the two bucket-wide reads. RING_LIST and RING_COUNT name the bucket by
// database and bucket name and carry no operation byte, so these two values are never on
// the wire: they are the names the handler gives its asks, so one authorizer table maps
// every ring request. They sit above the RING_KEYED read classes and are not accepted by
// DecodeRingKeyed.
const (
	// RingClassList is a RING_LIST: a filtered page of a bucket's documents.
	RingClassList RingClass = 0x85
	// RingClassCount is a RING_COUNT: how many documents a bucket holds.
	RingClassCount RingClass = 0x86
)

// The classes of the RING_ADMIN commands. Like RingClassList they are never on the wire:
// the command is named by the cmd member of the body, and these are the names the handler
// gives its asks so one authorizer table maps every ring request.
const (
	// RingClassCreateDatabase is a create_database command.
	RingClassCreateDatabase RingClass = 0x91
	// RingClassCreateBucket is a create_bucket command.
	RingClassCreateBucket RingClass = 0x92
	// RingClassDescribeBucket is a describe_bucket command.
	RingClassDescribeBucket RingClass = 0x93
	// RingClassShowBuckets is a show_buckets command.
	RingClassShowBuckets RingClass = 0x94
	// RingClassShowDatabases is a show_databases command. It is asked once per database the
	// listing considers, and the answer is whether the session may see that database.
	RingClassShowDatabases RingClass = 0x95
	// RingClassDescribeDatabase is a describe_database command.
	RingClassDescribeDatabase RingClass = 0x96
	// RingClassAlterBucket is an alter_bucket command. It is 0x9A because 0x97 is a legacy
	// opcode's number and 0x98 and 0x99 are taken below.
	RingClassAlterBucket RingClass = 0x9A
	// RingClassDropBucket is a drop_bucket command. It is 0x9B, the next number after 0x9A.
	RingClassDropBucket RingClass = 0x9B
	// RingClassSeedingStatus is a seeding_status command. It is 0x9C, the next number after 0x9B. Classes are
	// never on the wire, so sharing a number with a legacy opcode (0x9C to 0x9E are OpConsistentGet,
	// OpVersionedGet and OpMembershipChange, as 0x98 and 0x99 already are) confuses nothing; an audit line
	// names the legacy opcode the class stands for, not the number.
	RingClassSeedingStatus RingClass = 0x9C
	// RingClassSeedingRetry is a seeding_retry command.
	RingClassSeedingRetry RingClass = 0x9D
	// RingClassSeedingForget is a seeding_forget command.
	RingClassSeedingForget RingClass = 0x9E
)

// RingClassTopology is a RING_TOPOLOGY. It is never on the wire either. Any authenticated
// session may ask it (owner, 2026-10-31): the class-only ask (no database) is answered yes to
// every session that has an identity, because the table names members and slots and no
// database, bucket or document. The ask that names a database is the visibility of that
// database's name: it is answered yes when the session holds any role on it, as the legacy
// list of databases does, and the handler leaves out the databases it is answered no for.
const RingClassTopology RingClass = 0x98

// RingClassDescribeNode is a describe_node command. It is never on the wire. Any session with
// an identity may ask it, as the legacy health request it stands for is open to every user; the
// class-only ask is the only ask. 0x97 stays unused: it is the number of a legacy opcode and
// would read as one in an audit line.
const RingClassDescribeNode RingClass = 0x99

// RingClasses lists every class a ring request is authorized as, so an authorizer's table
// and its tests can be checked against the whole set. RingClassList, RingClassCount and the
// admin classes are not carried by RING_KEYED.
func RingClasses() []RingClass {
	classes := []RingClass{
		cluster.OpInsert, cluster.OpUpdate, cluster.OpUpsert, cluster.OpDelete, cluster.OpStoreVerb,
		cluster.OpClaim, cluster.OpAck, cluster.OpRelease, cluster.OpExtend,
		RingClassGet, RingClassGetFields, RingClassPeek, RingClassStoreList, RingClassStoreCount, RingClassLeaseGet, RingClassStoreGet, RingClassNearest, RingClassAggregate, RingClassList, RingClassCount,
		RingClassCreateDatabase, RingClassCreateBucket, RingClassDescribeBucket, RingClassShowBuckets,
		RingClassShowDatabases, RingClassDescribeDatabase, RingClassTopology, RingClassDescribeNode, RingClassAlterBucket, RingClassDropBucket,
		RingClassSeedingStatus, RingClassSeedingRetry, RingClassSeedingForget,
	}
	classes = append(classes, membershipClasses()...)
	return append(classes, userClasses()...)
}

// RingAccess is one request for permission: the session's right to run Class on a bucket.
// Database and Bucket are names, as the legacy authorization maps them. Both empty asks
// whether the session holds the access on at least one database, which the handler checks
// before it looks a bucket id up so that a session with no grants learns nothing.
type RingAccess struct {
	Database, Bucket string
	Class            RingClass
}

// RingAuthorizer decides whether the session in ctx may make a ring request. A ring
// request names a bucket by id; the handler resolves the id to a database and bucket and
// asks before any Node method runs, and answers PERMISSION_DENIED for any error returned,
// with one text that does not depend on the error. An implementation fails closed: a
// context with no session, an unknown class and a session without a grant are all errors.
type RingAuthorizer interface {
	// Authorize returns nil when the session may run access.Class on the bucket.
	Authorize(ctx context.Context, access RingAccess) error
}

// errNoRingAuthorizer is why NewRingHandler refuses to build.
var errNoRingAuthorizer = errors.New("a ring handler needs an authorizer: every ring request names a bucket by id, and without one any authenticated session could read and write every bucket. Pass the server's ring authorizer (server.RingAuthorizer)")
