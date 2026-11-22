package cli

import (
	"context"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// RingAPI is the part of a ringclient.Client the ring statements use. The CLI
// talks to it so a test can stand in a fake server; ringConnection adapts the
// real client.
type RingAPI interface {
	// CreateDatabaseWithStorage creates a database; rf 0 leaves the replication factor to the server and
	// storage "" is disk.
	CreateDatabaseWithStorage(ctx context.Context, name string, rf int32, storage string) (ringclient.DatabaseInfo, error)
	// CreateBucket declares a bucket and every field.
	CreateBucket(ctx context.Context, database, name string, fields []ringclient.FieldDecl) (ringclient.BucketInfo, error)
	// CreateBucketExpiring is CreateBucket for a bucket with an expiry: expireField is the datetime field
	// its documents expire at, and a field's ExpirePath is a queue's element path.
	CreateBucketExpiring(ctx context.Context, database, name, expireField string, fields []ringclient.FieldDecl) (ringclient.BucketInfo, error)
	// AlterBucket adds and drops fields and returns the declaration in force afterwards.
	AlterBucket(ctx context.Context, database, name string, change ringclient.AlterChange) (ringclient.BucketInfo, error)
	// DropBucket drops a bucket and its documents.
	DropBucket(ctx context.Context, database, name string) error
	// DescribeBucket reads the declaration in force.
	DescribeBucket(ctx context.Context, database, name string) (ringclient.BucketInfo, error)
	// ShowBuckets lists one page of a database's buckets.
	ShowBuckets(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.BucketPage, error)
	// ShowDatabases lists one page of the databases the session may see.
	ShowDatabases(ctx context.Context, page ringclient.PageRequest) (ringclient.DatabasePage, error)
	// DescribeDatabase reads a database's id and replication factor and one page of its buckets.
	DescribeDatabase(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.DatabaseDescription, error)
	// DescribeNode reads the facts of the node the session is on, for the banner.
	DescribeNode(ctx context.Context) (ringclient.NodeDescription, error)
	// SeedingStatus lists one page of the slot pulls of the node the session is on.
	SeedingStatus(ctx context.Context, query ringclient.SeedingQuery) (ringclient.SeedingPage, error)
	// SeedingRetry starts the pull of one slot again from a fresh cut.
	SeedingRetry(ctx context.Context, database string, slot int) error
	// SeedingForget removes the retired record of one slot and keeps its range. anyway forgets a
	// record although this node may hold the only copy of the slot's data.
	SeedingForget(ctx context.Context, database string, slot int, anyway bool) error
	// Join registers a running node as a member; the answer is the server's line.
	Join(ctx context.Context, id, addr string, witness bool) (ringclient.MembershipResult, error)
	// Promote makes a member a voter.
	Promote(ctx context.Context, id string) (ringclient.MembershipResult, error)
	// ReplaceNode brings back a down or dead member that lost its data, at its own address;
	// acceptLoss accepts losing the pairs only it held.
	ReplaceNode(ctx context.Context, id, addr string, acceptLoss bool) (ringclient.MembershipResult, error)
	// ClusterView reads the members and the epoch the node holds, for nodes and status.
	ClusterView(ctx context.Context) (*ringclient.Topology, error)
	// Bucket returns the handle the keyed statements run on.
	Bucket(database, name string) RingBucket
	RingUsers
}

// RingUsers is the part of a ringclient.Client the user and role statements use. A new
// password is handed to CreateUser or AlterUser, which derive the credential in this process
// and send only that.
type RingUsers interface {
	// CreateUser creates an account; expectDropSeq is the DropSeq of the user list read before it.
	CreateUser(ctx context.Context, name, password string, roles []ringclient.UserGrant, expectDropSeq int64, options ...ringclient.CallOption) (ringclient.UserResult, error)
	// AlterUser changes an account's password, enabled flag or whole role list.
	AlterUser(ctx context.Context, name string, change ringclient.UserChange, options ...ringclient.CallOption) (ringclient.UserResult, error)
	// DropUser removes an account.
	DropUser(ctx context.Context, name string, expectSeq *int64, options ...ringclient.CallOption) (ringclient.UserResult, error)
	// GrantRole adds one grant.
	GrantRole(ctx context.Context, name string, grant ringclient.UserGrant, expectSeq *int64, options ...ringclient.CallOption) (ringclient.UserResult, error)
	// RevokeRole removes one grant.
	RevokeRole(ctx context.Context, name string, grant ringclient.UserGrant, expectSeq *int64, options ...ringclient.CallOption) (ringclient.UserResult, error)
	// ShowUsers lists the accounts with the store's revision and drop fence.
	ShowUsers(ctx context.Context) (ringclient.UserList, error)
	// DescribeUser reads one account and the drop fence.
	DescribeUser(ctx context.Context, name string) (ringclient.UserDescription, error)
	// PasswordMinLength is the server's minimum new password length, as it reported at login.
	PasswordMinLength() int
}

// RingBucket is the part of a ringclient.Bucket the keyed statements use.
type RingBucket interface {
	// Info returns the bucket's declaration.
	Info(ctx context.Context) (ringclient.BucketInfo, error)
	// Insert adds a document; an existing id is refused.
	Insert(ctx context.Context, id string, doc ringclient.Doc, options ...ringclient.CallOption) (ringclient.WriteResult, error)
	// Upsert adds a document or replaces its scalar fields.
	Upsert(ctx context.Context, id string, doc ringclient.Doc, options ...ringclient.CallOption) (ringclient.WriteResult, error)
	// Update changes scalar fields of an existing document with MongoDB's operators.
	Update(ctx context.Context, id string, change ringclient.Update, options ...ringclient.CallOption) (ringclient.WriteResult, error)
	// Delete removes a document and its stores.
	Delete(ctx context.Context, id string, options ...ringclient.CallOption) (ringclient.WriteResult, error)
	// Get reads one document.
	Get(ctx context.Context, id string) (bson.Raw, error)
	// GetFields reads one document and returns _id_ and only the named scalar fields.
	GetFields(ctx context.Context, id string, fields []string) (bson.Raw, error)
	// Push appends to a fifoStore or lifoStore field.
	Push(ctx context.Context, id, field string, element any, options ...ringclient.CallOption) (ringclient.WriteResult, error)
	// Put stores an element under a key of a hashStore or btreeStore field.
	Put(ctx context.Context, id, field, key string, element any, options ...ringclient.CallOption) (ringclient.WriteResult, error)
	// Inc adds to the numbers of an element of a hashStore or btreeStore field, creating it.
	Inc(ctx context.Context, id, field, key string, increments map[string]any, options ...ringclient.CallOption) (ringclient.WriteResult, error)
	// IncAfter is Inc returning the element as it stands after the add.
	IncAfter(ctx context.Context, id, field, key string, increments map[string]any, options ...ringclient.CallOption) (bson.Raw, ringclient.WriteResult, error)
	// Pop removes the next element of a fifoStore or lifoStore field.
	Pop(ctx context.Context, id, field string, options ...ringclient.CallOption) (bson.Raw, error)
	// Take removes the element under a key of a hashStore or btreeStore field.
	Take(ctx context.Context, id, field, key string, options ...ringclient.CallOption) (bson.Raw, error)
	// PeekKeyed reads the next elements without removing them, with the keys of a heapStore's.
	PeekKeyed(ctx context.Context, id, field string, count int) (ringclient.ElementPage, error)
	// PopN removes up to n elements in one write.
	PopN(ctx context.Context, id, field string, n int, options ...ringclient.CallOption) ([]bson.Raw, error)
	// StoreGet reads one element of a hashStore, btreeStore or heapStore field by key.
	StoreGet(ctx context.Context, id, field, key string) (bson.Raw, error)
	// StoreList reads one page of a store field.
	StoreList(ctx context.Context, id, field string, limit int, after []byte) (ringclient.ElementPage, error)
	// StoreCount counts a store field's elements.
	StoreCount(ctx context.Context, id, field string) (int64, error)
	// Nearest reads the elements of a store field closest to a vector, best first, with their scores.
	Nearest(ctx context.Context, id, field string, query ringclient.NearestQuery) (ringclient.NearestResult, error)
	// Aggregate reads a maintained sum or count of a store field, or describes them all.
	Aggregate(ctx context.Context, id, field string, query ringclient.AggregateQuery) (ringclient.AggregatePage, error)
	// LeaseGet reads the messages a lease still holds.
	LeaseGet(ctx context.Context, id, field string, lease [16]byte) (ringclient.LeaseRead, error)
	// Claim leases messages from a fifoStore or lifoStore field.
	Claim(ctx context.Context, id, field string, count int, options ...ringclient.CallOption) (ringclient.Lease, error)
	// Ack confirms a lease.
	Ack(ctx context.Context, id, field string, lease [16]byte, options ...ringclient.CallOption) (ringclient.LeaseChange, error)
	// Release gives a lease back.
	Release(ctx context.Context, id, field string, lease [16]byte, options ...ringclient.CallOption) (ringclient.LeaseChange, error)
	// Extend moves a live lease's deadline.
	Extend(ctx context.Context, id, field string, lease [16]byte, leaseMs int64, options ...ringclient.CallOption) (ringclient.LeaseChange, error)
	// List reads one page of the bucket's documents.
	List(ctx context.Context, filter any, page ringclient.PageRequest) (ringclient.EntryPage, error)
	// Count counts the bucket's documents.
	Count(ctx context.Context, filter any) (int64, error)
}

// ringConnection adapts a *ringclient.Client to RingAPI.
type ringConnection struct{ *ringclient.Client }

// Bucket returns the client's bucket handle as a RingBucket.
func (c ringConnection) Bucket(database, name string) RingBucket {
	return c.Client.Bucket(database, name)
}

// ClusterView asks the node for its topology without holding an earlier view.
func (c ringConnection) ClusterView(ctx context.Context) (*ringclient.Topology, error) {
	return c.Topology(ctx, nil)
}

// NewRingAPI wraps a connected client for a RingSession.
func NewRingAPI(client *ringclient.Client) RingAPI { return ringConnection{client} }
