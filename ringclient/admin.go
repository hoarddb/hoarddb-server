package ringclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

func (c *Client) admin(ctx context.Context, request wire.RingAdminRequest) (wire.RingAdminReply, error) {
	return c.adminWithID(ctx, request, c.NextRequestID())
}

// adminWithID sends one RING_ADMIN command under request id id and checks the reply answers it.
func (c *Client) adminWithID(ctx context.Context, request wire.RingAdminRequest, id [16]byte) (wire.RingAdminReply, error) {
	body, err := wire.EncodeRingAdmin(request)
	if err != nil {
		return wire.RingAdminReply{}, fmt.Errorf("ringclient: encoding %s: %w", request.Cmd, err)
	}
	payload, err := c.call(ctx, transport.OpRingAdmin, id, body)
	if err != nil {
		return wire.RingAdminReply{}, fmt.Errorf("%s: %w", request.Cmd, err)
	}
	reply, err := wire.DecodeRingAdminReply(payload)
	if err != nil {
		return wire.RingAdminReply{}, fmt.Errorf("ringclient: decoding the %s reply: %w", request.Cmd, err)
	}
	if reply.Cmd != request.Cmd {
		return wire.RingAdminReply{}, fmt.Errorf("ringclient: asked %s and the server answered %s. Check that the server is a hoarddb server of this version", request.Cmd, reply.Cmd)
	}
	return reply, nil
}

// CreateDatabase creates database name on disk. rf is the replication factor; 0 leaves it to the
// server, which means 1. A database that exists is a CONFLICT *Error.
func (c *Client) CreateDatabase(ctx context.Context, name string, rf int32) (DatabaseInfo, error) {
	return c.CreateDatabaseWithStorage(ctx, name, rf, "")
}

// MemoryStorageRefusal is the sentence a create with storage "memory" is refused with, here and on
// the server; the CLI shows it too.
func MemoryStorageRefusal(database string) string { return cluster.MemoryStorageRefusal(database) }

// CreateDatabaseWithStorage creates database name with the storage word "disk"; "" means disk.
// "memory" is refused with *Error INVALID_ARGUMENT before anything is sent, because the in-memory
// store is not built. rf is as CreateDatabase's. The returned Storage is the word the server answers,
// which is the one the database keeps.
func (c *Client) CreateDatabaseWithStorage(ctx context.Context, name string, rf int32, storage string) (DatabaseInfo, error) {
	if storage == wire.AdminStorageMemory {
		return DatabaseInfo{}, &Error{Code: cluster.CodeInvalidArgument, Message: MemoryStorageRefusal(name)}
	}
	request := wire.RingAdminRequest{Cmd: wire.AdminCreateDatabase, Name: name, Storage: storage}
	if rf != 0 {
		request.RF = &rf
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return DatabaseInfo{}, err
	}
	return DatabaseInfo{ID: reply.DatabaseID, RF: reply.RF, Storage: reply.Storage}, nil
}

// CreateBucket declares bucket name in database with the given fields and caches its header.
// Repeating a create with the same fields returns the existing bucket; the same name with
// other fields is BUCKET_EXISTS.
func (c *Client) CreateBucket(ctx context.Context, database, name string, fields []FieldDecl) (BucketInfo, error) {
	return c.CreateBucketExpiring(ctx, database, name, "", fields)
}

// CreateBucketExpiring is CreateBucket for a bucket that expires: expireField names the datetime
// field its documents expire at (empty for none) and a field's ExpirePath makes a fifoStore or
// lifoStore expire its elements at that path.
func (c *Client) CreateBucketExpiring(ctx context.Context, database, name, expireField string, fields []FieldDecl) (BucketInfo, error) {
	declared := make([]wire.RingAdminField, len(fields))
	var paths map[string]string
	var aggregates []wire.AggregateDecl
	for i, field := range fields {
		declared[i] = wire.RingAdminField{Name: field.Name, Type: field.Type, Of: field.Of}
		for _, aggregate := range field.Aggregates {
			aggregates = append(aggregates, wire.AggregateDecl{Store: field.Name, Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By})
		}
		if field.ExpirePath != "" {
			if paths == nil {
				paths = map[string]string{}
			}
			paths[field.Name] = field.ExpirePath
		}
	}
	reply, err := c.admin(ctx, wire.RingAdminRequest{Cmd: wire.AdminCreateBucket, Database: database, Name: name, Fields: declared, Aggregates: aggregates, ExpireField: expireField, ExpirePaths: paths})
	if err != nil {
		return BucketInfo{}, err
	}
	return c.remember(bucketInfoOf(database, name, reply)), nil
}

// AlterChange is what AlterBucket changes: fields to add and names of fields to drop. A name
// appears once in the whole change.
type AlterChange struct {
	Add  []FieldDecl
	Drop []string
	// SetExpire names the datetime field to expire documents at; DropExpire removes the bucket's
	// expiry. At most one of them.
	SetExpire  string
	DropExpire bool
}

// AlterBucket adds and drops fields of bucket name in database and returns the declaration
// in force afterwards, which it also caches as the bucket's header. Adding a field rewrites no
// document; a dropped field's values are deleted in the background and read as unset at once.
// A field's type never changes (INVALID_ARGUMENT). It is safe to repeat: the same alter run
// again changes nothing, and run after an interrupted one it finishes that one.
func (c *Client) AlterBucket(ctx context.Context, database, name string, change AlterChange) (BucketInfo, error) {
	added := make([]wire.RingAdminField, len(change.Add))
	for i, field := range change.Add {
		added[i] = wire.RingAdminField{Name: field.Name, Type: field.Type, Of: field.Of}
	}
	dropped := change.Drop
	if dropped == nil {
		dropped = []string{}
	}
	reply, err := c.admin(ctx, wire.RingAdminRequest{Cmd: wire.AdminAlterBucket, Database: database, Bucket: name, Add: added, Drop: dropped, ExpireField: change.SetExpire, DropExpire: change.DropExpire})
	if err != nil {
		return BucketInfo{}, err
	}
	return c.remember(bucketInfoOf(database, name, reply)), nil
}

// DropBucket drops bucket name of database and its documents, and forgets the bucket's cached
// header. It returns as soon as the bucket is dropping: it stops answering (BUCKET_DROPPING), its
// documents are deleted in the background by the node, and the name is free to create again when
// that is done (show buckets lists the bucket as dropping until then, and a create of the name
// is BUCKET_EXISTS). Nothing the caller does after this returns, closing the connection
// included, stops the deletion. A bucket that is not there is a NOT_FOUND *Error, which is also
// what a drop that finished answers when it is repeated. It is safe to repeat while the bucket is
// dropping. DropBucket drops whatever bucket the name carries, and its reply's id is not returned
// here, so a drop RETRIED after the first one completed and the name was created again drops the
// NEW bucket: a caller that cannot tell must use DropBucketExpecting with the id it holds.
func (c *Client) DropBucket(ctx context.Context, database, name string) error {
	return c.dropBucket(ctx, database, name, 0)
}

func (c *Client) dropBucket(ctx context.Context, database, name string, expect uint64) error {
	if _, err := c.admin(ctx, wire.RingAdminRequest{Cmd: wire.AdminDropBucket, Database: database, Bucket: name, ExpectBucketID: expect}); err != nil {
		return err
	}
	c.headerLock.Lock()
	delete(c.headers, headerKey(database, name))
	c.headerLock.Unlock()
	return nil
}

// DropBucketExpecting is DropBucket for a caller that holds the id of the bucket it means, from
// a describe or a create: if the name now carries another id, because the bucket was dropped and
// created again, nothing is dropped and the call is refused.
func (c *Client) DropBucketExpecting(ctx context.Context, database, name string, expect uint64) error {
	return c.dropBucket(ctx, database, name, expect)
}

// DescribeBucket reads the declaration in force and refreshes the cached header.
func (c *Client) DescribeBucket(ctx context.Context, database, name string) (BucketInfo, error) {
	reply, err := c.admin(ctx, wire.RingAdminRequest{Cmd: wire.AdminDescribeBucket, Database: database, Bucket: name})
	if err != nil {
		return BucketInfo{}, err
	}
	return c.remember(bucketInfoOf(database, name, reply)), nil
}

// ShowBuckets lists one page of the buckets of database in name-slot order. A page can hold
// fewer buckets than the limit; keep asking with the returned Cursor while More is true.
func (c *Client) ShowBuckets(ctx context.Context, database string, page PageRequest) (BucketPage, error) {
	request := wire.RingAdminRequest{Cmd: wire.AdminShowBuckets, Database: database, Cursor: page.Cursor}
	if page.Limit != 0 {
		request.Limit = &page.Limit
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return BucketPage{}, err
	}
	out := BucketPage{Buckets: make([]BucketSummary, len(reply.Buckets)), More: reply.More, Cursor: reply.Cursor}
	for i, listed := range reply.Buckets {
		out.Buckets[i] = BucketSummary{Name: listed.Name, ID: listed.BucketID, State: listed.State}
	}
	return out, nil
}

// ShowDatabases lists one page of the databases this session may see, by name. A session with
// no role on any database gets an empty page and no error. Keep asking with the returned
// Cursor while More is true.
func (c *Client) ShowDatabases(ctx context.Context, page PageRequest) (DatabasePage, error) {
	request := wire.RingAdminRequest{Cmd: wire.AdminShowDatabases, Cursor: page.Cursor}
	if page.Limit != 0 {
		request.Limit = &page.Limit
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return DatabasePage{}, err
	}
	out := DatabasePage{Databases: make([]DatabaseSummary, len(reply.Databases)), More: reply.More, Cursor: reply.Cursor}
	for i, listed := range reply.Databases {
		out.Databases[i] = DatabaseSummary{Name: listed.Name, ID: listed.DatabaseID, RF: listed.RF}
	}
	return out, nil
}

// DescribeDatabase reads a database's id and replication factor and one page of its buckets,
// paged as ShowBuckets is. A database that does not exist and one the session has no role on
// are the same PERMISSION_DENIED *Error.
func (c *Client) DescribeDatabase(ctx context.Context, database string, page PageRequest) (DatabaseDescription, error) {
	request := wire.RingAdminRequest{Cmd: wire.AdminDescribeDatabase, Database: database, Cursor: page.Cursor}
	if page.Limit != 0 {
		request.Limit = &page.Limit
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return DatabaseDescription{}, err
	}
	out := DatabaseDescription{Name: reply.Name, ID: reply.DatabaseID, RF: reply.RF, Storage: reply.Storage,
		MemoryBytes: reply.MemoryBytes, MemoryCapBytes: reply.MemoryCapBytes, Buckets: make([]BucketSummary, len(reply.Buckets)),
		More: reply.More, Cursor: reply.Cursor}
	for i, listed := range reply.Buckets {
		out.Buckets[i] = BucketSummary{Name: listed.Name, ID: listed.BucketID, State: listed.State}
	}
	return out, nil
}

func bucketInfoOf(database, name string, reply wire.RingAdminReply) BucketInfo {
	info := BucketInfo{Database: database, Name: name, ID: reply.BucketID, State: reply.State,
		DeclVersion: reply.DeclVersion, SchemaVersionSeen: reply.SchemaVersionSeen, Fields: make([]FieldInfo, len(reply.Fields))}
	for i, field := range reply.Fields {
		info.Fields[i] = FieldInfo{ID: field.ID, Name: field.Name, Type: field.Type, Of: field.Of}
	}
	info.ExpireField = reply.ExpireField
	info.ExpirePending, info.ExpirePendingMore = reply.ExpirePending, reply.ExpirePendingMore
	for _, store := range reply.ExpireStores {
		info.ExpireStores = append(info.ExpireStores, ExpireStoreInfo{Field: store.Field, Path: store.Path})
	}
	for _, aggregate := range reply.Aggregates {
		info.Aggregates = append(info.Aggregates, AggregateInfo{Store: aggregate.Store, ID: aggregate.ID, Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By, State: aggregate.State})
	}
	return info
}

func headerKey(database, name string) string { return database + "\x00" + name }

func (c *Client) remember(info BucketInfo) BucketInfo {
	c.headerLock.Lock()
	defer c.headerLock.Unlock()
	c.headers[headerKey(info.Database, info.Name)] = info
	return info
}

func (c *Client) cachedHeader(database, name string) (BucketInfo, bool) {
	c.headerLock.Lock()
	defer c.headerLock.Unlock()
	info, ok := c.headers[headerKey(database, name)]
	return info, ok
}

// DescribeNode reads the facts of the node this connection is on: id, build, uptime, its
// state in the cluster, the cluster's size, epoch and leader, the default replication factor,
// whether it is serving and, when the server can read it, the machine. Any authenticated
// session may ask. A server older than the command refuses it with an INVALID_ARGUMENT *Error.
func (c *Client) DescribeNode(ctx context.Context) (NodeDescription, error) {
	reply, err := c.admin(ctx, wire.RingAdminRequest{Cmd: wire.AdminDescribeNode})
	if err != nil {
		return NodeDescription{}, err
	}
	node := reply.Node
	if node == nil {
		return NodeDescription{}, errors.New("ringclient: the server answered describe_node without the node. Check that the server is a hoarddb server of this version")
	}
	out := NodeDescription{NodeID: node.NodeID, Version: node.Version, UptimeMS: node.UptimeMS, MemberState: node.MemberState, Members: node.Members,
		Voters: node.Voters, Epoch: node.Epoch, Leader: node.Leader, DefaultRF: node.DefaultRF, Serving: node.Serving,
		StateRange: node.StateRange, RefusedPeers: node.RefusedPeers, ClusterVersion: node.ClusterVersion,
		PasswordKey: node.PasswordKey}
	if host := node.Host; host != nil {
		out.Host = &HostDescription{CPUs: host.CPUs, HostCPUs: host.HostCPUs, MemUsed: host.MemUsed, MemTotal: host.MemTotal, MemSource: host.MemSource}
		if host.Load != nil {
			out.Host.Load = &Load{One: host.Load.One, Five: host.Load.Five, Fifteen: host.Load.Fifteen}
		}
		if host.Process != nil {
			out.Host.Process = &ProcessMemory{Used: host.Process.Used, Limit: host.Process.Limit}
		}
	}
	return out, nil
}

// SeedingStatus reads the slot pulls of the node this connection is on: the ones placed,
// seeding, stalled and retired, ordered by database then slot. It needs the cluster-manager
// role; any other session gets the one PERMISSION_DENIED *Error of the admin commands.
func (c *Client) SeedingStatus(ctx context.Context, query SeedingQuery) (SeedingPage, error) {
	request := wire.RingAdminRequest{Cmd: wire.AdminSeedingStatus, Database: query.Database, Cursor: query.Page.Cursor}
	if query.Slot != nil {
		slot, err := seedingSlot(*query.Slot)
		if err != nil {
			return SeedingPage{}, err
		}
		request.Slot = &slot
	}
	if query.Page.Limit != 0 {
		request.Limit = &query.Page.Limit
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return SeedingPage{}, err
	}
	if reply.Seeding == nil {
		return SeedingPage{}, errors.New("ringclient: the server answered seeding_status without rows. Check that the server is a hoarddb server of this version")
	}
	page := SeedingPage{Rows: make([]SeedingRow, len(reply.Seeding.Rows)), More: reply.Seeding.More, Cursor: reply.Seeding.Cursor}
	for i, row := range reply.Seeding.Rows {
		page.Rows[i] = SeedingRow{Database: row.Database, Slot: int(row.Slot), State: row.State, Source: row.Source, BytesDone: row.BytesDone,
			BytesKnown: row.BytesKnown, Entries: row.Entries, Idle: time.Duration(row.IdleMS) * time.Millisecond, Refusal: row.Refusal}
	}
	return page, nil
}

// SeedingRetry restarts the pull of one slot of a database on the node this connection is on,
// from a fresh cut. A slot with no seeding record is a SEED_NOT_HELD *Error that names the fix.
func (c *Client) SeedingRetry(ctx context.Context, database string, slot int) error {
	return c.seedingAction(ctx, wire.AdminSeedingRetry, database, slot, false)
}

// SeedingForget removes the retired record of one slot and keeps its range, so the slot seeds
// from scratch the next time it is in the node's copy set. A record that is not retired is a
// SEED_HELD *Error and nothing changes; no record is SEED_NOT_HELD. A retired record that no
// other member has confirmed a copy of is also SEED_HELD, because this node may hold the only copy of
// the slot's data; anyway goes ahead with that forget.
func (c *Client) SeedingForget(ctx context.Context, database string, slot int, anyway bool) error {
	return c.seedingAction(ctx, wire.AdminSeedingForget, database, slot, anyway)
}

func (c *Client) seedingAction(ctx context.Context, cmd, database string, slot int, anyway bool) error {
	wireSlot, err := seedingSlot(slot)
	if err != nil {
		return err
	}
	reply, err := c.admin(ctx, wire.RingAdminRequest{Cmd: cmd, Database: database, Slot: &wireSlot, Anyway: anyway})
	if err != nil {
		return err
	}
	if reply.Seeding == nil {
		return fmt.Errorf("ringclient: the server answered %s without the slot. Check that the server is a hoarddb server of this version", cmd)
	}
	return nil
}

// seedingSlot checks a slot against the 8192 a database has, so a slot that does not fit the
// wire's int32 is refused instead of being cut down to another slot.
func seedingSlot(slot int) (int32, error) {
	if slot < 0 || slot > 8191 {
		return 0, fmt.Errorf("ringclient: slot %d is outside 0 to 8191. Send a slot 'status seeding' lists", slot)
	}
	return int32(slot), nil
}

// MembershipResult is what join and promote answer: the committed position of the change and the
// server's one-line account of it, which a front end prints as it is.
type MembershipResult struct {
	// Epoch and StateSeq are the committed position; a repeated join answers the position its
	// registration committed at.
	Epoch, StateSeq int64
	// Result is the server's line.
	Result string
}

// Join registers the running node id at the internode address addr as a member of the cluster
// this connection is on. witness asks for a witness and must agree with the node's own
// configuration, which is the truth. A join of a known id at the same address answers the same
// result again.
func (c *Client) Join(ctx context.Context, id, addr string, witness bool) (MembershipResult, error) {
	return c.membership(ctx, wire.RingAdminRequest{Cmd: wire.AdminJoin, NodeID: id, DataAddr: addr, Witness: witness})
}

// Promote makes member id a voter, if the cluster has a free voter slot. A member that already
// votes is answered with the facts and nothing changes.
func (c *Client) Promote(ctx context.Context, id string) (MembershipResult, error) {
	return c.membership(ctx, wire.RingAdminRequest{Cmd: wire.AdminPromote, NodeID: id})
}

// ReplaceNode gives the identity of member id, which the cluster holds down or dead, to the node
// now running at its own internode address addr with an empty ring directory: the node joins
// under its own id and pulls every pair it owns from the other copies. The server refuses it when
// id held the only copy of some pairs unless acceptLoss is true, which accepts that those
// documents are lost.
func (c *Client) ReplaceNode(ctx context.Context, id, addr string, acceptLoss bool) (MembershipResult, error) {
	return c.membership(ctx, wire.RingAdminRequest{Cmd: wire.AdminReplaceNode, NodeID: id, DataAddr: addr, AcceptLoss: acceptLoss})
}

func (c *Client) membership(ctx context.Context, request wire.RingAdminRequest) (MembershipResult, error) {
	reply, err := c.admin(ctx, request)
	if err != nil {
		return MembershipResult{}, err
	}
	if reply.Membership == nil {
		return MembershipResult{}, fmt.Errorf("ringclient: the server answered %s without a result. Check that the server is a hoarddb server of this version", request.Cmd)
	}
	return MembershipResult{Epoch: reply.Membership.Epoch, StateSeq: reply.Membership.StateSeq, Result: reply.Membership.Result}, nil
}
