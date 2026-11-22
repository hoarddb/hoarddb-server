package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file is the privilege check for ring requests. A ring request names a bucket by
// id, so the legacy gate (authorize, which reads a database name out of the payload)
// cannot see it; wire.NewRingHandler resolves the id and asks this authorizer with the
// database and bucket it found. There is no second permission model: each ring class
// is mapped to the legacy opcode that does the same thing, and needs exactly the action
// opRules gives that opcode, so a change to opRules moves the ring with it.

// ringLegacyOpcode is the legacy opcode each ring class stands for. A store verb is one
// class for push, pop, take and put; all four legacy store mutations are writes, which
// TestU70EveryRingClassNeedsWhatItsLegacyOpcodesNeed pins.
var ringLegacyOpcode = map[wire.RingClass]transport.Opcode{
	cluster.OpInsert:    transport.OpPut,
	cluster.OpUpdate:    transport.OpUpdate,
	cluster.OpUpsert:    transport.OpUpsert,
	cluster.OpDelete:    transport.OpDelete,
	cluster.OpStoreVerb: transport.OpFieldPush,
	cluster.OpClaim:     transport.OpQueueClaim,
	cluster.OpAck:       transport.OpQueueAck,
	cluster.OpRelease:   transport.OpQueueRelease,
	cluster.OpExtend:    transport.OpQueueExtend,
	wire.RingClassGet:   transport.OpGet,
	// A projected get reads the same document as get and needs the same action: naming fewer
	// fields is never a way around authorization.
	wire.RingClassGetFields: transport.OpGet,
	wire.RingClassPeek:      transport.OpFieldPeek,
	wire.RingClassStoreList: transport.OpFieldList,
	// a nearest reads every element of a field, as a list does
	wire.RingClassNearest: transport.OpFieldList,
	// an aggregate read shows a total over every element and, for sum by and count by, every distinct group
	// value of the store, as a list does, so it needs the list grant, not the peek grant
	wire.RingClassAggregate:  transport.OpFieldList,
	wire.RingClassStoreCount: transport.OpFieldLen,
	// A lease read shows what a claim holds, as a peek shows what a pop would take.
	wire.RingClassLeaseGet: transport.OpFieldPeek,
	// A keyed read of one element is the legacy read of a field element by key.
	wire.RingClassStoreGet: transport.OpFieldGet,
	wire.RingClassList:     transport.OpSearch,
	wire.RingClassCount:    transport.OpCount,
	// RING_ADMIN commands stand for the legacy opcodes that create and read the catalog.
	wire.RingClassCreateDatabase: transport.OpCreateDB,
	wire.RingClassCreateBucket:   transport.OpCreateBucket,
	// alter_bucket changes the catalog as the legacy alter bucket does: db-admin on the database.
	wire.RingClassAlterBucket: transport.OpAlterBucket,
	// drop_bucket deletes a bucket as the legacy drop bucket does: db-admin on the database.
	wire.RingClassDropBucket:     transport.OpDeleteBucket,
	wire.RingClassDescribeBucket: transport.OpBucketInfo,
	wire.RingClassShowBuckets:    transport.OpListBuckets,
	// describe_database lists the buckets of a database; show_databases is the legacy
	// database listing, which carries no action and filters by grant (Authorize).
	wire.RingClassDescribeDatabase: transport.OpListBuckets,
	wire.RingClassShowDatabases:    transport.OpListDBs,
	// RING_TOPOLOGY stands for the legacy list of databases: no action; a database name is
	// shown by identityHoldsGrantOn (authorizeTopology).
	wire.RingClassTopology: transport.OpListDBs,
	// describe_node is the legacy health request: no action, no scope, open to every user
	// (authorizeIdentity).
	wire.RingClassDescribeNode: transport.OpHealthCheck,
	// Membership commands stand for the legacy cluster add and remove (ring_authz_membership.go).
	wire.RingClassJoin:        transport.OpClusterAdd,
	wire.RingClassPromote:     transport.OpClusterAdd,
	wire.RingClassLeave:       transport.OpClusterRemove,
	wire.RingClassRemoveNode:  transport.OpClusterRemove,
	wire.RingClassReplaceNode: transport.OpClusterAdd,
	// Seeding is a property of the node, not of a database. The status lists what the node is
	// pulling, which the node listing (cluster-manager) also shows; retry and forget act on
	// pulls, as the legacy migrate does (cluster-manager). All three are ruled by
	// authorizeUnscoped, which asks the legacy rule for ActionClusterAdmin on "*".
	wire.RingClassSeedingStatus: transport.OpListNodes,
	wire.RingClassSeedingRetry:  transport.OpMigrate,
	wire.RingClassSeedingForget: transport.OpMigrate,
	// The user commands stand for the legacy user opcodes, which opRules gives user-manager with no
	// database; all seven are ruled by authorizeUnscoped (ring_authz_users.go). This reads the
	// legacy table and adds no legacy opcode; the ring authorization table (legacy-deletion-build.md
	// 3.2, merge A) replaces it.
	wire.RingClassCreateUser:   transport.OpUserCreate,
	wire.RingClassAlterUser:    transport.OpUserAlter,
	wire.RingClassDropUser:     transport.OpUserDrop,
	wire.RingClassGrantRole:    transport.OpUserGrant,
	wire.RingClassRevokeRole:   transport.OpUserRevoke,
	wire.RingClassShowUsers:    transport.OpUserList,
	wire.RingClassDescribeUser: transport.OpUserGet,
}

// ringAction is the action a ring class needs on its database. A class with no legacy
// opcode, or whose opcode is not database-scoped, is an error: default deny.
func ringAction(class wire.RingClass) (Action, error) {
	op, ok := ringLegacyOpcode[class]
	if !ok {
		return ActionNone, fmt.Errorf("ring class 0x%02X has no authorization rule; refusing it (default deny)", class)
	}
	rule, ok := opRules[op]
	if !ok || rule.Scope != ScopeDatabase || rule.Action == ActionNone {
		return ActionNone, fmt.Errorf("ring class 0x%02X maps to %s, which is not a database-scoped privilege; refusing it (default deny)", class, op)
	}
	return rule.Action, nil
}

// RingAuthorizer returns the authorizer the ring handler (wire.NewRingHandler) asks.
func (s *Server) RingAuthorizer() wire.RingAuthorizer { return ringAuthorizer{server: s} }

type ringAuthorizer struct{ server *Server }

// Authorize applies the grants frozen on the session at AUTH, as the legacy gate does,
// with the differences a ring request needs: a context with no session is refused, not
// waved through, and a session on the internode listener is refused whatever it holds,
// because members forward client work inside a replication push and never as a ring
// frame. An empty access.Database asks whether any grant of the session carries the
// action. The error is logged and audited here (denial text, deduplicated) and is never
// shown to the client, which gets the handler's one PERMISSION_DENIED text.
func (a ringAuthorizer) Authorize(ctx context.Context, access wire.RingAccess) error {
	if access.Class == wire.RingClassShowDatabases {
		return a.mayListDatabase(ctx, access.Database)
	}
	if access.Class == wire.RingClassTopology {
		return authorizeTopology(ctx, access)
	}
	if access.Class == wire.RingClassDescribeNode {
		return authorizeIdentity(ctx)
	}
	if op, seeding := ringSeedingOpcode(access.Class); seeding {
		return a.authorizeUnscoped(ctx, ActionClusterAdmin, op)
	}
	if op, membership := ringMembershipOpcode(access.Class); membership {
		return a.authorizeUnscoped(ctx, ActionClusterAdmin, op)
	}
	if op, users := ringUserOpcode(access.Class); users {
		return a.authorizeUnscoped(ctx, ActionUserAdmin, op)
	}
	action, err := ringAction(access.Class)
	if err != nil {
		return err
	}
	identity, err := ringIdentity(ctx)
	if err != nil {
		return err
	}
	if identityAllows(identity, action, access.Database) {
		return nil
	}
	opcode := transport.OpRingKeyed
	if access.Class >= wire.RingClassCreateDatabase {
		opcode = transport.OpRingAdmin
	}
	a.server.recordDenial(identity.Username, action, access.Database, opcode, identity.Grants)
	message := fmt.Sprintf("permission denied: user %q lacks %q on %q", identity.Username, action.String(), displayResource(access.Database))
	if access.Database == "" {
		message = fmt.Sprintf("permission denied: user %q has no grant on any database that allows %q", identity.Username, action.String())
	}
	return &sentencedError{message: message, cause: ErrPermissionDenied}
}

// ringIdentity is the authenticated identity of the session in ctx. A context with no
// session is refused, not waved through, and so is a session on the internode listener.
func ringIdentity(ctx context.Context) (transport.Identity, error) {
	sess, ok := transport.SessionFromContext(ctx)
	if !ok || sess == nil {
		return transport.Identity{}, errors.New("ring request without a session: refusing it (default deny)")
	}
	if sess.Peer {
		return transport.Identity{}, errNotAClusterMember
	}
	identity, ok := sess.Identity()
	if !ok {
		return transport.Identity{}, &sentencedError{message: "permission denied: the connection has no authenticated identity", cause: ErrPermissionDenied}
	}
	return identity, nil
}

// mayListDatabase answers whether show_databases lists the database. The legacy listing has
// no action: it shows a database to any session holding any role on it, or on "*"
// (databasesVisibleTo). A database left out is a filter, not a denial, so nothing is audited
// or logged here; a session with no session or identity sees none.
func (a ringAuthorizer) mayListDatabase(ctx context.Context, database string) error {
	sess, ok := transport.SessionFromContext(ctx)
	if !ok || sess == nil || sess.Peer {
		return errors.New("show_databases without a client session: listing nothing")
	}
	identity, ok := sess.Identity()
	if !ok || !identityHoldsGrantOn(identity, database) {
		return ErrPermissionDenied
	}
	return nil
}

// authorizeTopology is the rule of RING_TOPOLOGY (owner, 2026-10-31): any authenticated
// session may read the members and the table, so the ask that names no database needs only
// an identity. A database name is tenant information: the ask that names one is answered
// yes by identityHoldsGrantOn, the one rule show_databases and the legacy list of databases
// share, so they can never disagree. A "no" is a filter, not a denial, so it is
// neither audited nor logged.
func authorizeTopology(ctx context.Context, access wire.RingAccess) error {
	identity, err := ringIdentity(ctx)
	if err != nil {
		return err
	}
	if access.Database == "" {
		return nil
	}
	if identityHoldsGrantOn(identity, access.Database) {
		return nil
	}
	return &sentencedError{message: "permission denied: the user holds no role on this database, so its name is not listed", cause: ErrPermissionDenied}
}

// authorizeIdentity is the rule of describe_node: the legacy health request carries no action
// and no scope ("status stays open to every user"), so any session with an identity may ask,
// and a context with no session, no identity or on the internode listener may not. The facts
// it returns name no database, bucket, user or other node's address.
func authorizeIdentity(ctx context.Context) error {
	_, err := ringIdentity(ctx)
	return err
}

// ringSeedingOpcode is the legacy node operation a seeding class stands for.
func ringSeedingOpcode(class wire.RingClass) (transport.Opcode, bool) {
	switch class {
	case wire.RingClassSeedingStatus, wire.RingClassSeedingRetry, wire.RingClassSeedingForget:
		return ringLegacyOpcode[class], true
	}
	return 0, false
}
