package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
)

// The local declaration read of a boot replay (the deferred pair gate spec section 5,
// invariant G-5). A pair deferred at a restart answers no peer with its position, so the node
// that took over a declaration's slot can wait for this node while this node's replay waits
// for that node's answer, which its store lacks. When the declaration lives in the very pair
// being replayed it is on this node: the replay applies the pair's records in log order, so
// the store holds every change of the document that precedes the record. Only that pair:
// a declaration in another pair this node holds may be behind the owner's.

// pairEngineSource is the face of a FieldEngineSource that can answer for one record of a boot
// replay, knowing the pair the record belongs to.
type pairEngineSource interface {
	// inPair returns the source an apply of rec reads its declarations through.
	inPair(rec AppliedRecord) FieldEngineSource
}

var _ pairEngineSource = declaredEngines{}

// inPair implements pairEngineSource.
func (e declaredEngines) inPair(rec AppliedRecord) FieldEngineSource {
	return replayEngines{engines: e, slot: rec.Slot, db: rec.DB, declVersion: rec.Op.DeclVersion}
}

// replayEngines is declaredEngines for the apply of one record of a boot replay of pair (slot,
// db). It is made per record and lives for that apply only, so nothing of it outlasts the record
// or is seen by an apply of another pair.
type replayEngines struct {
	engines     declaredEngines
	slot        uint16
	db          uint64
	declVersion int64
}

var (
	_ FieldEngineSource = replayEngines{}
	_ ExpireTableSource = replayEngines{}
	_ expireNames       = replayEngines{}
)

// FieldEngine implements FieldEngineSource.
func (r replayEngines) FieldEngine(db, bucketID uint64, fieldID uint16) (store.FieldEngine, error) {
	schemaView, err := r.view(db, bucketID)
	if err != nil {
		return 0, err
	}
	return fieldEngineOf(schemaView, db, bucketID, fieldID)
}

// ExpireTable implements ExpireTableSource.
func (r replayEngines) ExpireTable(db, bucketID uint64) (machine.ExpireTable, error) {
	schemaView, err := r.view(db, bucketID)
	if err != nil {
		return machine.ExpireTable{}, err
	}
	return machine.EffectiveExpire(schemaView.Decl, schemaView.Published), nil
}

// ExpireNames implements expireNames; it reads the cache only, as declaredEngines does.
func (r replayEngines) ExpireNames(db, bucketID uint64, fieldID uint16) (database, bucket, field string, ok bool) {
	return r.engines.ExpireNames(db, bucketID, fieldID)
}

// view is declaredEngines.view, and when that read could not be answered (ErrDeclUnavailable: the
// slot's owner answered not found without a drop proven here, or the read failed) the
// declaration from this node's own store, if it lives in the pair being replayed. A drop is
// never decided from the local document: only the routed read and the drop's own record do.
func (r replayEngines) view(db, bucketID uint64) (SchemaView, error) {
	routed, err := r.engines.view(db, bucketID)
	if err == nil || !errors.Is(err, ErrDeclUnavailable) || db != r.db {
		return routed, err
	}
	node := r.engines.node
	ctx, cancel := context.WithTimeout(node.stopCtx, bootTimeout)
	defer cancel()
	entry, held, localErr := node.fetcher.readInPair(ctx, pairReadRequest{
		db: db, bucketID: bucketID, slot: r.slot,
		local: NewLocalDeclarationReader(node.kv, node.cfg.NodeID, node.ringAdvice()), self: cluster.Member{ID: node.cfg.NodeID},
	})
	if !held {
		return SchemaView{}, err
	}
	if localErr != nil {
		return SchemaView{}, fmt.Errorf("%w; the declaration lives in slot %d, the pair being replayed, and this node's own store does not settle it either: %w", err, r.slot, localErr)
	}
	published := node.fetcher.cfg.Published(db)
	local := SchemaView{Present: true, Decl: entry.Decl, Name: entry.Name, Seen: published, Published: published}
	if entry.Decl.State == machine.StateDropping {
		return SchemaView{}, fmt.Errorf("%w; this node's own copy of the declaration, in slot %d, the pair being replayed, says the bucket is being dropped, and only the routed read decides a drop", err, r.slot)
	}
	if _, version := machine.EffectiveTable(local.Decl, local.Published); version < r.declVersion {
		return SchemaView{}, fmt.Errorf("%w; this node's own copy of the declaration, in slot %d, the pair being replayed, is at version %d and the record was written under %d", err, r.slot, version, r.declVersion)
	}
	return local, nil
}

// pairReadRequest is one local declaration read for a record of the pair (slot, db).
type pairReadRequest struct {
	db, bucketID uint64
	slot         uint16
	// local reads this node's own store; self is this node, the owner local answers for.
	local DeclarationReader
	self  cluster.Member
}

// readInPair reads the declaration of a bucket id the way Refetch does, except that each of its
// two documents is read from this node's own store when it lives in the request's slot:
//
//   - the reverse-index document (id to name), placed by the id: read locally in the slot, else
//     routed as Refetch reads it; a name already cached is used as Refetch uses it;
//   - the declaration, placed by the name: read locally in the slot, and never otherwise.
//
// held is false when the declaration does not live in the slot, when no read named the bucket,
// or when a placement cannot be computed yet (the database's name is in the committed state): the
// rule does not apply and the caller keeps the routed read's answer. With held true, err says why the local document does not
// settle the record: not found, which proves nothing, or a document of another bucket id. Nothing
// read here is cached.
func (f *Fetcher) readInPair(ctx context.Context, req pairReadRequest) (entry machine.Entry, held bool, err error) {
	name, named := f.nameInPair(ctx, req)
	if !named {
		return machine.Entry{}, false, nil
	}
	encID, err := store.EncodeComponent(name)
	if err != nil {
		return machine.Entry{}, false, fmt.Errorf("encoding bucket name %q: %w", name, err)
	}
	read, err := f.placement(req.db, encID)
	if err != nil || read.Slot != req.slot {
		return machine.Entry{}, false, nil
	}
	stored, found, err := req.local.ReadDeclaration(ctx, req.self, read)
	if err != nil {
		return machine.Entry{}, true, fmt.Errorf("reading the declaration of bucket %q from this node's store: %w", name, err)
	}
	if !found {
		return machine.Entry{}, true, fmt.Errorf("this node's store holds no declaration of bucket %q, which proves nothing about the bucket", name)
	}
	decl, err := store.DecodeDeclaration(stored)
	if err != nil {
		return machine.Entry{}, true, fmt.Errorf("decoding the declaration of bucket %q from this node's store: %w", name, err)
	}
	if decl.BucketID != req.bucketID {
		return machine.Entry{}, true, fmt.Errorf("this node's store declares bucket %q with id %d, not %d", name, decl.BucketID, req.bucketID)
	}
	return machine.Entry{DBID: req.db, Name: name, Decl: decl}, true, nil
}

// nameInPair is the name of req's bucket id: from the cache, else from the reverse-index document,
// read locally when it lives in req's slot and routed otherwise. named is false when no read gave
// a name; the routed read that failed before has already said why.
func (f *Fetcher) nameInPair(ctx context.Context, req pairReadRequest) (name string, named bool) {
	if entry, ok := f.peekByID(req.db, req.bucketID); ok {
		return entry.Name, true
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], req.bucketID)
	encID, err := store.EncodeComponent(raw[:])
	if err != nil {
		return "", false
	}
	read, err := f.placement(req.db, encID)
	if err != nil {
		return "", false
	}
	var found bool
	if read.Slot == req.slot {
		name, found, err = req.local.ReadBucketName(ctx, req.self, read)
	} else {
		name, found, err = f.readName(ctx, req.db, req.bucketID)
	}
	return name, err == nil && found
}
