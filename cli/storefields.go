package cli

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A bucket's store fields are a property of the bucket, and a document's own
// bytes never mention them: the stores sit beside the document and are read
// through db.<bucket>["<key>"].<Field>. So a document printed on its own left a
// reader unable to see that it has stores at all.
//
// On a terminal a document read out of a bucket now names them — `"Photos":
// "lifoStore"` — and prints _id_ first. Nothing else changes: when stdout or
// stdin is not a terminal, or TERM cannot take the display, every document
// prints byte for byte as it always has, because a script parses it.
//
// The display costs one BUCKET_INFO per bucket per session. That reply is
// answered without walking a store (O(declared stores), whatever the stores
// hold), it is cached for the session, and it is forgotten when a statement
// that can change a declaration runs. It never fails a read: if the call
// cannot be answered the document prints undecorated, and the failure is
// remembered too.
//
// The cache serves the display and nothing else. The lookups a statement makes
// before it runs (the bucket's type for get and for a push with a numeric
// priority, the engine of a top-level store field) each ask one fresh
// BUCKET_INFO and never read it (BUG-0520): a cached type is wrong, with no
// server refusal to notice, when another session recreates a hash as a blob (the
// server answers the get with the blob's metadata) or a fifo as a hash (the CLI
// refuses the get itself and never asks). A stale display is cosmetic; a stale
// route is not. They asked BUCKET_STATS before, which counts the elements of
// every store field by walking them, and BUCKET_INFO walks none. Their fresh
// answer does refresh this cache, so the display never sees an older one.

// storeField is one store a bucket declares on each of its documents.
type storeField struct {
	name   string
	engine string
}

// documentDecoration is what a terminal adds to a bucket's documents.
type documentDecoration struct {
	stores []storeField
}

// cachedDeclaration is what the session learned about one bucket from one
// BUCKET_INFO: its type and its top-level store fields. The lookups of a
// statement and the terminal display both read it, so a bucket costs one call
// per session, not one per use.
type cachedDeclaration struct {
	bucketType server.BucketType
	stores     []server.StoreFieldInfo
	// notFound is a reply that says the bucket is not there (found: false).
	notFound bool
	// failure is why the call could not be answered: no read grant, a timeout,
	// a bucket missing as an ERROR frame. It is the error as send returned it,
	// so a statement that has to report it prints what it always printed.
	failure error
	// answeredByStats is set when the server did not know BUCKET_INFO and the
	// declaration came from BUCKET_STATS instead (see readDeclaration). The
	// display stays off for it, as it was on such a server before.
	answeredByStats bool
	// infoUnsupported remembers that the server does not know BUCKET_INFO, so a
	// later statement goes straight to the fallback.
	infoUnsupported bool
	// decoration is what the display adds, built once from stores.
	decoration *documentDecoration
}

// usable says the declaration answers the type and engine lookups.
func (d *cachedDeclaration) usable() bool { return d.failure == nil && !d.notFound }

// documentDecorationAllowed is the gate: a terminal at both ends, and a TERM
// that can take the display. It is the test the banner and the pager apply.
func documentDecorationAllowed(stdoutIsTTY, stdinIsTerminal, termCanTakeEscapes bool) bool {
	return stdoutIsTTY && stdinIsTerminal && termCanTakeEscapes
}

// SetDocumentDecoration turns the terminal display of a document's declared
// stores on or off.
func (h *CommandHandler) SetDocumentDecoration(on bool) {
	h.decorate = on
}

// engineDisplayWord spells an engine as a declaration does. The wire carries
// the bare catalog word ("lifo"); the table (store_types.go) owns the suffix. A
// word the table does not know — a newer server's — prints as it was sent.
func engineDisplayWord(engine string) string {
	for _, st := range storeTypes {
		if st.Keyword == engine {
			return st.storeFieldName()
		}
	}
	return engine
}

// forgetsDeclarations says whether a statement can change what a bucket
// declares or whether it exists, or move the session to another server. It is a
// list of the statements known to leave declarations alone: anything else,
// including a verb this list has not heard of, forgets.
func forgetsDeclarations(cmd *Cmd) bool {
	switch cmd.Type {
	case "bucket":
		switch cmd.Method {
		case "insert", "upsert", "find", "get", "delete", "search", "push", "pop", "peek", "update",
			"claim", "ack", "release", "extend", "dead", "requeue", "discard",
			"len", "length", "count", "distinct", "put", "info", "stream", "range", "first", "last", "stats":
			return false
		}
	case "global":
		switch cmd.Method {
		case "use", "show", "describe", "status", "encryption", "nodes",
			"write-concern", "consistent-get", "master", "help":
			return false
		}
	}
	return true
}

// bucketInfoOpcode is how a server writes the opcode of BUCKET_INFO in the
// error it answers when it does not know the call.
var bucketInfoOpcode = fmt.Sprintf("opcode 0x%02X", uint8(transport.OpBucketInfo))

// isUnsupportedOpcode reports whether err is a server saying it does not know
// BUCKET_INFO. A server older than the call says so in one of two voices, and
// it is the authorization layer's that a real one uses, because it rejects an
// opcode it has no rule for before the dispatcher is reached:
//
//	no authorization rule is declared for opcode 0xD9 "UNKNOWN"; refusing the frame (default deny)
//	unsupported opcode 0xD9 (request id 1): this server does not implement it
//
// Both name the opcode, and the match requires it: a permission error, a
// missing bucket, a timeout and another opcode's refusal are not an older
// server. TestAnOlderServersWordsAreRecognisedExactly pins the wording.
func isUnsupportedOpcode(err error) bool {
	var serverErr *ServerError
	if !errors.As(err, &serverErr) || !strings.Contains(serverErr.Message, bucketInfoOpcode) {
		return false
	}
	return strings.Contains(serverErr.Message, "no authorization rule is declared") ||
		strings.Contains(serverErr.Message, "unsupported opcode")
}

// fetchInfo asks the server for a bucket's declaration with one BUCKET_INFO.
func (h *CommandHandler) fetchInfo(database, bucket string) (*server.BucketInfoResponse, error) {
	h.declarationFetches++
	if h.declarationSource != nil {
		return h.declarationSource(database, bucket)
	}
	var resp server.BucketInfoResponse
	req := &server.BucketInfoRequest{Database: database, Bucket: bucket}
	if err := h.send(transport.OpBucketInfo, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// readDeclaration asks the server about one bucket. withFallback lets it ask
// BUCKET_STATS when the server does not know BUCKET_INFO, which a statement
// that needs the type must do and the display, which only decorates, must not:
// the fallback walks the stores, and a display that cost that would be
// a regression on exactly the servers that cannot afford it.
func (h *CommandHandler) readDeclaration(database, bucket string, withFallback, infoUnsupported bool) *cachedDeclaration {
	declaration := &cachedDeclaration{infoUnsupported: infoUnsupported}
	if !infoUnsupported {
		resp, err := h.fetchInfo(database, bucket)
		switch {
		case err == nil && resp.Found:
			declaration.bucketType = resp.Bucket.Type
			declaration.stores = resp.StoreFields
			return declaration
		case err == nil:
			declaration.notFound = true
			return declaration
		case !isUnsupportedOpcode(err):
			declaration.failure = err
			return declaration
		}
		declaration.infoUnsupported = true
	}
	if !withFallback {
		declaration.failure = errors.New("the server does not answer BUCKET_INFO")
		return declaration
	}
	return h.readDeclarationFromStats(database, bucket, declaration)
}

// readDeclarationFromStats is the lookup an older server needs: BUCKET_STATS
// names the type and every store field, nested ones included, at the cost of
// walking them. The top-level stores are kept, as BUCKET_INFO names them.
func (h *CommandHandler) readDeclarationFromStats(database, bucket string, declaration *cachedDeclaration) *cachedDeclaration {
	var stats server.BucketStatsResponse
	if err := h.send(transport.OpBucketStats, &server.BucketStatsRequest{Database: database, Bucket: bucket}, &stats); err != nil {
		declaration.failure = err
		return declaration
	}
	if !stats.Found {
		declaration.notFound = true
		return declaration
	}
	declaration.bucketType = stats.Type
	declaration.answeredByStats = true
	for _, store := range stats.Stores {
		if store.Parent == "" {
			declaration.stores = append(declaration.stores, server.StoreFieldInfo{Name: store.Name, Engine: store.Engine})
		}
	}
	return declaration
}

// displayDeclaration is the session's cached declaration of a bucket, for the
// display: asked once, and a failure remembered so it is not retried on every
// read. It never falls back to BUCKET_STATS, which would add the walk on
// exactly the servers that cannot take it.
func (h *CommandHandler) displayDeclaration(database, bucket string) *cachedDeclaration {
	key := scanTarget{database: database, bucket: bucket}
	if known, asked := h.declarations[key]; asked {
		return known
	}
	declaration := h.readDeclaration(database, bucket, false, false)
	if h.declarations == nil {
		h.declarations = make(map[scanTarget]*cachedDeclaration)
	}
	h.declarations[key] = declaration
	return declaration
}

// freshDeclaration asks the server now and never reads the cache: the lookup
// every statement that routes or stores by type or engine makes (owner decision
// 2026-10-12, option Z). The fresh answer replaces the cache entry, because it
// is newer than what is there: the display can only see it fresher, never
// staler. A failed lookup drops the entry instead of leaving an answer the
// server just contradicted.
//
// On a server that does not know BUCKET_INFO the lookup asks BUCKET_STATS, every
// time, for every path including get, as the CLI did before BUCKET_INFO; only
// the refused BUCKET_INFO is remembered (infoUnsupported), so it is not asked
// again for that bucket until a statement forgets declarations.
func (h *CommandHandler) freshDeclaration(database, bucket string) *cachedDeclaration {
	key := scanTarget{database: database, bucket: bucket}
	known, asked := h.declarations[key]
	declaration := h.readDeclaration(database, bucket, true, asked && known.infoUnsupported)
	if !declaration.usable() {
		delete(h.declarations, key)
		return declaration
	}
	if h.declarations == nil {
		h.declarations = make(map[scanTarget]*cachedDeclaration)
	}
	h.declarations[key] = declaration
	return declaration
}

// decorationFor is what to add to a document read out of the bucket, or nil to
// print it as it is: the gate is closed, or the declaration could not be read.
// The decoration shows a report the user asked for, so a failure is not an
// error and has no line of its own.
func (h *CommandHandler) decorationFor(database, bucket string) *documentDecoration {
	if !h.decorate {
		return nil
	}
	declaration := h.displayDeclaration(database, bucket)
	if !declaration.usable() || declaration.answeredByStats {
		return nil
	}
	if declaration.decoration == nil {
		declaration.decoration = &documentDecoration{stores: make([]storeField, 0, len(declaration.stores))}
		for _, store := range declaration.stores {
			declaration.decoration.stores = append(declaration.decoration.stores, storeField{name: store.Name, engine: engineDisplayWord(store.Engine)})
		}
	}
	return declaration.decoration
}

// bucketDoc renders one of a bucket's own documents: find, get, search, range,
// first/last, a consistent get, an update's result. It is not used for a store
// field's elements or a queue's pop: those are elements, not documents of the
// bucket, and the bucket's declarations do not belong on them.
func (h *CommandHandler) bucketDoc(document interface{}, database, bucket string) string {
	return h.renderDocument(document, h.decorationFor(database, bucket))
}

// printBucketDocuments is printDocuments for a bucket's own documents.
func (h *CommandHandler) printBucketDocuments(docs []bson.Raw, database, bucket string) {
	h.printDocumentsWith(docs, h.decorationFor(database, bucket))
}

// decoratedLine renders a document with its bucket's declared stores: _id_
// first, the document's own fields sorted, then the stores in declaration
// order. A store whose name the document already holds as a field is not added:
// the document's own value prints, in its place among the fields.
func decoratedLine(doc interface{}, useExtJSON bool, decoration *documentDecoration) string {
	fields, early := decodedDocument(doc)
	if early != "" {
		return early
	}
	var added []string
	for _, store := range decoration.stores {
		if _, held := fields[store.name]; !held {
			fields[store.name] = store.engine
			added = append(added, store.name)
		}
	}
	if useExtJSON {
		// The driver marshals Extended JSON and orders its own keys.
		return jsonLineExtJSONImpl(fields)
	}

	isAdded := make(map[string]bool, len(added))
	for _, name := range added {
		isAdded[name] = true
	}
	own := make([]string, 0, len(fields))
	for key := range fields {
		if key != idFieldName && !isAdded[key] {
			own = append(own, key)
		}
	}
	sort.Strings(own)
	keys := make([]string, 0, len(fields))
	if _, hasID := fields[idFieldName]; hasID {
		keys = append(keys, idFieldName)
	}
	keys = append(keys, own...)
	keys = append(keys, added...)
	return objectLine(fields, keys)
}

// idFieldName is the document's own key field, printed first on a terminal:
// a document is addressed by it.
const idFieldName = "_id_"
