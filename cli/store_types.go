package cli

import (
	"strings"

	"github.com/hoarddb/hoarddb-server/server"
)

// storeType is one row of the store-type table: how the parser and the hints
// spell it, the aliases it accepts, the protocol type it maps to, its help
// topic, and a one-line purpose. Everything that names a store type — the
// parser's bucketTypes map, the unknown-type error, the completion
// vocabulary, the help topics, and the hint text — is derived from this
// table, so a new store type is wired in exactly one place.
//
// A type has two spellings, and they are not interchangeable. `type: lifo`
// names the bucket's own engine; `Photos lifoStore` declares a per-document
// store field. The Store suffix is part of the field spelling only — the
// parser refuses `type: lifoStore` as an unknown bucket type — so both
// spellings come from this table rather than being written out twice.
type storeType struct {
	Keyword   string   // what the parser and the hints print: "btree"
	Aliases   []string // accepted spellings: "b+tree"
	Purpose   string   // one line, for the type list and the help footer
	HelpTopic string   // the `help <topic>` key
}

// storeTypes is the ordered list of the six store types. The order is the
// order the hints print them in.
var storeTypes = []storeType{
	{Keyword: "hash", HelpTopic: "hash", Purpose: "key → document (the default)"},
	{Keyword: "btree", Aliases: []string{"b+tree"}, HelpTopic: "btree", Purpose: "sorted keys, range scans"},
	{Keyword: "fifo", HelpTopic: "fifo", Purpose: "queue, oldest first"},
	{Keyword: "lifo", HelpTopic: "lifo", Purpose: "queue, newest first"},
	{Keyword: "heap", HelpTopic: "heap", Purpose: "priority order"},
	{Keyword: "blob", HelpTopic: "blob", Purpose: "binary values"},
}

// bucketTypes maps every accepted spelling (keyword or alias) to the wire
// bucket type. Deriving it from the table keeps the parser and the protocol
// from disagreeing about what a type spells like on the wire.
var bucketTypes = func() map[string]server.BucketType {
	byName := make(map[string]server.BucketType, len(storeTypes))
	for _, st := range storeTypes {
		byName[st.Keyword] = server.BucketType(st.Keyword)
		for _, alias := range st.Aliases {
			byName[alias] = byName[st.Keyword]
		}
	}
	return byName
}()

// storeTypeKeywords returns the canonical keywords in table order, the shape
// the unknown-type error and the hint list share.
func storeTypeKeywords() []string {
	keywords := make([]string, 0, len(storeTypes))
	for _, st := range storeTypes {
		keywords = append(keywords, st.Keyword)
	}
	return keywords
}

// storeTypeList is the pipe-separated keyword list the hints print, e.g.
// "hash | btree | fifo | lifo | heap | blob".
func storeTypeList() string {
	return strings.Join(storeTypeKeywords(), " | ")
}

// storeFieldName is the field spelling of one type: the keyword plus the Store
// suffix the declaration parser requires — "fifoStore", "lifoStore".
func (st storeType) storeFieldName() string {
	return st.Keyword + "Store"
}

// storeFieldNames returns every type's field spelling in table order. The
// declaration grammar (declare.go) accepts a store field only when the second
// word ends in "Store", so this list is what a hint must print when it wants a
// developer to declare one.
func storeFieldNames() []string {
	names := make([]string, 0, len(storeTypes))
	for _, st := range storeTypes {
		names = append(names, st.storeFieldName())
	}
	return names
}

// storeFieldList is the comma-separated field-spelling list the hints print,
// e.g. "hashStore, btreeStore, fifoStore, lifoStore, heapStore, blobStore".
// Table order is deliberate: it matches storeTypeList position for position, so
// `type: hash` and `hashStore` line up for a reader scanning both lines.
func storeFieldList() string {
	return strings.Join(storeFieldNames(), ", ")
}

// unknownBucketTypeError builds the parser's refusal for a type the table
// does not carry, naming the type and listing the valid ones from the table.
func unknownBucketTypeError(typeName string) error {
	return usagef("unknown bucket type %q — valid types: %s", typeName, strings.Join(storeTypeKeywords(), ", "))
}

// helpTopicList is every `help <topic>` argument in display order: the six
// store types from the table, then the non-store topics. It drives the
// completion vocabulary and the "Available topics:" line, so the two cannot
// drift apart.
func helpTopicList() []string {
	topics := make([]string, 0, len(storeTypes)+8)
	for _, st := range storeTypes {
		topics = append(topics, st.HelpTopic)
	}
	return append(topics, "claim", "placement", "fields", "batch", "user", "theme", "set", "operators", "config")
}
