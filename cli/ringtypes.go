package cli

// Statement is one parsed ring statement: the grammar form's verb and its
// typed arguments in the order the form names them. Nothing here talks to a
// server; the statement unit executes a Statement.
type Statement struct {
	// Verb names the grammar form, for example "create bucket" or "insert".
	Verb string
	// Args are the typed arguments of the form, in grammar order.
	Args []Arg
}

// ArgKind says which fields of an Arg carry the value.
type ArgKind int

// The argument kinds a statement can carry.
const (
	// ArgText is a name, id, address, hex cursor or word held in Text.
	ArgText ArgKind = iota
	// ArgInt is a number held in Int.
	ArgInt
	// ArgFields is the field declaration list of a create bucket.
	ArgFields
	// ArgChanges is the add/drop list of an alter bucket.
	ArgChanges
	// ArgDoc is a document literal held in Doc.
	ArgDoc
	// ArgValues is the comma-separated literal list of a store verb.
	ArgValues
	// ArgFlag is a switch that was present, such as --accept-loss.
	ArgFlag
)

// Arg is one typed argument of a Statement.
type Arg struct {
	// Name is the grammar's name for the argument, such as "database".
	Name string
	// Kind says which of the fields below carry the value.
	Kind ArgKind
	// Text carries a name, id, address, hex cursor or word.
	Text string
	// Int carries a number.
	Int int64
	// Fields carries the declared fields of a create bucket.
	Fields []FieldDecl
	// Changes carries the edits of an alter bucket.
	Changes []FieldChange
	// Doc carries a document literal.
	Doc Value
	// Values carries the arguments of a store verb.
	Values []Value
	// Implied is true when the argument was not written and came from the
	// selected database ('use <db>').
	Implied bool
	// Indexes carries the index entries of a create bucket's fields, or the add index entries of an alter bucket.
	Indexes []IndexDecl
	// DropIndexes carries the names of the drop index entries of an alter bucket.
	DropIndexes []string
	// ExpireField is the datetime field of `expire at <Field>` in a create bucket, or of
	// `set expire at <Field>` in an alter bucket; empty when none was written.
	ExpireField string
	// DropExpire is true for `drop expire at` in an alter bucket.
	DropExpire bool
	// Span is the byte range, in the parsed line, of the free-form text of a
	// fields, changes, document, values or address argument (between the
	// delimiters, for the braced kinds); zero for the other kinds.
	Span [2]int
}

// AggregateDecl is one aggregate a store field declares in its with (...) clause.
// Kind is "sum", "sum by" or "count by"; Field is empty for count by and By is
// empty for sum.
type AggregateDecl struct {
	// Kind says which of the three aggregate forms this is.
	Kind string
	// Field is the summed path, empty for count by.
	Field string
	// By is the grouping path, empty for sum.
	By string
}

// FieldDecl is one declared field of a create bucket.
type FieldDecl struct {
	// Name is the field name.
	Name string
	// Type is the canonical type word: a scalar word or one of the six store words.
	Type string
	// Aggregates are the with (...) aggregates of a store field, in the order written.
	Aggregates []AggregateDecl
	// ExpirePath is the BSON key path of `expire at "<path>"` on a fifoStore or lifoStore field.
	ExpirePath string
	// Of is the bucket a field is declared of, "<Bucket>" or "<Database>.<Bucket>"; empty for none.
	Of string
}

// IndexDecl is one `index <Name> ( <Field> [desc], … )` entry of a create or alter body.
type IndexDecl struct {
	// Name is the index name.
	Name string
	// Components are the index's fields, in the order written, one to four.
	Components []IndexComponentDecl
}

// IndexComponentDecl is one field of an index, with its direction.
type IndexComponentDecl struct {
	// Field is the field name.
	Field string
	// Desc is true for a descending field, written 'desc'.
	Desc bool
}

// FieldChange is one edit of an alter bucket.
type FieldChange struct {
	// Add is true for 'add <Field> <type>' and false for 'drop <Field>'.
	Add bool
	// Name is the field name.
	Name string
	// Type is the canonical type word of an add; empty for a drop.
	Type string
}

// ValueKind says which field of a Value carries its content.
type ValueKind int

// The literal kinds a document or a store verb argument can hold.
const (
	// ValueString is a double-quoted string held in Text.
	ValueString ValueKind = iota
	// ValueInt is an integer held in Int.
	ValueInt
	// ValueFloat is a number with a fraction or an integer too large for Int, held in Float.
	ValueFloat
	// ValueBool is true or false, held in Bool.
	ValueBool
	// ValueNull is null.
	ValueNull
	// ValueDoc is a document, held in Entries in the order written.
	ValueDoc
	// ValueArray is an array, held in Elems.
	ValueArray
	// ValueBinary is a BSON binary held in Bytes and Subtype.
	ValueBinary
)

// Value is a parsed literal: the content of a document or a store verb argument.
type Value struct {
	// Kind says which of the fields below carry the content.
	Kind ValueKind
	// Text carries a string.
	Text string
	// Int carries an integer.
	Int int64
	// Float carries a fraction.
	Float float64
	// Bool carries true or false.
	Bool bool
	// Entries carry a document's keys and values in the order written.
	Entries []DocEntry
	// Elems carry an array's elements.
	Elems []Value
	// Bytes carries a binary's content and Subtype its BSON subtype byte.
	Bytes   []byte
	Subtype byte
}

// DocEntry is one key and value of a document literal.
type DocEntry struct {
	// Key is the key as written, without quotes.
	Key string
	// Value is the entry's value.
	Value Value
}

// ParseError is a refusal of a statement's text. Its message names the thing
// that is wrong and the exact statement that works.
type ParseError struct {
	// Pos is the 1-based character position of the problem; 0 when the
	// statement as a whole is wrong.
	Pos int
	// Message is the full text, position and fix included, on one line: what a
	// piped run prints.
	Message string
	// Problem, Form, Example and Topic are the parts of Message that a prompt
	// session prints on lines of their own. They are empty for a refusal that
	// has no form to quote.
	Problem, Form, Example, Topic string
	// Suggestion is the statement word a mistyped first word was close to.
	Suggestion string
}

// Error returns the message.
func (e *ParseError) Error() string { return e.Message }

// NameSource answers the names completion offers after a verb. The statement
// unit fills it from the cluster; a nil source offers only the grammar's words.
type NameSource interface {
	// Databases lists the database names.
	Databases() []string
	// Buckets lists the bucket names of a database.
	Buckets(database string) []string
	// Fields lists every declared field of a bucket.
	Fields(database, bucket string) []string
	// StoreFields lists the declared store fields of a bucket.
	StoreFields(database, bucket string) []string
	// Nodes lists the member node ids.
	Nodes() []string
}
