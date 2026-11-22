package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// RingSession runs ring statements against a RingAPI. It holds the database
// 'use' selected, nothing else: every effect is
// one request to the server.
type RingSession struct {
	api      RingAPI
	selected string
	mintID   func() (string, error)
	// pretty, extJSON and noPager are the output settings 'set' changes and the
	// -pretty, -extjson and -no-pager flags start with, as in v1.0.
	pretty, extJSON, noPager bool
	theme                    *Theme
	// script reads the next line of a piped run, where a new password follows its statement; nil at a terminal.
	script func() (string, error)
	// newPassword is where a user statement takes a new password from besides the script;
	// promptPassword asks for one at the terminal, twice, with no echo.
	newPassword    ringPasswordSource
	promptPassword func() (string, error)
	// tookPassword is set by a statement that read a new password, for the line after it.
	tookPassword bool
	input        io.Reader
	interactive  bool
	// address and user are the connection's own facts, shown in the banner; clock is time.Now except in tests.
	address, user string
	clock         func() time.Time
}

// RingOption sets up a session at construction.
type RingOption func(*RingSession)

// WithRingPretty starts the session with documents printed on several lines (-pretty).
func WithRingPretty(on bool) RingOption { return func(s *RingSession) { s.pretty = on } }

// WithRingExtJSON starts the session printing documents as Extended JSON (-extjson).
func WithRingExtJSON(on bool) RingOption { return func(s *RingSession) { s.extJSON = on } }

// WithRingNoPager starts the session with paging off (-no-pager).
func WithRingNoPager() RingOption { return func(s *RingSession) { s.noPager = true } }

// WithRingTheme gives the session the prompt theme and error label of v1.0.
func WithRingTheme(theme *Theme) RingOption { return func(s *RingSession) { s.theme = theme } }

// WithRingInput names the terminal input the pager takes its keys from.
func WithRingInput(input io.Reader) RingOption { return func(s *RingSession) { s.input = input } }

// NewRingSession returns a session on api with no database selected. Without
// WithRingTheme the prompt is the plain one, so tests do not depend on a home directory.
func NewRingSession(api RingAPI, options ...RingOption) *RingSession {
	s := &RingSession{api: api, mintID: ringMintID, theme: resolveTheme("plain", false), promptPassword: ringPromptPassword,
		newPassword: ringPasswordSource{warnings: io.Discard}}
	for _, option := range options {
		option(s)
	}
	return s
}

// Selected is the database 'use' chose, or empty.
func (s *RingSession) Selected() string { return s.selected }

// ringMintID makes a document id for an insert that has none: a random UUID
// (version 4) in text, so two clients never collide and the id sorts nowhere
// in particular (SPEC-GAP U52-2: the client mints, the server only checks).
func ringMintID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("making a document id: reading random bytes: %w. Give the insert an id yourself: db.<bucket>.insert({_id_: \"<id>\", …})", err)
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:]), nil
}

// Execute parses and runs one statement. A failure is a *RingError carrying
// the text and the exit status; 'exit' and 'quit' return ErrExit.
func (s *RingSession) Execute(ctx context.Context, line string) (RingOutput, error) {
	switch strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ";"))) {
	case "exit", "quit":
		return RingOutput{}, ErrExit
	}
	afterPassword := s.tookPassword
	s.tookPassword = false
	statement, err := Parse(line, s.selected)
	if err != nil {
		if afterPassword && s.script != nil {
			return RingOutput{}, s.strayPasswordLine()
		}
		return RingOutput{}, ringParseFailure(line, err)
	}
	out, err := s.run(ctx, statement)
	if err != nil {
		return out, ringFailure(err)
	}
	return out, nil
}

// ringParseFailure renders a refused statement. A line the legacy engine
// would have taken gets the sentence that says where it went.
func ringParseFailure(line string, err error) *RingError {
	message := err.Error()
	if _, legacy := ParseCommand(line); legacy == nil && !ringStartsStatement(line) {
		message += " (the CLI does not run this; 'help' lists the statements it runs)"
	}
	return &RingError{Code: "PARSE_ERROR", Message: message, Exit: ExitRefused, Err: err}
}

// ringStartsStatement reports whether the line's first word begins a ring statement.
func ringStartsStatement(line string) bool {
	first := strings.ToLower(strings.TrimRight(strings.Fields(line + " x")[0], ".["))
	if cut := strings.IndexAny(first, ".["); cut >= 0 {
		first = first[:cut]
	}
	// db.<bucket>... is spelled in both engines, so a refused db line may be a
	// legacy spelling and still gets the hint; every other shared word is a
	// ring statement the user mistyped.
	for _, form := range ringForms {
		if form.Pattern[0].text == first && first != "db" {
			return true
		}
	}
	return false
}

// ringFailure turns any error of a statement into a *RingError: a server
// refusal keeps its code, text and the exit status of conventions Errors; a
// lost connection or a timeout is ExitUnavailable (the outcome is unknown);
// everything else the client refused before sending is ExitRefused.
func ringFailure(err error) *RingError {
	var failed *RingError
	if errors.As(err, &failed) {
		return failed
	}
	var refusal *ringclient.Error
	if errors.As(err, &refusal) {
		return &RingError{Code: refusal.Code.String(), Message: refusal.Message, Exit: refusal.Code.Exit(), Err: err}
	}
	message := strings.TrimPrefix(err.Error(), "ringclient: ")
	var network net.Error
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &network) {
		return &RingError{Message: message, Exit: ExitUnavailable, Err: err}
	}
	return &RingError{Message: message, Exit: ExitRefused, Err: err}
}

// refusal builds a local refusal that names the fix.
func ringRefusal(code, format string, args ...any) *RingError {
	return &RingError{Code: code, Message: fmt.Sprintf(format, args...), Exit: ExitRefused}
}

func (s *RingSession) run(ctx context.Context, st Statement) (RingOutput, error) {
	switch st.Verb {
	case "create database":
		return s.createDatabase(ctx, st)
	case "create bucket":
		return s.createBucket(ctx, st)
	case "alter database":
		if _, storage := st.Arg("storage"); storage {
			return RingOutput{}, ringRefusal("", "%s", alterStorageRefusal(ringText(st, "database")))
		}
	case "alter bucket":
		return s.alterBucket(ctx, st)
	case "drop bucket":
		return s.dropBucket(ctx, st)
	case "describe bucket":
		return s.describeBucket(ctx, st)
	case "show buckets":
		return s.showBuckets(ctx, st)
	case "use":
		return s.use(ctx, st)
	case "help":
		return ringHelp(st)
	case "set":
		return s.set(st)
	case "show databases":
		return s.showDatabases(ctx, st)
	case "show banner":
		return s.showBanner(ctx)
	case "nodes":
		return s.nodes(ctx)
	case "status":
		return s.clusterStatus(ctx)
	case "describe database":
		return s.describeDatabase(ctx, st)
	case "status seeding":
		return s.statusSeeding(ctx, st)
	case "seeding retry", "seeding forget":
		return s.seedingAction(ctx, st)
	case "join", "promote":
		return s.membership(ctx, st)
	case "replace-node":
		return s.replaceNode(ctx, st)
	case "create user", "alter user", "alter user roles", "drop user", "grant role", "revoke role", "show users", "user list", "user describe":
		return s.userStatement(ctx, st)
	case "get", "insert", "upsert", "update", "delete", "list", "count", "field":
		return s.keyed(ctx, st)
	}
	return RingOutput{}, ringRefusal("", "%s is not available yet: the ring wire carries create database, create bucket, "+
		"the show and describe statements for databases and buckets, the document and store statements, the user and role statements and the seeding statements, and has no request for it. Run 'help' to list what runs",
		st.Verb)
}

// membership runs join and promote: the server's own line is the output, because it alone knows
// whether the node voted, was refused a slot or was already a member.
func (s *RingSession) membership(ctx context.Context, st Statement) (RingOutput, error) {
	var outcome ringclient.MembershipResult
	var err error
	if st.Verb == "join" {
		outcome, err = s.api.Join(ctx, ringText(st, "node"), ringText(st, "addr"), false)
	} else {
		outcome, err = s.api.Promote(ctx, ringText(st, "node"))
	}
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(outcome.Result), nil
}

// replaceNode runs replace-node (BUG-0870). This release gives a node its own identity back at its
// own address, so --with must name the node itself; anything else is refused here, before a
// request the server could only refuse.
func (s *RingSession) replaceNode(ctx context.Context, st Statement) (RingOutput, error) {
	node, with, addr := ringText(st, "node"), ringText(st, "with"), ringText(st, "addr")
	if with != node {
		return RingOutput{}, ringRefusal("INVALID_ARGUMENT", "%s", strings.NewReplacer("<node>", node, "<with>", with, "<addr>", addr).Replace(
			"replace-node gives <node> its own identity back, so --with names <node> too: this release does not move an identity to another node id, and <with> is not <node>. Nothing was sent. "+
				"Start <node> again on an empty ring directory and run 'replace-node <node> --with <node>@<addr>'"))
	}
	_, acceptLoss := st.Arg("accept_loss")
	outcome, err := s.api.ReplaceNode(ctx, node, addr, acceptLoss)
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(outcome.Result), nil
}

func ringText(st Statement, name string) string {
	arg, _ := st.Arg(name)
	return arg.Text
}

func ringLines(lines ...string) RingOutput { return RingOutput{Lines: lines} }

func ringHelp(st Statement) (RingOutput, error) {
	var text string
	if topic, ok := st.Arg("topic"); ok {
		found, err := HelpFor(topic.Text)
		if err != nil {
			return RingOutput{}, ringRefusal("", "%s", err.Error())
		}
		text = found
	} else {
		text = Help()
	}
	return RingOutput{Lines: strings.Split(strings.TrimRight(text, "\n"), "\n")}, nil
}

// ringCopiesOutOfRange names the range a replication factor must fall in. The wire carries it as
// an int32, so a larger number must be refused rather than narrowed to another copy count.
func ringCopiesOutOfRange(text string) string {
	return fmt.Sprintf("replication_factor %s is out of range: write a whole number from 1 to %d", text, math.MaxInt32)
}

func (s *RingSession) createDatabase(ctx context.Context, st Statement) (RingOutput, error) {
	name := ringText(st, "database")
	var factor int32
	rfArg, explicit := st.Arg("rf")
	if explicit {
		// An explicit 0 is refused too: the wire reads 0 as "omit", so it would take the server default.
		if rfArg.Int < 1 || rfArg.Int > math.MaxInt32 {
			return RingOutput{}, ringRefusal("INVALID_ARGUMENT", "%s", ringCopiesOutOfRange(strconv.FormatInt(rfArg.Int, 10)))
		}
		factor = int32(rfArg.Int)
	}
	if ringText(st, "storage") == "memory" {
		return RingOutput{}, ringRefusal("INVALID_ARGUMENT", "%s", ringclient.MemoryStorageRefusal(name))
	}
	info, err := s.api.CreateDatabaseWithStorage(ctx, name, factor, ringText(st, "storage"))
	if err != nil {
		return RingOutput{}, err
	}
	created := fmt.Sprintf("OK  created database %s (replication factor %d)", name, info.RF)
	switch {
	case !explicit:
		created = fmt.Sprintf("OK  created database %s (replication factor %d, this node's default)", name, info.RF)
	}
	return ringLines(
		created,
		"Next: use "+name,
		"Then: create bucket <name> { Name string, Purchases fifoStore }   (declare every field)"), nil
}

// alterStorageRefusal names the fix for a storage member of alter database: a database keeps the store
// it was created with, so the fix is a new database and the data moved by hand.
func alterStorageRefusal(database string) string {
	return fmt.Sprintf(`storage of database "%s" cannot be changed: its data lives in a different store. `+
		`Create a new database with 'create database <name> { storage: disk }', then move the data with dump and restore`, database)
}

func ringFieldList(fields []ringclient.FieldInfo) (text string, count int) {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.Name != "_id_" {
			part := field.Name + " " + field.Type
			if field.Of != "" {
				part += " of " + field.Of
			}
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return "(none)", 0
	}
	return strings.Join(parts, ", "), len(parts)
}

// ringUnbuiltRefusal is the refusal of an index: the grammar parses it so a statement can be
// read back, but the ring wire does not carry it, and a statement that printed OK while dropping
// it would leave a bucket the developer did not ask for.
func ringUnbuiltRefusal(outcome, clause string) *RingError {
	return ringRefusal("INVALID_ARGUMENT", "Indexes are parsed but not built in this build; nothing was %s. "+
		"%s without the index entry", outcome, clause)
}

func (s *RingSession) createBucket(ctx context.Context, st Statement) (RingOutput, error) {
	fieldsArg, _ := st.Arg("fields")
	if len(fieldsArg.Indexes) > 0 {
		return RingOutput{}, ringUnbuiltRefusal("created", "Create the bucket")
	}
	declared := make([]ringclient.FieldDecl, len(fieldsArg.Fields))
	for i, field := range fieldsArg.Fields {
		declared[i] = ringclient.FieldDecl{Name: field.Name, Type: field.Type, ExpirePath: field.ExpirePath, Of: field.Of}
		for _, aggregate := range field.Aggregates {
			declared[i].Aggregates = append(declared[i].Aggregates, ringclient.AggregateDecl{Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By})
		}
	}
	database, name := ringText(st, "database"), ringText(st, "bucket")
	info, err := s.api.CreateBucketExpiring(ctx, database, name, fieldsArg.ExpireField, declared)
	if err != nil {
		return RingOutput{}, err
	}
	text, count := ringFieldList(info.Fields)
	return ringLines(fmt.Sprintf("OK  created bucket %s.%s (%d fields)", database, name, count), "Fields: "+text), nil
}

// alterBucket adds and drops the fields the statement names and prints what changed. It reads
// the declaration first, for two reasons: a name to drop that the bucket does not declare is
// almost always a typo, and the server (which treats it as already dropped, so that a repeated
// alter finishes) would say nothing; and the lines below are the difference between the table
// before and the table after.
func (s *RingSession) alterBucket(ctx context.Context, st Statement) (RingOutput, error) {
	changes, _ := st.Arg("changes")
	if len(changes.Indexes) > 0 || len(changes.DropIndexes) > 0 {
		return RingOutput{}, ringUnbuiltRefusal("changed", "Alter the bucket")
	}
	database, name := ringText(st, "database"), ringText(st, "bucket")
	before, err := s.api.DescribeBucket(ctx, database, name)
	if err != nil {
		return RingOutput{}, err
	}
	change := ringclient.AlterChange{SetExpire: changes.ExpireField, DropExpire: changes.DropExpire}
	hasAdd := slices.ContainsFunc(changes.Changes, func(edit FieldChange) bool { return edit.Add })
	for _, edit := range changes.Changes {
		if edit.Add {
			change.Add = append(change.Add, ringclient.FieldDecl{Name: edit.Name, Type: edit.Type})
			continue
		}
		// Only a statement that drops and adds nothing is checked for a typo: one that also adds
		// may be the repeat of an alter that finished, which the server answers as a no-op.
		if !hasAdd && !slices.ContainsFunc(before.Fields, func(field ringclient.FieldInfo) bool { return field.Name == edit.Name }) {
			text, _ := ringFieldList(before.Fields)
			return RingOutput{}, ringRefusal("INVALID_ARGUMENT", "field %q is not declared in %s.%s, so there is nothing to drop (declared: %s). Nothing was changed. "+
				"Run 'describe bucket %s in %s' to see the fields; if an earlier drop of %s finished, it is already gone",
				edit.Name, database, name, text, name, database, edit.Name)
		}
		change.Drop = append(change.Drop, edit.Name)
	}
	after, err := s.api.AlterBucket(ctx, database, name, change)
	if err != nil {
		return RingOutput{}, err
	}
	var added, dropped []string
	for _, field := range after.Fields {
		if !slices.ContainsFunc(before.Fields, func(old ringclient.FieldInfo) bool { return old.ID == field.ID }) {
			added = append(added, terminalSafe(field.Name)+" "+terminalSafe(field.Type))
		}
	}
	for _, field := range before.Fields {
		if !slices.ContainsFunc(after.Fields, func(now ringclient.FieldInfo) bool { return now.ID == field.ID }) {
			dropped = append(dropped, terminalSafe(field.Name))
		}
	}
	_, count := ringFieldList(after.Fields)
	expiryChanged := after.ExpireField != before.ExpireField
	if len(added) == 0 && len(dropped) == 0 && !expiryChanged {
		return ringLines(fmt.Sprintf("OK  bucket %s.%s already has these fields (%d fields), so nothing changed", database, name, count)), nil
	}
	if len(added) == 0 && len(dropped) == 0 {
		if after.ExpireField == "" {
			return ringLines(fmt.Sprintf("OK  altered bucket %s.%s: expiry removed", database, name)), nil
		}
		return ringLines(fmt.Sprintf("OK  altered bucket %s.%s: expires at %s", database, name, terminalSafe(after.ExpireField))), nil
	}
	lines := []string{fmt.Sprintf("OK  altered bucket %s.%s (%d fields)", database, name, count)}
	if len(added) > 0 {
		lines = append(lines, "Added: "+strings.Join(added, ", "))
	}
	if len(dropped) > 0 {
		lines = append(lines, "Dropped: "+strings.Join(dropped, ", ")+" (its values are deleted in the background)")
	}
	return RingOutput{Lines: lines}, nil
}

// dropBucket starts the drop of a bucket and its documents and prints that it is dropping: the
// node deletes the documents in the background and the name is free when that is done. It asks nothing first, as the drop bucket of v1.0
// does not: no prompt, no --yes, the same for a terminal and a pipe. The server refuses a bucket
// that is not there with the commands to list or create it, and a drop cut short is finished by
// running the same statement again.
func (s *RingSession) dropBucket(ctx context.Context, st Statement) (RingOutput, error) {
	database, name := ringText(st, "database"), ringText(st, "bucket")
	if err := s.api.DropBucket(ctx, database, name); err != nil {
		return RingOutput{}, err
	}
	return ringLines(fmt.Sprintf("OK  dropping bucket %s.%s; its documents are deleted in the background", terminalSafe(database), terminalSafe(name))), nil
}

func (s *RingSession) describeBucket(ctx context.Context, st Statement) (RingOutput, error) {
	database, name := ringText(st, "database"), ringText(st, "bucket")
	info, err := s.api.DescribeBucket(ctx, database, name)
	if err != nil {
		return RingOutput{}, err
	}
	rows := []fact{{"Id", fmt.Sprintf("%016x", info.ID)}, {"State", info.State}, {"Declaration version", fmt.Sprint(info.DeclVersion)}}
	lines := []string{fmt.Sprintf("BUCKET %s.%s", terminalSafe(database), terminalSafe(name)), "---"}
	declared := make([]ringclient.FieldInfo, 0, len(info.Fields))
	for _, field := range info.Fields {
		if field.Name != "_id_" {
			declared = append(declared, field)
		}
	}
	if len(declared) == 0 {
		rows = append(rows, fact{"Fields", "(none)"})
	} else {
		rows = append(rows, fact{"Fields", "(in declaration order)"})
	}
	lines = append(lines, strings.Split(strings.TrimRight(formatFacts(rows), "\n"), "\n")...)
	width := 12
	for _, field := range declared {
		width = max(width, len(terminalSafe(field.Name)))
	}
	for _, field := range declared {
		line := fmt.Sprintf("  %-*s %s", width, terminalSafe(field.Name), terminalSafe(field.Type))
		if clause := ringAggregateClause(info.Aggregates, field.Name); clause != "" {
			line += " " + clause
		}
		lines = append(lines, line)
	}
	lines = append(lines, ringExpireLines(info, declared)...)
	return RingOutput{Lines: lines}, nil
}

// ringExpireLines is the Expires block of describe bucket: the datetime field the bucket expires its
// documents at, then each queue field with the element path it expires at. Nothing when the bucket
// declares no expiry. The last line counts what is past its time and not yet removed.
func ringExpireLines(described ringclient.BucketInfo, declared []ringclient.FieldInfo) []string {
	if described.ExpireField == "" && len(described.ExpireStores) == 0 {
		return nil
	}
	var lines []string
	if described.ExpireField != "" {
		lines = append(lines, "Expires:             at "+terminalSafe(described.ExpireField))
	} else {
		lines = append(lines, "Expires:             (queue elements only)")
	}
	for _, store := range described.ExpireStores {
		typeWord := ""
		for _, field := range declared {
			if field.Name == store.Field {
				typeWord = field.Type
			}
		}
		lines = append(lines, fmt.Sprintf("  %-12s %-11s expire at %q", terminalSafe(store.Field), terminalSafe(typeWord), store.Path))
	}
	pending := strconv.FormatInt(described.ExpirePending, 10)
	if described.ExpirePendingMore {
		pending += "+"
	}
	return append(lines, "Past their time:     "+pending+" (hidden from reads, removed on the next retention tick)")
}

// ringAggregateClause renders the with (...) clause of one store field the way the create
// grammar writes it, so a describe line parses back; "" when the field keeps none.
func ringAggregateClause(aggregates []ringclient.AggregateInfo, store string) string {
	var parts []string
	for _, aggregate := range aggregates {
		if aggregate.Store != store {
			continue
		}
		text := ringAggregateText(AggregateDecl{Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By})
		if aggregate.State == "building" {
			text += " (building)"
		}
		parts = append(parts, text)
	}
	if len(parts) == 0 {
		return ""
	}
	return "with (" + strings.Join(parts, ", ") + ")"
}

func (s *RingSession) showBuckets(ctx context.Context, st Statement) (RingOutput, error) {
	database := ringText(st, "database")
	page, limit, hasLimit, err := ringPageOf(st)
	if err != nil {
		return RingOutput{}, err
	}
	listed, err := s.api.ShowBuckets(ctx, database, page)
	if err != nil {
		return RingOutput{}, err
	}
	lines := []string{fmt.Sprintf("BUCKETS in %s", terminalSafe(database)), "---"}
	if len(listed.Buckets) == 0 && !listed.More && len(page.Cursor) == 0 {
		lines = append(lines, fmt.Sprintf("(no buckets yet — create one with 'create bucket <name> in %s { Name string }')", terminalSafe(database)))
		return RingOutput{Lines: lines}, nil
	}
	width := len("NAME")
	for _, bucket := range listed.Buckets {
		width = max(width, len(terminalSafe(bucket.Name)))
	}
	lines = append(lines, fmt.Sprintf("%-*s  %-16s  %s", width, "NAME", "ID", "STATE"))
	for _, bucket := range listed.Buckets {
		lines = append(lines, fmt.Sprintf("%-*s  %016x  %s", width, terminalSafe(bucket.Name), bucket.ID, bucket.State))
	}
	if listed.More {
		lines = append(lines, ringMoreHint("show buckets in "+database, limit, hasLimit, listed.Cursor))
	}
	return RingOutput{Lines: lines}, nil
}

func (s *RingSession) use(ctx context.Context, st Statement) (RingOutput, error) {
	database := ringText(st, "database")
	if _, err := s.api.ShowBuckets(ctx, database, ringclient.PageRequest{Limit: 1}); err != nil {
		return RingOutput{}, err
	}
	s.selected = database
	return ringLines(fmt.Sprintf("Switched to database '%s'", database)), nil
}

// ringBSON turns a literal into the value ringclient marshals.
func ringBSON(v Value) any {
	switch v.Kind {
	case ValueString:
		return v.Text
	case ValueInt:
		return v.Int
	case ValueFloat:
		return v.Float
	case ValueBool:
		return v.Bool
	case ValueArray:
		elements := make(bson.A, len(v.Elems))
		for i, element := range v.Elems {
			elements[i] = ringBSON(element)
		}
		return elements
	case ValueDoc:
		doc := make(bson.D, len(v.Entries))
		for i, entry := range v.Entries {
			doc[i] = bson.E{Key: entry.Key, Value: ringBSON(entry.Value)}
		}
		return doc
	case ValueBinary:
		return primitive.Binary{Subtype: v.Subtype, Data: v.Bytes}
	}
	return nil
}

func ringDecodeCursor(text string) ([]byte, error) {
	cursor, err := hex.DecodeString(text)
	if err != nil {
		return nil, ringRefusal("PARSE_ERROR", "cursor %q is not hex: pass the digits the last page printed after 'cursor'", text)
	}
	return cursor, nil
}

func ringPage(cursor []byte) ringclient.PageRequest { return ringclient.PageRequest{Cursor: cursor} }
