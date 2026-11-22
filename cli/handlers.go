package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// CommandHandler dispatches commands to the appropriate handler function.
// It uses the driver connection to communicate with the hoardDB server.
type CommandHandler struct {
	terminal io.Writer
	// chatter receives the handler's commentary (debug lines, the password-file
	// warning). Nil means the terminal, which is also what a test gets that
	// replaces terminal after construction.
	chatter io.Writer
	debug   bool
	conn    *transport.DriverClient

	// currentDB is set by the CLI and used when a command does not name a
	// database explicitly (db.<bucket>.<op>() after 'use <database>').
	currentDB func() string

	// reconnect dials a different server from 'connect <address>'.
	reconnect func(addr string) error

	// wc is the session write concern for subsequent writes ("one",
	// "majority", "all"); empty means the server default. Set by the
	// 'write-concern <level>' command and carried on every insert.
	wc string

	// passwordFile and passwordStdin are the non-interactive password sources
	// for user creation (spec 6.3). Empty/false means prompt with no echo.
	passwordFile  string
	passwordStdin bool

	// passwordMinLength is the minimum the connected server reported on
	// AUTH_OK (its HOARDDB_PASSWORD_MIN_LENGTH). The server never receives a
	// new password, so this CLI is where the policy is measured (BUG-0493).
	// Zero means the server reported none.
	passwordMinLength int

	// scriptLines reads the next line of the piped statement stream. It is
	// set only while a non-interactive session is running, and it is the one
	// reader of that stream: a password that follows `create user` in a script
	// is the next line of the same scanner, not a second buffered reader of
	// stdin that would have swallowed or missed it.
	scriptLines func() (string, error)

	// extendedJSON controls whether documents are printed as Extended JSON,
	// which preserves type information (e.g. {"$date": "..."} for dates).
	extendedJSON bool

	// pretty lays each document out on several lines instead of one JSON object
	// per line. The default is one object per line, which is what a script and
	// an agent read; this is for a person reading a document.
	pretty bool

	// lastScanPage remembers which bucket served the last page find() printed
	// for each database and bucket, so the next page can say which bucket it
	// continues (ScanRequest.ExpectBucketID). It is tied to the connection the
	// id came from: another server's ids mean nothing here.
	lastScanPage map[scanTarget]scannedPage

	// decorate is the terminal display of a document's declared stores
	// (cli/storefields.go). Off, every document prints as it always has.
	decorate bool
	// declarationFetches counts the BUCKET_INFO calls the session made, for
	// the type and engine lookups and the display alike.
	declarationFetches int
	// declarations is what the session learned about each bucket (cli/
	// storefields.go): its type and top-level store fields, from one
	// BUCKET_INFO. The type and engine lookups of a statement and the display
	// read it; a failed read is remembered too, so the display does not retry.
	declarations map[scanTarget]*cachedDeclaration
	// declarationSource replaces the BUCKET_INFO call in a test.
	declarationSource func(database, bucket string) (*server.BucketInfoResponse, error)
}

// scanTarget names the bucket a find() page was read from.
type scanTarget struct{ database, bucket string }

// scannedPage is the identity the server reported for a printed page.
type scannedPage struct {
	conn     *transport.DriverClient
	bucketID int64
}

// NewCommandHandler creates a new CommandHandler.
func NewCommandHandler(terminal io.Writer, debug bool) *CommandHandler {
	return &CommandHandler{
		terminal: terminal,
		debug:    debug,
	}
}

// ErrNotConnected is returned by commands that need a live server session.
var ErrNotConnected = errors.New("not connected to server")

// SetConn sets the driver connection for server communication.
func (h *CommandHandler) SetConn(conn *transport.DriverClient) {
	h.conn = conn
	// Another connection may be another server, with other declarations.
	clear(h.declarations)
}

// SetPasswordSource configures where a user-creation password comes from:
// a 0600 file or stdin when non-interactive, otherwise a no-echo prompt.
func (h *CommandHandler) SetPasswordSource(file string, stdin bool) {
	h.passwordFile = file
	h.passwordStdin = stdin
}

// SetScriptLines gives the handler the line reader of a piped session, or nil
// for a terminal session. See CommandHandler.scriptLines.
func (h *CommandHandler) SetScriptLines(next func() (string, error)) {
	h.scriptLines = next
}

// AuthError is an OpAuthFail with the server's reason code. It exists so the
// CLI can tell a token that needs a password fallback from a credential
// failure that must not be retried.
type AuthError struct {
	Reason  transport.AuthFailReason
	Message string
}

func (e *AuthError) Error() string {
	switch e.Reason {
	case transport.AuthFailTokenExpired:
		return "authentication failed (TOKEN_EXPIRED): the AUTH token expired; re-run with -password or -auth-token"
	case transport.AuthFailTokenInvalid:
		return "authentication failed (TOKEN_INVALID): the AUTH token was rejected; authenticate again with -user/-password"
	case transport.AuthFailTokenUnsupported:
		return "authentication failed (TOKEN_UNSUPPORTED): this server cannot resume by token; use -user/-password"
	case transport.AuthFailProtocolMismatch:
		return "authentication failed (PROTOCOL_MISMATCH): the client and server protocol majors disagree; upgrade the older side"
	case transport.AuthFailRateLimited:
		// Two server causes share this reason with different remedies (a busy
		// password-hashing gate, or the per-IP failed-login ledger); only the
		// server's text says which.
		if e.Message != "" {
			return "authentication failed (RATE_LIMITED, not a credential failure): " + terminalSafeText(e.Message)
		}
		return "authentication failed (RATE_LIMITED): too many attempts; wait and retry (this is not a credential failure)"
	case transport.AuthFailAlreadyAuthed:
		return "authentication failed (ALREADY_AUTHED): the connection is already authenticated"
	}
	if e.Message != "" {
		return fmt.Sprintf("authentication failed (%s): %s", e.Reason, terminalSafeText(e.Message))
	}
	return fmt.Sprintf("authentication failed (%s)", e.Reason)
}

// send issues a BSON request over the driver connection and decodes the reply.
//
// An OpError or OpAuthFail frame is turned into a Go error so callers can
// treat a server-side failure exactly like a transport failure. OpAuthFail
// carries its reason code in an *AuthError, and an OpError its wire code in a
// *ServerError, so callers recover either with errors.As instead of reading text.
func (h *CommandHandler) send(op transport.Opcode, req, dst interface{}) error {
	if h.conn == nil {
		return ErrNotConnected
	}

	frame, err := h.conn.SendRequestBSON(context.Background(), op, req, dst)
	if err != nil {
		return err
	}

	switch frame.Opcode {
	case transport.OpAuthFail:
		reason := transport.AuthFailCredentials
		message := ""
		if len(frame.Payload) > 0 {
			var fail server.AuthFailResponse
			if uerr := bson.Unmarshal(frame.Payload, &fail); uerr == nil && fail.Reason != 0 {
				reason = transport.AuthFailReason(fail.Reason)
				message = fail.Message
			}
		}
		return &AuthError{Reason: reason, Message: message}
	case transport.OpError:
		return decodeServerError(frame.Payload)
	}
	h.noteFewerCopies(frame.Payload)
	return nil
}

// noteFewerCopies says, on the commentary stream, when a data write's answer
// reports fewer copies than a majority of the bucket's factor (spec §19.7):
// `majority` and `all` succeed against the copies that are in sync, and the
// number is the only sign that one was not. A majority is not noted, because
// the owner answers a majority write the moment a majority holds it, so a
// healthy cluster answers "2 of 3" on nearly every write with the third copy
// milliseconds behind (BUG-0691). The cost: with one of three out, a write
// still reads "2 of 3" and is silent; `status` shows the member. Any answer that carries both numbers is a data
// write's; nothing else carries them. A bare number left a developer with no
// cause and nowhere to look (BUG-0601), so the note names the two places the
// cause is - `status` and the node logs - and names the common causes briefly:
// a replica still connecting just after 'create bucket' is normal, a node down
// or over its disk high-water mark is not.
func (h *CommandHandler) noteFewerCopies(payload []byte) {
	if len(payload) == 0 {
		return
	}
	raw := bson.Raw(payload)
	confirmed, confirmedOK := raw.Lookup("copies_confirmed").AsInt64OK()
	factor, factorOK := raw.Lookup("replication_factor").AsInt64OK()
	if confirmedOK && factorOK && confirmed < int64(server.QuorumSize(int(factor))) {
		h.notef("stored on %d of %d copies; the other copies are behind or refused it.\n"+
			"Run 'status' and read the node logs - a replica still catching up just after 'create bucket' is normal; a node down or over its disk high-water mark is not.\n",
			confirmed, factor)
	}
}

// decodeServerError turns an OpError payload into a *ServerError. A payload
// that is empty or is not an ErrorResponse yields a Raw error: no code
// arrived, and the code is not guessed.
func decodeServerError(payload []byte) *ServerError {
	var errResp server.ErrorResponse
	if len(payload) == 0 || bson.Unmarshal(payload, &errResp) != nil {
		return &ServerError{Raw: true}
	}
	return &ServerError{
		Code:           transport.ErrorCode(errResp.Code),
		Message:        errResp.Message,
		MaxResultBytes: errResp.MaxResultBytes,
		RetainedBytes:  errResp.RetainedBytes,
	}
}

// SetCurrentDatabase wires the handler to the CLI's active database so
// bucket commands work without repeating the database name.
func (h *CommandHandler) SetCurrentDatabase(current func() string) {
	h.currentDB = current
}

// SetExtendedJSON configures whether documents are printed as Extended JSON,
// which preserves type information (e.g. {"$date": "..."} for dates).
func (h *CommandHandler) SetExtendedJSON(on bool) {
	h.extendedJSON = on
}

// SetPretty configures whether each document is laid out on several lines. The
// default is one JSON object per line, which is what a script and an agent read;
// this is for a person reading a document.
func (h *CommandHandler) SetPretty(on bool) {
	h.pretty = on
}

// SetReconnect wires the handler to the CLI's dialer so 'connect <address>'
// can move the session to another node.
func (h *CommandHandler) SetReconnect(reconnect func(addr string) error) {
	h.reconnect = reconnect
}

// database returns the database a command applies to: the one named in the
// command, or the CLI's active database.
func (h *CommandHandler) database(cmd *Cmd) string {
	if cmd.Database != "" {
		return cmd.Database
	}
	if h.currentDB != nil {
		return h.currentDB()
	}
	return ""
}

// noBucketDatabase is the refusal for a bucket statement with no database in
// force and no `in <database>` clause: it names both fixes, and nothing is sent.
func noBucketDatabase(verb, bucket string) error {
	return usagef("no database selected for '%s bucket %s'. Run 'use <database>' first, or name the database in the statement: '%s bucket %s in <database>' ('show databases' lists what exists)",
		verb, bucket, verb, bucket)
}

// Handle dispatches a parsed command to the appropriate handler.
func (h *CommandHandler) Handle(cmd *Cmd) error {
	if h.debug {
		h.notef("[DEBUG] Command: %+v\n", cmd)
	}

	if forgetsDeclarations(cmd) {
		clear(h.declarations)
	}
	switch cmd.Type {
	case "global":
		return h.handleGlobal(cmd)
	case "bucket":
		return h.handleBucket(cmd)
	case "user":
		return h.handleUser(cmd)
	case "admin":
		return h.handleAdmin(cmd)
	case "dump":
		return h.handleDump(cmd.Args)
	case "restore":
		return h.handleRestore(cmd.Args)
	default:
		return usagef("unknown command type: %s", cmd.Type)
	}
}

func (h *CommandHandler) printf(format string, args ...interface{}) {
	_, _ = io.WriteString(h.terminal, neutralizeControls(fmt.Sprintf(format, args...)))
}

// notef prints commentary about the session, as opposed to a statement's
// result, to the chatter writer, or to the terminal when none is set.
func (h *CommandHandler) notef(format string, args ...interface{}) {
	writer := h.chatter
	if writer == nil {
		writer = h.terminal
	}
	_, _ = io.WriteString(writer, neutralizeControls(fmt.Sprintf(format, args...)))
}

// SetChatter sends the handler's commentary to w instead of the terminal.
func (h *CommandHandler) SetChatter(w io.Writer) {
	h.chatter = w
}

// handleGlobal handles global commands: use, show, describe, status, help, exit.
func (h *CommandHandler) handleGlobal(cmd *Cmd) error {
	switch cmd.Method {
	case "use":
		return h.handleUse(cmd)
	case "batch":
		return h.handleBatch(cmd)
	case "show":
		return h.handleShow(cmd)
	case "describe":
		return h.handleDescribe(cmd)
	case "status":
		return h.handleStatus()
	case "encryption":
		return h.handleEncryption()
	case "nodes":
		return h.handleNodes(cmd)
	case "join":
		return h.handleJoin(cmd)
	case "migrate":
		return h.handleMigrate(cmd)
	case "consistent-get":
		return h.handleConsistentGet(cmd)
	case "write-concern":
		return h.handleWriteConcern(cmd)
	case "remove-node":
		return h.handleRemoveNode(cmd)
	case "master":
		return h.handleMaster()
	case "help":
		if len(cmd.Args) > 0 {
			return h.handleHelpTopic(cmd.Args[0])
		}
		h.handleHelp()
		return nil
	case "exit":
		return ErrExit
	case "connect":
		return h.handleConnect(cmd)
	default:
		return usagef("unknown global command: %s", cmd.Method)
	}
}

// handleUse sets the current database context, after checking the database
// exists so a typo fails immediately instead of on the next command.
func (h *CommandHandler) handleUse(cmd *Cmd) error {
	if cmd.Database == "" {
		return usagef("usage: use <database>")
	}

	var resp server.ListDatabasesResponse
	if err := h.send(transport.OpListDBs, &server.ListDatabasesRequest{}, &resp); err != nil {
		silentMember, refused := listingRefusedFor(err)
		if !refused {
			return fmt.Errorf("checking database %q: %w", cmd.Database, err)
		}
		// 'use' needs one yes-or-no fact, not the complete listing the cluster
		// refuses while a member is silent (BUG-0687), and refusing here would
		// block every database-scoped statement during the outage an operator
		// has to inspect. A typo ends on the first statement with the server's
		// own not-found, which names the fix.
		h.printf("Switched to database '%s'\n", cmd.Database)
		h.notef("Could not confirm database '%s' exists: member %s did not answer. "+
			"If '%s' is a typo, the first statement that uses it says so. Run 'nodes' to see every member and its state.\n",
			cmd.Database, silentMember, cmd.Database)
		return nil
	}
	for _, db := range resp.Databases {
		if db.Name == cmd.Database {
			h.printf("Switched to database '%s'\n", cmd.Database)
			return nil
		}
	}

	if len(resp.Databases) == 0 {
		if resp.Message != "" && resp.Message != server.NoDatabasesMessage {
			return notFoundf("database", "database %q not found — %s", cmd.Database, resp.Message)
		}
		return notFoundf("database", "database %q not found — this server has no databases yet; create one with 'create database %s'", cmd.Database, cmd.Database)
	}
	names := make([]string, 0, len(resp.Databases))
	for _, db := range resp.Databases {
		names = append(names, db.Name)
	}
	return notFoundf("database", "database %q not found — available: %s (or 'create database %s')",
		cmd.Database, strings.Join(names, ", "), cmd.Database)
}

// listingRefusedFor reports the member named when err is the cluster's refusal
// of a listing because that member did not answer.
func listingRefusedFor(err error) (string, bool) {
	var refusal *ServerError
	if !errors.As(err, &refusal) || refusal.Code != transport.ErrUnavailable {
		return "", false
	}
	return server.ListingRefusalMember(refusal.Message)
}

// handleShow dispatches show commands.
func (h *CommandHandler) handleShow(cmd *Cmd) error {
	if len(cmd.Args) == 0 {
		return usagef("usage: show databases|buckets|users|banner")
	}

	target := cmd.Args[0]
	switch target {
	case "databases":
		return h.handleShowDatabases(cmd)
	case "buckets":
		return h.handleShowBuckets(cmd)
	case "users":
		return h.handleShowUsers(cmd)
	default:
		return usagef("unknown show target: %s", target)
	}
}

// handleShowDatabases lists all databases on the connected server.
func (h *CommandHandler) handleShowDatabases(cmd *Cmd) error {
	var resp server.ListDatabasesResponse
	if err := h.send(transport.OpListDBs, &server.ListDatabasesRequest{}, &resp); err != nil {
		return fmt.Errorf("listing databases: %w", err)
	}

	h.printf("DATABASES\n")
	h.printf("---\n")
	if len(resp.Databases) == 0 {
		if resp.Message != "" && resp.Message != server.NoDatabasesMessage {
			h.printf("(%s)\n", resp.Message)
			return nil
		}
		h.printf("(no databases yet — create one with 'create database <name>')\n")
		return nil
	}

	nameWidth := len("NAME")
	for _, db := range resp.Databases {
		if len(terminalSafe(db.Name)) > nameWidth {
			nameWidth = len(terminalSafe(db.Name))
		}
	}
	h.printf("%-*s  %s\n", nameWidth, "NAME", "BUCKETS")
	for _, db := range resp.Databases {
		h.printf("%-*s  %d\n", nameWidth, terminalSafe(db.Name), db.BucketCount)
	}
	return nil
}

// handleShowBuckets lists the buckets of the active (or named) database.
func (h *CommandHandler) handleShowBuckets(cmd *Cmd) error {
	dbName := h.database(cmd)
	if dbName == "" {
		return usagef("no database selected — run 'use <database>' or 'show buckets in <database>'; 'show databases' lists what exists")
	}

	var resp server.ListBucketsResponse
	if err := h.send(transport.OpListBuckets, &server.ListBucketsRequest{Database: dbName}, &resp); err != nil {
		return fmt.Errorf("listing buckets in %q: %w", dbName, err)
	}
	if !resp.Found {
		return &RefusedError{Message: resp.Message}
	}

	h.printf("BUCKETS in %s\n", terminalSafe(dbName))
	h.printf("---\n")
	if resp.Encrypted != "" {
		h.printFacts([]fact{{"Encrypted", resp.Encrypted + " (this node's store; the same for every bucket)"}})
	}
	h.printBucketList(resp.Buckets)
	return nil
}

// handleShowUsers lists the server's users.
func (h *CommandHandler) handleShowUsers(cmd *Cmd) error {
	return h.handleUserList()
}

// BucketNames returns the bucket names of a database. It exists for Tab
// completion, which needs the names without printing them; a completion that
// printed a listing would corrupt the line being edited.
func (h *CommandHandler) BucketNames(dbName string) ([]string, error) {
	if dbName == "" {
		return nil, usagef("no database selected")
	}
	var resp server.ListBucketsResponse
	if err := h.send(transport.OpListBuckets, &server.ListBucketsRequest{Database: dbName}, &resp); err != nil {
		return nil, fmt.Errorf("listing buckets in %q: %w", dbName, err)
	}
	if !resp.Found {
		return nil, &RefusedError{Message: resp.Message}
	}
	names := make([]string, 0, len(resp.Buckets))
	for _, bucket := range resp.Buckets {
		// A name with control characters is left out: Tab would write it into
		// the line being edited, and it cannot be typed as a statement anyway.
		if terminalSafe(bucket.Name) != bucket.Name {
			continue
		}
		names = append(names, bucket.Name)
	}
	return names, nil
}

// handleDescribe describes a database or bucket.
func (h *CommandHandler) handleDescribe(cmd *Cmd) error {
	if len(cmd.Args) == 0 {
		return usagef("usage: describe bucket <name> | describe database <name>")
	}

	target := cmd.Args[0]
	switch target {
	case "bucket":
		return h.handleDescribeBucket(cmd)
	case "database":
		return h.handleDescribeDatabase(cmd)
	default:
		return usagef("unknown describe target %q — expected 'describe bucket <name>' or 'describe database <name>'", target)
	}
}

// fact is one "Label: value" row of a report.
type fact struct {
	label string
	value string
}

// printFacts prints the rows with every label padded to the widest, so the values
// line up in a column. Aligning to the widest label rather than a fixed width
// keeps a block tight when all its labels are short, and a label that needs the
// room gets it.
func (h *CommandHandler) printFacts(rows []fact) {
	h.printf("%s", formatFacts(rows))
}

// formatFacts lays the rows out as printFacts prints them; ring mode's
// describe prints the same block.
func formatFacts(rows []fact) string {
	var b strings.Builder
	width := 0
	for _, row := range rows {
		if n := len([]rune(row.label)); n > width {
			width = n
		}
	}
	for _, row := range rows {
		fmt.Fprintf(&b, "%s:%s%s\n", row.label, strings.Repeat(" ", width-len([]rune(row.label))+1), terminalSafeText(row.value))
	}
	return b.String()
}

// bucketFacts is what a bucket report says, in the order it says it, separate
// from the printing so the layout can be pinned without a server. noIndexes is
// the phrasing for a bucket with none, which the two callers word differently.
func bucketFacts(resp server.BucketStatsResponse, noIndexes string) []fact {
	rows := []fact{
		{"Type", string(resp.Type)},
		{"Entries", strconv.FormatInt(resp.Entries, 10)},
		{"Logical size", humanBytes(resp.LogicalBytes) + " (before compression and compaction)"},
	}
	// An older server reports no encryption mode at all; a bucket view then says
	// nothing about it rather than claiming plaintext.
	if resp.Encrypted != "" {
		rows = append(rows, fact{"Encrypted", resp.Encrypted + " (this node's store)"})
	}
	// An older server reports no factor; the block then says nothing about
	// copies rather than guessing.
	if resp.ReplicationFactor > 0 {
		origin := "declared"
		if resp.Inherited {
			origin = "inherited"
		}
		rows = append(rows, fact{"Replication factor", fmt.Sprintf("%d (%s)", resp.ReplicationFactor, origin)})
	}
	// Only a hash bucket has a placement; an older server sends none.
	if resp.Placement != "" {
		rows = append(rows, fact{"Placement", resp.Placement})
	}
	rows = append(rows, copyFacts(resp, time.Now())...)
	if len(resp.Indexes) == 0 {
		return append(rows, fact{"Indexes", noIndexes})
	}
	return append(rows, fact{"Indexes", strings.Join(resp.Indexes, ", ")})
}

// handleDescribeDatabase prints a database and the buckets it holds.
func (h *CommandHandler) handleDescribeDatabase(cmd *Cmd) error {
	dbName := cmd.Name
	if dbName == "" {
		dbName = h.database(cmd)
	}
	if dbName == "" {
		return usagef("usage: describe database <name>")
	}

	var dbs server.ListDatabasesResponse
	if err := h.send(transport.OpListDBs, &server.ListDatabasesRequest{}, &dbs); err != nil {
		return fmt.Errorf("describing database %q: %w", dbName, err)
	}
	found := false
	for _, db := range dbs.Databases {
		if db.Name == dbName {
			found = true
		}
	}
	if !found {
		return notFoundf("database", "database %q not found — 'show databases' lists what exists", dbName)
	}

	var resp server.ListBucketsResponse
	if err := h.send(transport.OpListBuckets, &server.ListBucketsRequest{Database: dbName}, &resp); err != nil {
		return fmt.Errorf("describing database %q: %w", dbName, err)
	}

	h.printf("DATABASE %s\n", terminalSafe(dbName))
	h.printf("---\n")
	h.printf("Buckets: %d\n", len(resp.Buckets))
	if len(resp.Buckets) > 0 {
		h.printf("\n")
		h.printBucketList(resp.Buckets)
	}
	return nil
}

// handleDescribeBucket prints a bucket's type, entry count, logical size and
// secondary indexes.
func (h *CommandHandler) handleDescribeBucket(cmd *Cmd) error {
	if cmd.Name == "" {
		return usagef("%s", describeBucketUsage)
	}
	dbName := h.database(cmd)
	if dbName == "" {
		return noBucketDatabase("describe", cmd.Name)
	}

	var resp server.BucketStatsResponse
	if err := h.send(transport.OpBucketStats, &server.BucketStatsRequest{Database: dbName, Bucket: cmd.Name}, &resp); err != nil {
		return fmt.Errorf("describing bucket %q: %w", cmd.Name, err)
	}
	if !resp.Found {
		if strings.TrimSpace(resp.Message) == "" {
			return notFoundf("bucket", "bucket %q not found in database %q — run 'show buckets' to list what exists", cmd.Name, dbName)
		}
		return &NotFoundError{Kind: "bucket", Message: resp.Message}
	}

	rows := bucketFacts(resp, "(none — declare them when you create the bucket)")
	if len(resp.Stores) > 0 {
		// The header of the store list belongs to the same block, so it is
		// aligned with it; the list itself is indented under it.
		rows = append(rows, fact{"Stores", "(elements summed over every document)"})
	}
	h.printf("BUCKET %s.%s\n", terminalSafe(resp.Database), terminalSafe(resp.Bucket))
	h.printf("---\n")
	h.printFacts(rows)
	for _, declared := range resp.Stores {
		on := ""
		if declared.Parent != "" {
			on = " on " + declared.Parent + "'s elements"
		}
		h.printf("  %-12s %sStore%s  %d element(s)\n", terminalSafe(declared.Name), terminalSafe(declared.Engine), terminalSafe(on), declared.Elements)
	}
	h.printDeadBannerOf(dbName, cmd.Name, resp.Type)
	return nil
}

// handleStatus prints server status information.
func (h *CommandHandler) handleStatus() error {
	if h.conn == nil {
		h.printf("Not connected to server.\n")
		return nil
	}

	var resp server.HealthResponse
	if err := h.send(transport.OpHealthCheck, &server.HealthRequest{}, &resp); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	h.printf("STATUS\n")
	h.printf("---\n")
	h.printf("Node ID:      %s\n", terminalSafe(resp.NodeID))
	h.printf("Version:      %s\n", terminalSafe(resp.Version))
	h.printf("Uptime:       %s\n", terminalSafe(resp.Uptime))
	h.printf("Buckets:      %d\n", resp.BucketCount)
	h.printf("Healthy:      %v\n", resp.Healthy)
	for _, reason := range resp.Reasons {
		h.printf("              %s\n", terminalSafeText(reason))
	}
	h.printStoreRefusal(resp.StoreRefusal)
	h.printMembership(resp.Membership, resp.MembershipDetail)
	h.printDuplicates(resp.Duplicates)
	h.printMemtable(resp.Memtable)
	h.printWAL(resp.WAL)
	h.printCapacity(resp.Capacity, time.Now())
	return nil
}

// printStoreRefusal prints status's store row when the node store refuses
// writes, with why and what to check; nothing while it accepts them.
func (h *CommandHandler) printStoreRefusal(refusal string) {
	if refusal != "" {
		h.printf("Store:        REFUSING WRITES - %s\n", terminalSafeText(refusal))
	}
}

// printMembership prints status's membership row: whether a member has
// confirmed this node, and why not when 'join' would not help. A standalone
// server, or one from before the field, sends nothing, and nothing is printed.
func (h *CommandHandler) printMembership(membership, detail string) {
	switch membership {
	case "":
	case server.MembershipNotAddedYet:
		h.printf("Membership:   NOT ADDED YET - run 'join <address>' on a member\n")
	case server.MembershipRefused:
		h.printf("Membership:   NOT CONFIRMED - %s\n", terminalSafeText(detail))
	case server.MembershipWaitingForFounders:
		h.printf("Membership:   WAITING FOR FOUNDERS - %s not answered yet; start them, or run 'remove-node <node-id>' "+
			"for one that is not coming back\n", terminalSafeText(detail))
	default:
		h.printf("Membership:   %s\n", terminalSafeText(membership))
	}
}

// printDuplicates prints status's row for each node id the node sees two
// processes running under (BUG-0600), with what to do; nothing when there are
// none or the server is older.
func (h *CommandHandler) printDuplicates(duplicates []string) {
	for _, duplicate := range duplicates {
		h.printf("Duplicate:    %s\n", terminalSafeText(duplicate))
	}
}

// printMemtable prints status's memtable row: the size, why, and the one
// thing wrong with it if anything is. An older server sends no memtable, and
// nothing is printed.
func (h *CommandHandler) printMemtable(memtable *server.HealthMemtable) {
	if memtable == nil {
		return
	}
	var why string
	switch memtable.Source {
	case "config":
		why = fmt.Sprintf("set in configuration; this store's choice is %d MiB", memtable.ChoiceMB)
	case "env":
		why = fmt.Sprintf("set by HOARDDB_MEMTABLE_MB; this store's choice is %d MiB", memtable.ChoiceMB)
	case "upgrade":
		why = "kept from the store's last run"
	case "no-limit-read":
		why = "chosen at first start without a memory limit (it could not be read)"
	default:
		why = fmt.Sprintf("chosen at first start from %s memory", humanBytes(memtable.ChosenFromLimitBytes))
	}
	batch := fmt.Sprintf("; largest atomic batch %d MB", (memtable.BatchBytes+500_000)/1_000_000)
	if memtable.Source == "config" || memtable.Source == "env" {
		batch = ""
	}
	switch {
	case len(memtable.Differs) > 0:
		members := make([]string, 0, len(memtable.Differs))
		for _, peer := range memtable.Differs {
			members = append(members, fmt.Sprintf("%s (%d MiB)", terminalSafe(peer.NodeID), peer.MB))
		}
		h.printf("Memtable:     %d MiB  DIFFERS from member %s; members must match (see the log)\n", memtable.MB, strings.Join(members, ", "))
	case memtable.Large:
		h.printf("Memtable:     %d MiB  %s; LARGE for this node's %s memory limit (see the log)\n", memtable.MB, why, humanBytes(memtable.LimitBytes))
	default:
		h.printf("Memtable:     %d MiB  %s%s\n", memtable.MB, why, batch)
	}
}

// printWAL prints status's write-ahead log row: whether its segments are
// preallocated and why, and whether the next one is ready. A refusal follows on
// its own line. Nothing is printed for a node with no log or an older server.
func (h *CommandHandler) printWAL(wal *server.HealthWAL) {
	if wal == nil {
		return
	}
	mode := terminalSafe(wal.Configured)
	if wal.Configured == "auto" {
		mode = "auto: " + terminalSafeText(wal.Reason)
	}
	if wal.Preallocated {
		spare := "spare ready"
		if !wal.SpareReady {
			spare = "spare not ready yet"
		}
		h.printf("WAL:          preallocated (%s), %s\n", mode, spare)
	} else {
		h.printf("WAL:          not preallocated (%s)\n", mode)
	}
	if wal.Refusal != "" {
		h.printf("              refusing writes: %s\n", terminalSafeText(wal.Refusal))
	}
}

// printCapacity prints status's capacity rows: one line per resource, with
// "-" where a resource could not be read, so an unavailable row never shows a
// healthy-looking 0. An older server sends no capacity, and nothing is
// printed. now is when the answer arrived, for the age of a stale sample.
func (h *CommandHandler) printCapacity(capacity *server.HealthCapacity, now time.Time) {
	if capacity == nil {
		return
	}
	h.printf("Capacity:\n")
	stale, red := false, false
	for _, row := range capacity.Rows {
		ratio := "-"
		if row.Ratio != nil {
			ratio = fmt.Sprintf("%.2f", *row.Ratio)
		}
		h.printf("  %-9s %6s  threshold %.2f  %s\n", terminalSafe(row.Resource), ratio, row.Threshold, terminalSafe(row.State))
		stale = stale || row.State == "stale"
		red = red || row.State == "red"
	}
	if stale {
		if capacity.SampledAt > 0 {
			age := now.Sub(time.Unix(capacity.SampledAt, 0)).Round(time.Second)
			h.printf("capacity last sampled %s ago; the capacity sampler may be stuck\n", age)
		} else {
			h.printf("capacity has not been sampled yet; the capacity sampler may be stuck\n")
		}
	}
	if red {
		h.printf("A red row's action is in this node's log (the capacity WARN line).\n")
	}
}

// printNodeRow prints one row of 'nodes', and under it the duplicate mark
// while two processes run under the node's id (BUG-0600).
func (h *CommandHandler) printNodeRow(node *server.NodeInfo) {
	h.printf("  %-20s  %-25s  %-10s  %s\n",
		terminalSafe(node.NodeID), terminalSafe(node.Addr), terminalSafe(node.Status),
		node.JoinedAt.Format("2006-01-02 15:04:05"))
	if node.Duplicate != "" {
		h.printf("  %-20s  %s\n", "", terminalSafeText(node.Duplicate))
	}
}

// handleNodes prints cluster node information, or, as 'nodes history', the
// answering node's membership history.
func (h *CommandHandler) handleNodes(cmd *Cmd) error {
	history := len(cmd.Args) == 1 && strings.EqualFold(cmd.Args[0], "history")
	if len(cmd.Args) > 0 && !history {
		return usagef("usage: nodes [history]")
	}
	if h.conn == nil {
		h.printf("Not connected to server.\n")
		return nil
	}

	var resp server.NodeListResponse
	if err := h.send(transport.OpListNodes, &server.NodeListRequest{History: history}, &resp); err != nil {
		return fmt.Errorf("list nodes failed: %w", err)
	}
	if history {
		return h.printMembershipHistory(h.conn.RemoteAddr(), resp)
	}

	h.printf("NODES\n")
	h.printf("---\n")
	if len(resp.Nodes) == 0 {
		h.printf("(no nodes registered)\n")
		return nil
	}
	for _, node := range resp.Nodes {
		h.printNodeRow(node)
	}
	h.printSchemaChanges(resp.SchemaChanges)
	return nil
}

// printMembershipHistory prints the membership history a member answered,
// oldest first, one line per epoch with a short digest of its set, so two
// members' histories compare line by line. A member that does not report one
// runs a build without 'nodes history': an empty history from it would agree
// with every other member, so it is refused, not printed.
func (h *CommandHandler) printMembershipHistory(member string, resp server.NodeListResponse) error {
	if !resp.HistoryReported {
		return refusedf("member %s does not report its membership history: it runs a build without 'nodes history'. "+
			"Upgrade it, then run 'nodes history' again", member)
	}
	h.printf("MEMBERSHIP HISTORY of %s\n", member)
	h.printf("---\n")
	if len(resp.MembershipHistory) == 0 {
		h.printf("(no membership recorded)\n")
		return nil
	}
	for _, record := range resp.MembershipHistory {
		ids := make([]string, 0, len(record.Members))
		for _, peer := range record.Members {
			ids = append(ids, peer.ID)
		}
		slices.Sort(ids)
		// The digest is of the ids as the members hold them, so two members'
		// histories still compare; only what is shown is escaped.
		shown := make([]string, len(ids))
		for index, id := range ids {
			shown[index] = terminalSafe(id)
		}
		h.printf("epoch %d: %s  (digest %s)\n", record.Epoch, strings.Join(shown, " "), membershipDigest(ids))
	}
	return nil
}

// membershipDigest is the first 8 hex digits of xxHash64 over a member set's
// sorted ids joined by newlines.
func membershipDigest(sortedIDs []string) string {
	return fmt.Sprintf("%016x", xxhash.Sum64String(strings.Join(sortedIDs, "\n")))[:8]
}

// printSchemaChanges prints, below the node table, whether the answering
// node's view pauses schema changes while a node joins, and the refusal's
// paragraph when it does (join-freeze spec §7.2). A standalone server sends
// nothing, and nothing is printed.
func (h *CommandHandler) printSchemaChanges(report *server.SchemaChangesReport) {
	switch {
	case report == nil:
	case report.Paused:
		h.printf("\nSchema changes: PAUSED\n%s\n", terminalSafeText(report.Message))
	case report.SettledAt == 0:
		h.printf("\nSchema changes: allowed (no member has joined since the cluster formed)\n")
	default:
		h.printf("\nSchema changes: allowed (every member settled at epoch %d)\n", report.SettledAt)
	}
}

// handleConnect moves the session to another server.
func (h *CommandHandler) handleConnect(cmd *Cmd) error {
	if len(cmd.Args) == 0 {
		return usagef("usage: connect <host:port>")
	}
	if h.reconnect == nil {
		return usagef("connect is unavailable in this session — start the CLI with -address <host:port>")
	}

	addr := cmd.Args[0]
	if err := h.reconnect(addr); err != nil {
		return fmt.Errorf("connect %s: %w", addr, err)
	}
	h.printf("Connected to %s\n", addr)
	return nil
}

// handleHelp prints the help text.
func (h *CommandHandler) handleHelp() {
	h.printf(`
hoardDB Query Language (HQL) — Quick Reference
=============================================

Global Commands:
  use <database>                  Switch to a database (checks it exists)
  show databases                  List all databases
  show buckets                    List buckets in current database
  show buckets in <database>      List buckets in a specific database
  show banner                     Print the session banner again
  set <setting> [on|off]          Change how output is printed: pretty, extjson, pager
  describe bucket <name>          Bucket type, entries, logical size, indexes
  describe bucket <name> in <database>  Same, naming the database (drop and alter bucket take 'in <database>' too)
  describe database <name>        Database buckets
  status                          Server status
  encryption                      Is this node's store encrypted at rest (mode, keystore, key ids)
  nodes                           List cluster nodes, and whether schema changes are paused by a join
  nodes history                   Print this member's membership history, one line per epoch, to compare across members
  join <id@host:port>             Add a node to the cluster and start rebalancing
  remove-node <node-id>           Remove a cluster member (run it from another node)
  connect <host:port>             Move this session to another server
  help                            Show this help message
  batch { <stmt>; <stmt>; ... }   Many store-field writes and reads in one request; not a transaction (help batch)
  help <topic>                    Help on: %s
  exit / quit                     Exit the CLI

Not available yet (the CLI says so instead of pretending to work):
  db.<bucket>.stream(...)         (use get(), optionally {out: "file"})

User Management (mutations are served by the cluster master; passwords are
prompted twice with no echo, or read from a 0600 file / stdin — never a flag):
  create user <name> roles [role@db, ...]     Create a user
  alter user <name> password                  Change the password (prompted twice)
  alter user <name> disable|enable            Disable or enable a user
  alter user <name> roles [role@db, ...]      Replace a user's role set
  drop user <name>                            Drop a user
  grant role <role@db> to <user>              Add one grant
  revoke role <role@db> from <user>           Remove one grant
  user list                                   List users (never hashes)
  user describe <name>                        One user's roles and status
  master                                      Show the node serving user changes
  --password-file <path> / --password-stdin   Non-interactive password source
  Worked examples: help user

Schema Management:
  create database <name>          Create a new database
  drop database <name>            Drop a database and its buckets (deletes data)
  create bucket <name> { type: hash }               Create a bucket (hash is default)
  create bucket <name> { type: hash, indices: ["Email", {field: "Login", unique: true}] }
  alter bucket <name> { Tags hashStore }            Add a store field (indexes: declare them at create)
  alter bucket Sessions { replication_factor: 5 }   Raise how many copies a bucket keeps (db-manager)
  alter bucket Sessions { replication_factor: 2 }   Lower how many copies a bucket keeps; the removed copies are deleted once the rest hold everything (db-manager)
  drop bucket <name>              Drop a bucket (deletes its data)

Document syntax (all of these are accepted):
  {"field": "value"}              strict JSON
  {field: "value", n: 1}          unquoted keys
  {field: 'value',}               single quotes, trailing comma

Hash Store Operations (key-value — the default bucket type):
  db.<bucket>.insert({field: value, ...})        Insert (returns the new _id_)
  db.<bucket>.upsert({_id_: "key", field: value})  Replace the document's fields, or store it (stores kept)
  db.<bucket>.find({_id_: "<uuid>"})             Fetch one document by key
  db.<bucket>.find("<key>")                      Same, bare key
  db.<bucket>.find()                             First 100 documents
  db.<bucket>.find({field: value})               First 100 matching (scans)
  db.<bucket>.find({field: {$regex: "^f"}})      Filter operators: help operators
  db.<bucket>.find({}, {limit: 10})              Take 10; works on every type but blob
  db.<bucket>.find({}, {skip: 100})              Next page
  db.<bucket>.find({}, {limit: 10, skip: 100})   Both together
  db.<bucket>.find({}, {sort: {Age: -1}, limit: 10})   Order first, then take 10
  db.<bucket>.find({}, {projection: {Email: 0}, limit: 10})  Fewer fields back
  db.<bucket>.delete("<key>")                    Delete by key
  db.<bucket>.update("<key>", {$set: {field: value}})
  db.<bucket>.update("<key>", {$set: {f: v}, $unset: ["old"], $inc: {n: 1}})
  db.<bucket>.count()                            Number of entries
  db.<bucket>.search({field: value})             Search, served by an index when one fits
  db.<bucket>.stats()                            Type, entries, logical size, indexes
  help hash                                      This topic, on its own

Queue Operations (FIFO/LIFO):
  db.<bucket>.push({field: value, ...})
  db.<bucket>.pop()
  db.<bucket>.pop({limit: 3})
  db.<bucket>.peek()
  db.<bucket>.peek({limit: 3})
  db.<bucket>.length()
  db.<bucket>.claim({limit: 10, lease: "30s"})
  db.<bucket>.ack({lease_id: "<lease_id>", item_ids: ["<item_id>"]})
  db.<bucket>.release({lease_id: "<lease_id>"})
  db.<bucket>.extend({lease_id: "<lease_id>", lease: "60s"})
  db.<bucket>.dead()
  db.<bucket>.requeue({all: true})
  db.<bucket>.discard({all: true})
  pop() removes and returns the next entry; pop({limit: N}) removes and returns the next N, in pop order.
  pop removes at most once; to process without losing work, use claim and ack (help claim).
  To look without removing, use peek.
  peek() returns the one entry pop() would take next.
  peek({limit: N}) returns the next N, in pop order, and removes none of them.

B+Tree Operations:
  db.<bucket>.insert({_id_: "key", field: value})
  db.<bucket>.upsert({_id_: "key", field: value})  Replace the document under that key, or store it
  db.<bucket>.get("key")
  db.<bucket>.range({$gte: low, $lte: high})
  db.<bucket>.first()
  db.<bucket>.last()

Blob Operations:
  db.<bucket>.put({_id_: "key", data: "hello", name: "hello.txt"})
  db.<bucket>.get("key")                         Metadata + content
  db.<bucket>.get({_id_: "key", out: "f.bin"})   Write content to a local file
  db.<bucket>.info("key")                        Metadata and size only
  db.<bucket>.delete("key")

Heap Operations:
  db.<bucket>.push({priority: N, field: value})
  db.<bucket>.pop()
  db.<bucket>.pop({limit: 3})
  db.<bucket>.peek()
  db.<bucket>.peek({limit: 3})
  db.<bucket>.claim({limit: 10, lease: "30s"})
  db.<bucket>.ack({lease_id: "<lease_id>", item_ids: ["<item_id>"]})
  db.<bucket>.release({lease_id: "<lease_id>"})
  db.<bucket>.extend({lease_id: "<lease_id>", lease: "60s"})
  db.<bucket>.dead()
  db.<bucket>.requeue({all: true})
  db.<bucket>.discard({all: true})
  pop removes at most once; to process without losing work, use claim and ack (help claim).

Store Fields — a store on each document (help fields):
  A bucket can declare fields, and a field can itself be a store of any of the
  six types. The store belongs to each document, not to the bucket, so every
  post has its own Comments.

  create bucket Post { Comments fifoStore, Photos blobStore, Tags hashStore }
  db.Post.insert({_id_: "p1", Title: "Sunset"})   the document the stores hang off
  db.<bucket>["<key>"].<field>.<verb>(...)

  [h]oardDB [app]> db.Post["p1"].Comments.push({by: "ada", text: "Lovely light"})
  OK

  That push added one comment to the fifoStore Comments of the Post document
  "p1". The key is quoted, and the document must already exist.

  fifo, lifo and heap fields:   push, pop, peek, len, list
  hash and btree fields:       put, get, delete, len, list
  blob fields:                 put, get, delete, len, list
  len(), length() and count() all answer the count

  db.Post["p1"].Comments.pop()                Remove the oldest
  db.Post["p1"].Comments.pop({limit: 3})      Remove the oldest 3; peek looks without removing
  db.Post["p1"].Comments.len()                How many it holds
  db.Post["p1"].Tags.put("go", {n: 12})       Add an element (hash/btree)
  db.Post["p1"].Tags.get("go")                Read it back (hash/btree/blob)
  db.Post["p1"].Comments.list({limit: 10})    Elements, in storage order

  A store on a store's elements, reached through the element:
  create bucket Post { Comments fifoStore { Flags fifoStore } }
  db.Post["p1"].Comments[0].Flags.push({reason: "spam"})

  peek() returns the one element pop() would take next; peek({limit: N}) the
  next N, in pop order. list reads in storage order, which on a lifo is the
  reverse of pop order. "help fields" has the details, including what a blob
  field's put carries.

Statistics:
  db.<bucket>.stats()

Counting: len(), length() and count() are the same verb at either level — on a
bucket and on a store field — and return the same number whichever you write.

Taking N elements is also one verb per level, and the names differ: on a bucket
it is find({}, {limit: N}), on a store field it is list({limit: N}). There is no
list() on a bucket.

Update Operators (server-applied):
  $set {field: value}   $unset ["field"]   $inc {field: N}

Long output arrives a screen at a time, the way more(1) shows it: space for the
next page, enter for one line, b for the page before, g and G for the first and
last, q to stop. A report that fits is printed in place, and a piped run is
never paged. Start the CLI with -no-pager to print everything in full.

Statements end with ; (optional in interactive mode).
`, strings.Join(helpTopicList(), ", "))
}

// handleHelpTopic prints help for a specific topic. An unknown topic is a
// usage error, so a script that checks the exit status can tell a typo from a
// found topic.
func (h *CommandHandler) handleHelpTopic(topic string) error {
	topics := map[string]string{
		"fifo": `FIFO Store Operations (First In, First Out):
  db.<bucket>.push({field: value})    Append to tail
  db.<bucket>.pop()                    Remove from head (oldest first)
  db.<bucket>.pop({limit: N})          Remove the next N from head (oldest first)
  db.<bucket>.peek()                   Look at head without removing
  db.<bucket>.length()                 Number of items
  pop() and pop({limit: N}) remove what they return; peek() and peek({limit: N}) look without removing.
  peek() returns the one entry pop() would take next.
  peek({limit: N}) returns the next N, in pop order, and removes none of them.
  pop removes at most once; to process without losing work, use claim and ack (help claim).

Example:
  db.Comments.push({Author: "Bob", Text: "Hi"})
  db.Comments.pop()    → {Author: "Bob", Text: "Hi"}`,

		"lifo": `LIFO Store Operations (Last In, First Out):
  db.<bucket>.push({field: value})    Push to top
  db.<bucket>.pop()                    Remove from top (newest first)
  db.<bucket>.pop({limit: N})          Remove the next N from top (newest first)
  db.<bucket>.peek()                   Look at top without removing
  db.<bucket>.length()                 Number of items
  pop() and pop({limit: N}) remove what they return; peek() and peek({limit: N}) look without removing.
  peek() returns the one entry pop() would take next.
  peek({limit: N}) returns the next N, in pop order, and removes none of them.
  pop removes at most once; to process without losing work, use claim and ack (help claim).

Example:
  db.Sessions.push({IP: "192.0.2.1"})
  db.Sessions.pop()   → {IP: "192.0.2.1"}`,

		"hash": `Hash Store Operations (Key-Value):
  db.<bucket>.insert({field: value})       Insert document
  db.<bucket>.find({_id_: "<uuid>"})       Find by ID
  db.<bucket>.find("<key>")                Find by key
  db.<bucket>.find()                       First 100 documents
  db.<bucket>.find({field: value})         First 100 matching (scans; no index needed)
  db.<bucket>.find({}, {skip: 100})        Next page
  db.<bucket>.update({_id_: ".."}, {$set: {f: v}})  Update
  db.<bucket>.delete({_id_: "<uuid>"})     Delete
  db.<bucket>.count()                       Count all
  db.<bucket>.count({field: value})         Count with filter

On a hash store field (help fields): put, get, delete, not insert.
  db.<bucket>["<key>"].<field>.put("go", {n: 12})
  db.<bucket>["<key>"].<field>.get("go")`,

		"btree": `B+Tree Store Operations (Sorted):
  db.<bucket>.insert({key: value, ...})       Insert with sorted key
  db.<bucket>.get({key: value})               Get by exact key
  db.<bucket>.range({key: {$gte: lo, $lte: hi}})  Range query
  db.<bucket>.first()                          Lowest key
  db.<bucket>.last()                           Highest key
  db.<bucket>.length()                         Count

On a btree store field (help fields): put, get, delete, not insert.
  db.<bucket>["<key>"].<field>.put("pasta", {mins: 20})
  db.<bucket>["<key>"].<field>.get("pasta")`,

		"heap": `Heap Store Operations (Priority Queue):
  db.<bucket>.push({priority: N, field: value})  Push with priority
  db.<bucket>.pop()                               Highest priority first
  db.<bucket>.pop({limit: N})                     The next N, highest priority first, removed
  db.<bucket>.peek()                              See highest without removing
  db.<bucket>.length()                            Count
  pop() and pop({limit: N}) remove what they return; peek() and peek({limit: N}) look without removing.
  peek() returns the one entry pop() would take next.
  peek({limit: N}) returns the next N, highest priority first, and removes none of them.
  pop removes at most once; to process without losing work, use claim and ack (help claim).

Example:
  db.Alerts.push({priority: 10, Msg: "Critical"})
  db.Alerts.pop()    → {Msg: "Critical"}   (the priority orders the element; it is not stored)`,

		"placement": `Placement (hash buckets):
  A hash bucket can be spread or kept whole. The choice is made when the bucket is created and recorded
  with it; describe bucket shows it. In this build a bucket is whole unless you name spread, until the
  spread collection work completes; the end state is spread by default.

    create bucket User in app { Name string }                    whole (the default in this build)
    create bucket User in app { placement: spread, Name string } divided over the cluster

  whole: the bucket lives on one node (and its replicas), and a unique constraint on any field is
         checked there. A bucket that is the unit of one entity, a few thousand documents, belongs here.
  spread: the documents are divided over the cluster by _id_. A unique constraint on a field other than
         _id_ is refused, because no part can see the values held by the others, and an update that
         matches by a filter instead of an _id_ is refused at the door. Update by _id_ instead.

  A hash bucket on a standalone server is always on one node; spread changes nothing there beyond
  the refusals above, so a bucket that will need unique or filtered updates should be created whole.
  Every other store type is whole and takes no placement.

  Placement is fixed when the bucket is created. To change it, recreate the bucket:
    dump --out ./dump-User app User
    drop bucket User in app
    create bucket User in app { placement: whole, Name string, Email string unique }
    restore --dir ./dump-User
  A dump does not carry blob payloads or blob store contents; copy those out first. A restore into an
  existing bucket restores documents, not placement.`,

		"claim": `Claim and Acknowledge (work a worker must not lose):
  pop() removes an entry at most once: a worker that dies after it has lost the work.
  claim takes entries under a lease instead. An entry nobody acknowledges before the
  lease ends goes back to the queue and is delivered again, up to 5 times.
  Works on fifo, lifo and heap buckets (not on store fields: use pop there).

  db.Jobs.claim({limit: 10, lease: "30s"})      Take up to 10; they come back if not acknowledged in 30s
  db.Jobs.ack({lease_id: "<lease_id>", item_ids: ["<item_id>"]})   Acknowledge the ones you finished
  db.Jobs.ack({lease_id: "<lease_id>"})          Acknowledge every entry the lease still holds
  db.Jobs.extend({lease_id: "<lease_id>", lease: "60s"})   Keep working past the lease
  db.Jobs.release({lease_id: "<lease_id>"})      Give them back now
  db.Jobs.dead()                                 Entries claimed 5 times without an acknowledgement
  db.Jobs.dead({limit: 50, after: "<dead_id>"})  The next page of them
  db.Jobs.requeue({dead_id: "<dead_id>"})        Put one back in the queue, its count reset
  db.Jobs.requeue({all: true})                   Put every dead entry back
  db.Jobs.discard({dead_id: "<dead_id>"})        Drop one for good
  db.Jobs.discard({all: true})                   Drop every dead entry

  lease takes a unit: "1500ms", "30s", "5m" or "1h", from 1s to 1h. A bare 30 is refused.
  Without a lease the default is 30s; without a limit, claim takes one entry.

  A whole loop, with the ids claim prints on its last line:

  [h]oardDB [app]> db.Jobs.push({task: "resize"})
  OK  id=3f2b8c1e-5d4a-4e7b-9c6d-1a2b3c4d5e6f
  [h]oardDB [app]> db.Jobs.claim({lease: "30s"})
  {"_id_": "3f2b8c1e-5d4a-4e7b-9c6d-1a2b3c4d5e6f", "task": "resize"}
  lease_id 9f3a0c1d5e7b2a4c6d8e0f1a2b3c4d5e6f708192 for 30s; item_ids ["00000000000000a1"]
  [h]oardDB [app]> db.Jobs.ack({lease_id: "9f3a0c1d5e7b2a4c6d8e0f1a2b3c4d5e6f708192", item_ids: ["00000000000000a1"]})
  acked 1

  The item_ids are the handles claim printed, not the documents' _id_ values.
  len() does not count claimed entries; it prints them beside the number.
  An entry claimed 5 times without an ack is dead, not delivered again: describe bucket
  and show buckets say which buckets hold some, and dead() lists them.`,

		"blob": `Blob Store Operations (Binary Data):
  db.<bucket>.put({data: <bytes>, name: "file.jpg"})  Store blob
  db.<bucket>.get({_id_: "<uuid>"})                    Retrieve blob
  db.<bucket>.info({_id_: "<uuid>"})                   Metadata only
  db.<bucket>.delete({_id_: "<uuid>"})                  Delete blob
  db.<bucket>.stream({_id_: "<uuid>"})                  Stream blob

On a blob store field (help fields): put, get, delete. The bytes go in data.
  db.<bucket>["<key>"].<field>.put("ph1", {data: "hello", name: "b.txt"})
  db.<bucket>["<key>"].<field>.get("ph1")`,

		"fields": `Field Stores (a store on each document):
  create bucket Post { Comments fifoStore, Photos blobStore }   Declare store fields
  alter bucket Post { Tags hashStore }                          Add one (never remove)
  Each document of Post has its own Comments, Photos and Tags.

  db.Post["<key>"].Comments.push({field: value})   fifo/lifo/heap: push, pop, peek
  db.Post["<key>"].Comments.pop()                  The oldest comment of that post
  db.Post["<key>"].Comments.pop({limit: 3})        The oldest 3, removed, in pop order
  db.Post["<key>"].Comments.peek()                 The one pop() would take next
  db.Post["<key>"].Comments.peek({limit: 3})       The next 3, in pop order, none removed
  list({limit: N}) reads in storage order instead: pop order on a fifo, the
  reverse of it on a lifo.
  db.Post["<key>"].Tags.put("go", {w: 1})           hash/btree/blob: put, get, delete
  db.Post["<key>"].Tags.get("go")
  db.Post["<key>"].Comments.len()                   Element count of that post
  len(), length() and count() all answer this
  db.Post["<key>"].Comments.list({limit: 10})       Elements in order

  A store on a store's elements (L3), addressed through the element:
  create bucket Post { Comments fifoStore { Flags fifoStore } }
  db.Post["<key>"].Comments[0].Flags.push({reason: "spam"})

  Deleting a document deletes its stores. describe bucket Post lists them.`,

		"batch": `Batch (many store-field writes and reads, to any buckets, in one request):
  batch { <statement>; <statement>; ... }
  A batch carries push, put, delete, get, len and peek of a store field, and an
  update of one key; at most 1,000 statements and 16 MiB. Statements address the
  session database ('use <database>' first).

  A batch is NOT a transaction: each statement is applied, refused or left
  unknown on its own, and the table says which. Statements of one bucket apply
  in order; after one that is not applied, the later ones of that bucket are not
  run. Statements of different buckets have no order.

Example:
  [h]oardDB [social]> batch {
  ...>   db.User_7["7"].Posts.put("p-1001", {text: "hello"});
  ...>   db.User_42["42"].Feed.push({author: "7", post: "p-1001", text: "hello"});
  ...>   db.User_7.update("7", {$inc: {posts_n: 1}});
  ...>   db.User_42["42"].Feed.len()
  ...> }

  Piped, it exits 0 when every statement succeeded, 2 when the batch was
  refused whole (nothing ran) and 3 when some statement was not a success:
  printf 'use social\nbatch { db.User_42["42"].Feed.push({text: "hi"}) }\n' | hoarddb`,

		"user": `User Management (the cluster master serves changes; passwords are
prompted twice with no echo, never given as a flag):
  create user ada roles [reader@app]               Create a user with roles
  alter user ada roles [writer@app, reader@logs]   Replace the whole role set
  grant role db-manager@app to ada                 Add one grant
  revoke role writer@app from ada                  Remove one grant
  alter user ada disable                           Block the user, keep the entry
  alter user ada enable                            Let the user in again
  alter user ada password                          Change the password
  alter user admin password                        The same for the bootstrap admin
  drop user ada                                    Delete the user
  user list                                        Users, never hashes
  user describe ada                                One user's roles and status

A role is <role>@<database>. Roles: root, reader, writer, db-manager,
user-manager, cluster-manager, backup-operator, restore-operator.

The bootstrap admin comes from the environment on the first start:
  HOARDDB_ROOT_USER=admin HOARDDB_ROOT_PASSWORD=<password> hoarddb-server

Changes take effect on the next authentication.`,

		"theme": `Prompt theme: a YAML file at ~/.hoarddb/theme.yml, or --theme <path>, or
HOARDDB_THEME=<path>. --theme plain turns colour off. 'theme' shows what resolved.
Keys: ` + strings.Join(themeFileKeys(), ", ") + `.
A file need only name what it changes; the rest comes from the built-in theme.

Example, recolour the mark and keep everything else:
  styles:
    accent: {fg: "#a99cf5"}

Styles: brand, accent, tile, database, error. Colours are #rrggbb.
The default prompt paints brand, accent and database, and the session banner's
frame and labels paint accent; tile is only painted by a prompt_format of your
own that puts it in a span.
prompt_format uses %database for the current database and {style:text} spans.`,

		"set": `Session output settings, changed while the session runs:
  set pretty on|off     each document on several lines instead of one JSON object
                        per line, with a blank line between the documents of a list
  set extjson on|off    dates as {"$date": "..."} instead of plain strings
  set pager on|off      a report longer than a screen, one screen at a time

'set <setting>' on its own says what that setting is. The flags --pretty,
--extjson and --no-pager set the same three at startup, and a piped run never
pages whatever the setting says: paging needs a terminal at both ends.

The values are never changed by a setting: --pretty reflows the JSON the CLI
already prints, so a 64-bit integer, a decimal and a date's text are the same
values, byte for byte, on one line or on several.`,

		"operators": `Query Operators:

find filters (db.<bucket>.find({...})) — applied by the server as it scans:
  bare value       exact match: {field: value}
  $gt $gte $lt $lte    comparison (numbers numerically, strings lexicographically)
  $in                  {field: {$in: [v1, v2]}}
  $ne                  not equal: {field: {$ne: value}}
  $exists              field presence: {field: {$exists: true}}
  $nin                 not in array: {field: {$nin: [v1, v2]}}
  $regex               {field: {$regex: "pattern"}} — Go regular expression
  $and                 top level only: {$and: [{...}, {...}]}
  $or                  top level only: {$or: [{...}, {...}]}

  A bare value is an EXACT match: {image: "f*"} looks for the literal string
  "f*", not for values starting with f — there is no wildcard operator. Use a
  regular expression: {image: {$regex: "^f"}}, or "^f.*$" to pin the whole value.
  $regex is unanchored, so {$regex: "o"} matches every value containing "o".

search filters (db.<bucket>.search({...})) — the find grammar above:
  An index serves the filter's equality fields: {Email: "a@example.com"}, or a
  prefix of a compound index's fields in order. Every other predicate — and
  every operator — is checked against each document the index returns. With
  no index on the filter's first fields, search is a full scan, like find.
  The header names the index that served the query, or says (full scan):
  Found 1 results (via Email_1): / Found 3 results (full scan):

update operators: $set {field: value}   $unset ["field"]   $inc {field: N}
                  $push {field: value}  $addToSet {field: value}
                  $mul {field: N}  $min {field: value}  $max {field: value}
                  $rename {oldName: "newName"}
                  A key is a path: {$set: {Address.City: "Tirana"}} sets one member,
                  {$inc: {Lines[0].Qty: 1}} one element's; no quotes needed.
                  An update whose keys overlap ("Address" and "Address.City", or
                  $rename pairs that share a name: {a: "b", b: "c"}) is refused.
range bounds:    $gte, $lte (btree buckets, db.<bucket>.range({$gte: lo, $lte: hi}))

Not implemented: $not $nor $all $elemMatch $size $pull — the server refuses
one by name rather than matching nothing.

Examples:
  db.User.insert({Name: "Jane", Age: 41})        (unquoted keys are fine)
  db.User.find({Caption: {$regex: "sunset"}})
  db.User.find({$and: [{Status: "active"}, {Age: {$gte: 18}}]})
  db.User.update("k1", {$set: {Name: "Jane"}, $inc: {Age: 1}})
  db.Events.range({$gte: "2026-01-01", $lte: "2026-12-31"})`,

		"config": `Configuration (environment variables):
  HOARDDB_TCP_LISTEN          Driver (client) listen address (default: 127.0.0.1:7433)
  HOARDDB_LISTEN              Internode listen address (default: 127.0.0.1:4433)
  HOARDDB_DATA_DIR            Data directory (default: ./data; /data in the Docker image)
  HOARDDB_NODE_ID             Node identifier
  HOARDDB_CLUSTER_ENABLED     true starts a ring node; copies are set per database: create database <name> { replication_factor: N }
  HOARDDB_SEED_NODES          Ring only: the nodes already in the cluster, <node-id>@<host:port>, comma-separated; the first node lists none
  HOARDDB_SEED_FINGERPRINTS   Ring only: one <node-id>=SHA256:<64 hex> per listed node
  HOARDDB_ROOT_USER           Root username (first start)
  HOARDDB_ROOT_PASSWORD       Root password (first start)
  HOARDDB_TLS_MODE            auto (the only mode in this version)
  HOARDDB_LOG_LEVEL           debug | info | warn | error
  HOARDDB_METRICS_ENABLED     true | false
  HOARDDB_AUDIT_ENABLED       true | false`,
	}

	text, ok := topics[topic]
	if !ok {
		return usagef("unknown help topic %q. Available topics: %s. Run 'help' for the quick reference",
			topic, strings.Join(helpTopicList(), ", "))
	}
	h.printf("\n%s\n", text)
	return nil
}

// handleBucket dispatches bucket operations to real driver calls.
func (h *CommandHandler) handleBucket(cmd *Cmd) error {
	dbName := h.database(cmd)
	if dbName == "" {
		return usagef("no database selected — run 'use <database>' first ('show databases' lists what exists)")
	}

	bucketName := cmd.Bucket
	method := strings.ToLower(cmd.Method)

	if h.debug {
		h.notef("[%s] db.%s.%s(%s)\n",
			strings.ToUpper(method), bucketName, method, strings.Join(cmd.Args, ", "))
	}

	// If not connected, return an error.
	if h.conn == nil {
		return ErrNotConnected
	}
	// A store field's verbs share the bucket verbs' names, which is why the
	// route keys on the address and not on the method.
	if cmd.DocKey != "" {
		return h.handleField(dbName, cmd)
	}

	switch method {
	case "insert":
		return h.handleInsert(dbName, bucketName, cmd.Args)
	case "upsert":
		return h.handleUpsert(dbName, bucketName, cmd.Args)
	case "find":
		return h.handleFind("find", dbName, bucketName, cmd.Args)
	case "get":
		return h.handleGet(dbName, bucketName, cmd.Args)
	case "delete":
		return h.handleDelete(dbName, bucketName, cmd.Args)
	case "search":
		return h.handleSearch(dbName, bucketName, cmd.Args)
	case "push":
		return h.handlePush(dbName, bucketName, cmd.Args)
	case "pop":
		return h.handlePop(dbName, bucketName, cmd.Args)
	case "peek":
		return h.handlePeek(dbName, bucketName, cmd.Args)
	case "claim":
		return h.handleClaim(dbName, bucketName, cmd.Args)
	case "ack":
		return h.handleAck(dbName, bucketName, cmd.Args)
	case "release":
		return h.handleRelease(dbName, bucketName, cmd.Args)
	case "extend":
		return h.handleExtend(dbName, bucketName, cmd.Args)
	case "dead":
		return h.handleDead(dbName, bucketName, cmd.Args)
	case "requeue":
		return h.handleRequeue(dbName, bucketName, cmd.Args)
	case "discard":
		return h.handleDiscard(dbName, bucketName, cmd.Args)
	case "update":
		return h.handleUpdate(dbName, bucketName, cmd.Args)
	case "len", "length", "count":
		return h.handleCount(dbName, bucketName, cmd.Args)
	case "distinct":
		return h.handleDistinct(dbName, bucketName, cmd.Args)
	case "put":
		return h.handleBlobPut(dbName, bucketName, cmd.Args)
	case "info":
		return h.handleBlobInfo(dbName, bucketName, cmd.Args)
	case "stream":
		return errStreamUnsupported(bucketName)
	case "range":
		return h.handleRange(dbName, bucketName, cmd.Args)
	case "first":
		return h.handleEdge(dbName, bucketName, transport.OpBTreeFirst, "first")
	case "last":
		return h.handleEdge(dbName, bucketName, transport.OpBTreeLast, "last")
	case "stats":
		return h.handleBucketStats(dbName, bucketName)
	default:
		return usagef("unknown bucket method %q — run 'help' for the supported operations", method)
	}
}

// handleInsert makes a real Insert call.
func (h *CommandHandler) handleInsert(dbName, bucketName string, args []string) error {
	data := make(map[string]interface{})
	if len(args) > 0 {
		doc, err := parseDocument(args[0])
		if err != nil {
			return fmt.Errorf("insert: %w", err)
		}
		data = doc
	}
	return h.sendDocument(transport.OpPut, "insert", dbName, bucketName, data)
}

// handleUpsert sends UPSERT: the document replaces the scalar fields of the one
// stored under its _id_ (or is stored, when there is none) and the document's
// stores are kept. Unlike insert it needs a document: an upsert of nothing
// would replace a document with an empty one.
func (h *CommandHandler) handleUpsert(dbName, bucketName string, args []string) error {
	if len(args) == 0 {
		return usagef(`upsert needs a document — db.%s.upsert({_id_: "jane", Name: "Jane"}) replaces the scalar fields of "jane" and keeps its stores`, bucketName)
	}
	doc, err := parseDocument(args[0])
	if err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	return h.sendDocument(transport.OpUpsert, "upsert", dbName, bucketName, doc)
}

// sendDocument sends one document under op (PUT or UPSERT) and prints the id
// the server stored it under.
func (h *CommandHandler) sendDocument(op transport.Opcode, verb, dbName, bucketName string, data map[string]interface{}) error {
	req := &server.InsertRequest{
		Database: dbName,
		Bucket:   bucketName,
		Data:     data,
	}
	if h.wc != "" {
		req.WriteConcern = h.wc
	}

	var resp server.InsertResponse
	if err := h.send(op, req, &resp); err != nil {
		return fmt.Errorf("%s failed: %w", verb, err)
	}

	if !resp.Success {
		return &RefusedError{Message: resp.Message}
	}
	h.printf("OK  id=%s\n", terminalSafe(resp.ID))
	return nil
}

// handleGet routes "get" to the operation the bucket's store type defines:
// a document lookup for hash and btree buckets, and metadata plus content for
// blob buckets. Queue and heap buckets are read by position, so the error
// points at the verb that works there. The type is asked fresh for every get:
// a cached one is wrong, with no refusal to notice, after another session
// recreates the bucket as another type.
func (h *CommandHandler) handleGet(dbName, bucketName string, args []string) error {
	bucketType, err := h.bucketTypeOf(dbName, bucketName)
	if err != nil {
		return err
	}

	switch bucketType {
	case server.BucketTypeBlob:
		return h.handleBlobContent(dbName, bucketName, args)
	case server.BucketTypeFIFO, server.BucketTypeLIFO:
		return usagef("db.%s.get() is not defined for %s buckets — they are read by position:\n       db.%s.peek()  shows the next entry without removing it\n       db.%s.pop()   removes and returns the next entry",
			bucketName, bucketType, bucketName, bucketName)
	case server.BucketTypeHeap:
		return usagef("db.%s.get() is not defined for heap buckets — use db.%s.peek() for the highest-priority entry, or db.%s.pop() to remove it",
			bucketName, bucketName, bucketName)
	default:
		return h.handleFind("get", dbName, bucketName, args)
	}
}

// bucketTypeOf says which store backs a bucket, so "get" and a push with a
// priority can dispatch by type instead of guessing from the arguments. It is one
// fresh BUCKET_INFO per call (freshDeclaration), not BUCKET_STATS: the type is
// all it needs, and BUCKET_STATS walks every element of every store field to
// count them.
func (h *CommandHandler) bucketTypeOf(dbName, bucketName string) (server.BucketType, error) {
	return typeOfDeclaration(h.freshDeclaration(dbName, bucketName), dbName, bucketName)
}

// typeOfDeclaration is the bucket's type, or the error a failed lookup always
// printed.
func typeOfDeclaration(declaration *cachedDeclaration, dbName, bucketName string) (server.BucketType, error) {
	if declaration.failure != nil {
		return "", fmt.Errorf("inspecting bucket %q: %w", bucketName, declaration.failure)
	}
	if declaration.notFound {
		return "", notFoundf("bucket", "bucket %q not found in database %q — run 'show buckets' to list what exists, or 'create bucket <name> in %s {type: hash}' to create it",
			bucketName, dbName, dbName)
	}
	return declaration.bucketType, nil
}

// handleBlobContent implements "get" for blob buckets: the payload prints
// inline when it is text, and is written to a file when the argument carries
// {out: "path"}.
func (h *CommandHandler) handleBlobContent(dbName, bucketName string, args []string) error {
	if len(args) == 0 {
		return usagef(`usage: db.%s.get("my-key") or db.%s.get({_id_: "my-key", out: "payload.bin"})`, bucketName, bucketName)
	}
	key, err := keyFromArgs(args)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if key == "" {
		return usagef(`get needs a key: db.%s.get("my-key")`, bucketName)
	}

	if docArg := documentArg(args); docArg != "" {
		if opts, err := parseOptionDocument(docArg); err == nil {
			if out, ok := opts["out"]; ok {
				return h.readBlobToFile(dbName, bucketName, key, scalarString(out))
			}
		}
	}

	var resp server.BlobReadResponse
	req := &server.BlobReadRequest{Database: dbName, Bucket: bucketName, Key: key}
	if err := h.send(transport.OpBlobRead, req, &resp); err != nil {
		return fmt.Errorf("get failed: %w", err)
	}
	if !resp.Found {
		return h.printMiss(resp.Message, "NOT FOUND")
	}

	h.printf("Key:    %s\n", terminalSafe(key))
	h.printf("Blob:   %s\n", terminalSafe(resp.BlobID))
	h.printf("Size:   %s\n", humanBytes(int64(resp.Size)))
	if len(resp.Metadata) > 0 {
		h.printf("Metadata: %s\n", h.doc(resp.Metadata))
	}
	if isTextPayload(resp.Data) {
		h.printf("Data:\n%s\n", terminalSafeText(strings.TrimRight(string(resp.Data), "\n")))
	} else {
		h.printf("Data:   binary (%s) — re-run with {out: \"file\"} to save it\n", humanBytes(int64(resp.Size)))
	}
	return nil
}

// isTextPayload reports whether blob content is printable text, so "get" can
// print it inline rather than dumping binary at the terminal.
func isTextPayload(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	sample := data
	if len(sample) > 512 {
		sample = sample[:512]
	}
	for _, b := range sample {
		if b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		if b < 0x20 || b > 0x7e {
			return false
		}
	}
	return true
}

// findPlan is the decision handleFind makes from its arguments: a point read
// for a key, or a scan for no argument or a filter document.
type findPlan struct {
	opcode     transport.Opcode
	key        string
	filter     map[string]interface{}
	skip       int
	limit      int
	sort       map[string]int
	projection map[string]int
}

// findOptionKeys are the options find's second document reads.
var findOptionKeys = map[string]bool{"skip": true, "limit": true, "sort": true, "projection": true}

// planFind decides whether a find argument list is a point read or a scan. A
// key argument keeps the point-read behaviour find("alice") and
// find({_id_: "alice"}) have always had. No argument, or a document with no
// key field, is a scan: the document is the filter, and an optional second
// document carries skip/limit. Limit 0 means the server's default page size,
// which the CLI does not duplicate.
func planFind(args []string) (findPlan, error) {
	if len(args) == 0 {
		return findPlan{opcode: transport.OpScan}, nil
	}

	key, err := keyFromArgs(args)
	if err != nil {
		return findPlan{}, err
	}
	if key != "" {
		return findPlan{opcode: transport.OpGet, key: key}, nil
	}

	first := strings.TrimSpace(args[0])
	if !strings.HasPrefix(first, "{") {
		return findPlan{}, &UsageError{Message: `empty key — pass a key, or no argument to scan the bucket`}
	}

	filter, err := parseDocument(first)
	if err != nil {
		return findPlan{}, err
	}
	plan := findPlan{opcode: transport.OpScan, filter: filter}

	if len(args) > 1 {
		options, err := parseOptionDocument(args[1])
		if err != nil {
			return findPlan{}, err
		}
		if plan.limit, err = readLimitOption(options, findOptionKeys); err != nil {
			return findPlan{}, err
		}
		if plan.skip, err = readSkipOption(options); err != nil {
			return findPlan{}, err
		}
		if plan.sort, err = readSortOption(options); err != nil {
			return findPlan{}, err
		}
		if plan.projection, err = readProjectionOption(options); err != nil {
			return findPlan{}, err
		}
	}
	return plan, nil
}

// scanTruncationLine is the spec 3.3 next-page hint, printed only when more
// matches exist past the page. skip is the page's starting offset and shown is
// how many documents this page carried.
func scanTruncationLine(shown int, total int64, skip int, hasMore bool) string {
	if !hasMore {
		return ""
	}
	return fmt.Sprintf("showing %d of %s — next: find({}, {skip: %d})", shown, groupThousands(total), skip+shown)
}

// groupThousands renders an integer with comma separators, so 1234 reads as
// 1,234 in the truncation line.
func groupThousands(n int64) string {
	digits := strconv.FormatInt(n, 10)
	if len(digits) <= 3 {
		return digits
	}
	var grouped strings.Builder
	lead := len(digits) % 3
	if lead > 0 {
		grouped.WriteString(digits[:lead])
	}
	for i := lead; i < len(digits); i += 3 {
		if grouped.Len() > 0 {
			grouped.WriteByte(',')
		}
		grouped.WriteString(digits[i : i+3])
	}
	return grouped.String()
}

// handleFind makes a real Find call for a key, or a Scan for no argument or a
// filter document. For blob buckets it reads the blob (get/info) so the same
// verb works whichever store backs the bucket.
func (h *CommandHandler) handleFind(method, dbName, bucketName string, args []string) error {
	if method == "info" {
		return errStreamUnsupported(bucketName)
	}

	plan, err := planFind(args)
	if err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	if plan.opcode == transport.OpScan {
		return h.scanBucket(dbName, bucketName, plan)
	}

	key := plan.key

	// A document argument may carry output options, e.g. {_id_: "k", out: "f.bin"}.
	if docArg := documentArg(args); docArg != "" {
		if opts, err := parseOptionDocument(docArg); err == nil {
			if out, ok := opts["out"]; ok {
				return h.readBlobToFile(dbName, bucketName, key, scalarString(out))
			}
		}
	}

	req := &server.FindRequest{
		Database: dbName,
		Bucket:   bucketName,
		Key:      key,
	}

	var resp server.FindResponse
	if err := h.send(transport.OpGet, req, &resp); err != nil {
		return fmt.Errorf("find failed: %w", err)
	}

	if !resp.Found {
		return h.printMiss(resp.Message, "NOT FOUND")
	}
	h.printf("%s\n", h.bucketDoc(resp.Data, dbName, bucketName))
	return nil
}

// scanBucket issues the OpScan for a planned scan and prints the page. A
// bucket that does not exist or a blob bucket that cannot scan arrives as
// Found=false or a Message, both of which are errors the user must act on.
func (h *CommandHandler) scanBucket(dbName, bucketName string, plan findPlan) error {
	req := &server.ScanRequest{
		Database:   dbName,
		Bucket:     bucketName,
		Filter:     plan.filter,
		Skip:       plan.skip,
		Limit:      plan.limit,
		Sort:       plan.sort,
		Projection: plan.projection,
	}
	target := scanTarget{database: dbName, bucket: bucketName}
	// Only a continuation names a previous page; the first page has none.
	if previous, ok := h.lastScanPage[target]; ok && plan.skip != 0 && previous.conn == h.conn {
		req.ExpectBucketID = previous.bucketID
	}

	var resp server.ScanResponse
	if err := h.send(transport.OpScan, req, &resp); err != nil {
		return fmt.Errorf("find failed: %w", err)
	}
	if !resp.Found {
		return &RefusedError{Message: resp.Message}
	}
	if resp.Message != "" {
		return &RefusedError{Message: resp.Message}
	}
	if resp.BucketID != 0 {
		if h.lastScanPage == nil {
			h.lastScanPage = make(map[scanTarget]scannedPage)
		}
		h.lastScanPage[target] = scannedPage{conn: h.conn, bucketID: resp.BucketID}
	}
	if len(resp.Documents) == 0 {
		h.printf("NO MATCHES\n")
		return nil
	}

	h.printBucketDocuments(resp.Documents, dbName, bucketName)
	if line := scanTruncationLine(len(resp.Documents), resp.Total, plan.skip, resp.HasMore); line != "" {
		h.printf("%s\n", line)
	}
	return nil
}

// handleDelete makes a real Delete call.
func (h *CommandHandler) handleDelete(dbName, bucketName string, args []string) error {
	if len(args) == 0 {
		return usagef("usage: db.%s.delete(\"<key>\")", bucketName)
	}

	key, err := keyFromArgs(args)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if key == "" {
		return usagef("delete needs a key: db.%s.delete(\"my-key\")", bucketName)
	}

	req := &server.DeleteRequest{
		Database: dbName,
		Bucket:   bucketName,
		Key:      key,
	}

	var resp server.DeleteResponse
	if err := h.send(transport.OpDelete, req, &resp); err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	if !resp.Success {
		// An absent key is a result, not a refusal: it prints as the field-store
		// delete does, and a script that deletes twice is not failed by the
		// second answer.
		if resp.Message == deleteAbsentMessage || resp.Message == deleteAbsentBlobMessage {
			h.printf("NOT FOUND  %q\n", key)
			return nil
		}
		return &RefusedError{Message: resp.Message}
	}
	h.printf("OK  deleted %q\n", key)
	return nil
}

// handleSearch makes a real Search call using secondary indexes.
func (h *CommandHandler) handleSearch(dbName, bucketName string, args []string) error {
	if len(args) == 0 {
		return usagef("usage: db.%s.search({field: value})", bucketName)
	}

	filter, err := parseDocument(args[0])
	if err != nil {
		return fmt.Errorf("search: %w", err)
	}

	req := &server.SearchRequest{
		Database: dbName,
		Bucket:   bucketName,
		Filter:   filter,
	}

	var resp server.SearchResponse
	if err := h.send(transport.OpSearch, req, &resp); err != nil {
		return fmt.Errorf("search failed: %w", err)
	}

	if resp.Message != "" {
		return &RefusedError{Message: resp.Message}
	}

	// The server says on every response which index served the search, or that
	// it scanned. Naming it is the only way a developer can tell whether a
	// declared index earns its keep: a filter on an indexed field with an
	// operator in it scans, because an operator document is not an equality the
	// index key can hold, and the two look identical in the results.
	index := searchIndexNote(resp.IndexUsed)
	if len(resp.Results) == 0 {
		h.printf("NO RESULTS%s\n", terminalSafeText(index))
		return nil
	}

	h.printf("Found %d results%s:\n", resp.Total, terminalSafeText(index))
	h.printBucketDocuments(resp.Results, dbName, bucketName)
	return nil
}

// searchIndexNote renders the parenthetical the search header carries: what
// served the query. An index is named as "(via <name>)" and a scan says so,
// because a search that scanned looks exactly like one that did not otherwise.
//
// The server's own wording for a scan is indexUsedFullScan (server/
// search_plan.go); an empty IndexUsed is treated the same way, so a server from
// before the field existed — which cannot have used an index — is not reported
// as having used one.
func searchIndexNote(indexUsed string) string {
	if indexUsed == "" || indexUsed == indexUsedFullScan {
		return " (full scan)"
	}
	return " (via " + indexUsed + ")"
}

// indexUsedFullScan is the server's index_used value for a query no index
// served (server/search_plan.go:indexUsedFullScan). It is repeated here rather
// than exported so the CLI does not take a dependency on a server internal for
// one string; TestSearchSaysWhetherAnIndexServedIt fails if the two drift.
const indexUsedFullScan = "none — full scan"

// handlePush makes a real Push call.
func (h *CommandHandler) handlePush(dbName, bucketName string, args []string) error {
	data := make(map[string]interface{})
	if len(args) > 0 {
		doc, err := parseDocument(args[0])
		if err != nil {
			return fmt.Errorf("push: %w", err)
		}
		data = doc
	}

	// Only a heap consumes a priority: it orders the element and is not part
	// of the stored document. On every other engine "priority" is an ordinary
	// field the user chose, and stripping it lost their data, so
	// the bucket's type is asked for — only when the field is present.
	var priority int64
	if wholePriority, isNumber, fractionErr := heapPriority(data["priority"]); isNumber {
		bucketType, err := h.bucketTypeOf(dbName, bucketName)
		if err != nil {
			return fmt.Errorf("push: %w", err)
		}
		if bucketType == server.BucketTypeHeap {
			if fractionErr != nil {
				return fmt.Errorf("push: %w", fractionErr)
			}
			priority = wholePriority
			delete(data, "priority")
		}
	}

	req := &server.PushRequest{
		Database: dbName,
		Bucket:   bucketName,
		Data:     data,
		Priority: priority,
	}

	var resp server.PushResponse
	if err := h.send(transport.OpFIFOPush, req, &resp); err != nil {
		return fmt.Errorf("push failed: %w", err)
	}

	if !resp.Success {
		return &RefusedError{Message: resp.Message}
	}
	h.printf("OK  id=%s\n", terminalSafe(resp.ID))
	return nil
}

// printMiss handles a Found:false reply: an absence prints absentLine and
// succeeds, anything else is the error missError builds.
func (h *CommandHandler) printMiss(message, absentLine string) error {
	if err := missError(message); err != nil {
		return err
	}
	h.printf("%s\n", terminalSafeText(absentLine))
	return nil
}

// handlePop makes a real Pop call: it removes the next entries, in pop order,
// and prints one line each. The count is read, and refused if it is not
// understood, before anything is sent: a pop that ignored its arguments removed
// one entry whatever it was given (BUG-0546).
func (h *CommandHandler) handlePop(dbName, bucketName string, args []string) error {
	limit, err := readVerbLimit("pop", "db."+bucketName, args)
	if err != nil {
		return err
	}
	req := &server.PopRequest{
		Database: dbName,
		Bucket:   bucketName,
		Limit:    limit,
	}

	var resp server.PopResponse
	if err := h.send(transport.OpFIFOPop, req, &resp); err != nil {
		return fmt.Errorf("pop failed: %w", err)
	}

	if !resp.Found {
		return h.printMiss(resp.Message, "EMPTY  "+resp.Message+claimedNote(bucketName, resp.Pending, resp.Dead, 0))
	}
	for _, entry := range resp.Items {
		h.printf("%s\n", h.doc(entry))
	}
	return nil
}

// handlePeek makes a real Peek call: the next entries a pop would take, in pop
// order, one line each.
func (h *CommandHandler) handlePeek(dbName, bucketName string, args []string) error {
	limit, err := readVerbLimit("peek", "db."+bucketName, args)
	if err != nil {
		return err
	}
	req := &server.PeekRequest{
		Database: dbName,
		Bucket:   bucketName,
		Limit:    limit,
	}

	var resp server.PeekResponse
	if err := h.send(transport.OpFIFOPeek, req, &resp); err != nil {
		return fmt.Errorf("peek failed: %w", err)
	}

	if !resp.Found {
		return h.printMiss(resp.Message, "EMPTY  "+resp.Message+claimedNote(bucketName, resp.Pending, 0, 0))
	}
	for _, entry := range resp.Items {
		h.printf("%s\n", h.doc(entry))
	}
	if resp.Expired {
		h.printf("%s\n", claimedBeside(bucketName, resp.Pending, 0, true))
	}
	return nil
}

// handleUpdate applies the update operators to an existing document.
func (h *CommandHandler) handleUpdate(dbName, bucketName string, args []string) error {
	req, err := buildUpdateRequest(dbName, bucketName, args)
	if err != nil {
		return err
	}
	var resp server.UpdateResponse
	if err := h.send(transport.OpUpdate, req, &resp); err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	if !resp.Success {
		return &RefusedError{Message: resp.Message}
	}

	h.printf("OK  updated %q\n", req.Key)
	h.printf("%s\n", h.bucketDoc(resp.Doc, dbName, bucketName))
	return nil
}

// buildUpdateRequest reads db.<bucket>.update("<key>", {operators}) into the
// request a lone update and a batch's update item both send.
func buildUpdateRequest(dbName, bucketName string, args []string) (*server.UpdateRequest, error) {
	if len(args) < 2 {
		return nil, usagef(`usage: db.%s.update("<key>", {$set: {field: value}})
       supported operators: $set {field: value}, $unset ["field"], $inc {field: N},
       $push {field: value}, $addToSet {field: value}, $mul {field: N},
       $min {field: value}, $max {field: value}, $rename {oldName: "newName"}`,
			bucketName)
	}

	key, err := keyFromArgs(args[:1])
	if err != nil {
		return nil, fmt.Errorf("update: %w", err)
	}
	if key == "" {
		return nil, usagef("update needs a key: db.%s.update(\"my-key\", {$set: {field: value}})", bucketName)
	}

	req := &server.UpdateRequest{Database: dbName, Bucket: bucketName, Key: key}
	for _, arg := range args[1:] {
		ops, err := parseDocument(arg)
		if err != nil {
			return nil, fmt.Errorf("update: %w", err)
		}
		if err := applyUpdateOperators(req, ops); err != nil {
			return nil, err
		}
	}
	if updateIsEmpty(req) {
		return nil, usagef(`nothing to update: pass an operator document, e.g.
       db.%s.update("%s", {$set: {field: value}})`, bucketName, key)
	}
	return req, nil
}

// updateOperatorNames is every operator applyUpdateOperators takes. The refusal
// of an unknown operator lists them, and a test holds the help text to the same
// set, so the two cannot drift again.
var updateOperatorNames = []string{"$set", "$unset", "$inc", "$push", "$addToSet", "$mul", "$min", "$max", "$rename"}

// updateIsEmpty reports whether no operator filled the request.
func updateIsEmpty(req *server.UpdateRequest) bool {
	return req.Set == nil && len(req.Unset) == 0 && req.Inc == nil && req.Push == nil &&
		req.AddToSet == nil && req.Mul == nil && req.Min == nil && req.Max == nil && req.Rename == nil
}

// applyUpdateOperators folds one operator document into an UpdateRequest.
func applyUpdateOperators(req *server.UpdateRequest, ops map[string]interface{}) error {
	for op, value := range ops {
		var err error
		switch op {
		case "$set":
			err = foldOperand(&req.Set, op, `{$set: {field: value}}`, value)
		case "$unset":
			err = foldUnset(req, value)
		case "$inc":
			err = foldInc(req, value)
		case "$push":
			err = foldOperand(&req.Push, op, `{$push: {field: value}}`, value)
		case "$addToSet":
			err = foldOperand(&req.AddToSet, op, `{$addToSet: {field: value}}`, value)
		case "$mul":
			err = foldMul(req, value)
		case "$min":
			err = foldOperand(&req.Min, op, `{$min: {field: value}}`, value)
		case "$max":
			err = foldOperand(&req.Max, op, `{$max: {field: value}}`, value)
		case "$rename":
			err = foldRename(req, value)
		default:
			return usagef("%s is not supported — supported update operators: %s", op, strings.Join(updateOperatorNames, ", "))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// operandDocument reads an operator's {field: ...} document.
func operandDocument(op, form string, value interface{}) (map[string]interface{}, error) {
	fields, ok := value.(map[string]interface{})
	if !ok {
		return nil, usagef("%s needs a document: %s", op, form)
	}
	return fields, nil
}

// foldOperand merges the operators whose operand is kept as typed: $set,
// $push, $addToSet, $min and $max. The server checks the value against the
// field it lands on.
func foldOperand(target *map[string]interface{}, op, form string, value interface{}) error {
	fields, err := operandDocument(op, form, value)
	if err != nil {
		return err
	}
	if *target == nil {
		*target = make(map[string]interface{}, len(fields))
	}
	for k, v := range fields {
		(*target)[k] = v
	}
	return nil
}

func foldUnset(req *server.UpdateRequest, value interface{}) error {
	fields, err := stringList(value)
	if err != nil {
		return usagef("$unset needs a list of field names: {$unset: [\"a\", \"b\"]}")
	}
	req.Unset = append(req.Unset, fields...)
	return nil
}

func foldInc(req *server.UpdateRequest, value interface{}) error {
	fields, err := operandDocument("$inc", `{$inc: {field: N}}`, value)
	if err != nil {
		return err
	}
	if req.Inc == nil {
		req.Inc = make(map[string]int64, len(fields))
	}
	for k, v := range fields {
		n, ok := numberToInt64(v)
		if !ok {
			return usagef("$inc on %q needs a whole number that fits in 64 bits, got %v\n       write a whole number, or $set the new value: {$set: {%s: <value>}}", k, v, k)
		}
		req.Inc[k] += n
	}
	return nil
}

// foldMul reads $mul's factors. The server multiplies as a double, so a
// fraction is fine here, unlike $inc's whole number.
func foldMul(req *server.UpdateRequest, value interface{}) error {
	fields, err := operandDocument("$mul", `{$mul: {field: N}}`, value)
	if err != nil {
		return err
	}
	if req.Mul == nil {
		req.Mul = make(map[string]float64, len(fields))
	}
	for k, v := range fields {
		factor, ok := numberToFloat64(v)
		if !ok {
			return usagef("$mul on %q needs a number, got %v\n       write a number, or $set the new value: {$set: {%s: <value>}}", k, v, k)
		}
		req.Mul[k] = factor
	}
	return nil
}

// foldRename reads $rename's pairs: the new name is a string, a path to a
// member (the server refuses a position on either side).
func foldRename(req *server.UpdateRequest, value interface{}) error {
	const form = `{$rename: {oldName: "newName"}}`
	fields, err := operandDocument("$rename", form, value)
	if err != nil {
		return err
	}
	if req.Rename == nil {
		req.Rename = make(map[string]string, len(fields))
	}
	for oldName, v := range fields {
		newName, ok := v.(string)
		if !ok {
			return usagef("$rename of %q needs the new name as a string, got %v\n       write %s", oldName, v, form)
		}
		req.Rename[oldName] = newName
	}
	return nil
}

// numberToFloat64 reads a parsed number of any width as a double.
func numberToFloat64(value interface{}) (float64, bool) {
	switch t := value.(type) {
	case float64:
		return t, true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	}
	return 0, false
}

// stringList reads a list of field names. A document parsed as Extended JSON
// carries its lists as primitive.A, one built in process as []interface{}.
func stringList(value interface{}) ([]string, error) {
	var list []interface{}
	switch typed := value.(type) {
	case []interface{}:
		list = typed
	case primitive.A:
		list = typed
	case string:
		return []string{typed}, nil
	default:
		return nil, usagef("not a list")
	}
	fieldNames := make([]string, 0, len(list))
	for _, fieldName := range list {
		fieldNames = append(fieldNames, scalarString(fieldName))
	}
	return fieldNames, nil
}

// numberToInt64 narrows a parsed number to an integer. The data parser sends
// int32 and int64, an option document float64. A double is accepted only when
// it is whole and fits an int64: anything else would be truncated or converted
// to an undefined value, and the number the user typed would quietly become
// another one.
func numberToInt64(value interface{}) (int64, bool) {
	switch t := value.(type) {
	case float64:
		// 2^63 is the first double that no longer fits an int64.
		if t != math.Trunc(t) || t >= 1<<63 || t < -(1<<63) {
			return 0, false
		}
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	}
	return 0, false
}

// heapPriority reads a heap priority out of a parsed document's "priority"
// value: an int32, an int64 or a whole double. isNumber is false for anything
// else, which is not a priority and is left in the document. A fraction is a
// number that cannot be one, so fractionErr says how to write it; the caller
// raises it only where a priority is consumed, because on any other engine
// "priority" is the user's own field.
func heapPriority(value interface{}) (priority int64, isNumber bool, fractionErr error) {
	switch typed := value.(type) {
	case int32:
		return int64(typed), true, nil
	case int64:
		return typed, true, nil
	case float64:
		// 2^63 is the first double that no longer fits an int64.
		if typed != math.Trunc(typed) || typed >= 1<<63 || typed < -(1<<63) {
			return 0, true, usagef("priority must be a whole number that fits in 64 bits, got %v\n       a heap orders by an integer: write {priority: 3}", typed)
		}
		return int64(typed), true, nil
	}
	return 0, false, nil
}

// handleCount asks the server for a bucket's entry count, optionally filtered.
func (h *CommandHandler) handleCount(dbName, bucketName string, args []string) error {
	filter := make(map[string]interface{})
	if len(args) > 0 {
		doc, err := parseDocument(args[0])
		if err != nil {
			return fmt.Errorf("count: %w", err)
		}
		filter = doc
	}

	var resp server.CountResponse
	req := &server.CountRequest{
		Database: dbName,
		Bucket:   bucketName,
		Filter:   filter,
	}
	if err := h.send(transport.OpCount, req, &resp); err != nil {
		return fmt.Errorf("count failed: %w", err)
	}
	if resp.Message != "" {
		return &RefusedError{Message: resp.Message}
	}
	// The dead hint is len()'s alone, and only when the number is 0
	// (claim-ack-spec §8.1): a bucket with live entries is not an operator's
	// dead-letter incident.
	var dead int64
	if resp.Count == 0 {
		dead = resp.Dead
	}
	if beside := claimedBeside(bucketName, resp.Pending, dead, resp.Expired); beside != "" {
		h.printf("%d  %s\n", resp.Count, beside)
		return nil
	}
	h.printf("%d\n", resp.Count)
	return nil
}

// handleDistinct asks the server for the distinct values of a field, optionally filtered.
func (h *CommandHandler) handleDistinct(dbName, bucketName string, args []string) error {
	if len(args) == 0 {
		return usagef(`usage: db.%s.distinct("field") or db.%s.distinct("field", {filter})`, bucketName, bucketName)
	}

	fieldStr := strings.TrimSpace(args[0])
	fieldStr = strings.Trim(fieldStr, `"'`)

	filter := make(map[string]interface{})
	if len(args) > 1 {
		doc, err := parseDocument(args[1])
		if err != nil {
			return fmt.Errorf("distinct: %w", err)
		}
		filter = doc
	}

	var resp server.DistinctResponse
	req := &server.DistinctRequest{
		Database: dbName,
		Bucket:   bucketName,
		Field:    fieldStr,
		Filter:   filter,
	}
	if err := h.send(transport.OpDistinct, req, &resp); err != nil {
		return fmt.Errorf("distinct failed: %w", err)
	}
	if resp.Message != "" {
		return &RefusedError{Message: resp.Message}
	}
	if len(resp.Values) == 0 {
		h.printf("NO DISTINCT VALUES\n")
		return nil
	}
	for _, v := range resp.Values {
		data, _ := json.Marshal(printableValue(v))
		h.printf("%s\n", string(data))
	}
	return nil
}

// handleBlobPut stores bytes in a blob bucket.
func (h *CommandHandler) handleBlobPut(dbName, bucketName string, args []string) error {
	if len(args) == 0 {
		return usagef(`usage: db.%s.put({_id_: "key", data: "text"})
       'data' is the payload; every other field is stored as metadata`, bucketName)
	}

	doc, err := parseDocument(args[0])
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	// An explicit empty key is not a request for a generated one — omitting it
	// is. Refuse it here, where the difference is still visible: the wire
	// carries only the key, which an omission renders the same way.
	for _, name := range keyFieldNames {
		raw, present := doc[name]
		if !present {
			continue
		}
		if text, isString := raw.(string); isString && text == "" {
			return usagef(`an _id_ must be a non-empty string, or omit it for a generated key — db.%s.put({_id_: "key", data: "text"})`, bucketName)
		}
	}

	key := ""
	if k, err := keyFromArgs(args[:1]); err == nil {
		key = k
	}

	payload, metadata := blobPayload(doc)
	if len(payload) == 0 {
		return usagef(`put needs a payload: db.%s.put({_id_: "key", data: "text"})`, bucketName)
	}

	if err := checkBlobFitsAServer(key, int64(len(payload))); err != nil {
		return err
	}

	req := &server.BlobWriteRequest{
		Database: dbName,
		Bucket:   bucketName,
		Key:      key,
		Data:     payload,
		Metadata: metadata,
	}

	var resp server.BlobWriteResponse
	if err := h.send(transport.OpBlobWrite, req, &resp); err != nil {
		return fmt.Errorf("put failed: %w", err)
	}
	if !resp.Success {
		return &RefusedError{Message: resp.Message}
	}

	h.printf("OK  key=%s  blob=%s  %s\n", terminalSafe(resp.Key), terminalSafe(resp.BlobID), humanBytes(int64(resp.Size)))
	return nil
}

// blobPayload splits a put document into the blob bytes and its metadata.
// A "data" string is stored as-is; otherwise the whole document (minus keys)
// is stored as JSON so any document can be a blob.
func blobPayload(doc map[string]interface{}) ([]byte, map[string]interface{}) {
	metadata := make(map[string]interface{}, len(doc))
	for k, v := range doc {
		metadata[k] = v
	}
	for _, name := range keyFieldNames {
		delete(metadata, name)
	}

	if raw, ok := metadata["data"]; ok {
		delete(metadata, "data")
		switch t := raw.(type) {
		case string:
			return []byte(t), metadata
		case nil:
			return nil, metadata
		default:
			if encoded, err := json.Marshal(t); err == nil {
				return encoded, metadata
			}
			return []byte(fmt.Sprintf("%v", t)), metadata
		}
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, metadata
	}
	return encoded, metadata
}

// handleBlobInfo prints a blob's metadata and size without the payload.
func (h *CommandHandler) handleBlobInfo(dbName, bucketName string, args []string) error {
	if len(args) == 0 {
		return usagef("usage: db.%s.info(\"<key>\")", bucketName)
	}
	key, err := keyFromArgs(args)
	if err != nil {
		return fmt.Errorf("info: %w", err)
	}
	if key == "" {
		return usagef("info needs a key: db.%s.info(\"my-key\")", bucketName)
	}

	var resp server.BlobReadResponse
	req := &server.BlobReadRequest{Database: dbName, Bucket: bucketName, Key: key}
	if err := h.send(transport.OpBlobRead, req, &resp); err != nil {
		return fmt.Errorf("info failed: %w", err)
	}
	if !resp.Found {
		return h.printMiss(resp.Message, "NOT FOUND")
	}

	h.printf("Key:      %s\n", terminalSafe(key))
	h.printf("Blob:     %s\n", terminalSafe(resp.BlobID))
	h.printf("Size:     %s\n", humanBytes(int64(resp.Size)))
	if len(resp.Metadata) > 0 {
		h.printf("Metadata: %s\n", h.doc(resp.Metadata))
	} else {
		h.printf("Metadata: (none)\n")
	}
	return nil
}

// readBlobToFile writes a blob's payload to a local file.
func (h *CommandHandler) readBlobToFile(dbName, bucketName, key, path string) error {
	var resp server.BlobReadResponse
	req := &server.BlobReadRequest{Database: dbName, Bucket: bucketName, Key: key}
	if err := h.send(transport.OpBlobRead, req, &resp); err != nil {
		return fmt.Errorf("get failed: %w", err)
	}
	if !resp.Found {
		return refusedf("blob %q not found in %s.%s", key, dbName, bucketName)
	}

	if err := os.WriteFile(path, resp.Data, 0o600); err != nil {
		return usagef("writing %s: %v", path, err)
	}
	h.printf("OK  wrote %s (%s)\n", path, humanBytes(int64(len(resp.Data))))
	return nil
}

// handleRange runs a btree range scan.
func (h *CommandHandler) handleRange(dbName, bucketName string, args []string) error {
	if len(args) == 0 {
		return usagef("usage: db.%s.range({$gte: low, $lte: high})", bucketName)
	}

	bounds, err := parseDocument(args[0])
	if err != nil {
		return fmt.Errorf("range: %w", err)
	}

	req := &server.BTreeRangeRequest{Database: dbName, Bucket: bucketName}
	if v, ok := bounds["$gte"]; ok {
		req.Low = scalarString(v)
	}
	if v, ok := bounds["$lte"]; ok {
		req.High = scalarString(v)
	}
	if req.Low == "" && req.High == "" {
		return usagef(`range needs bounds: db.%s.range({$gte: "a", $lte: "z"})`, bucketName)
	}

	var resp server.BTreeRangeResponse
	if err := h.send(transport.OpBTreeRange, req, &resp); err != nil {
		return fmt.Errorf("range failed: %w", err)
	}
	if resp.Message != "" {
		return &RefusedError{Message: resp.Message}
	}
	if len(resp.Entries) == 0 {
		h.printf("(no entries in range)\n")
		return nil
	}

	h.printf("%d entr%s in range:\n", len(resp.Entries), plural(len(resp.Entries), "y", "ies"))
	for _, entry := range resp.Entries {
		h.printf("  %-20s %s\n", terminalSafe(entry.Key), h.bucketDoc(entry.Value, dbName, bucketName))
	}
	return nil
}

// handleEdge prints the lowest (first) or highest (last) btree entry.
func (h *CommandHandler) handleEdge(dbName, bucketName string, opcode transport.Opcode, label string) error {
	var resp server.BTreeRangeResponse
	req := &server.BTreeRangeRequest{Database: dbName, Bucket: bucketName}
	if err := h.send(opcode, req, &resp); err != nil {
		return fmt.Errorf("%s failed: %w", label, err)
	}
	if resp.Message != "" {
		return &RefusedError{Message: resp.Message}
	}
	if len(resp.Entries) == 0 {
		h.printf("(bucket is empty)\n")
		return nil
	}
	entry := resp.Entries[0]
	h.printf("%s: %s\n", label, terminalSafe(entry.Key))
	h.printf("%s\n", h.bucketDoc(entry.Value, dbName, bucketName))
	return nil
}

// handleBucketStats prints a bucket's type, entries, logical size and indexes.
func (h *CommandHandler) handleBucketStats(dbName, bucketName string) error {
	var resp server.BucketStatsResponse
	req := &server.BucketStatsRequest{Database: dbName, Bucket: bucketName}
	if err := h.send(transport.OpBucketStats, req, &resp); err != nil {
		return fmt.Errorf("stats failed: %w", err)
	}

	h.printf("BUCKET %s.%s\n", terminalSafe(dbName), terminalSafe(bucketName))
	h.printf("---\n")
	h.printFacts(bucketFacts(resp, "(none)"))
	return nil
}

// plural picks the singular or plural suffix for a count.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// handleUser dispatches user management commands.
func (h *CommandHandler) handleUser(cmd *Cmd) error {
	switch cmd.Method {
	case "create_user":
		return h.handleUserCreate(cmd)
	case "alter_user":
		return h.handleUserAlter(cmd)
	case "drop_user":
		return h.handleUserDrop(cmd)
	case "disable_user":
		cmd.Options = map[string]interface{}{"detail": "disable"}
		return h.handleUserAlter(cmd)
	case "enable_user":
		cmd.Options = map[string]interface{}{"detail": "enable"}
		return h.handleUserAlter(cmd)
	case "grant_role":
		return h.handleUserGrant(cmd)
	case "revoke_role":
		return h.handleUserRevoke(cmd)
	case "list_users":
		return h.handleUserList()
	case "describe_user":
		return h.handleUserDescribe(cmd)
	case "grant_access", "revoke_access":
		return usagef("the 'access' form is not supported; grant a role instead:\n  grant role writer@%s to %s", cmd.Args[0], cmd.Name)
	default:
		return usagef("unknown user command: %s", cmd.Method)
	}
}

// handleJoin adds a node to the cluster. The add triggers the membership
// proposal and, once it commits, rebalancing — the operator does not run a
// separate migration command.
func (h *CommandHandler) handleJoin(cmd *Cmd) error {
	addr, _ := cmd.Options["seed"].(string)
	if addr == "" {
		return usagef("usage: join <address>  (add a node to the cluster; this starts rebalancing)")
	}
	var resp server.ClusterAddResponse
	if err := h.send(transport.OpClusterAdd, &server.ClusterAddRequest{Address: addr}, &resp); err != nil {
		return fmt.Errorf("join %q: %w", addr, err)
	}
	if !resp.Success {
		return refusedf("join %q: %s", addr, resp.Error)
	}
	h.printf("Added %s. %s\n", terminalSafe(addr), terminalSafeText(resp.Message))
	return nil
}

// handleMigrate pulls a bucket onto the node the CLI is connected to (its ring
// owner) so it becomes servable there. The data moves from a peer that still
// holds it; the fence that refused writes to this bucket lifts once it lands.
func (h *CommandHandler) handleMigrate(cmd *Cmd) error {
	if len(cmd.Args) < 2 {
		return usagef("usage: migrate <database> <bucket>")
	}
	var resp server.MigrateResponse
	if err := h.send(transport.OpMigrate, &server.MigrateRequest{Database: cmd.Args[0], Bucket: cmd.Args[1]}, &resp); err != nil {
		return fmt.Errorf("migrate %s %s: %w", cmd.Args[0], cmd.Args[1], err)
	}
	if !resp.Success {
		return refusedf("migrate %s %s: %s", cmd.Args[0], cmd.Args[1], resp.Error)
	}
	h.printf("%s\n", terminalSafeText(resp.Message))
	return nil
}

// handleConsistentGet runs a quorum read: the freshest copy across a quorum of
// holders, repairing a stale replica on the way if one is detected.
func (h *CommandHandler) handleConsistentGet(cmd *Cmd) error {
	if len(cmd.Args) < 3 {
		return usagef("usage: consistent-get <database> <bucket> <key>")
	}
	var resp server.ConsistentGetResponse
	if err := h.send(transport.OpConsistentGet, &server.ConsistentGetRequest{
		Database: cmd.Args[0], Bucket: cmd.Args[1], Key: cmd.Args[2],
	}, &resp); err != nil {
		return fmt.Errorf("consistent get %s: %w", cmd.Args[2], err)
	}
	if !resp.Found && resp.Message != "" {
		return refusedf("consistent get: %s", resp.Message)
	}
	if !resp.Found {
		h.printf("not found\n")
		return nil
	}
	h.printf("%s\n", h.bucketDoc(resp.Data, cmd.Args[0], cmd.Args[1]))
	if resp.Repaired {
		h.notef("note: this read repaired a replica\n")
	}
	return nil
}

// handleWriteConcern sets the session's per-write durability level.
func (h *CommandHandler) handleWriteConcern(cmd *Cmd) error {
	level := ""
	if len(cmd.Args) > 0 {
		level = cmd.Args[0]
	}
	level = strings.ToLower(strings.TrimSpace(level))
	switch level {
	case "one", "majority", "all":
		h.wc = level
		h.printf("Write concern set to %s for subsequent writes.\n", level)
		return nil
	case "":
		return usagef("usage: write-concern <one|majority|all>")
	default:
		return usagef("unknown write concern %q — use one, majority, or all", level)
	}
}

// handleRemoveNode removes a member from the cluster by majority vote. Run it
// from a node other than the one being removed.
func (h *CommandHandler) handleRemoveNode(cmd *Cmd) error {
	if len(cmd.Args) < 1 {
		return usagef("usage: remove-node <node-id>")
	}
	var resp server.ClusterRemoveResponse
	if err := h.send(transport.OpClusterRemove, &server.ClusterRemoveRequest{NodeID: cmd.Args[0]}, &resp); err != nil {
		return fmt.Errorf("remove-node %s: %w", cmd.Args[0], err)
	}
	if !resp.Success {
		return refusedf("remove-node %s: %s", cmd.Args[0], resp.Error)
	}
	h.printf("Removed %s. %s\n", terminalSafe(cmd.Args[0]), terminalSafeText(resp.Message))
	return nil
}

// handleAdmin handles database and bucket lifecycle commands.
func (h *CommandHandler) handleAdmin(cmd *Cmd) error {
	switch cmd.Method {
	case "create_database":
		return h.handleCreateDatabase(cmd)
	case "drop_database":
		return h.handleDropDatabase(cmd)
	case "create_bucket":
		return h.handleCreateBucket(cmd)
	case "drop_bucket":
		return h.handleDropBucket(cmd)
	case "alter_bucket":
		return h.handleAlterBucket(cmd)
	default:
		return usagef("unknown admin command: %s", cmd.Method)
	}
}

// handleCreateDatabase creates a database and switches the session to it.
func (h *CommandHandler) handleCreateDatabase(cmd *Cmd) error {
	if cmd.Database == "" {
		return usagef("usage: create database <name>")
	}

	var resp server.CreateDatabaseResponse
	req := &server.CreateDatabaseRequest{Name: cmd.Database}
	if err := h.send(transport.OpCreateDB, req, &resp); err != nil {
		return fmt.Errorf("create database %q: %w", cmd.Database, err)
	}
	if !resp.Success {
		return &RefusedError{Message: resp.Message}
	}

	h.printf("OK  created database %s\n", cmd.Database)
	h.printCreateDatabaseHint(cmd.Database)
	return nil
}

// printCreateDatabaseHint prints the next steps after a database is created:
// use first, then the bucket line. Use comes first because a fresh database
// never becomes the session's database (handleCreateDatabase does not touch the
// current database). Both type lists come from the store-type table, and the
// hint stays terse — the per-type detail lives in `help <type>`.
//
// The hint shows two vocabularies because a type has two spellings that are
// not interchangeable: `type: lifo` sets the bucket's engine, `Photos lifoStore`
// declares a store field each document carries. Showing only the first taught
// exactly one thing to write — `Photos lifo` — and the declaration parser
// refuses that, because a store field's engine must carry the Store suffix.
func (h *CommandHandler) printCreateDatabaseHint(dbName string) {
	h.printf("Next: use %s\n", dbName)
	h.printf("Then: create bucket <name> {type: %s}   (hash is the default)\n", storeTypeList())
	h.printf("     create bucket Post {Photos lifoStore, Comments fifoStore}\n")
	h.printf("     a store field is <name> <type>Store — %s\n", storeFieldList())
}

// handleDropDatabase drops a database and everything in it.
func (h *CommandHandler) handleDropDatabase(cmd *Cmd) error {
	if cmd.Database == "" {
		return usagef("usage: drop database <name>")
	}

	var resp server.DeleteDatabaseResponse
	req := &server.DeleteDatabaseRequest{Name: cmd.Database}
	if err := h.send(transport.OpDeleteDB, req, &resp); err != nil {
		return fmt.Errorf("drop database %q: %w", cmd.Database, err)
	}
	if !resp.Success {
		return &RefusedError{Message: resp.Message}
	}
	h.printf("OK  %s\n", terminalSafeText(resp.Message))
	return nil
}

// handleCreateBucket creates a bucket.
func (h *CommandHandler) handleCreateBucket(cmd *Cmd) error {
	if cmd.Name == "" {
		return usagef("usage: create bucket <name> { type: %s }", storeTypeList())
	}
	dbName := h.database(cmd)
	if dbName == "" {
		return usagef("no database selected — run 'use <database>' first (or 'create database <name>')")
	}

	schema, err := parseBucketSchema(cmd, dbName)
	if err != nil {
		return err
	}
	bucketType, options, scalars := schema.Type, schema.Options, schema.Scalars

	var resp server.CreateBucketResponse
	req := &server.CreateBucketRequest{
		Database:   dbName,
		Name:       cmd.Name,
		Type:       bucketType,
		SchemaJSON: schemaJSON(cmd),
		Options:    options,

		ReplicationFactor: schema.ReplicationFactor,
	}
	if err := h.send(transport.OpCreateBucket, req, &resp); err != nil {
		return fmt.Errorf("create bucket %q: %w", cmd.Name, err)
	}
	if !resp.Success {
		return &RefusedError{Message: resp.Message}
	}

	if strings.Contains(resp.Message, "already exists") {
		// Not created: say what the server said, including any factor the
		// create named that the existing bucket does not have.
		h.printf("OK  %s\n", terminalSafeText(resp.Message))
		return nil
	}
	// A hash bucket's line names the placement the server recorded; every other
	// type keeps the line it always had (document placement spec §3.3 item 5).
	kind := terminalSafe(string(bucketType))
	if resp.Placement != "" {
		kind += ", " + terminalSafe(resp.Placement)
	}
	h.printf("OK  created bucket %s.%s (type %s)\n", terminalSafe(dbName), terminalSafe(cmd.Name), kind)
	// The confirmation's two optional rows are one block, so they share a column.
	var declared []fact
	if indices, ok := options["indices"]; ok {
		declared = append(declared, fact{"Indexes", indexList(indices)})
	}
	if stores, ok := options["stores"].([]interface{}); ok {
		declared = append(declared, fact{"Stores", fmt.Sprintf("%d store field(s) declared", len(stores))})
	}
	h.printFacts(declared)
	if len(scalars) > 0 {
		h.printf("%s\n", scalarNote(scalars))
	}
	return nil
}

// handleDropBucket drops a bucket and its data.
func (h *CommandHandler) handleDropBucket(cmd *Cmd) error {
	if cmd.Name == "" {
		return usagef("%s", dropBucketUsage)
	}
	dbName := h.database(cmd)
	if dbName == "" {
		return noBucketDatabase("drop", cmd.Name)
	}

	var resp server.DeleteBucketResponse
	req := &server.DeleteBucketRequest{Database: dbName, Name: cmd.Name}
	if err := h.send(transport.OpDeleteBucket, req, &resp); err != nil {
		return fmt.Errorf("drop bucket %q: %w", cmd.Name, err)
	}
	if !resp.Success {
		return &RefusedError{Message: resp.Message}
	}
	h.printf("OK  %s\n", terminalSafeText(resp.Message))
	return nil
}

// parseBucketSchema reads the bucket type and options out of a create
// command:
//
//	create bucket Users { type: hash, indices: ["Email"] }
//	create bucket Events type fifo
//	create bucket Cache                 (defaults to hash)
func parseBucketSchema(cmd *Cmd, dbName string) (bucketSchema, error) {
	options := map[string]interface{}{}
	typeName := ""
	var declaration bucketDeclaration

	if cmd.Options != nil {
		// parseCreateBucket stores the schema under "schema"; "detail" is
		// accepted too because other admin verbs use it.
		for _, key := range []string{"schema", "detail"} {
			detail, ok := cmd.Options[key].(string)
			if !ok || detail == "" {
				continue
			}
			if doc, err := parseOptionDocument(detail); err == nil {
				for k, v := range doc {
					options[k] = v
				}
				break
			}
			if fields := strings.Fields(strings.Trim(detail, "{}")); len(fields) == 2 && strings.EqualFold(fields[0], "type") {
				if candidate, ok := bucketTypes[strings.ToLower(fields[1])]; ok {
					typeName = string(candidate)
					break
				}
			}
			// A body that is no document is a declaration: store fields,
			// scalar fields and options, entry by entry. Before, it was
			// dropped here and a plain hash bucket created in its place.
			parsed, err := parseDeclarationBody(detail)
			if err != nil {
				return bucketSchema{}, fmt.Errorf("create bucket %s: %w", cmd.Name, err)
			}
			if len(parsed.removals) > 0 {
				return bucketSchema{}, usagef("create bucket %s: %q removes a field, and a create only declares them", cmd.Name, parsed.removals[0])
			}
			declaration = parsed
			for k, v := range parsed.options {
				options[k] = v
			}
			break
		}
		if cfg, ok := cmd.Options["config"].(string); ok {
			if doc, err := parseOptionDocument(cfg); err == nil {
				for k, v := range doc {
					options[k] = v
				}
			}
		}
	}
	if raw, ok := options["type"]; ok {
		typeName = strings.ToLower(scalarString(raw))
	}
	if typeName == "" {
		typeName = "hash"
	}

	bucketType, ok := bucketTypes[typeName]
	if !ok {
		return bucketSchema{}, unknownBucketTypeError(typeName)
	}
	// The declaration goes to the server as written — names and objects alike —
	// and the server's parser is the one that refuses a bad one. Coercing each
	// element to a string sent an index object as its JSON text, which the
	// server then indexed as a field of that name.
	if raw, ok := options["indices"]; ok {
		if name, isName := raw.(string); isName {
			options["indices"] = []interface{}{name}
		}
	}
	replicationFactor := 0
	if raw, ok := options["replication_factor"]; ok {
		parsed, err := replicationFactorOption(raw)
		if err != nil {
			return bucketSchema{}, fmt.Errorf("create bucket %s: %w", cmd.Name, err)
		}
		replicationFactor = parsed
	}
	delete(options, "type")
	delete(options, "replication_factor")
	if indices := declaration.indicesOption(options["indices"]); indices != nil {
		options["indices"] = indices
	}
	if len(declaration.stores) > 0 {
		if err := declaration.checkNames(cmd.Name); err != nil {
			return bucketSchema{}, err
		}
		// The server's own validator, called before anything is sent, so the
		// CLI refuses exactly what the server refuses (spec §4.5, T-11).
		if err := server.ValidateStoreDeclarations(server.StoreValidation{
			Database: dbName, Bucket: cmd.Name, BucketType: bucketType, Added: declaration.stores,
		}); err != nil {
			return bucketSchema{}, usagef("%v", err)
		}
		options["stores"] = storesOption(declaration.stores)
	}
	return bucketSchema{Type: bucketType, Options: options, Scalars: declaration.scalarsToNote(), ReplicationFactor: replicationFactor}, nil
}

// bucketSchema is what a create-bucket body declares: the store engine, the
// scalar fields it named, the per-bucket properties lifted out into typed
// values, and the options that still travel to the server in the opaque map.
//
// It is a struct rather than a fifth return value because a growing tuple is
// the shape root AGENTS.md slop rule 7 is about.
type bucketSchema struct {
	Type    server.BucketType
	Options map[string]interface{}
	Scalars []string
	// ReplicationFactor is the declared number of copies; zero means the body
	// declared none and the bucket inherits the node's factor.
	ReplicationFactor int
}

// replicationFactorOption reads a per-bucket replication factor out of a
// create-bucket body. It is a count of copies, so a fraction, a string and a
// number below one are all refused here, at the CLI, where the message can
// name the form that works — rather than travelling to the server inside the
// options map whose contract is "ignore what you do not know".
func replicationFactorOption(raw interface{}) (int, error) {
	count, isNumber := raw.(float64)
	if !isNumber || count < 1 || count != float64(int(count)) {
		return 0, usagef(
			"replication_factor must be a whole number of copies, 1 or more, got %s. Write it as a number, or leave it out to inherit the node's factor: create bucket Sessions { replication_factor: 3 }",
			scalarString(raw))
	}
	return int(count), nil
}

// bucketTypes is defined in store_types.go, derived from the store-type
// table: parser vocabulary, protocol type, completion and hints all read the
// one table so a new store type is wired in one place.

// schemaJSON returns the raw schema text of a create command, if any.
func schemaJSON(cmd *Cmd) string {
	if cmd.Options == nil {
		return ""
	}
	if schema, ok := cmd.Options["schema"].(string); ok {
		return schema
	}
	return ""
}

// ErrExit is a sentinel error to signal the REPL to exit.
var ErrExit = fmt.Errorf("exit")
