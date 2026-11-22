package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The HQL batch statement (batch-request spec §8.3):
//
//	batch {
//	  db.User_7["7"].Posts.put("p-1001", {text: "hello"});
//	  db.User_42["42"].Feed.push({author: "7", post: "p-1001"});
//	  db.User_7.update("7", {$inc: {posts_n: 1}})
//	}
//
// sends its statements as one BATCH request. A batch is not a transaction:
// each statement is applied, refused or left unknown on its own, and the table
// says which. Statements of one bucket apply in order, and after one that is
// not applied the later ones of its bucket are not run.

// BatchOutcomeError is a batch that was refused whole, or of which some
// statement was not a success. Exit is the process's exit status in a piped
// run: ExitRefused for a whole-batch refusal, ExitPartial otherwise.
type BatchOutcomeError struct {
	Exit    int
	Message string
	// Cause is the server's refusal of the whole batch, when it was refused.
	Cause error
}

func (e *BatchOutcomeError) Error() string { return terminalSafeText(e.Message) }

func (e *BatchOutcomeError) Unwrap() error { return e.Cause }

// parseBatch reads `batch { <statement>; ... }`. The statements are split on
// the semicolons outside quotes and brackets, and each is parsed when the
// batch runs, as a statement typed alone is.
func parseBatch(line string) (*Cmd, error) {
	rest := strings.TrimSpace(line[len("batch"):])
	if !strings.HasPrefix(rest, "{") || !strings.HasSuffix(rest, "}") {
		return nil, usagef("usage: batch { <statement>; <statement>; ... }, e.g.\n" +
			"       batch { db.User[\"ada\"].Feed.push({text: \"hi\"}); db.User.update(\"ada\", {$inc: {posts: 1}}) }")
	}
	body := rest[1 : len(rest)-1]
	var statements []string
	for _, statement := range splitStatements(body) {
		if statement = strings.TrimSpace(statement); statement != "" {
			statements = append(statements, statement)
		}
	}
	if len(statements) == 0 {
		return nil, usagef("a batch needs at least one statement: batch { db.User[\"ada\"].Feed.push({text: \"hi\"}) }")
	}
	return &Cmd{Type: "global", Method: "batch", Args: statements}, nil
}

// splitStatements splits text on the semicolons outside quotes and brackets.
func splitStatements(text string) []string {
	var parts []string
	var quote byte
	depth, start := 0, 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '{' || c == '[' || c == '(':
			depth++
		case (c == '}' || c == ']' || c == ')') && depth > 0:
			depth--
		case c == ';' && depth == 0:
			parts = append(parts, text[start:i])
			start = i + 1
		}
	}
	return append(parts, text[start:])
}

// batchStatement is one statement of a batch and the item it sends.
type batchStatement struct {
	item server.BatchItem
	form string
}

// handleBatch runs a batch statement: every statement is read into an item,
// the batch is checked against the server's own rules, sent as one request,
// and answered with one row per statement and a summary.
func (h *CommandHandler) handleBatch(cmd *Cmd) error {
	dbName := h.database(cmd)
	if dbName == "" {
		return usagef("no database selected — run 'use <database>' first ('show databases' lists what exists)")
	}
	if h.conn == nil {
		return ErrNotConnected
	}
	statements := make([]batchStatement, 0, len(cmd.Args))
	for i, text := range cmd.Args {
		statement, err := h.batchStatementOf(dbName, i, len(cmd.Args), text)
		if err != nil {
			return err
		}
		statements = append(statements, statement)
	}
	req := server.BatchRequest{Items: make([]server.BatchItem, len(statements)), WriteConcern: h.wc}
	for i, statement := range statements {
		req.Items[i] = statement.item
	}
	// Refused before anything is sent, in the server's own words.
	// It is the same whole refusal the server would send, so it exits the same
	// way (ExitRefused) for a script that tells "nothing ran" from a bad command.
	if err := server.CheckBatch(req); err != nil {
		return &BatchOutcomeError{Exit: ExitRefused, Message: err.Error(), Cause: usagef("%s", err.Error())}
	}
	var resp server.BatchResponse
	if err := h.send(transport.OpBatch, &req, &resp); err != nil {
		var serverErr *ServerError
		if errors.As(err, &serverErr) {
			return &BatchOutcomeError{Exit: ExitRefused, Message: err.Error(), Cause: err}
		}
		return err
	}
	if len(resp.Answers) != len(statements) {
		return &BatchOutcomeError{Exit: ExitUnavailable, Message: fmt.Sprintf(
			"the server answered %d of the %d statements; treat every write of the batch as possibly stored and read it back before resending", len(resp.Answers), len(statements))}
	}
	return h.printBatchAnswers(statements, resp.Answers)
}

// batchStatementOf reads statement i of total into the item it sends. Only a
// store field's push, put, delete, get, len and peek and an update of one key
// are carried; pop and list are read so the server's own refusal names them.
func (h *CommandHandler) batchStatementOf(dbName string, i, total int, text string) (batchStatement, error) {
	position := fmt.Sprintf("statement %d of %d (%s)", i+1, total, text)
	cmd, err := ParseCommand(text)
	if err != nil {
		return batchStatement{}, usagef("%s: %v", position, err)
	}
	if cmd.Type != "bucket" {
		return batchStatement{}, usagef("%s is not one a batch carries. A batch carries push, put, delete, get, len and peek of a store field, and an update of one key.", position)
	}
	if cmd.Database != "" && cmd.Database != dbName {
		return batchStatement{}, usagef("%s names database %q; a batch statement addresses the session database (%s)", position, cmd.Database, dbName)
	}
	op, request, err := h.batchRequestOf(dbName, cmd)
	if err != nil {
		return batchStatement{}, fmt.Errorf("%s: %w", position, err)
	}
	payload, err := bson.Marshal(request)
	if err != nil {
		return batchStatement{}, fmt.Errorf("%s: encoding the request: %w", position, err)
	}
	return batchStatement{item: server.BatchItem{Opcode: int32(op), Payload: payload}, form: server.DescribeBatchItem(op, payload)}, nil
}

// batchRequestOf is the opcode and request a statement sends, read as the
// statement's own handler reads it.
func (h *CommandHandler) batchRequestOf(dbName string, cmd *Cmd) (transport.Opcode, any, error) {
	method := strings.ToLower(cmd.Method)
	if cmd.DocKey == "" {
		if method != "update" {
			return 0, nil, usagef("db.%s.%s() is not one a batch carries. A batch carries push, put, delete, get, len and peek of a store field, and an update of one key.", cmd.Bucket, method)
		}
		req, err := buildUpdateRequest(dbName, cmd.Bucket, cmd.Args)
		return transport.OpUpdate, req, err
	}
	req := server.FieldRequest{Database: dbName, Bucket: cmd.Bucket, Key: cmd.DocKey, Field: cmd.Field, Path: cmd.Path}
	address := fmt.Sprintf("db.%s[%q].%s.%s()", cmd.Bucket, cmd.DocKey, cmd.Field, method)
	where := fmt.Sprintf("db.%s[%q].%s", cmd.Bucket, cmd.DocKey, cmd.Field)
	switch method {
	case "push":
		if len(cmd.Args) > 0 {
			data, err := parseDocument(cmd.Args[0])
			if err != nil {
				return 0, nil, err
			}
			req.Data = data
			if priority, isNumber, fractionErr := heapPriority(data["priority"]); isNumber {
				if fractionErr != nil {
					if err := h.refuseFractionOnHeapField(dbName, cmd, address, fractionErr); err != nil {
						return 0, nil, err
					}
				} else {
					req.Priority = priority
				}
			}
		}
		return transport.OpFieldPush, &req, nil
	case "put":
		if len(cmd.Args) < 2 {
			return 0, nil, usagef(`usage: db.%s[%q].%s.put("key", {...})`, cmd.Bucket, cmd.DocKey, cmd.Field)
		}
		req.Element = unquote(cmd.Args[0])
		data, err := parseDocument(cmd.Args[1])
		if err != nil {
			return 0, nil, err
		}
		req.Data = data
		if _, hasPayload := data["data"]; hasPayload {
			isBlob, err := h.fieldIsBlob(dbName, cmd)
			if err != nil {
				return 0, nil, err
			}
			if isBlob {
				req.Content, req.Data = blobPayload(data)
			}
		}
		return transport.OpFieldPut, &req, nil
	case "delete":
		if len(cmd.Args) < 1 {
			return 0, nil, usagef(`usage: db.%s[%q].%s.delete("key")`, cmd.Bucket, cmd.DocKey, cmd.Field)
		}
		req.Element = unquote(cmd.Args[0])
		return transport.OpFieldDelete, &req, nil
	case "get":
		if len(cmd.Args) < 1 {
			return 0, nil, usagef(`usage: db.%s[%q].%s.get("key")`, cmd.Bucket, cmd.DocKey, cmd.Field)
		}
		req.Element = unquote(cmd.Args[0])
		return transport.OpFieldGet, &req, nil
	case "len", "length", "count":
		return transport.OpFieldLen, &req, nil
	case "peek":
		limit, err := readVerbLimit("peek", where, cmd.Args)
		if err != nil {
			return 0, nil, err
		}
		return transport.OpFieldPeek, &server.FieldPeekRequest{FieldRequest: req, Limit: limit}, nil
	case "pop":
		return transport.OpFieldPop, &server.FieldPopRequest{FieldRequest: req}, nil
	case "list":
		return transport.OpFieldList, &server.FieldListRequest{FieldRequest: req}, nil
	}
	return 0, nil, usagef("%q is not a field-store verb a batch carries: push, put, delete, get, len (or length, count) and peek", cmd.Method)
}

// batchTally counts a batch's answers by §4.1's table.
type batchTally struct {
	succeeded, notApplied, applied, unknown, failed, readFailed int
}

// printBatchAnswers prints one row per statement, with the lone forms of each
// result, and the summary; any statement that is not a success makes the batch
// partial.
func (h *CommandHandler) printBatchAnswers(statements []batchStatement, answers []server.BatchAnswer) error {
	formWidth := len("statement")
	for _, statement := range statements {
		formWidth = max(formWidth, len([]rune(statement.form)))
	}
	numberWidth := len(fmt.Sprint(len(statements)))
	h.printf("%*s  %-*s  %s\n", numberWidth+1, "#", formWidth, "statement", "result")
	var tally batchTally
	for i, answer := range answers {
		result := h.batchResult(statements[i].item, answer, &tally)
		form := statements[i].form
		pad := formWidth - len([]rune(form))
		h.printf("%*d  %s%s  %s\n", numberWidth+1, i+1, terminalSafeText(form), strings.Repeat(" ", pad), result)
	}
	writes := false
	for _, statement := range statements {
		writes = writes || !transport.Opcode(statement.item.Opcode).IsReadOnly()
	}
	summary := tally.summary(len(statements), writes)
	h.printf("%s\n", summary)
	if tally.succeeded == len(statements) {
		return nil
	}
	return &BatchOutcomeError{Exit: ExitPartial, Message: "some statements of the batch were not a success; the table above says which, and which are safe to resend"}
}

// batchResult is one row's result column, in the lone statement's words.
func (h *CommandHandler) batchResult(item server.BatchItem, answer server.BatchAnswer, tally *batchTally) string {
	op := transport.Opcode(item.Opcode)
	if transport.Opcode(answer.Opcode) == transport.OpError {
		var refusal server.ErrorResponse
		if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
			tally.unknown++
			return "ERROR: an answer that could not be read"
		}
		switch {
		case op.IsReadOnly():
			tally.readFailed++
		case refusal.WriteOutcome == "not_applied":
			tally.notApplied++
		case refusal.WriteOutcome == "applied":
			tally.applied++
		default:
			tally.unknown++
		}
		return "ERROR: " + terminalSafeText(refusal.Message)
	}
	raw := bson.Raw(answer.Payload)
	if success, isBool := raw.Lookup("success").BooleanOK(); isBool && !success {
		tally.failed++
		message, _ := raw.Lookup("message").StringValueOK()
		return "ERROR: " + terminalSafeText(message)
	}
	tally.succeeded++
	switch op {
	case transport.OpFieldGet, transport.OpFieldPeek:
		var resp server.FieldResponse
		if err := bson.Unmarshal(answer.Payload, &resp); err != nil || !resp.Found {
			if op == transport.OpFieldPeek {
				return "EMPTY"
			}
			return "NOT FOUND"
		}
		if op == transport.OpFieldGet {
			return h.doc(resp.Element)
		}
		entries := make([]string, len(resp.Items))
		for i, entry := range resp.Items {
			entries[i] = h.doc(entry.Element)
		}
		return strings.Join(entries, ", ")
	case transport.OpFieldLen:
		n, _ := raw.Lookup("len").AsInt64OK()
		return fmt.Sprint(n)
	case transport.OpFieldDelete:
		if found, _ := raw.Lookup("found").BooleanOK(); !found {
			key, _ := raw.Lookup("element_key").StringValueOK()
			return "NOT FOUND  " + terminalSafe(key)
		}
	case transport.OpUpdate:
		if omitted, _ := raw.Lookup("doc_omitted").BooleanOK(); omitted {
			return "OK (the document was left out: the batch's reply was full; read it if you need it)"
		}
	}
	return "OK" + fewerCopiesNote(raw)
}

// fewerCopiesNote is the lone write's note when fewer copies than a majority
// confirmed it, shortened for a row.
func fewerCopiesNote(raw bson.Raw) string {
	confirmed, confirmedOK := raw.Lookup("copies_confirmed").AsInt64OK()
	factor, factorOK := raw.Lookup("replication_factor").AsInt64OK()
	if confirmedOK && factorOK && confirmed < int64(server.QuorumSize(int(factor))) {
		return fmt.Sprintf("  stored on %d of %d copies; run 'status'", confirmed, factor)
	}
	return ""
}

// summary is the line under the table; hasWrites says whether any statement
// stores something.
func (t batchTally) summary(total int, hasWrites bool) string {
	parts := []string{fmt.Sprintf("%d succeeded", t.succeeded)}
	if t.notApplied > 0 {
		parts = append(parts, fmt.Sprintf("%d not applied (nothing stored for %s; safe to resend)", t.notApplied, itOrThem(t.notApplied)))
	}
	if t.applied > 0 {
		parts = append(parts, fmt.Sprintf("%d stored on fewer copies than asked (do not resend a push or an $inc)", t.applied))
	}
	if t.unknown > 0 {
		parts = append(parts, fmt.Sprintf("%d unknown (may be stored; read %s back before resending a push or an $inc)", t.unknown, itOrThem(t.unknown)))
	}
	if t.failed > 0 {
		parts = append(parts, fmt.Sprintf("%d refused by the store (resending fails the same way; fix the cause)", t.failed))
	}
	if t.readFailed > 0 {
		parts = append(parts, fmt.Sprintf("%d reads not answered (nothing changed; safe to resend)", t.readFailed))
	}
	line := fmt.Sprintf("%d %s: %s.", total, plural(total, "statement", "statements"), strings.Join(parts, ", "))
	// The warning is about writes; a batch of reads has nothing to apply.
	if hasWrites {
		line += " Statements in one batch are not applied together."
	}
	return line
}

func itOrThem(n int) string {
	if n == 1 {
		return "it"
	}
	return "them"
}
