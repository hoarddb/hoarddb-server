package wire

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// This file is the codec and the server end of the three seeding commands of RING_ADMIN
// (entry-ring-admin-wire.md, "Seeding"): seeding_status, seeding_retry and seeding_forget.
// The codec follows ringadmin.go: members in the stated order, none duplicated or extra, an
// optional member absent rather than written as its default, sizes checked before a document
// is unpacked. The server end forwards to the node's SeedingView and nothing else.

const (
	// AdminSeedingStatus lists the slot pulls of this node.
	AdminSeedingStatus = "seeding_status"
	// AdminSeedingRetry restarts the pull of one slot.
	AdminSeedingRetry = "seeding_retry"
	// AdminSeedingForget removes the retired record of one slot and keeps its range.
	AdminSeedingForget = "seeding_forget"
)

const (
	// seedingMaxPage is the ceiling of a seeding_status page. A worst-case row is about 2.3
	// KiB, so a full page is under 2.5 MiB and always fits one frame.
	seedingMaxPage = 1000
	// seedingMaxRefusal bounds the refusal text of a row, in bytes.
	seedingMaxRefusal = 512
	// seedingCursorMax is the longest cursor: two bytes of slot and a database name.
	seedingCursorMax = 2 + ringMaxName
)

// RingAdminSeedRow is one slot of one database in a seeding_status reply. Every member is
// always written: a text with no value is the empty string and a number with none is 0.
type RingAdminSeedRow struct {
	// Database and Slot name the pull.
	Database string
	Slot     int32
	// State is one of placing, seeding, stalled, ready and retired.
	State string
	// Source is the node id the pull reads from, or empty.
	Source string
	// BytesDone, BytesKnown, Entries and IdleMS are the counters of SeedRow, with the idle time in milliseconds.
	BytesDone, BytesKnown, Entries, IdleMS int64
	// Refusal is the last refusal text of the source, or empty.
	Refusal string
}

// RingAdminSeeding is the reply of the three seeding commands. A seeding_status reply sets
// Rows (non-nil, possibly empty), More and Cursor; a seeding_retry or seeding_forget reply
// sets Database and Slot, the slot it acted on, and leaves the rest zero.
type RingAdminSeeding struct {
	Rows   []RingAdminSeedRow
	More   bool
	Cursor []byte
	// Database and Slot answer seeding_retry and seeding_forget.
	Database string
	Slot     int32
}

type adminSeedingStatusDoc struct {
	Cmd      string `bson:"cmd"`
	Database string `bson:"database,omitempty"`
	Slot     *int32 `bson:"slot,omitempty"`
	Limit    *int32 `bson:"limit,omitempty"`
	Cursor   []byte `bson:"cursor,omitempty"`
}

type adminSeedingActionDoc struct {
	Cmd      string `bson:"cmd"`
	Database string `bson:"database"`
	Slot     int32  `bson:"slot"`
	// Anyway is left out unless true, so a retry and a plain forget send the document every
	// earlier build sent.
	Anyway bool `bson:"anyway,omitempty"`
}

type adminSeedRowDoc struct {
	Database   string `bson:"database"`
	Slot       int32  `bson:"slot"`
	State      string `bson:"state"`
	Source     string `bson:"source"`
	BytesDone  int64  `bson:"bytes_done"`
	BytesKnown int64  `bson:"bytes_known"`
	Entries    int64  `bson:"entries"`
	IdleMS     int64  `bson:"idle_ms"`
	Refusal    string `bson:"refusal"`
}

type adminSeedingStatusReplyDoc struct {
	Cmd    string            `bson:"cmd"`
	Rows   []adminSeedRowDoc `bson:"rows"`
	More   bool              `bson:"more,omitempty"`
	Cursor []byte            `bson:"cursor,omitempty"`
}

type adminSeedingActionReplyDoc struct {
	Cmd      string `bson:"cmd"`
	Database string `bson:"database"`
	Slot     int32  `bson:"slot"`
}

// unmarshalSeeding unpacks a seeding document into doc. The size and depth were checked by the caller.
func unmarshalSeeding(raw []byte, doc any) error {
	if err := bson.Unmarshal(raw, doc); err != nil {
		return ringRefusal("a seeding document is not the document of entry-ring-admin-wire.md (the members and types of that command): %v", err)
	}
	return nil
}

func isSeedingCmd(cmd string) bool {
	return cmd == AdminSeedingStatus || cmd == AdminSeedingRetry || cmd == AdminSeedingForget
}

func checkSeedingSlot(cmd string, slot int32) error {
	if slot < 0 || slot >= slotCount {
		return ringRefusal("%s: slot is %d and a slot is 0 to %d. Send a slot 'status seeding' lists", cmd, slot, slotCount-1)
	}
	return nil
}

// checkSeedingRequest checks the members of the three seeding requests.
func checkSeedingRequest(req RingAdminRequest) error {
	if req.Anyway && req.Cmd != AdminSeedingForget {
		return ringRefusal("%s takes no anyway, which only seeding_forget takes. Remove anyway", req.Cmd)
	}
	if req.Cmd != AdminSeedingStatus {
		takes := "database slot"
		if req.Cmd == AdminSeedingForget {
			takes += " anyway"
		}
		if err := adminStray(req, takes); err != nil {
			return err
		}
		if err := checkAdminName(req.Cmd, "database", req.Database); err != nil {
			return err
		}
		if req.Slot == nil {
			return ringRefusal("%s needs a slot, 0 to %d, and this request has none. Run 'status seeding' to list the slots of this node, then send one of them", req.Cmd, slotCount-1)
		}
		return checkSeedingSlot(req.Cmd, *req.Slot)
	}
	if err := adminStray(req, "database slot limit cursor"); err != nil {
		return err
	}
	if req.Database != "" {
		if err := checkAdminName(req.Cmd, "database", req.Database); err != nil {
			return err
		}
	}
	if req.Slot != nil {
		if req.Database == "" {
			return ringRefusal("seeding_status: a slot needs the database it belongs to, and this request names none. Send database with slot, or leave both out")
		}
		if err := checkSeedingSlot(req.Cmd, *req.Slot); err != nil {
			return err
		}
	}
	if req.Limit != nil && (*req.Limit < 0 || *req.Limit > seedingMaxPage) {
		return ringRefusal("seeding_status: limit is %d and a page holds 1 to %d. Leave limit out for the default of %d", *req.Limit, seedingMaxPage, defaultPageLimit)
	}
	if len(req.Cursor) > seedingCursorMax {
		return ringRefusal("seeding_status: cursor is %d bytes and at most %d are taken. Send the cursor of the previous reply unchanged", len(req.Cursor), seedingCursorMax)
	}
	return nil
}

func seedingDocOf(req RingAdminRequest) any {
	if req.Cmd == AdminSeedingStatus {
		return adminSeedingStatusDoc{Cmd: req.Cmd, Database: req.Database, Slot: req.Slot, Limit: req.Limit, Cursor: req.Cursor}
	}
	return adminSeedingActionDoc{Cmd: req.Cmd, Database: req.Database, Slot: *req.Slot, Anyway: req.Anyway}
}

// checkSeedingRow bounds one row of a status reply.
func checkSeedingRow(index int, row RingAdminSeedRow) error {
	if len(row.Database) == 0 || len(row.Database) > ringMaxName {
		return ringRefusal("row %d: database is %d bytes and a name is 1 to %d", index, len(row.Database), ringMaxName)
	}
	if row.Slot < 0 || row.Slot >= slotCount {
		return ringRefusal("row %d: slot is %d and a slot is 0 to %d", index, row.Slot, slotCount-1)
	}
	if !slices.Contains(seedStates, SeedState(row.State)) {
		return ringRefusal("row %d: state is %q and a state is one of placing, seeding, stalled, ready, retired or retiring", index, row.State)
	}
	for _, counter := range []struct {
		name  string
		count int64
	}{{"bytes_done", row.BytesDone}, {"bytes_known", row.BytesKnown}, {"entries", row.Entries}, {"idle_ms", row.IdleMS}} {
		if counter.count < 0 {
			return ringRefusal("row %d: %s is %d and a counter is 0 or more", index, counter.name, counter.count)
		}
	}
	if len(row.Source) > ringMaxNodeText {
		return ringRefusal("row %d: source is %d bytes and at most %d are taken", index, len(row.Source), ringMaxNodeText)
	}
	if len(row.Refusal) > seedingMaxRefusal {
		return ringRefusal("row %d: refusal is %d bytes and at most %d are taken", index, len(row.Refusal), seedingMaxRefusal)
	}
	if !utf8.ValidString(row.Refusal) || !utf8.ValidString(row.Source) {
		return ringRefusal("row %d: source and refusal are text and must be UTF-8", index)
	}
	return nil
}

// checkSeedingReply checks a seeding reply. The caller has checked that no other member is set.
func checkSeedingReply(cmd string, reply *RingAdminSeeding) error {
	if cmd == AdminSeedingStatus {
		if reply.Rows == nil || reply.Database != "" || reply.Slot != 0 {
			return ringRefusal("a seeding_status reply is rows, more and cursor, and nothing else")
		}
		if len(reply.Rows) > seedingMaxPage {
			return ringRefusal("a seeding_status page holds %d rows and at most %d", len(reply.Rows), seedingMaxPage)
		}
		if reply.More != (len(reply.Cursor) != 0) || len(reply.Cursor) > seedingCursorMax {
			return ringRefusal("a seeding_status reply has a cursor of at most %d bytes exactly when more is true", seedingCursorMax)
		}
		for i, row := range reply.Rows {
			if err := checkSeedingRow(i, row); err != nil {
				return err
			}
		}
		return nil
	}
	if reply.Rows != nil || reply.More || len(reply.Cursor) != 0 {
		return ringRefusal("a %s reply is database and slot, and nothing else", cmd)
	}
	if err := checkAdminName(cmd, "database", reply.Database); err != nil {
		return err
	}
	return checkSeedingSlot(cmd, reply.Slot)
}

func seedingReplyDocOf(cmd string, reply *RingAdminSeeding) any {
	if cmd != AdminSeedingStatus {
		return adminSeedingActionReplyDoc{Cmd: cmd, Database: reply.Database, Slot: reply.Slot}
	}
	rows := make([]adminSeedRowDoc, 0, len(reply.Rows))
	for _, row := range reply.Rows {
		rows = append(rows, adminSeedRowDoc(row))
	}
	return adminSeedingStatusReplyDoc{Cmd: cmd, Rows: rows, More: reply.More, Cursor: reply.Cursor}
}

func unpackSeedingRequest(cmd string, raw []byte) (RingAdminRequest, error) {
	if cmd == AdminSeedingStatus {
		var doc adminSeedingStatusDoc
		if err := unmarshalSeeding(raw, &doc); err != nil {
			return RingAdminRequest{}, err
		}
		return RingAdminRequest{Cmd: cmd, Database: doc.Database, Slot: doc.Slot, Limit: doc.Limit, Cursor: doc.Cursor}, nil
	}
	var doc adminSeedingActionDoc
	if err := unmarshalSeeding(raw, &doc); err != nil {
		return RingAdminRequest{}, err
	}
	return RingAdminRequest{Cmd: cmd, Database: doc.Database, Slot: &doc.Slot, Anyway: doc.Anyway}, nil
}

// countRows counts the elements of the rows array of a validated status document without
// building them, so a reply of many tiny rows is refused before anything is allocated for it.
func countRows(raw []byte) (int, error) {
	value, err := bson.Raw(raw).LookupErr("rows")
	if err != nil {
		return 0, nil // the unpack names the missing member
	}
	array, ok := value.ArrayOK()
	if !ok {
		return 0, nil
	}
	count, at := 0, 4
	for at < len(array) && array[at] != 0 {
		keyEnd := bytes.IndexByte(array[at+1:], 0)
		if keyEnd < 0 || at+1+keyEnd+5 > len(array) {
			return 0, ringRefusal("a seeding_status reply has a rows array that is cut short")
		}
		size := int(binary.LittleEndian.Uint32(array[at+1+keyEnd+1:]))
		if array[at] != 0x03 || size < 5 || at+1+keyEnd+1+size > len(array) {
			return 0, ringRefusal("a seeding_status reply has a rows array that holds something other than row documents")
		}
		at += 1 + keyEnd + 1 + size
		if count++; count > seedingMaxPage {
			return 0, ringRefusal("a seeding_status page holds more than %d rows and at most %d", seedingMaxPage, seedingMaxPage)
		}
	}
	return count, nil
}

func unpackSeedingReply(cmd string, raw []byte) (RingAdminReply, error) {
	if cmd != AdminSeedingStatus {
		var doc adminSeedingActionReplyDoc
		if err := unmarshalSeeding(raw, &doc); err != nil {
			return RingAdminReply{}, err
		}
		return RingAdminReply{Cmd: cmd, Seeding: &RingAdminSeeding{Database: doc.Database, Slot: doc.Slot}}, nil
	}
	if _, err := countRows(raw); err != nil {
		return RingAdminReply{}, err
	}
	var doc adminSeedingStatusReplyDoc
	if err := unmarshalSeeding(raw, &doc); err != nil {
		return RingAdminReply{}, err
	}
	rows := make([]RingAdminSeedRow, 0, len(doc.Rows))
	for _, row := range doc.Rows {
		rows = append(rows, RingAdminSeedRow(row))
	}
	return RingAdminReply{Cmd: cmd, Seeding: &RingAdminSeeding{Rows: rows, More: doc.More, Cursor: doc.Cursor}}, nil
}

// The server end.

// seedingDenied is the one PERMISSION_DENIED answer of the three commands. It names no
// database, slot or user: seeding is run by the cluster-manager role, which the legacy node
// operations (add, remove, migrate, list nodes) also need.
func seedingDenied() error {
	return &cluster.Error{Code: cluster.CodePermissionDenied, Message: "this session cannot run that command: seeding is run by the cluster-manager role granted on every database (cluster-manager@*). " +
		"Nothing was done. Ask an administrator to run 'grant role cluster-manager@* to <user>' if the user should run it"}
}

// serveSeeding answers one of the three seeding commands. The one ask is class-only: seeding
// is a property of the node, not of a database, so the role that runs the cluster holds it
// whatever databases it was granted.
func (s *ringServer) serveSeeding(ctx context.Context, req RingAdminRequest) (RingAdminReply, error) {
	class := map[string]RingClass{AdminSeedingStatus: RingClassSeedingStatus, AdminSeedingRetry: RingClassSeedingRetry, AdminSeedingForget: RingClassSeedingForget}[req.Cmd]
	if err := s.authz.Authorize(ctx, RingAccess{Class: class}); err != nil {
		return RingAdminReply{}, seedingDenied()
	}
	view := s.node.SeedingView()
	switch req.Cmd {
	case AdminSeedingRetry, AdminSeedingForget:
		var err error
		if req.Cmd == AdminSeedingForget {
			err = view.Forget(ctx, req.Database, int(*req.Slot), req.Anyway)
		} else {
			err = view.Retry(ctx, req.Database, int(*req.Slot))
		}
		if err != nil {
			return RingAdminReply{}, seedingFailure(req.Cmd, err)
		}
		return RingAdminReply{Cmd: req.Cmd, Seeding: &RingAdminSeeding{Database: req.Database, Slot: *req.Slot}}, nil
	}
	return seedingStatus(req, view.Status())
}

// seedingFailure keeps a refusal the view made and makes any other error UNAVAILABLE, so a
// client knows nothing is known to have happened and a retry is safe.
func seedingFailure(command string, err error) error {
	var refusal *cluster.Error
	if errors.As(err, &refusal) {
		return err
	}
	slog.Error("the seeding engine failed a command", "command", command, "error", err)
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: "the seeding engine could not finish that command, and whether it took effect is not known. " +
		"Run 'status seeding' to see the slot, then retry if it is still there; the cause is in the node's log"}
}

// cutUTF8 cuts text to at most limit bytes without splitting a rune.
func cutUTF8(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// seedingRowOf turns a row of the view into a wire row the codec accepts: free text is
// made valid UTF-8 and cut to its bound and a negative counter is 0, because an engine's
// refusal text or clock must never make the whole status unreadable. A state or slot no
// reader can show is an error: that is a bug in the engine, said as UNAVAILABLE.
func seedingRowOf(row SeedRow) (RingAdminSeedRow, error) {
	out := RingAdminSeedRow{Database: row.Database, Slot: int32(row.Slot), State: string(row.State),
		Source:    cutUTF8(strings.ToValidUTF8(row.Source, "?"), ringMaxNodeText),
		BytesDone: max(row.BytesDone, 0), BytesKnown: max(row.BytesKnown, 0), Entries: max(row.Entries, 0), IdleMS: max(row.Idle.Milliseconds(), 0),
		Refusal: cutUTF8(strings.ToValidUTF8(row.Refusal, "?"), seedingMaxRefusal)}
	if row.Slot < 0 || row.Slot >= slotCount || checkSeedingRow(0, out) != nil {
		return RingAdminSeedRow{}, fmt.Errorf("the seeding engine returned a row the wire cannot carry for slot %d of database %q", row.Slot, row.Database)
	}
	return out, nil
}

// seedingCursorOf and seedingCursorAfter are the resume point of a status page: the slot of
// the last row returned as two bytes, then its database name. Rows are ordered by database
// then slot, so the next page is every row after that pair.
func seedingCursorOf(row RingAdminSeedRow) []byte {
	return append([]byte{byte(row.Slot >> 8), byte(row.Slot)}, row.Database...)
}

func seedingCursorAfter(cursor []byte) (database string, slot int32, err error) {
	if len(cursor) < 3 || len(cursor) > seedingCursorMax {
		return "", 0, ringRefusal("the seeding_status cursor is not one this node issued. Nothing was done. Send seeding_status again without a cursor to start from the first row")
	}
	slot = int32(cursor[0])<<8 | int32(cursor[1])
	if slot >= slotCount {
		return "", 0, ringRefusal("the seeding_status cursor is not one this node issued. Nothing was done. Send seeding_status again without a cursor to start from the first row")
	}
	return string(cursor[2:]), slot, nil
}

// seedingStatus filters, orders and pages the rows of the view. The cost is the rows the
// view holds (in flight, queued and retired), never the buckets of a database.
func seedingStatus(req RingAdminRequest, view []SeedRow) (RingAdminReply, error) {
	want := defaultPageLimit
	if req.Limit != nil && *req.Limit != 0 {
		want = int(*req.Limit)
	}
	var afterDatabase string
	var afterSlot int32
	if len(req.Cursor) > 0 {
		var err error
		if afterDatabase, afterSlot, err = seedingCursorAfter(req.Cursor); err != nil {
			return RingAdminReply{}, err
		}
	}
	rows := make([]RingAdminSeedRow, 0, len(view))
	for _, row := range view {
		if req.Database != "" && row.Database != req.Database || req.Slot != nil && int32(row.Slot) != *req.Slot {
			continue
		}
		listed, err := seedingRowOf(row)
		if err != nil {
			slog.Error("the seeding engine returned a row the wire cannot carry", "database", row.Database, "slot", row.Slot, "state", string(row.State))
			return RingAdminReply{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: err.Error() + ", so the status cannot be shown. The row is in the node's log; narrow the status past it with 'status seeding <db> <slot>' for the others"}
		}
		rows = append(rows, listed)
	}
	slices.SortFunc(rows, func(a, b RingAdminSeedRow) int {
		return cmp.Or(cmp.Compare(a.Database, b.Database), cmp.Compare(a.Slot, b.Slot))
	})
	if len(req.Cursor) > 0 {
		start, found := slices.BinarySearchFunc(rows, RingAdminSeedRow{Database: afterDatabase, Slot: afterSlot}, func(a, b RingAdminSeedRow) int {
			return cmp.Or(cmp.Compare(a.Database, b.Database), cmp.Compare(a.Slot, b.Slot))
		})
		if found {
			start++
		}
		rows = rows[start:]
	}
	seeding := &RingAdminSeeding{Rows: rows}
	if len(rows) > want {
		seeding.Rows, seeding.More = rows[:want], true
		seeding.Cursor = seedingCursorOf(rows[want-1])
	}
	return RingAdminReply{Cmd: req.Cmd, Seeding: seeding}, nil
}
