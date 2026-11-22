package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
)

// MaskBytes is the size of a sub-scan's slot mask: bit s is slot s, the least
// significant bit of byte 0 being slot 0 (conventions 6.3).
const MaskBytes = tableSlots / 8

// SlotMask is the set of slots one owner owes a sub-scan.
type SlotMask [MaskBytes]byte

// Set adds slot to the mask. A slot outside 0..8191 is ignored.
func (m *SlotMask) Set(slot uint16) {
	if slot < tableSlots {
		m[slot/8] |= 1 << (slot % 8)
	}
}

// Has reports whether slot is in the mask.
func (m *SlotMask) Has(slot uint16) bool {
	return slot < tableSlots && m[slot/8]&(1<<(slot%8)) != 0
}

// Count is the number of slots in the mask.
func (m *SlotMask) Count() int {
	n := 0
	for slot := uint16(0); slot < tableSlots; slot++ {
		if m.Has(slot) {
			n++
		}
	}
	return n
}

// ListEntry is one document of a page: its slot, encoded id and BSON body.
type ListEntry struct {
	Slot  uint16
	EncID []byte
	Body  []byte
}

// SlotListRequest is the SLOT_LIST body (conventions 6.3). Epoch rides in the
// envelope on the wire; it is a field here so the owner can fence on it.
type SlotListRequest struct {
	Epoch    uint64
	DBID     uint64
	BucketID uint64
	// Filter is an equality document on declared scalar fields, or nil.
	Filter []byte
	Mask   SlotMask
	// ResumeSlot and ResumeID name the last document already emitted; every
	// returned entry is strictly after them. ResumeID is empty on the first page.
	ResumeSlot uint16
	ResumeID   []byte
	// Want is the most entries the owner returns.
	Want int
	// Fields projects the body: nil returns every declared field; a non-nil slice (empty
	// allowed) returns _id_ and only the scalar field ids it names, in field-id order, and
	// reads no store state. The filter still applies to every declared scalar.
	Fields []uint16
	// MaxBytes is a stop condition for this owner, never a refusal.
	MaxBytes int64
}

// SlotListReply is the SLOT_LIST_REPLY body.
type SlotListReply struct {
	// Entries are in (slot, encID) order.
	Entries []ListEntry
	// More is true when the owner stopped at Want or at its byte share.
	More bool
}

// SlotCountRequest is the SLOT_COUNT body.
type SlotCountRequest struct {
	Epoch    uint64
	DBID     uint64
	BucketID uint64
	Filter   []byte
	Mask     SlotMask
}

// maxEntryID is the 1024-byte cap of an encoded _id_ (conventions 5).
const maxEntryID = 1024

// entrySize is the length AppendEntry adds for entry.
func entrySize(entry ListEntry) int {
	return 2 + uvarintLen(len(entry.EncID)) + len(entry.EncID) + uvarintLen(len(entry.Body)) + len(entry.Body)
}

func uvarintLen(n int) int { return len(binary.AppendUvarint(nil, uint64(n))) }

// AppendEntry appends the wire record of entry to dst:
// [slot:2 BE][uvarint idlen][encID][uvarint bodylen][body].
func AppendEntry(dst []byte, entry ListEntry) []byte {
	dst = binary.BigEndian.AppendUint16(dst, entry.Slot)
	dst = binary.AppendUvarint(dst, uint64(len(entry.EncID)))
	dst = append(dst, entry.EncID...)
	dst = binary.AppendUvarint(dst, uint64(len(entry.Body)))
	return append(dst, entry.Body...)
}

// DecodeEntries parses an entries blob until it ends. Every length is checked
// against the bytes left before anything is allocated, so a hostile blob costs
// no more memory than its own size. It refuses a slot above 8191, an id of no
// bytes or over 1024, a length that is not the shortest uvarint and a truncated
// record.
func DecodeEntries(blob []byte) ([]ListEntry, error) {
	var entries []ListEntry
	rest := blob
	for len(rest) > 0 {
		if len(rest) < 2 {
			return nil, entriesError("record %d is cut off before its slot ends", len(entries))
		}
		entry := ListEntry{Slot: binary.BigEndian.Uint16(rest)}
		if entry.Slot >= tableSlots {
			return nil, entriesError("record %d names slot %d, and there are %d", len(entries), entry.Slot, tableSlots)
		}
		rest = rest[2:]
		encID, after, err := takeSized(rest, "id", len(entries))
		if err != nil {
			return nil, err
		}
		if len(encID) == 0 || len(encID) > maxEntryID {
			return nil, entriesError("record %d has an id of %d bytes, want 1 to %d", len(entries), len(encID), maxEntryID)
		}
		body, after, err := takeSized(after, "body", len(entries))
		if err != nil {
			return nil, err
		}
		entry.EncID, entry.Body = bytes.Clone(encID), bytes.Clone(body)
		entries = append(entries, entry)
		rest = after
	}
	return entries, nil
}

// takeSized reads a canonical uvarint length and that many bytes.
func takeSized(raw []byte, what string, record int) (taken, rest []byte, err error) {
	length, n, ok := readCanonicalUvarint(raw)
	if !ok {
		return nil, nil, entriesError("record %d has a %s length that is not a shortest uvarint", record, what)
	}
	raw = raw[n:]
	if length > uint64(len(raw)) {
		return nil, nil, entriesError("record %d has a %s of %d bytes and %d are left", record, what, length, len(raw))
	}
	return raw[:length], raw[length:], nil
}

func entriesError(format string, args ...any) error {
	return &Error{Code: CodeInvalidArgument, Message: "a list answer is damaged: " + fmt.Sprintf(format, args...)}
}

// OwnerScanner sends one sub-scan to one owner. An owner that does not own a
// slot of the mask, or sees another epoch, answers with a *Error carrying
// CodeNotOwner or CodeStaleEpoch; any other error means the owner did not answer.
type OwnerScanner interface {
	// ScanList sends SLOT_LIST to owner.
	ScanList(ctx context.Context, owner Member, req SlotListRequest) (SlotListReply, error)
	// ScanCount sends SLOT_COUNT to owner.
	ScanCount(ctx context.Context, owner Member, req SlotCountRequest) (int64, error)
}

// BucketTarget is what a database and bucket name resolve to.
type BucketTarget struct {
	DatabaseID, BucketID uint64
	// RF is the database's replication factor, which fixes the copy set.
	RF int
}

// BucketResolver resolves a database and bucket name; a miss is a NOT_FOUND *Error.
type BucketResolver interface {
	// Resolve returns the ids and replication factor of the bucket.
	Resolve(ctx context.Context, database, bucket string) (BucketTarget, error)
}

// ScanTopology is the coordinator's view of the committed table.
type ScanTopology interface {
	// Epoch is the committed epoch.
	Epoch() uint64
	// Table is the slot table of that epoch.
	Table() *SlotTable
	// Refresh reloads the table and the epoch.
	Refresh(ctx context.Context) error
}

// ScanLimits are the page bounds of conventions 16.
type ScanLimits struct {
	// DefaultLimit is the page size when a request names none (100).
	DefaultLimit int
	// MaxLimit is the largest page size (10000).
	MaxLimit int
	// MaxResultBytes is the ceiling of one page (query.max_result_bytes).
	MaxResultBytes int64
}

// CoordinatorConfig wires a Coordinator.
type CoordinatorConfig struct {
	Topology ScanTopology
	Scanner  OwnerScanner
	Buckets  BucketResolver
	Limits   ScanLimits
}

// Coordinator serves list and count by scatter-gather: one sub-scan per owner,
// never one per slot, never a replica. It holds no state between calls, so it
// costs nothing per bucket and nothing per cursor.
type Coordinator struct{ cfg CoordinatorConfig }

// NewCoordinator returns a coordinator.
func NewCoordinator(cfg CoordinatorConfig) *Coordinator { return &Coordinator{cfg: cfg} }

// ListRequest is a list call.
type ListRequest struct {
	Database, Bucket string
	// Filter is an equality document on declared scalar fields, or nil.
	Filter []byte
	// Limit is the page size; 0 means the default.
	Limit int
	// Cursor continues a previous page; nil starts at the beginning.
	Cursor []byte
}

// ListResponse is one page.
type ListResponse struct {
	Entries []ListEntry
	// More is true when documents remain after this page; Cursor is then set.
	More   bool
	Cursor []byte
}

// CountRequest is a count call.
type CountRequest struct {
	Database, Bucket string
	Filter           []byte
}

// CountResponse is the one integer: no limit applies, because a limited count is a wrong number.
type CountResponse struct{ Count int64 }

// ownerGroup is the slots one node answers for in one call.
type ownerGroup struct {
	member Member
	mask   SlotMask
	slots  int
}

// callName carries the words an OWNER_DOWN text needs.
type callName struct{ op, database, bucket string }

func scanRefusal(code Code, cond Condition, args map[string]string) *Error {
	return &Error{Code: code, Message: code.Text(cond, args)}
}

func isRefusal(err error) bool {
	var coded *Error
	return errors.As(err, &coded) && (coded.Code == CodeNotOwner || coded.Code == CodeStaleEpoch)
}

// groupByOwner groups the slots from first up by the owner the table names. A slot with
// no owner (every copy down) is a call that cannot be answered whole.
func (c *Coordinator) groupByOwner(table *SlotTable, target BucketTarget, first uint16, name callName) ([]ownerGroup, error) {
	byID := map[string]*ownerGroup{}
	for slot := first; slot < tableSlots; slot++ {
		owner, ok := table.Owner(slot, target.RF)
		if !ok {
			return nil, c.ownerDown(name, silentOwner(table, slot, target.RF), orphanedSlots(table, first, target.RF))
		}
		group := byID[owner.ID]
		if group == nil {
			group = &ownerGroup{member: owner}
			byID[owner.ID] = group
		}
		group.mask.Set(slot)
		group.slots++
	}
	groups := make([]ownerGroup, 0, len(byID))
	for _, group := range byID {
		groups = append(groups, *group)
	}
	slices.SortFunc(groups, func(a, b ownerGroup) int { return bytes.Compare([]byte(a.member.ID), []byte(b.member.ID)) })
	return groups, nil
}

// silentOwner names the node that should have answered a slot with no live copy.
func silentOwner(table *SlotTable, slot uint16, rf int) string {
	if copies := table.Copies(slot, rf); len(copies) > 0 {
		return copies[0].ID
	}
	return "(no data member)"
}

// orphanedSlots counts the slots from first up whose copies are all down.
func orphanedSlots(table *SlotTable, first uint16, rf int) int {
	n := 0
	for slot := first; slot < tableSlots; slot++ {
		if _, ok := table.Owner(slot, rf); !ok {
			n++
		}
	}
	return n
}

func (c *Coordinator) ownerDown(name callName, owner string, slots int) *Error {
	return scanRefusal(CodeOwnerDown, CondOwnerDown, map[string]string{
		"op": name.op, "db": name.database, "bucket": name.bucket, "owner": owner, "n": strconv.Itoa(slots),
	})
}

// fanOut calls every group at once and returns the answers in group order. It fails
// whole: a refusal (the caller refreshes and retries once), another coded error, or
// OWNER_DOWN naming the first silent owner. It never returns some owners' answers.
func fanOut[T any](ctx context.Context, c *Coordinator, name callName, groups []ownerGroup, call func(context.Context, ownerGroup) (T, error)) ([]T, error) {
	answers := make([]T, len(groups))
	failures := make([]error, len(groups))
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			answers[i], failures[i] = call(ctx, groups[i])
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%s %s.%s: %w", name.op, name.database, name.bucket, err)
	}
	for _, failure := range failures {
		if failure != nil && isRefusal(failure) {
			return nil, failure
		}
	}
	for i, failure := range failures {
		if failure == nil {
			continue
		}
		var coded *Error
		if errors.As(failure, &coded) {
			return nil, failure
		}
		return nil, c.ownerDown(name, groups[i].member.ID, groups[i].slots)
	}
	return answers, nil
}

// limit applies conventions 16: absent or 0 is the default, below 1 or above the maximum is refused.
func (c *Coordinator) limit(req ListRequest) (int, error) {
	args := map[string]string{
		"n": strconv.Itoa(req.Limit), "max": strconv.Itoa(c.cfg.Limits.MaxLimit),
		"default": strconv.Itoa(c.cfg.Limits.DefaultLimit), "db": req.Database, "bucket": req.Bucket,
	}
	switch {
	case req.Limit == 0:
		return c.cfg.Limits.DefaultLimit, nil
	case req.Limit < 0:
		return 0, scanRefusal(CodeInvalidArgument, CondLimitBelowOne, args)
	case req.Limit > c.cfg.Limits.MaxLimit:
		return 0, scanRefusal(CodeInvalidArgument, CondLimitAboveMax, args)
	}
	return req.Limit, nil
}

// List returns one page, ordered by (slot, encoded id) and continued by the cursor.
// A refusal by an owner refreshes the table once and restarts the page from the same
// resume position; a second refusal, or any owner that does not answer, fails the call.
func (c *Coordinator) List(ctx context.Context, req ListRequest) (ListResponse, error) {
	limit, err := c.limit(req)
	if err != nil {
		return ListResponse{}, err
	}
	target, err := c.cfg.Buckets.Resolve(ctx, req.Database, req.Bucket)
	if err != nil {
		return ListResponse{}, fmt.Errorf("listing %s.%s: %w", req.Database, req.Bucket, err)
	}
	var resume ListCursor
	if len(req.Cursor) > 0 {
		resume, err = DecodeCursor(req.Cursor, target.DatabaseID, target.BucketID)
		if err != nil {
			var bad *CursorError
			if !errors.As(err, &bad) {
				return ListResponse{}, fmt.Errorf("reading the cursor of %s.%s: %w", req.Database, req.Bucket, err)
			}
			return ListResponse{}, scanRefusal(CodeInvalidArgument, CondBadCursor, map[string]string{
				"db": req.Database, "bucket": req.Bucket, "reason": bad.Reason,
			})
		}
	}
	page, err := c.listOnce(ctx, req, target, resume, limit)
	if isRefusal(err) {
		if refreshErr := c.cfg.Topology.Refresh(ctx); refreshErr != nil {
			return ListResponse{}, fmt.Errorf("refreshing the slot table of %s.%s after %v: %w", req.Database, req.Bucket, err, refreshErr)
		}
		page, err = c.listOnce(ctx, req, target, resume, limit)
	}
	return page, err
}

func (c *Coordinator) listOnce(ctx context.Context, req ListRequest, target BucketTarget, resume ListCursor, limit int) (ListResponse, error) {
	name := callName{op: "list", database: req.Database, bucket: req.Bucket}
	groups, err := c.groupByOwner(c.cfg.Topology.Table(), target, resume.Slot, name)
	if err != nil {
		return ListResponse{}, err
	}
	epoch := c.cfg.Topology.Epoch()
	share := max(c.cfg.Limits.MaxResultBytes/int64(len(groups)), 1)
	replies, err := fanOut(ctx, c, name, groups, func(ctx context.Context, group ownerGroup) (SlotListReply, error) {
		return c.cfg.Scanner.ScanList(ctx, group.member, SlotListRequest{
			Epoch: epoch, DBID: target.DatabaseID, BucketID: target.BucketID, Filter: req.Filter, Mask: group.mask,
			ResumeSlot: resume.Slot, ResumeID: resume.EncID, Want: limit, MaxBytes: share,
		})
	})
	if err != nil {
		return ListResponse{}, err
	}
	return c.merge(req, target, replies, limit)
}

// merge is the k-way merge by (slot, encID). It stops early when an owner whose list is
// used up said it has more, because that owner may hold a smaller document than any
// other list shows.
func (c *Coordinator) merge(req ListRequest, target BucketTarget, replies []SlotListReply, limit int) (ListResponse, error) {
	next := make([]int, len(replies))
	var page ListResponse
	var size int64
	stopped := false
	for len(page.Entries) < limit && !stopped {
		best := -1
		for i, reply := range replies {
			if next[i] == len(reply.Entries) {
				if reply.More {
					stopped = true
				}
				continue
			}
			if best < 0 || entryBefore(reply.Entries[next[i]], replies[best].Entries[next[best]]) {
				best = i
			}
		}
		if stopped || best < 0 {
			break
		}
		entry := replies[best].Entries[next[best]]
		width := int64(entrySize(entry))
		if len(page.Entries) == 0 && width > c.cfg.Limits.MaxResultBytes {
			return ListResponse{}, scanRefusal(CodeResultTooLarge, CondResultTooLarge, map[string]string{
				"db": req.Database, "bucket": req.Bucket, "n": strconv.FormatInt(width, 10),
			})
		}
		if size+width > c.cfg.Limits.MaxResultBytes {
			stopped = true
			break
		}
		size += width
		page.Entries = append(page.Entries, entry)
		next[best]++
	}
	for i, reply := range replies {
		if next[i] < len(reply.Entries) || reply.More {
			page.More = true
		}
	}
	page.More = page.More || stopped
	if !page.More {
		return page, nil
	}
	if len(page.Entries) == 0 {
		return ListResponse{}, fmt.Errorf("listing %s.%s: an owner reported more documents and returned none", req.Database, req.Bucket)
	}
	last := page.Entries[len(page.Entries)-1]
	page.Cursor = EncodeCursor(ListCursor{DatabaseID: target.DatabaseID, BucketID: target.BucketID, Slot: last.Slot, EncID: last.EncID})
	return page, nil
}

func entryBefore(a, b ListEntry) bool {
	if a.Slot != b.Slot {
		return a.Slot < b.Slot
	}
	return bytes.Compare(a.EncID, b.EncID) < 0
}

// Count returns the number of live documents matching the filter, summed over the owners.
func (c *Coordinator) Count(ctx context.Context, req CountRequest) (CountResponse, error) {
	target, err := c.cfg.Buckets.Resolve(ctx, req.Database, req.Bucket)
	if err != nil {
		return CountResponse{}, fmt.Errorf("counting %s.%s: %w", req.Database, req.Bucket, err)
	}
	total, err := c.countOnce(ctx, req, target)
	if isRefusal(err) {
		if refreshErr := c.cfg.Topology.Refresh(ctx); refreshErr != nil {
			return CountResponse{}, fmt.Errorf("refreshing the slot table of %s.%s after %v: %w", req.Database, req.Bucket, err, refreshErr)
		}
		total, err = c.countOnce(ctx, req, target)
	}
	if err != nil {
		return CountResponse{}, err
	}
	return CountResponse{Count: total}, nil
}

func (c *Coordinator) countOnce(ctx context.Context, req CountRequest, target BucketTarget) (int64, error) {
	name := callName{op: "count", database: req.Database, bucket: req.Bucket}
	groups, err := c.groupByOwner(c.cfg.Topology.Table(), target, 0, name)
	if err != nil {
		return 0, err
	}
	epoch := c.cfg.Topology.Epoch()
	counts, err := fanOut(ctx, c, name, groups, func(ctx context.Context, group ownerGroup) (int64, error) {
		return c.cfg.Scanner.ScanCount(ctx, group.member, SlotCountRequest{
			Epoch: epoch, DBID: target.DatabaseID, BucketID: target.BucketID, Filter: req.Filter, Mask: group.mask,
		})
	})
	if err != nil {
		return 0, err
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	return total, nil
}
