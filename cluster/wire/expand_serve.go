package wire

import (
	"context"
	"errors"
	"strconv"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// This file binds expandRows to the three reads that carry expand: a store list, a bucket
// list and a get. Each builds the ids of its rows, calls the expander once, and keeps
// reply rows and answers in step.

// expandCeiling is the most bytes an expanded page holds: the page ceiling unless a test
// lowers it.
func (s *ringServer) expandCeiling() int64 {
	if s.expandBytes > 0 {
		return s.expandBytes
	}
	return pageBytes
}

// expandWithRetry runs expandRows and, when an owner answers with a stale epoch, runs it
// once more from the same rows: one refresh, one retry, never a mix of epochs.
func (s *ringServer) expandWithRetry(ctx context.Context, near bucketRef, specs []RingExpandSpec, ids [][]string, local bool) ([][]byte, []int, int, string, error) {
	s.node.expandStats.Calls.Add(1)
	answers, kept, filtered, served, err := s.expandRows(ctx, near, specs, ids, local)
	var coded *cluster.Error
	if errors.As(err, &coded) && coded.Code == cluster.CodeStaleEpoch {
		return s.expandRows(ctx, near, specs, ids, local)
	}
	return answers, kept, filtered, served, err
}

// cutAtRow returns how many leading rows fit under ceiling when row i takes sizes[i]
// bytes. The first row alone over the ceiling is not cut: it is -1, and the caller
// refuses the page.
func cutAtRow(sizes []int64, ceiling int64) int {
	var total int64
	for i, size := range sizes {
		if total+size > ceiling {
			if i == 0 {
				return -1
			}
			return i
		}
		total += size
	}
	return len(sizes)
}

// refuseLocalWithoutExpand refuses local on a list that expands nothing: the elements and the
// rows of a list are read from the owner, so the flag would be accepted and do nothing, and the
// developer would think the read accepted lag. local applies to keyed document reads and to the
// far reads of expand.
func refuseLocalWithoutExpand(options RingReadOptions, what string) error {
	if !options.Local || len(options.Expand) > 0 {
		return nil
	}
	return ringRefusal("local: true on %s that expands nothing does nothing: its rows are read from the owner. Nothing was read. Add expand to read the far documents from a copy here, or drop local", what)
}

// filteredBeforeCut counts the rows the where dropped among those up to the last row kept by a
// page cut after fit kept rows. Rows after the cut are read again on the next page, which
// counts them then, so a row is counted once.
func filteredBeforeCut(kept []int, fit int) int {
	return kept[fit-1] + 1 - fit
}

// expandStoreList expands the element keys of a hashStore or btreeStore page. The page is
// cut at a whole element when the expansion would pass the ceiling.
func (s *ringServer) expandStoreList(ctx context.Context, near bucketRef, call fieldCall, options RingReadOptions, page FieldPage) (RingReply, error) {
	specs := options.Expand
	if len(specs) != 1 || specs[0].Field != "" {
		return RingReply{}, ringRefusal("expand on a store list names exactly one bucket and no field: the element keys are the ids. Run the list with expand: true, or expand: \"<Bucket>\"")
	}
	spec := specs[0]
	spec.Field = call.fieldName()
	ids := make([][]string, len(page.Keys))
	for i, key := range page.Keys {
		ids[i] = []string{key}
	}
	answers, kept, filtered, served, err := s.expandWithRetry(ctx, near, []RingExpandSpec{spec}, ids, options.Local)
	if err != nil {
		return RingReply{}, err
	}
	reply := RingReply{More: page.More, Cursor: page.Cursor, Expanded: answers, Served: served}
	keys := make([]string, len(kept))
	sizes := make([]int64, len(kept))
	for k, row := range kept {
		keys[k] = page.Keys[row]
		reply.Elements = append(reply.Elements, page.Elements[row])
		sizes[k] = int64(len(page.Elements[row]) + len(answers[k]) + len(keys[k]))
	}
	if fit := cutAtRow(sizes, s.expandCeiling()); fit == -1 {
		return RingReply{}, &cluster.Error{Code: cluster.CodeResultTooLarge, Message: cluster.CodeResultTooLarge.Text(cluster.CondResultTooLarge, map[string]string{
			"db": call.ref.db, "bucket": call.ref.name, "n": strconv.FormatInt(sizes[0], 10),
		})}
	} else if fit < len(kept) {
		cursor, err := store.EncodeComponent(keys[fit-1])
		if err != nil {
			return RingReply{}, err
		}
		reply.Elements, reply.Expanded, keys = reply.Elements[:fit], reply.Expanded[:fit], keys[:fit]
		reply.More, reply.Cursor = true, cursor
		filtered = filteredBeforeCut(kept, fit)
	}
	reply.Keys = keys
	if len(spec.Where) > 0 {
		count := int32(filtered)
		reply.ExpandFiltered = &count
	}
	found := len(reply.Elements) > 0
	reply.Found = &found
	return reply, nil
}

// expandGet expands the fields of one document. A where that the document does not meet
// answers as not found, counted in ExpandFiltered.
func (s *ringServer) expandGet(ctx context.Context, near bucketRef, options RingReadOptions, body []byte, nearServed string) (RingReply, error) {
	ids := make([]string, len(options.Expand))
	for j, spec := range options.Expand {
		ids[j] = idText(bson.Raw(body), spec.Field)
	}
	answers, _, filtered, served, err := s.expandWithRetry(ctx, near, options.Expand, [][]string{ids}, options.Local)
	if err != nil {
		return RingReply{}, err
	}
	if served == servedLocal && nearServed != servedLocal {
		served = servedOwner
	}
	reply := RingReply{Found: ringTrue(), Elements: [][]byte{body}, Expanded: answers, Served: served}
	if filtered > 0 {
		count := int32(filtered)
		reply = RingReply{Found: ringFalse(), ExpandFiltered: &count}
	}
	return reply, nil
}

// idText is the string value of field in body, "" when it is absent, null or not text.
func idText(body bson.Raw, field string) string {
	value, err := body.LookupErr(field)
	if err != nil {
		return ""
	}
	text, _ := value.StringValueOK()
	return text
}

// expandEntries expands the named scalar fields of a bucket list page and cuts the page at
// a whole row when the expansion would pass the ceiling.
func (s *ringServer) expandEntries(ctx context.Context, ref bucketRef, req RingListRequest, page ListPage) (RingReply, error) {
	specs := req.Options.Expand
	ids := make([][]string, len(page.Entries))
	for i, entry := range page.Entries {
		ids[i] = make([]string, len(specs))
		for j, spec := range specs {
			ids[i][j] = idText(bson.Raw(entry.Body), spec.Field)
		}
	}
	answers, kept, filtered, served, err := s.expandWithRetry(ctx, ref, specs, ids, req.Options.Local)
	if err != nil {
		return RingReply{}, err
	}
	sizes := make([]int64, len(kept))
	rows := make([]cluster.ListEntry, len(kept))
	for k, row := range kept {
		rows[k] = page.Entries[row]
		sizes[k] = int64(len(cluster.AppendEntry(nil, rows[k])) + len(answers[k]))
	}
	more, cursor := page.More, page.Cursor
	if fit := cutAtRow(sizes, s.expandCeiling()); fit == -1 {
		return RingReply{}, &cluster.Error{Code: cluster.CodeResultTooLarge, Message: cluster.CodeResultTooLarge.Text(cluster.CondResultTooLarge, map[string]string{
			"db": ref.db, "bucket": ref.name, "n": strconv.FormatInt(sizes[0], 10),
		})}
	} else if fit < len(rows) {
		last := rows[fit-1]
		rows, answers = rows[:fit], answers[:fit]
		more = true
		cursor = cluster.EncodeCursor(cluster.ListCursor{DatabaseID: ref.dbID, BucketID: ref.entry.Decl.BucketID, Slot: last.Slot, EncID: last.EncID})
		filtered = filteredBeforeCut(kept, fit)
	}
	var entries []byte
	for _, entry := range rows {
		entries = cluster.AppendEntry(entries, entry)
	}
	reply := RingReply{Entries: entries, More: more, Cursor: cursor, Expanded: answers, Served: served}
	for _, spec := range specs {
		if len(spec.Where) > 0 {
			count := int32(filtered)
			reply.ExpandFiltered = &count
			break
		}
	}
	return reply, nil
}
