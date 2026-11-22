package ringclient

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// List reads one page of the documents of the bucket, optionally those whose scalar fields
// equal the members of filter (a bson.D or any value that marshals to a document), and
// optionally only the scalar fields page.Fields names besides _id_. A filter still sees
// every declared field, whatever is returned. It is the
// one read that is O(documents): page through it with the returned Cursor while More is true.
func (b *Bucket) List(ctx context.Context, filter any, page PageRequest) (EntryPage, error) {
	raw, err := filterBytes(filter)
	if err != nil {
		return EntryPage{}, err
	}
	options, err := readOptions(page.Expand, page.Local)
	if err != nil {
		return EntryPage{}, err
	}
	request := wire.RingListRequest{Database: b.database, Bucket: b.bucket, Filter: raw, Fields: page.Fields, Cursor: page.Cursor, Options: options}
	if page.Limit != 0 {
		request.Limit = &page.Limit
	}
	body, err := wire.EncodeRingList(request)
	if err != nil {
		return EntryPage{}, fmt.Errorf("ringclient: encoding the list of %s: %w", b.where(), err)
	}
	payload, err := b.client.call(ctx, transport.OpRingList, b.client.NextRequestID(), body)
	if err != nil {
		return EntryPage{}, err
	}
	reply, err := wire.DecodeRingReply(payload)
	if err != nil {
		return EntryPage{}, fmt.Errorf("ringclient: decoding the list reply from %s: %w", b.where(), err)
	}
	entries, err := cluster.DecodeEntries(reply.Entries)
	if err != nil {
		return EntryPage{}, fmt.Errorf("ringclient: decoding the entries of %s: %w", b.where(), err)
	}
	out := EntryPage{Entries: make([]Entry, len(entries)), More: reply.More, Cursor: reply.Cursor, Expanded: expandedList(reply.Expanded), Served: reply.Served}
	if reply.ExpandFiltered != nil {
		out.ExpandFiltered = int(*reply.ExpandFiltered)
	}
	for i, entry := range entries {
		out.Entries[i] = Entry{ID: decodeID(entry.EncID), Body: entry.Body}
	}
	return out, nil
}

// Count returns how many documents the bucket holds, or how many match filter.
func (b *Bucket) Count(ctx context.Context, filter any) (int64, error) {
	raw, err := filterBytes(filter)
	if err != nil {
		return 0, err
	}
	body, err := wire.EncodeRingCount(wire.RingCountRequest{Database: b.database, Bucket: b.bucket, Filter: raw})
	if err != nil {
		return 0, fmt.Errorf("ringclient: encoding the count of %s: %w", b.where(), err)
	}
	payload, err := b.client.call(ctx, transport.OpRingCount, b.client.NextRequestID(), body)
	if err != nil {
		return 0, err
	}
	reply, err := wire.DecodeRingReply(payload)
	if err != nil {
		return 0, fmt.Errorf("ringclient: decoding the count reply from %s: %w", b.where(), err)
	}
	if reply.Count == nil {
		return 0, fmt.Errorf("ringclient: the server answered a count of %s without a count", b.where())
	}
	return *reply.Count, nil
}

// readOptions builds the options trailer of an expanded or local read. The cap of 8 expanded
// fields is checked here, before anything is sent.
func readOptions(expand []ExpandRequest, local bool) (wire.RingReadOptions, error) {
	options := wire.RingReadOptions{Local: local}
	for _, request := range expand {
		where, err := filterBytes(request.Where)
		if err != nil {
			return wire.RingReadOptions{}, fmt.Errorf("ringclient: encoding the where of expand %q: %w", request.Field, err)
		}
		options.Expand = append(options.Expand, wire.RingExpandSpec{Field: request.Field, Database: request.Database, Bucket: request.Bucket, Fields: request.Fields, Where: where})
	}
	if _, err := wire.EncodeRingReadOptions(options); err != nil {
		return wire.RingReadOptions{}, fmt.Errorf("ringclient: %w", err)
	}
	return options, nil
}
