package main

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringPageMax is the most elements one StoreList round trip returns (the server's ceiling).
const ringPageMax = 1000

// ringOps runs the operations of one layout on one connection.
type ringOps struct {
	client   *ringclient.Client
	layout   model.Layout
	database string
	// batch sends a write that fans out to several elements as one batch frame.
	batch bool
	// multiget reads the documents of a page with one get-many frame instead of one Get each.
	multiget bool
	// native reads and writes the copies of the native layout variant (-param native=on).
	native bool
	// aggregates says the ledger declares its aggregates, so ledger.totals_by_category can read them.
	aggregates bool
	// reads is the seam the read paths call; tests put a fake behind it.
	reads ringReads
}

// bucket is used by the write, count and scan paths, which are not behind the reads seam.
func (o *ringOps) bucket(name string) *ringclient.Bucket { return o.client.Bucket(o.database, name) }

// readStore reads the elements of one store from the first, limit at most (0 reads every
// element), one StoreList round trip per page. It returns the elements and the round trips.
func (o *ringOps) readStore(ctx context.Context, ref StoreRef, limit int) ([]model.Element, int, error) {
	return o.readStoreAt(ctx, ref, 0, limit)
}

// readStoreAt is readStore for a page that starts offset elements into the store. The ring
// client has no offset and no way to start at an element number: StoreList takes only the
// opaque cursor the previous page returned. So this adapter reads forward from the first
// element and discards the first offset of them, one round trip per 1000 elements, all counted
// in the trips it returns (an offset within the first 1000 elements costs one trip that carries
// offset+limit elements). That is the cost of an offset on this engine and the record shows it
// in round_trips_per_op.
func (o *ringOps) readStoreAt(ctx context.Context, ref StoreRef, offset, limit int) ([]model.Element, int, error) {
	kind, err := KindOf(o.layout, ref.Bucket, ref.Store)
	if err != nil {
		return nil, 0, fmt.Errorf("ring: %w", err)
	}
	if offset > 0 && limit < 1 {
		return nil, 0, fmt.Errorf("ring: reading %s of %s at offset %d needs a limit", ref.Store, ref.Owner, offset)
	}
	var elements []model.Element
	var cursor []byte
	trips, seen := 0, 0
	for {
		want := ringPageMax
		if limit > 0 && offset+limit-seen < want {
			want = offset + limit - seen
		}
		page, err := o.reads.StoreList(ctx, ref.Bucket, ref.Owner, ref.Store, want, cursor)
		trips++
		if err != nil {
			return nil, trips, fmt.Errorf("reading %s of %s %s in %s: %w", ref.Store, ref.Bucket, ref.Owner, o.database, err)
		}
		if kind.Keyed() && len(page.Keys) != len(page.Elements) {
			return nil, trips, fmt.Errorf("reading %s of %s %s: the server returned %d keys for %d elements", ref.Store, ref.Bucket, ref.Owner, len(page.Keys), len(page.Elements))
		}
		for i, raw := range page.Elements {
			seen++
			if seen <= offset {
				continue
			}
			key := ""
			if kind.Keyed() {
				key = page.Keys[i]
			}
			element, err := ringElement(ref, key, raw)
			if err != nil {
				return nil, trips, err
			}
			elements = append(elements, element)
		}
		if !page.More || (limit > 0 && len(elements) >= limit) {
			return elements, trips, nil
		}
		if len(page.Elements) == 0 {
			return nil, trips, fmt.Errorf("reading %s of %s %s: the server returned an empty page and said more follow", ref.Store, ref.Bucket, ref.Owner)
		}
		cursor = page.Cursor
	}
}

// getDocument reads one document by id.
func (o *ringOps) getDocument(ctx context.Context, bucket, id string) (model.Document, error) {
	raw, err := o.reads.GetFields(ctx, bucket, id, ringScalars[o.layout][bucket])
	if err != nil {
		return model.Document{}, fmt.Errorf("reading %s %s in %s: %w", bucket, id, o.database, err)
	}
	return ringDocument(bucket, id, raw)
}

// getDocuments reads the documents of ids from one bucket with get-many, in the order of ids.
// It returns the documents and the frames the client sends, which is the round trips charged.
// A document that is gone fails the read exactly as getDocument does, so the arms do the same work.
func (o *ringOps) getDocuments(ctx context.Context, bucket string, ids []string) ([]model.Document, int, error) {
	if len(ids) == 0 {
		return nil, 0, nil
	}
	trips := ringGetManyFrames(len(ids))
	answers, err := o.reads.GetMany(ctx, bucket, ids, ringScalars[o.layout][bucket])
	if err != nil {
		return nil, trips, fmt.Errorf("reading %d %s documents in %s: %w", len(ids), bucket, o.database, err)
	}
	if len(answers) != len(ids) {
		return nil, trips, fmt.Errorf("reading %d %s documents in %s: the server answered %d", len(ids), bucket, o.database, len(answers))
	}
	docs := make([]model.Document, 0, len(ids))
	for i, answer := range answers {
		switch {
		case answer.Err != nil:
			return nil, trips, fmt.Errorf("reading %s %s in %s: %w", bucket, ids[i], o.database, answer.Err)
		case !answer.Found:
			return nil, trips, fmt.Errorf("reading %s %s in %s: %w", bucket, ids[i], o.database, ringclient.ErrNotFound)
		}
		doc, err := ringDocument(bucket, ids[i], answer.Doc)
		if err != nil {
			return nil, trips, err
		}
		docs = append(docs, doc)
	}
	return docs, trips, nil
}

// textField returns the string member name of an element body.
func textField(element model.Element, name string) (string, error) {
	for _, field := range element.Body {
		if field.Name == name {
			if text, ok := field.Value.(string); ok {
				return text, nil
			}
			return "", fmt.Errorf("member %q of an element of %s/%s/%s is not text", name, element.Bucket, element.Owner, element.Store)
		}
	}
	return "", fmt.Errorf("an element of %s/%s/%s has no member %q", element.Bucket, element.Owner, element.Store, name)
}

// Read runs one read.
func (o *ringOps) Read(ctx context.Context, op ReadOp) (Reply, error) {
	e := op.Entity
	var reply Reply
	add := func(ref StoreRef, limit int) ([]model.Element, error) {
		elements, trips, err := o.readStoreAt(ctx, ref, op.Offset, limit)
		reply.RoundTrips += trips
		return elements, err
	}
	// addFirst reads a store from its first element: the offset belongs to the paged store of the
	// read only, never to the members of a room page or a hash store.
	addFirst := func(ref StoreRef, limit int) ([]model.Element, error) {
		elements, trips, err := o.readStore(ctx, ref, limit)
		reply.RoundTrips += trips
		return elements, err
	}
	switch op.Kind {
	case ReadSocialFeed:
		feed, err := add(StoreRef{"User", e, "Feed"}, op.Limit)
		if err != nil {
			return Reply{}, err
		}
		if o.native {
			// The copies in the elements are the posts: no document is read.
			reply.Elements, reply.Documents, err = nativeSplit(feed)
			if err != nil {
				return Reply{}, err
			}
			break
		}
		reply.Elements = feed
		if o.multiget {
			posts := make([]string, 0, len(feed))
			for _, element := range feed {
				post, err := textField(element, "post")
				if err != nil {
					return Reply{}, err
				}
				posts = append(posts, post)
			}
			docs, trips, err := o.getDocuments(ctx, "Post", posts)
			reply.RoundTrips += trips
			if err != nil {
				return Reply{}, err
			}
			reply.Documents = docs
			break
		}
		for _, element := range feed {
			post, err := textField(element, "post")
			if err != nil {
				return Reply{}, err
			}
			doc, err := o.getDocument(ctx, "Post", post)
			reply.RoundTrips++
			if err != nil {
				return Reply{}, err
			}
			reply.Documents = append(reply.Documents, doc)
		}
	case ReadSocialInbox:
		elements, err := add(StoreRef{"User", e, "Inbox"}, op.Limit)
		if err != nil {
			return Reply{}, err
		}
		reply.Elements = elements
	case ReadSocialFollowers:
		elements, err := addFirst(StoreRef{"User", e, "Followers"}, op.Limit)
		if err != nil {
			return Reply{}, err
		}
		reply.Elements = elements
	case ReadSocialFollowScan:
		docs, trips, err := o.followScan(ctx, e)
		reply.RoundTrips += trips
		if err != nil {
			return Reply{}, err
		}
		reply.Documents = docs
	case ReadChatRoomPage:
		messages, err := add(StoreRef{"Room", e, "Messages"}, op.Limit)
		if err != nil {
			return Reply{}, err
		}
		members, err := addFirst(StoreRef{"Room", e, "Members"}, op.MemberLimit)
		if err != nil {
			return Reply{}, err
		}
		if o.native {
			var names []model.Document
			members, names, err = nativeSplit(members)
			if err != nil {
				return Reply{}, err
			}
			reply.Elements, reply.Documents = append(messages, members...), names
			break
		}
		reply.Elements = append(messages, members...)
		if o.multiget {
			users := make([]string, len(members))
			for i, member := range members {
				users[i] = member.Key
			}
			docs, trips, err := o.getDocuments(ctx, "User", users)
			reply.RoundTrips += trips
			if err != nil {
				return Reply{}, err
			}
			reply.Documents = docs
			break
		}
		for _, member := range members {
			doc, err := o.getDocument(ctx, "User", member.Key)
			reply.RoundTrips++
			if err != nil {
				return Reply{}, err
			}
			reply.Documents = append(reply.Documents, doc)
		}
	case ReadChatUserRooms:
		elements, err := addFirst(StoreRef{"User", e, "Rooms"}, op.Limit)
		if err != nil {
			return Reply{}, err
		}
		reply.Elements = elements
	case ReadLedgerPage, ReadLedgerAll:
		elements, err := add(StoreRef{"Account", e, "Expenses"}, op.Limit)
		if err != nil {
			return Reply{}, err
		}
		reply.Elements = elements
	case ReadLedgerTotals:
		totals, trips, err := o.ledgerTotals(ctx, e)
		if err != nil {
			return Reply{}, err
		}
		reply.Totals, reply.RoundTrips = totals, trips
	case ReadLedgerInvoices:
		elements, err := addFirst(StoreRef{"Account", e, "Invoices"}, op.Limit)
		if err != nil {
			return Reply{}, err
		}
		reply.Elements = elements
	case ReadAgentMemories:
		elements, err := addFirst(StoreRef{"User", e, "Memories"}, op.Limit)
		if err != nil {
			return Reply{}, err
		}
		reply.Elements = elements
	case ReadAgentSession:
		elements, err := add(StoreRef{"Session", e, "Messages"}, op.Limit)
		if err != nil {
			return Reply{}, err
		}
		reply.Elements = elements
	default:
		return Reply{}, fmt.Errorf("ring: unknown read %s", op.Kind)
	}
	return reply, nil
}

// ReadStore implements StoreReader: every element of one store, with Key set for keyed stores.
// It is not counted in any round trip total.
func (o *ringOps) ReadStore(ctx context.Context, ref StoreRef) ([]model.Element, error) {
	elements, _, err := o.readStore(ctx, ref, 0)
	if err != nil || !o.native {
		return elements, err
	}
	// A native adapter hands the audit the common shape; ReadEmbeds has the copies.
	common, _, err := nativeSplit(elements)
	return common, err
}

// followScan lists every Follow document whose To is user. It is O(documents) on this engine.
func (o *ringOps) followScan(ctx context.Context, user string) ([]model.Document, int, error) {
	var docs []model.Document
	var cursor []byte
	trips := 0
	for {
		page, err := o.bucket("Follow").List(ctx, bson.D{{Key: "To", Value: user}}, ringclient.PageRequest{Cursor: cursor, Fields: ringScalars[o.layout]["Follow"]})
		trips++
		if err != nil {
			return nil, trips, fmt.Errorf("listing the Follow documents that point at %s in %s: %w", user, o.database, err)
		}
		for _, entry := range page.Entries {
			doc, err := ringDocument("Follow", entry.ID, entry.Body)
			if err != nil {
				return nil, trips, err
			}
			docs = append(docs, doc)
		}
		if !page.More {
			return docs, trips, nil
		}
		cursor = page.Cursor
	}
}

// acknowledged turns an owner outcome that is not "applied" into an error.
func acknowledged(doing string, written ringclient.WriteResult, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", doing, err)
	}
	if !written.Applied() {
		return fmt.Errorf("%s: the owner's outcome was %q, not applied", doing, written.Outcome)
	}
	return nil
}

// Write runs one write: the claim if any, the pre-reads, the documents, then the elements.
func (o *ringOps) Write(ctx context.Context, op WriteOp) (Reply, error) {
	var reply Reply
	for _, ref := range op.PreRead {
		_, trips, err := o.readStore(ctx, ref, 0)
		reply.RoundTrips += trips
		if err != nil {
			return Reply{}, err
		}
	}
	if op.Claim != nil {
		claimed, trips, err := o.claim(ctx, op.Claim.Run)
		reply.RoundTrips += trips
		if err != nil {
			return Reply{}, err
		}
		if claimed == nil {
			return reply, nil
		}
		reply.Elements = []model.Element{*claimed}
	}
	writes, err := o.ringWrites(op)
	if err != nil {
		return Reply{}, err
	}
	trips, err := o.applyWrites(ctx, writes)
	reply.RoundTrips += trips
	if err != nil {
		return Reply{}, err
	}
	return reply, nil
}

// claim leases one task of run, acknowledges it and returns it; nil when the queue is empty.
func (o *ringOps) claim(ctx context.Context, run string) (*model.Element, int, error) {
	bucket := o.bucket("Run")
	lease, err := bucket.Claim(ctx, run, "Tasks", 1)
	trips := 1
	if err := acknowledged(fmt.Sprintf("claiming a task of run %s in %s", run, o.database), lease.Write, err); err != nil {
		return nil, trips, err
	}
	if len(lease.Elements) == 0 {
		return nil, trips, nil
	}
	ref := StoreRef{"Run", run, "Tasks"}
	task, err := ringElement(ref, "", lease.Elements[0])
	if err != nil {
		return nil, trips, err
	}
	change, err := bucket.Ack(ctx, run, "Tasks", lease.ID)
	trips++
	if err := acknowledged(fmt.Sprintf("acknowledging the task of run %s in %s", run, o.database), change.Write, err); err != nil {
		return nil, trips, err
	}
	return &task, trips, nil
}

// Count returns the element count of each store, or the document count of a bucket.
func (o *ringOps) Count(ctx context.Context, refs []StoreRef) (map[StoreRef]int64, error) {
	counts := make(map[StoreRef]int64, len(refs))
	for _, ref := range refs {
		var (
			n   int64
			err error
		)
		if ref.Store == "" {
			n, err = o.bucket(ref.Bucket).Count(ctx, nil)
		} else {
			n, err = o.bucket(ref.Bucket).StoreCount(ctx, ref.Owner, ref.Store)
		}
		if err != nil {
			return nil, fmt.Errorf("counting %s/%s/%s in %s: %w", ref.Bucket, ref.Owner, ref.Store, o.database, err)
		}
		counts[ref] = n
	}
	return counts, nil
}
