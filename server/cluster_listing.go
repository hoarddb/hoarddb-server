package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A node holds the catalog entries of the buckets it holds a copy of, not of
// every bucket in the cluster, so a listing answered from its own catalog was
// the node's part of the answer (BUG-0468). The node a client reached asks
// every member for its part, in parallel, and merges them by name: one round
// trip per member, nothing kept per bucket, and nothing a membership change has
// to update. A member that does not answer refuses the listing (owner ruling,
// 2026-10-18): a short list would read as a correct one.

// ErrListingIncomplete is the refusal of a cluster-wide listing that a member
// did not answer. It is retryable: the member is down or the membership is
// moving.
var ErrListingIncomplete = errors.New("a member did not answer a cluster-wide listing")

// clusterListingTimeout bounds the wait for every member's part of a listing.
// A member's part is a walk of its own catalog, so a member that has not
// answered in this time is down or overloaded, and the client is told to retry
// rather than kept waiting on it.
//
// It must stay clearly below the drivers' default per-request deadline
// (the driver spec §5.2, RequestTimeout, 10 s): at that deadline or
// above it, a hung member makes the client time out locally and never read
// the refusal that names the member and says retry. Half of it leaves the
// coordinator's own walk and the reply room inside a default deadline.
const clusterListingTimeout = 5 * time.Second

// memberListDatabasesRequest is the LIST_DATABASES payload one member sends
// another for its part of a cluster-wide listing. It is internode only and
// not the client's ListDatabasesRequest, whose fields the driver vectors pin;
// a client that sends owned_counts anyway gets this node's owned counts, which
// tell it nothing it could not compute from the ring.
type memberListDatabasesRequest struct {
	// OwnedCounts asks the member to count, per database, only the buckets it
	// is the ring owner of, so the asker can sum the members' counts without
	// naming every bucket. A node outside a cluster ignores it.
	OwnedCounts bool `bson:"owned_counts,omitempty"`
}

// isClusterListingOpcode reports whether op lists what the cluster holds, and
// so is answered by asking every member.
func isClusterListingOpcode(op transport.Opcode) bool {
	return op == transport.OpListBuckets || op == transport.OpListDBs
}

// serveClusterListing answers a client's LIST_BUCKETS or LIST_DATABASES with
// the whole cluster's listing.
func (c *Cluster) serveClusterListing(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	if frame.Opcode == transport.OpListDBs {
		resp, err := c.listDatabasesClusterWide(ctx)
		if err != nil {
			return errorFrame(frame, err), nil
		}
		return respond(frame, transport.OpListDBs, databasesVisibleTo(ctx, resp))
	}
	var req ListBucketsRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorFrame(frame, fmt.Errorf("unmarshal list buckets request: %w", err)), nil
	}
	resp, err := c.listBucketsClusterWide(ctx, req)
	if err != nil {
		return errorFrame(frame, err), nil
	}
	return respond(frame, transport.OpListBuckets, resp)
}

// memberPart is one member's answer to a listing: the member and its reply
// frame's opcode and payload.
type memberPart struct {
	member  ring.Node
	opcode  transport.Opcode
	payload []byte
}

// askEveryOtherMember sends one listing request to every other member at once
// and returns their answers, or ErrListingIncomplete naming the first member,
// in member order, that did not answer within clusterListingTimeout.
func (c *Cluster) askEveryOtherMember(ctx context.Context, op transport.Opcode, request any) ([]memberPart, error) {
	payload, err := bson.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encoding the %s request for the other members: %w", op, err)
	}
	var others []ring.Node
	for _, m := range c.Members() {
		if m.ID != c.self.ID {
			others = append(others, m)
		}
	}
	askCtx, cancel := context.WithTimeout(ctx, clusterListingTimeout)
	defer cancel()

	parts := make([]memberPart, len(others))
	failures := make([]error, len(others))
	var asked sync.WaitGroup
	for i, member := range others {
		asked.Add(1)
		go func() {
			defer asked.Done()
			parts[i], failures[i] = c.askMemberForPart(askCtx, member, op, payload)
		}()
	}
	asked.Wait()
	for _, failure := range failures {
		if failure != nil {
			return nil, failure
		}
	}
	return parts, nil
}

// askMemberForPart asks one member for its own part of a listing. The request
// goes as a forwarded read with no bucket, which the member answers from its
// own catalog (Cluster.applyLocal) and never asks further.
func (c *Cluster) askMemberForPart(ctx context.Context, member ring.Node, op transport.Opcode, payload []byte) (memberPart, error) {
	req := &ReplPushRequest{
		Token:          c.localToken(),
		FromNode:       c.self.ID,
		Entries:        []ReplEntry{{Op: int32(op), Payload: payload}},
		ReturnResponse: true,
		Epoch:          c.Epoch(),
	}
	resp, err := c.peerFor(member).call(ctx, transport.LaneRequest, req)
	if err != nil {
		return memberPart{}, listingIncomplete(op, member, err.Error())
	}
	if !resp.Success {
		return memberPart{}, listingIncomplete(op, member, resp.Error)
	}
	if len(resp.Responses) != 1 {
		return memberPart{}, listingIncomplete(op, member, fmt.Sprintf("it returned %d answers for one request", len(resp.Responses)))
	}
	answer := resp.Responses[0]
	return memberPart{member: member, opcode: transport.Opcode(answer.Opcode), payload: answer.Payload}, nil
}

// The two fixed phrases around the member's name in a listing refusal.
// ListingRefusalMember reads the name back out by them, so the refusal and the
// reader cannot drift apart.
const (
	listingRefusalBeforeMember = "asks every member of the cluster, and member "
	listingRefusalAfterMember  = " did not answer ("
)

// listingIncomplete is the refusal of a listing a member did not answer: it
// names the member, why, and that a retry is the way out.
func listingIncomplete(op transport.Opcode, member ring.Node, why string) error {
	return &sentencedError{cause: ErrListingIncomplete, message: fmt.Sprintf(
		"%s "+listingRefusalBeforeMember+"%s"+listingRefusalAfterMember+"%s), so the listing would be incomplete and was not sent. "+
			"Retry in a moment; if the member is down, run 'nodes' to see every member and its state",
		op, member.ID, why)}
}

// ListingRefusalMember returns the member a cluster-wide listing refusal
// names, and false when message is not such a refusal. The CLI's `use` reads
// it: a listing the cluster refuses for one silent member must not stop a
// session selecting a database (BUG-0687).
func ListingRefusalMember(message string) (string, bool) {
	_, afterPhrase, found := strings.Cut(message, listingRefusalBeforeMember)
	if !found {
		return "", false
	}
	member, _, found := strings.Cut(afterPhrase, listingRefusalAfterMember)
	return member, found && member != ""
}

// decodePart decodes a member's part into into. A member's refusal is
// returned as notFound when it is "not found" (the member holds nothing of the
// database), and as an error naming the member otherwise.
func decodePart(op transport.Opcode, part memberPart, into any) (notFound bool, err error) {
	if part.opcode == transport.OpError {
		var refusal ErrorResponse
		if err := bson.Unmarshal(part.payload, &refusal); err != nil {
			return false, fmt.Errorf("decoding member %s's refusal of %s: %w", part.member.ID, op, err)
		}
		if refusal.Code == int(transport.ErrNotFound) {
			return true, nil
		}
		return false, listingIncomplete(op, part.member, refusal.Message)
	}
	if err := bson.Unmarshal(part.payload, into); err != nil {
		return false, fmt.Errorf("decoding member %s's part of %s: %w", part.member.ID, op, err)
	}
	return false, nil
}

// listBucketsClusterWide is LIST_BUCKETS for the whole cluster: the union of
// every member's buckets in the database, one row per bucket. A bucket's row,
// and so its entry count, is this node's when it holds a copy, so a copy it
// keeps outside the bucket's set still shows as leaving here (user docs,
// limitations); else the owner's when the owner holds it, so every other node
// answers with the same row; else that of the first member that answered.
func (c *Cluster) listBucketsClusterWide(ctx context.Context, req ListBucketsRequest) (*ListBucketsResponse, error) {
	if req.Database == "" {
		return c.s.ListBuckets(req)
	}
	parts, err := c.askEveryOtherMember(ctx, transport.OpListBuckets, req)
	if err != nil {
		return nil, err
	}

	const (
		fromOtherHolder = iota
		fromOwner
		fromThisNode
	)
	rows := map[string]BucketInfo{}
	rowSource := map[string]int{}
	found := false
	merge := func(member string, listing *ListBucketsResponse) error {
		found = true
		for _, row := range listing.Buckets {
			source := fromOtherHolder
			if member == c.self.ID {
				source = fromThisNode
			} else {
				owner, err := c.Owner(placementOf(req.Database, row.Name))
				if err != nil {
					return fmt.Errorf("placing bucket %q of database %q to merge the listing: %w", row.Name, req.Database, err)
				}
				if owner.ID == member {
					source = fromOwner
				}
			}
			if previous, seen := rowSource[row.Name]; !seen || source > previous {
				rows[row.Name] = row
				rowSource[row.Name] = source
			}
		}
		return nil
	}

	local, err := c.s.ListBuckets(req)
	switch {
	case errors.Is(err, ErrDatabaseNotFound):
	case err != nil:
		return nil, err
	default:
		if err := merge(c.self.ID, local); err != nil {
			return nil, err
		}
	}
	for _, part := range parts {
		var listing ListBucketsResponse
		notFound, err := decodePart(transport.OpListBuckets, part, &listing)
		if err != nil {
			return nil, err
		}
		if notFound {
			continue
		}
		if err := merge(part.member.ID, &listing); err != nil {
			return nil, err
		}
	}
	if !found {
		return nil, databaseNotFoundInClusterErr(req.Database)
	}

	names := make([]string, 0, len(rows))
	for name := range rows {
		names = append(names, name)
	}
	sort.Strings(names)
	resp := &ListBucketsResponse{
		Found:     true,
		Database:  req.Database,
		Buckets:   make([]BucketInfo, 0, len(names)),
		Encrypted: c.s.encryptedLabel(),
	}
	for _, name := range names {
		resp.Buckets = append(resp.Buckets, rows[name])
	}
	if len(resp.Buckets) == 0 {
		resp.Message = fmt.Sprintf(
			"database %q has no buckets yet — create one with 'create bucket <name> in %s { type: hash }'",
			req.Database, req.Database)
	}
	return resp, nil
}

// listDatabasesClusterWide is LIST_DATABASES for the whole cluster: every
// database any member has, with the sum of the members' owned bucket counts.
// Each bucket has one ring owner, so the sum counts it once at any factor. A
// bucket whose new owner has not copied it in yet, after a membership change,
// is in no owner's catalog, so the count can be short by those buckets until
// the pull lands; LIST_BUCKETS, which merges names, is not.
func (c *Cluster) listDatabasesClusterWide(ctx context.Context) (*ListDatabasesResponse, error) {
	parts, err := c.askEveryOtherMember(ctx, transport.OpListDBs, memberListDatabasesRequest{OwnedCounts: true})
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	add := func(listing *ListDatabasesResponse) {
		for _, db := range listing.Databases {
			counts[db.Name] += db.BucketCount
		}
	}
	local, err := c.s.listDatabases(true)
	if err != nil {
		return nil, err
	}
	add(local)
	for _, part := range parts {
		var listing ListDatabasesResponse
		notFound, err := decodePart(transport.OpListDBs, part, &listing)
		if err != nil {
			return nil, err
		}
		if notFound {
			continue
		}
		add(&listing)
	}

	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	resp := &ListDatabasesResponse{Databases: make([]DatabaseInfo, 0, len(names))}
	for _, name := range names {
		resp.Databases = append(resp.Databases, DatabaseInfo{Name: name, BucketCount: counts[name]})
	}
	if len(resp.Databases) == 0 {
		resp.Message = NoDatabasesMessage
	}
	return resp, nil
}
