package main

import (
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The -hoard-batch variant (batch-request spec §7): each write operation's
// sub-operations, and a post page's three reads, go out as one BATCH request
// instead of one request each. A batch is not atomic: every item is answered
// on its own, with its own durability and quorum wait. The variant is recorded
// as "batch" and is never the comparison of record, which sends one write at a
// time on every engine (owner decision D7).

// batchItemLimit is the most items one BATCH carries.
const batchItemLimit = 1000

// hoardBatch collects the items of one BATCH and what each one does, for an
// error that names it.
type hoardBatch struct {
	items []server.BatchItem
	doing []string
}

// add appends one item: the request a lone call would send.
func (b *hoardBatch) add(op transport.Opcode, request interface{}, doing string) error {
	payload, err := bson.Marshal(request)
	if err != nil {
		return fmt.Errorf("%s: encoding the batch item: %w", doing, err)
	}
	b.items = append(b.items, server.BatchItem{Opcode: int32(op), Payload: payload})
	b.doing = append(b.doing, doing)
	return nil
}

// sendBatch sends the items as one request and returns one answer per item.
func (o *hoardOps) sendBatch(batch *hoardBatch) ([]server.BatchAnswer, error) {
	var reply server.BatchResponse
	if err := o.caller.call(transport.OpBatch, server.BatchRequest{Items: batch.items}, &reply); err != nil {
		return nil, fmt.Errorf("sending a batch of %d items (%s first): %w", len(batch.items), batch.doing[0], err)
	}
	if len(reply.Answers) != len(batch.items) {
		return nil, fmt.Errorf("a batch of %d items (%s first) was answered %d times", len(batch.items), batch.doing[0], len(reply.Answers))
	}
	return reply.Answers, nil
}

// batchAnswerError is the error of an item whose answer is not a success, as
// the lone call reports it, or nil.
func batchAnswerError(answer server.BatchAnswer, doing string) error {
	if transport.Opcode(answer.Opcode) == transport.OpError {
		var refusal server.ErrorResponse
		if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
			return fmt.Errorf("%s: an error answer that could not be read: %w", doing, err)
		}
		return fmt.Errorf("%s: %w", doing, &serverError{op: transport.OpBatch, code: transport.ErrorCode(refusal.Code), message: refusal.Message})
	}
	success, err := bson.Raw(answer.Payload).LookupErr("success")
	if err != nil {
		return nil
	}
	if ok, isBool := success.BooleanOK(); isBool && !ok {
		message, _ := bson.Raw(answer.Payload).Lookup("message").StringValueOK()
		return fmt.Errorf("%s: %s", doing, message)
	}
	return nil
}

// sendWrites sends a batch of writes and returns the first item's error, in
// batch order, after every item has been answered: a lone operation stops at
// its first failure, and the batch's other items stand as they were answered.
func (o *hoardOps) sendWrites(batch *hoardBatch) error {
	answers, err := o.sendBatch(batch)
	if err != nil {
		return err
	}
	for i, answer := range answers {
		if err := batchAnswerError(answer, batch.doing[i]); err != nil {
			return err
		}
	}
	return nil
}

// publishPostBatched stores the post and copies it into every follower's feed
// in batches of at most 1,000 items, the first carrying the post: with 999
// followers or fewer, the publish is the follower list and one batch. A chunk
// whose answers hold a failure ends the publish, as the documented loop does.
func (o *hoardOps) publishPostBatched(post postRecord, author int, followers []int, ledger deliveryLedger) (int, error) {
	batch := &hoardBatch{}
	put := o.field(author, "Posts")
	put.Element, put.Data = postKey(post.Seq), post.document()
	if err := batch.add(transport.OpFieldPut, put, fmt.Sprintf("storing post %d of user %d", post.Seq, author)); err != nil {
		return 0, err
	}
	recipients := []int{-1}
	for _, index := range o.place.byShard(followers) {
		follower := followers[index]
		push := o.field(follower, "Feed")
		push.Data = post.document()
		if err := batch.add(transport.OpFieldPush, push, fmt.Sprintf("copying post %d of user %d into the feed of user %d", post.Seq, author, follower)); err != nil {
			return 0, err
		}
		recipients = append(recipients, follower)
		if len(batch.items) == batchItemLimit {
			if err := o.sendPublishChunk(batch, recipients, ledger); err != nil {
				return 0, err
			}
			batch, recipients = &hoardBatch{}, nil
		}
	}
	if len(batch.items) > 0 {
		if err := o.sendPublishChunk(batch, recipients, ledger); err != nil {
			return 0, err
		}
	}
	return len(followers), nil
}

// sendPublishChunk sends one chunk of a publish. recipients[i] is the
// follower item i delivers to, or -1 for the post itself. Every follower of the
// chunk is marked sending before it goes out, and delivered when its item's
// answer is a success.
func (o *hoardOps) sendPublishChunk(batch *hoardBatch, recipients []int, ledger deliveryLedger) error {
	for _, follower := range recipients {
		if follower >= 0 {
			ledger.sending(follower)
		}
	}
	answers, err := o.sendBatch(batch)
	if err != nil {
		return err
	}
	var first error
	for i, answer := range answers {
		if itemErr := batchAnswerError(answer, batch.doing[i]); itemErr != nil {
			if first == nil {
				first = itemErr
			}
			continue
		}
		if recipients[i] >= 0 {
			ledger.delivered(recipients[i])
		}
	}
	return first
}

// likePostBatched is the like and its notification, one chain of the author's
// bucket.
func (o *hoardOps) likePostBatched(liker, author int, seq int64) error {
	batch := &hoardBatch{}
	put := o.postStore(author, seq, "Likes")
	put.Element, put.Data = userID(liker), map[string]interface{}{"liker": userID(liker), "ts": time.Now().UTC()}
	if err := batch.add(transport.OpFieldPut, put, fmt.Sprintf("user %d liking post %d of user %d", liker, seq, author)); err != nil {
		return err
	}
	if err := o.addNotify(batch, author, "like", liker, fmt.Sprintf("%d/%s", author, postKey(seq))); err != nil {
		return err
	}
	return o.sendWrites(batch)
}

// commentOnPostBatched is the comment and its notification.
func (o *hoardOps) commentOnPostBatched(commenter, author int, seq int64, text string) error {
	batch := &hoardBatch{}
	push := o.postStore(author, seq, "Comments")
	push.Data = map[string]interface{}{"author": userID(commenter), "text": text, "ts": time.Now().UTC()}
	if err := batch.add(transport.OpFieldPush, push, fmt.Sprintf("user %d commenting on post %d of user %d", commenter, seq, author)); err != nil {
		return err
	}
	if err := o.addNotify(batch, author, "comment", commenter, fmt.Sprintf("%d/%s", author, postKey(seq))); err != nil {
		return err
	}
	return o.sendWrites(batch)
}

// addNotify is notify's push as a batch item.
func (o *hoardOps) addNotify(batch *hoardBatch, user int, kind string, actor int, subject string) error {
	request := o.field(user, "Inbox")
	request.Data = map[string]interface{}{"type": kind, "actor": userID(actor), "target": subject, "ts": time.Now().UTC()}
	return batch.add(transport.OpFieldPush, request, fmt.Sprintf("notifying user %d of a %s", user, kind))
}

// addCounts is adjustCounts' two updates as batch items.
func (o *hoardOps) addCounts(batch *hoardBatch, actor, target int, delta int64) error {
	for _, change := range []struct {
		user    int
		counter string
	}{{target, "followers_n"}, {actor, "following_n"}} {
		request := server.UpdateRequest{Database: o.database, Bucket: o.place.bucketOf(change.user), Key: userID(change.user), Inc: map[string]int64{change.counter: delta}}
		if err := batch.add(transport.OpUpdate, request, fmt.Sprintf("changing %s of user %d", change.counter, change.user)); err != nil {
			return err
		}
	}
	return nil
}

// followBatched is the two edges, the two counters and the notification:
// the actor's side is a two-item chain, the target's a three-item chain.
func (o *hoardOps) followBatched(actor, target int) error {
	since := map[string]interface{}{"since": time.Now().UTC()}
	following := o.field(actor, "Following")
	following.Element, following.Data = userID(target), since
	followers := o.field(target, "Followers")
	followers.Element, followers.Data = userID(actor), since
	doing := fmt.Sprintf("user %d following user %d", actor, target)
	batch := &hoardBatch{}
	if err := batch.add(transport.OpFieldPut, following, doing); err != nil {
		return err
	}
	if err := batch.add(transport.OpFieldPut, followers, doing); err != nil {
		return err
	}
	if err := o.addCounts(batch, actor, target, 1); err != nil {
		return err
	}
	if err := o.addNotify(batch, target, "follow", actor, userID(target)); err != nil {
		return err
	}
	return o.sendWrites(batch)
}

// unfollowBatched is the two edges and the two counters.
func (o *hoardOps) unfollowBatched(actor, target int) error {
	following := o.field(actor, "Following")
	following.Element = userID(target)
	followers := o.field(target, "Followers")
	followers.Element = userID(actor)
	doing := fmt.Sprintf("user %d unfollowing user %d", actor, target)
	batch := &hoardBatch{}
	if err := batch.add(transport.OpFieldDelete, following, doing); err != nil {
		return err
	}
	if err := batch.add(transport.OpFieldDelete, followers, doing); err != nil {
		return err
	}
	if err := o.addCounts(batch, actor, target, -1); err != nil {
		return err
	}
	return o.sendWrites(batch)
}

// postPageBatched is the post and its two counts in one request. A post that
// is not there answers found false, and the counts beside it are not read.
func (o *hoardOps) postPageBatched(author int, seq int64) (postPage, error) {
	doing := fmt.Sprintf("reading post %d of user %d", seq, author)
	getRequest := o.field(author, "Posts")
	getRequest.Element = postKey(seq)
	batch := &hoardBatch{}
	for _, item := range []struct {
		op      transport.Opcode
		request server.FieldRequest
		doing   string
	}{
		{transport.OpFieldGet, getRequest, doing},
		{transport.OpFieldLen, o.postStore(author, seq, "Likes"), doing + ": counting likes"},
		{transport.OpFieldLen, o.postStore(author, seq, "Comments"), doing + ": counting comments"},
	} {
		if err := batch.add(item.op, item.request, item.doing); err != nil {
			return postPage{}, err
		}
	}
	answers, err := o.sendBatch(batch)
	if err != nil {
		return postPage{}, err
	}
	if err := batchAnswerError(answers[0], doing); err != nil {
		return postPage{}, err
	}
	var got server.FieldResponse
	if err := bson.Unmarshal(answers[0].Payload, &got); err != nil {
		return postPage{}, fmt.Errorf("%s: decoding: %w", doing, err)
	}
	if !got.Found {
		return postPage{}, nil
	}
	page := postPage{Found: true}
	if err := bson.Unmarshal(got.Element, &page.Post); err != nil {
		return postPage{}, fmt.Errorf("%s: decoding: %w", doing, err)
	}
	counts := make([]int64, 2)
	for i := range counts {
		if err := batchAnswerError(answers[i+1], batch.doing[i+1]); err != nil {
			return postPage{}, err
		}
		n, _ := bson.Raw(answers[i+1].Payload).Lookup("len").AsInt64OK()
		counts[i] = n
	}
	page.Likes, page.Comments = counts[0], counts[1]
	return page, nil
}
