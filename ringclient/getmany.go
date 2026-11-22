package ringclient

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// getManyFrameKeys is the most keys one RING_GET_MANY frame carries.
const getManyFrameKeys = 255

// getManyFramesInFlight bounds how many frames one GetMany call has on the connection at once, so
// a call with a million ids queues its frames and does not open a million waiters.
const getManyFramesInFlight = 16

// Key names one document for [Client.GetMany].
type Key struct {
	// Bucket is the bucket the document lives in. Keys of one call may name different buckets and
	// databases; every Bucket must belong to the Client the call is made on.
	Bucket *Bucket
	// ID is the document id.
	ID string
	// Fields names the scalar fields to return (a get_fields); nil returns the whole document, as Get does.
	Fields []string
	// Local lets a ready copy on the node answer this key; its answer then says ServedFrom "local".
	Local bool
}

// GetAnswer is what one key of a get-many came to. A document that is not there is Found false
// with no Err: a miss is data, as in a list.
type GetAnswer struct {
	// Doc is the document, when Found.
	Doc bson.Raw
	// Found says the document exists.
	Found bool
	// Err is the server's refusal of this key, an *[Error], or the reason the key could not be
	// built; the other members are zero when it is set.
	Err error
	// ServedFrom is "local" when a ready copy on the node answered and "owner" when the owner did
	// (when the server says which); empty otherwise.
	ServedFrom string
}

// GetMany reads the documents of keys, on any buckets and databases, and returns one answer per
// key in the order of keys. It is MongoDB's find with $in, with a slot per key instead of a
// result that omits what is missing: a missing document is Found false, and a key the server
// refused (a bucket the session may not read, a document too large for the reply, an owner that is
// down) is that key's Err while the rest are answered. The returned error is for a failure of a
// whole frame (the connection, a refusal that names no key); then the answers are nil.
//
// More than 255 keys are sent as concurrent frames of 255 and rejoined in order. Each key is read
// at its owner's current state: the keys are not one snapshot. A key refused because its bucket's
// declaration or id moved describes that bucket again once and is sent once more with its bucket's
// other such keys. A server that does not know the opcode answers with its own refusal, which is
// returned as it came; nothing falls back to lone reads. [WithWriteDeadline] sets the frame's
// deadline_ms: keys not read when it is spent answer UNAVAILABLE. A request id option is ignored:
// reads are repeatable and a frame draws its own.
func (c *Client) GetMany(ctx context.Context, keys []Key, options ...CallOption) ([]GetAnswer, error) {
	config := newCallConfig(options)
	answers := make([]GetAnswer, len(keys))
	work := make([]manyRead, 0, len(keys))
	var checkedFields []string
	for index, key := range keys {
		switch {
		case key.Bucket == nil:
			return nil, fmt.Errorf("ringclient: key %d of the get-many names no Bucket. Set Key.Bucket with Client.Bucket(database, name)", index)
		case key.Bucket.client != c:
			return nil, fmt.Errorf("ringclient: key %d of the get-many names a Bucket of another Client. Make the Bucket with the Client that calls GetMany", index)
		case key.Bucket.batch != nil:
			return nil, fmt.Errorf("ringclient: key %d of the get-many names %s through Bucket.In(batch), and a batch carries writes only. Use a Bucket that was not made with In", index, key.Bucket.where())
		}
		encoded, _, err := encodedID(key.ID)
		if err != nil {
			answers[index].Err = err
			continue
		}
		// The frame's codec refuses a bad Fields list for the whole frame; the key that carries
		// it is refused here, so the other keys are still read. A call's ids usually share one
		// list, so it is judged once.
		if key.Fields != nil && (checkedFields == nil || !slices.Equal(key.Fields, checkedFields)) {
			if err := wire.CheckRingGetFields(key.Fields); err != nil {
				answers[index].Err = fmt.Errorf("ringclient: key %d of the get-many (%q in %s): %w", index, key.ID, key.Bucket.where(), err)
				continue
			}
			checkedFields = key.Fields
		}
		work = append(work, manyRead{index: index, key: key, encoded: encoded})
	}
	infos := c.manyHeaders(ctx, work, answers)
	live := work[:0]
	for _, read := range work {
		if answers[read.index].Err == nil {
			live = append(live, read)
		}
	}
	if err := c.sendMany(ctx, config.deadlineMs, live, infos, answers); err != nil {
		return nil, err
	}
	if err := c.resendMoved(ctx, config.deadlineMs, live, infos, answers); err != nil {
		return nil, err
	}
	return answers, nil
}

// GetMany reads the documents ids of this bucket in one frame (more than 255 ids go as concurrent
// frames), each as Get does, or as GetFields does when fields is not nil. See [Client.GetMany].
func (b *Bucket) GetMany(ctx context.Context, ids []string, fields []string, options ...CallOption) ([]GetAnswer, error) {
	keys := make([]Key, len(ids))
	for index, id := range ids {
		keys[index] = Key{Bucket: b, ID: id, Fields: fields}
	}
	return b.client.GetMany(ctx, keys, options...)
}

// manyRead is one key of the call with its encoded id; index is its place in the answers.
type manyRead struct {
	index   int
	key     Key
	encoded []byte
}

// bucketKey names a bucket for the per-call header table.
func bucketKey(bucket *Bucket) string { return bucket.database + "\x00" + bucket.bucket }

// manyHeaders learns the header of every distinct bucket of work, from the cache or by one
// describe each. A bucket that cannot be described fails its own keys and no others.
func (c *Client) manyHeaders(ctx context.Context, work []manyRead, answers []GetAnswer) map[string]BucketInfo {
	infos := map[string]BucketInfo{}
	failed := map[string]error{}
	for _, read := range work {
		name := bucketKey(read.key.Bucket)
		if _, ok := infos[name]; ok {
			continue
		}
		if err, ok := failed[name]; ok {
			answers[read.index].Err = err
			continue
		}
		info, err := read.key.Bucket.Info(ctx)
		if err != nil {
			failed[name] = err
			answers[read.index].Err = err
			continue
		}
		infos[name] = info
	}
	return infos
}

// sendMany sends reads in frames of 255, up to getManyFramesInFlight at once, and puts each
// answer in its place. It returns the first failure of a whole frame, and stops starting frames
// once one failed.
func (c *Client) sendMany(ctx context.Context, deadlineMs uint32, reads []manyRead, infos map[string]BucketInfo, answers []GetAnswer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		group   sync.WaitGroup
		once    sync.Once
		failure error
		slots   = make(chan struct{}, getManyFramesInFlight)
	)
	for start := 0; start < len(reads); start += getManyFrameKeys {
		chunk := reads[start:min(start+getManyFrameKeys, len(reads))]
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { <-slots }()
			if err := c.sendFrame(ctx, deadlineMs, chunk, infos, answers); err != nil {
				once.Do(func() { failure = err; cancel() })
			}
		}()
	}
	group.Wait()
	if failure != nil {
		return failure
	}
	return ctx.Err()
}

// sendFrame sends one frame of at most 255 keys and decodes its answers into answers.
func (c *Client) sendFrame(ctx context.Context, deadlineMs uint32, chunk []manyRead, infos map[string]BucketInfo, answers []GetAnswer) error {
	request := wire.RingGetManyRequest{Prologue: wire.RingPrologue{DeadlineMs: deadlineMs}, Reads: make([]wire.RingRead, len(chunk))}
	for position, read := range chunk {
		class := byte(wire.RingClassGet)
		if read.key.Fields != nil {
			class = wire.RingClassGetFields
		}
		built := readHeader(infos[bucketKey(read.key.Bucket)], class, read.encoded)
		built.Fields = read.key.Fields
		built.Options = wire.RingReadOptions{Local: read.key.Local}
		request.Reads[position] = built
	}
	body, err := wire.EncodeRingGetMany(request)
	if err != nil {
		return fmt.Errorf("ringclient: encoding the get-many frame: %w", err)
	}
	payload, err := c.call(ctx, transport.OpRingGetMany, c.NextRequestID(), body)
	if err != nil {
		return err
	}
	reply, err := wire.DecodeRingReply(payload)
	if err != nil {
		return fmt.Errorf("ringclient: decoding the get-many reply: %w", err)
	}
	if len(reply.Answers) != len(chunk) {
		return fmt.Errorf("ringclient: the server answered a get-many of %d keys with %d answers", len(chunk), len(reply.Answers))
	}
	for position, answer := range reply.Answers {
		answers[chunk[position].index] = getAnswerOf(answer, chunk[position].key)
	}
	return nil
}

// getAnswerOf turns the answer of one key into a GetAnswer.
func getAnswerOf(answer wire.RingAnswer, key Key) GetAnswer {
	if answer.Error != nil {
		return GetAnswer{Err: &Error{Code: answer.Error.Code, Message: answer.Error.Message, Detail: answer.Error.Detail}}
	}
	lone := answer.Reply
	if lone.Found == nil || !*lone.Found {
		return GetAnswer{}
	}
	if len(lone.Elements) != 1 {
		return GetAnswer{Err: fmt.Errorf("ringclient: the server answered %q of %s as found with %d documents, want 1", key.ID, key.Bucket.where(), len(lone.Elements))}
	}
	return GetAnswer{Doc: bson.Raw(lone.Elements[0]), Found: true, ServedFrom: lone.Served}
}

// movedRead is a key refused for a reason a fresh bucket header can fix.
type movedRead struct {
	read    manyRead
	refusal *Error
}

// resendMoved describes again, once, each bucket that answered a key with a refusal a fresh header
// can fix (SCHEMA_NOT_CURRENT, or an unknown bucket id when the header really changed), as a lone
// Get does, and sends only those keys once more. A key still refused keeps its refusal.
func (c *Client) resendMoved(ctx context.Context, deadlineMs uint32, reads []manyRead, infos map[string]BucketInfo, answers []GetAnswer) error {
	moved := map[string][]movedRead{}
	var order []string
	for _, read := range reads {
		var refusal *Error
		if !errors.As(answers[read.index].Err, &refusal) || (refusal.Code != cluster.CodeSchemaNotCurrent && !staleBucketID(refusal)) {
			continue
		}
		name := bucketKey(read.key.Bucket)
		if _, seen := moved[name]; !seen {
			order = append(order, name)
		}
		moved[name] = append(moved[name], movedRead{read: read, refusal: refusal})
	}
	var again []manyRead
	for _, name := range order {
		keys := moved[name]
		bucket := keys[0].read.key.Bucket
		fresh, err := c.DescribeBucket(ctx, bucket.database, bucket.bucket)
		if err != nil {
			for _, key := range keys {
				answers[key.read.index].Err = fmt.Errorf("refreshing the header of %s after %s: %w", bucket.where(), key.refusal.Code, err)
			}
			continue
		}
		old := infos[name]
		changed := fresh.ID != old.ID || fresh.DeclVersion != old.DeclVersion || fresh.SchemaVersionSeen != old.SchemaVersionSeen
		infos[name] = fresh
		for _, key := range keys {
			if key.refusal.Code == cluster.CodeSchemaNotCurrent || changed {
				again = append(again, key.read)
			}
		}
	}
	if len(again) == 0 {
		return nil
	}
	// Request order, so the frames of 255 hold the earliest keys first.
	slices.SortFunc(again, func(left, right manyRead) int { return left.index - right.index })
	return c.sendMany(ctx, deadlineMs, again, infos, answers)
}
