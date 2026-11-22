package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The limits of one BATCH request (batch-request spec §3.4, owner decision D4).
const (
	// batchMaxItems is the most items one batch carries: a fan-out to 999
	// followers plus the post is one batch.
	batchMaxItems = 1000
	// batchMaxBytes bounds the BATCH frame's payload, so a sub-batch forwarded
	// to one owner, with its per-entry framing and the door's stamps, fits
	// transport.MaxFrameLength.
	batchMaxBytes = 16 << 20
)

// errBatchTooLarge is a batch over the item count or the byte limit: the fix
// is to split it, which is not the answer to a malformed request, so it has a
// code of its own on the wire (BATCH_TOO_LARGE).
var errBatchTooLarge = errors.New("the batch is over its limits")

// errBatchShape is a batch the decode rules refuse (§3.2): INVALID_ARGUMENT.
var errBatchShape = errors.New("the batch is malformed")

// batchNothingRun ends every whole-batch refusal.
const batchNothingRun = "Nothing in the batch was run."

// batchCarriesText lists what a batch carries, for the refusal of an opcode
// it does not.
const batchCarriesText = "A batch carries push, put, delete, get, len and peek of a store field, and an update of one key."

// batchRefusal is a whole-batch refusal: nothing in the batch ran. code is the
// wire code; cause, when set, is the item gate's own error, whose code the
// refusal carries.
type batchRefusal struct {
	code    transport.ErrorCode
	message string
	cause   error
}

func (r *batchRefusal) Error() string { return r.message }

func (r *batchRefusal) Unwrap() error { return r.cause }

// batchItem is one decoded item of a batch. payload is the item's request
// bytes: as decoded, then as the door's gates leave them (§3.2: the item that
// runs is the synthetic frame after every gate, stamps included).
type batchItem struct {
	op       transport.Opcode
	payload  []byte
	database string
	bucket   string
}

// write reports whether the item changes stored state.
func (it batchItem) write() bool { return !it.op.IsReadOnly() }

// batchRequestDecoded is a batch after the decode rules: its items in order and
// the batch's one write concern (zero when it names none).
type batchRequestDecoded struct {
	items        []batchItem
	writeConcern WriteConcernLevel
	hasWrite     bool
}

// batchCarried reports whether a batch carries op (owner decision D1).
func batchCarried(op transport.Opcode) bool {
	switch op {
	case transport.OpFieldPush, transport.OpFieldPut, transport.OpFieldDelete, transport.OpUpdate,
		transport.OpFieldGet, transport.OpFieldLen, transport.OpFieldPeek:
		return true
	}
	return false
}

// decodeBatch applies §3.2's decode rules and §3.4's limits to a BATCH
// payload. Any failure refuses the whole batch, naming the item.
func decodeBatch(payload []byte) (*batchRequestDecoded, error) {
	envelope := bson.Raw(payload)
	if err := envelope.Validate(); err != nil {
		return nil, batchShapeRefusal(fmt.Sprintf("The batch is not a BSON document (%v). Send {items: [{opcode, payload}, ...]}. %s", err, batchNothingRun))
	}
	itemsValue, err := envelope.LookupErr("items")
	if err != nil {
		return nil, batchShapeRefusal("The batch has no items array. Send {items: [{opcode, payload}, ...]}, where each payload is the request the item would send on its own. " + batchNothingRun)
	}
	if itemsValue.Type == bson.TypeNull {
		return nil, batchShapeRefusal("A batch needs at least one item. Add items, or send the request on its own. " + batchNothingRun)
	}
	if itemsValue.Type != bson.TypeArray {
		return nil, batchShapeRefusal(fmt.Sprintf("The batch's items must be an array; it is %s. Send {items: [{opcode, payload}, ...]}. %s", bsonTypeName(itemsValue.Type), batchNothingRun))
	}
	values, err := itemsValue.Array().Values()
	if err != nil {
		return nil, batchShapeRefusal(fmt.Sprintf("The batch's items array cannot be read (%v). %s", err, batchNothingRun))
	}
	total := len(values)
	switch {
	case total == 0:
		return nil, batchShapeRefusal("A batch needs at least one item. Add items, or send the request on its own. " + batchNothingRun)
	case total > batchMaxItems:
		return nil, &batchRefusal{code: transport.ErrBatchTooLarge, cause: errBatchTooLarge, message: fmt.Sprintf(
			"This batch has %s items; a batch carries at most %s. %s %s",
			groupThousands(total), groupThousands(batchMaxItems), countSplitAdvice(total), batchNothingRun)}
	}
	meta := decodeFrameMeta(payload)
	wc, err := meta.writeConcern()
	if err != nil {
		return nil, &batchRefusal{code: codeFor(err), cause: err, message: fmt.Sprintf("The batch's write concern is refused: %s. %s", strings.TrimSuffix(explainError(err), "."), batchNothingRun)}
	}
	decoded := &batchRequestDecoded{items: make([]batchItem, 0, total), writeConcern: wc}
	sizes := make([]int, 0, total)
	for i, value := range values {
		item, err := decodeBatchItem(i, total, value)
		if err != nil {
			return nil, err
		}
		decoded.items = append(decoded.items, item)
		decoded.hasWrite = decoded.hasWrite || item.write()
		sizes = append(sizes, len(value.Value))
	}
	if len(payload) > batchMaxBytes {
		return nil, &batchRefusal{code: transport.ErrBatchTooLarge, cause: errBatchTooLarge, message: fmt.Sprintf(
			"This batch is %s; a batch carries at most %s. %s, or store large values in a blob field and keep a pointer. %s",
			formatMiB(int64(len(payload))), formatMiB(batchMaxBytes), sizeSplitAdvice(sizes), batchNothingRun)}
	}
	return decoded, nil
}

// decodeBatchItem decodes item i (0-based) of total: {opcode, payload}.
func decodeBatchItem(i, total int, value bson.RawValue) (batchItem, error) {
	position := fmt.Sprintf("Item %s of %s", groupThousands(i+1), groupThousands(total))
	if value.Type != bson.TypeEmbeddedDocument {
		return batchItem{}, batchShapeRefusal(fmt.Sprintf("%s is %s, not a document. Each item is {opcode, payload}, where payload is the request the item would send on its own. %s",
			position, bsonTypeName(value.Type), batchNothingRun))
	}
	itemDoc := value.Document()
	opcodeValue, err := itemDoc.LookupErr("opcode")
	if err != nil {
		return batchItem{}, batchShapeRefusal(fmt.Sprintf("%s has no opcode. Each item is {opcode, payload}, where opcode is the request's opcode (0xD0 for a field push). %s", position, batchNothingRun))
	}
	if opcodeValue.Type != bson.TypeInt32 {
		return batchItem{}, batchShapeRefusal(fmt.Sprintf("%s: opcode must be a 32-bit integer from 0 to 255; it is %s. %s", position, articled(bsonTypeName(opcodeValue.Type)), batchNothingRun))
	}
	number := opcodeValue.Int32()
	if number < 0 || number > 0xFF {
		return batchItem{}, batchShapeRefusal(fmt.Sprintf("%s: opcode must be a 32-bit integer from 0 to 255; it is %d. %s", position, number, batchNothingRun))
	}
	op := transport.Opcode(number)
	payloadValue, err := itemDoc.LookupErr("payload")
	if err != nil || payloadValue.Type != bson.TypeEmbeddedDocument {
		return batchItem{}, batchShapeRefusal(fmt.Sprintf("%s has no payload document. Each item is {opcode, payload}, where payload is the request the item would send on its own. %s", position, batchNothingRun))
	}
	itemPayload := []byte(payloadValue.Document())
	if refusal := batchOpcodeRefusal(position, op, itemPayload); refusal != nil {
		return batchItem{}, refusal
	}
	meta := decodeFrameMeta(itemPayload)
	bucket, database := meta.scope(op)
	if bucket == "" {
		return batchItem{}, batchShapeRefusal(fmt.Sprintf("%s names no bucket; every request a batch carries is addressed to one bucket. %s", position, batchNothingRun))
	}
	for _, spelling := range []string{"writeconcern", "write_concern"} {
		if _, err := bson.Raw(itemPayload).LookupErr(spelling); err == nil {
			return batchItem{}, batchShapeRefusal(fmt.Sprintf("%s names a write concern (%s); a batch takes one, on the batch. Remove it from the item and set writeconcern on the batch. %s",
				position, spelling, batchNothingRun))
		}
	}
	if len(itemPayload) > batchMaxBytes {
		return batchItem{}, batchShapeRefusal(fmt.Sprintf("%s, %s, is %s, more than a whole batch carries (%s). Send it on its own. %s",
			position, describeBatchItem(op, itemPayload), formatMiB(int64(len(itemPayload))), formatMiB(batchMaxBytes), batchNothingRun))
	}
	return batchItem{op: op, payload: itemPayload, database: database, bucket: bucket}, nil
}

// batchOpcodeRefusal refuses an opcode a batch does not carry, and an update
// that is not of one key (§3.3), with the reason the refusal gives.
func batchOpcodeRefusal(position string, op transport.Opcode, payload []byte) error {
	switch {
	case op == transport.OpBatch:
		return batchShapeRefusal(fmt.Sprintf("%s is itself a batch; a batch cannot carry another. Put its items in this batch. %s", position, batchNothingRun))
	case op == transport.OpFieldPop:
		_, database := decodeFrameMeta(payload).scope(op)
		return batchShapeRefusal(fmt.Sprintf("%s, %s, is a pop, which a batch does not carry: a lost reply would lose the popped entries. Send it on its own%s. %s",
			position, describeBatchItem(op, payload), useDatabaseHint(database), batchNothingRun))
	case op == transport.OpFieldList:
		return batchShapeRefusal(fmt.Sprintf("%s, a list of %s, is paged and its answer can fill a reply by itself, so a batch does not carry it. Send it on its own. %s",
			position, fieldTarget(payload), batchNothingRun))
	case op == transport.OpUpdate:
		return batchUpdateRefusal(position, payload)
	case batchCarried(op):
		return nil
	case isNodeToNode(op):
		return batchShapeRefusal(fmt.Sprintf("%s is %s, which only cluster members send. %s %s", position, op, batchCarriesText, batchNothingRun))
	case op.String() != "UNKNOWN":
		return batchShapeRefusal(fmt.Sprintf("%s is %s, which a batch does not carry. Send it on its own. %s %s", position, op, batchCarriesText, batchNothingRun))
	}
	return batchShapeRefusal(fmt.Sprintf("%s names opcode 0x%02X, which is not a hoardDB request. %s %s", position, uint8(op), batchCarriesText, batchNothingRun))
}

// batchUpdateRefusal refuses an update that is a scan of the bucket (a filter
// or multi) or that names no key: a batch carries an update of one key.
func batchUpdateRefusal(position string, payload []byte) error {
	var update UpdateRequest
	if err := bson.Unmarshal(payload, &update); err != nil {
		return batchShapeRefusal(fmt.Sprintf("%s is an update whose payload cannot be read (%v). %s", position, err, batchNothingRun))
	}
	keyed := fmt.Sprintf("db.%s.update(%q, {…})", update.Bucket, "<key>")
	switch {
	case update.Multi:
		return batchShapeRefusal(fmt.Sprintf("%s, an update of every matching document of db.%s, is a scan of the bucket, which a batch does not carry. Send it on its own; a batch carries an update of one key, %s. %s",
			position, update.Bucket, keyed, batchNothingRun))
	case len(update.Filter) > 0:
		return batchShapeRefusal(fmt.Sprintf("%s, an update of db.%s with a filter, is a scan of the bucket, which a batch does not carry. Send it on its own; a batch carries an update of one key, %s. %s",
			position, update.Bucket, keyed, batchNothingRun))
	case update.Key == "":
		return batchShapeRefusal(fmt.Sprintf("%s, an update of db.%s, names no key; a batch carries an update of one key, %s. %s",
			position, update.Bucket, keyed, batchNothingRun))
	}
	return nil
}

// isNodeToNode reports whether op is one only cluster members send: the
// driver listener refuses these by name (transport's isNodeToNodeOpcode).
func isNodeToNode(op transport.Opcode) bool {
	switch op {
	case transport.OpGossip, transport.OpReplPush, transport.OpReplAck, transport.OpMigrateStart, transport.OpMigrateData,
		transport.OpMigrateDone, transport.OpClusterPropose, transport.OpVersionedGet, transport.OpMembershipChange,
		transport.OpAuthConfigPush, transport.OpAuthConfigAck,
		transport.OpStreamOpen, transport.OpStreamData, transport.OpStreamEnd, transport.OpStreamAck:
		return true
	}
	return false
}

func batchShapeRefusal(message string) error {
	return &batchRefusal{code: transport.ErrInvalidArgument, cause: errBatchShape, message: message}
}

// countSplitAdvice is the computed split of a batch over the item count.
func countSplitAdvice(total int) string {
	batches := (total + batchMaxItems - 1) / batchMaxItems
	last := (batches-1)*batchMaxItems + 1
	if batches == 2 {
		return fmt.Sprintf("Send it as 2 batches: items 1 to %s, then %s to %s.",
			groupThousands(batchMaxItems), groupThousands(last), groupThousands(total))
	}
	ranges := make([]string, 0, 4)
	for b := 0; b < batches; b++ {
		if batches > 3 && b == 2 {
			ranges = append(ranges, "…")
			b = batches - 1
		}
		first := b*batchMaxItems + 1
		end := min((b+1)*batchMaxItems, total)
		ranges = append(ranges, fmt.Sprintf("%s to %s", groupThousands(first), groupThousands(end)))
	}
	return fmt.Sprintf("Send it as %d batches of at most %s items (items %s).", batches, groupThousands(batchMaxItems), strings.Join(ranges, ", "))
}

// batchItemFraming is the bytes the envelope spends on one item beside its
// payload: the item document's key, length and terminator, the opcode member.
const batchItemFraming = 64

// sizeSplitAdvice is the computed split of a batch over the byte limit: items
// are packed in order into batches of at most batchMaxBytes each.
func sizeSplitAdvice(sizes []int) string {
	batches, firstEnd, used := 1, 0, 0
	for i, size := range sizes {
		if used > 0 && used+size+batchItemFraming > batchMaxBytes-batchItemFraming {
			if batches == 1 {
				firstEnd = i
			}
			batches++
			used = 0
		}
		used += size + batchItemFraming
	}
	if firstEnd == 0 {
		firstEnd = len(sizes)
	}
	return fmt.Sprintf("Send it as %d batches, the first ending at item %s", batches, groupThousands(firstEnd))
}

// describeBatchItem is the HQL form of an item for a message, as
// forwardLostAdvice writes one: db.User_42["42"].Feed.push(…). A payload that
// does not decode is named by its opcode.
func describeBatchItem(op transport.Opcode, payload []byte) string {
	if op == transport.OpUpdate {
		var update UpdateRequest
		if err := bson.Unmarshal(payload, &update); err != nil {
			return op.String()
		}
		return fmt.Sprintf("db.%s.update(%q, …)", displayBucket(update.Bucket), update.Key)
	}
	var field FieldPeekRequest
	if err := bson.Unmarshal(payload, &field); err != nil {
		return op.String()
	}
	target := fieldTargetOf(field.FieldRequest)
	switch op {
	case transport.OpFieldPush:
		return target + ".push(…)"
	case transport.OpFieldPut:
		return fmt.Sprintf("%s.put(%q, …)", target, field.Element)
	case transport.OpFieldDelete:
		return fmt.Sprintf("%s.delete(%q)", target, field.Element)
	case transport.OpFieldGet:
		return fmt.Sprintf("%s.get(%q)", target, field.Element)
	case transport.OpFieldLen:
		return target + ".len()"
	case transport.OpFieldPeek:
		if field.Limit > 1 {
			return fmt.Sprintf("%s.peek({limit: %d})", target, field.Limit)
		}
		return target + ".peek()"
	case transport.OpFieldPop:
		return target + ".pop()"
	}
	return op.String()
}

// fieldTarget is the HQL name of the store field a payload addresses.
func fieldTarget(payload []byte) string {
	var field FieldRequest
	if err := bson.Unmarshal(payload, &field); err != nil {
		return "the field"
	}
	return fieldTargetOf(field)
}

// fieldTargetOf is db.Bucket["key"].Field, with an L3 path as its L2 field and
// element: db.Thread["t1"].Comments[0].Flags.
func fieldTargetOf(field FieldRequest) string {
	target := fmt.Sprintf("db.%s[%q]", displayBucket(field.Bucket), field.Key)
	if len(field.Path) == 2 {
		parent, _ := field.Path[0].(string)
		target += "." + parent + pathElementForm(field.Path[1])
	}
	return target + "." + field.Field
}

// pathElementForm is an L3 path's element as HQL indexes it: [0] for a
// position, ["k"] for an element key.
func pathElementForm(element any) string {
	switch v := element.(type) {
	case string:
		return fmt.Sprintf("[%q]", v)
	case int32, int64, int:
		return fmt.Sprintf("[%d]", v)
	}
	return fmt.Sprintf("[%v]", element)
}

// useDatabaseHint is " (run 'use <database>' first)", the note a message with
// an HQL form adds, as forwardLostAdvice does; empty without a database.
func useDatabaseHint(database string) string {
	if database == "" {
		return ""
	}
	return fmt.Sprintf(" (run 'use %s' first)", database)
}

// bsonTypeName names a BSON type for a message: "a double", not "0x01".
func bsonTypeName(t bsontype.Type) string {
	switch t {
	case bson.TypeDouble:
		return "double"
	case bson.TypeString:
		return "string"
	case bson.TypeEmbeddedDocument:
		return "document"
	case bson.TypeArray:
		return "array"
	case bson.TypeBinary:
		return "binary"
	case bson.TypeBoolean:
		return "boolean"
	case bson.TypeNull:
		return "null"
	case bson.TypeInt64:
		return "64-bit integer"
	case bson.TypeInt32:
		return "32-bit integer"
	case bson.TypeDecimal128:
		return "decimal"
	}
	return t.String()
}

// articled puts "a" or "an" before a type name.
func articled(name string) string {
	if strings.ContainsAny(name[:1], "aeiou") {
		return "an " + name
	}
	return "a " + name
}

// CheckBatch applies the server's decode rules and limits to a BATCH request
// a client is about to send, with the server's own refusal texts: the CLI
// refuses a batch it would not carry before anything is sent
// (batch-request spec §8.3). nil means the server's decode would accept it;
// the server's door still runs every item's gates.
func CheckBatch(req BatchRequest) error {
	payload, err := bson.Marshal(req)
	if err != nil {
		return fmt.Errorf("encoding the batch: %w", err)
	}
	_, err = decodeBatch(payload)
	return err
}

// DescribeBatchItem is the HQL form of a batch item, as the server's messages
// write it: db.User_42["42"].Feed.push(…), db.User_7.update("7", …).
func DescribeBatchItem(op transport.Opcode, payload []byte) string {
	return describeBatchItem(op, payload)
}
