package cli

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The dead verbs: an entry claimed five times without an acknowledgement is
// kept apart as dead instead of being delivered for ever. dead lists them,
// requeue puts them back in the queue with their count reset, and discard drops
// them (claim-ack-spec 12, CA-24, CA-25).

// checkDeadIDValue is the rule of a dead_id: the text dead() printed. Its form
// is the server's to check.
func checkDeadIDValue(verb, where string, raw any) error {
	if text, isText := raw.(string); !isText || text == "" {
		shown, _ := json.Marshal(raw)
		return usagef(`%s's dead_id is the text dead() returned, as in %s.%s({dead_id: "<dead_id>"}); got %s`, verb, where, verb, shown)
	}
	return nil
}

// checkAfterValue is the rule of dead's after: the dead_id of the last entry of
// the page before.
func checkAfterValue(verb, where string, raw any) error {
	if text, isText := raw.(string); !isText || text == "" {
		shown, _ := json.Marshal(raw)
		return usagef(`%s's after is the dead_id of the last entry listed, as in %s.%s({after: "<dead_id>"}); got %s`, verb, where, verb, shown)
	}
	return nil
}

// checkAllValue is the rule of requeue's and discard's all: true. all: false
// would name nothing, so it is refused rather than read as neither.
func checkAllValue(verb, where string, raw any) error {
	if flag, isBool := raw.(bool); !isBool || !flag {
		shown, _ := json.Marshal(raw)
		return usagef("%s's all is true, as in %s.%s({all: true}); got %s", verb, where, verb, shown)
	}
	return nil
}

// requireOneDeadSelector refuses a requeue or a discard that names both a
// dead_id and all, or neither.
func requireOneDeadSelector(verb, where string, options map[string]any) error {
	_, byID := options["dead_id"]
	_, everything := options["all"]
	if byID == everything {
		return usagef(`%s names one dead entry or all of them, not both and not neither: %s.%s({dead_id: "<dead_id>"}) or %s.%s({all: true})`,
			verb, where, verb, where, verb)
	}
	return nil
}

var (
	deadOptionRules = verbOptionRules{
		keys:    map[string]func(verb, where string, raw any) error{"limit": checkLimitValue, "after": checkAfterValue},
		names:   []string{"limit", "after"},
		example: `dead({limit: 50, after: "<dead_id>"})`,
	}
	requeueOptionRules = verbOptionRules{
		keys:    map[string]func(verb, where string, raw any) error{"dead_id": checkDeadIDValue, "all": checkAllValue},
		names:   []string{"dead_id", "all"},
		example: `requeue({dead_id: "<dead_id>"})`,
		require: requireOneDeadSelector,
	}
	discardOptionRules = verbOptionRules{
		keys:    map[string]func(verb, where string, raw any) error{"dead_id": checkDeadIDValue, "all": checkAllValue},
		names:   []string{"dead_id", "all"},
		example: `discard({dead_id: "<dead_id>"})`,
		require: requireOneDeadSelector,
	}
)

// deadBanner is the line that tells a developer a bucket holds dead entries and
// the three commands that deal with them. It is the server's wording
// (server/dead_letter.go:deadBanner); TestDeadBannerTextMatchesTheServers fails
// if the two drift.
func deadBanner(bucketName string, dead int64) string {
	noun := "entries"
	if dead == 1 {
		noun = "entry"
	}
	bucketName = terminalSafe(bucketName)
	return fmt.Sprintf("db.%s has %d dead %s (claimed %d times without an acknowledgement).\n"+
		"  Read them:     db.%s.dead()\n  Retry them:    db.%s.requeue({all: true})\n  Drop them:     db.%s.discard({all: true})",
		bucketName, dead, noun, store.MaxDeliveries, bucketName, bucketName, bucketName)
}

// handleDead lists a page of a bucket's dead entries, oldest first, and says
// how to read the next page.
func (h *CommandHandler) handleDead(dbName, bucketName string, args []string) error {
	where := "db." + bucketName
	options, err := readVerbOptions("dead", where, args, verbOptions["dead"])
	if err != nil {
		return err
	}
	req := &server.DeadListRequest{Database: dbName, Bucket: bucketName}
	if raw, given := options["limit"]; given {
		req.Limit = int(min(raw.(float64), math.MaxInt32))
	}
	if raw, given := options["after"]; given {
		req.After = raw.(string)
	}

	var resp server.DeadListResponse
	if err := h.send(transport.OpDeadList, req, &resp); err != nil {
		return fmt.Errorf("dead failed: %w", err)
	}
	// A list with entries carries the dead-count banner as its message. A list
	// with none and a message is a refusal (the bucket's type has no dead
	// entries, or the read failed), never an empty list.
	if !resp.Found && resp.Message != "" {
		return &RefusedError{Message: resp.Message}
	}
	for _, item := range resp.Items {
		h.printf("%s\n", h.deadLine(item))
	}
	switch {
	case resp.Next != "":
		h.printf("more: %s\n", deadStatement(where, req.Limit, resp.Next))
	case len(resp.Items) == 0 && req.After != "":
		h.printf("(no dead entries after %q)\n", req.After)
	case len(resp.Items) == 0:
		h.printf("(no dead entries)\n")
	}
	return nil
}

// deadStatement is the dead() statement that reads the page after next, with
// the limit the user chose when they chose one.
func deadStatement(where string, limit int, next string) string {
	if limit > 0 {
		return fmt.Sprintf("%s.dead({limit: %d, after: %q})", where, limit, next)
	}
	return fmt.Sprintf("%s.dead({after: %q})", where, next)
}

// deadLine is one dead entry as a line: the handle requeue and discard take,
// how often it was delivered, why it died and when, and the entry itself.
func (h *CommandHandler) deadLine(item server.DeadItem) string {
	return fmt.Sprintf(`{"dead_id": %s, "deliveries": %d, "reason": %s, "dead_at": %s, "data": %s}`,
		jsonText(item.DeadID), item.Deliveries, jsonText(item.Reason), jsonText(item.DeadAt.UTC().Format(time.RFC3339)), h.doc(item.Data))
}

// jsonText quotes a string as JSON.
func jsonText(text string) string {
	quoted, _ := json.Marshal(text)
	return string(quoted)
}

// handleRequeue puts dead entries back in the queue with their count reset.
func (h *CommandHandler) handleRequeue(dbName, bucketName string, args []string) error {
	return h.handleDeadMove("requeue", "requeued", transport.OpDeadRequeue, dbName, bucketName, args)
}

// handleDiscard drops dead entries.
func (h *CommandHandler) handleDiscard(dbName, bucketName string, args []string) error {
	return h.handleDeadMove("discard", "discarded", transport.OpDeadDiscard, dbName, bucketName, args)
}

// handleDeadMove is requeue and discard. One request considers at most 100
// entries, so the all form repeats. It must not repeat just because left is
// above zero: left counts the entries the bucket refuses by name, which stay
// where they are however often they are asked for, and a loop on left would
// never end (claim-ack D3 and D4 verification). A request whose 100 were all
// refusals moved nothing yet left movable entries behind them, so the next
// request starts after the last refused dead_id (BUG-0637). It stops when a
// request moved nothing and either considered fewer than 100 (the list ended)
// or named no refusal it had not already seen (a server that ignores after),
// and always prints what was moved and what is left.
func (h *CommandHandler) handleDeadMove(verb, past string, opcode transport.Opcode, dbName, bucketName string, args []string) error {
	options, err := readVerbOptions(verb, "db."+bucketName, args, verbOptions[verb])
	if err != nil {
		return err
	}
	req := &server.DeadMoveRequest{Database: dbName, Bucket: bucketName}
	if raw, byID := options["dead_id"]; byID {
		req.DeadID = raw.(string)
	} else {
		req.All = true
	}

	var moved int
	var left int64
	var refused []server.DeadRefusal
	seen := map[string]bool{}
	for {
		var resp server.DeadMoveResponse
		if err := h.send(opcode, req, &resp); err != nil {
			return fmt.Errorf("%s failed: %w", verb, err)
		}
		if !resp.Success && len(resp.Refused) == 0 {
			return refusedMove(verb, resp.Message)
		}
		moved += resp.Done
		left = resp.Left
		unseen := 0
		for _, entry := range resp.Refused {
			if !seen[entry.DeadID] {
				seen[entry.DeadID] = true
				refused = append(refused, entry)
				unseen++
			}
		}
		if !req.All || resp.Left == 0 {
			break
		}
		if len(resp.Refused) > 0 {
			req.After = resp.Refused[len(resp.Refused)-1].DeadID
		}
		if resp.Done == 0 && (len(resp.Refused) < store.MaxDeadMoveLimit || unseen == 0) {
			break
		}
	}

	if req.All || moved > 0 || len(refused) == 0 {
		line := fmt.Sprintf("%s %d", past, moved)
		if req.All && left > 0 {
			line += fmt.Sprintf(", left %d", left)
		}
		h.printf("%s\n", line)
	}
	if len(refused) == 0 {
		return nil
	}
	messages := make([]string, len(refused))
	for index, entry := range refused {
		messages[index] = entry.Message
	}
	return &RefusedError{Message: strings.Join(messages, "\n")}
}

// refusedMove is the error for an answer that moved nothing and named no entry:
// the bucket's type has no dead entries, or the store failed.
func refusedMove(verb, message string) error {
	if message == "" {
		return refusedf("the server refused the %s without saying why; list the dead entries with dead() and try again", verb)
	}
	return &RefusedError{Message: message}
}

// printDeadBanners prints the banner of every listed bucket that holds dead
// entries. A bucket with none prints nothing, so an idle listing is unchanged.
func (h *CommandHandler) printDeadBanners(buckets []server.BucketInfo) {
	for _, bucket := range buckets {
		if bucket.Dead > 0 {
			h.printf("\n%s\n", deadBanner(bucket.Name, bucket.Dead))
		}
	}
}

// printDeadBannerOf prints the banner for one bucket after describe bucket. The
// dead count is not in BUCKET_STATS, so it asks BUCKET_INFO, which costs the
// same for a bucket of any size, and only for the types that have dead
// entries. The banner only decorates, so a server older than BUCKET_INFO loses
// the banner quietly and a failed lookup says so on the chatter stream, and
// neither takes the description away (readDeclaration's reasoning).
func (h *CommandHandler) printDeadBannerOf(dbName, bucketName string, bucketType server.BucketType) {
	switch bucketType {
	case server.BucketTypeFIFO, server.BucketTypeLIFO, server.BucketTypeHeap:
	default:
		return
	}
	info, err := h.fetchInfo(dbName, bucketName)
	switch {
	case err != nil && isUnsupportedOpcode(err):
		return
	case err != nil:
		h.notef("could not read the dead-entry count of %s: %v\n", bucketName, err)
		return
	case !info.Found:
		return
	}
	h.printDeadBanners([]server.BucketInfo{info.Bucket})
}
