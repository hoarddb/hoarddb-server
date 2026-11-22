package cli

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The lease verbs: claim takes entries under a lease, ack finishes them,
// release gives them back at once and extend moves the lease's end out
// (claim-ack-spec 12). Each reads one option document through the shared
// strict reader, and the CLI refuses what it can in the user's own words
// before anything is sent.

// leaseUnits are the units a lease is written in, in milliseconds.
var leaseUnits = map[string]int64{"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}

// leaseSpelling matches a whole number and one unit: "30s", "1500ms", "1h".
var leaseSpelling = regexp.MustCompile(`^([0-9]+)(ms|s|m|h)$`)

// leaseExample is a statement that names a lease, for the messages about one:
// claim's and extend's examples differ because extend also needs a lease_id.
func leaseExample(verb, where string) string {
	if verb == "extend" {
		return fmt.Sprintf(`%s.extend({lease_id: "<lease_id>", lease: "60s"})`, where)
	}
	return fmt.Sprintf(`%s.%s({lease: "30s"})`, where, verb)
}

// formatLease writes a lease length the way a user would type it: the largest
// unit that divides it, so 3,600,000 is "1h" and 1,500 is "1500ms".
func formatLease(milliseconds int64) string {
	switch {
	case milliseconds%leaseUnits["h"] == 0:
		return fmt.Sprintf("%dh", milliseconds/leaseUnits["h"])
	case milliseconds%leaseUnits["m"] == 0:
		return fmt.Sprintf("%dm", milliseconds/leaseUnits["m"])
	case milliseconds%leaseUnits["s"] == 0:
		return fmt.Sprintf("%ds", milliseconds/leaseUnits["s"])
	}
	return fmt.Sprintf("%dms", milliseconds)
}

// parseLease reads a lease option in milliseconds. A bare number is refused
// because "30" could be seconds or milliseconds and a guess here either holds
// work for 8 hours or returns it after 30 ms. A lease out of bounds is refused
// in the user's spelling, with the bounds the store defines, before sending.
func parseLease(verb, where string, raw any) (int64, error) {
	example := leaseExample(verb, where)
	shown, _ := json.Marshal(raw)
	text, isText := raw.(string)
	if number, isNumber := raw.(float64); isNumber {
		return 0, usagef("%s's lease needs a unit, as in %s; got %s", verb, example, strconv.FormatFloat(number, 'f', -1, 64))
	}
	if !isText {
		return 0, usagef("%s's lease is a duration with a unit (ms, s, m or h), as in %s; got %s", verb, example, shown)
	}
	parts := leaseSpelling.FindStringSubmatch(strings.TrimSpace(text))
	if parts == nil {
		if _, err := strconv.ParseUint(strings.TrimSpace(text), 10, 64); err == nil {
			return 0, usagef("%s's lease needs a unit, as in %s; got %s", verb, example, shown)
		}
		return 0, usagef("%s's lease is a duration with a unit (ms, s, m or h), as in %s; got %s", verb, example, shown)
	}
	// The division keeps count*unit from overflowing on a number of 19 digits;
	// a count that does not even parse is too long as well.
	count, err := strconv.ParseInt(parts[1], 10, 64)
	unit := leaseUnits[parts[2]]
	if err != nil || count > store.MaxLeaseMs/unit {
		return 0, leaseTooLong(verb, where, text)
	}
	milliseconds := count * unit
	if milliseconds < store.MinLeaseMs {
		return 0, usagef("%s's lease %q is below the minimum of %q: use %q or more. Nothing was sent",
			verb, text, formatLease(store.MinLeaseMs), formatLease(store.MinLeaseMs))
	}
	return milliseconds, nil
}

// leaseTooLong is the refusal of a lease above the maximum, with the way to
// work longer than one lease: claim or extend with the maximum, and extend
// again while the work runs.
func leaseTooLong(verb, where, text string) error {
	maximum := formatLease(store.MaxLeaseMs)
	if verb == "extend" {
		return usagef("extend's lease %q is above the maximum of %q: extend with lease %q and extend again while the work runs. Nothing was sent",
			text, maximum, maximum)
	}
	return usagef(`%s's lease %q is above the maximum of %q: %s with lease %q and call %s.extend({lease_id: "<lease_id>", lease: %q}) while the work runs. Nothing was sent`,
		verb, text, maximum, verb, maximum, where, maximum)
}

// checkLeaseValue is the rule of a lease key.
func checkLeaseValue(verb, where string, raw any) error {
	_, err := parseLease(verb, where, raw)
	return err
}

// checkLeaseIDValue is the rule of a lease_id: the text claim printed. Its
// form is the server's to check, and the server's message names the fix.
func checkLeaseIDValue(verb, where string, raw any) error {
	if text, isText := raw.(string); !isText || text == "" {
		shown, _ := json.Marshal(raw)
		return usagef(`%s's lease_id is the text claim returned, as in %s.%s({lease_id: "<lease_id>"}); got %s`, verb, where, verb, shown)
	}
	return nil
}

// checkItemIDsValue is the rule of ack's item_ids: a list of the handles claim
// returned. An id that is a document's _id_ is refused by the server, whose
// message says what item ids are.
func checkItemIDsValue(verb, where string, raw any) error {
	example := fmt.Sprintf(`%s.%s({lease_id: "<lease_id>", item_ids: ["<item_id>"]})`, where, verb)
	listed, isList := raw.([]any)
	if !isList {
		shown, _ := json.Marshal(raw)
		return usagef("%s's item_ids is a list of the item_ids claim returned, as in %s; got %s", verb, example, shown)
	}
	for _, element := range listed {
		if _, isText := element.(string); !isText {
			shown, _ := json.Marshal(element)
			return usagef("%s's item_ids is a list of the item_ids claim returned, as in %s; got %s in the list", verb, example, shown)
		}
	}
	switch {
	case len(listed) == 0:
		return usagef("%s's item_ids is empty: name the entries to acknowledge, or leave item_ids out to acknowledge the whole lease", verb)
	case len(listed) > store.MaxItemIDsPerAck:
		return usagef("%s names %d item_ids, and takes at most %d at a time: acknowledge them in lists of %d, or leave item_ids out to acknowledge the whole lease",
			verb, len(listed), store.MaxItemIDsPerAck, store.MaxItemIDsPerAck)
	}
	return nil
}

// requireLeaseID refuses a lease verb that names no lease. It says where the
// id is: claim prints it on the line that starts with lease_id.
func requireLeaseID(verb, where string, options map[string]any) error {
	if _, named := options["lease_id"]; !named {
		return usagef(`%s needs the lease_id claim returned, as in %s.%s({lease_id: "<lease_id>"}); claim prints it on the line that starts lease_id`,
			verb, where, verb)
	}
	return nil
}

// requireLeaseAndID is extend's: the lease to move and how long to move it.
func requireLeaseAndID(verb, where string, options map[string]any) error {
	if err := requireLeaseID(verb, where, options); err != nil {
		return err
	}
	if _, named := options["lease"]; !named {
		return usagef("extend needs a lease, as in %s", leaseExample(verb, where))
	}
	return nil
}

var (
	claimOptionRules = verbOptionRules{
		keys:    map[string]func(verb, where string, raw any) error{"limit": checkLimitValue, "lease": checkLeaseValue},
		names:   []string{"limit", "lease"},
		example: `claim({limit: 10, lease: "30s"})`,
	}
	ackOptionRules = verbOptionRules{
		keys:    map[string]func(verb, where string, raw any) error{"lease_id": checkLeaseIDValue, "item_ids": checkItemIDsValue},
		names:   []string{"lease_id", "item_ids"},
		example: `ack({lease_id: "<lease_id>", item_ids: ["<item_id>"]})`,
		require: requireLeaseID,
	}
	releaseOptionRules = verbOptionRules{
		keys:    map[string]func(verb, where string, raw any) error{"lease_id": checkLeaseIDValue},
		names:   []string{"lease_id"},
		example: `release({lease_id: "<lease_id>"})`,
		require: requireLeaseID,
	}
	extendOptionRules = verbOptionRules{
		keys:    map[string]func(verb, where string, raw any) error{"lease_id": checkLeaseIDValue, "lease": checkLeaseValue},
		names:   []string{"lease_id", "lease"},
		example: `extend({lease_id: "<lease_id>", lease: "60s"})`,
		require: requireLeaseAndID,
	}
)

// handleClaim takes entries under a lease and prints each, then the line that
// carries what ack needs. The options are read, and refused if they are not
// understood, before anything is sent.
func (h *CommandHandler) handleClaim(dbName, bucketName string, args []string) error {
	where := "db." + bucketName
	options, err := readVerbOptions("claim", where, args, verbOptions["claim"])
	if err != nil {
		return err
	}
	req := &server.ClaimRequest{Database: dbName, Bucket: bucketName}
	if raw, given := options["limit"]; given {
		req.Limit = int(min(raw.(float64), math.MaxInt32))
	}
	if raw, given := options["lease"]; given {
		if req.LeaseMs, err = parseLease("claim", where, raw); err != nil {
			return err
		}
	}

	var resp server.ClaimResponse
	if err := h.send(transport.OpQueueClaim, req, &resp); err != nil {
		return fmt.Errorf("claim failed: %w", err)
	}
	if !resp.Found {
		return h.printMiss(resp.Message, "EMPTY  "+resp.Message+claimedNote(bucketName, resp.Pending, resp.Dead, resp.Returned))
	}
	for _, entry := range resp.Items {
		h.printf("%s\n", h.doc(entry))
	}
	h.printf("%s\n", leaseLine(resp))
	return nil
}

// leaseLine is the line after the entries: the lease, how long it lasts, and
// the handles ack names an entry by. Deliveries are shown only when some
// entry was delivered before, which the server signals by sending them.
func leaseLine(resp server.ClaimResponse) string {
	line := fmt.Sprintf("lease_id %s for %s; item_ids %s", terminalSafe(resp.LeaseID), formatLease(resp.LeaseMs), quotedList(resp.ItemIDs))
	if len(resp.Deliveries) > 0 {
		counts := make([]string, len(resp.Deliveries))
		for index, delivery := range resp.Deliveries {
			counts[index] = strconv.Itoa(int(delivery))
		}
		line += "; deliveries [" + strings.Join(counts, ", ") + "]"
	}
	return line
}

// quotedList writes strings as a list the option reader takes back:
// ["a", "b"].
func quotedList(items []string) string {
	quoted := make([]string, len(items))
	for index, item := range items {
		quoted[index] = strconv.Quote(item)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// claimedNote is the parenthesis after an empty answer from a bucket that holds
// claimed or dead entries, or just returned expired ones. The words before it
// stay the bare absence text the CLI sorts replies by (cli/notfound.go:absentMessages).
func claimedNote(bucketName string, pending, dead, returned int64) string {
	var note string
	switch {
	case pending == 1:
		note = fmt.Sprintf(" (1 entry is claimed; db.%s.len() does not count it)", bucketName)
	case pending > 1:
		note = fmt.Sprintf(" (%d entries are claimed; db.%s.len() does not count them)", pending, bucketName)
	}
	switch {
	case dead == 1:
		note += fmt.Sprintf(" (1 entry is dead-lettered; see db.%s.dead())", bucketName)
	case dead > 1:
		note += fmt.Sprintf(" (%d entries are dead-lettered; see db.%s.dead())", dead, bucketName)
	}
	if returned > 0 {
		note += fmt.Sprintf(" (returned %d expired entries to the queue; claim again)", returned)
	}
	return note
}

// claimedBeside is what len() and peek say of the entries a lease holds and of
// the dead entries len() does not count: they are not in the number, and some
// may be due back. The dead hint is len()'s alone (claim-ack-spec §8.1).
func claimedBeside(bucketName string, pending, dead int64, expired bool) string {
	bucketName = terminalSafe(bucketName)
	var parts []string
	switch {
	case pending == 1:
		parts = append(parts, "plus 1 claimed")
	case pending > 1:
		parts = append(parts, fmt.Sprintf("plus %d claimed", pending))
	}
	if pending > 0 && expired {
		parts[len(parts)-1] += ", some of them expired; claim or pop returns them first"
	}
	switch {
	case dead == 1:
		parts = append(parts, fmt.Sprintf("1 entry is dead-lettered; see db.%s.dead()", bucketName))
	case dead > 1:
		parts = append(parts, fmt.Sprintf("%d entries are dead-lettered; see db.%s.dead()", dead, bucketName))
	}
	if len(parts) == 0 {
		return ""
	}
	return "(" + strings.Join(parts, "; ") + ")"
}

// handleAck acknowledges a lease's entries, all of them or the item_ids named.
func (h *CommandHandler) handleAck(dbName, bucketName string, args []string) error {
	options, err := readVerbOptions("ack", "db."+bucketName, args, verbOptions["ack"])
	if err != nil {
		return err
	}
	req := &server.LeaseRequest{Database: dbName, Bucket: bucketName, LeaseID: options["lease_id"].(string)}
	if listed, given := options["item_ids"].([]any); given {
		for _, element := range listed {
			req.ItemIDs = append(req.ItemIDs, element.(string))
		}
	}
	resp, err := h.sendLease("ack", transport.OpQueueAck, req)
	if err != nil {
		return err
	}
	line := fmt.Sprintf("acked %d", resp.Acked)
	if resp.Missing > 0 {
		line += fmt.Sprintf(", missing %d", resp.Missing)
	}
	if resp.Remaining > 0 {
		line += fmt.Sprintf(", remaining %d", resp.Remaining)
	}
	h.printf("%s\n", line)
	return nil
}

// handleRelease gives a lease's entries back to the queue at once.
func (h *CommandHandler) handleRelease(dbName, bucketName string, args []string) error {
	options, err := readVerbOptions("release", "db."+bucketName, args, verbOptions["release"])
	if err != nil {
		return err
	}
	req := &server.LeaseRequest{Database: dbName, Bucket: bucketName, LeaseID: options["lease_id"].(string)}
	resp, err := h.sendLease("release", transport.OpQueueRelease, req)
	if err != nil {
		return err
	}
	h.printf("released %d\n", resp.Released)
	return nil
}

// handleExtend moves a lease's end out so work can outlast the first lease.
func (h *CommandHandler) handleExtend(dbName, bucketName string, args []string) error {
	where := "db." + bucketName
	options, err := readVerbOptions("extend", where, args, verbOptions["extend"])
	if err != nil {
		return err
	}
	leaseMs, err := parseLease("extend", where, options["lease"])
	if err != nil {
		return err
	}
	req := &server.LeaseRequest{Database: dbName, Bucket: bucketName, LeaseID: options["lease_id"].(string), LeaseMs: leaseMs}
	resp, err := h.sendLease("extend", transport.OpQueueExtend, req)
	if err != nil {
		return err
	}
	h.printf("extended for %s (until %s, bucket clock)\n", formatLease(resp.LeaseMs), resp.LeaseDeadline.UTC().Format("2006-01-02T15:04:05Z"))
	return nil
}

// sendLease sends one ack, release or extend and turns an answer that did not
// do it into an error. An unknown or expired lease is a refusal with the
// server's message, which names the fix, so a script sees a non-zero exit and
// never a success line (spec 12).
func (h *CommandHandler) sendLease(verb string, opcode transport.Opcode, req *server.LeaseRequest) (*server.LeaseResponse, error) {
	var resp server.LeaseResponse
	if err := h.send(opcode, req, &resp); err != nil {
		return nil, fmt.Errorf("%s failed: %w", verb, err)
	}
	if resp.Success {
		return &resp, nil
	}
	if resp.Message == "" {
		return nil, refusedf("the server refused the %s of the lease without saying why; its state is %s. Claim again with db.%s.claim()",
			verb, resp.LeaseState, req.Bucket)
	}
	return nil, &RefusedError{Message: resp.Message}
}

// refuseOnAStoreField is the answer to a lease verb or a dead verb addressed to
// a store field: a field has no leases and no dead entries (spec 9.5, D-15),
// and its consuming verb is pop. Nothing is sent.
func refuseOnAStoreField(verb string, cmd *Cmd) error {
	address := fmt.Sprintf("db.%s[%q].%s", cmd.Bucket, cmd.DocKey, cmd.Field)
	return usagef("%s is not available on a store field: %s is consumed with %s.pop(), which removes at most once. Claim, ack, release, extend and the dead verbs work on a fifo, lifo or heap bucket",
		verb, address, address)
}
