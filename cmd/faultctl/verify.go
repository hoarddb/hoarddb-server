package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The five invariants below are the definitions of cluster/harness/ledger.go (slice H1 of
// the ring fault plan) with the same names, applied to CSVs instead of a
// live Cluster. They are copied, not imported: this command must not link the harness. If the
// ledger's definition changes, change it here in the same commit.
//
//   - no-acked-write-lost (I1): every write acknowledged "applied" is readable at the end of
//     the run, unless its id is in the declared loss set (--lost), which is printed.
//   - reads-monotone-per-client (I2): for one (client, id) a read never goes backwards: once
//     a seq has been served, a lower seq or NOT_FOUND is a regression.
//   - one-tail-per-pair (I3): one id is acked by one owner per committed epoch; two served_by
//     values for the same (id, pos_epoch) mean two tails.
//   - ack-vocabulary-sound (I4): applied needs copies >= majority(RF); applied_degraded needs
//     1 <= copies < majority(RF); unknown never sits beside an applied of the same id.
//   - refusals-are-named (I5): every refusal from a running node is one of namedRefusals; a
//     bare deadline (TIMEOUT) or lost connection (CONN) is expected only inside a declared
//     down window, and more than MaxRefused refusals in a row from a live node is a bug.
var namedRefusals = map[string]bool{
	"LEASE_LAPSED": true, "NO_QUORUM": true, "NOT_OWNER": true, "DOCUMENT_EXISTS": true, "STATE_MOVED": true,
	"RF_UNSUPPORTED": true, "INVALID_ARGUMENT": true, "OWNER_DOWN": true, "UNAVAILABLE": true,
}

// verdict is one invariant's judgement; Excused lists the ids a declared loss set let pass.
type verdict struct {
	Name    string
	OK      bool
	Detail  string
	Excused []string
}

// window is a span of wall-clock time, Unix nanoseconds, in which the node was down on purpose.
type window struct{ From, To int64 }

type verifyOptions struct {
	Lost       map[string]string // id -> reason; the run's declared loss set
	RF         int               // replication factor the run's database was created with
	MaxRefused int               // I5: consecutive refusals a live node may give; 0 means no bound
	Down       []window
}

func (o verifyOptions) isDown(atNs int64) bool {
	for _, span := range o.Down {
		if atNs >= span.From && atNs <= span.To {
			return true
		}
	}
	return false
}

// verify judges the write CSV (acks) and the read CSV (obs) and returns one verdict per invariant.
func verify(acks, obs []attempt, opts verifyOptions) []verdict {
	return []verdict{
		checkNoAckedWriteLost(acks, obs, opts),
		checkReadsMonotone(obs),
		checkOneTailPerPair(acks),
		checkAckVocabulary(acks, opts.RF),
		checkRefusalsAreNamed(acks, obs, opts),
	}
}

// isAcked is true for every answer the ledger records as an acknowledgement: applied,
// applied_degraded and unknown. A refusal has an empty outcome and promised nothing.
func isAcked(row attempt) bool {
	return row.Outcome == "applied" || row.Outcome == "applied_degraded" || row.Outcome == "unknown"
}

// checkNoAckedWriteLost is I1 for the CSV shape. faultctl write gives every id to exactly one
// client (ids are "<client>-<n>"), so the newest acknowledged seq of an id is the seq the last
// conclusive read must hold or exceed. An id written by two clients is out of scope here: the
// read CSV names the reader, not the writer, so the per-writer comparison of the ledger
// (cluster/harness/ledger_check.go judgeLoss) cannot be made from these files.
func checkNoAckedWriteLost(acks, obs []attempt, opts verifyOptions) verdict {
	last := map[string]attempt{} // the latest conclusive read of each id
	for _, row := range obs {
		if row.ErrCode == "" && row.AtNs >= last[row.ID].AtNs {
			last[row.ID] = row
		}
	}
	newest := map[string]attempt{} // the newest acknowledged write of each id
	checked := 0
	for _, ack := range acks {
		if !isAcked(ack) {
			continue
		}
		checked++
		if prev, seen := newest[ack.ID]; !seen || ack.Seq >= prev.Seq {
			newest[ack.ID] = ack
		}
	}
	ids := make([]string, 0, len(newest))
	for id := range newest {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := verdict{Name: "no-acked-write-lost", OK: true}
	var gone []string
	for _, id := range ids {
		ack := newest[id]
		seen, read := last[id]
		var why string
		switch {
		case !read:
			why = "never read back"
		case seen.Seq < 0:
			why = "read back as not found"
		case seen.Seq < ack.Seq:
			why = fmt.Sprintf("read back at seq %d, below the acked seq %d", seen.Seq, ack.Seq)
		default:
			continue
		}
		if reason, declared := opts.Lost[id]; declared {
			result.Excused = append(result.Excused, fmt.Sprintf("%s: acked %s/%d via %s at %s; excused: %s",
				id, ack.Outcome, ack.Copies, ack.Via, time.Unix(0, ack.AtNs).UTC().Format(time.RFC3339Nano), reason))
			continue
		}
		gone = append(gone, id+" ("+why+")")
	}
	result.Detail = fmt.Sprintf("%d acked writes checked, %d excused", checked, len(result.Excused))
	if len(gone) > 0 {
		result.OK = false
		result.Detail += "; missing: " + strings.Join(gone, ", ")
	}
	if singleNode(acks) {
		result.Detail += " (one node: no failover to lose a write to, so this is the durability check)"
	}
	return result
}

func checkReadsMonotone(obs []attempt) verdict {
	ordered := append([]attempt(nil), obs...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].AtNs < ordered[j].AtNs })
	type stream struct{ client, id string }
	highest := map[stream]attempt{}
	var regressions []string
	for _, row := range ordered {
		if row.ErrCode != "" {
			continue
		}
		key := stream{row.Client, row.ID}
		if before, seen := highest[key]; seen && row.Seq < before.Seq {
			regressions = append(regressions, fmt.Sprintf("%s/%s read seq %d at %d after seq %d at %d",
				row.Client, row.ID, row.Seq, row.AtNs, before.Seq, before.AtNs))
			continue
		}
		highest[key] = row
	}
	result := verdict{Name: "reads-monotone-per-client", OK: len(regressions) == 0, Detail: fmt.Sprintf("%d reads checked", len(ordered))}
	if !result.OK {
		result.Detail += "; regressions: " + strings.Join(regressions, "; ")
	}
	return result
}

func checkOneTailPerPair(acks []attempt) verdict {
	type slot struct {
		id    string
		epoch uint64
	}
	owners := map[slot]map[string]bool{}
	for _, ack := range acks {
		if !isAcked(ack) || ack.ServedBy == "" {
			continue
		}
		key := slot{ack.ID, ack.PosEpoch}
		if owners[key] == nil {
			owners[key] = map[string]bool{}
		}
		owners[key][ack.ServedBy] = true
	}
	var doubled []string
	for key, set := range owners {
		if len(set) > 1 {
			doubled = append(doubled, fmt.Sprintf("%s at epoch %d acked by %d owners", key.id, key.epoch, len(set)))
		}
	}
	sort.Strings(doubled)
	result := verdict{Name: "one-tail-per-pair", OK: len(doubled) == 0, Detail: fmt.Sprintf("%d (id, epoch) slots checked", len(owners))}
	if !result.OK {
		result.Detail += "; " + strings.Join(doubled, "; ")
	}
	if singleNode(acks) {
		result.Detail += " (trivial at N = 1: one node cannot have two tails)"
	}
	return result
}

// checkAckVocabulary is I4. The CSV has no per-row replication factor, so replicationFactor is
// the run's one --rf for every row; a run that changes RF between rows needs one verify per RF.
func checkAckVocabulary(acks []attempt, replicationFactor int) verdict {
	majority := replicationFactor/2 + 1
	type request struct {
		client, id string
		seq        int64
	}
	applied := map[request]bool{}
	unknown := map[request]bool{}
	var breaches []string
	for _, ack := range acks {
		req := request{ack.Client, ack.ID, ack.Seq}
		switch ack.Outcome {
		case "":
			if ack.ErrCode == "" {
				breaches = append(breaches, fmt.Sprintf("%s seq %d has neither an outcome nor an err_code: the request has no answer", ack.ID, ack.Seq))
			}
		case "applied":
			applied[req] = true
			if ack.Copies < majority {
				breaches = append(breaches, fmt.Sprintf("%s applied with %d copies, majority of RF %d is %d", ack.ID, ack.Copies, replicationFactor, majority))
			}
		case "applied_degraded":
			if ack.Copies < 1 || ack.Copies >= majority {
				breaches = append(breaches, fmt.Sprintf("%s applied_degraded with %d copies, want 1..%d for RF %d", ack.ID, ack.Copies, majority-1, replicationFactor))
			}
		case "unknown":
			unknown[req] = true
		default:
			breaches = append(breaches, fmt.Sprintf("%s has outcome %q, not in the vocabulary", ack.ID, ack.Outcome))
		}
	}
	for req := range unknown {
		if applied[req] {
			breaches = append(breaches, fmt.Sprintf("request %s seq %d of %q was answered both unknown and applied", req.id, req.seq, req.client))
		}
	}
	sort.Strings(breaches)
	result := verdict{Name: "ack-vocabulary-sound", OK: len(breaches) == 0, Detail: fmt.Sprintf("RF %d, majority %d", replicationFactor, majority)}
	if !result.OK {
		result.Detail += "; " + strings.Join(breaches, "; ")
	}
	return result
}

func checkRefusalsAreNamed(acks, obs []attempt, opts verifyOptions) verdict {
	all := append(append([]attempt(nil), acks...), obs...)
	sort.SliceStable(all, func(i, j int) bool { return all[i].AtNs < all[j].AtNs })
	var unnamed []string
	counts := map[string]int{}
	run := map[string]int{}
	overlong := map[string]bool{}
	for _, row := range all {
		if row.ErrCode == "" {
			run[row.Client] = 0
			continue
		}
		// A killed node explains a deadline or a dropped connection and nothing else: an
		// unnamed code from a node that was down is still a bug.
		if (row.ErrCode == "TIMEOUT" || row.ErrCode == "CONN") && opts.isDown(row.AtNs) {
			continue
		}
		counts[row.ErrCode]++
		if !namedRefusals[row.ErrCode] {
			unnamed = append(unnamed, fmt.Sprintf("%s/%s code %s at %d", row.Client, row.ID, row.ErrCode, row.AtNs))
			continue
		}
		run[row.Client]++
		if opts.MaxRefused > 0 && run[row.Client] > opts.MaxRefused {
			overlong[row.Client] = true
		}
	}
	summary := make([]string, 0, len(counts))
	for code, count := range counts {
		summary = append(summary, code+"="+strconv.Itoa(count))
	}
	sort.Strings(summary)
	result := verdict{Name: "refusals-are-named", OK: len(unnamed) == 0 && len(overlong) == 0, Detail: "refusals (TIMEOUT and CONN inside down windows left out): " + strings.Join(summary, " ")}
	if len(unnamed) > 0 {
		result.Detail += "; not a named refusal from a live node: " + strings.Join(unnamed, "; ")
	}
	for client := range overlong {
		result.Detail += fmt.Sprintf("; client %s was refused more than %d times in a row", client, opts.MaxRefused)
	}
	return result
}

// singleNode reports whether every row went to one address, which is what makes I3 trivial.
func singleNode(rows []attempt) bool {
	seen := map[string]bool{}
	for _, row := range rows {
		seen[row.Via] = true
	}
	return len(seen) <= 1
}

// loadLost reads the declared loss set: one "id reason" per line, # comments and blank lines skipped.
func loadLost(path string) (map[string]string, error) {
	lost := map[string]string{}
	if path == "" {
		return lost, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening the loss set %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		id, reason, _ := strings.Cut(line, " ")
		if reason = strings.TrimSpace(reason); reason == "" {
			reason = "declared in " + path
		}
		lost[id] = reason
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading the loss set %s: %w", path, err)
	}
	return lost, nil
}

// parseDown reads "from_ns:to_ns,from_ns:to_ns" into windows.
func parseDown(spec string) ([]window, error) {
	var spans []window
	for _, part := range strings.Split(spec, ",") {
		if part == "" {
			continue
		}
		from, to, ok := strings.Cut(part, ":")
		start, errStart := strconv.ParseInt(from, 10, 64)
		end, errEnd := strconv.ParseInt(to, 10, 64)
		if !ok || errStart != nil || errEnd != nil || end < start {
			return nil, fmt.Errorf("down window %q is not from_ns:to_ns (Unix nanoseconds, from <= to), for example --down 1700000000000000000:1700000005000000000", part)
		}
		spans = append(spans, window{start, end})
	}
	return spans, nil
}
