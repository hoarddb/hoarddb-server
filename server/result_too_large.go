package server

import (
	"errors"
	"fmt"
	"strconv"
)

// ErrResultTooLarge is the sentinel under every query result refused for
// exceeding the node's result ceiling (HOARDDB_QUERY_MAX_RESULT_BYTES). It
// reaches the wire as RESULT_TOO_LARGE, code 0x0E (query-pipelining spec W-2).
var ErrResultTooLarge = errors.New("result too large")

// resultShape picks which of W-2's messages a refusal renders, because the
// fix differs: page with a smaller limit, page by the sort field, or narrow a
// distinct.
type resultShape int

const (
	// resultShapeFind is a result whose page itself outgrew the ceiling.
	resultShapeFind resultShape = iota
	// resultShapeRanking is a sorted query whose keys, held to rank
	// skip+limit documents, outgrew the ceiling.
	resultShapeRanking
	// resultShapeDistinct is a distinct whose set of values outgrew it.
	resultShapeDistinct
	// resultShapePeek is a peek whose entries outgrew the ceiling from the
	// second entry on.
	resultShapePeek
	// resultShapePop is a pop whose entries outgrew the ceiling from the second
	// entry on. It says that nothing was removed.
	resultShapePop
	// resultShapeDead is a dead list whose entries outgrew the ceiling. It
	// says that nothing was changed, which a list never does.
	resultShapeDead
)

// resultTooLargeError is a query refused by its result budget. errorFrame
// sends its own Error() text as the message and its ceiling and retained
// bytes as W-3's fields, whatever wraps it on the way.
type resultTooLargeError struct {
	// query is the display form: `find() on app.events`,
	// `distinct("session_id") on app.events`.
	query string
	shape resultShape
	// ceiling is the node's result ceiling and retained the encoded bytes
	// the query held when it stopped, both in bytes.
	ceiling  int64
	retained int64
	// documents is how many the query had kept when it stopped.
	documents int
	// limit and skip are the request's; limit 0 is no limit.
	limit int
	skip  int
	// sortField is the ranking shape's sort field.
	sortField string
	// onOwner is set when the ceiling is the bucket owner's stamped one rather
	// than this node's, so the fix names the owner's setting.
	onOwner bool
}

// Error renders W-2's message for the refusal's shape (AGENTS.md rule 3:
// the query, the ceiling, and the fix).
func (e *resultTooLargeError) Error() string {
	ceiling := formatMiB(e.ceiling)
	switch e.shape {
	case resultShapeRanking:
		return fmt.Sprintf("%s sorts for skip %d + limit %d, which needs more than %s to rank. "+
			"Page by the sort field instead of skip — {%s: {$gt: <last value you saw>}}, sort {%s: 1}, limit %d — "+
			"or add a filter that narrows the match set.",
			e.query, e.skip, e.limit, ceiling, e.sortField, e.sortField, e.limit)
	case resultShapePeek:
		// A peek has no skip and no projection, so the only fixes are a smaller
		// limit or a higher ceiling. At least one entry always fits: the first
		// is exempt from the ceiling.
		fits := max(e.documents, 1)
		return fmt.Sprintf("%s would return more than %s (stopped at %.1f MiB after %s entries). "+
			"Ask for fewer entries — peek({limit: %d}) fits — or raise HOARDDB_QUERY_MAX_RESULT_BYTES (%s).",
			e.query, ceiling, float64(e.retained)/(1<<20), groupThousands(e.documents), fits, ceiling)
	case resultShapePop:
		fits := max(e.documents, 1)
		setting := "HOARDDB_QUERY_MAX_RESULT_BYTES"
		if e.onOwner {
			setting += " on the bucket's owner"
		}
		return fmt.Sprintf("%s would return more than %s (stopped at %.1f MiB after %s entries), so nothing was removed. "+
			"Ask for fewer entries — pop({limit: %d}) fits — or raise %s (%s)",
			e.query, ceiling, float64(e.retained)/(1<<20), groupThousands(e.documents), fits, setting, ceiling)
	case resultShapeDead:
		fits := max(e.documents, 1)
		return fmt.Sprintf("%s would return more than %s (stopped at %.1f MiB after %s entries), so the list is not complete. "+
			"Ask for fewer entries — dead({limit: %d}) fits — or raise HOARDDB_QUERY_MAX_RESULT_BYTES (%s)",
			e.query, ceiling, float64(e.retained)/(1<<20), groupThousands(e.documents), fits, ceiling)
	case resultShapeDistinct:
		return fmt.Sprintf("%s found more than %s of distinct values. Add a filter to narrow it, or count instead.",
			e.query, ceiling)
	default:
		limit := "no limit"
		if e.limit > 0 {
			limit = "limit " + strconv.Itoa(e.limit)
		}
		return fmt.Sprintf("%s would return more than %s (stopped at %.1f MiB after %s documents, %s). "+
			"Ask for fewer documents per request — {limit: 1000} and page with skip — or add a projection "+
			"to return fewer fields. The ceiling is HOARDDB_QUERY_MAX_RESULT_BYTES (%s).",
			e.query, ceiling, float64(e.retained)/(1<<20), groupThousands(e.documents), limit, ceiling)
	}
}

// Unwrap makes the refusal match ErrResultTooLarge with errors.Is.
func (e *resultTooLargeError) Unwrap() error { return ErrResultTooLarge }

// formatMiB renders a byte count as whole MiB when it is one, and to one
// decimal place otherwise.
func formatMiB(bytes int64) string {
	if bytes%(1<<20) == 0 {
		return fmt.Sprintf("%d MiB", bytes>>20)
	}
	return fmt.Sprintf("%.1f MiB", float64(bytes)/(1<<20))
}

// groupThousands renders a count with a comma between each group of three digits.
func groupThousands(n int) string {
	digits := strconv.Itoa(n)
	for i := len(digits) - 3; i > 0; i -= 3 {
		digits = digits[:i] + "," + digits[i:]
	}
	return digits
}
