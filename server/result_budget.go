package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
)

// The result budget (query-pipelining spec §3.2, R-3, R-4): what one query
// may hold while it builds its answer. A query charges the encoded bytes of
// each element it keeps, at the moment it keeps it — a page document, a sort
// key, a distinct value, a range entry — and is refused with
// RESULT_TOO_LARGE once what it holds would pass the node's ceiling. Charging
// the encoded element, header included, is what makes the ceiling bound the
// response frame by derivation rather than by argument
// (TestBudgetChargesEqualEncodedSize).

// The query methods, as the metric label and the test hooks name them. They
// are constants rather than Opcode.String(), which names one of them UNKNOWN.
const (
	queryMethodScan       = "SCAN"
	queryMethodSearch     = "SEARCH"
	queryMethodBTreeRange = "BTREE_RANGE"
	queryMethodCount      = "COUNT"
	queryMethodDistinct   = "DISTINCT"
	queryMethodPeek       = "PEEK"
	queryMethodPop        = "POP"
	queryMethodClaim      = "CLAIM"
	queryMethodDead       = "DEAD"
)

// defaultMaxResultBytes is the ceiling when the server's configuration does
// not set one: a Server built without config.Load, as many tests build it.
// config.DefaultConfig carries the same 16 MiB.
const defaultMaxResultBytes = 16 << 20

// queryCheckInterval is how many entries a query walks between looks at its
// context. Until the listener cancels a connection's context early (spec
// §3.4), only shutdown cancels it, and then the answer is undeliverable anyway.
const queryCheckInterval = 256

// queryBudgetHook, when set, runs as each query returns, with what its budget
// still held and the most it held at once. Nil in production.
var queryBudgetHook atomic.Pointer[func(method string, retained, highWater int64)]

// queryVisitHook, when set, runs for each entry a query walk visits. Nil in
// production.
var queryVisitHook atomic.Pointer[func(method string)]

// resultBudget is one query's accountant. The query's own goroutine owns it,
// so its element arithmetic takes no lock; only the node-wide gauge of
// retained bytes is shared. The gauge is a lower bound on the query's memory:
// it counts the retained elements, not the encoded copies the role table of
// §3.2 charges, which is what makes it independent of the process heap.
type resultBudget struct {
	method  string
	ceiling int64
	// counts is whether a refusal moves queryResultTooLarge. A pop is applied
	// by every replica and replayed, and each applier charges the same entries
	// to reach the same decision; only the apply that answers the client counts
	// the refusal, so one request moves the metric once.
	counts    bool
	retained  int64
	highWater int64
	documents int
	// nodeUsed, when set, is the node-wide gauge of retained bytes: charge,
	// release and report keep it in step with retained.
	nodeUsed *atomic.Int64
	// refusal describes the query for W-2's message; charge fills in what
	// the query held when it stopped.
	refusal resultTooLargeError
}

// maxResultBytes is the node's result ceiling. The owner of a logged pop
// stamps this value on it, so the stamp and the budget read one function.
func (s *Server) maxResultBytes() int64 {
	if s.config != nil && s.config.Query.MaxResultBytes > 0 {
		return int64(s.config.Query.MaxResultBytes)
	}
	return defaultMaxResultBytes
}

// newResultBudget starts a query's budget at the node's ceiling.
func (s *Server) newResultBudget(method string, refusal resultTooLargeError) *resultBudget {
	return s.newResultBudgetWithCeiling(method, s.maxResultBytes(), true, refusal)
}

// newResultBudgetWithCeiling starts a budget at a ceiling the caller chose.
// Pop uses it alone: the ceiling a logged pop is judged by is the one its
// owner stamped, not this node's, so that every copy removes the same entries.
func (s *Server) newResultBudgetWithCeiling(method string, ceiling int64, counts bool, refusal resultTooLargeError) *resultBudget {
	refusal.ceiling = ceiling
	return &resultBudget{method: method, ceiling: ceiling, counts: counts, refusal: refusal, nodeUsed: &s.queryGate().retainedBytes}
}

// charge adds n bytes the query now holds, or refuses the query if that
// would take it past the ceiling.
func (b *resultBudget) charge(n int64) error {
	if b.retained+n > b.ceiling {
		if b.counts {
			queryResultTooLarge.WithLabelValues(b.method).Inc()
		}
		refusal := b.refusal
		refusal.retained = b.retained
		refusal.documents = b.documents
		return &refusal
	}
	b.retained += n
	b.highWater = max(b.highWater, b.retained)
	if b.nodeUsed != nil {
		b.nodeUsed.Add(n)
	}
	return nil
}

// chargeDocument is charge for an element of the answer itself, which W-2's
// message counts.
func (b *resultBudget) chargeDocument(n int64) error {
	if err := b.charge(n); err != nil {
		return err
	}
	b.documents++
	return nil
}

// chargeFirst adds the first entry of a peek without ever refusing it, and
// counts it as a document. A pop returns an entry up to the document limit,
// which may exceed the ceiling, so a peek that refused it would refuse what a
// pop delivers. It adds to documents as well as retained because a refusal
// that follows would otherwise say "after 0 entries".
func (b *resultBudget) chargeFirst(n int64) {
	b.retained += n
	b.highWater = max(b.highWater, b.retained)
	b.documents++
	if b.nodeUsed != nil {
		b.nodeUsed.Add(n)
	}
}

// release gives back n bytes the query no longer holds: a sort key evicted
// from the ranking heap.
func (b *resultBudget) release(n int64) {
	b.retained -= n
	if b.nodeUsed != nil {
		b.nodeUsed.Add(-n)
	}
}

// report hands the budget to queryBudgetHook as the query returns and gives
// back to the node gauge whatever the query still held; from here the answer
// frame, not the budget, owns those bytes.
func (b *resultBudget) report() {
	if b.nodeUsed != nil {
		b.nodeUsed.Add(-b.retained)
	}
	if hook := queryBudgetHook.Load(); hook != nil {
		(*hook)(b.method, b.retained, b.highWater)
	}
}

// elementHeader is what an array element costs on the wire besides its value:
// the type byte, the index as a decimal string, and its terminator.
func elementHeader(index int) int64 {
	return int64(1 + len(strconv.Itoa(index)) + 1)
}

// queryWalk is one query's view of its walk: it counts the entries visited
// and looks at the query's context every queryCheckInterval of them.
type queryWalk struct {
	ctx     context.Context
	method  string
	where   string
	visited int
	hook    *func(method string)
}

func newQueryWalk(ctx context.Context, method, database, bucket string) *queryWalk {
	return &queryWalk{ctx: ctx, method: method, where: database + "." + bucket, hook: queryVisitHook.Load()}
}

// visit counts one entry, and on the first of every queryCheckInterval
// entries returns the context's error if it is done.
func (w *queryWalk) visit() error {
	w.visited++
	if w.hook != nil {
		(*w.hook)(w.method)
	}
	if (w.visited-1)%queryCheckInterval == 0 {
		return w.check()
	}
	return nil
}

// check returns the context's error, if it is done, naming the query and how
// far it got. Executors also call it once before answering.
func (w *queryWalk) check() error {
	if err := w.ctx.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			queryCancelled.Inc()
		}
		return fmt.Errorf("%s on %s stopped after %d entries: %w", w.method, w.where, w.visited, err)
	}
	return nil
}
