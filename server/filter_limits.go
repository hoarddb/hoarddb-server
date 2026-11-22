package server

import (
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
)

// A filter is evaluated once per document a query examines, so its cost
// multiplies by the size of the bucket. These limits bound what one filter can
// cost before any document is read. Each is generous for a real query and
// small beside what a request frame could otherwise carry. They are fixed,
// and the Limits page of the user docs lists them.
const (
	// maxRegexPatternBytes bounds one $regex pattern's text.
	maxRegexPatternBytes = 4096
	// maxRegexProgramSize bounds the work one $regex adds per byte of a
	// document's string: the instructions its compiled program holds, with
	// repeats counted at their full size. 512 holds prefix, contains, email
	// shape and case-insensitive word lists with room to spare.
	maxRegexProgramSize = 512
	// maxFilterRegexProgramSize bounds the same for every $regex in one filter.
	maxFilterRegexProgramSize = 2048
	// maxMatchWork bounds the regex work done on one document, in program
	// steps times bytes scanned, summed over every $regex the document meets
	// (a $or of regexes shares it). Go's regexp costs 7 to 8 ns per step per
	// byte on the 4-vCPU test hosts (measured 2026-10-21), so 1.5 million is
	// about 11 ms: a pattern of 100 steps runs over a field of up to 15,000
	// bytes, and a 512-step pattern over one of 2,900.
	maxMatchWork = 1500000
	// maxFilterLogicalClauses bounds the filters inside one $and or $or.
	maxFilterLogicalClauses = 64
	// maxFilterInMembers bounds the values in one $in or $nin.
	maxFilterInMembers = 1000
	// maxFilterDepth bounds how deep $and and $or nest.
	maxFilterDepth = 8
	// maxFilterClauses bounds the clauses of one whole filter.
	maxFilterClauses = 1000
)

// ErrFilterRefused is under every refusal of a filter for its cost or its
// form. It is the request's fault, so it reaches the wire as
// ErrInvalidArgument, not as an internal fault to report.
var ErrFilterRefused = errors.New("filter refused")

type filterRefusal struct{ message string }

func (r *filterRefusal) Error() string { return r.message }

func (r *filterRefusal) Is(target error) bool { return target == ErrFilterRefused }

// refuseFilter formats a refusal that matches ErrFilterRefused. Callers pass
// no %w operand: the message already names the limit and the fix.
func refuseFilter(format string, args ...interface{}) error {
	return &filterRefusal{message: fmt.Sprintf(format, args...)}
}

// matchBudget is the regex work one document may cost the whole filter.
type matchBudget struct{ remaining int }

// boundedRegex is a $regex compiled once for the query, with the program size
// its work is charged by.
type boundedRegex struct {
	field      string
	expression *regexp.Regexp
	steps      int
}

// matchBytes matches a stored string's bytes, which it does not copy. It
// charges steps times the text's length to budget before it scans, and
// refuses when the document has none left: the scan's cost is known first, so
// a long field never has to be read to be refused.
func (r *boundedRegex) matchBytes(text []byte, budget *matchBudget) (bool, error) {
	if err := r.charge(len(text), budget); err != nil {
		return false, err
	}
	return r.expression.Match(text), nil
}

// charge takes the work of scanning length bytes from budget, or refuses
// when the document has too little left.
func (r *boundedRegex) charge(length int, budget *matchBudget) error {
	work := r.steps * length
	if work > budget.remaining {
		return refuseFilter("$regex on %q would scan %d bytes, past this server's work limit of %d pattern steps times bytes per document (this pattern has %d steps). Use a shorter or simpler pattern, filter on a shorter field, or narrow the query with an indexed field first", r.field, length, maxMatchWork, r.steps)
	}
	budget.remaining -= work
	return nil
}

// filterBudget tracks what the clauses compiled so far for one query have
// spent against the whole-filter limits.
type filterBudget struct {
	clauses     int
	programSize int
}

func (b *filterBudget) chargeClause() error {
	b.clauses++
	if b.clauses == maxFilterClauses+1 {
		return refuseFilter("the filter has more than %d clauses; this server's limit is %d per query. Narrow the filter, or run two queries", maxFilterClauses, maxFilterClauses)
	}
	return nil
}

// prepareCondition checks a field's operator document against the cost limits
// and replaces each $regex pattern with its compiled form, so the operator
// never compiles per document. A condition that is not an operator document is
// returned as it is.
func prepareCondition(key string, condition interface{}, budget *filterBudget) (interface{}, error) {
	operators, isOperators := operatorDocument(condition)
	if !isOperators {
		return condition, nil
	}
	prepared := make(map[string]interface{}, len(operators))
	for operator, operand := range operators {
		switch operator {
		case "$in", "$nin":
			if members, ok := asList(operand); ok && len(members) > maxFilterInMembers {
				return nil, refuseFilter("%s on %q holds %d values; this server's limit is %d per %s. Split the query, or filter on a range ($gte and $lte) instead of listing values", operator, key, len(members), maxFilterInMembers, operator)
			}
		case "$regex":
			pattern, ok := operand.(string)
			if !ok {
				return nil, refuseFilter("$regex expects a string pattern, got %T", operand)
			}
			compiled, err := compileBoundedRegex(key, pattern, budget)
			if err != nil {
				return nil, err
			}
			operand = compiled
		}
		prepared[operator] = operand
	}
	return prepared, nil
}

// compileBoundedRegex compiles pattern after checking its length and its
// program size, estimated from the parse tree before the program is built:
// compiling a program that is too large is itself the cost being refused.
func compileBoundedRegex(key, pattern string, budget *filterBudget) (*boundedRegex, error) {
	if len(pattern) > maxRegexPatternBytes {
		return nil, refuseFilter("$regex on %q is %d bytes; this server's limit is %d bytes per pattern. Shorten the pattern, or match a prefix with $gte and $lt", key, len(pattern), maxRegexPatternBytes)
	}
	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil, refuseFilter("$regex pattern %q is not a valid regular expression: %v", pattern, err)
	}
	size := estimateProgramSize(parsed, maxFilterRegexProgramSize+1)
	if size > maxRegexProgramSize {
		return nil, refuseFilter("$regex on %q is too large to run: it expands to more than %d steps; this server's limit is %d per pattern. Replace large repeats such as a{1000} with a smaller bound or a simpler pattern", key, maxRegexProgramSize, maxRegexProgramSize)
	}
	budget.programSize += size
	if budget.programSize > maxFilterRegexProgramSize {
		return nil, refuseFilter("the $regex patterns in this filter expand to more than %d steps together; this server's limit is %d per query. Use fewer or simpler patterns, or run two queries", maxFilterRegexProgramSize, maxFilterRegexProgramSize)
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, refuseFilter("$regex pattern %q is not a valid regular expression: %v", pattern, err)
	}
	return &boundedRegex{field: key, expression: compiled, steps: scanSteps(parsed, size)}, nil
}

// scanSteps is what a match charges per byte scanned. A case-sensitive
// literal anchored at the start of the text (^literal, or ^literal$) is a
// prefix comparison, which costs a fixed few operations whatever the field's
// length, so it is charged one. Any other literal is not cheap: Go finds the
// candidate positions with a substring search but verifies each one with the
// program, so on self-overlapping text (a{499}b over a field of a's) it costs
// about its length times the field's, and an end-anchored-only literal gets no
// prefix search at all. Those, and a case-insensitive literal, are charged
// their full size.
func scanSteps(parsed *syntax.Regexp, size int) int {
	if parsed.Op != syntax.OpConcat || len(parsed.Sub) < 2 || parsed.Sub[0].Op != syntax.OpBeginText {
		return size
	}
	parts := parsed.Sub[1:]
	if parts[len(parts)-1].Op == syntax.OpEndText {
		parts = parts[:len(parts)-1]
	}
	if len(parts) == 1 && parts[0].Op == syntax.OpLiteral && parts[0].Flags&syntax.FoldCase == 0 {
		return 1
	}
	return size
}

// estimateProgramSize returns the instruction count the compiled form of
// expression would have, stopping once it passes ceiling so a pathological
// pattern is not walked to the end. It follows the compiler: an optional or
// repeated part costs a split instruction beside its body, so \pL{0,1000} is
// about 2,000 steps, not 1,000.
func estimateProgramSize(expression *syntax.Regexp, ceiling int) int {
	body := 0
	for _, sub := range expression.Sub {
		body += estimateProgramSize(sub, ceiling)
		if body >= ceiling {
			return ceiling
		}
	}
	switch expression.Op {
	case syntax.OpLiteral:
		return len(expression.Rune)
	case syntax.OpRepeat:
		return repeatSize(body, expression.Min, expression.Max, ceiling)
	case syntax.OpAlternate:
		return body + len(expression.Sub)
	case syntax.OpStar, syntax.OpPlus, syntax.OpQuest, syntax.OpCapture:
		return body + 2
	}
	return body + 1
}

// repeatSize is the steps of a body repeated min to max times (max -1 is
// unbounded): min plain copies, then max-min optional ones each with a split.
func repeatSize(body, min, max, ceiling int) int {
	optional := 1
	if max >= 0 {
		optional = max - min
	}
	copies := min*body + optional*(body+1)
	if body > 0 && (min+optional) > ceiling/body {
		return ceiling
	}
	return copies + 1
}
