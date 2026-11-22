package cli

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// verbOptionRules is what one verb accepts in its option document: the words
// its usage messages use, and the value rule of each key it takes. Peek and
// pop have one key, limit, and the same rule for it, and their messages are
// the one-key form (reads and read). A verb with other keys sets names and
// example instead, and the reader words its messages from those: the keys it
// takes, in the order they are shown, and one example statement.
type verbOptionRules struct {
	// reads and read finish the usage lines: "reads the next N without
	// removing them" after a statement, "read the next N without removing
	// them" after "Use ... to". N is the count the message fills in. They are
	// the one-key form, used only when example is empty.
	reads, read string
	keys        map[string]func(verb, where string, raw any) error
	// names are the keys of a multi-key verb in the order its usage shows
	// them, and example is a statement after "<where>.", for instance
	// `claim({limit: 10, lease: "30s"})`. Both are empty for peek and pop.
	names   []string
	example string
	// require runs once the keys given have passed their rules, with the
	// options (nil when none were given), and refuses a statement that lacks
	// a key the verb cannot do without.
	require func(verb, where string, options map[string]any) error
}

// multiKey reports whether the verb words its messages from names and example.
func (rules verbOptionRules) multiKey() bool { return rules.example != "" }

// verbOptions are the rules by verb. The lease verbs' rules are written in
// lease_verbs.go.
var verbOptions = map[string]verbOptionRules{
	"claim":   claimOptionRules,
	"ack":     ackOptionRules,
	"release": releaseOptionRules,
	"extend":  extendOptionRules,
	"dead":    deadOptionRules,
	"requeue": requeueOptionRules,
	"discard": discardOptionRules,
	"peek": {
		reads: "reads the next %s without removing them",
		read:  "read the next %s without removing them",
		keys:  map[string]func(verb, where string, raw any) error{"limit": checkLimitValue},
	},
	"pop": {
		reads: "removes and returns the next %s",
		read:  "remove and return the next %s",
		keys:  map[string]func(verb, where string, raw any) error{"limit": checkLimitValue},
	},
}

// checkLimitValue is the rule of a limit: a whole number of 1 or more. A limit
// of 0 is a request for nothing and refused; peek() and pop() are the
// one-entry form. It is deliberately not fieldList's reader, which reads
// options["limit"].(float64): that ignores {limit: "3"} (the default applies),
// truncates {limit: 1.5} to 1 and never looks at the other keys, so a
// mistyped key would quietly answer one entry.
func checkLimitValue(verb, where string, raw any) error {
	number, isNumber := raw.(float64)
	if !isNumber || number != math.Trunc(number) || number < 1 {
		shown, _ := json.Marshal(raw)
		return usagef("%s's limit is a whole number of 1 or more, as in %s.%s({limit: 3}); got %s", verb, where, verb, shown)
	}
	return nil
}

// readVerbOptions is the one reader of a verb's argument list, shared by the
// bucket verbs and the store-field verbs so the two levels cannot drift. where
// is the address the usage errors show: db.Jobs or db.Post["p1"].Comments. The
// verb takes at most one argument, an option document, and every key in it
// must be one the verb's rules name and pass that key's value rule. It returns
// nil options, meaning none given, for verb() and for a verb({}) with no keys.
func readVerbOptions(verb, where string, arguments []string, rules verbOptionRules) (map[string]any, error) {
	options, err := readOptionDocument(verb, where, arguments, rules)
	if err != nil {
		return nil, err
	}
	if rules.require != nil {
		if err := rules.require(verb, where, options); err != nil {
			return nil, err
		}
	}
	return options, nil
}

// readOptionDocument is readVerbOptions without the verb's required keys: the
// argument is read, its keys are checked against the rules, and each value
// passes its key's rule.
func readOptionDocument(verb, where string, arguments []string, rules verbOptionRules) (map[string]any, error) {
	switch len(arguments) {
	case 0:
		return nil, nil
	case 1:
	default:
		if rules.multiKey() {
			return nil, usagef("%s takes at most one argument, an option document: %s.%s", verb, where, rules.example)
		}
		return nil, usagef("%s takes at most one argument, an option document: %s.%s({limit: N}) %s",
			verb, where, verb, fmt.Sprintf(rules.reads, "N"))
	}
	argument := strings.TrimSpace(arguments[0])
	if !strings.HasPrefix(argument, "{") {
		if rules.multiKey() {
			return nil, usagef("%s(%s) is not supported: the options go in an option document.\n       Use %s.%s.",
				verb, argument, where, rules.example)
		}
		count := "N"
		if _, err := strconv.Atoi(argument); err == nil {
			count = argument
		}
		return nil, usagef("%s(%s) is not supported: the count goes in an option document.\n       Use %s.%s({limit: %s}) to %s.",
			verb, argument, where, verb, count, fmt.Sprintf(rules.read, count))
	}
	options, err := parseOptionDocument(argument)
	if err != nil {
		return nil, fmt.Errorf("%s.%s: %w", where, verb, err)
	}
	given := make([]string, 0, len(options))
	for key := range options {
		given = append(given, key)
	}
	sort.Strings(given)
	for _, key := range given {
		if _, accepted := rules.keys[key]; !accepted {
			if rules.multiKey() {
				return nil, usagef("%s %s: %s.%s", verb, unknownKeyClause(rules.names, key), where, rules.example)
			}
			return nil, usagef("%s has one option, limit, and %q is not it: %s.%s({limit: 3}) %s",
				verb, key, where, verb, fmt.Sprintf(rules.reads, "3"))
		}
	}
	for _, key := range given {
		if err := rules.keys[key](verb, where, options[key]); err != nil {
			return nil, err
		}
	}
	return options, nil
}

// readVerbLimit is peek's and pop's reader of their count: it returns 0,
// meaning absent, for verb() and verb({}). There is no upper bound here: the
// server owns the caps (1000 for peek, 100 for pop) and their message, so each
// number has one source. A value above an int32 is sent as math.MaxInt32,
// because the server's refusal names the number it was sent either way. It is
// not readLimitOption, which reads a page size from a parsed document for find
// and list.
func readVerbLimit(verb, where string, arguments []string) (int, error) {
	options, err := readVerbOptions(verb, where, arguments, verbOptions[verb])
	if err != nil {
		return 0, err
	}
	raw, present := options["limit"]
	if !present {
		return 0, nil
	}
	return int(min(raw.(float64), math.MaxInt32)), nil
}

// unknownKeyClause words what a multi-key verb accepts and that the key given
// is not among it: `has the options limit and lease, and "bogus" is not one of
// them`, or, for a verb with one key, `has the option lease_id, and "limit" is
// not it`.
func unknownKeyClause(names []string, key string) string {
	if len(names) == 1 {
		return fmt.Sprintf("has the option %s, and %q is not it", names[0], key)
	}
	listed := strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
	return fmt.Sprintf("has the options %s, and %q is not one of them", listed, key)
}
