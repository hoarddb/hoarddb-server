package model

import (
	"fmt"
	"strconv"
	"strings"
)

// Variant is the layout variant a seed holds: which base layout (the common one every engine
// holds, or the native one with embedded copies), whether the ledger declares its aggregates, and
// how many expenses each ledger account holds. It travels as one word, "common", "native",
// "common+agg", "native+agg" and with "+deep<N>" added for a ledger of N expenses per account
// ("common+agg+deep10000"), so the wrapper, the seed copy tag and the seeder's report all compare
// one string.
type Variant struct {
	Native     bool
	Aggregates bool
	// LedgerExpenses is the expenses per ledger account; 0 is the default depth.
	LedgerExpenses int
}

// Options are the generator options this variant asks for.
func (v Variant) Options() Options { return Options{LedgerExpenses: v.LedgerExpenses} }

// ParseVariant reads the word of -layout-variant. The empty word is the common layout. The
// modifiers +agg and +deep<N> may come in either order, each at most once.
func ParseVariant(word string) (Variant, error) {
	base, suffix, hasSuffix := strings.Cut(word, "+")
	var variant Variant
	switch base {
	case "", "common":
	case "native":
		variant.Native = true
	default:
		return Variant{}, fmt.Errorf("layout variant %q is not common, native, common+agg or native+agg, with an optional +deep<N>. Use common (the layout every engine holds), native (hoarddb with embedded copies), add +agg to declare the ledger's aggregates (hoarddb) and +deep<N> for N expenses per ledger account", word)
	}
	if !hasSuffix {
		return variant, nil
	}
	seenDeep := false
	for _, modifier := range strings.Split(suffix, "+") {
		switch {
		case modifier == "agg" && !variant.Aggregates:
			variant.Aggregates = true
		case strings.HasPrefix(modifier, "deep") && !seenDeep:
			depth, err := strconv.Atoi(strings.TrimPrefix(modifier, "deep"))
			if err != nil || depth < 1 {
				return Variant{}, fmt.Errorf("layout variant %q: %q is not +deep<N> with N a whole number of 1 or more, the expenses per ledger account", word, modifier)
			}
			variant.LedgerExpenses, seenDeep = depth, true
		default:
			return Variant{}, fmt.Errorf("layout variant %q has the modifier %q, which is unknown or repeated. The modifiers are +agg (declare the ledger's aggregates) and +deep<N> (N expenses per ledger account), each once", word, modifier)
		}
	}
	return variant, nil
}

// String is the canonical word ParseVariant reads back.
func (v Variant) String() string {
	word := "common"
	if v.Native {
		word = "native"
	}
	if v.Aggregates {
		word += "+agg"
	}
	if v.LedgerExpenses > 0 {
		word += "+deep" + strconv.Itoa(v.LedgerExpenses)
	}
	return word
}
