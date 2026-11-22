package server

import "testing"

// TestACopyThatAcceptedATermWaitsForItsEntriesBeforeItIsCopied is the follower
// table's waiting row (spec §20.8.6) for a copy that accepted its owner's term
// and has applied none of the term's entries when that owner is removed: the
// next owner's base is further into the same term, and the term's origin is
// still delivering. The proof test met it as a founder copied whole, though
// every previous owner stayed reachable. A copy at (term 2, through 0) is
// transparent when compared for equality with a base (A-10.2) — it holds what
// (1, 54407) holds — but it is behind in term 2. Guard: compare only the
// transparent lineage — the copy is not waited for, and is copied whole.
func TestACopyThatAcceptedATermWaitsForItsEntriesBeforeItIsCopied(t *testing.T) {
	cases := []struct {
		name  string
		own   ReplCopy
		owner ReplCopy
		want  bool
	}{
		{name: "accepted term 2, none of its entries; the base is 8 entries into term 2",
			own: ReplCopy{Term: 2, BaseTerm: 1, BaseThrough: 54407}, owner: ReplCopy{Term: 3, BaseTerm: 2, BaseThrough: 8}, want: true},
		{name: "3 entries into term 2; the base is 8 entries into it",
			own: ReplCopy{Term: 2, Through: 3, BaseTerm: 1, BaseThrough: 54407}, owner: ReplCopy{Term: 3, BaseTerm: 2, BaseThrough: 8}, want: true},
		{name: "term 1 behind a base in term 1",
			own: ReplCopy{Term: 1, Through: 50}, owner: ReplCopy{Term: 2, BaseTerm: 1, BaseThrough: 54}, want: true},
		{name: "at the base",
			own: ReplCopy{Term: 2, Through: 8, BaseTerm: 1, BaseThrough: 54407}, owner: ReplCopy{Term: 3, BaseTerm: 2, BaseThrough: 8}, want: false},
		{name: "past the base",
			own: ReplCopy{Term: 2, Through: 9, BaseTerm: 1, BaseThrough: 54407}, owner: ReplCopy{Term: 3, BaseTerm: 2, BaseThrough: 8}, want: false},
		{name: "a base of another term",
			own: ReplCopy{Term: 1, Through: 50}, owner: ReplCopy{Term: 3, BaseTerm: 2, BaseThrough: 8}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := behindTheBase(tc.own, tc.owner); got != tc.want {
				t.Errorf("a copy at (term %d, through %d, base %d/%d) under an owner whose base is %d/%d: waits = %v, want %v",
					tc.own.Term, tc.own.Through, tc.own.BaseTerm, tc.own.BaseThrough, tc.owner.BaseTerm, tc.owner.BaseThrough, got, tc.want)
			}
		})
	}
}
