package cli

import "testing"

// TestPeekAndPopMessagesUnchanged pins the exact usage messages of peek and
// pop, captured on the tree before the lease verbs generalised the option
// reader. The reader is shared, and a multi-key verb's wording must not leak
// into the one-key verbs (claim-ack-spec 12; build doc step 18).
func TestPeekAndPopMessagesUnchanged(t *testing.T) {
	cases := []struct {
		verb, where string
		arguments   []string
		want        string
	}{
		{"peek", "db.Jobs", []string{"1", "2"}, "peek takes at most one argument, an option document: db.Jobs.peek({limit: N}) reads the next N without removing them"},
		{"peek", "db.Jobs", []string{"3"}, "peek(3) is not supported: the count goes in an option document.\n       Use db.Jobs.peek({limit: 3}) to read the next 3 without removing them."},
		{"peek", "db.Jobs", []string{"abc"}, "peek(abc) is not supported: the count goes in an option document.\n       Use db.Jobs.peek({limit: N}) to read the next N without removing them."},
		{"peek", "db.Jobs", []string{`{lmit: 3}`}, `peek has one option, limit, and "lmit" is not it: db.Jobs.peek({limit: 3}) reads the next 3 without removing them`},
		{"peek", "db.Jobs", []string{`{limit: 0}`}, "peek's limit is a whole number of 1 or more, as in db.Jobs.peek({limit: 3}); got 0"},
		{"peek", "db.Jobs", []string{`{limit: "3"}`}, `peek's limit is a whole number of 1 or more, as in db.Jobs.peek({limit: 3}); got "3"`},
		{"peek", `db.Post["p1"].Comments`, []string{`{lmit: 3}`}, `peek has one option, limit, and "lmit" is not it: db.Post["p1"].Comments.peek({limit: 3}) reads the next 3 without removing them`},
		{"pop", "db.Jobs", []string{"1", "2"}, "pop takes at most one argument, an option document: db.Jobs.pop({limit: N}) removes and returns the next N"},
		{"pop", "db.Jobs", []string{"3"}, "pop(3) is not supported: the count goes in an option document.\n       Use db.Jobs.pop({limit: 3}) to remove and return the next 3."},
		{"pop", "db.Jobs", []string{"abc"}, "pop(abc) is not supported: the count goes in an option document.\n       Use db.Jobs.pop({limit: N}) to remove and return the next N."},
		{"pop", "db.Jobs", []string{`{lmit: 3}`}, `pop has one option, limit, and "lmit" is not it: db.Jobs.pop({limit: 3}) removes and returns the next 3`},
		{"pop", "db.Jobs", []string{`{limit: 0}`}, "pop's limit is a whole number of 1 or more, as in db.Jobs.pop({limit: 3}); got 0"},
		{"pop", "db.Jobs", []string{`{limit: "3"}`}, `pop's limit is a whole number of 1 or more, as in db.Jobs.pop({limit: 3}); got "3"`},
		{"pop", `db.Post["p1"].Comments`, []string{"3"}, "pop(3) is not supported: the count goes in an option document.\n       Use db.Post[\"p1\"].Comments.pop({limit: 3}) to remove and return the next 3."},
	}
	for _, tc := range cases {
		t.Run(tc.verb+"/"+tc.where+"/"+tc.arguments[0], func(t *testing.T) {
			_, err := readVerbOptions(tc.verb, tc.where, tc.arguments, verbOptions[tc.verb])
			if err == nil || err.Error() != tc.want {
				t.Errorf("%s%v:\n got: %v\nwant: %s", tc.verb, tc.arguments, err, tc.want)
			}
		})
	}
}
