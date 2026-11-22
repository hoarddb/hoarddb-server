package server

import "testing"

// TestProofExemptsOnlyAConfirmedOneWriteLostWithItsRemovedOwner is BUG-0544's
// deterministic proof: spec §20.6 lets a write answered copies_confirmed 1,
// at any concern, be lost with its old owner when that owner is removed
// while stopped, and nothing that promised more. The exemption needs positive
// evidence of who answered.
func TestProofExemptsOnlyAConfirmedOneWriteLostWithItsRemovedOwner(t *testing.T) {
	removed := map[string]bool{"killed": true}
	cases := []struct {
		name string
		ack  proofAck
		held bool
		want bool
	}{
		{"one, confirmed 1, owner killed and removed", proofAck{confirmed: 1, ownerAtAck: "killed", ownerAtSend: "killed"}, false, true},
		// BUG-0547: the answer does not name the node that gave it, so the
		// owner at send time is not evidence when the owner changed in flight.
		// Only the coordinator's recorded forward target is.
		{"one, confirmed 1, owner changed in flight, forwarded to the new owner, no running holder", proofAck{confirmed: 1, ownerAtAck: "", ownerAtSend: "killed", forwardedTo: "new"}, false, false},
		{"one, confirmed 1, owner changed in flight, forwarded to the killed owner, no running holder", proofAck{confirmed: 1, ownerAtAck: "", ownerAtSend: "killed", forwardedTo: "killed"}, false, true},
		{"one, confirmed 1, owner changed in flight, no forward target recorded, no running holder", proofAck{confirmed: 1, ownerAtAck: "", ownerAtSend: "killed"}, false, false},
		{"one, confirmed 1, owner changed in flight, a running node holds it", proofAck{confirmed: 1, ownerAtAck: "", ownerAtSend: "killed"}, true, false},
		{"one, confirmed 2, owner killed and removed", proofAck{confirmed: 2, ownerAtAck: "killed", ownerAtSend: "killed"}, false, false},
		{"one, confirmed 1, owner still running", proofAck{confirmed: 1, ownerAtAck: "alive", ownerAtSend: "alive"}, false, false},
		{"one, confirmed 1, owner stopped but not removed", proofAck{confirmed: 1, ownerAtAck: "stopped", ownerAtSend: "stopped"}, false, false},
		{"one, confirmed 1, no record of an answering owner", proofAck{confirmed: 1}, false, false},
		{"one, confirmed 1, owner changed in flight, sender still running", proofAck{confirmed: 1, ownerAtSend: "alive"}, false, false},
		{"majority, confirmed 1 (spec 20.6: any concern), owner killed and removed", proofAck{confirmed: 1, ownerAtAck: "killed", ownerAtSend: "killed"}, false, true},
		{"majority, confirmed 2, owner killed and removed", proofAck{confirmed: 2, ownerAtAck: "killed", ownerAtSend: "killed"}, false, false},
		{"all, confirmed 3, owner killed and removed", proofAck{confirmed: 3, ownerAtAck: "killed", ownerAtSend: "killed"}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mayBeLostAtOne(tc.ack, removed, tc.held); got != tc.want {
				t.Errorf("mayBeLostAtOne(%+v, held %v) = %v, want %v", tc.ack, tc.held, got, tc.want)
			}
		})
	}
}
