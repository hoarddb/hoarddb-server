package machine

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func quorumAdmission() Admission {
	a := admitGood()
	a.Members = 5
	return a
}

// TestAValidLeaseWithoutAQuorumIsRefusedNoQuorum: a lease can outlive the majority that
// granted it by up to step_down_ms; the quorum input closes that window (membership 8).
func TestAValidLeaseWithoutAQuorumIsRefusedNoQuorum(t *testing.T) {
	a := quorumAdmission()
	a.QuorumLost = true
	if got := Check(a); got.Code != cluster.CodeNoQuorum {
		t.Fatalf("owner with a valid lease and no quorum: %+v, want NO_QUORUM", got)
	}
}

// TestAOneMemberClusterIsNeverWithoutAQuorum: a lone voter is its own majority.
func TestAOneMemberClusterIsNeverWithoutAQuorum(t *testing.T) {
	a := quorumAdmission()
	a.Members, a.QuorumLost = 1, true
	if got := Check(a); got.Code == cluster.CodeNoQuorum {
		t.Fatalf("a one-member cluster was refused NO_QUORUM: %+v", got)
	}
}

// TestALapsedLeaseStillReportsLeaseLapsed: the lease check keeps its place before the quorum one.
func TestALapsedLeaseStillReportsLeaseLapsed(t *testing.T) {
	a := quorumAdmission()
	a.LeaseValid, a.QuorumLost = false, true
	if got := Check(a); got.Code != cluster.CodeLeaseLapsed {
		t.Fatalf("lapsed lease and no quorum: %+v, want LEASE_LAPSED", got)
	}
}
