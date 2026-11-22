package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/version"
)

var startTime = time.Now()

// HealthCheck reports live node state: how many buckets are attached, how
// long the process has been up, and whether anything makes the node unhealthy
// (Reasons). It reads node totals only. Counting entries
// meant taking every bucket's lock and reading its store, which made a probe
// slower the more buckets a node held; one bucket's count is on demand
// (BucketStats, 'describe bucket').
func (s *Server) HealthCheck() (*HealthResponse, error) {
	health := &HealthResponse{
		NodeID:      s.config.Server.NodeID,
		Uptime:      time.Since(startTime).Truncate(time.Second).String(),
		Version:     version.Version,
		BucketCount: int(s.bucketsAttached.Load()),
		Healthy:     true,
		Capacity:    s.capacity.healthCapacity(s.probes.Load().now()),
		Memtable:    s.memtable.healthMemtable(s.memtableDisagreements()),
		WAL:         s.healthWAL(),
		Encryption:  s.healthEncryption(),
		Host:        s.hostFacts.get(),
	}
	health.Membership, health.MembershipDetail = s.healthMembership()
	if s.cluster.Enabled() {
		health.Duplicates = s.cluster.instances.notes(time.Now())
	}
	if refusal := s.storeWriteRefusal(); refusal != nil {
		health.StoreRefusal = refusal.Error()
	}
	if passwordKey := s.ringPasswordKey.Load(); passwordKey != nil {
		health.PasswordKey = (*passwordKey)()
	}
	health.Reasons = healthReasons(health)
	if loading := s.ringLoading.Load(); loading != nil {
		if reason := (*loading)(); reason != "" {
			// First: a node that serves nothing is not healthy whatever else is true of it.
			health.Reasons = append([]string{reason}, health.Reasons...)
		}
	}
	health.Healthy = len(health.Reasons) == 0
	return health, nil
}

// healthReasons lists what is wrong with a node, from the parts of its health
// answer. Only a condition a person has to act on counts: a capacity row that
// is red, a store that should be sealed and is not, a member this node cannot
// be confirmed by, a store that cannot persist writes. A stale or approaching row, and a node still waiting to be
// added, are shown by status and are not reasons to take the node out of
// service.
func healthReasons(health *HealthResponse) []string {
	var reasons []string
	if health.Encryption != nil && health.Encryption.Problem != "" {
		reasons = append(reasons, "encryption: "+health.Encryption.Problem)
	}
	if health.Capacity != nil {
		for _, row := range health.Capacity.Rows {
			if row.State == stateRed.String() {
				reasons = append(reasons, fmt.Sprintf("capacity: %s is over its limit; run 'status' for the numbers", row.Resource))
			}
		}
	}
	if health.Membership == MembershipRefused {
		reasons = append(reasons, "membership: "+health.MembershipDetail)
	}
	if health.StoreRefusal != "" {
		reasons = append(reasons, "store: "+health.StoreRefusal)
	}
	return reasons
}

// writeRefuser is a node store that can say it refuses writes
// (storage.BadgerKV).
type writeRefuser interface {
	WriteRefusal() error
}

// storeWriteRefusal is why the node store, or the write-ahead log of a node
// that keeps one, refuses writes, or nil.
func (s *Server) storeWriteRefusal() error {
	if refuser, ok := s.kv.(writeRefuser); ok {
		if refusal := refuser.WriteRefusal(); refusal != nil {
			return refusal
		}
	}
	if s.wal != nil {
		return s.wal.WriteRefusal()
	}
	return nil
}

// healthMembership is status's membership line: whether a member has
// confirmed this node's member list, and, when a member it will not exchange
// membership with keeps it unconfirmed, why. A standalone server has none to
// confirm.
func (s *Server) healthMembership() (membership, detail string) {
	if !s.cluster.Enabled() {
		return "", ""
	}
	if !s.cluster.bootstrapped.Load() {
		return MembershipConfirmed, ""
	}
	if refusal := s.cluster.membershipRefusal(); refusal != nil {
		return MembershipRefused, refusal.Error()
	}
	if silent, founder := s.cluster.foundersNotAnswered(); founder {
		return MembershipWaitingForFounders, strings.Join(silent, ", ")
	}
	return MembershipNotAddedYet, ""
}

// healthWAL is HEALTH_CHECK's wal field: nil on a node that keeps no log.
func (s *Server) healthWAL() *HealthWAL {
	if s.wal == nil {
		return nil
	}
	stats := s.wal.Stats()
	health := &HealthWAL{
		Preallocated: stats.Preallocated,
		Configured:   s.walMode.Configured,
		Reason:       s.walMode.Reason,
		SpareReady:   stats.SpareReady,
	}
	if refusal := s.wal.WriteRefusal(); refusal != nil {
		health.Refusal = refusal.Error()
	}
	return health
}

// ringLoadingReason words why a ring node serves nothing yet, or "" when it serves. A readiness
// probe reads it from HEALTH_CHECK's reasons, so it sees why and not only that.
func ringLoadingReason(diverged, loaded bool) string {
	switch {
	case diverged:
		return "diverged: this node refused a committed entry its leader cannot have appended and is stopping; start it on the build the leader runs and keep its data directory"
	case !loaded:
		return "replaying its log: this node is applying its saved cluster state and serves nothing yet; it finishes by itself, and if it does not, read the server log"
	}
	return ""
}
