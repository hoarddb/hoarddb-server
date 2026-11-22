package ringclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Member is one ranked data member of the cluster.
type Member struct {
	// ID is the node id and Addr the address a driver connects to: the member's client address,
	// or empty when it published none (the data address is the internode listener and serves no
	// driver, so a caller skips a member whose Addr is empty).
	ID, Addr string
	// DataAddr and ClientAddr are the two committed addresses; ClientAddr is empty when
	// the member published none.
	DataAddr, ClientAddr string
	// Voter is true when the member votes in the cluster state group.
	Voter bool
	// State is active, down or leaving; a down member keeps its rank and is skipped as owner.
	State string
}

// TopologyDatabase is a database the session may see, with its replication factor.
type TopologyDatabase struct {
	Name string
	ID   int64
	RF   int32
}

// Topology is the cluster view a smart driver routes with: the members, the full ranking
// of all 8192 slots and the databases the session may see. It is immutable; ask the
// server again with [Client.Topology] when a request is refused STALE_EPOCH or NOT_OWNER.
type Topology struct {
	// Epoch and StateSeq identify the view; send them back to learn whether it changed.
	Epoch, StateSeq int64
	// Voters is how many members vote in the cluster state group, from the committed voter list, so
	// it counts a voter that is joining or dead and so is in neither Members nor Witnesses. Derive
	// the quorum from it, not from the Voter flags alone.
	Voters int32
	// Members are the ranked data members, sorted by id.
	Members []Member
	// Witnesses are the witnesses (voting or not; members that hold no documents), sorted by id. A driver
	// never routes to one, and the server publishes no client address for a witness, so Addr is empty.
	Witnesses []Member
	// Databases are the databases this session may see, by id.
	Databases []TopologyDatabase

	table *cluster.SlotTable
}

// Topology asks the node for the cluster view. Pass nil for the first call. Pass the view
// you hold to learn whether it is still current: the server then answers with the numbers
// alone and the same *Topology comes back. Any authenticated session may call it; the
// databases listed are the ones the session holds a role on.
func (c *Client) Topology(ctx context.Context, have *Topology) (*Topology, error) {
	var request wire.RingTopologyRequest
	if have != nil {
		request = wire.RingTopologyRequest{Epoch: have.Epoch, StateSeq: have.StateSeq}
	}
	body, err := wire.EncodeRingTopology(request)
	if err != nil {
		return nil, fmt.Errorf("ringclient: encoding the topology request: %w", err)
	}
	payload, err := c.call(ctx, transport.OpRingTopology, c.NextRequestID(), body)
	if err != nil {
		return nil, fmt.Errorf("topology: %w", err)
	}
	reply, err := wire.DecodeRingTopologyReply(payload)
	if err != nil {
		return nil, fmt.Errorf("ringclient: decoding the topology reply: %w", err)
	}
	if reply.Unchanged {
		if have == nil {
			return nil, errors.New("ringclient: the server answered unchanged to a request that held nothing")
		}
		return have, nil
	}
	topology := &Topology{Epoch: reply.Epoch, StateSeq: reply.StateSeq, Voters: reply.Voters, table: reply.Table}
	for _, member := range reply.Table.Members() {
		topology.Members = append(topology.Members, memberOf(member))
	}
	for _, witness := range reply.Witnesses {
		topology.Witnesses = append(topology.Witnesses, memberOf(witness))
	}
	for _, database := range reply.Databases {
		topology.Databases = append(topology.Databases, TopologyDatabase{Name: database.Name, ID: database.ID, RF: database.RF})
	}
	return topology, nil
}

// memberOf is the driver's view of a committed member: it connects to the client address the
// member published, and to nothing when it published none.
func memberOf(member cluster.Member) Member {
	return Member{ID: member.ID, Addr: member.DriverAddr(), DataAddr: member.DataAddr, ClientAddr: member.ClientAddr,
		Voter: member.Suffrage == "voter", State: member.State.String()}
}

// Owner is the member that serves slot of database: the first of the first RF names of the
// slot's ranking whose state is not down (conventions section 4). It reports false when the
// database is not in the view, the slot is out of range or every copy is down.
func (t *Topology) Owner(database string, slot uint16) (Member, bool) {
	for _, listed := range t.Databases {
		if listed.Name != database {
			continue
		}
		owner, ok := t.table.Owner(slot, int(listed.RF))
		if !ok {
			return Member{}, false
		}
		return memberOf(owner), true
	}
	return Member{}, false
}
