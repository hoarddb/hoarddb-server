package cluster

import (
	"encoding/binary"
	"fmt"
)

// Diverged is what ApplyCommand returns for a command stamped above the state's StateSeq. The
// stamping leader's state was at BaseSeq when it appended the command, so a node whose own
// state is behind that disagrees with the leader about the state at some earlier sequence
// number: it refused a command the leader accepted. It is deliberately not an *Error, so no
// proposer can take it for STATE_MOVED and restamp; the dry-run callers map it to STATE_MOVED
// (a proposer's own stamp can only be ahead of a stale state), and Apply records it.
type Diverged struct {
	// BaseSeq is the stamp the command carries.
	BaseSeq uint64
	// StateSeq is the state_seq of the state that was asked to apply it.
	StateSeq int64
}

// Error says what is known: the two counters, and that the node and the proposer disagree.
func (e *Diverged) Error() string {
	return fmt.Sprintf("the command is stamped at state_seq %d but this node's state is at %d: this node and the node that proposed the command disagree about the cluster state", e.BaseSeq, e.StateSeq)
}

// Divergence is the first committed entry this node refused for a reason the stamping leader's
// dry run would have refused too (or could not have accepted), recorded once by the FSM.
type Divergence struct {
	// Index and Term place the entry in the Raft log.
	Index, Term uint64
	// BaseSeq is the entry's stamp when its header is long enough to carry one.
	BaseSeq uint64
	// StateSeq is this node's state_seq when the entry was refused.
	StateSeq int64
	// Kind is the entry's command kind, 0 when the header is too short.
	Kind byte
	// Reason is the refusal's own message.
	Reason string
}

// rawBaseSeq reads the stamp from bytes 2 to 10 of a command's header without decoding it, so
// an entry whose version or kind this build refuses still has a stamp. ok is false when the
// entry is shorter than the header. The position never moves for any command version.
func rawBaseSeq(raw []byte) (stamp uint64, ok bool) {
	if len(raw) < commandHeaderBytes {
		return 0, false
	}
	return binary.BigEndian.Uint64(raw[2:commandHeaderBytes]), true
}

// DivergedExitCode is the exit status of a node that stopped because it diverged (EX_SOFTWARE).
// The text names it, and a service manager that restarts on failure is told to exclude it.
const DivergedExitCode = 70

// FatalText is text 1 of the mixed-build spec: what a node logs at the first divergence. The
// leader of the entry's term is named only when the caller knows it, and never guessed.
func FatalText(record Divergence, leader string) string {
	who := ""
	if leader != "" {
		who = ", " + leader
	}
	return fmt.Sprintf("this node's cluster state and the node that proposed committed entry %d (term %d, the leader of that term%s) disagree: %s. "+
		"Stopping so this node cannot serve a stale view or drop what the cluster accepted. "+
		"The proposer's build is the one that wrote the entry and the only one that can apply the log: start the nodes that stop with this message on the build the proposer runs ('nodes' lists the members). "+
		"Starting a node on a different build can be blocked by local formats it has already written, in which case keep its data directory untouched. "+
		"If the node stops again with this message after that, its stored state is stale or its raft log is damaged (the log store has no page checksums, and a hard kill cannot truncate its tail, so damage means disk trouble). "+
		"Keep its data directory untouched (do not delete it: with replication factor 1 it holds the only copy) and replace the node: move its ring directory aside and keep it, "+
		"start it again with the same node id, keys and address, wait until 'nodes' shows it down, then run 'replace-node <node> --with <node>@<addr>' on any member. "+
		"Exit code %d.", record.Index, record.Term, who, record.Reason, DivergedExitCode)
}
