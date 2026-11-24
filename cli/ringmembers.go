package cli

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/storage"
)

// A cluster member answers list, count and show buckets for the slots it owns, and a bucket's
// declaration is listed only by the member that owns its slot of the _buckets system bucket
// (cluster/wire/node_list.go:Node.ownedMask). A dump or a restore that asked one member saw that
// member's share (BUG-0903). ringMemberSet is the CLI's answer with the requests a v1.1.0 server
// already serves: it connects to every member that can own a slot and pages each one in turn.
//
// Why each document is read exactly once: for one committed epoch the slot table is the same on
// every member, a slot of a database has exactly one owner (cluster/table.go:SlotTable.Owner, the
// first copy that is not down), and a member's scan covers exactly the slots the table gives it,
// or refuses (cluster/wire/readgate.go:ReadGate.AdmitMask refuses a held or forwarded slot; it
// never shrinks the mask). So the members' answers are disjoint and their union is every slot
// that has an owner, provided every member used the same epoch for every page. The set checks
// that: all members report one epoch when it opens, and again when the run ends (checkUnchanged);
// a member's committed epoch only grows, so an equal start and end means no page was read at
// another one. A slot with no owner (every copy down) is refused before the run starts.
//
// The cost is O(members) connections and O(members) topology reads per run, and a loop over the
// 8192 slots per database in scope; nothing per bucket or per document is added on the server.

// ringReadMember is one member a run reads from.
type ringReadMember struct {
	id, addr string
	client   *ringclient.Client
	// entry is true for the member the operator dialled, whose client the caller closes.
	entry bool
}

// ringMemberSet is every data member that can own a slot, in member id order: the order a dump
// writes their shares in, so two dumps of a quiet cluster are equal record for record whichever
// member they were taken through.
type ringMemberSet struct {
	members []ringReadMember
	epoch   int64
	// copies is the replication factor of each database the session may see when the set opened;
	// the owner of a slot depends on it, so a change is a membership change for the run.
	copies map[string]int32
	// seenBuckets holds the bucket names one fanned-out ShowBuckets listing has returned, for the
	// one listing in progress. A name twice means the members' shares overlapped, which a fixed
	// epoch rules out; it is refused rather than merged, because documents would be doubled too.
	seenBuckets map[string]bool
	// voters and dataMembers are the cluster's committed voter count and its ranked data
	// members: a database of more than one copy needs three voters, and each copy a data member
	// (cluster/fsm.go, the RF_UNSUPPORTED rule), so restore checks a dump's copies against them
	// before its first create.
	voters, dataMembers int
}

// ringMemberDial is what a run needs to reach the other members: the open connection, the dial
// options it was made with (user, password, trust), the --member addresses and where the
// insecure warnings go.
type ringMemberDial struct {
	entry     *ringclient.Client
	options   ringclient.Options
	addresses map[string]string
	notice    io.Writer
	// verb is "dump" or "restore", for the texts.
	verb string
}

// ringMemberAddresses is the repeatable --member id=host:port flag.
type ringMemberAddresses map[string]string

func (m *ringMemberAddresses) String() string {
	pairs := make([]string, 0, len(*m))
	for id, addr := range *m {
		pairs = append(pairs, id+"="+addr)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// Set adds one member's client address.
func (m *ringMemberAddresses) Set(value string) error {
	id, addr, ok := strings.Cut(value, "=")
	if !ok || id == "" || addr == "" {
		return fmt.Errorf("--member takes <node id>=<host:port>, for example --member node2=db2.example:7433, and got %q", value)
	}
	if *m == nil {
		*m = ringMemberAddresses{}
	}
	(*m)[id] = addr
	return nil
}

// ringMemberFlagUsage is the help line of --member, shared by dump and restore.
const ringMemberFlagUsage = "the client address of a cluster member that publishes none, as <node id>=<host:port>; repeat for each such member (run 'nodes' to see the ids)"

// openRingMembers learns the members from the entry connection and connects to every one that
// can own a slot. scope names the databases the run reads (all it may see when empty); a slot of
// one of them whose every copy is down is refused here, because no member answers for it.
func openRingMembers(ctx context.Context, dial ringMemberDial, scope []string) (*ringMemberSet, error) {
	view, err := dial.entry.Topology(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("reading the cluster's members from %s: %w", dial.options.Addr, err)
	}
	self, err := dial.entry.DescribeNode(ctx)
	if err != nil {
		return nil, fmt.Errorf("asking %s which member it is: %w", dial.options.Addr, err)
	}
	set := &ringMemberSet{epoch: view.Epoch, copies: map[string]int32{}, voters: int(view.Voters), dataMembers: len(view.Members)}
	for _, database := range view.Databases {
		set.copies[database.Name] = database.RF
	}
	if err := checkMemberFlags(view, dial); err != nil {
		return nil, err
	}
	if err := checkEverySlotHasAnOwner(view, scope, dial.verb); err != nil {
		return nil, err
	}
	for _, member := range view.Members {
		if member.State == "down" {
			continue
		}
		if member.ID == self.NodeID {
			set.members = append(set.members, ringReadMember{id: member.ID, addr: dial.options.Addr, client: dial.entry, entry: true})
			continue
		}
		opened, err := dialRingMember(ctx, dial, member, view.Epoch)
		if err != nil {
			set.close()
			return nil, err
		}
		set.members = append(set.members, opened)
	}
	if len(set.members) == 0 {
		// A node that holds no cluster table at all answers for every slot itself.
		set.members = []ringReadMember{{id: self.NodeID, addr: dial.options.Addr, client: dial.entry, entry: true}}
	}
	return set, nil
}

// checkMemberFlags refuses a --member that names no data member, so a typing error is not
// silently ignored while the member it meant is refused for having no address.
func checkMemberFlags(view *ringclient.Topology, dial ringMemberDial) error {
	ids := make([]string, 0, len(view.Members))
	for _, member := range view.Members {
		ids = append(ids, member.ID)
	}
	for id := range dial.addresses {
		if !slices.Contains(ids, id) {
			return &RingError{Exit: ExitUsage, Message: fmt.Sprintf("--member names %q, which is not a data member of this cluster: its data members are %s. Run 'nodes' to see them, and give --member as <node id>=<host:port>",
				terminalSafeText(id), terminalSafeText(strings.Join(ids, ", ")))}
		}
	}
	return nil
}

// checkEverySlotHasAnOwner refuses a database in scope with a slot whose every copy is on a
// member that is down: its documents are readable through no member, so a file without them
// would look complete.
func checkEverySlotHasAnOwner(view *ringclient.Topology, scope []string, verb string) error {
	var down []string
	for _, member := range view.Members {
		if member.State == "down" {
			down = append(down, member.ID)
		}
	}
	if len(down) == 0 {
		return nil
	}
	var unreadable []string
	for _, database := range view.Databases {
		if len(scope) > 0 && !slices.Contains(scope, database.Name) {
			continue
		}
		for slot := range uint16(storage.SlotCount) {
			if _, owned := view.Owner(database.Name, slot); !owned {
				unreadable = append(unreadable, fmt.Sprintf("%q", terminalSafeText(database.Name)))
				break
			}
		}
	}
	if len(unreadable) == 0 {
		return nil
	}
	downText := terminalSafeText(strings.Join(down, ", "))
	subject := "database " + unreadable[0] + " has"
	if len(unreadable) > 1 {
		subject = "databases " + strings.Join(unreadable, ", ") + " have"
	}
	return &RingError{Exit: ExitUnavailable, Message: fmt.Sprintf(
		"%s data whose every copy is on a member that is down (%s), so no member can answer for it and the %s would leave it out. Start %s again, wait until 'nodes' shows it active, and run the %s again; or name only the other databases with --db",
		subject, downText, verb, downText, verb)}
}

// dialRingMember connects to one member with the entry's user, password and trust, and checks
// that the server there is that member at the run's epoch.
func dialRingMember(ctx context.Context, dial ringMemberDial, member ringclient.Member, epoch int64) (ringReadMember, error) {
	addr := member.Addr
	if given, named := dial.addresses[member.ID]; named {
		addr = given
	}
	id := terminalSafeText(member.ID)
	if addr == "" {
		return ringReadMember{}, &RingError{Exit: ExitUsage, Message: fmt.Sprintf(
			"member %s publishes no client address (its client listener is on a loopback or wildcard address and tcp.advertise is not set), so the %s cannot reach it, and the data it owns can be read only there. Name its client address on the command: --member %s=<host:port>, once for each such member; or set HOARDDB_TCP_ADVERTISE on %s and restart it",
			id, dial.verb, id, id)}
	}
	options := dial.options
	options.Addr = addr
	if options.Insecure {
		_, _ = fmt.Fprint(dial.notice, insecureWarning(addr))
	}
	dialCtx, cancel := context.WithTimeout(ctx, ringDialTimeout)
	defer cancel()
	client, err := ringclient.Dial(dialCtx, options)
	if err != nil {
		return ringReadMember{}, memberDialRefusal(dial, member.ID, addr, err)
	}
	opened := ringReadMember{id: member.ID, addr: addr, client: client}
	if err := checkMemberIdentity(ctx, dial.verb, opened, epoch); err != nil {
		_ = client.Close()
		return ringReadMember{}, err
	}
	return opened, nil
}

// presentedFingerprint finds the fingerprint a refused certificate carried in a dial error.
var presentedFingerprint = regexp.MustCompile(`SHA256:[0-9a-f]{64}`)

// memberDialRefusal words a member that could not be reached, verified or logged in to. The
// member owns slots no other member answers for, so the run stops; it never retries the member
// with weaker trust.
func memberDialRefusal(dial ringMemberDial, id, addr string, err error) error {
	cause := terminalSafeText(strings.TrimPrefix(err.Error(), "ringclient: "))
	safeID, safeAddr := terminalSafeText(id), terminalSafeText(addr)
	if !dial.options.Insecure && (strings.Contains(cause, "is not pinned") || strings.Contains(cause, "does not match")) {
		presented := presentedFingerprint.FindString(cause)
		if presented == "" {
			presented = "a fingerprint you did not give"
		}
		return &RingError{Exit: ExitConnect, Err: err, Message: fmt.Sprintf(
			"member %s at %s presented a certificate that matches no -fingerprint given (it presented %s), and the %s never falls back to an unverified connection. Every member has its own certificate: pass -fingerprint once for each member, adding %s's (it prints it when it starts; check the one above against that line)",
			safeID, safeAddr, presented, dial.verb, safeID)}
	}
	exit := ringConnectExit(err)
	if exit == ExitAuth {
		return &RingError{Exit: exit, Err: err, Message: fmt.Sprintf(
			"member %s at %s refused the login: %s. The data that member owns can be read only there; check that the user exists on the cluster and that %s has finished joining ('nodes' shows it active), then run the %s again",
			safeID, safeAddr, cause, safeID, dial.verb)}
	}
	return &RingError{Exit: exit, Err: err, Message: fmt.Sprintf(
		"cannot reach member %s at %s: %s. The data that member owns can be read only there, so the %s stopped. Check that %s is running and reachable from here (run 'nodes'); if it listens elsewhere, name it with --member %s=<host:port>; then run the %s again",
		safeID, safeAddr, cause, dial.verb, safeID, safeID, dial.verb)}
}

// checkMemberIdentity refuses a server that is not the member the table names, or that is at
// another epoch: an address reused by another member would read one share twice and another not
// at all.
func checkMemberIdentity(ctx context.Context, verb string, member ringReadMember, epoch int64) error {
	node, err := member.client.DescribeNode(ctx)
	if err != nil {
		return fmt.Errorf("asking member %s at %s which member it is: %w", member.id, member.addr, err)
	}
	if node.NodeID != member.id {
		return &RingError{Exit: ExitUsage, Message: fmt.Sprintf(
			"the server at %s is member %s, not %s, so the %s would read one member's data twice. Run 'nodes' to see each member's address, and correct --member %s=<host:port>",
			terminalSafeText(member.addr), terminalSafeText(node.NodeID), terminalSafeText(member.id), verb, terminalSafeText(member.id))}
	}
	view, err := member.client.Topology(ctx, nil)
	if err != nil {
		return fmt.Errorf("reading the cluster's members from member %s at %s: %w", member.id, member.addr, err)
	}
	if view.Epoch != epoch {
		return membershipChangedError(verb, epoch, view.Epoch, member.id)
	}
	return nil
}

// membershipChangedError is the refusal of a run whose members are not all at one epoch: a slot
// that moved between two members' reads can be read twice or not at all.
func membershipChangedError(verb string, want, got int64, id string) error {
	return &RingError{Exit: ExitUnavailable, Message: fmt.Sprintf(
		"the cluster's membership changed while the %s ran (epoch %d at the start, %d on member %s), so a document whose owner moved could be read twice or not at all. Run the %s again when 'nodes' shows the same members on every node",
		verb, want, got, terminalSafeText(id), verb)}
}

// checkUnchanged asks every member for its view again: the same epoch, and the same copies for
// every database in scope, means every page of the run was read under one slot table.
func (s *ringMemberSet) checkUnchanged(ctx context.Context, verb string, scope []string) error {
	for _, member := range s.members {
		view, err := member.client.Topology(ctx, nil)
		if err != nil {
			return fmt.Errorf("reading the cluster's members from member %s again: %w", member.id, err)
		}
		if view.Epoch != s.epoch {
			return membershipChangedError(verb, s.epoch, view.Epoch, member.id)
		}
		for _, database := range view.Databases {
			was, known := s.copies[database.Name]
			if !known || was == database.RF || (len(scope) > 0 && !slices.Contains(scope, database.Name)) {
				continue
			}
			return &RingError{Exit: ExitUnavailable, Message: fmt.Sprintf(
				"database %q changed from %d to %d copies while the %s ran, which moves the owners of its data, so a document could be read twice or not at all. Run the %s again",
				terminalSafeText(database.Name), was, database.RF, verb, verb)}
		}
	}
	return nil
}

// close closes the connections the set opened; the entry's is the caller's.
func (s *ringMemberSet) close() {
	for _, member := range s.members {
		if !member.entry {
			_ = member.client.Close()
		}
	}
}

// fanoutCursor is a cursor over the members: the index of the member being paged, two bytes,
// then that member's own cursor.
func encodeFanoutCursor(member int, inner []byte) []byte {
	cursor := binary.BigEndian.AppendUint16(nil, uint16(member))
	return append(cursor, inner...)
}

func decodeFanoutCursor(cursor []byte, members int) (int, []byte, error) {
	if len(cursor) == 0 {
		return 0, nil, nil
	}
	if len(cursor) < 2 {
		return 0, nil, errors.New("the cursor is shorter than its member index")
	}
	member := int(binary.BigEndian.Uint16(cursor))
	if member >= members {
		return 0, nil, fmt.Errorf("the cursor names member %d of %d", member, members)
	}
	return member, cursor[2:], nil
}

// fanoutPage pages one listing across the members in order: a member's pages, then the next
// member's from its start. page.Cursor is a fanout cursor. A member whose share is empty is
// stepped over in the same call, so a page with More set holds entries unless it is the hand-over
// to the next member.
func fanoutPage[P any](ctx context.Context, s *ringMemberSet, page ringclient.PageRequest,
	call func(context.Context, *ringclient.Client, ringclient.PageRequest) (P, error),
	shape func(*P) (entries int, more bool, cursor *[]byte, moreFlag *bool)) (P, error) {
	var none P
	index, inner, err := decodeFanoutCursor(page.Cursor, len(s.members))
	if err != nil {
		return none, fmt.Errorf("continuing a listing across the cluster's members: %w", err)
	}
	for {
		request := page
		request.Cursor = inner
		got, err := call(ctx, s.members[index].client, request)
		if err != nil {
			return none, err
		}
		entries, more, cursor, moreFlag := shape(&got)
		switch {
		case more:
			*cursor = encodeFanoutCursor(index, *cursor)
			return got, nil
		case index+1 == len(s.members):
			*cursor, *moreFlag = nil, false
			return got, nil
		case entries > 0:
			*cursor, *moreFlag = encodeFanoutCursor(index+1, nil), true
			return got, nil
		}
		index, inner = index+1, nil
	}
}

// ShowBuckets is one page of the buckets of database across every member: each member lists the
// declarations it owns. A name listed twice is refused (see seenBuckets).
func (s *ringMemberSet) ShowBuckets(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.BucketPage, error) {
	if len(page.Cursor) == 0 {
		s.seenBuckets = map[string]bool{}
	}
	listed, err := fanoutPage(ctx, s, page, func(ctx context.Context, client *ringclient.Client, request ringclient.PageRequest) (ringclient.BucketPage, error) {
		return client.ShowBuckets(ctx, database, request)
	}, func(p *ringclient.BucketPage) (int, bool, *[]byte, *bool) {
		return len(p.Buckets), p.More, &p.Cursor, &p.More
	})
	if err != nil {
		return listed, err
	}
	for _, bucket := range listed.Buckets {
		if s.seenBuckets[bucket.Name] {
			return ringclient.BucketPage{}, &RingError{Exit: ExitUnavailable, Message: fmt.Sprintf(
				"bucket %q of database %q was listed by two members, so the members' shares overlap: the cluster's membership is changing. Run the same command again when 'nodes' shows the same members on every node",
				terminalSafeText(bucket.Name), terminalSafeText(database))}
		}
		s.seenBuckets[bucket.Name] = true
	}
	return listed, nil
}

// ListDocuments is one page of a bucket's documents across every member.
func (s *ringMemberSet) ListDocuments(ctx context.Context, database, bucket string, page ringclient.PageRequest) (ringclient.EntryPage, error) {
	return fanoutPage(ctx, s, page, func(ctx context.Context, client *ringclient.Client, request ringclient.PageRequest) (ringclient.EntryPage, error) {
		return ringClientSource{client: client}.ListDocuments(ctx, database, bucket, request)
	}, func(p *ringclient.EntryPage) (int, bool, *[]byte, *bool) {
		return len(p.Entries), p.More, &p.Cursor, &p.More
	})
}

// entryClient is the connection the operator dialled: declarations and database rows are the
// cluster's and read the same on every member.
func (s *ringMemberSet) entryClient() *ringclient.Client {
	for _, member := range s.members {
		if member.entry {
			return member.client
		}
	}
	return s.members[0].client
}

// DescribeDatabase reads a database's row through the entry member.
func (s *ringMemberSet) DescribeDatabase(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.DatabaseDescription, error) {
	return s.entryClient().DescribeDatabase(ctx, database, page)
}

// DescribeBucket reads a declaration through the entry member, which fetches it from its owner.
func (s *ringMemberSet) DescribeBucket(ctx context.Context, database, bucket string) (ringclient.BucketInfo, error) {
	return s.entryClient().DescribeBucket(ctx, database, bucket)
}

// ringFanoutSource is the dump's source over a member set: the listings fan out, the rest goes
// through the entry connection (a keyed store read is forwarded to its owner by the member).
type ringFanoutSource struct {
	ringClientSource
	members *ringMemberSet
}

// ShowBuckets lists every member's declarations.
func (s ringFanoutSource) ShowBuckets(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.BucketPage, error) {
	return s.members.ShowBuckets(ctx, database, page)
}

// ListDocuments lists every member's documents of the bucket.
func (s ringFanoutSource) ListDocuments(ctx context.Context, database, bucket string, page ringclient.PageRequest) (ringclient.EntryPage, error) {
	return s.members.ListDocuments(ctx, database, bucket, page)
}

// checkCopies refuses, before anything is created, a database of the dump the target cannot hold
// at its number of copies; the create would be refused RF_UNSUPPORTED after the databases before
// it in the file were already made.
func (s *ringMemberSet) checkCopies(dumpName string, copies int32) error {
	if copies <= 1 || (s.voters >= 3 && int(copies) <= s.dataMembers) {
		return nil
	}
	return restoreRefusal("Database %q in the dump has %d copies, and this cluster cannot hold them: it has %s and %s, and more than one copy needs at least 3 voters and one data member per copy. Nothing was written. Restore into a cluster with enough members (join them before the restore), or restore the other databases one at a time: hoarddb restore <file> --db <name>",
		terminalSafeText(dumpName), copies, countNoun(s.voters, "voter"), countNoun(s.dataMembers, "data member"))
}

// countNoun is "1 voter" or "3 voters".
func countNoun(count int, noun string) string {
	if count == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", count, noun)
}
