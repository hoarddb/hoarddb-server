package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// linkBucketKey names a bucket of the dump by the names the dump holds, before --as.
type linkBucketKey struct{ db, name string }

// restoreLink is one field of a bucket that is 'of' another bucket of the same run.
type restoreLink struct {
	field  string
	target linkBucketKey
}

// restoreLinkPlan is what pass 2 learned about links between buckets. It holds one record
// per bucket that declares or is the target of a link in the run, never one per bucket of the
// dump (AGENTS.md rule 11), and nothing for the buckets that no link touches.
type restoreLinkPlan struct {
	// order lists, per database that no cross-database link touches, its linked buckets with
	// targets first.
	order map[string][]*dumpBucket
	// group is the set of databases that link into each other across databases. They cannot be
	// restored one at a time (each has a link into another), so a restore creates all of them,
	// then every linked bucket of theirs in one order across databases (groupOrder, targets
	// first), and only then fills them in file order. groupDatabases holds their db records in
	// file order.
	group          map[string]bool
	groupOrder     []*dumpBucket
	groupDatabases []*dumpDB
}

// linked returns the buckets of one database that must be created ahead of their place in
// the file, targets first. A database of the group has none here: the group creates its own.
func (p *restoreLinkPlan) linked(dumpDatabase string) []*dumpBucket {
	if p == nil {
		return nil
	}
	return p.order[dumpDatabase]
}

// linkTargetOf splits a field's 'of' into the database and bucket it names: "<Bucket>" is a
// bucket of the record's own database and "<Database>.<Bucket>" a bucket of that database,
// the split the server makes (cluster/wire/createb_refs.go:Node.resolveReferences).
func linkTargetOf(record *dumpBucket, of string) linkBucketKey {
	database, bucket, qualified := strings.Cut(of, ".")
	if !qualified {
		return linkBucketKey{db: record.DB, name: of}
	}
	return linkBucketKey{db: database, name: bucket}
}

// restoreOf is the 'of' a field is created with on the target: a link written
// "<Database>.<Bucket>" that names the one database --as renames follows it to its new name.
func (f *ringRestoreFlags) restoreOf(of string) string {
	database, bucket, qualified := strings.Cut(of, ".")
	if qualified && f.as != "" && database == f.database {
		return f.as + "." + bucket
	}
	return of
}

// linkCollector reads the bucket records of the selected databases in pass 2 and keeps the
// ones that declare a link.
type linkCollector struct {
	flags *ringRestoreFlags
	// dumpDatabases is every database the dump holds, selected or not, and selectedOrder the
	// position of each selected one in the file.
	dumpDatabases map[string]bool
	selectedOrder map[string]int
	// selectedRecords are the db records of the selected databases, in file order.
	selectedRecords []*dumpDB
	// declaring holds the buckets that have a field 'of' another bucket than themselves.
	declaring map[linkBucketKey]*dumpBucket
}

func newLinkCollector(flags *ringRestoreFlags) *linkCollector {
	return &linkCollector{
		flags:         flags,
		dumpDatabases: map[string]bool{},
		selectedOrder: map[string]int{},
		declaring:     map[linkBucketKey]*dumpBucket{},
	}
}

// addDatabase notes a db record of the dump.
func (c *linkCollector) addDatabase(record *dumpDB) {
	c.dumpDatabases[record.Name] = true
	if c.flags.selects(record.Name) {
		c.selectedOrder[record.Name] = len(c.selectedOrder)
		c.selectedRecords = append(c.selectedRecords, record)
	}
}

// addBucket notes a bucket of a selected database. A link to a bucket that was dropped
// before the dump (#<id>) can never be rebuilt, so it is refused here, before anything is
// created: a bucket made without its link would be a different bucket.
func (c *linkCollector) addBucket(record *dumpBucket) error {
	for _, field := range record.Fields {
		if strings.HasPrefix(field.Of, "#") {
			return restoreRefusal("Bucket %q field %q links to a bucket that was dropped before this dump was taken (%s), so the link cannot be recreated. Nothing was written. Restore is refused for this dump: run 'alter bucket %s in %s { drop %s }' on the source to remove the field, take a new dump with 'hoarddb dump', and restore that.",
				terminalSafeText(record.Name), terminalSafeText(field.Name), terminalSafeText(field.Of),
				terminalSafeText(record.Name), terminalSafeText(record.DB), terminalSafeText(field.Name))
		}
		if field.Of != "" && linkTargetOf(record, field.Of) != (linkBucketKey{db: record.DB, name: record.Name}) {
			c.declaring[linkBucketKey{db: record.DB, name: record.Name}] = record
		}
	}
	return nil
}

// links lists the fields of a bucket that are 'of' a bucket other than itself, in field order.
func linksOf(record *dumpBucket) []restoreLink {
	var links []restoreLink
	for _, field := range record.Fields {
		if field.Of == "" {
			continue
		}
		target := linkTargetOf(record, field.Of)
		if target == (linkBucketKey{db: record.DB, name: record.Name}) {
			continue
		}
		links = append(links, restoreLink{field: field.Name, target: target})
	}
	return links
}

// resolve decides every link before the first create (section 3.4, pass 2). A target in a
// database this run restores must be in the dump, and in the same database or in one that
// the file holds earlier, so that it exists when the link is created; a target anywhere else
// must be an existing bucket of the target ring. It returns the order the linked buckets of
// each database are created in, or the refusal that names the fix.
func (c *linkCollector) resolve(ctx context.Context, source restoreTargetSource, path string) (*restoreLinkPlan, error) {
	declaring := c.sortedDeclaring()
	wanted := map[linkBucketKey]bool{}
	for _, record := range declaring {
		for _, link := range linksOf(record) {
			database := link.target.db
			if _, inRun := c.selectedOrder[database]; inRun {
				wanted[link.target] = true
				continue
			}
			if err := c.checkTargetOnRing(ctx, source, record, link); err != nil {
				return nil, err
			}
		}
	}
	found, err := c.findTargets(path, wanted)
	if err != nil {
		return nil, err
	}
	for _, record := range declaring {
		for _, link := range linksOf(record) {
			if _, inRun := c.selectedOrder[link.target.db]; inRun && found[link.target] == nil {
				return nil, restoreRefusal("Bucket %q field %q of database %q links to bucket %q, which is not in this dump, so it cannot exist on the target when the link is created. Nothing was written. Restore is refused for this dump: take a new dump of the whole database with 'hoarddb dump' and restore that.",
					terminalSafeText(record.Name), terminalSafeText(link.field), terminalSafeText(record.DB), terminalSafeText(link.target.db+"."+link.target.name))
			}
		}
	}
	return c.orderLinkedBuckets(declaring, found)
}

// sortedDeclaring returns the declaring buckets in a fixed order, so a refusal is the same on
// every run.
func (c *linkCollector) sortedDeclaring() []*dumpBucket {
	records := make([]*dumpBucket, 0, len(c.declaring))
	for _, record := range c.declaring {
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].DB != records[j].DB {
			return c.selectedOrder[records[i].DB] < c.selectedOrder[records[j].DB]
		}
		return records[i].Name < records[j].Name
	})
	return records
}

// checkTargetOnRing refuses a link whose target is not a bucket of the target ring. A session
// without the privilege to describe it counts as absent, as pass 2 does for a database, and
// the create that follows reports the real reason.
func (c *linkCollector) checkTargetOnRing(ctx context.Context, source restoreTargetSource, record *dumpBucket, link restoreLink) error {
	_, err := retryQuorumCtx(ctx, func() (ringclient.BucketInfo, error) {
		return source.DescribeBucket(ctx, link.target.db, link.target.name)
	})
	if err == nil {
		return nil
	}
	var refusal *ringclient.Error
	if !errors.As(err, &refusal) || (refusal.Code != cluster.CodeNotFound && refusal.Code != cluster.CodePermissionDenied) {
		return fmt.Errorf("checking that bucket %q exists in database %q: %w", link.target.name, link.target.db, err)
	}
	fix := "Create it on the target ring first, then run restore again."
	if c.dumpDatabases[link.target.db] {
		fix = fmt.Sprintf("The dump holds database %s: restore it first with hoarddb restore %s --db %s, then run this restore again. Databases whose buckets link into each other cannot be restored one at a time: restore them together, without --db.",
			terminalSafeText(link.target.db), c.flags.file, terminalSafeText(link.target.db))
	}
	return restoreRefusal("Bucket %q field %q of database %q links to bucket %q, which does not exist on the target ring and is not restored by this run. Nothing was written. %s",
		terminalSafeText(record.Name), terminalSafeText(link.field), terminalSafeText(record.DB), terminalSafeText(link.target.db+"."+link.target.name), fix)
}

// findTargets reads the bucket records of the dump once more and returns, for each wanted
// key, its record: the links of the declaring buckets were kept from the first read, so only
// the targets that declare nothing are read here.
func (c *linkCollector) findTargets(path string, wanted map[linkBucketKey]bool) (map[linkBucketKey]*dumpBucket, error) {
	found := map[linkBucketKey]*dumpBucket{}
	missing := 0
	for key := range wanted {
		if record, ok := c.declaring[key]; ok {
			found[key] = record
			continue
		}
		missing++
	}
	if missing == 0 {
		return found, nil
	}
	skimmer, err := openDumpSkimmer(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = skimmer.close() }()
	for {
		record, _, err := skimmer.next()
		if errors.Is(err, io.EOF) {
			return found, nil
		}
		if err != nil {
			return nil, err
		}
		if bucket, ok := record.(*dumpBucket); ok {
			key := linkBucketKey{db: bucket.DB, name: bucket.Name}
			if wanted[key] && found[key] == nil {
				found[key] = bucket
			}
		}
	}
}

// orderLinkedBuckets sorts the buckets that take part in a link, across databases, so that
// each comes after the buckets it links to. A circle cannot be created in any order, and the
// ring cannot make one (a bucket may be 'of' only a bucket that exists when it is created, and
// alter cannot add a reference), so a dump that holds one is refused with the buckets named.
// Databases that link into each other across databases form the group, which a restore creates
// together; the other databases keep their own order.
func (c *linkCollector) orderLinkedBuckets(declaring []*dumpBucket, found map[linkBucketKey]*dumpBucket) (*restoreLinkPlan, error) {
	plan := &restoreLinkPlan{order: map[string][]*dumpBucket{}, group: map[string]bool{}}
	const (
		unseen = iota
		inProgress
		done
	)
	state := map[linkBucketKey]int{}
	var global []*dumpBucket
	var visit func(record *dumpBucket, path []string) error
	visit = func(record *dumpBucket, path []string) error {
		key := linkBucketKey{db: record.DB, name: record.Name}
		state[key] = inProgress
		path = append(path, record.DB+"."+record.Name)
		for _, link := range linksOf(record) {
			target := found[link.target]
			if target == nil {
				continue // a bucket of the target ring, outside the run
			}
			if link.target.db != record.DB {
				plan.group[record.DB], plan.group[link.target.db] = true, true
			}
			switch state[link.target] {
			case inProgress:
				circle := path
				for position, name := range path {
					if name == link.target.db+"."+link.target.name {
						circle = path[position:]
						break
					}
				}
				return restoreRefusal("Restore refused: buckets %s link to each other in a circle, and a bucket can only link to a bucket that already exists, so they cannot be created. Nothing was written. A dump the ring wrote never holds a circle: check that this file is the one 'hoarddb dump' wrote.",
					terminalSafeText(strings.Join(quoteAll(circle), ", ")))
			case unseen:
				if err := visit(target, path); err != nil {
					return err
				}
			}
		}
		state[key] = done
		global = append(global, record)
		return nil
	}
	for _, record := range declaring {
		if state[linkBucketKey{db: record.DB, name: record.Name}] == unseen {
			if err := visit(record, nil); err != nil {
				return nil, err
			}
		}
	}
	for _, record := range global {
		if plan.group[record.DB] {
			plan.groupOrder = append(plan.groupOrder, record)
			continue
		}
		plan.order[record.DB] = append(plan.order[record.DB], record)
	}
	for _, record := range c.selectedRecords {
		if plan.group[record.Name] {
			plan.groupDatabases = append(plan.groupDatabases, record)
		}
	}
	return plan, nil
}

// quoteAll wraps each name in double quotes.
func quoteAll(names []string) []string {
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = fmt.Sprintf("%q", name)
	}
	return quoted
}
