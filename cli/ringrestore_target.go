package cli

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// restoreListedNames is how many bucket or database names a refusal lists before it says
// "and N more": a target with a million buckets must not print a million names.
const restoreListedNames = 10

// restoreSkimPrefixBytes is how much of a record the skimmer reads to learn its kind: the
// length prefix, the t member's type, its key and a string no longer than the longest kind
// word, which is under this.
const restoreSkimPrefixBytes = 32

// dumpSkimmer walks the records of a dump that pass 1 has verified and hands back only the
// db and bucket records, seeking past every other one (section 3.4, pass 2): a record
// starts with its own length, so a skip costs one small read and no allocation however
// large the entity is. It keeps an offset and one record, never the file.
type dumpSkimmer struct {
	file   *os.File
	offset int64
	size   int64
}

// openDumpSkimmer opens path and positions the skimmer at the first record, after the
// magic. The file is the one pass 1 verified, so a bad magic here means it changed.
func openDumpSkimmer(path string) (*dumpSkimmer, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening the dump %s: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("reading the size of the dump %s: %w", path, err), file.Close())
	}
	magic := make([]byte, len(dumpMagic))
	if _, err := file.ReadAt(magic, 0); err != nil || string(magic) != dumpMagic {
		return nil, errors.Join(fmt.Errorf("the dump %s changed after it was verified: it no longer starts with its magic. Run the restore again", path), file.Close())
	}
	return &dumpSkimmer{file: file, offset: int64(len(dumpMagic)), size: info.Size()}, nil
}

// close releases the file.
func (s *dumpSkimmer) close() error { return s.file.Close() }

// next returns the next db or bucket record, with the offset where it started. It returns
// io.EOF at the summary, where the records a restore applies end. A file that no longer
// reads as it did in pass 1 is an error that says so.
func (s *dumpSkimmer) next() (dumpRecord, int64, error) {
	for {
		start := s.offset
		prefix := make([]byte, restoreSkimPrefixBytes)
		read, err := s.file.ReadAt(prefix, start)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, start, fmt.Errorf("reading the dump at offset %d: %w", start, err)
		}
		prefix = prefix[:read]
		if len(prefix) < 4 {
			return nil, start, io.EOF
		}
		length := int64(int32(binary.LittleEndian.Uint32(prefix)))
		if length < dumpRecordMinBytes || length > maxDumpRecordBytes || start+length > s.size {
			return nil, start, fmt.Errorf("the dump changed after it was verified: the record at offset %d declares %d bytes. Run the restore again", start, length)
		}
		kind, err := readRecordKind(prefix[:min(int64(len(prefix)), length)])
		if err != nil {
			return nil, start, fmt.Errorf("the dump changed after it was verified: the record at offset %d cannot be read: %w", start, err)
		}
		s.offset = start + length
		switch kind {
		case dumpKindSummary, dumpKindFooter:
			return nil, start, io.EOF
		case dumpKindDB, dumpKindBucket:
			body := make([]byte, length)
			if _, err := s.file.ReadAt(body, start); err != nil {
				return nil, start, fmt.Errorf("reading the %s record at offset %d: %w", kind, start, err)
			}
			record, err := decodeRecord(body)
			if err != nil {
				return nil, start, fmt.Errorf("the dump changed after it was verified: the %s record at offset %d does not decode: %w", kind, start, err)
			}
			return record, start, nil
		}
	}
}

// restoreTargetSource is the two read calls pass 2 makes of the ring, so a test can answer
// them without a server (a bucket that is dropping cannot be held still on a live one).
type restoreTargetSource interface {
	// DescribeDatabase is a database's replication factor and storage.
	DescribeDatabase(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.DatabaseDescription, error)
	// ShowBuckets is one page of the buckets of database, with their state.
	ShowBuckets(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.BucketPage, error)
	// DescribeBucket is one bucket's declaration, read right before its create.
	DescribeBucket(ctx context.Context, database, bucket string) (ringclient.BucketInfo, error)
}

// restoreCopiesChecker is a target that knows how many copies it can hold (ringMemberSet), so a
// dump database it cannot hold is refused in pass 2, before the first create.
type restoreCopiesChecker interface {
	checkCopies(dumpName string, copies int32) error
}

// restoreBucketListPage is how many buckets one ShowBuckets page asks for in pass 2.
const restoreBucketListPage = 500

// restoreTargetState is what pass 2 learned about one database of the target: whether it
// exists, and how many buckets are active or still dropping, with the first few names of
// each. It never holds more than restoreListedNames names, whatever the target holds.
type restoreTargetState struct {
	exists        bool
	active        int64
	dropping      int64
	activeNames   []string
	droppingNames []string
}

// readTargetState reads database from the target. A database that does not exist and one
// the session has no role on answer the same way (ringclient.Client.DescribeDatabase), so
// both count as absent here and the create that follows reports the real reason before
// anything is written.
func readTargetState(ctx context.Context, source restoreTargetSource, database string) (restoreTargetState, ringclient.DatabaseDescription, error) {
	description, err := retryQuorumCtx(ctx, func() (ringclient.DatabaseDescription, error) {
		return source.DescribeDatabase(ctx, database, ringclient.PageRequest{Limit: 1})
	})
	if err != nil {
		var refusal *ringclient.Error
		if errors.As(err, &refusal) && (refusal.Code == cluster.CodePermissionDenied || refusal.Code == cluster.CodeNotFound) {
			return restoreTargetState{}, description, nil
		}
		return restoreTargetState{}, description, fmt.Errorf("reading database %q on the target: %w", database, err)
	}
	state := restoreTargetState{exists: true}
	var cursor []byte
	for {
		page, err := retryQuorumCtx(ctx, func() (ringclient.BucketPage, error) {
			return source.ShowBuckets(ctx, database, ringclient.PageRequest{Limit: restoreBucketListPage, Cursor: cursor})
		})
		if err != nil {
			return state, description, fmt.Errorf("listing the buckets of database %q on the target: %w", database, err)
		}
		for _, bucket := range page.Buckets {
			if bucket.State == dumpBucketDroppingState {
				state.dropping++
				if len(state.droppingNames) < restoreListedNames {
					state.droppingNames = append(state.droppingNames, bucket.Name)
				}
				continue
			}
			state.active++
			if len(state.activeNames) < restoreListedNames {
				state.activeNames = append(state.activeNames, bucket.Name)
			}
		}
		if !page.More {
			return state, description, nil
		}
		cursor = page.Cursor
	}
}

// restoreTargetName is the name a dump's database gets on the target: --as for the one
// database --db selects, the dump's own name otherwise.
func (f *ringRestoreFlags) restoreTargetName(dumpName string) string {
	if f.as != "" && dumpName == f.database {
		return f.as
	}
	return dumpName
}

// selects reports whether the dump's database is one this run restores.
func (f *ringRestoreFlags) selects(dumpName string) bool {
	return f.database == "" || f.database == dumpName
}

// restoreRefusal is a pass-2 refusal: nothing was written, exit 2, and the text names what
// does exist and the command that fixes it (AGENTS.md rule 3).
func restoreRefusal(format string, args ...any) *RingError {
	return &RingError{Exit: ExitRefused, Message: fmt.Sprintf(format, args...)}
}

// quotedNames renders at most restoreListedNames names, then "and N more".
func quotedNames(names []string, total int64) string {
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = terminalSafeText(name)
	}
	text := strings.Join(quoted, ", ")
	if extra := total - int64(len(names)); extra > 0 {
		text += fmt.Sprintf(", and %s more", formatCount(extra))
	}
	return text
}

// checkTargets is pass 2: it reads the dump's db and bucket records and the target, writes
// nothing, and refuses, before anything is created, a database that is not empty, one that
// is still dropping buckets, one with another replication factor or storage, a bucket
// that links to a bucket that cannot exist when the link is created, and a dump that does
// not hold the database --db names. It keeps no list of what the dump holds, only the
// buckets that take part in a link: every other record is checked as it is read.
func (r *ringRestoreRun) checkTargets() error {
	plan, err := planRestoreTargets(r.ctx, r.flags, r.targetSource(), r.flags.file)
	r.links = plan
	return err
}

// checkRestoreTargets is checkTargets over any source, so a test can drive it.
func checkRestoreTargets(ctx context.Context, flags *ringRestoreFlags, source restoreTargetSource, path string) error {
	_, err := planRestoreTargets(ctx, flags, source, path)
	return err
}

// planRestoreTargets is pass 2: it refuses what cannot be restored and returns the order the
// linked buckets of each database are created in.
func planRestoreTargets(ctx context.Context, flags *ringRestoreFlags, source restoreTargetSource, path string) (*restoreLinkPlan, error) {
	skimmer, err := openDumpSkimmer(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = skimmer.close() }()

	collector := newLinkCollector(flags)
	var (
		inScope      bool
		sawSelected  bool
		listedNames  []string
		databaseSeen int64
	)
	for {
		record, _, err := skimmer.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		switch record := record.(type) {
		case *dumpDB:
			collector.addDatabase(record)
			databaseSeen++
			if len(listedNames) < restoreListedNames {
				listedNames = append(listedNames, record.Name)
			}
			inScope = flags.selects(record.Name)
			if !inScope {
				continue
			}
			sawSelected = true
			if err := checkRestoreDatabase(ctx, flags, source, record); err != nil {
				return nil, err
			}
		case *dumpBucket:
			if !flags.selects(record.DB) {
				continue
			}
			if err := collector.addBucket(record); err != nil {
				return nil, err
			}
		}
	}
	if flags.database != "" && !sawSelected {
		return nil, restoreRefusal("Database %q is not in this dump. The dump holds %s. Pick one of those: hoarddb restore %s --db %s",
			terminalSafeText(flags.database), quotedNames(listedNames, databaseSeen), flags.file, terminalSafeText(firstOrEmpty(listedNames)))
	}
	return collector.resolve(ctx, source, path)
}

func firstOrEmpty(names []string) string {
	if len(names) == 0 {
		return "<name>"
	}
	return names[0]
}

// checkRestoreDatabase decides one database of the dump against the target.
func checkRestoreDatabase(ctx context.Context, flags *ringRestoreFlags, source restoreTargetSource, record *dumpDB) error {
	target := flags.restoreTargetName(record.Name)
	state, description, err := readTargetState(ctx, source, target)
	if err != nil {
		return err
	}
	if !state.exists {
		if capacity, ok := source.(restoreCopiesChecker); ok {
			return capacity.checkCopies(record.Name, record.RF)
		}
		return nil
	}
	renameCommand := fmt.Sprintf("hoarddb restore %s --db %s --as %s", flags.file, terminalSafeText(record.Name), terminalSafeText(target+"_restored"))
	if description.RF != record.RF || storageWord(description.Storage) != storageWord(record.Storage) {
		return restoreRefusal("Database %q exists with replication factor %d and storage %s; the dump has replication factor %d and storage %s. Nothing was written. Restore into a new name: %s",
			terminalSafeText(target), description.RF, storageWord(description.Storage), record.RF, storageWord(record.Storage), renameCommand)
	}
	switch {
	case state.active > 0:
		return restoreRefusal("%s", nonEmptyTargetText(target, state, renameCommand))
	case state.dropping > 0:
		return restoreRefusal("Restore refused: database %q still has %s that %s being dropped (%s). A bucket's name is free again only when its drop has finished. Nothing was written. Run 'show buckets in %s' until it lists nothing, then run restore again, or restore into a new name: %s",
			terminalSafeText(target), bucketCount(state.dropping), isOrAre(state.dropping), quotedNames(state.droppingNames, state.dropping), terminalSafeText(target), renameCommand)
	}
	return nil
}

// bucketCount renders "1 bucket" and "3 buckets".
func bucketCount(count int64) string {
	if count == 1 {
		return "1 bucket"
	}
	return formatCount(count) + " buckets"
}

// isOrAre is the verb that agrees with count.
func isOrAre(count int64) string {
	if count == 1 {
		return "is"
	}
	return "are"
}

// storageWord normalises the empty storage word to the default, which is disk.
func storageWord(word string) string {
	if word == "" {
		return "disk"
	}
	return word
}

// nonEmptyTargetText is the refusal of a database that already holds buckets: the one
// section 3.4 fixes, with the drop commands for the buckets that are active, and the wait
// line for those that are still dropping. A drop database statement does not exist on the ring, so
// the text says the buckets are dropped one by one and never quotes it as a command (a quoted
// statement must parse: TestOneGrammarForEveryQuotedFix).
func nonEmptyTargetText(target string, state restoreTargetState, renameCommand string) string {
	var text strings.Builder
	fmt.Fprintf(&text, "Restore refused: database %q already holds %s (%s), and restore never merges into existing data. Nothing was written.\n",
		terminalSafeText(target), bucketCount(state.active), quotedNames(state.activeNames, state.active))
	fmt.Fprintf(&text, "Restore into a new name:  %s\n", renameCommand)
	drops := make([]string, len(state.activeNames))
	for index, name := range state.activeNames {
		drops[index] = fmt.Sprintf("drop bucket %s in %s", terminalSafeText(name), terminalSafeText(target))
	}
	more := ""
	if extra := state.active - int64(len(state.activeNames)); extra > 0 {
		more = fmt.Sprintf(";  and %s more", formatCount(extra))
	}
	fmt.Fprintf(&text, "Or empty %q first, then wait until 'show buckets in %s' lists nothing:  %s%s\n",
		terminalSafeText(target), terminalSafeText(target), strings.Join(drops, ";  "), more)
	if state.dropping > 0 {
		fmt.Fprintf(&text, "%s more buckets are still being dropped (%s); a name is free again only when its drop has finished.\n",
			formatCount(state.dropping), quotedNames(state.droppingNames, state.dropping))
	}
	text.WriteString("(There is no drop database statement yet, so the buckets are dropped one by one.)")
	return text.String()
}

// targetSource is the target the pass-2 reads go to: the client, unless a test replaced it.
func (r *ringRestoreRun) targetSource() restoreTargetSource {
	if r.target != nil {
		return r.target
	}
	return r.client
}

// restoreBeforeBucketCreate is a seam for tests, nil in the binary: it runs between the check
// that a bucket is absent and its create, the one-round-trip window the check cannot close.
var restoreBeforeBucketCreate func()

// checkDatabaseStillEmpty is pass 2's decision for one database, taken again right before the
// database is created. An equal create of an existing database succeeds, so without this
// check a database made by another writer after pass 2, or one that gained buckets, would be
// adopted and merged into. It refuses with exit 2 and the --as command.
func (r *ringRestoreRun) checkDatabaseStillEmpty(flags *ringRestoreFlags, record *dumpDB) error {
	target := flags.restoreTargetName(record.Name)
	state, _, err := readTargetState(r.ctx, r.targetSource(), target)
	if err != nil {
		return err
	}
	if state.active == 0 && state.dropping == 0 {
		return nil
	}
	return restoreRefusal("Restore refused: database %q gained %s (%s) after the restore began, so something else is writing to it, and restore never merges into existing data. Nothing of this database was written by the restore. %s",
		terminalSafeText(target), bucketCount(state.active+state.dropping), quotedNames(append(append([]string{}, state.activeNames...), state.droppingNames...), state.active+state.dropping),
		r.renameAdvice(flags, record.Name, target))
}

// checkBucketAbsent asks the target for the bucket right before its create. CreateBucket of an
// equal declaration on an existing bucket succeeds, and nothing tells a created bucket from an
// adopted one, so this read is what keeps a restore from filling a bucket an application made
// after pass 2. The window that remains is the one round trip between this read and the create.
// A describe that fails for any reason but "no such bucket" or "no privilege" (a lost connection,
// an overloaded node) stops the restore with that failure's own exit: absence was not confirmed,
// and creating on an unconfirmed bucket could adopt one. A session without the privilege to
// describe is treated as absent, as pass 2 does for a database, and so has no protection here.
func (r *ringRestoreRun) checkBucketAbsent(flags *ringRestoreFlags, dumpDatabase, bucket, target string) error {
	_, err := r.targetSource().DescribeBucket(r.ctx, target, bucket)
	if err == nil {
		return restoreRefusal("Restore refused: bucket %q already exists in database %q (it appeared after the restore began, so something else is writing to it), and restore never merges into existing data. Nothing was written into it. %s",
			terminalSafeText(bucket), terminalSafeText(target), r.renameAdvice(flags, dumpDatabase, target))
	}
	var refusal *ringclient.Error
	if errors.As(err, &refusal) && (refusal.Code == cluster.CodeNotFound || refusal.Code == cluster.CodePermissionDenied) {
		return nil
	}
	return fmt.Errorf("checking that bucket %q is absent from database %q: %w", bucket, target, err)
}

// checkBucketStillEmpty refuses a bucket created ahead for a link that holds a document when
// the restore reaches it: an application can write to it from the moment it exists. It reads
// one entry. The window that remains is the round trip between this read and the first insert.
func (r *ringRestoreRun) checkBucketStillEmpty(flags *ringRestoreFlags, dumpDatabase, bucket, target string) error {
	list := func(ctx context.Context) (ringclient.EntryPage, error) {
		return r.client.Bucket(target, bucket).List(ctx, nil, ringclient.PageRequest{Limit: 1})
	}
	if r.members != nil {
		list = func(ctx context.Context) (ringclient.EntryPage, error) {
			return r.members.ListDocuments(ctx, target, bucket, ringclient.PageRequest{Limit: 1})
		}
	}
	page, err := list(r.ctx)
	if err != nil {
		return fmt.Errorf("checking that bucket %q of database %q is still empty: %w", bucket, target, err)
	}
	if len(page.Entries) == 0 {
		return nil
	}
	return restoreRefusal("Restore refused: bucket %q in database %q was created empty for a link and now holds a document (written by something else while the restore ran), and restore never merges into existing data. Nothing of this bucket was written by the restore. %s",
		terminalSafeText(bucket), terminalSafeText(target), r.renameAdvice(flags, dumpDatabase, target))
}

// renameAdvice is the sentence that follows a refusal after the restore began: restore under
// another name. For a database that links into others (or is linked into) that command
// would leave them linked to the buckets left behind, so the advice is to restore the group
// together instead.
func (r *ringRestoreRun) renameAdvice(flags *ringRestoreFlags, dumpDatabase, target string) string {
	if r.links != nil && r.links.group[dumpDatabase] {
		return "Databases that link into each other can only be restored together: drop what the restore made in each of them, wait until 'show buckets' lists nothing, and run the same restore again."
	}
	return fmt.Sprintf("Restore into a new name: hoarddb restore %s --db %s --as %s", flags.file, terminalSafeText(dumpDatabase), terminalSafeText(target+"_restored"))
}

// checkGroupDatabaseUntouched refuses a database of the group that holds more buckets than the
// restore made in it: something else created a bucket there between the group's creation and
// the moment the file reaches the database's record.
func (r *ringRestoreRun) checkGroupDatabaseUntouched(record *dumpDB, made int64) error {
	target := r.flags.restoreTargetName(record.Name)
	state, _, err := readTargetState(r.ctx, r.targetSource(), target)
	if err != nil {
		return err
	}
	if state.active+state.dropping == made {
		return nil
	}
	return restoreRefusal("Restore refused: database %q holds %s, and the restore made %s in it, so something else is writing to it, and restore never merges into existing data. Nothing was filled. %s",
		terminalSafeText(target), bucketCount(state.active+state.dropping), bucketCount(made), r.renameAdvice(r.flags, record.Name, target))
}
