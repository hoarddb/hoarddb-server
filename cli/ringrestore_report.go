package cli

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// restoreProgress is what a restore has completed, kept in constant space: a restore of a
// million buckets must not hold a million names (AGENTS.md rule 11). It counts the buckets
// of the database in progress and names the first few, and it knows the one bucket that is
// created and not yet complete.
type restoreProgress struct {
	// dumpDatabase and targetDatabase name the database in progress, empty between databases.
	// created says its create was answered: a database whose create failed is not started.
	dumpDatabase, targetDatabase string
	created                      bool
	// completeBuckets counts the buckets of the database in progress that are complete, and
	// completeNames holds the first restoreListedNames of them.
	completeBuckets int64
	completeNames   []string
	// partialBucket is the bucket created and not yet complete, empty when none.
	partialBucket string
	// ahead are the buckets created before their records because another bucket links to
	// them, in creation order. aheadDone marks those whose records the restore has reached, so
	// finding one is a map lookup and not a search of the list; aheadHead is the first that may
	// still be waiting, so the report reads the first names without scanning the done ones.
	ahead        []string
	aheadDone    map[string]bool
	aheadHead    int
	aheadPending int
	// finishedDatabases counts the databases written in full, and finishedNames holds the
	// first restoreListedNames of them, so a report can print their recovery commands when
	// the end of the file shows the file changed.
	finishedDatabases int64
	finishedNames     []string
}

// attemptDatabase notes the database about to be created, so a failed create names it as the
// database that was not started.
func (p *restoreProgress) attemptDatabase(dumpName, targetName string) {
	p.dumpDatabase, p.targetDatabase, p.created = dumpName, targetName, false
	p.completeBuckets, p.completeNames, p.partialBucket = 0, nil, ""
}

// beginDatabase notes that the database was created.
func (p *restoreProgress) beginDatabase() { p.created = true }

// aheadKey names a bucket by its target database and name.
func aheadKey(database, bucket string) string { return database + "." + bucket }

// createdAhead notes a bucket created for a link before the file reached its records.
func (p *restoreProgress) createdAhead(database, bucket string) {
	p.ahead = append(p.ahead, aheadKey(database, bucket))
	p.aheadPending++
}

// beginBucket notes the bucket whose records are being written. A bucket created ahead is no
// longer waiting once its records have begun.
func (p *restoreProgress) beginBucket(database, bucket string) {
	p.partialBucket = bucket
	key := aheadKey(database, bucket)
	if p.aheadPending == 0 || p.aheadDone[key] {
		return
	}
	if p.aheadDone == nil {
		p.aheadDone = map[string]bool{}
	}
	p.aheadDone[key] = true
	p.aheadPending--
}

// aheadWaiting returns the first names of the buckets created ahead and not reached, and how
// many wait in all.
func (p *restoreProgress) aheadWaiting() ([]string, int64) {
	var names []string
	for p.aheadHead < len(p.ahead) && p.aheadDone[p.ahead[p.aheadHead]] {
		p.aheadHead++
	}
	for index := p.aheadHead; index < len(p.ahead) && len(names) < restoreListedNames; index++ {
		if !p.aheadDone[p.ahead[index]] {
			names = append(names, p.ahead[index])
		}
	}
	return names, int64(p.aheadPending)
}

func (p *restoreProgress) completeBucket() {
	if p.partialBucket == "" {
		return
	}
	p.completeBuckets++
	if len(p.completeNames) < restoreListedNames {
		p.completeNames = append(p.completeNames, p.partialBucket)
	}
	p.partialBucket = ""
}

func (p *restoreProgress) completeDatabase() {
	p.finishedDatabases++
	if len(p.finishedNames) < restoreListedNames {
		p.finishedNames = append(p.finishedNames, p.dumpDatabase)
	}
	p.dumpDatabase, p.targetDatabase, p.created = "", "", false
	p.completeBuckets, p.completeNames, p.partialBucket = 0, nil, ""
}

// stateReport says what a restore that stopped left behind and what the operator does next
// (section 3.5). It is built from constant-size state and from a skim of the rest of the
// dump for the databases not started, so it holds no list that grows with the ring. Running
// restore again into a name that now holds buckets is refused by design, so the recovery is
// per database: one command for the database in progress, one for each not started, and none
// for a finished one (R2-11: --as renames one database, never a whole dump).
func (r *ringRestoreRun) stateReport(applier *restoreApplier) string {
	progress := applier.progress
	var text strings.Builder
	touched := progress.finishedDatabases > 0 || progress.created || len(applier.groupNames) > 0
	if !touched {
		text.WriteString("Nothing was created on the ring.")
		if lines := r.notStartedCommands(applier, progress.dumpDatabase); len(lines) > 0 {
			text.WriteString(" Run the same command again.")
		}
		return text.String()
	}
	var commands []string
	if applier.footerMismatch {
		// The checksum of the whole file is known only at its end, and it did not match
		// pass 1's, so no database is known to be right, finished ones included.
		fmt.Fprintf(&text, "The dump changed while it was being restored, so nothing written from it is known to be right, including the %s databases written in full. Take a good copy of the dump first.\n", formatCount(progress.finishedDatabases))
		for _, name := range progress.finishedNames {
			if applier.groupCreated[name] {
				continue // restoring one database of the group alone would mislink the others
			}
			commands = append(commands, fmt.Sprintf("hoarddb restore %s --db %s --as %s", r.flags.file, terminalSafeText(name), terminalSafeText(retryName(name, name))))
		}
	} else if progress.finishedDatabases > 0 {
		fmt.Fprintf(&text, "%s databases were written in full (the file's checksum is checked at its end).\n", formatCount(progress.finishedDatabases))
	}
	if progress.created {
		fmt.Fprintf(&text, "Database %q was created and is not complete. ", terminalSafeText(progress.targetDatabase))
		if progress.completeBuckets > 0 {
			fmt.Fprintf(&text, "Its complete buckets: %s. ", quotedNames(progress.completeNames, progress.completeBuckets))
		} else {
			text.WriteString("It has no complete bucket. ")
		}
		if progress.partialBucket != "" {
			fmt.Fprintf(&text, "Bucket %q was created and is not complete (%s entities and %s elements were confirmed by the ring before it stopped). ",
				terminalSafeText(progress.partialBucket), formatCount(applier.tally.bucketEntities), formatCount(applier.tally.bucketElements))
		}
		if names, waiting := applier.progress.aheadWaiting(); waiting > 0 {
			fmt.Fprintf(&text, "Buckets %s were created empty because other buckets link to them, and the restore stopped before it filled them. ",
				quotedNames(names, waiting))
		}
		if applier.tally.orphans > 0 {
			fmt.Fprintf(&text, "%s documents exist without their stores (%s). ", formatCount(applier.tally.orphans), quotedNames(applier.tally.orphanNames, applier.tally.orphans))
		}
		if applier.torn {
			text.WriteString("The last group was torn: a store frame failed or was not confirmed, so some documents of it may hold some of their elements and not others. ")
		}
		text.WriteString("\n")
		// A database of the group is linked into, or links into, another one: a command that
		// restores it alone under a new name would leave the others linked to the buckets left
		// behind here, so the group has its own advice below.
		if !applier.groupCreated[progress.dumpDatabase] {
			commands = append(commands, fmt.Sprintf("hoarddb restore %s --db %s --as %s", r.flags.file, terminalSafeText(progress.dumpDatabase), terminalSafeText(retryName(progress.dumpDatabase, progress.targetDatabase))))
		}
	}
	notStarted := r.notStartedCommands(applier, ifNotCreated(progress))
	commands = append(commands, notStarted...)
	text.WriteString("A restore never merges into a database that holds buckets, so running it again as it was is refused. ")
	if len(commands) > 0 {
		text.WriteString("Restore what is not finished, each database under its own command:\n")
		for _, command := range commands {
			fmt.Fprintf(&text, "  %s\n", command)
		}
	}
	if applier.footerMismatch && len(progress.finishedNames) > 0 {
		fmt.Fprintf(&text, "Or drop the buckets of %s, wait until 'show buckets in <database>' lists nothing, and restore from a good copy of the dump. ", quotedNames(progress.finishedNames, progress.finishedDatabases))
	}
	if len(applier.groupNames) > 0 {
		if names, waiting := applier.progress.aheadWaiting(); waiting > 0 && !progress.created {
			fmt.Fprintf(&text, "Buckets %s were created empty because other buckets link to them, and the restore stopped before it filled them. ", quotedNames(names, waiting))
		}
		fmt.Fprintf(&text, "\nBuckets link across databases %s, so these databases were created together and can only be restored together: no command restores one of them alone and leaves the others right. Drop every bucket in each of them, wait until 'show buckets in <database>' lists nothing for each, and run the same restore again.",
			quotedNames(applier.groupNames[:min(len(applier.groupNames), restoreListedNames)], int64(len(applier.groupNames))))
	} else if progress.created {
		fmt.Fprintf(&text, "Or drop the buckets of %q, wait until 'show buckets in %s' lists nothing, and run the same restore again.",
			terminalSafeText(progress.targetDatabase), terminalSafeText(progress.targetDatabase))
	}
	return strings.TrimRight(text.String(), "\n ")
}

// ifNotCreated returns the dump name of a database whose create failed, which is not
// started and gets a plain command, and "" when the database in progress was created.
func ifNotCreated(progress restoreProgress) string {
	if progress.created {
		return ""
	}
	return progress.dumpDatabase
}

// retryName is the name the database in progress is restored under: not the one it holds.
func retryName(dumpName, heldName string) string {
	if candidate := dumpName + "_restored"; candidate != heldName {
		return candidate
	}
	return dumpName + "_restored_2"
}

// notStartedCommands lists one plain restore command for every selected database the restore
// did not reach: the database whose create failed (failedName, when not empty) and every
// database whose record comes after the position the reader stopped at, found by skimming the
// rest of the file. A skim that fails adds nothing: the report is already about a failure.
func (r *ringRestoreRun) notStartedCommands(applier *restoreApplier, failedName string) []string {
	var commands []string
	if failedName != "" && !applier.groupCreated[failedName] {
		commands = append(commands, fmt.Sprintf("hoarddb restore %s --db %s", r.flags.file, terminalSafeText(failedName)))
	}
	if applier.reader == nil {
		return commands
	}
	skimmer, err := openDumpSkimmer(r.flags.file)
	if err != nil {
		return commands
	}
	defer func() { _ = skimmer.close() }()
	skimmer.offset = applier.recordStart
	for {
		record, _, err := skimmer.next()
		if errors.Is(err, io.EOF) || err != nil {
			return commands
		}
		// The database in progress, or the one whose create failed, has its own line.
		if database, ok := record.(*dumpDB); ok && r.flags.selects(database.Name) && database.Name != applier.progress.dumpDatabase && !applier.groupCreated[database.Name] {
			commands = append(commands, fmt.Sprintf("hoarddb restore %s --db %s", r.flags.file, terminalSafeText(database.Name)))
		}
	}
}
