package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"
)

// walSyscalls is the table of syscalls the log makes where the order or the
// failure of a call decides what survives a crash. Each entry is the real call
// in production. Tests in this package replace entries before the first append,
// either to record the order in which a rotation or an open makes them or to
// fail one, which a healthy disk cannot do on demand.
type walSyscalls struct {
	// flush is the flush of a growing or grown segment's entries: the group
	// commit waitForSync's leader runs, and a rotation's flush of the segment
	// it retires (R1). The size of such a segment changes with its entries, so
	// it is a whole-file fsync.
	flush func(*os.File) error
	// flushData is the same flush on a preallocated segment whose size never
	// changed: every block was written and fsynced when the spare was born, so
	// a data-only flush makes an entry durable (spec §4.1). It is its own entry,
	// apart from fdatasync, so a test that hooks the group commit never sees a
	// fill's flushes.
	flushData func(*os.File) error
	// fsync is every other whole-file fsync: a born segment's header, a
	// spare's birth (B6), R2's cut, Close, FastForward, the open's tail.
	fsync func(*os.File) error
	// fdatasync flushes a spare's data: each zero chunk of a fill (B5) and a
	// re-headed spare's new header.
	fdatasync func(*os.File) error
	// fallocate reserves a spare's blocks before its zeros are written (B3).
	// The real call ignores a filesystem that does not support it, since the
	// zeros allocate the blocks anyway.
	fallocate func(file *os.File, size int64) error
	// statfsFree is the bytes free to an unprivileged writer on the filesystem
	// holding a directory: a fill refuses to start without room for two
	// segments (B2).
	statfsFree func(dirPath string) (uint64, error)
	// ftruncate cuts a segment: a torn tail, or a rolled-back partial entry.
	ftruncate func(*os.File, int64) error
	// rename moves a born segment into its segment name. It never replaces a
	// file already under that name: a segment that may hold entries is never
	// overwritten by an empty one, whatever index a birth was asked for.
	rename func(oldPath, newPath string) error
	// writeAt writes one framed entry at the log's write offset. It is a
	// positioned write so the offset in memory, not the file's position or
	// size, decides where an entry lands.
	writeAt func(file *os.File, entry []byte, offset int64) (int, error)
	// syncDir fsyncs a directory, which is what makes a rename into it durable.
	syncDir func(dirPath string) error
	// remove deletes a file the log no longer wants under that name.
	remove func(path string) error
	// close closes the segment a rotation retires. It is in the table because
	// where it falls in the rotation is part of the order: the outgoing segment
	// is closed only after its flush and before the next segment takes a name.
	close func(*os.File) error
	// now is the clock the rotation's refusal is rate-limited by, and the
	// filler's hourly warning.
	now func() time.Time
	// fillRetry is how long the filler waits after a failed fill before it
	// tries again. It is with the clock because it is a wait a test must be
	// able to shorten: a filler that never retries would otherwise pass every
	// test that runs for less than one interval.
	fillRetry time.Duration
	// fillStage is nil in production. A test sets it to see a fill reach a
	// stage ("begin", "created", "allocated", "chunk", "ready") and to hold it
	// there: a fill's interleaving with a rotation cannot be forced otherwise.
	fillStage func(stage string)
}

// walSyscallsAtOpen returns the table an open starts with. It is a variable so
// a test can record or fail the calls the open itself makes (a new log's first
// segment is born inside the open, before a test could replace the table on the
// returned WAL); production never replaces it.
var walSyscallsAtOpen = realWALSyscalls

// realWALSyscalls returns the table production uses.
func realWALSyscalls() walSyscalls {
	return walSyscalls{
		flush:      (*os.File).Sync,
		flushData:  fdatasyncFile,
		fsync:      (*os.File).Sync,
		fdatasync:  fdatasyncFile,
		fallocate:  fallocateFile,
		statfsFree: freeBytesAt,
		ftruncate:  (*os.File).Truncate,
		rename:     renameNoReplace,
		writeAt:    (*os.File).WriteAt,
		syncDir:    syncDirectoryPath,
		remove:     os.Remove,
		close:      (*os.File).Close,
		now:        time.Now,
		fillRetry:  walSpareRetryInterval,
	}
}

// renameIfAbsent renames oldPath to newPath unless newPath exists, in which
// case it refuses with an error that matches fs.ErrExist. The check and the
// rename are two steps, which is safe here because only the log renames into
// its directory and its births are serialised (under WAL.mu, or inside the open
// before the log is shared). It is the fallback for a filesystem or a platform
// without a no-replace rename.
func renameIfAbsent(oldPath, newPath string) error {
	_, err := os.Lstat(newPath)
	if err == nil {
		return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: fs.ErrExist}
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("checking that %s does not exist before renaming %s to it: %w", newPath, oldPath, err)
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", oldPath, newPath, err)
	}
	return nil
}
