package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync/atomic"
	"time"
)

// dirCheckInterval bounds how often a write re-checks its directory. Writes
// between two checks are not checked: at most this long after a removal they
// are still acknowledged, which loses nothing the removal had not already
// lost, since every earlier write went with the directory too. One stat per
// interval per node, whatever the write rate and the bucket count.
const dirCheckInterval = 100 * time.Millisecond

// guardEpoch is the process reference the directory guard measures intervals
// from. time.Since reads the monotonic clock, so a wall-clock step (an NTP
// correction, a manual change) neither stretches nor collapses an interval.
var guardEpoch = time.Now()

// DirectoryGoneError says the directory a store or log writes into is no
// longer the one it opened: it was removed, or moved away and another put in
// its place. The open files still take writes, but nothing on disk has their
// name any more, so whatever is written now is lost at the next restart.
type DirectoryGoneError struct {
	Dir   string
	Cause error // the stat that found nothing, or nil when the path is not the opened directory
	// Replaced names what the stat found in the directory's place, from its
	// Mode(): "a regular file is in its place", "another directory is in its
	// place" or "something else is in its place". Empty when Cause is set.
	Replaced string
}

func (e *DirectoryGoneError) Error() string {
	found := e.Replaced
	if e.Cause != nil {
		found = e.Cause.Error()
	}
	if found == "" {
		found = "another directory is in its place"
	}
	return fmt.Sprintf("%s was removed or replaced while the node ran (%s), so what is written now could not be "+
		"found after a restart. %s. Until then reads are served from the files it still holds open",
		e.Dir, found, adviceForDirectory(e.Dir, "Restore the node's data directory from a backup and restart the node, "+
			"or keep what is left of it"))
}

// replacedBy names what a stat found where the opened directory was.
func replacedBy(mode fs.FileMode) string {
	switch {
	case mode.IsRegular():
		return "a regular file is in its place"
	case mode.IsDir():
		return "another directory is in its place"
	default:
		return "something else is in its place"
	}
}

func (e *DirectoryGoneError) Unwrap() error { return e.Cause }

// dirGuard notices that a directory was removed or replaced under an open
// store, from the identity (device and inode) it had when the store opened.
// Once it has noticed, it refuses for good: a directory put back is not the
// one the open files live in.
type dirGuard struct {
	dir    string
	opened os.FileInfo
	// nextCheck is when, in nanoseconds since guardEpoch, the next caller stats
	// the directory.
	nextCheck atomic.Int64
	gone      atomic.Pointer[DirectoryGoneError]
}

func newDirGuard(dir string) (*dirGuard, error) {
	opened, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("recording the identity of %s: %w", dir, err)
	}
	return &dirGuard{dir: dir, opened: opened}, nil
}

// check returns the directory's refusal, or nil while it is still the one
// opened. It stats the directory at most once per dirCheckInterval; callers
// between two stats share the last answer.
func (g *dirGuard) check() error {
	if gone := g.gone.Load(); gone != nil {
		return gone
	}
	now := int64(time.Since(guardEpoch))
	next := g.nextCheck.Load()
	if now < next || !g.nextCheck.CompareAndSwap(next, now+int64(dirCheckInterval)) {
		return nil
	}
	return g.checkNow()
}

// checkNow is check without the interval: it stats the directory on every call. The sync
// marker's in-place advance asks it, since a write through the marker's open descriptor goes on
// working after the directory is removed or replaced, where the rename it replaced failed.
func (g *dirGuard) checkNow() error {
	if gone := g.gone.Load(); gone != nil {
		return gone
	}
	current, err := os.Stat(g.dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		g.gone.CompareAndSwap(nil, &DirectoryGoneError{Dir: g.dir, Cause: err})
	case err != nil:
		// A stat refused for another reason (a parent made unsearchable, an
		// I/O error) proves nothing about the open files' names: the store's
		// own writes report a failing disk, and the next check asks again.
		return nil
	case !os.SameFile(g.opened, current):
		g.gone.CompareAndSwap(nil, &DirectoryGoneError{Dir: g.dir, Replaced: replacedBy(current.Mode())})
	default:
		return nil
	}
	return g.gone.Load()
}
