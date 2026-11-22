package storage

import (
	"path/filepath"
	"sync"
)

// directoryAdvice holds, by directory, the advice a ring node gives for a refusal that names its
// store or its log (DirectoryGoneError, a damaged incarnation file, a hole in the log). What is safe
// depends on whether the node is a cluster member, and storage does not know: a member put on an
// empty directory keeps its id and forgets its Raft term and log (store-vouched-cut review R3-2).
// One entry per directory of an open ring node, cleared at its close.
var directoryAdvice sync.Map

// SetDirectoryAdvice sets the advice that ends the refusals about dir: a ring node registers its
// store and its log directories at open.
func SetDirectoryAdvice(dir, advice string) {
	directoryAdvice.Store(filepath.Clean(dir), advice)
}

// ClearDirectoryAdvice removes what SetDirectoryAdvice stored for dir.
func ClearDirectoryAdvice(dir string) {
	directoryAdvice.Delete(filepath.Clean(dir))
}

// adviceForDirectory is the node's advice for dir, or fallback, the single node's advice, when none
// is registered. No text of this package tells anyone to empty a directory.
func adviceForDirectory(dir, fallback string) string {
	if registered, ok := directoryAdvice.Load(filepath.Clean(dir)); ok {
		return registered.(string)
	}
	return fallback
}
