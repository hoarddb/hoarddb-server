//go:build !linux

package storage

// renameNoReplace renames oldPath to newPath and refuses, with an error that
// matches fs.ErrExist, when newPath already exists. Off Linux there is no
// portable no-replace rename, so it is renameIfAbsent.
func renameNoReplace(oldPath, newPath string) error {
	return renameIfAbsent(oldPath, newPath)
}
