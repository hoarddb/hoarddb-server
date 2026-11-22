//go:build !linux

package storage

import "fmt"

// lockStoreDirectory has no lock to take here, so the first-flush repair is
// not done on this platform and the store is refused with manual advice.
func lockStoreDirectory(dir string) (release func(), held bool, err error) {
	return nil, false, fmt.Errorf("the store directory %s cannot be locked on this platform", dir)
}
