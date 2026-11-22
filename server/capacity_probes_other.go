//go:build !linux

package server

import "errors"

// Off Linux every host probe refuses rather than report a value it did not
// read (node-capacity-spec.md N-4): the sampler then reports each resource as
// unavailable, never as empty.

func readOpenFDs() (open, limit int64, err error) {
	return 0, 0, errors.New("fds capacity measurement is only implemented on linux")
}

func readMappings() (count, limit int64, err error) {
	return 0, 0, errors.New("mappings capacity measurement is only implemented on linux")
}

func hostAnonMemory() (int64, error) {
	return 0, errors.New("memory capacity measurement is only implemented on linux")
}

func hostMemoryLimit() (memoryLimit, error) {
	return memoryLimit{}, errors.New("memory capacity measurement is only implemented on linux")
}
