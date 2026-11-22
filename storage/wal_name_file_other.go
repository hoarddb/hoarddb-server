//go:build !unix

package storage

import "os"

// fileUnderName returns f unchanged where there is no portable way to give an
// open descriptor another name: errors on the segment name the spare it was
// born as instead of the segment, which changes no behaviour.
func fileUnderName(f *os.File, _ string) (*os.File, error) {
	return f, nil
}
