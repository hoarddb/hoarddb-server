package cli

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

const (
	privateDumpFileMode = 0o600
	privateDumpDirMode  = 0o700
)

// createPrivateFile creates or truncates a dump file so only its owner can
// read it. A dump holds every document in plaintext, so the mode is chosen
// here and not left to the umask. A symbolic link at path is refused rather
// than followed. A regular file left by an earlier dump is rewritten and
// narrowed to 0600: re-dumping into an existing dump directory is a supported
// flow, and refusing it would make the owner delete files to repeat a command.
func createPrivateFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW, privateDumpFileMode)
	if errors.Is(err, syscall.ELOOP) {
		return nil, fmt.Errorf("%s is a symbolic link; remove it, or dump to a directory that does not contain it", path)
	}
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(privateDumpFileMode); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("restricting %s to its owner: %w", path, err)
	}
	return file, nil
}
