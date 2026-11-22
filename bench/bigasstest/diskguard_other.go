//go:build !unix

package main

import (
	"context"
	"errors"
)

// statfsFree is unavailable off Unix: use -free-cmd.
func statfsFree(string) freeFunc {
	return func(context.Context) (int64, error) {
		return 0, errors.New("-data-dir needs a Unix host. Pass -free-cmd instead")
	}
}
