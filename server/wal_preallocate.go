package server

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/hoarddb/hoarddb-server/config"
)

// walSegmentBytes is the size at which the log rotates; zero means the
// storage default of 64 MiB. A variable only so a test can rotate a
// preallocating log in kilobytes.
var walSegmentBytes int64

// walPreallocateResolver turns storage.wal_preallocate into the bool the log
// opens with. A variable so the server tests, which pin it off in TestMain,
// can ask for another answer.
var walPreallocateResolver = resolveWALPreallocateFromConfig

// resolveWALPreallocateFromConfig resolves the key as Load left it, reading the
// real filesystem for auto.
func resolveWALPreallocateFromConfig(cfg *config.Config, walDir string) walModeResolution {
	return resolveWALPreallocate(cfg.Storage.WALPreallocate, walDir, walFilesystemType)
}

// The magic numbers statfs reports in f_type (statfs(2)). ext2, ext3 and ext4
// share one.
const (
	fsMagicExt   = 0xEF53
	fsMagicXFS   = 0x58465342
	fsMagicBtrfs = 0x9123683E
	fsMagicZFS   = 0x2FC12FC1
	fsMagicTmpfs = 0x01021994
)

// walModeResolution is what storage.wal_preallocate came to at this start.
type walModeResolution struct {
	// On is the mode in effect, the bool the log is opened with.
	On bool
	// Configured is the value of the key: off, on or auto.
	Configured string
	// Filesystem names the filesystem auto read (ext2/3/4, xfs, btrfs, zfs,
	// tmpfs, or 0x followed by its type), empty when none was read.
	Filesystem string
	// Reason is why auto chose as it did, empty for off and on.
	Reason string
}

// resolveWALPreallocate resolves the configured mode. For auto it creates walDir
// when it is missing, so a first start has something to statfs, and asks probe
// for the filesystem type: ext2/3/4 gives on, everything else gives off until
// it has been measured (spec §6). A filesystem it cannot read gives off.
func resolveWALPreallocate(configured string, walDir string, probe func(dir string) (uint32, error)) walModeResolution {
	resolution := walModeResolution{Configured: configured}
	switch configured {
	case config.WALPreallocateOn:
		resolution.On = true
		return resolution
	case config.WALPreallocateAuto:
	default:
		return resolution
	}
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		resolution.Reason = fmt.Sprintf("could not read the filesystem of %s: %v", walDir, err)
		return resolution
	}
	fsType, err := probe(walDir)
	if err != nil {
		resolution.Reason = fmt.Sprintf("could not read the filesystem of %s: %v", walDir, err)
		return resolution
	}
	switch fsType {
	case fsMagicExt:
		resolution.On, resolution.Filesystem, resolution.Reason = true, "ext2/3/4", "ext2/3/4"
	case fsMagicXFS:
		resolution.Filesystem, resolution.Reason = "xfs", "XFS is not measured yet"
	case fsMagicBtrfs:
		resolution.Filesystem, resolution.Reason = "btrfs", "btrfs is copy-on-write, so it would not help"
	case fsMagicZFS:
		resolution.Filesystem, resolution.Reason = "zfs", "ZFS is copy-on-write, so it would not help"
	case fsMagicTmpfs:
		resolution.Filesystem, resolution.Reason = "tmpfs", "tmpfs is memory, so it would cost RAM"
	default:
		resolution.Filesystem = fmt.Sprintf("%#x", fsType)
		resolution.Reason = fmt.Sprintf("filesystem %#x is not ext2/3/4", fsType)
	}
	return resolution
}

// startLine is the one INFO line a start logs for the mode in effect.
func (r walModeResolution) startLine() string {
	const preallocated = "write-ahead log segments are preallocated "
	const plain = "write-ahead log segments are not preallocated "
	switch {
	case r.Configured == config.WALPreallocateOn && r.On:
		return preallocated + "(storage.wal_preallocate=on)"
	case r.Configured == config.WALPreallocateAuto && r.On:
		return preallocated + "(storage.wal_preallocate=auto: " + r.Reason + ")"
	case r.Configured == config.WALPreallocateOff:
		return plain + "(storage.wal_preallocate=off)"
	default:
		return plain + "(storage.wal_preallocate=" + r.Configured + ": " + r.Reason + ")"
	}
}

// logWALMode logs the mode in effect, with the fields a log search wants.
func (r walModeResolution) logWALMode(logger *slog.Logger) {
	logger.Info(r.startLine(), "mode", r.modeWord(), "configured", r.Configured, "filesystem", r.Filesystem, "reason", r.Reason, "component", "storage")
}

// logRingWALMode logs the mode of the entry-ring node's log. The line carries the
// node's name so it is never read as the legacy node's mode: the two nodes
// share the key and keep separate logs.
func (r walModeResolution) logRingWALMode(logger *slog.Logger) {
	logger.Info("entry-ring: "+r.startLine(), "mode", r.modeWord(), "configured", r.Configured, "filesystem", r.Filesystem, "reason", r.Reason, "component", "storage")
}

// modeWord is "on" when the log preallocates and "off" when it does not.
func (r walModeResolution) modeWord() string {
	if r.On {
		return "on"
	}
	return "off"
}

// logWALPreallocateNoEffect says so when the operator set the key to on or auto
// on a node whose legacy store keeps no write-ahead log, so a single-node
// benchmark run with the key set cannot pass for a preallocated run. The
// default (key unset) says nothing: auto is the default and would print this
// on every start. The entry-ring node has its own log and its own line.
func logWALPreallocateNoEffect(cfg *config.Config, logger *slog.Logger) {
	if !cfg.Storage.WALPreallocateSet || cfg.Storage.WALPreallocate == config.WALPreallocateOff {
		return
	}
	logger.Info("storage.wal_preallocate="+cfg.Storage.WALPreallocate+" has no effect here: this node keeps no node-to-node write-ahead log (replication_factor 1, no seed nodes); the entry-ring node logs its own mode",
		"configured", cfg.Storage.WALPreallocate, "component", "storage")
}
