package server

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

// originFormatMarker is written in the WAL directory the first time this node
// puts an origin-flagged entry in its log. Its presence is how a restart knows
// the operator has already been told.
const originFormatMarker = "origin-format"

// noteOriginFormat logs, once per data directory, that the log is in the
// origin format. No other build was released (owner ruling 2026-11-21), so
// the line names the format and the directory and no build that would misread
// it; it is INFO because nothing is wrong with this node. A failure to write the marker only means the line is repeated on the
// next start, so it is logged at debug and not returned: the append it follows
// has already succeeded and must not be reported as failed.
func (c *Cluster) noteOriginFormat() {
	if !c.originFormatNoted.CompareAndSwap(false, true) {
		return
	}
	walDir := filepath.Join(c.s.config.Storage.DataDir, "wal")
	markerPath := filepath.Join(walDir, originFormatMarker)
	marker, err := os.OpenFile(markerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return
	}
	if err != nil {
		c.logger.Debug("could not record the origin-format notice", "path", markerPath, "error", err)
	} else if closeErr := marker.Close(); closeErr != nil {
		c.logger.Debug("could not close the origin-format marker", "path", markerPath, "error", closeErr)
	}
	c.logger.Info("this node writes its log in the origin format in "+walDir,
		"data_dir", c.s.config.Storage.DataDir)
}

// segmentFormat2Marker is written in the WAL directory at the first start on a
// build that writes log segments in format 2. Its presence is how a later start
// knows the operator has already been told.
const segmentFormat2Marker = "segment-format-2"

// noteSegmentFormat2 logs, once per data directory, that the log is in
// segment format 2. Every open starts a v2 segment, so the line is logged at
// the first open, before any write (WAL preallocated segments spec §5). No
// other build was released, so the line names no build that would refuse it.
// It is INFO because nothing is wrong with this node. A failure to write the
// marker only means the line is repeated on the next start, so it is logged at
// debug and not returned: the log is already open and a start must not fail
// over a notice.
func noteSegmentFormat2(walDir string, logger *slog.Logger) {
	markerPath := filepath.Join(walDir, segmentFormat2Marker)
	marker, err := os.OpenFile(markerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return
	}
	if err != nil {
		logger.Debug("could not record the segment format 2 notice", "path", markerPath, "error", err)
	} else if closeErr := marker.Close(); closeErr != nil {
		logger.Debug("could not close the segment format 2 marker", "path", markerPath, "error", closeErr)
	}
	logger.Info("this node writes its log in segment format 2 in "+walDir, "dir", walDir)
}
