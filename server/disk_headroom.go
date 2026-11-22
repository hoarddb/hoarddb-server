package server

import (
	"fmt"
	"time"
)

// diskFreeBytes is the statfs-backed free-space probe restore's per-bucket
// headroom check (dump-restore-spec.md §9.1/§11/§19 A-15) uses: unlike the
// join guard's percentage, the check needs an absolute byte count to compare
// against a multiplier of the bucket's raw dump bytes. It is a package
// variable so a deterministic test overrides it rather than filling a real
// disk (dump-restore-build.md trap #10; see restore_test.go).
var diskFreeBytes = diskFreeBytesStatfs

// diskStat is one raw statfs reading of the filesystem holding the data
// directory. The join guard's percentage and the capacity sampler's byte
// gauges and ratio are all computed from one of these, so they describe the
// same instant (node-capacity-spec.md §3.3). Blocks and Bavail are counted in
// Frsize units (POSIX), which is why Frsize, not Bsize, is the multiplier.
type diskStat struct {
	Blocks, Bavail, Frsize uint64
}

// usedBytes counts blocks a non-root writer can no longer use as used: the
// root reserve is exactly what an mmap'd storage write has no error path
// through, so counting it as used keeps headroom conservative.
func (st diskStat) usedBytes() uint64 { return (st.Blocks - st.Bavail) * st.Frsize }

func (st diskStat) sizeBytes() uint64 { return st.Blocks * st.Frsize }

// usedPercent is the guard's expression, unchanged from the one it replaced,
// character for character: switching to integer arithmetic would move the
// guard's decision at boundary values (node-capacity-build.md §4 D2).
func (st diskStat) usedPercent() float64 {
	return float64(st.Blocks-st.Bavail) / float64(st.Blocks) * 100
}

// validate refuses a reading whose size cannot be measured. It sits
// downstream of the probe member, not inside the real statfs call, so an
// injected zero-block fixture reaches the same check a real one does.
func (st diskStat) validate(dir string) error {
	if st.Blocks == 0 || st.Frsize == 0 {
		return fmt.Errorf("filesystem holding %s reports zero blocks; its size cannot be measured", dir)
	}
	return nil
}

// diskBelowMark is the one comparison between a disk reading and the
// high-water mark. The join guard and the capacity sampler's disk state both
// call it, so the gauge's red line and the node's refusal cannot disagree,
// even by one ulp. A mark of 0 or less disables the guard.
func diskBelowMark(st diskStat, limitPct int) bool {
	return limitPct <= 0 || st.usedPercent() < float64(limitPct)
}

// readDiskStat reads the filesystem holding dir through this server's probe
// set and validates the reading.
func (s *Server) readDiskStat(dir string) (diskStat, error) {
	st, err := s.probes.Load().diskStatfs(dir)
	if err != nil {
		return diskStat{}, fmt.Errorf("reading the filesystem holding %s: %w", dir, err)
	}
	if err := st.validate(dir); err != nil {
		return diskStat{}, err
	}
	return st, nil
}

// fullDisk is the first of the node's filesystems (Server.diskDirs) that is
// at or above the disk high-water mark, with its reading. A filesystem that
// cannot be read is passed over: the guard fails open per filesystem, and a
// membership change is never refused just because headroom could not be
// measured. A mark of 0 disables the guard, and nothing is full: no
// filesystem is read then, because a replica asks on every bucket create its
// owner's stream delivers (refuseReplayedCreateOnFullDisk).
func (s *Server) fullDisk() (dir string, st diskStat, full bool) {
	limit := s.config.Server.DiskHighWaterPct
	if limit <= 0 {
		return "", diskStat{}, false
	}
	for _, dir := range s.diskDirs() {
		st, err := s.readDiskStat(dir)
		if err != nil {
			continue
		}
		if !diskBelowMark(st, limit) {
			return dir, st, true
		}
	}
	return "", diskStat{}, false
}

// diskHeadroomOK reports whether every filesystem this node writes to is
// below the configured disk high-water mark. A blob directory on its own
// filesystem counts: blobs are the node's largest, least bounded bytes, and a
// copied-in bucket brings them.
func (c *Cluster) diskHeadroomOK() bool {
	_, _, full := c.s.fullDisk()
	return !full
}

// diskFullJoinError explains why this node will not propose or vote for a new
// member, with the recovery step (AGENTS.md rule 3: the error says what to do
// next). The vote is only this node's: a majority of the others can still add
// the member, and what protects this node's disk then is the refusal to copy
// buckets in (refuseLandingOnFullDisk), which the text says. It reads the same
// probe the guard read, so the percentage it prints is the one the guard
// decided on, and it names the filesystem that is full.
func (c *Cluster) diskFullJoinError() error {
	dir, st, full := c.s.fullDisk()
	var pct float64
	if full {
		pct = st.usedPercent()
	}
	limit := c.s.config.Server.DiskHighWaterPct
	const outcome = "so it does not propose or vote for a new member. A majority of the other members can still add one; " +
		"this node then copies in no bucket it does not already hold until it is below the mark"
	if full && dir != c.s.config.Storage.DataDir {
		return fmt.Errorf("%w: node %s's blob directory %s is on a filesystem above the disk high-water mark (%.0f%% used, limit %d%%), %s. Free space on that filesystem or raise HOARDDB_DISK_HIGH_WATER before joining another node",
			ErrMemberDiskFull, c.self.ID, dir, pct, limit, outcome)
	}
	return fmt.Errorf("%w: node %s is above the disk high-water mark (%.0f%% used, limit %d%%), %s. Free space on this node or raise HOARDDB_DISK_HIGH_WATER before joining another node",
		ErrMemberDiskFull, c.self.ID, pct, limit, outcome)
}

// refuseLandingOnFullDisk is the disk guard on a pull (pullBucketWith), whatever
// asked for it — this node's sweep after any adoption, a peer's hand-off, the
// take-over of a bucket this node now owns, or an operator's migrate. A node
// that cannot veto a membership change can still refuse the writes it brings
// (BUG-0104; owner ruling 2026-10-14, fail closed): an mmap-backed write into a
// full filesystem is a SIGBUS, not an error. Adoption, giving copies away and
// releasing them proceed, since they never grow this node's disk. It fails
// open exactly as diskHeadroomOK does, whose decision it is: on a filesystem
// that cannot be measured, and with the mark at 0 or less. A pull is one of
// the two ways a bucket this node does not hold lands here; the other is an
// owner's stream replaying the bucket's create, which
// refuseReplayedCreateOnFullDisk guards (BUG-0550).
//
// A refused pull leaves a copy this node holds as it was (BUG-0551, owner
// ruling 2026-10-14: keep the copy). A follower's copy afresh (copyAfresh) and
// a take-over over an own copy (copyForTakeOver) have the pull release the
// copy, after this guard and after refuseReplacingPastTheMark (BUG-0664). A
// diverged replica's re-seed (reseedDiverged) releases before its pull, so it
// calls this before the release too; a refusal counts once toward the WARN,
// since the step returns on it, and a disk that crosses the mark between the
// two calls is refused at the pull, after the release.
//
// The refusal is the caller's error, and it is written to the log at WARN at
// most once per reclaimWarnInterval, with the number of copies refused since
// the last one: a sweep asks once per bucket per pass, and a WARN per ask would
// be O(buckets) lines (warnRefusedLanding).
func (c *Cluster) refuseLandingOnFullDisk(database, bucket string) error {
	dir, st, full := c.s.fullDisk()
	if !full {
		return nil
	}
	refusal := c.diskFullPullError(database, bucket, dir, st)
	c.warnRefusedLanding(database, bucket, refusal)
	return refusal
}

// refuseReplacingPastTheMark is the disk guard on a pull that replaces a copy
// this node holds (pullPlan.replaces): the copy is kept until the new one has
// landed, so the new one must fit beside it. incoming is the size of the copy
// being pulled — a lower bound before its chunks are fetched, its exact size
// after — and the pull is refused when adding it to any filesystem the node
// writes to would take that filesystem to the high-water mark or past it. The
// whole copy is counted against each, since its keys go to the data directory's
// and its blob payloads to the blob directory's. It fails open as
// refuseLandingOnFullDisk does, and its refusals share that guard's WARN.
func (c *Cluster) refuseReplacingPastTheMark(database, bucket string, incoming uint64) error {
	limit := c.s.config.Server.DiskHighWaterPct
	if limit <= 0 {
		return nil
	}
	for _, dir := range c.s.diskDirs() {
		st, err := c.s.readDiskStat(dir)
		if err != nil {
			continue
		}
		if diskBelowMarkWith(st, limit, incoming) {
			continue
		}
		refusal := c.diskFullReplaceError(database, bucket, dir, st, incoming)
		c.warnRefusedLanding(database, bucket, refusal)
		return refusal
	}
	return nil
}

// diskBelowMarkWith reports whether the filesystem st describes stays below
// the mark once incoming more bytes are written to it.
func diskBelowMarkWith(st diskStat, limitPct int, incoming uint64) bool {
	return float64(st.usedBytes()+incoming)/float64(st.sizeBytes())*100 < float64(limitPct)
}

// diskFullReplaceError is the refusal of a pull that cannot fit beside the
// copy it replaces: which node and bucket, that the copy is kept, the reading
// it would come to, and the two ways out (AGENTS.md rule 3).
func (c *Cluster) diskFullReplaceError(database, bucket, dir string, st diskStat, incoming uint64) error {
	after := float64(st.usedBytes()+incoming) / float64(st.sizeBytes()) * 100
	return fmt.Errorf("%w: node %s refused to copy bucket %q of database %q over its own copy: the copy it holds is kept until the new one "+
		"has landed, so both must fit, and the new copy's %d bytes would take %s from %.0f%% to %.0f%% used, past the disk high-water mark "+
		"(limit %d%%). Nothing was copied, and %s's copy of the bucket is kept as it was. Free space on that filesystem (or raise "+
		"HOARDDB_DISK_HIGH_WATER), then run 'migrate %s %s' on %s; or take the node out of the cluster with 'remove-node %s' "+
		"run on another member",
		ErrMemberDiskFull, c.self.ID, bucket, database, incoming, c.fullFilesystemName(dir), st.usedPercent(), after,
		c.s.config.Server.DiskHighWaterPct, c.self.ID, database, bucket, c.self.ID, c.self.ID)
}

// refuseReplayedCreateOnFullDisk is the disk guard on the second way a bucket
// this node does not hold lands here: its owner's stream delivering the
// bucket's create, replayed from a log that still holds it, which applies with
// no pull (BUG-0550). The owner cannot see this node's disk (no gossip,
// handshake or push answer carries it), so the owner's ruling of 2026-10-14
// falls back to refusing the create here. The stream is ordered, so the
// refusal stalls it at this entry for every bucket until the node is below the
// mark, and the error says so. A create of a bucket this node holds lands
// nothing and is not refused. It fails open as refuseLandingOnFullDisk does.
func (c *Cluster) refuseReplayedCreateOnFullDisk(sender, database, bucket string) error {
	if c.s.bucketStoreOpen(database, bucket) {
		return nil
	}
	dir, st, full := c.s.fullDisk()
	if !full {
		return nil
	}
	refusal := c.diskFullReplayError(sender, database, bucket, dir, st)
	c.warnRefusedLanding(database, bucket, refusal)
	return refusal
}

// warnRefusedLanding writes a refused landing to the log at WARN at most once
// per reclaimWarnInterval, with the number refused since the last one: a sweep
// asks once per bucket per pass and an owner re-sends a refused create on
// every retry, and a WARN per refusal would be O(buckets) lines.
func (c *Cluster) warnRefusedLanding(database, bucket string, refusal error) {
	c.diskRefusals.Add(1)
	now := time.Now().UnixNano()
	last := c.diskRefusalWarnedAt.Load()
	if (last == 0 || now-last >= int64(reclaimWarnInterval)) && c.diskRefusalWarnedAt.CompareAndSwap(last, now) {
		c.logger.Warn("refused to copy a bucket in: this node's disk is above the high-water mark",
			"node_id", c.self.ID, "database", database, "bucket", bucket,
			"refused_since_last_warning", c.diskRefusals.Swap(0), "error", refusal)
	}
}

// fullFilesystemName names the filesystem fullDisk found full, as a refusal
// that travels to another node must: by this node's id, not "this node".
func (c *Cluster) fullFilesystemName(dir string) string {
	if dir != c.s.config.Storage.DataDir {
		return fmt.Sprintf("the filesystem holding %s's blob directory %s", c.self.ID, dir)
	}
	return fmt.Sprintf("the filesystem holding %s's data directory", c.self.ID)
}

// diskFullReplayError is the refusal of a replayed create on a full
// filesystem. It is the owner's stall reason, so it is written for the
// owner's operator as much as this node's: which node, which bucket, the
// stall it causes, and the two ways out (AGENTS.md rule 3).
func (c *Cluster) diskFullReplayError(sender, database, bucket, dir string, st diskStat) error {
	return fmt.Errorf("%w: node %s refused the create of bucket %q of database %q replicated from %s: %s is above the disk high-water mark "+
		"(%.0f%% used, limit %d%%), and a write into a full filesystem can crash the node. Nothing was applied. Replication from %s to %s "+
		"is stalled at this entry, for every bucket, until %s accepts it: the copies %s already holds fall behind and count toward no "+
		"write concern, and %s keeps its log for %s meanwhile. Free space on that filesystem (or raise HOARDDB_DISK_HIGH_WATER) and "+
		"the stream resumes on its own; or take the node out of the cluster with 'remove-node %s' run on another member",
		ErrMemberDiskFull, c.self.ID, bucket, database, sender, c.fullFilesystemName(dir), st.usedPercent(),
		c.s.config.Server.DiskHighWaterPct, sender, c.self.ID, c.self.ID, c.self.ID, sender, c.self.ID, c.self.ID)
}

// diskFullPullError is the refusal of one pull on a full filesystem: which
// node, which bucket, what it costs while it lasts, and the two ways out
// (AGENTS.md rule 3). It travels to whoever asked — an operator's migrate, a
// peer's hand-off, a client whose write a take-over refuses — so it names the
// node rather than saying "this node".
func (c *Cluster) diskFullPullError(database, bucket, dir string, st diskStat) error {
	return fmt.Errorf("%w: node %s refused to copy in bucket %q of database %q: %s is above the disk high-water mark "+
		"(%.0f%% used, limit %d%%), and a write into a full filesystem can crash the node. Nothing was copied, and the bucket's "+
		"other copies are kept; until %s holds it, a bucket it owns refuses writes and one it is a replica of is a copy short. "+
		"Free space on that filesystem (or raise HOARDDB_DISK_HIGH_WATER), then run 'migrate %s %s' on %s; "+
		"or take the node out of the cluster with 'remove-node %s' run on another member",
		ErrMemberDiskFull, c.self.ID, bucket, database, c.fullFilesystemName(dir), st.usedPercent(), c.s.config.Server.DiskHighWaterPct,
		c.self.ID, database, bucket, c.self.ID, c.self.ID)
}
