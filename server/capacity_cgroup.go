package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// cgroupPaths names the files the memory-limit walk reads. Production reads
// the host (hostCgroupPaths); a test points every field into a t.TempDir()
// tree. root prefixes every mount point read from mountinfo, which is how a
// temporary directory stands in for /sys/fs/cgroup.
type cgroupPaths struct {
	procCgroup    string
	procMountinfo string
	procMeminfo   string
	root          string
}

// hostCgroupPaths are the real files the walk reads on a Linux host.
var hostCgroupPaths = cgroupPaths{
	procCgroup:    "/proc/self/cgroup",
	procMountinfo: "/proc/self/mountinfo",
	procMeminfo:   "/proc/meminfo",
	root:          "/",
}

// memoryLimitSource says where the memory ratio's denominator came from. The
// sampler announces it, because a limit set by memory.high means "throttled",
// not "full" (node-capacity-build.md §0.2).
type memoryLimitSource int

const (
	// limitCgroupMax: the smallest limit is a memory.max, a hard limit.
	limitCgroupMax memoryLimitSource = iota + 1
	// limitCgroupHigh: the smallest limit is a memory.high, a throttle line.
	limitCgroupHigh
	// limitMemTotalNoLimit: the walk reached the true root cgroup and no level
	// sets a limit, so the host's MemTotal is the honest upper bound.
	limitMemTotalNoLimit
	// limitMemTotalNamespaceRoot: the walk stopped at a cgroup-namespace
	// root with no limit; a limit above the namespace would not be visible.
	limitMemTotalNamespaceRoot
	// limitMemTotalV1: the memory controller is cgroup v1's, which this probe
	// does not read, so MemTotal may overstate the limit.
	limitMemTotalV1
)

// memoryLimit is one resolved memory limit. bytes is the ratio's denominator.
// hardBytes is the smallest memory.max alone, or 0 when no level sets one,
// so the announcement can name the hard limit behind a memory.high. When a
// memory.high is the limit and no memory.max is set, the hard bound is the
// host's MemTotal, and hostMemTotal carries it for that announcement.
type memoryLimit struct {
	bytes        int64
	source       memoryLimitSource
	cgroup       string
	hardBytes    int64
	hardCgroup   string
	hostMemTotal int64
}

// cgroupMount is one cgroup2 line of mountinfo: the mount's root in the
// hierarchy and where it is mounted.
type cgroupMount struct {
	hierarchyRoot string
	mountPoint    string
}

// readMemoryLimit resolves the memory limit the kernel applies to this
// process (node-capacity-spec.md §3.2, with build doc §0.2 and §0.3 item 1).
// It never turns a failed read into MemTotal: MemTotal is returned only when
// the walk proves no visible level sets a limit, or when the host's memory
// controller is cgroup v1's.
func readMemoryLimit(p cgroupPaths) (memoryLimit, error) {
	cgroupLines, err := readLines(p.procCgroup)
	if err != nil {
		return memoryLimit{}, fmt.Errorf("reading %s: %w", p.procCgroup, err)
	}
	// A hybrid host mounts cgroup2 but leaves the memory controller on a v1
	// hierarchy. Its v2 tree then shows no memory controller anywhere, and
	// reading it would report "no limit" while a v1 limit may apply, so the
	// v1 line decides before mountinfo is looked at.
	if memoryOnV1(cgroupLines) {
		return memTotalLimit(p, limitMemTotalV1)
	}
	v2Path, hasV2 := v2CgroupPath(cgroupLines)
	mounts, err := cgroup2Mounts(p.procMountinfo)
	if err != nil {
		return memoryLimit{}, err
	}
	if !hasV2 && len(mounts) == 0 {
		return memTotalLimit(p, limitMemTotalV1)
	}
	if !hasV2 {
		return memoryLimit{}, fmt.Errorf("%s has no cgroup v2 (0::) line, but a cgroup2 filesystem is mounted; this process's cgroup cannot be located", p.procCgroup)
	}
	mount, ok := longestMountFor(mounts, v2Path)
	if !ok {
		return memoryLimit{}, fmt.Errorf("cgroup %s is under no cgroup2 mount in %s; the process may be in a cgroup namespace this mount does not show", v2Path, p.procMountinfo)
	}
	return walkMemoryLimit(p, mount, v2Path)
}

// memoryOnV1 reports whether any v1 hierarchy line of /proc/self/cgroup
// lists the memory controller. The match is on an exact comma-separated
// token, so a named hierarchy such as "name=systemd" never matches.
func memoryOnV1(cgroupLines []string) bool {
	for _, line := range cgroupLines {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 || fields[0] == "0" {
			continue
		}
		for _, controller := range strings.Split(fields[1], ",") {
			if controller == "memory" {
				return true
			}
		}
	}
	return false
}

// v2CgroupPath returns the path of the "0::<path>" line, the process's
// cgroup in the unified hierarchy.
func v2CgroupPath(cgroupLines []string) (string, bool) {
	for _, line := range cgroupLines {
		if cgPath, ok := strings.CutPrefix(line, "0::"); ok {
			return cgPath, true
		}
	}
	return "", false
}

// cgroup2Mounts lists the cgroup2 mounts in a mountinfo file. The filesystem
// type is the first field after the " - " separator; fields 4 and 5 are the
// mount's root in the hierarchy and its mount point (proc(5)).
func cgroup2Mounts(mountinfoPath string) ([]cgroupMount, error) {
	lines, err := readLines(mountinfoPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", mountinfoPath, err)
	}
	var mounts []cgroupMount
	for _, line := range lines {
		before, after, found := strings.Cut(line, " - ")
		if !found {
			continue
		}
		fsFields := strings.Fields(after)
		if len(fsFields) == 0 || fsFields[0] != "cgroup2" {
			continue
		}
		mountFields := strings.Fields(before)
		if len(mountFields) < 5 {
			return nil, fmt.Errorf("%s has a cgroup2 line with %d fields before the separator, want at least 5: %q", mountinfoPath, len(mountFields), line)
		}
		mounts = append(mounts, cgroupMount{
			hierarchyRoot: unescapeMountinfo(mountFields[3]),
			mountPoint:    unescapeMountinfo(mountFields[4]),
		})
	}
	return mounts, nil
}

// unescapeMountinfo undoes mountinfo's octal escapes (\040 for a space, and
// the like), which the kernel writes for whitespace and backslashes in paths.
func unescapeMountinfo(field string) string {
	if !strings.Contains(field, `\`) {
		return field
	}
	var out strings.Builder
	for i := 0; i < len(field); i++ {
		if field[i] == '\\' && i+3 < len(field) {
			if code, err := strconv.ParseUint(field[i+1:i+4], 8, 8); err == nil {
				out.WriteByte(byte(code))
				i += 3
				continue
			}
		}
		out.WriteByte(field[i])
	}
	return out.String()
}

// longestMountFor picks the cgroup2 mount whose hierarchy root is the longest
// path-component prefix of cgPath.
func longestMountFor(mounts []cgroupMount, cgPath string) (cgroupMount, bool) {
	var best cgroupMount
	found := false
	for _, m := range mounts {
		if !pathHasPrefix(cgPath, m.hierarchyRoot) {
			continue
		}
		if !found || len(m.hierarchyRoot) > len(best.hierarchyRoot) {
			best, found = m, true
		}
	}
	return best, found
}

// pathHasPrefix reports whether prefix is cgPath or one of its ancestors, by
// whole path components: "/a" is a prefix of "/a/b" but not of "/ab".
func pathHasPrefix(cgPath, prefix string) bool {
	if prefix == "/" {
		return strings.HasPrefix(cgPath, "/")
	}
	return cgPath == prefix || strings.HasPrefix(cgPath, prefix+"/")
}

// memoryWalk accumulates the candidates the walk finds.
type memoryWalk struct {
	limit memoryLimit
	found bool
}

// offer records one level's limit file. On an equal value memory.max wins: a
// node at a value that is both its throttle line and its hard limit is at
// the hard limit, and "full" is then the true word.
func (w *memoryWalk) offer(limitBytes int64, source memoryLimitSource, cgroup string) {
	if source == limitCgroupMax && (w.limit.hardBytes == 0 || limitBytes < w.limit.hardBytes) {
		w.limit.hardBytes, w.limit.hardCgroup = limitBytes, cgroup
	}
	better := !w.found || limitBytes < w.limit.bytes ||
		(limitBytes == w.limit.bytes && source == limitCgroupMax && w.limit.source == limitCgroupHigh)
	if better {
		w.limit.bytes, w.limit.source, w.limit.cgroup = limitBytes, source, cgroup
		w.found = true
	}
}

// walkMemoryLimit walks from the process's cgroup up to the mount point,
// inclusive, and takes the smallest memory.max or memory.high on the way.
func walkMemoryLimit(p cgroupPaths, mount cgroupMount, v2Path string) (memoryLimit, error) {
	rel := strings.TrimPrefix(strings.TrimPrefix(v2Path, mount.hierarchyRoot), "/")
	if rel != "" && !filepath.IsLocal(rel) {
		return memoryLimit{}, fmt.Errorf("cgroup %s leaves its cgroup2 mount root %s; the process's cgroup is not visible from this namespace", v2Path, mount.hierarchyRoot)
	}
	mountDir, err := filepath.EvalSymlinks(filepath.Join(p.root, mount.mountPoint))
	if err != nil {
		return memoryLimit{}, fmt.Errorf("resolving the cgroup2 mount point %s: %w", mount.mountPoint, err)
	}
	leafDir, err := filepath.EvalSymlinks(filepath.Join(mountDir, rel))
	if err != nil {
		return memoryLimit{}, fmt.Errorf("this process's cgroup directory for %s cannot be read: %w", v2Path, err)
	}
	if leafRel, err := filepath.Rel(mountDir, leafDir); err != nil || !filepath.IsLocal(leafRel) {
		return memoryLimit{}, fmt.Errorf("this process's cgroup directory for %s resolves to %s, outside the cgroup2 mount %s", v2Path, leafDir, mountDir)
	}

	var walk memoryWalk
	for dir := leafDir; ; dir = filepath.Dir(dir) {
		levelRel, err := filepath.Rel(mountDir, dir)
		if err != nil {
			return memoryLimit{}, fmt.Errorf("placing %s under the cgroup2 mount %s: %w", dir, mountDir, err)
		}
		cgroup := path.Join(mount.hierarchyRoot, filepath.ToSlash(levelRel))
		isMount := dir == mountDir

		hasType, err := cgroupFileExists(filepath.Join(dir, "cgroup.type"))
		if err != nil {
			return memoryLimit{}, err
		}
		if !hasType {
			// Only the true root cgroup has no cgroup.type, and it has no
			// memory.max either (cgroup-v2.rst lists both as non-root files).
			if !isMount {
				return memoryLimit{}, fmt.Errorf("cgroup %s has no cgroup.type but is not the cgroup2 mount point %s; the cgroup layout is inconsistent", cgroup, mount.mountPoint)
			}
			return finishMemoryLimit(p, walk, limitMemTotalNoLimit)
		}
		if err := readLevelLimits(dir, cgroup, &walk); err != nil {
			return memoryLimit{}, err
		}
		if isMount {
			// A mount point that has cgroup.type is a namespace root: the
			// ancestors above it exist but this process cannot see them.
			return finishMemoryLimit(p, walk, limitMemTotalNamespaceRoot)
		}
	}
}

// readLevelLimits reads one non-root level. A level without the memory
// controller imposes no limit; a level with it must have both memory.max and
// memory.high readable, and each is "max" or a positive byte count.
func readLevelLimits(dir, cgroup string, walk *memoryWalk) error {
	controllersPath := filepath.Join(dir, "cgroup.controllers")
	controllers, err := os.ReadFile(controllersPath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", controllersPath, err)
	}
	if !containsField(string(controllers), "memory") {
		return nil
	}
	for _, file := range []struct {
		name   string
		source memoryLimitSource
	}{
		{name: "memory.max", source: limitCgroupMax},
		{name: "memory.high", source: limitCgroupHigh},
	} {
		limitBytes, limited, err := readCgroupLimitFile(filepath.Join(dir, file.name))
		if err != nil {
			return fmt.Errorf("cgroup %s: %w", cgroup, err)
		}
		if limited {
			walk.offer(limitBytes, file.source, cgroup)
		}
	}
	return nil
}

// readCgroupLimitFile reads a memory.max or memory.high file: "max" is no
// limit, a positive integer is a limit, and anything else, including 0 or a
// missing file, is an error.
func readCgroupLimitFile(limitPath string) (int64, bool, error) {
	raw, err := os.ReadFile(limitPath)
	if err != nil {
		return 0, false, fmt.Errorf("reading %s: %w", limitPath, err)
	}
	text := strings.TrimSpace(string(raw))
	if text == "max" {
		return 0, false, nil
	}
	limitBytes, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("%s holds %q, which is neither \"max\" nor a byte count", limitPath, text)
	}
	if limitBytes <= 0 {
		return 0, false, fmt.Errorf("%s holds %d; a memory limit must be positive", limitPath, limitBytes)
	}
	return limitBytes, true, nil
}

// finishMemoryLimit returns the smallest candidate, or MemTotal when the walk
// found none. noLimit says why there was none.
func finishMemoryLimit(p cgroupPaths, walk memoryWalk, noLimit memoryLimitSource) (memoryLimit, error) {
	if !walk.found {
		return memTotalLimit(p, noLimit)
	}
	if walk.limit.source == limitCgroupHigh && walk.limit.hardBytes == 0 {
		// A throttle line with no hard limit behind it: the announcement names
		// MemTotal as the hard bound, so it is read, and a failure to read it
		// is a failure like any other, not a silent gap in the text.
		memTotal, err := readMeminfoField(p.procMeminfo, "MemTotal")
		if err != nil {
			return memoryLimit{}, err
		}
		walk.limit.hostMemTotal = memTotal
	}
	return walk.limit, nil
}

// memTotalLimit is the host's MemTotal as the limit. It is used only when a
// walk has proved there is nothing smaller to use, never after a failed read.
func memTotalLimit(p cgroupPaths, source memoryLimitSource) (memoryLimit, error) {
	memTotal, err := readMeminfoField(p.procMeminfo, "MemTotal")
	if err != nil {
		return memoryLimit{}, err
	}
	return memoryLimit{bytes: memTotal, source: source}, nil
}

// readMeminfoField reads one "<field>: <n> kB" line of a /proc/meminfo-style
// file, in bytes. A missing field is an error, never a zero.
func readMeminfoField(meminfoPath, field string) (int64, error) {
	lines, err := readLines(meminfoPath)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", meminfoPath, err)
	}
	for _, line := range lines {
		if value, ok := strings.CutPrefix(line, field+":"); ok {
			return parseKilobytes(meminfoPath, field, value)
		}
	}
	return 0, fmt.Errorf("%s has no %s line; the memory reading cannot be trusted", meminfoPath, field)
}

// parseKilobytes parses the value part of a "<n> kB" line into bytes.
func parseKilobytes(sourcePath, field, value string) (int64, error) {
	parts := strings.Fields(value)
	if len(parts) != 2 || parts[1] != "kB" {
		return 0, fmt.Errorf("%s: the %s line %q is not \"<n> kB\"", sourcePath, field, strings.TrimSpace(value))
	}
	kilobytes, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || kilobytes < 0 {
		return 0, fmt.Errorf("%s: the %s value %q is not a byte count", sourcePath, field, parts[0])
	}
	return kilobytes * 1024, nil
}

// containsField reports whether token is one of s's whitespace-separated
// fields.
func containsField(s, token string) bool {
	for _, field := range strings.Fields(s) {
		if field == token {
			return true
		}
	}
	return false
}

// cgroupFileExists distinguishes "absent" from "could not tell": only ErrNotExist
// is absence, and any other error is returned.
func cgroupFileExists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("checking %s: %w", filePath, err)
}

// readLines reads a small text file as lines.
func readLines(filePath string) ([]string, error) {
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var lines []string
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("splitting %s into lines: %w", filePath, err)
	}
	return lines, nil
}
