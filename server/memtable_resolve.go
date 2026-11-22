package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/version"
)

// The memtable is decided once, when the store is created, from the memory the
// node is allowed, and kept from then on (memtable-host-scaled-design.md R-2).
// Re-deriving it at every start would let a node that crashed at a larger size,
// on a host since resized down, ask for a smaller one and be refused by the
// shrink guard. Host sizing therefore runs only when the store directory holds
// no store (no MANIFEST), which has no write-ahead log to replay.

// memtableChoiceFileName is the store's choice, beside the memtable-size marker
// so a copy of the store carries it. The marker means "last opened at" and is
// rewritten at every open; the choice means "what this store runs at when
// nothing is set" and is written once. One file with both meanings is how this class of defect
// happened before.
const memtableChoiceFileName = "memtable-choice.json"

// The choice file's source values.
const (
	memtableSourceHost        = "host"
	memtableSourceNoLimitRead = "no-limit-read"
	memtableSourceUpgrade     = "upgrade"
)

// The sources status reports for an explicit size.
const (
	memtableSourceConfig = "config"
	memtableSourceEnv    = "env"
)

// memtableEnvVar is the variable that sets storage.memtable_mb.
const memtableEnvVar = "HOARDDB_MEMTABLE_MB"

// memtableMemoryLimit reads the memory limit a new store is sized from. Tests
// replace it, so that no server they build depends on the test host's memory.
var memtableMemoryLimit = hostMemoryLimit

// memtableChoice is the content of memtable-choice.json. It is plaintext in an
// encrypted store, as the marker is: it names no data.
type memtableChoice struct {
	MemtableMB       int       `json:"memtable_mb"`
	Source           string    `json:"source"`
	MemoryLimitBytes int64     `json:"memory_limit_bytes"`
	MemoryLimitFrom  string    `json:"memory_limit_from"`
	ChosenAt         time.Time `json:"chosen_at"`
	Build            string    `json:"build"`
}

// memtableInputs is everything resolveMemtable reads from outside the store
// directory.
type memtableInputs struct {
	storeDir string
	// explicitMB is storage.memtable_mb after the environment; 0 is not set.
	explicitMB int
	// explicitFromEnv says HOARDDB_MEMTABLE_MB set it, not the config file.
	explicitFromEnv bool
	// clustered says the node is or may become a cluster member, whose fix
	// for a too-large memtable involves every member.
	clustered   bool
	memoryLimit func() (memoryLimit, error)
	// onLinux is false where hostMemoryLimit always fails, so a failure there
	// is the platform, not a probe error worth a WARN.
	onLinux bool
	now     func() time.Time
}

// memtableInputsFor reads the inputs a start resolves the memtable from.
func memtableInputsFor(cfg *config.Config) memtableInputs {
	return memtableInputs{
		storeDir:        filepath.Join(cfg.Storage.DataDir, nodeStoreDirName),
		explicitMB:      cfg.Storage.MemtableMB,
		explicitFromEnv: os.Getenv(memtableEnvVar) != "",
		clustered:       cfg.Server.ReplicationFactor > 1 || len(cfg.HashRing.SeedNodes) > 0,
		memoryLimit:     memtableMemoryLimit,
		onLinux:         runtime.GOOS == "linux",
		now:             time.Now,
	}
}

// memtableResolution is the size a start applies, and everything the start-up
// line, the WARN, status and gossip say about it.
type memtableResolution struct {
	appliedMB int
	// explicitBy names what set appliedMB when it is not the choice: the
	// environment variable or the config file. Empty when the choice applies.
	explicitBy string
	choice     memtableChoice
	choicePath string
	// chosenNow is true on the start that wrote the choice.
	chosenNow bool
	clustered bool
	// limit is this start's reading of the memory limit, and limitErr why
	// there is none. onLinux says whether a reading was possible at all.
	limit    memoryLimit
	limitErr error
	onLinux  bool
	// hostMB is what a new store on this host would choose now.
	hostMB int
}

// resolveMemtable decides the memtable a start opens the store at:
//  1. an explicit storage.memtable_mb or HOARDDB_MEMTABLE_MB, for this run;
//  2. the store's choice file;
//  3. for a store an earlier build wrote, the size it last ran at, recorded
//     as the choice so that it never shrinks;
//  4. for a new store only, the host sizing, recorded as the choice.
//
// The choice is written durably before the store first opens, so a start that
// dies in between finds it and never sizes again.
func resolveMemtable(inputs memtableInputs) (memtableResolution, error) {
	resolution := memtableResolution{
		choicePath: filepath.Join(inputs.storeDir, memtableChoiceFileName),
		clustered:  inputs.clustered,
		onLinux:    inputs.onLinux,
	}
	resolution.limit, resolution.limitErr = inputs.memoryLimit()
	resolution.hostMB = hostSizedMemtableMB(resolution.limit, resolution.limitErr, inputs.onLinux)

	choice, found, err := readMemtableChoice(resolution.choicePath)
	if err != nil {
		return memtableResolution{}, err
	}
	if !found {
		choice, err = newMemtableChoice(inputs, resolution)
		if err != nil {
			return memtableResolution{}, err
		}
		if err := writeMemtableChoice(inputs.storeDir, resolution.choicePath, choice); err != nil {
			return memtableResolution{}, err
		}
		resolution.chosenNow = true
	}
	resolution.choice = choice
	resolution.appliedMB = choice.MemtableMB
	if inputs.explicitMB != 0 {
		resolution.appliedMB = inputs.explicitMB
		resolution.explicitBy = "storage.memtable_mb in the config file"
		if inputs.explicitFromEnv {
			resolution.explicitBy = memtableEnvVar
		}
	}
	return resolution, nil
}

// hostSizedMemtableMB is what a new store chooses: config.HostMemtableMB of the
// limit, or, when no limit was read, the floor on Linux (a constrained
// container that cannot be read must not get a large memtable) and the
// automatic maximum elsewhere, where no limit is ever read and the hosts are
// developer machines.
func hostSizedMemtableMB(limit memoryLimit, limitErr error, onLinux bool) int {
	switch {
	case limitErr == nil:
		return config.HostMemtableMB(limit.bytes)
	case onLinux:
		return config.MinMemtableMB
	}
	return config.AutoMemtableMaxMB
}

// newMemtableChoice is the choice for a store that has none.
func newMemtableChoice(inputs memtableInputs, resolution memtableResolution) (memtableChoice, error) {
	choice := memtableChoice{
		ChosenAt: inputs.now().UTC(),
		Build:    fmt.Sprintf("%s (%s)", version.Version, version.Commit),
	}
	if resolution.limitErr == nil {
		choice.MemoryLimitBytes = resolution.limit.bytes
		choice.MemoryLimitFrom = memtableLimitOrigin(resolution.limit)
	}
	_, manifestErr := os.Stat(filepath.Join(inputs.storeDir, "MANIFEST"))
	switch {
	case manifestErr == nil:
		lastRunMB, err := storage.PreviousMemtableMB(inputs.storeDir)
		if err != nil {
			return memtableChoice{}, fmt.Errorf("reading the memtable size the store at %s last ran at: %w", inputs.storeDir, err)
		}
		choice.MemtableMB = min(max(lastRunMB, config.MinMemtableMB), config.MaxMemtableMB)
		choice.Source = memtableSourceUpgrade
	case !errors.Is(manifestErr, os.ErrNotExist):
		return memtableChoice{}, fmt.Errorf("checking whether %s holds a store: %w", inputs.storeDir, manifestErr)
	case resolution.limitErr == nil:
		choice.MemtableMB = resolution.hostMB
		choice.Source = memtableSourceHost
	default:
		choice.MemtableMB = resolution.hostMB
		choice.Source = memtableSourceNoLimitRead
	}
	return choice, nil
}

// readMemtableChoice reads the store's choice; found is false when there is
// none.
func readMemtableChoice(choicePath string) (memtableChoice, bool, error) {
	raw, err := os.ReadFile(choicePath)
	if errors.Is(err, os.ErrNotExist) {
		return memtableChoice{}, false, nil
	}
	if err != nil {
		return memtableChoice{}, false, fmt.Errorf("reading this store's memtable choice %s: %w", choicePath, err)
	}
	var choice memtableChoice
	if err := json.Unmarshal(raw, &choice); err != nil || choice.MemtableMB < config.MinMemtableMB || choice.MemtableMB > config.MaxMemtableMB {
		return memtableChoice{}, false, fmt.Errorf("%s cannot be read as this store's memtable choice (%q). "+
			"Delete it and start again: a store that exists then keeps the size it last ran at, and a new one chooses again from this node's memory",
			choicePath, raw)
	}
	return choice, true, nil
}

func writeMemtableChoice(storeDir, choicePath string, choice memtableChoice) error {
	encoded, err := json.Marshal(choice)
	if err != nil {
		return fmt.Errorf("encoding the memtable choice: %w", err)
	}
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		return fmt.Errorf("creating the node store directory %s: %w", storeDir, err)
	}
	if err := storage.WriteFileDurably(choicePath, append(encoded, '\n')); err != nil {
		return fmt.Errorf("saving this store's memtable choice: %w", err)
	}
	return nil
}

// memtableLimitOrigin is where a memory limit came from, in the words the
// start-up line and the choice file use.
func memtableLimitOrigin(limit memoryLimit) string {
	switch limit.source {
	case limitCgroupMax:
		return "memory.max on cgroup " + limit.cgroup
	case limitCgroupHigh:
		return "memory.high on cgroup " + limit.cgroup + ", a throttle line"
	case limitMemTotalNamespaceRoot:
		return "the host's MemTotal; no cgroup memory limit is visible from this cgroup namespace"
	case limitMemTotalV1:
		return "the host's MemTotal; this host uses cgroup v1, whose limits hoardDB does not read"
	}
	return "the host's MemTotal; no cgroup memory limit is set"
}

// formatPeak prints a predicted peak in decimal GB, as the measurement that
// gave the slope was read.
func formatPeak(peakBytes int64) string {
	return fmt.Sprintf("%.2f GB", float64(peakBytes)/1e9)
}

// formatBatch prints the atomic batch bound in whole decimal MB.
func formatBatch(memtableMB int) string {
	return fmt.Sprintf("%d MB", (config.MemtableBatchBytes(memtableMB)+500_000)/1_000_000)
}

// formatGiBUp prints a memory figure a node needs in GiB rounded up to a
// tenth, so "at least" is never below what the figure says: 2.03 GiB prints
// as 2.1, not as the 2.0 the node may already have.
func formatGiBUp(byteCount int64) string {
	const tenthGiB = (1 << 30) / 10
	return fmt.Sprintf("%.1f GiB", float64((byteCount+tenthGiB-1)/tenthGiB)/10)
}

// memtableOverrideHint is the start-up line's closing sentence.
const memtableOverrideHint = "To use another size, set storage.memtable_mb (32 to 1024) in the config file."

// startLine is the INFO every start logs (memtable-host-scaled-design.md §5.1).
func (r memtableResolution) startLine() string {
	applied := r.appliedMB
	burst := fmt.Sprintf("Write bursts can peak near %s; the largest atomic batch is %s.", formatPeak(config.PredictedPeakBytes(applied)), formatBatch(applied))
	saved := fmt.Sprintf("The choice is saved in %s and kept on every restart.", r.choicePath)
	switch {
	case r.explicitBy != "":
		return fmt.Sprintf("memtable: %d MiB, set by %s (this store's own choice, used when it is unset, is %d MiB). %s",
			applied, r.explicitBy, r.choice.MemtableMB, burst)
	case r.choice.Source == memtableSourceUpgrade && r.chosenNow:
		// The store has data and no choice file, and the marker records the
		// last size opened, which may have been an explicit one-off: the line
		// says both plainly.
		return fmt.Sprintf("memtable: %d MiB, the size this store last ran at, kept so it never shrinks, because the store had no %s. "+
			"That size is now this store's choice, even if a setting asked for it at the last run. A new store on this host would choose %d MiB. %s %s %s",
			applied, memtableChoiceFileName, r.hostMB, burst, saved, memtableOverrideHint)
	case r.choice.Source == memtableSourceUpgrade:
		return fmt.Sprintf("memtable: %d MiB, the size this store kept on %s from its last run, when it had no %s, so that it never shrinks. A new store on this host would choose %d MiB. %s %s",
			applied, r.choice.ChosenAt.Format("2006-01-02"), memtableChoiceFileName, r.hostMB, burst, memtableOverrideHint)
	case r.chosenNow && r.choice.Source == memtableSourceHost:
		return fmt.Sprintf("memtable: %d MiB, chosen now for this new store from this node's memory limit of %s (%s). "+
			"Write bursts can peak near %s of memory at this size; the largest atomic batch is %s. "+
			"The choice is saved in %s and kept on every restart, even if this host's memory changes. %s",
			applied, formatBytesIEC(r.choice.MemoryLimitBytes), r.choice.MemoryLimitFrom,
			formatPeak(config.PredictedPeakBytes(applied)), formatBatch(applied), r.choicePath, memtableOverrideHint)
	case r.chosenNow:
		return fmt.Sprintf("memtable: %d MiB, chosen now for this new store without a memory limit: %s. "+
			"Write bursts can peak near %s of memory at this size; the largest atomic batch is %s. %s %s",
			applied, r.noLimitReason(), formatPeak(config.PredictedPeakBytes(applied)), formatBatch(applied), saved, memtableOverrideHint)
	case r.choice.Source == memtableSourceHost:
		return fmt.Sprintf("memtable: %d MiB, the size this store chose at its first start on %s (memory limit then %s, now %s). %s %s",
			applied, r.choice.ChosenAt.Format("2006-01-02"), formatBytesIEC(r.choice.MemoryLimitBytes), r.currentLimitText(), burst, memtableOverrideHint)
	}
	return fmt.Sprintf("memtable: %d MiB, the size this store chose at its first start on %s, when its memory limit could not be read (memory limit now %s). %s %s",
		applied, r.choice.ChosenAt.Format("2006-01-02"), r.currentLimitText(), burst, memtableOverrideHint)
}

// noLimitReason says why a new store was sized without a memory limit.
func (r memtableResolution) noLimitReason() string {
	if r.onLinux {
		return fmt.Sprintf("this node's memory limit could not be read (%v), so it takes the smallest size", r.limitErr)
	}
	return fmt.Sprintf("memory limits are read on Linux only, so it takes %d MiB, the largest size a node chooses for itself", config.AutoMemtableMaxMB)
}

func (r memtableResolution) currentLimitText() string {
	if r.limitErr != nil {
		return "unreadable"
	}
	return formatBytesIEC(r.limit.bytes)
}

// warnings are the WARN lines a start logs, in order.
func (r memtableResolution) warnings() []string {
	var lines []string
	if r.chosenNow && r.choice.Source == memtableSourceNoLimitRead && r.onLinux {
		lines = append(lines, fmt.Sprintf("memtable: this node's memory limit could not be read (%v), so this new store takes the smallest memtable, %d MiB, "+
			"rather than risk a large one in a constrained container. If this node has 3.5 GiB of memory or more, stop it cleanly (SIGTERM) "+
			"and set storage.memtable_mb: 64 in the config file (%d from 7 GiB); otherwise nothing needs doing",
			r.limitErr, config.MinMemtableMB, config.AutoMemtableMaxMB))
	}
	if warning, warns := r.tooLargeWarning(); warns {
		lines = append(lines, warning)
	}
	return lines
}

// tooLargeWarning is the WARN for a memtable whose predicted peak is over a
// third of the memory limit, whatever set it (memtable-host-scaled-design.md
// §5.2). A third is a policy line, not a measured cliff. It is silent when no
// limit was read: there is nothing to compare with.
func (r memtableResolution) tooLargeWarning() (string, bool) {
	if r.limitErr != nil {
		return "", false
	}
	return memtableTooLargeText(memtableFit{
		memtableMB: r.appliedMB,
		limitBytes: r.limit.bytes,
		hostMB:     r.hostMB,
		clustered:  r.clustered,
	})
}

// memtableFit is a memtable size against a node's memory limit.
type memtableFit struct {
	memtableMB int
	limitBytes int64
	// hostMB is the size a new store on this host would choose: the smaller
	// size the WARN offers.
	hostMB    int
	clustered bool
}

// tooLarge reports whether the predicted peak is over a third of the limit.
func (f memtableFit) tooLarge() bool {
	return config.PredictedPeakBytes(f.memtableMB) > f.limitBytes/3
}

// memoryNeeded is the limit under which this memtable would not warn.
func (f memtableFit) memoryNeeded() int64 {
	return 3 * config.PredictedPeakBytes(f.memtableMB)
}

func memtableTooLargeText(fit memtableFit) (string, bool) {
	if !fit.tooLarge() {
		return "", false
	}
	peak := config.PredictedPeakBytes(fit.memtableMB)
	percent := (peak*100 + fit.limitBytes/2) / fit.limitBytes
	head := fmt.Sprintf("memtable: %d MiB is large for this node's memory limit of %s: write bursts can peak near %s, %d%% of the limit, before caches and other work. ",
		fit.memtableMB, formatBytesIEC(fit.limitBytes), formatPeak(peak), percent)
	needed := formatGiBUp(fit.memoryNeeded())
	switch {
	case fit.hostMB >= fit.memtableMB:
		return head + fmt.Sprintf("%d MiB is the smallest size, so give this node at least %s of memory.", fit.memtableMB, needed), true
	case fit.clustered:
		return head + fmt.Sprintf("Give this node at least %s of memory, or lower it to %d MiB on every member, one at a time, stopping each cleanly (SIGTERM); "+
			"a member alone at another size is refused by its peers.", needed, fit.hostMB), true
	}
	return head + fmt.Sprintf("Give this node at least %s of memory, or stop it cleanly (SIGTERM) and set storage.memtable_mb: %d in the config file "+
		"(the size a new store on this host would choose).", needed, fit.hostMB), true
}

// healthMemtable is status's view of the resolution, with the members that
// gossip a different size.
func (r memtableResolution) healthMemtable(differs []MemtablePeer) *HealthMemtable {
	source := r.choice.Source
	switch r.explicitBy {
	case "":
	case memtableEnvVar:
		source = memtableSourceEnv
	default:
		source = memtableSourceConfig
	}
	health := &HealthMemtable{
		MB:                   r.appliedMB,
		Source:               source,
		ChoiceMB:             r.choice.MemtableMB,
		ChosenFromLimitBytes: r.choice.MemoryLimitBytes,
		ChosenAt:             r.choice.ChosenAt.Unix(),
		BatchBytes:           config.MemtableBatchBytes(r.appliedMB),
		Differs:              differs,
	}
	if r.limitErr == nil {
		health.LimitBytes = r.limit.bytes
	}
	_, health.Large = r.tooLargeWarning()
	return health
}
