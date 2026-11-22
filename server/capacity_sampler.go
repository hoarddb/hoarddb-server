package server

import (
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// capacitySampleInterval is Prometheus's default scrape interval; it is
	// not configurable (node-capacity-spec.md OQ-4).
	capacitySampleInterval = 15 * time.Second
	// capacityStaleAfter is when status stops trusting the last sample: a
	// sampler stuck in a probe keeps its old rows, and they must not read ok.
	capacityStaleAfter = 3 * capacitySampleInterval
	// capacityPolicyThreshold is the red line for every resource but a disk
	// with the join guard on. It is policy, not a measured cliff.
	capacityPolicyThreshold = 0.80
	// capacityAmberBelow puts disk's early warning this far below its red
	// line, when the join guard is on.
	capacityAmberBelow = 0.10
	// capacityRecoveryBelow is the hysteresis: a resource leaves red, or disk
	// leaves amber, only this far below its line, so a reading on the line
	// does not flap between a WARN and an INFO every sample.
	capacityRecoveryBelow = 0.05
)

// Values of the resource label.
const (
	resourceFDs      = "fds"
	resourceMappings = "mappings"
	resourceMemory   = "memory"
	resourceDisk     = "disk"
	// resourceBlobDisk is the blob directory's filesystem, a resource only
	// on a node whose blob directory is on a filesystem of its own.
	resourceBlobDisk = "blob_disk"
)

// capacityResources is the closed set of resource label values, in the
// order every loop and every status row uses.
var capacityResources = []string{resourceFDs, resourceMappings, resourceMemory, resourceDisk}

// capacityState is a resource's state as status reports it. stale is not a
// stored state: healthCapacity derives it from the sample's age.
type capacityState int

const (
	stateOK capacityState = iota + 1
	stateApproaching
	stateRed
	stateUnavailable
)

// stateStale is status's word for a row whose sample is too old to trust.
const stateStale = "stale"

func (st capacityState) String() string {
	switch st {
	case stateOK:
		return "ok"
	case stateApproaching:
		return "approaching"
	case stateRed:
		return "red"
	case stateUnavailable:
		return "unavailable"
	}
	return fmt.Sprintf("capacityState(%d)", int(st))
}

// capacityRowSample is one resource in one sample. hasRatio is false when
// the resource could not be read, so status never shows a ratio of 0 for it.
type capacityRowSample struct {
	resource  string
	ratio     float64
	hasRatio  bool
	threshold float64
	state     capacityState
}

// capacitySample is the last completed sample, which status reads.
type capacitySample struct {
	sampledAt time.Time
	rows      []capacityRowSample
}

// resourceTrack is what the sampler remembers about one resource between
// samples. Owned by sampleMu.
type resourceTrack struct {
	state capacityState
	// lastWarned is when the WARN for the current state was last written:
	// red, approaching or unavailable repeat at most once an hour.
	lastWarned time.Time
}

// resourceReading is one successful read of a resource, validated.
type resourceReading struct {
	ratio float64
	// redNow is the raw red test for this reading, before hysteresis.
	redNow bool
	// redText and amberText are the WARNs this reading would write.
	redText, amberText string
	// publish sets the resource's reading and limit series.
	publish func(m *capacityMetrics)
	// memLimit is the memory reading's denominator, announced when it changes.
	memLimit memoryLimit
}

// capacitySampler reads every capacity probe on a timer, off the request and
// scrape paths, publishes what it read, and says in the log when an operator
// must act. A scrape reads series that are already set and does no I/O.
type capacitySampler struct {
	s       *Server
	metrics *capacityMetrics
	logger  *slog.Logger

	// diskLimitPct and dataDir are copied from the config once, at
	// construction: tests write DiskHighWaterPct on running servers, and a
	// sampler that read it each time would race them and could disagree with
	// the threshold it published.
	diskLimitPct int
	dataDir      string
	thresholds   map[string]float64
	// resources is capacityResources, plus resourceBlobDisk when the blob
	// directory is on a filesystem of its own (Server.blobDiskSeparate): the
	// resource label values this sampler reads and status shows, in order.
	resources []string
	// blobRoot is the blob directory, read only when resources carries
	// resourceBlobDisk.
	blobRoot string

	// sampleMu serialises samples and owns every field down to lastMu.
	sampleMu sync.Mutex
	tracks   map[string]*resourceTrack
	// dataFailedAt is when each failing data-bytes part was last warned
	// about; a part that reads again is removed.
	dataFailedAt map[string]time.Time
	// announcedLimit is the memory limit the log last announced.
	announcedLimit memoryLimit
	limitAnnounced bool
	v1Warned       time.Time

	// lastMu guards last, which status reads. It is never held while a probe
	// runs, so status never waits behind a stuck probe.
	lastMu sync.Mutex
	last   capacitySample

	// exited is closed when the sampler goroutine returns.
	exited chan struct{}
}

// newCapacitySampler builds a sampler and its registry for s. It starts
// nothing; startCapacitySampler does.
func newCapacitySampler(s *Server) (*capacitySampler, error) {
	diskLimitPct := s.config.Server.DiskHighWaterPct
	resources := capacityResources
	if s.blobDiskSeparate {
		resources = append(slices.Clone(capacityResources), resourceBlobDisk)
	}
	thresholds := make(map[string]float64, len(resources))
	tracks := make(map[string]*resourceTrack, len(resources))
	for _, resource := range resources {
		thresholds[resource] = capacityPolicyThreshold
		// With the guard on, a disk's red line is the guard's own mark, so
		// the gauge and this node's vote cannot disagree. With it off, a full
		// disk is still a full disk, so the policy line still warns (spec
		// §5.1). The blob filesystem takes the same line as the data one: the
		// guard reads both against one mark.
		if isDiskResource(resource) && diskLimitPct > 0 {
			thresholds[resource] = float64(diskLimitPct) / 100
		}
		tracks[resource] = &resourceTrack{}
	}
	metrics, err := newCapacityMetrics(thresholds, resources)
	if err != nil {
		return nil, fmt.Errorf("building the capacity metrics: %w", err)
	}
	metrics.memtableBytes.Set(float64(int64(s.memtable.appliedMB) << 20))
	if s.storeSeal.sealed {
		metrics.encryptionSealed.Set(1)
	}
	metrics.encryptionInfo.WithLabelValues(s.config.Encryption.Mode, s.config.Encryption.Keystore).Set(1)
	return &capacitySampler{
		s:            s,
		metrics:      metrics,
		logger:       s.logger,
		diskLimitPct: diskLimitPct,
		dataDir:      s.config.Storage.DataDir,
		thresholds:   thresholds,
		resources:    resources,
		blobRoot:     s.blobRoot(),
		tracks:       tracks,
		dataFailedAt: make(map[string]time.Time, len(capacityDataParts)),
		exited:       make(chan struct{}),
	}, nil
}

// CapacityFirstSampleGate, when set before a server starts, is called on the
// sampler goroutine and must return before the first sample is taken. It is a
// test seam: the first sample logs the memory limit, and a test in
// another package can only prove it reads its log safely if it chooses when
// that background write happens. Unset, it costs one atomic load at start.
var CapacityFirstSampleGate atomic.Pointer[func()]

// startCapacitySampler runs cs until the server's context ends. The first
// sample is taken at once: without it a fresh node has no capacity series and
// reads stale in status for a whole interval. The sampler runs whatever
// Metrics.Enabled says; with metrics off, one INFO says where its output goes.
func (s *Server) startCapacitySampler(cs *capacitySampler) {
	if !s.config.Metrics.Enabled {
		cs.logger.Info(metricsDisabledText)
	}
	s.capacity = cs
	s.capacityWG.Add(1)
	go func() {
		// Done is deferred first so it runs last: exited is closed before
		// Stop's Wait can return, so "Stop returned" implies "exited closed".
		defer s.capacityWG.Done()
		defer close(cs.exited)
		if gate := CapacityFirstSampleGate.Load(); gate != nil {
			(*gate)()
		}
		cs.sampleNow()
		ticker := time.NewTicker(capacitySampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				cs.sampleNow()
			}
		}
	}()
}

// sampleNow takes one sample of every resource and publishes it. The probe
// set is loaded once, so a probe override mid-sample cannot split one sample
// across two probe sets.
func (cs *capacitySampler) sampleNow() {
	cs.sampleMu.Lock()
	defer cs.sampleMu.Unlock()

	probes := cs.s.probes.Load()
	now := probes.now()
	sample := capacitySample{sampledAt: now, rows: make([]capacityRowSample, 0, len(cs.resources))}

	fdsReading, err := cs.readFDs(probes)
	sample.rows = append(sample.rows, cs.sampleResource(resourceFDs, fdsReading, err, now))
	mappingsReading, err := cs.readMappings(probes)
	sample.rows = append(sample.rows, cs.sampleResource(resourceMappings, mappingsReading, err, now))
	memoryReading, err := cs.readMemory(probes)
	sample.rows = append(sample.rows, cs.sampleResource(resourceMemory, memoryReading, err, now))
	if err == nil {
		cs.announceMemoryLimit(memoryReading.memLimit, now)
	}
	diskReading, err := cs.readDisk(probes, resourceDisk)
	sample.rows = append(sample.rows, cs.sampleResource(resourceDisk, diskReading, err, now))
	if slices.Contains(cs.resources, resourceBlobDisk) {
		blobReading, err := cs.readDisk(probes, resourceBlobDisk)
		sample.rows = append(sample.rows, cs.sampleResource(resourceBlobDisk, blobReading, err, now))
	}
	cs.sampleDataBytes(probes, now)
	// The bucket count has no probe and so no failure mode: it is absent
	// only before the first sample.
	cs.metrics.buckets.WithLabelValues().Set(float64(cs.s.bucketsAttached.Load()))

	cs.lastMu.Lock()
	cs.last = sample
	cs.lastMu.Unlock()
	cs.metrics.lastSample.Set(float64(now.UnixNano()) / 1e9)
	// The tick that already exists prints the dead-letter names a closed
	// window held, so a burst followed by silence is still said.
	cs.s.flushDeadLetterLog()
}

// sampleResource publishes one resource's reading, or hands its failure to
// applyProbeFailure, moves its state and writes what the move calls for.
func (cs *capacitySampler) sampleResource(resource string, reading resourceReading, readErr error, now time.Time) capacityRowSample {
	row := capacityRowSample{resource: resource, threshold: cs.thresholds[resource]}
	if readErr != nil {
		cs.applyProbeFailure(resource, readErr, now)
		row.state = stateUnavailable
		return row
	}
	reading.publish(cs.metrics)
	cs.metrics.ratio.WithLabelValues(resource).Set(reading.ratio)

	track := cs.tracks[resource]
	prev := track.state
	next := cs.nextState(resource, prev, reading)
	if prev == stateUnavailable {
		cs.logger.Info(readAgainText(resource, reading.ratio), "resource", resource)
	}
	cs.announceTransition(resource, track, next, reading, now)
	track.state = next
	row.ratio, row.hasRatio, row.state = reading.ratio, true, next
	return row
}

// nextState moves a resource's state with hysteresis. Red is left only below
// threshold − capacityRecoveryBelow; disk's amber line (guard on only) is
// left only capacityRecoveryBelow under itself.
func (cs *capacitySampler) nextState(resource string, prev capacityState, reading resourceReading) capacityState {
	threshold := cs.thresholds[resource]
	if reading.redNow || (prev == stateRed && reading.ratio >= threshold-capacityRecoveryBelow) {
		return stateRed
	}
	if isDiskResource(resource) && cs.diskLimitPct > 0 {
		amberLine := threshold - capacityAmberBelow
		if reading.ratio >= amberLine || (prev == stateApproaching && reading.ratio >= amberLine-capacityRecoveryBelow) {
			return stateApproaching
		}
	}
	return stateOK
}

// announceTransition writes the WARN or INFO a state move calls for. A WARN
// is written on a crossing up into red or approaching, and repeated at most
// once an hour while the state holds. Leaving red writes the recovery INFO;
// a drop from red into approaching writes no amber WARN, because that WARN
// is for upward crossings. Leaving approaching downwards is silent.
func (cs *capacitySampler) announceTransition(resource string, track *resourceTrack, next capacityState, reading resourceReading, now time.Time) {
	prev := track.state
	threshold := cs.thresholds[resource]
	attrs := []any{"resource", resource, "ratio", reading.ratio, "threshold", threshold}
	switch {
	case next == stateRed && (prev != stateRed || now.Sub(track.lastWarned) >= reclaimWarnInterval):
		cs.logger.Warn(reading.redText, attrs...)
		track.lastWarned = now
	case next != stateRed && prev == stateRed:
		cs.logger.Info(recoveryText(resource, reading.ratio, threshold), attrs...)
	case next == stateApproaching && (prev != stateApproaching || now.Sub(track.lastWarned) >= reclaimWarnInterval):
		cs.logger.Warn(reading.amberText, attrs...)
		track.lastWarned = now
	}
}

// applyProbeFailure is the only code on a probe's error path. It only
// deletes: the resource's ratio, reading and limit series go, its error
// counter rises, and status shows it as unavailable. The threshold stays, so
// an alert on "threshold without ratio" fires. The failure is written to the
// log whatever Metrics.Enabled says, once per episode and then hourly, and
// never as a recovery: the resource did not recover, it stopped being
// measured.
func (cs *capacitySampler) applyProbeFailure(resource string, readErr error, now time.Time) {
	cs.metrics.deleteResource(resource)
	cs.metrics.sampleErrors.WithLabelValues(resource).Inc()
	track := cs.tracks[resource]
	if track.state != stateUnavailable || now.Sub(track.lastWarned) >= reclaimWarnInterval {
		cs.logger.Warn(probeFailureText(resource, readErr, cs.whatToCheck(resource)), "resource", resource, "error", readErr)
		track.lastWarned = now
	}
	track.state = stateUnavailable
}

// whatToCheck names what an operator should look at when a resource cannot
// be read.
func (cs *capacitySampler) whatToCheck(resource string) string {
	switch resource {
	case resourceFDs:
		return "check that /proc/self/fd can be listed and that RLIMIT_NOFILE can be read"
	case resourceMappings:
		return "check that /proc/self/maps and /proc/sys/vm/max_map_count can be read"
	case resourceMemory:
		return "check that /proc/self/status can be read, and this process's cgroup directory under the cgroup2 mount (see /proc/self/cgroup and /proc/self/mountinfo)"
	case resourceBlobDisk:
		return fmt.Sprintf("check the blob directory %s and the filesystem it is on", cs.blobRoot)
	}
	return fmt.Sprintf("check the data directory %s and the filesystem it is on", cs.dataDir)
}

// announceMemoryLimit writes where the memory ratio's denominator comes from:
// at the first sample, and again only when its source, cgroup or value
// changes. A cgroup v1 host gets a WARN instead, at the first sample and
// hourly, because its real limit is not read.
func (cs *capacitySampler) announceMemoryLimit(limit memoryLimit, now time.Time) {
	if limit.source == limitMemTotalV1 {
		wasV1 := cs.limitAnnounced && cs.announcedLimit.source == limitMemTotalV1
		if !wasV1 || now.Sub(cs.v1Warned) >= reclaimWarnInterval {
			cs.logger.Warn(memoryLimitV1Text, "limit_bytes", limit.bytes)
			cs.v1Warned = now
		}
		cs.announcedLimit, cs.limitAnnounced = limit, true
		return
	}
	unchanged := cs.limitAnnounced && cs.announcedLimit.source == limit.source &&
		cs.announcedLimit.cgroup == limit.cgroup && cs.announcedLimit.bytes == limit.bytes
	if unchanged {
		return
	}
	cs.logger.Info(memoryLimitText(limit), "limit_bytes", limit.bytes)
	cs.announcedLimit, cs.limitAnnounced = limit, true
}

// validateLimit refuses a limit a ratio cannot be taken against: zero or less
// would give +Inf, NaN or a healthy-looking 0 (spec §3).
func validateLimit(resource string, limit int64) error {
	if limit <= 0 {
		return fmt.Errorf("the %s limit reads %d; a limit must be positive", resource, limit)
	}
	return nil
}

func (cs *capacitySampler) readFDs(probes *capacityProbes) (resourceReading, error) {
	open, limit, err := probes.openFDs()
	if err != nil {
		return resourceReading{}, fmt.Errorf("reading open descriptors: %w", err)
	}
	if err := validateLimit(resourceFDs, limit); err != nil {
		return resourceReading{}, err
	}
	ratio := float64(open) / float64(limit)
	return resourceReading{
		ratio:   ratio,
		redNow:  ratio >= cs.thresholds[resourceFDs],
		redText: fdsRedText(ratio, open, limit),
		publish: func(*capacityMetrics) {},
	}, nil
}

func (cs *capacitySampler) readMappings(probes *capacityProbes) (resourceReading, error) {
	count, limit, err := probes.mappings()
	if err != nil {
		return resourceReading{}, fmt.Errorf("reading memory mappings: %w", err)
	}
	if err := validateLimit(resourceMappings, limit); err != nil {
		return resourceReading{}, err
	}
	ratio := float64(count) / float64(limit)
	return resourceReading{
		ratio:   ratio,
		redNow:  ratio >= cs.thresholds[resourceMappings],
		redText: mappingsRedText(ratio, count, limit),
		publish: func(m *capacityMetrics) {
			m.mappings.WithLabelValues().Set(float64(count))
			m.mappingsLimit.WithLabelValues().Set(float64(limit))
		},
	}, nil
}

func (cs *capacitySampler) readMemory(probes *capacityProbes) (resourceReading, error) {
	anon, err := probes.anonMemory()
	if err != nil {
		return resourceReading{}, fmt.Errorf("reading anonymous memory: %w", err)
	}
	limit, err := probes.memoryLimit()
	if err != nil {
		return resourceReading{}, fmt.Errorf("reading the memory limit: %w", err)
	}
	if err := validateLimit(resourceMemory, limit.bytes); err != nil {
		return resourceReading{}, err
	}
	// Not clamped: above 1.0 the node is over its limit, and by how much
	// matters.
	ratio := float64(anon) / float64(limit.bytes)
	return resourceReading{
		ratio:    ratio,
		redNow:   ratio >= cs.thresholds[resourceMemory],
		redText:  memoryRedText(ratio, limit),
		memLimit: limit,
		publish: func(m *capacityMetrics) {
			m.anonBytes.WithLabelValues().Set(float64(anon))
			m.memLimitBytes.WithLabelValues().Set(float64(limit.bytes))
		},
	}, nil
}

// readDisk computes one filesystem's ratio and both byte gauges from one
// statfs, so the ratio is exactly used/size at one instant: the data
// directory's for resourceDisk, the blob directory's for resourceBlobDisk.
// With the guard on, red is the guard's own comparison (diskBelowMark), not
// ratio >= threshold: the two differ by one ulp at some boundaries, and the
// gauge must not go red where the node would still accept a join.
func (cs *capacitySampler) readDisk(probes *capacityProbes, resource string) (resourceReading, error) {
	dir, subject := cs.dataDir, diskSubjectData
	if resource == resourceBlobDisk {
		dir, subject = cs.blobRoot, fmt.Sprintf(diskSubjectBlob, cs.blobRoot)
	}
	st, err := probes.diskStatfs(dir)
	if err != nil {
		return resourceReading{}, fmt.Errorf("reading the filesystem holding %s: %w", dir, err)
	}
	if err := st.validate(dir); err != nil {
		return resourceReading{}, err
	}
	used, size := float64(st.usedBytes()), float64(st.sizeBytes())
	ratio := used / size
	threshold := cs.thresholds[resource]
	guardOn := cs.diskLimitPct > 0
	redNow := ratio >= threshold
	if guardOn {
		redNow = !diskBelowMark(st, cs.diskLimitPct)
	}
	return resourceReading{
		ratio:     ratio,
		redNow:    redNow,
		redText:   diskRedText(subject, ratio, threshold, guardOn),
		amberText: diskAmberText(subject, ratio, threshold),
		publish: func(m *capacityMetrics) {
			usedGauge, sizeGauge := m.diskUsedBytes, m.diskSizeBytes
			if resource == resourceBlobDisk {
				usedGauge, sizeGauge = m.blobDiskUsedBytes, m.blobDiskSizeBytes
			}
			usedGauge.WithLabelValues().Set(used)
			sizeGauge.WithLabelValues().Set(size)
		},
	}, nil
}

// isDiskResource is whether a resource is a filesystem the join guard reads,
// and so takes the guard's mark and an amber line.
func isDiskResource(resource string) bool {
	return resource == resourceDisk || resource == resourceBlobDisk
}

// sampleDataBytes publishes the bytes hoardDB itself holds. These parts have
// no ratio, so a failure deletes the part and counts against no resource. A
// node with no WAL has no wal part: absent, not 0, and not a failure.
func (cs *capacitySampler) sampleDataBytes(probes *capacityProbes, now time.Time) {
	lsm, vlog, err := probes.storeSize()
	if err != nil {
		cs.metrics.dataBytes.DeleteLabelValues(dataPartStoreLSM)
		cs.metrics.dataBytes.DeleteLabelValues(dataPartStoreVlog)
		cs.warnDataFailure("store", err, now)
	} else {
		delete(cs.dataFailedAt, "store")
		cs.metrics.dataBytes.WithLabelValues(dataPartStoreLSM).Set(float64(lsm))
		cs.metrics.dataBytes.WithLabelValues(dataPartStoreVlog).Set(float64(vlog))
	}

	walBytes, present, err := probes.walBytes()
	switch {
	case err != nil:
		cs.metrics.dataBytes.DeleteLabelValues(dataPartWAL)
		cs.warnDataFailure(dataPartWAL, err, now)
	case !present:
		delete(cs.dataFailedAt, dataPartWAL)
		cs.metrics.dataBytes.DeleteLabelValues(dataPartWAL)
	default:
		delete(cs.dataFailedAt, dataPartWAL)
		cs.metrics.dataBytes.WithLabelValues(dataPartWAL).Set(float64(walBytes))
	}
}

// warnDataFailure logs a data-bytes failure once per episode and then hourly,
// on the same cadence as a resource's.
func (cs *capacitySampler) warnDataFailure(part string, readErr error, now time.Time) {
	warnedAt, failing := cs.dataFailedAt[part]
	if failing && now.Sub(warnedAt) < reclaimWarnInterval {
		return
	}
	cs.logger.Warn(dataFailureText(part, readErr), "part", part, "error", readErr)
	cs.dataFailedAt[part] = now
}

// healthCapacity is status's view of the last sample. It takes only lastMu,
// never sampleMu, so status answers even while a probe is stuck. A sample
// older than capacityStaleAfter, or none at all, makes every row stale: the
// last known ratios are still shown, but no row reads ok.
func (cs *capacitySampler) healthCapacity(now time.Time) *HealthCapacity {
	cs.lastMu.Lock()
	last := cs.last
	cs.lastMu.Unlock()

	stale := last.sampledAt.IsZero() || now.Sub(last.sampledAt) > capacityStaleAfter
	health := &HealthCapacity{Rows: make([]CapacityRow, 0, len(cs.resources))}
	if !last.sampledAt.IsZero() {
		health.SampledAt = last.sampledAt.Unix()
	}
	if len(last.rows) == 0 {
		for _, resource := range cs.resources {
			health.Rows = append(health.Rows, CapacityRow{Resource: resource, Threshold: cs.thresholds[resource], State: stateStale})
		}
		return health
	}
	for _, sampled := range last.rows {
		row := CapacityRow{Resource: sampled.resource, Threshold: sampled.threshold, State: sampled.state.String()}
		if sampled.hasRatio {
			ratio := sampled.ratio
			row.Ratio = &ratio
		}
		if stale {
			row.State = stateStale
		}
		health.Rows = append(health.Rows, row)
	}
	return health
}
