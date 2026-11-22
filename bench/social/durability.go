package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// durabilityFailureKey is the engine fact that holds a P6 integrity failure about
// the durability level (spec 2, failure 1); empty or absent means none.
const durabilityFailureKey = "durability_integrity_failure"

// unsafeAckMarker is the part of the start-up WARN line (server/ack_sync.go:
// unsafeAckWarning) that says the unsafe switch is on. The line is logged at
// start and nowhere else, so its presence in the log is the only trace the
// switch leaves at run time: HealthResponse carries none of the durability
// settings.
const unsafeAckMarker = "unsafe_ack_before_sync is on"

// Environment variables the harness launches hoardDB with (config/config.go).
const (
	envUnsafeAck         = "HOARDDB_UNSAFE_ACK_BEFORE_SYNC"
	envReplicationFactor = "HOARDDB_REPLICATION_FACTOR"
	envWriteConcern      = "HOARDDB_WRITE_CONCERN"
	envWALPreallocate    = "HOARDDB_WAL_PREALLOCATE"
)

// walEffectiveNone is the effective preallocation mode of a node that keeps no
// write-ahead log: the key is set but there is nothing for it to act on.
const walEffectiveNone = "none (no write-ahead log)"

// walPreallocateEffective reads the mode a node says it started with from the
// startup line of the server log (spec 6, "The startup line"). The last such
// line wins, so a log that holds several starts reports the newest. It is
// "unknown" when no line is there: a server that predates the key logs none.
func walPreallocateEffective(logText string) string {
	effective := "unknown"
	for _, line := range strings.Split(logText, "\n") {
		switch {
		case strings.Contains(line, "entry-ring: write-ahead log segments"):
			// The entry-ring node's log is a separate log; this run reads the
			// legacy node's mode.
			continue
		case strings.Contains(line, "storage.wal_preallocate=on has no effect"):
			effective = walEffectiveNone
		case strings.Contains(line, "write-ahead log segments are preallocated (storage.wal_preallocate="):
			effective = walStartupMode(line, "on")
		case strings.Contains(line, "write-ahead log segments are not preallocated (storage.wal_preallocate="):
			effective = walStartupMode(line, "off")
		}
	}
	return effective
}

// walStartupMode is the effective mode of a startup line: word, plus the
// reason in parentheses when the key was auto, which is the only value whose
// result depends on the machine.
func walStartupMode(line, word string) string {
	_, tail, _ := strings.Cut(line, "storage.wal_preallocate=")
	inner, _, _ := strings.Cut(tail, ")")
	configured, reason, hasReason := strings.Cut(inner, ": ")
	if configured == "auto" && hasReason {
		return word + " (" + reason + ")"
	}
	return word
}

// walPreallocateProblem says where the configured mode and the effective one
// disagree, or "" when they agree or the effective mode is unknown.
func walPreallocateProblem(configured, effective string) string {
	switch {
	case effective == "unknown":
		return ""
	case effective == walEffectiveNone && (configured == "on" || configured == "auto"):
		return fmt.Sprintf("%s=%s has no effect: this node keeps no write-ahead log, so the run would pass for a preallocated one without being it; use a node with a replication factor of 2 or more", envWALPreallocate, configured)
	case (configured == "on" || configured == "off") && effective != configured && !strings.HasPrefix(effective, configured+" ("):
		return fmt.Sprintf("%s=%s but the server's startup line says wal_preallocate is %s; the run measures a different mode than it states", envWALPreallocate, configured, effective)
	}
	return ""
}

// parseLaunchEnvironment reads KEY=VALUE lines as a shell would export them:
// blank lines and comments are skipped, "export " is dropped, and one pair of
// quotes around a value is removed.
func parseLaunchEnvironment(text string) map[string]string {
	environment := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "export "))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		environment[strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(value), `"'`)
	}
	return environment
}

// launchFacts are the durability settings of one hoardDB node as the harness
// launched it and as its log recorded them.
type launchFacts struct {
	replicationFactor string
	writeConcern      string
	unsafeAck         bool
	warningInLog      bool
	// walConfigured is HOARDDB_WAL_PREALLOCATE from the launch environment:
	// the value, "unset", or "unknown" when no environment file was given.
	// walEffective is what the server's startup line says, or "unknown".
	walConfigured, walEffective string
}

// readLaunchFacts reads the environment file and the server log the harness
// saved. Either path may be empty, in which case that source is unknown and
// the facts it would give stay at their defaults.
func readLaunchFacts(envPath, logPath string) (launchFacts, error) {
	facts := launchFacts{replicationFactor: "default", writeConcern: "node default", walConfigured: "unknown", walEffective: "unknown"}
	if envPath != "" {
		text, err := os.ReadFile(envPath)
		if err != nil {
			return facts, fmt.Errorf("reading the launch environment %s (-hoard-env): %w", envPath, err)
		}
		environment := parseLaunchEnvironment(string(text))
		if value := environment[envReplicationFactor]; value != "" {
			facts.replicationFactor = value
		}
		if value := environment[envWriteConcern]; value != "" {
			facts.writeConcern = value
		}
		facts.walConfigured = "unset"
		if value := environment[envWALPreallocate]; value != "" {
			facts.walConfigured = value
		}
		switch strings.ToLower(environment[envUnsafeAck]) {
		case "true", "1", "yes":
			facts.unsafeAck = true
		}
	}
	if logPath != "" {
		text, err := os.ReadFile(logPath)
		if err != nil {
			return facts, fmt.Errorf("reading the server log %s (-hoard-log): %w", logPath, err)
		}
		facts.warningInLog = strings.Contains(string(text), unsafeAckMarker)
		facts.walEffective = walPreallocateEffective(string(text))
	}
	return facts, nil
}

// durabilityProblems says where the launch facts contradict each other or the
// level the run claims (spec 2, failure 1). L-A, L-R and L-R-all are durable
// acknowledgements and need the switch off and the WARN line absent; L-B is the
// relaxed level and needs both on. known says which sources were given.
func durabilityProblems(level string, facts launchFacts, haveEnv, haveLog bool) []string {
	var problems []string
	if haveEnv && haveLog && facts.unsafeAck != facts.warningInLog {
		problems = append(problems, fmt.Sprintf("the launch environment and the server log disagree: %s is %t but the WARN line is %s; the node that is running is not the one the harness launched", envUnsafeAck, facts.unsafeAck, presence(facts.warningInLog)))
	}
	switch level {
	case "L-A", "L-R", "L-R-all":
		if haveEnv && facts.unsafeAck || haveLog && facts.warningInLog {
			problems = append(problems, fmt.Sprintf("the run claims %s, a durable acknowledgement, but hoardDB was launched with the unsafe switch on (%s=%t, WARN line %s); restart it without %s", level, envUnsafeAck, facts.unsafeAck, presence(facts.warningInLog), envUnsafeAck))
		}
	case "L-B":
		if haveEnv && !facts.unsafeAck || haveLog && !facts.warningInLog {
			problems = append(problems, fmt.Sprintf("the run claims L-B, the relaxed level, but hoardDB was not launched with the unsafe switch (%s=%t, WARN line %s); start it with %s=true or claim L-A", envUnsafeAck, facts.unsafeAck, presence(facts.warningInLog), envUnsafeAck))
		}
	}
	return problems
}

// parseLevel checks -level against the durability levels of spec 3.3; empty is
// a run that claims none.
func parseLevel(name string) (string, error) {
	for _, level := range []string{"", "L-A", "L-B", "L-R", "L-R-all"} {
		if strings.EqualFold(strings.TrimSpace(name), level) {
			return level, nil
		}
	}
	return "", fmt.Errorf("-level %q is not L-A, L-B, L-R or L-R-all (spec 3.3)", name)
}

func presence(present bool) string {
	if present {
		return "present"
	}
	return "absent"
}

// durability reports the facts that decide when hoardDB's acknowledgement is on
// disk: the version it answers health with, and, from the launch environment
// and the server log the harness saved, the replication factor, the write
// concern and whether the unsafe switch is set. A problem is recorded under
// durabilityFailureKey so the leg fails on it and a reviewer reads why.
func (h *hoardEngine) durability() map[string]string {
	return launchDurability(h.cfg, h.nodeVersion())
}

// launchDurability is durability for any hoardDB engine: the node's version, which the
// engine asked for, and the facts of the launch files the harness saved.
func launchDurability(cfg *config, version string) map[string]string {
	facts := map[string]string{"version": version, "durability_level": cfg.Level}
	launch, err := readLaunchFacts(cfg.HoardEnv, cfg.HoardLog)
	var problems []string
	if err != nil {
		problems = append(problems, err.Error())
	}
	facts["replication_factor"], facts["write_concern"] = launch.replicationFactor, launch.writeConcern
	facts["wal_preallocate"], facts["wal_preallocate_effective"] = launch.walConfigured, launch.walEffective
	facts["unsafe_ack_before_sync"], facts["unsafe_ack_warning_in_log"] = "unknown", "unknown"
	if cfg.HoardEnv != "" && err == nil {
		facts["unsafe_ack_before_sync"] = fmt.Sprint(launch.unsafeAck)
	}
	if cfg.HoardLog != "" && err == nil {
		facts["unsafe_ack_warning_in_log"] = presence(launch.warningInLog)
	}
	if err == nil {
		problems = append(problems, durabilityProblems(cfg.Level, launch, cfg.HoardEnv != "", cfg.HoardLog != "")...)
		if problem := walPreallocateProblem(launch.walConfigured, launch.walEffective); problem != "" {
			problems = append(problems, problem)
		}
	}
	if len(problems) > 0 {
		facts[durabilityFailureKey] = strings.Join(problems, "; ")
	}
	return facts
}

// nodeVersion asks the first node for its version. The answer is a fact to
// record, so a node that cannot be asked is recorded as such.
func (h *hoardEngine) nodeVersion() string {
	caller, release, err := h.openCaller()
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	defer release()
	var health server.HealthResponse
	if err := caller.call(transport.OpHealthCheck, struct{}{}, &health); err != nil {
		return "unknown (" + err.Error() + ")"
	}
	return health.Version
}
