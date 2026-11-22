package cluster

// Value returns the current value of the cluster setting key with the dynamic type
// SET_SETTING's body carries for it (bool, int32 or int64), and false for a key that is not a
// setting. A caller builds expect_value from it and reads the effect of a command with it.
func (s Settings) Value(key string) (any, bool) {
	switch key {
	case "stale_reads":
		return s.StaleReads, true
	case "probe_failures":
		return s.ProbeFailures, true
	case "heartbeat_ms":
		return s.HeartbeatMs, true
	case "step_down_ms":
		return s.StepDownMs, true
	case "failover_ms":
		return s.FailoverMs, true
	case "dead_ms":
		return s.DeadMs, true
	case "probe_interval_ms":
		return s.ProbeIntervalMs, true
	case "probe_timeout_ms":
		return s.ProbeTimeoutMs, true
	}
	return nil, false
}
