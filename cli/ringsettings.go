package cli

import "strings"

// set runs 'set pretty|extjson|pager [on|off]' with the sentences and
// semantics of v1.0 (toggleOutcome is shared with the legacy session).
func (s *RingSession) set(st Statement) (RingOutput, error) {
	setting, value := ringText(st, "setting"), ringText(st, "value")
	var current *bool
	scope := "documents printed from now on"
	paging := false
	switch setting {
	case "pretty":
		current = &s.pretty
	case "extjson":
		current = &s.extJSON
	default:
		paging = true
		on := !s.noPager
		current = &on
		scope = "reports longer than a screen"
	}
	text, change, err := toggleOutcome(setting, value, *current, scope)
	if err != nil {
		return RingOutput{}, &RingError{Message: err.Error(), Exit: ExitRefused, Err: err}
	}
	if change != nil {
		if paging {
			s.noPager = !*change
		} else {
			*current = *change
		}
	}
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if paging && value != "" && !s.noPager && !s.interactive {
		lines = append(lines, "Paging needs a terminal at both ends, and this session's input is not one: a piped run prints its reports in full.")
	}
	return RingOutput{Lines: lines}, nil
}
