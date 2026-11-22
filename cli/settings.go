package cli

import (
	"fmt"
	"strings"
)

// A session's output settings, changed while it runs.
//
// The three are the ones the flags of the same names set at startup, and a
// session is long enough that reading one document indented should not mean
// starting another one. The shape is the one 'write-concern <level>' established:
// name the settings that exist, refuse anything that is not one of them, and say
// what changed.

// handleSet changes a setting, or reports it when no value is given.
func (c *CLI) handleSet(cmd *Cmd) error {
	if len(cmd.Args) == 0 || len(cmd.Args) > 2 {
		return usagef("usage: set <pretty|extjson|pager> [on|off]")
	}
	setting := strings.ToLower(strings.TrimSpace(cmd.Args[0]))
	value := ""
	if len(cmd.Args) == 2 {
		value = strings.ToLower(strings.TrimSpace(cmd.Args[1]))
	}

	switch setting {
	case "pretty":
		return c.applyToggle(setting, value, c.pretty, "documents printed from now on", func(on bool) {
			c.pretty = on
			c.handler.SetPretty(on)
		})
	case "extjson":
		return c.applyToggle(setting, value, c.extendedJSON, "documents printed from now on", func(on bool) {
			c.extendedJSON = on
			c.handler.SetExtendedJSON(on)
		})
	case "pager":
		if err := c.applyToggle(setting, value, !c.noPager, "reports longer than a screen", func(on bool) {
			c.noPager = !on
		}); err != nil {
			return err
		}
		if value != "" && !c.noPager && !c.interactive() {
			// Say it now rather than leave a user wondering why turning paging on
			// changed nothing: paging needs a terminal at both ends, and this
			// session is reading its statements from somewhere else.
			c.notef("Paging needs a terminal at both ends, and this session's input is not one: a piped run prints its reports in full.\n")
		}
		return nil
	default:
		return usagef("unknown setting %q — a session has pretty, extjson and pager", setting)
	}
}

// applyToggle reports a setting, or changes it and says so. Reporting and
// changing share the wording, so what a session says about a setting is what it
// says when it changes one.
func (c *CLI) applyToggle(setting, value string, current bool, scope string, apply func(bool)) error {
	text, change, err := toggleOutcome(setting, value, current, scope)
	if err != nil {
		return err
	}
	if change != nil {
		apply(*change)
	}
	c.printf("%s", text)
	return nil
}

// toggleOutcome is what 'set <setting> [value]' says and does, without a
// session: the sentence to print, and the new state when the statement changes
// one. Ring mode calls it too, so both modes word a setting the same way.
func toggleOutcome(setting, value string, current bool, scope string) (text string, change *bool, err error) {
	if value == "" {
		return fmt.Sprintf("%s is %s; 'set %s %s' changes it.\n", setting, onOff(current), setting, onOff(!current)), nil, nil
	}
	on, err := parseOnOff(setting, value)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%s is now %s for %s.\n", setting, onOff(on), scope), &on, nil
}

// onOff words a setting's state.
func onOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}

// parseOnOff reads the value a user wrote for a setting. The words a person
// types for a switch are all accepted; anything else is refused by name rather
// than treated as false.
func parseOnOff(setting, value string) (bool, error) {
	switch value {
	case "on", "true", "yes":
		return true, nil
	case "off", "false", "no":
		return false, nil
	}
	return false, usagef("set %s: %q is not on or off", setting, value)
}
