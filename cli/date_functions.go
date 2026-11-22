package cli

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/server"
)

// evaluateDateExpression evaluates a date expression and returns the result.
// Supported expressions:
//
//	now()                              -> date (sentinel, server-evaluated)
//	ISODate("...")                     -> date literal
//	new Date("...")                    -> date literal (alias)
//	date.parse("...")                  -> date (parse from string)
//	d.getTime()                        -> int64 (ms since epoch)
//	d.toISOString()                    -> string (RFC 3339)
//	d.add(<n>, "<unit>")               -> date (add duration)
//	d.trunc("<unit>")                  -> date (truncate)
//	d.format("<pattern>")              -> string (format with %Y %m %d %H %M %S %L specifiers)
//
// The only available binding for d is a date literal or now(). An attempt to
// call a method on a field reference (ts.getTime()) errors.
func evaluateDateExpression(expr string) (interface{}, error) {
	expr = strings.TrimSpace(expr)

	if expr == "now()" {
		return server.NowMarker(), nil
	}

	// Parse a date literal or method chain on one
	return evaluateDateLiteral(expr)
}

// evaluateDateLiteral parses a date literal and evaluates any method chain on it.
func evaluateDateLiteral(expr string) (interface{}, error) {
	// Try to extract a date literal from the start of expr
	if argEnd, isDateLiteral := dateLiteralAt(expr, 0); isDateLiteral {
		literal := expr[0 : argEnd+1]
		dt, err := parseDateLiteral(literal)
		if err != nil {
			return nil, err
		}

		// Check if there are method calls after the literal
		rest := strings.TrimSpace(expr[argEnd+1:])
		if rest == "" {
			return dt, nil
		}

		// Evaluate method chain
		return evaluateMethodChain(dt, rest)
	}

	// Try date.parse(...)
	if strings.HasPrefix(expr, "date.parse(") {
		return evaluateDateParse(expr)
	}

	return nil, usagef("date expression %q: unrecognized format", expr)
}

// checkForNamedTimeZone checks if a date string contains a bracketed timezone name
// and returns an error if it does, with the offset form as the suggested fix (D-7).
func checkForNamedTimeZone(dateStr string) error {
	// Look for pattern like [Europe/Tirane] in the date string
	if strings.Contains(dateStr, "[") && strings.Contains(dateStr, "]") {
		// Extract the bracketed part
		start := strings.Index(dateStr, "[")
		end := strings.Index(dateStr, "]")
		if start < end {
			zoneName := dateStr[start+1 : end]
			return errNamedTimeZone(zoneName)
		}
	}
	return nil
}

// parseDateLiteral parses an ISODate(...) or new Date(...) literal.
func parseDateLiteral(literal string) (primitive.DateTime, error) {
	extJSON, err := dateLiteralToExtJSON(literal)
	if err != nil {
		return 0, err
	}

	// Check for named time zones
	if err := checkForNamedTimeZone(extJSON); err != nil {
		return 0, err
	}

	// Create a temporary document with the date field and unmarshal with Extended JSON support
	tempJSON := []byte(fmt.Sprintf(`{"_temp": %s}`, extJSON))

	var revived map[string]interface{}
	if err := bson.UnmarshalExtJSON(tempJSON, false, &revived); err != nil {
		return 0, usagef("parsing date literal %q: %v", literal, err)
	}

	// Extract the date from the _temp field
	dateVal, ok := revived["_temp"]
	if !ok {
		return 0, usagef("date literal %q: missing _temp field", literal)
	}

	dt, ok := dateVal.(primitive.DateTime)
	if !ok {
		return 0, usagef("date literal %q: wrong type %T (expected primitive.DateTime)", literal, dateVal)
	}

	return dt, nil
}

// evaluateDateParse evaluates date.parse("...")
func evaluateDateParse(expr string) (interface{}, error) {
	// Extract the argument from date.parse("...")
	if !strings.HasPrefix(expr, "date.parse(") {
		return nil, usagef("date.parse: invalid syntax")
	}

	rest := expr[len("date.parse("):]
	if !strings.HasSuffix(rest, ")") {
		return nil, usagef("date.parse: unterminated call")
	}
	rest = rest[:len(rest)-1]

	// Extract quoted string
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "\"") || !strings.HasSuffix(rest, "\"") {
		return nil, usagef("date.parse: argument must be a quoted string")
	}

	argStr := rest[1 : len(rest)-1]

	// Check for named time zones
	if err := checkForNamedTimeZone(argStr); err != nil {
		return nil, err
	}

	// Try to parse as Extended JSON string or $numberLong form
	// First try as a quoted string (relaxed Extended JSON)
	tempJSON := []byte(fmt.Sprintf(`{"_temp": {"$date": %q}}`, argStr))
	var revived map[string]interface{}
	if err := bson.UnmarshalExtJSON(tempJSON, false, &revived); err != nil {
		// Try as a numeric form
		tempJSON = []byte(fmt.Sprintf(`{"_temp": {"$date": {"$numberLong": %q}}}`, argStr))
		if err := bson.UnmarshalExtJSON(tempJSON, false, &revived); err != nil {
			return nil, usagef("date.parse(%q): %v", argStr, err)
		}
	}

	dateVal, ok := revived["_temp"]
	if !ok {
		return nil, usagef("date.parse(%q): missing _temp", argStr)
	}

	dt, ok := dateVal.(primitive.DateTime)
	if !ok {
		return nil, usagef("date.parse(%q): wrong type %T", argStr, dateVal)
	}

	return dt, nil
}

// evaluateMethodChain evaluates method calls on a date value (d.method().method()...)
func evaluateMethodChain(val interface{}, chainExpr string) (interface{}, error) {
	current := val

	for chainExpr != "" {
		chainExpr = strings.TrimSpace(chainExpr)
		if !strings.HasPrefix(chainExpr, ".") {
			return nil, usagef("expected . before method call")
		}
		chainExpr = chainExpr[1:]

		// Parse the method call
		result, remaining, err := evaluateMethod(current, chainExpr)
		if err != nil {
			return nil, err
		}

		current = result
		chainExpr = remaining
	}

	return current, nil
}

// evaluateMethod evaluates a single method call and returns the result and remaining string.
func evaluateMethod(val interface{}, methodExpr string) (interface{}, string, error) {
	dt, ok := val.(primitive.DateTime)
	if !ok {
		return nil, "", usagef("method call on non-date value of type %T", val)
	}

	// Parse method name and arguments
	parenIdx := strings.Index(methodExpr, "(")
	if parenIdx == -1 {
		return nil, "", usagef("method call %q: missing (", methodExpr)
	}

	methodName := methodExpr[:parenIdx]

	// Find matching closing paren (general matcher, not specific to date literals)
	closeIdx := findMethodClosingParen(methodExpr, parenIdx)
	if closeIdx == -1 {
		return nil, "", usagef("method call %s: unterminated (", methodName)
	}

	argsStr := methodExpr[parenIdx+1 : closeIdx]
	remaining := methodExpr[closeIdx+1:]

	// Execute the method
	switch methodName {
	case "getTime":
		if argsStr != "" {
			return nil, "", usagef("%s: takes no arguments", methodName)
		}
		return int64(dt), remaining, nil

	case "toISOString":
		if argsStr != "" {
			return nil, "", usagef("%s: takes no arguments", methodName)
		}
		t := time.UnixMilli(int64(dt)).UTC()
		return t.Format(time.RFC3339Nano), remaining, nil

	case "add":
		result, err := methodAdd(dt, argsStr)
		return result, remaining, err

	case "trunc":
		result, err := methodTrunc(dt, argsStr)
		return result, remaining, err

	case "format":
		result, err := methodFormat(dt, argsStr)
		return result, remaining, err

	default:
		return nil, "", errUnknownDateMethod(methodName)
	}
}

// methodAdd implements d.add(<n>, "<unit>")
func methodAdd(dt primitive.DateTime, argsStr string) (interface{}, error) {
	parts := parseMethodArgs(argsStr)
	if len(parts) != 2 {
		return nil, usagef("add: expects 2 arguments (number, unit)")
	}

	// Parse the number
	n, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, usagef("add: first argument must be a number, got %q", parts[0])
	}

	// Parse the unit
	unit := strings.Trim(parts[1], `" `)

	switch unit {
	case "ms":
		return primitive.NewDateTimeFromTime(time.UnixMilli(int64(dt) + n).UTC()), nil
	case "s":
		return primitive.NewDateTimeFromTime(time.UnixMilli(int64(dt) + n*1000).UTC()), nil
	case "m":
		return primitive.NewDateTimeFromTime(time.UnixMilli(int64(dt) + n*60*1000).UTC()), nil
	case "h":
		return primitive.NewDateTimeFromTime(time.UnixMilli(int64(dt) + n*60*60*1000).UTC()), nil
	case "d":
		return primitive.NewDateTimeFromTime(time.UnixMilli(int64(dt) + n*24*60*60*1000).UTC()), nil
	case "w":
		return primitive.NewDateTimeFromTime(time.UnixMilli(int64(dt) + n*7*24*60*60*1000).UTC()), nil
	default:
		return nil, errDateAddRejectsUnknownUnit(unit)
	}
}

// methodTrunc implements d.trunc("<unit>")
func methodTrunc(dt primitive.DateTime, argsStr string) (interface{}, error) {
	unit := strings.Trim(argsStr, `" `)

	t := time.UnixMilli(int64(dt)).UTC()

	switch unit {
	case "ms":
		return dt, nil
	case "s":
		return primitive.NewDateTimeFromTime(t.Truncate(time.Second)), nil
	case "m":
		return primitive.NewDateTimeFromTime(t.Truncate(time.Minute)), nil
	case "h":
		return primitive.NewDateTimeFromTime(t.Truncate(time.Hour)), nil
	case "d":
		return primitive.NewDateTimeFromTime(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)), nil
	case "w":
		// Truncate to start of week (Monday)
		for t.Weekday() != time.Monday {
			t = t.AddDate(0, 0, -1)
		}
		return primitive.NewDateTimeFromTime(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)), nil
	default:
		return nil, usagef("trunc: unknown unit %q (supported: ms, s, m, h, d, w)", unit)
	}
}

// methodFormat implements d.format("<pattern>") with %-specifiers (%Y %m %d %H %M %S %L)
func methodFormat(dt primitive.DateTime, argsStr string) (interface{}, error) {
	pattern := strings.Trim(argsStr, `" `)

	t := time.UnixMilli(int64(dt)).UTC()

	// Supported format specifiers.
	result := pattern
	result = strings.ReplaceAll(result, "%Y", fmt.Sprintf("%04d", t.Year()))
	result = strings.ReplaceAll(result, "%m", fmt.Sprintf("%02d", t.Month()))
	result = strings.ReplaceAll(result, "%d", fmt.Sprintf("%02d", t.Day()))
	result = strings.ReplaceAll(result, "%H", fmt.Sprintf("%02d", t.Hour()))
	result = strings.ReplaceAll(result, "%M", fmt.Sprintf("%02d", t.Minute()))
	result = strings.ReplaceAll(result, "%S", fmt.Sprintf("%02d", t.Second()))
	result = strings.ReplaceAll(result, "%L", fmt.Sprintf("%03d", t.Nanosecond()/1e6))

	// Check for unknown specifiers
	re := regexp.MustCompile(`%[a-zA-Z]`)
	if matches := re.FindAllString(result, -1); len(matches) > 0 {
		return nil, usagef("format: unknown specifier %q", matches[0])
	}

	return result, nil
}

// parseMethodArgs parses method arguments, handling quoted strings with commas.
func parseMethodArgs(argsStr string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false

	for i := 0; i < len(argsStr); i++ {
		c := argsStr[i]
		if c == '"' && (i == 0 || argsStr[i-1] != '\\') {
			inQuotes = !inQuotes
			current.WriteByte(c)
		} else if c == ',' && !inQuotes {
			parts = append(parts, strings.TrimSpace(current.String()))
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}

	return parts
}

// findMethodClosingParen finds the index of the closing parenthesis matching the one at parenIdx.
// This is a general matcher for method calls, unlike documents.go's findClosingParen which is specific to date literals.
func findMethodClosingParen(s string, parenIdx int) int {
	if parenIdx >= len(s) || s[parenIdx] != '(' {
		return -1
	}

	depth := 1
	inQuotes := false
	for i := parenIdx + 1; i < len(s); i++ {
		c := s[i]
		if c == '"' && (i == 0 || s[i-1] != '\\') {
			inQuotes = !inQuotes
		} else if !inQuotes {
			switch c {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					return i
				}
			}
		}
	}
	return -1
}
