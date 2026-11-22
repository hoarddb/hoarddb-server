package cli

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestDateFunctionSetEvaluatesEachMember runs each D-5 function member through
// the evaluator. The spec names them all; these tests confirm each works.
func TestDateFunctionSetEvaluatesEachMember(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantType string // "DateTime", "int64", "string"
	}{
		{
			name:     "now",
			expr:     "now()",
			wantType: "marker", // now() returns the server marker, not a DateTime
		},
		{
			name:     "ISODate",
			expr:     `ISODate("2026-09-19T16:47:43Z")`,
			wantType: "DateTime",
		},
		{
			name:     "new Date",
			expr:     `new Date("2026-09-19T16:47:43Z")`,
			wantType: "DateTime",
		},
		{
			name:     "date.parse",
			expr:     `date.parse("2026-09-19T16:47:43Z")`,
			wantType: "DateTime",
		},
		{
			name:     "getTime",
			expr:     `ISODate("2026-09-19T16:47:43Z").getTime()`,
			wantType: "int64",
		},
		{
			name:     "toISOString",
			expr:     `ISODate("2026-09-19T16:47:43Z").toISOString()`,
			wantType: "string",
		},
		{
			name:     "add days",
			expr:     `ISODate("2026-09-19T16:47:43Z").add(1, "d")`,
			wantType: "DateTime",
		},
		{
			name:     "add hours",
			expr:     `ISODate("2026-09-19T16:47:43Z").add(1, "h")`,
			wantType: "DateTime",
		},
		{
			name:     "add minutes",
			expr:     `ISODate("2026-09-19T16:47:43Z").add(1, "m")`,
			wantType: "DateTime",
		},
		{
			name:     "add seconds",
			expr:     `ISODate("2026-09-19T16:47:43Z").add(1, "s")`,
			wantType: "DateTime",
		},
		{
			name:     "add milliseconds",
			expr:     `ISODate("2026-09-19T16:47:43Z").add(1, "ms")`,
			wantType: "DateTime",
		},
		{
			name:     "add weeks",
			expr:     `ISODate("2026-09-19T16:47:43Z").add(1, "w")`,
			wantType: "DateTime",
		},
		{
			name:     "trunc",
			expr:     `ISODate("2026-09-19T16:47:43Z").trunc("d")`,
			wantType: "DateTime",
		},
		{
			name:     "format",
			expr:     `ISODate("2026-09-19T16:47:43Z").format("%Y-%m-%d")`,
			wantType: "string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := evaluateDateExpression(tt.expr)
			if err != nil {
				t.Fatalf("evaluateDateExpression(%q) error = %v", tt.expr, err)
			}
			if val == nil {
				t.Fatalf("evaluateDateExpression(%q) returned nil", tt.expr)
			}

			// Check the type
			switch tt.wantType {
			case "DateTime":
				if _, ok := val.(primitive.DateTime); !ok {
					t.Errorf("evaluateDateExpression(%q) type = %T, want primitive.DateTime", tt.expr, val)
				}
			case "int64":
				if _, ok := val.(int64); !ok {
					t.Errorf("evaluateDateExpression(%q) type = %T, want int64", tt.expr, val)
				}
			case "string":
				if _, ok := val.(string); !ok {
					t.Errorf("evaluateDateExpression(%q) type = %T, want string", tt.expr, val)
				}
			case "marker":
				if _, ok := val.(primitive.DBPointer); !ok {
					t.Errorf("evaluateDateExpression(%q) type = %T, want primitive.DBPointer", tt.expr, val)
				}
			}
		})
	}
}

// TestDateFormatSpecifiers verifies that .format() expands the documented %specifiers.
func TestDateFormatSpecifiers(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{
			name:    "year-month-day",
			pattern: "%Y-%m-%d",
			want:    "2026-09-19",
		},
		{
			name:    "time",
			pattern: "%H:%M:%S",
			want:    "16:47:43",
		},
		{
			name:    "milliseconds",
			pattern: "%L",
			want:    "000", // the ISODate has 000 millis
		},
		{
			name:    "full ISO-like",
			pattern: "%Y-%m-%dT%H:%M:%S.%LZ",
			want:    "2026-09-19T16:47:43.000Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := fmt.Sprintf(`ISODate("2026-09-19T16:47:43Z").format("%s")`, tt.pattern)
			val, err := evaluateDateExpression(expr)
			if err != nil {
				t.Fatalf("evaluateDateExpression(%q) error = %v", expr, err)
			}
			result, ok := val.(string)
			if !ok {
				t.Fatalf("format result type = %T, want string", val)
			}
			if result != tt.want {
				t.Errorf("format(%q) = %q, want %q", tt.pattern, result, tt.want)
			}
		})
	}
}

// TestUnknownDateMethodNamesTheSupportedList ensures that an unknown method
// produces an error with the full list of supported methods.
func TestUnknownDateMethodNamesTheSupportedList(t *testing.T) {
	expr := `ISODate("2026-09-19T16:47:43Z").getSeconds()`
	_, err := evaluateDateExpression(expr)
	if err == nil {
		t.Fatal("evaluateDateExpression should error on unknown method")
	}

	errText := err.Error()
	// Should name every supported method
	requiredMethods := []string{
		"getTime",
		"toISOString",
		"add",
		"trunc",
		"format",
	}
	for _, method := range requiredMethods {
		if !strings.Contains(errText, method) {
			t.Errorf("error message missing method %q; got: %s", method, errText)
		}
	}
}

// TestDateAddRejectsUnknownUnit checks that d.add with an unknown unit fails.
func TestDateAddRejectsUnknownUnit(t *testing.T) {
	expr := `ISODate("2026-09-19T16:47:43Z").add(1, "fortnight")`
	_, err := evaluateDateExpression(expr)
	if err == nil {
		t.Fatal("evaluateDateExpression should error on unknown unit")
	}

	errText := err.Error()
	// Should mention the valid units
	for _, unit := range []string{"ms", "s", "m", "h", "d", "w"} {
		if !strings.Contains(errText, unit) {
			t.Errorf("error message missing unit %q; got: %s", unit, errText)
		}
	}
}

// TestNamedTimeZoneIsRefusedWithTheFix confirms that a named time zone
// is rejected and the error names the offset form as the fix (D-7).
func TestNamedTimeZoneIsRefusedWithTheFix(t *testing.T) {
	expr := `ISODate("2026-09-19T16:47:43+01:00[Europe/Tirane]")`
	_, err := evaluateDateExpression(expr)
	if err == nil {
		t.Fatal("evaluateDateExpression should error on named time zone")
	}

	errText := err.Error()
	if !strings.Contains(errText, "offset") && !strings.Contains(errText, "named time zone") {
		t.Errorf("error should mention offset or named time zone; got: %s", errText)
	}
	if !strings.Contains(errText, "Europe/Tirane") && !strings.Contains(errText, "Tirane") {
		t.Errorf("error should mention the zone name; got: %s", errText)
	}
}

// TestNowIsServerSideAcrossClientClocks confirms that now() is evaluated
// server-side, not client-side. This is tested at the server level in
// server/scan_test.go; this is a placeholder for the CLI-level evaluation.
func TestNowEvaluatesAsDatetime(t *testing.T) {
	expr := "now()"
	val, err := evaluateDateExpression(expr)
	if err != nil {
		t.Fatalf("evaluateDateExpression(%q) error = %v", expr, err)
	}

	if val != server.NowMarker() {
		t.Errorf("now() evaluated to %#v; want the server's marker %#v", val, server.NowMarker())
	}
}

// TestTypedNowSentinelShapeIsDataAndNowCallIsTheMarker is BUG-0151 at the
// prompt: a document the user types holds {"$$now": true} as a sub-document,
// strict or relaxed, and only now() becomes the marker the server replaces.
func TestTypedNowSentinelShapeIsDataAndNowCallIsTheMarker(t *testing.T) {
	for _, form := range []struct {
		name, typed string
		wantCall    bool
	}{
		{"strict JSON", `{"n": {"$$now": true}, "list": [{"$$now": true}]}`, false},
		{"relaxed", `{n: {"$$now": true}, list: [{"$$now": true}], at: now()}`, true},
	} {
		t.Run(form.name, func(t *testing.T) {
			document, err := parseDocument(form.typed)
			if err != nil {
				t.Fatalf("parseDocument(%s): %v", form.typed, err)
			}
			literal, isDocument := document["n"].(map[string]interface{})
			if !isDocument || len(literal) != 1 || literal["$$now"] != true {
				t.Errorf("n = %#v; want the sub-document {\"$$now\": true} as typed", document["n"])
			}
			list, _ := document["list"].(primitive.A)
			if len(list) != 1 {
				t.Fatalf("list = %#v", document["list"])
			}
			if element, isDocument := list[0].(map[string]interface{}); !isDocument || element["$$now"] != true {
				t.Errorf("list[0] = %#v; want the sub-document {\"$$now\": true} as typed", list[0])
			}
			if form.wantCall && document["at"] != server.NowMarker() {
				t.Errorf("at = %#v; want the now() marker", document["at"])
			}
		})
	}
}

// TestTypedDeprecatedPointerCannotSpellTheNowMarker: the marker is a
// deprecated type, so typed text must not be able to produce it.
func TestTypedDeprecatedPointerCannotSpellTheNowMarker(t *testing.T) {
	typed := `{"at": {"$dbPointer": {"$ref": "$$hoardDB.now", "$id": {"$oid": "000000000000000000000000"}}}}`
	if document, err := parseDocument(typed); err == nil {
		t.Errorf("parseDocument(%s) = %#v; want the deprecated $dbPointer refused", typed, document)
	}
}
