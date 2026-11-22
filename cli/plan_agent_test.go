package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestPlanGuideMatchesGolden pins the guide: it changes only in the commit that changes a table.
func TestPlanGuideMatchesGolden(t *testing.T) {
	guide := renderGuide()
	if len(guide) > 8500 {
		t.Fatalf("the guide is %d bytes, over the 8500 that fit a system prompt", len(guide))
	}
	checkGolden(t, "testdata/plan/guide.golden.md", []byte(guide))
}

// TestPlanGuideNamesEveryTable: the guide carries each pattern, each rule and each visibility word.
func TestPlanGuideNamesEveryTable(t *testing.T) {
	guide := renderGuide()
	var wants []string
	for _, pattern := range planPatterns {
		wants = append(wants, pattern.ID+":")
	}
	wants = append(wants, planRuleNames[:]...)
	for _, shape := range planVisibilityShapes {
		wants = append(wants, "`"+shape.Word+"`")
	}
	wants = append(wants, planCostClasses...)
	for _, want := range wants {
		if !strings.Contains(guide, want) {
			t.Errorf("the guide lacks %q", want)
		}
	}
}

// TestPlanSchemaMatchesGolden pins the schema to the Go types.
func TestPlanSchemaMatchesGolden(t *testing.T) {
	checkGolden(t, "testdata/plan/model.schema.golden.json", planSchemaJSON())
}

// validateAgainst is a small validator: required, type and additionalProperties false.
func validateAgainst(schema map[string]any, value any, where string) error {
	switch schema["type"] {
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: want an object", where)
		}
		properties, _ := schema["properties"].(map[string]any)
		for key, item := range object {
			property, known := properties[key].(map[string]any)
			if !known {
				if extra, ok := schema["additionalProperties"].(map[string]any); ok {
					if err := validateAgainst(extra, item, where+"."+key); err != nil {
						return err
					}
					continue
				}
				return fmt.Errorf("%s: unknown field %q", where, key)
			}
			if err := validateAgainst(property, item, where+"."+key); err != nil {
				return err
			}
		}
		if required, ok := schema["required"].([]any); ok {
			for _, name := range required {
				if _, present := object[name.(string)]; !present {
					return fmt.Errorf("%s: missing %q", where, name)
				}
			}
		}
	case "array":
		list, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s: want an array", where)
		}
		for index, item := range list {
			if err := validateAgainst(schema["items"].(map[string]any), item, fmt.Sprintf("%s[%d]", where, index)); err != nil {
				return err
			}
		}
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s: want a string", where)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s: want a boolean", where)
		}
	case "integer":
		if number, ok := value.(float64); !ok || number != float64(int64(number)) {
			return fmt.Errorf("%s: want an integer", where)
		}
	}
	return nil
}

// TestPlanSchemaValidatesBlogModel: the schema accepts the blog model and
// rejects the same file with a misspelt key.
func TestPlanSchemaValidatesBlogModel(t *testing.T) {
	var schema map[string]any
	if err := json.Unmarshal(planSchemaJSON(), &schema); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/plan/blog.model.json")
	if err != nil {
		t.Fatal(err)
	}
	var model any
	if err := json.Unmarshal(data, &model); err != nil {
		t.Fatal(err)
	}
	if err := validateAgainst(schema, model, "model"); err != nil {
		t.Fatalf("the blog model does not validate: %v", err)
	}
	bad := strings.Replace(string(data), "present_permille", "presnt_permille", 1)
	var broken any
	if err := json.Unmarshal([]byte(bad), &broken); err != nil {
		t.Fatal(err)
	}
	if err := validateAgainst(schema, broken, "model"); err == nil {
		t.Fatal("a misspelt key validated")
	}
}

// TestPlanGuideAndSchemaCommands: the subcommands print the same bytes the golden pins.
func TestPlanGuideAndSchemaCommands(t *testing.T) {
	code, guide, _ := runPlanForTest(t, "", false, "guide")
	if code != ExitOK || guide != renderGuide() {
		t.Fatalf("guide: exit %d", code)
	}
	code, schema, _ := runPlanForTest(t, "", false, "schema")
	if code != ExitOK || schema != string(planSchemaJSON()) {
		t.Fatalf("schema: exit %d", code)
	}
}
