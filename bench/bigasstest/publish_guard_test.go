package main

import (
	"os"
	"regexp"
	"testing"
)

// TestNoEngineFigureInPublishedFiles keeps measured per-engine results out of the files that sit in the public
// module: an engine name within 80 characters of a number with req/s, GB or % is a run result, and those stay internal.
func TestNoEngineFigureInPublishedFiles(t *testing.T) {
	engine := `(?i)(postgres(ql)?|mysql|mongo(db)?|hoarddb|ring)`
	figure := `[0-9][0-9,.]*\s*(req/s|GB|MB|%)`
	near := regexp.MustCompile(`(?is)(` + engine + `.{0,80}` + figure + `|` + figure + `.{0,80}` + engine + `)`)
	for _, name := range []string{"README.md", "calibrate.go"} {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, hit := range near.FindAllString(string(body), -1) {
			t.Errorf("%s names an engine next to a figure: %q", name, hit)
		}
	}
}
