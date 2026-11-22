package cli

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/version"
)

// TestThemeDefaultsToTheLogoPalette checks the built-in theme contains the
// measured logo values (§2.1). It asserts against defaultTheme directly to
// avoid depending on the developer's home directory.
func TestThemeDefaultsToTheLogoPalette(t *testing.T) {
	theme := defaultTheme
	if theme.Styles["tile"].FG != "#f1efe8" || theme.Styles["tile"].BG != "#3c3489" {
		t.Fatalf("tile = %+v, want fg #f1efe8 bg #3c3489", theme.Styles["tile"])
	}
	if theme.Styles["accent"].FG != "#a99cf5" {
		t.Fatalf("accent = %+v, want #a99cf5", theme.Styles["accent"])
	}
	if theme.Styles["database"].FG != "#6f63d6" {
		t.Fatalf("database = %+v, want #6f63d6", theme.Styles["database"])
	}
}

// TestPromptColoursTheMarkWithoutABlock is the default's look, pinned: the
// `h` carries the same purple as `DB`, and the brackets around it are plain
// brand white. It used to be the other way round — `h` in brand on the tile's
// indigo background, with the brackets part of the block.
//
// The block is also the only thing the default ever painted a background on,
// so the no-background assertion is the substantive one: it is what a theme
// file gets back when it recolours `tile`, since the default no longer shows
// it (see lightVariant).
func TestPromptColoursTheMarkWithoutABlock(t *testing.T) {
	theme := defaultTheme
	theme.Colour = true

	rendered := theme.renderPrompt("app")
	accent, brand := fgEscape(theme.Styles["accent"].FG), fgEscape(theme.Styles["brand"].FG)

	if !strings.Contains(rendered, accent+"h"+"\x1b[0m") {
		t.Errorf("the h is not in the accent purple like DB: %q", rendered)
	}
	if !strings.HasPrefix(rendered, brand+"["+"\x1b[0m") {
		t.Errorf("the bracket before the h is not plain brand white: %q", rendered)
	}
	if !strings.Contains(rendered, brand+"]"+"\x1b[0m") {
		t.Errorf("the bracket after the h is not plain brand white: %q", rendered)
	}
	// A background escape means the tile's block is still being painted.
	if strings.Contains(rendered, "\x1b[48;") {
		t.Errorf("the default prompt paints a background block: %q", rendered)
	}
	// The mark and DB must read as one wordmark, so they share a style.
	if got := theme.renderPrompt(""); strings.Count(got, accent) < 2 {
		t.Errorf("the h and DB do not share the accent style: %q", got)
	}
}

// TestTileStyleSurvivesWithoutTheBlock guards the other half. `tile` is still a
// declared style after the default stops using it, because a theme file that
// paints its own block must still validate — and `lightVariant` still adapts
// it for light terminals.
func TestTileStyleSurvivesWithoutTheBlock(t *testing.T) {
	tile, ok := defaultTheme.Styles["tile"]
	if !ok {
		t.Fatal("the tile style was removed; a theme file that paints its own block no longer validates")
	}
	if tile.BG == "" {
		t.Fatal("the tile style has no background left, so it cannot paint a block")
	}

	// A theme that uses the tile still gets the light-terminal swap, and the
	// swap must not land on the built-in: resolveTheme hands out a theme
	// lightVariant rewrites, and Styles is a map, so a theme that shared the
	// package-level default would repaint it for every later resolution in the
	// process.
	resolved := resolveTheme("", true)
	before := resolved.Styles["tile"]
	lightVariant(resolved)
	if resolved.Styles["tile"] == before {
		t.Error("lightVariant did not adapt the tile of a resolved theme for a light terminal")
	}
	if defaultTheme.Styles["tile"] != tile {
		t.Errorf("lightVariant mutated the built-in default theme: tile is now %+v, want %+v",
			defaultTheme.Styles["tile"], tile)
	}
}

// TestThemeFilePrecedence checks that ~/.hoarddb/theme.yml overrides the
// built-in defaults. Uses a temp HOME so the test is independent of the
// developer's actual config file.
func TestThemeFilePrecedence(t *testing.T) {
	tempHome := t.TempDir()
	themeDir := filepath.Join(tempHome, ".hoarddb")
	if err := os.MkdirAll(themeDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	themePath := filepath.Join(themeDir, "theme.yml")
	content := "styles:\n  tile:\n    bg: \"#ffffff\"\n"
	if err := os.WriteFile(themePath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Setenv("HOME", tempHome)

	theme := resolveTheme("", true)
	if theme.Styles["tile"].BG != "#ffffff" {
		t.Fatalf("file precedence failed: tile.BG = %q, want #ffffff (from file)", theme.Styles["tile"].BG)
	}
}

// TestThemeFileOverridesStyles writes a theme file overriding one style and
// confirms the override lands.
func TestThemeFileOverridesStyles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "theme.yml")
	content := "styles:\n  accent:\n    fg: \"#ff0000\"\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	theme, err := loadThemeFile(path)
	if err != nil {
		t.Fatalf("loadThemeFile: %v", err)
	}
	if theme.Styles["accent"].FG != "#ff0000" {
		t.Fatalf("accent FG = %q, want #ff0000", theme.Styles["accent"].FG)
	}
	// A partial file naming only accent is legal; the pull-through of the
	// remaining default styles happens at resolve time, so loadThemeFile
	// returns exactly what the file declared.
	if len(theme.Styles) != 1 {
		t.Fatalf("loaded %d styles, want only the one the file named", len(theme.Styles))
	}
}

// TestThemeFileAddsPromptFormat confirms a file can replace the format
// strings wholesale.
func TestThemeFileAddsPromptFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "theme.yml")
	content := "prompt_format: \"{accent:DB} [%database]> \"\nprompt_format_nodb: \"{accent:DB}> \"\nstyles:\n  accent:\n    fg: \"#a99cf5\"\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	theme, err := loadThemeFile(path)
	if err != nil {
		t.Fatalf("loadThemeFile: %v", err)
	}
	withDB := theme.renderPrompt("app")
	if !strings.Contains(withDB, "DB") || !strings.Contains(withDB, "app") {
		t.Fatalf("rendered prompt %q lacks DB or the database", withDB)
	}
	nodb := theme.renderPrompt("")
	if strings.Contains(nodb, "app") || !strings.Contains(nodb, "DB") {
		t.Fatalf("nodb prompt %q should drop the database and keep the mark", nodb)
	}
}

// TestPartialThemeFileKeepsTheDefaultPrompt writes the theme file a user
// actually writes — recolour one style, say nothing about the format — and
// asserts the prompt still renders. A file that declares no prompt_format used
// to resolve to an empty prompt, because the loaded theme replaced the default
// outright and its format strings were blank.
func TestPartialThemeFileKeepsTheDefaultPrompt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "theme.yml")
	content := "styles:\n  tile:\n    fg: \"#f1efe8\"\n    bg: \"#8579e8\"\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	theme := resolveTheme(path, false)
	if theme.PromptFormat == "" || theme.PromptNoDB == "" {
		t.Fatalf("a file with no prompt_format left the format strings empty: %+v", theme)
	}
	if got := theme.renderPrompt(""); got != "[h]oardDB> " {
		t.Fatalf("renderPrompt() = %q, want %q", got, "[h]oardDB> ")
	}
	if got := theme.renderPrompt("app"); !strings.Contains(got, "[app]") {
		t.Fatalf("renderPrompt(app) = %q, want it to name the database", got)
	}
}

// TestPartialThemeFileKeepsTheDefaultStyles covers the other half of the pull-
// through: a format inherited from the default references brand, accent and
// database, so those styles must be inherited too or the prompt loses its
// colour. loadThemeFile stays literal — the merge is resolve's job.
func TestPartialThemeFileKeepsTheDefaultStyles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "theme.yml")
	content := "styles:\n  tile:\n    bg: \"#8579e8\"\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	loaded, err := loadThemeFile(path)
	if err != nil {
		t.Fatalf("loadThemeFile: %v", err)
	}
	if len(loaded.Styles) != 1 {
		t.Fatalf("loadThemeFile merged %d styles; it must return only what the file declared", len(loaded.Styles))
	}

	theme := resolveTheme(path, false)
	if theme.Styles["accent"].FG != defaultTheme.Styles["accent"].FG {
		t.Fatalf("accent = %+v, want the default %+v", theme.Styles["accent"], defaultTheme.Styles["accent"])
	}
	if theme.Styles["error"].FG != defaultTheme.Styles["error"].FG {
		t.Fatalf("error style = %+v, want the default", theme.Styles["error"])
	}
	// The one style the file named is the file's, not the default's.
	if theme.Styles["tile"].BG != "#8579e8" {
		t.Fatalf("tile.BG = %q, want the file's #8579e8", theme.Styles["tile"].BG)
	}
}

// TestBadThemeFallsBackWithAWarning is table-driven: invalid YAML, an unknown
// key, a malformed hex colour, an unknown style name in the format, and an
// unclosed brace each produce the default theme and a warning naming the
// file.
func TestBadThemeFallsBackWithAWarning(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string // a fragment the warning must name
	}{
		{"invalid yaml", "styles: [unclosed", "theme file"},
		{"unknown key", "nonsense: true\nstyles:\n  accent:\n    fg: \"#a99cf5\"\n", "nonsense"},
		{"malformed hex", "styles:\n  accent:\n    fg: \"red\"\n", "#rrggbb"},
		{"unknown style in format", "styles:\n  accent:\n    fg: \"#a99cf5\"\nprompt_format: \"{nope:hi}\"\n", "nope"},
		{"unclosed brace", "styles:\n  accent:\n    fg: \"#a99cf5\"\nprompt_format: \"{accent:hi\"\n", "unclosed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "theme.yml")
			if err := os.WriteFile(path, []byte(tc.content), 0644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}

			// Capture stderr so the test can assert the warning names the
			// file even though the REPL must keep going.
			old := os.Stderr
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("pipe: %v", err)
			}
			os.Stderr = w
			theme := resolveTheme(path, true)
			_ = w.Close()
			os.Stderr = old
			warning, _ := io.ReadAll(r)

			if theme == nil {
				t.Fatal("resolveTheme returned nil; the REPL must not refuse to start")
			}
			if theme.Source != "built-in" {
				t.Fatalf("broken theme %q resolved source %q, want the default", tc.name, theme.Source)
			}
			if !strings.Contains(string(warning), path) {
				t.Fatalf("warning %q does not name the file %s", warning, path)
			}
			if !strings.Contains(string(warning), tc.want) {
				t.Fatalf("warning %q does not name %q", warning, tc.want)
			}
		})
	}
}

// TestNoColourWhenPiped drives the prompt through a non-TTY writer and
// asserts no escape byte appears, whatever the theme says.
func TestNoColourWhenPiped(t *testing.T) {
	// A builder is not a TTY: resolveTheme must conclude colour is off.
	theme := resolveTheme("", false)
	if theme.Colour {
		t.Fatal("colour is on for a non-TTY writer")
	}
	rendered := theme.renderPrompt("app")
	if strings.ContainsRune(rendered, 0x1b) {
		t.Fatalf("piped prompt contains an escape byte: %q", rendered)
	}
}

// TestNoColourUnderNoColor and the TERM=dumb variant.
func TestNoColourUnderNoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	theme := resolveTheme("", true)
	if theme.Colour {
		t.Fatal("colour is on with NO_COLOR set")
	}
	if rendered := theme.renderPrompt("app"); strings.ContainsRune(rendered, 0x1b) {
		t.Fatalf("prompt contains an escape under NO_COLOR: %q", rendered)
	}
}

func TestNoColourTermDumb(t *testing.T) {
	t.Setenv("TERM", "dumb")
	theme := resolveTheme("", true)
	if theme.Colour {
		t.Fatal("colour is on with TERM=dumb")
	}
}

// TestPromptHidesEmptyDatabaseSpan checks the [ ] around an absent database
// vanish, and that prompt_format_nodb is used when set.
func TestPromptHidesEmptyDatabaseSpan(t *testing.T) {
	theme := defaultTheme
	theme.Colour = false

	withoutDB := theme.renderPrompt("")
	if strings.Contains(withoutDB, "[ ]") {
		t.Fatalf("prompt without a database shows [ ]: %q", withoutDB)
	}
	withDB := theme.renderPrompt("app")
	if !strings.Contains(withDB, "[app]") {
		t.Fatalf("prompt with a database lacks [app]: %q", withDB)
	}

	// prompt_format_nodb wins when both formats are set and no DB is selected.
	custom := defaultTheme
	custom.Colour = false
	custom.PromptNoDB = "no-db> "
	if got := custom.renderPrompt(""); got != "no-db> " {
		t.Fatalf("renderPrompt with nodb = %q, want the nodb format", got)
	}
}

// TestOSC11ReplyParsing is table-driven over the reply forms in §2.4 plus
// garbage and an empty read, with no terminal involved.
func TestOSC11ReplyParsing(t *testing.T) {
	cases := []struct {
		name  string
		reply []byte
		want  string
	}{
		{"bel dark", []byte("\x1b]11;rgb:0000/0000/0000\x07"), "dark"},
		{"bel light", []byte("\x1b]11;rgb:ffff/ffff/ffff\x07"), "light"},
		{"st terminator", []byte("\x1b]11;rgb:1111/2222/3333\x1b\\"), "dark"},
		{"hex form", []byte("\x1b]11;#ffffff\x07"), "light"},
		{"short components", []byte("\x1b]11;rgb:f/f/f\x07"), "light"},
		{"three-digit components", []byte("\x1b]11;rgb:fff/fff/fff\x07"), "light"},
		{"garbage", []byte("hello\x07"), ""},
		{"empty", nil, ""},
		{"partial", []byte("\x1b]11;rgb:ff"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseOSC11Reply(tc.reply); got != tc.want {
				t.Fatalf("parseOSC11Reply(%q) = %q, want %q", tc.reply, got, tc.want)
			}
		})
	}
}

// TestThemeVerbPrintsResolution drives the offline `theme` verb.
func TestThemeVerbPrintsResolution(t *testing.T) {
	var out strings.Builder
	c := NewCLI(WithTerminal(&out))
	// A builder is not a TTY, so colour resolves off; the verb still prints.
	cmd, err := ParseCommand("theme")
	if err != nil {
		t.Fatalf("ParseCommand(theme): %v", err)
	}
	if err := c.handle("theme"); err != nil {
		t.Fatalf("theme verb: %v", err)
	}
	_ = cmd
	if !strings.Contains(out.String(), "name:") || !strings.Contains(out.String(), "prompt:") {
		t.Fatalf("theme output lacks name/prompt: %q", out.String())
	}
}

// TestXterm256Fallback covers the colour-reduction path so no terminal with
// truecolor off gets an empty escape.
func TestXterm256Fallback(t *testing.T) {
	t.Setenv("COLORTERM", "")
	// The logo tile's indigo maps to some index in 16..255, never empty.
	escape := bgEscape("#3c3489")
	if escape == "" {
		t.Fatal("bgEscape returned empty for the tile colour")
	}
	if !strings.HasPrefix(escape, "\x1b[48;5;") {
		t.Fatalf("bgEscape = %q, want an xterm-256 background escape", escape)
	}
}

// TestHexComponentParity pins the rgb: component scaling.
func TestHexComponentParity(t *testing.T) {
	v, ok := hexComponent("ff")
	if !ok || v != 255 {
		t.Fatalf("hexComponent(ff) = %d, %v", v, ok)
	}
	v, ok = hexComponent("f")
	if !ok || v != 255 {
		t.Fatalf("hexComponent(f) = %d, %v (one digit scales by 0x11)", v, ok)
	}
	if _, ok := hexComponent(""); ok {
		t.Fatal("empty component parsed")
	}
}

// TestVersionVerbMatchesTheBanner pins GAP-4's in-band half: the REPL
// `version` verb and the -v flag print the same version string.
func TestVersionVerbMatchesTheBanner(t *testing.T) {
	var out strings.Builder
	c := NewCLI(WithTerminal(&out))
	if err := c.handle("version"); err != nil {
		t.Fatalf("version verb: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, version.String()) {
		t.Fatalf("version verb = %q, missing %q", got, version.String())
	}
	if !strings.Contains(got, "protocol=") {
		t.Fatalf("version verb = %q, missing the protocol", got)
	}
}

// themeWarning loads a theme file with the given content through resolveTheme
// and returns the warning it printed and the theme it fell back to.
func themeWarning(t *testing.T, content string) (string, *Theme) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "theme.yml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	theme := resolveTheme(path, true)
	_ = w.Close()
	os.Stderr = old
	warning, _ := io.ReadAll(r)
	return string(warning), theme
}

// yamlTagKeys reads a struct's yaml keys by reflection, independently of the
// production helper, so the tests below compare the message with the struct.
func yamlTagKeys(value any) []string {
	valueType := reflect.TypeOf(value)
	keys := make([]string, 0, valueType.NumField())
	for i := 0; i < valueType.NumField(); i++ {
		keys = append(keys, strings.Split(valueType.Field(i).Tag.Get("yaml"), ",")[0])
	}
	return keys
}

// TestThemeUnknownKeyNamesTheValidKeys: `prompt: red` was refused
// with yaml's text and the Go type name cli.themeFile, and no list of the keys
// a theme file does accept.
func TestThemeUnknownKeyNamesTheValidKeys(t *testing.T) {
	warning, theme := themeWarning(t, "prompt: red\n")
	if theme.Source != "built-in" {
		t.Errorf("theme source = %q, want the default theme", theme.Source)
	}
	if strings.Contains(warning, "cli.themeFile") {
		t.Errorf("warning leaks the Go type name: %q", warning)
	}
	if !strings.Contains(warning, `"prompt"`) {
		t.Errorf("warning does not name the key \"prompt\": %q", warning)
	}
	for _, key := range yamlTagKeys(themeFile{}) {
		if !strings.Contains(warning, key) {
			t.Errorf("warning does not list the valid key %q: %q", key, warning)
		}
	}
}

// TestThemeKeyListMatchesThemeFile fails when a key is added to themeFile and
// the message's list does not follow, because the list is read from the tags.
func TestThemeKeyListMatchesThemeFile(t *testing.T) {
	warning, _ := themeWarning(t, "prompt: red\n")
	_, afterMarker, found := strings.Cut(warning, "valid keys: ")
	if !found {
		t.Fatalf("warning has no %q list: %q", "valid keys: ", warning)
	}
	listed, _, _ := strings.Cut(afterMarker, " \u2014")
	got := strings.Split(listed, ", ")
	want := yamlTagKeys(themeFile{})
	if len(want) == 0 || strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("message lists %v, themeFile's yaml tags are %v", got, want)
	}
}
