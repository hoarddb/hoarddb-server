package cli

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

// Theme is the resolved prompt theme: the styles and the two format strings.
// It is built from the built-in default, overridden by ~/.hoarddb/theme.yml,
// then by $HOARDDB_THEME, then by --theme. `plain` resolves to a theme that
// renders no colour, which is what docs, screenshots and tests pin so output
// does not depend on whose home directory ran it.
type Theme struct {
	Name         string
	Source       string // "built-in", a path, or "plain"
	Colour       bool
	ColourReason string // why colour is off, when it is
	Background   string // "dark", "light", or "" when unknown
	Styles       map[string]Style
	PromptFormat string
	PromptNoDB   string
}

// Style is one named foreground/background pair, either of which may be unset.
type Style struct {
	FG string // "#rrggbb", or ""
	BG string
}

// themeFile is the on-disk YAML shape (spec §2.1). KnownFields(true) makes a
// typo an error instead of a silently-ignored key.
type themeFile struct {
	Version          int                  `yaml:"version"`
	Name             string               `yaml:"name"`
	Styles           map[string]StyleYAML `yaml:"styles"`
	PromptFormat     string               `yaml:"prompt_format"`
	PromptFormatNoDB string               `yaml:"prompt_format_nodb"`
}

// goTypeNames matches yaml.v3's " in type cli.X" suffix, an internal name a
// user cannot act on.
var goTypeNames = regexp.MustCompile(` in type cli\.\w+`)

// themeFileKeys lists the top-level keys themeFile accepts, read from its yaml
// tags so the warning for an unknown key cannot drift from the struct.
func themeFileKeys() []string {
	fileType := reflect.TypeOf(themeFile{})
	keys := make([]string, 0, fileType.NumField())
	for i := 0; i < fileType.NumField(); i++ {
		keys = append(keys, strings.Split(fileType.Field(i).Tag.Get("yaml"), ",")[0])
	}
	return keys
}

// rejectUnknownThemeKeys refuses a top-level key themeFile does not have and
// names the keys it does. It runs before the strict decode because yaml.v3
// reports the key only inside text that carries the Go type name; a syntax
// error is left for that decode to report.
func rejectUnknownThemeKeys(path string, content []byte) error {
	var root yaml.Node
	if err := yaml.Unmarshal(content, &root); err != nil || len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return nil
	}
	valid := themeFileKeys()
	mapping := root.Content[0].Content
	for i := 0; i+1 < len(mapping); i += 2 {
		keyNode := mapping[i]
		if !slices.Contains(valid, keyNode.Value) {
			return fmt.Errorf("theme file %s: unknown key %q (line %d); valid keys: %s \u2014 run 'help theme' for the format",
				path, keyNode.Value, keyNode.Line, strings.Join(valid, ", "))
		}
	}
	return nil
}

// StyleYAML mirrors Style with yaml tags (Style's fields are plain).
type StyleYAML struct {
	FG *string `yaml:"fg"`
	BG *string `yaml:"bg"`
}

// built-in theme: the logo palette, measured from the artwork.
//
// Contrast is why the default is built this way: #3c3489 as *text* on a black
// terminal is 1.78:1 (unreadable), while as a *background* under #f1efe8 it is
// 8.93:1 — which is exactly how the logo uses it. So no span in the default
// prompt uses #3c3489: accent carries the readable lightened indigo for the
// letters that carry the wordmark (the h and DB), and tile keeps the logo
// block available to a theme file that wants it.
var defaultTheme = Theme{
	Name:   "hoard",
	Source: "built-in",
	Styles: map[string]Style{
		"brand":    Style{FG: "#f1efe8", BG: ""},
		"accent":   Style{FG: "#a99cf5", BG: ""},
		"tile":     Style{FG: "#f1efe8", BG: "#3c3489"},
		"database": Style{FG: "#6f63d6", BG: ""},
		"error":    Style{FG: "#f07178", BG: ""},
	},
	// The mark reads as part of the wordmark: the h carries accent, the same
	// purple as DB, and the brackets are brand. No span paints a background,
	// so the prompt is one line of foreground colour on whatever the terminal
	// is — legible on a light background too, which a filled block is not.
	PromptFormat: "{brand:[}{accent:h}{brand:]}{brand:oard}{accent:DB}{database: [%database]}> ",
	PromptNoDB:   "{brand:[}{accent:h}{brand:]}{brand:oard}{accent:DB}> ",
}

// plainTheme is what `--theme plain` resolves to: the same prompt text, but
// no colour escapes at all.
var plainTheme = Theme{
	Name:         "plain",
	Source:       "plain",
	Colour:       false,
	ColourReason: "plain theme requested",
	PromptFormat: "[h]oardDB [%database]> ",
	PromptNoDB:   "[h]oardDB> ",
}

// hoarddbDir is where the CLI keeps the files that belong to a user rather than
// to a server: the theme.yml a session reads for its prompt. A session keeps no
// state of its own on disk — its history lives in the line editor and goes with
// it (cli/cli.go:Run).
func hoarddbDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("finding the home directory: %w", err)
	}
	return filepath.Join(homeDir, ".hoarddb"), nil
}

// resolveTheme loads the theme per the precedence chain: built-in default →
// ~/.hoarddb/theme.yml → $HOARDDB_THEME → --theme. It also decides whether
// colour is emitted at all (spec §2.3) and queries the terminal background
// for the dark/light variant when colour is on (spec §2.4).
//
// A broken theme file never stops the REPL: it prints one warning naming the
// file, the offending key and the fallback, then runs the default theme.
func resolveTheme(themeFlag string, stdoutIsTTY bool) *Theme {
	// A copy, because the session's theme is mutated after resolution:
	// lightVariant rewrites the tile for a light terminal. Styles is a map, so
	// copying the Theme by value shares it — writing through it would change
	// the built-in default for the rest of the process.
	theme := cloneTheme(defaultTheme)
	theme.Colour = colourAllowed(themeFlag, stdoutIsTTY)

	source := "built-in"
	name := theme.Name

	// Precedence: --theme beats HOARDDB_THEME beats the file. `plain` is
	// handled before any file is read, so a broken file cannot override a
	// user's explicit plain request.
	requested := themeFlag
	if requested == "" {
		requested = os.Getenv("HOARDDB_THEME")
	}
	if requested == "plain" {
		plain := plainTheme
		plain.Colour = false
		plain.ColourReason = "plain theme requested"
		return &plain
	}

	// The file, when it exists. The theme directory convention exists
	// already: transport.KnownServersPath is ~/.hoarddb/known_servers.
	path := requested
	if path == "" {
		if dir, err := hoarddbDir(); err == nil {
			path = filepath.Join(dir, "theme.yml")
		}
	}
	if path != "" {
		if _, statErr := os.Stat(path); statErr != nil && os.IsNotExist(statErr) {
			// No theme file is the normal case; silently use the built-in.
		} else {
			if loaded, ldErr := loadThemeFile(path); ldErr != nil {
				fmt.Fprintf(os.Stderr, "warning: %v — using the default theme\n", ldErr)
			} else {
				theme = inheritDefaults(loaded)
				source = path
				name = loaded.Name
			}
		}
	}

	theme.Name = name
	theme.Source = source
	theme.Colour = colourAllowed(themeFlag, stdoutIsTTY)
	return &theme
}

// cloneTheme copies a theme and its styles map. resolveTheme hands the session
// a theme lightVariant will rewrite, so it must not hand out the package-level
// default: Theme copies by value, Styles is a map, and every writer through one
// would otherwise land on defaultTheme itself.
func cloneTheme(t Theme) Theme {
	styles := make(map[string]Style, len(t.Styles))
	for name, style := range t.Styles {
		styles[name] = style
	}
	t.Styles = styles
	return t
}

// inheritDefaults fills in whatever a theme file left out. A file that
// recolours one style — the common case — says nothing about the format
// strings, and a loaded theme used to replace the default outright, so the
// prompt rendered as the empty string. Anything the file does not name comes
// from the built-in theme, which is what makes a partial file a recolouring
// rather than a blank prompt.
//
// loadThemeFile stays literal: it reports what the file said, so a caller can
// tell a partial file from a complete one. The merge belongs here, where the
// file and the default are both in hand.
func inheritDefaults(loaded Theme) Theme {
	for key, style := range defaultTheme.Styles {
		if _, ok := loaded.Styles[key]; !ok {
			loaded.Styles[key] = style
		}
	}
	if loaded.PromptFormat == "" {
		loaded.PromptFormat = defaultTheme.PromptFormat
	}
	if loaded.PromptNoDB == "" {
		loaded.PromptNoDB = defaultTheme.PromptNoDB
	}
	return loaded
}

// loadThemeFile reads and validates one theme file. Any problem — unreadable
// YAML, an unknown key, a colour that is not #rrggbb, an unknown style name
// in the format, an unclosed brace — is an error naming the file and the
// offending key, and the caller falls back to the default theme.
func loadThemeFile(path string) (Theme, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Theme{}, fmt.Errorf("%s: %w", path, err)
	}

	var file themeFile
	if err := rejectUnknownThemeKeys(path, data); err != nil {
		return Theme{}, err
	}
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&file); err != nil {
		return Theme{}, fmt.Errorf("theme file %s: %s", path, goTypeNames.ReplaceAllString(err.Error(), ""))
	}
	if file.Version != 0 && file.Version != 1 {
		return Theme{}, fmt.Errorf("theme file %s: unsupported version %d", path, file.Version)
	}

	theme := Theme{
		Name:         file.Name,
		Source:       path,
		Styles:       make(map[string]Style, len(file.Styles)),
		PromptFormat: file.PromptFormat,
		PromptNoDB:   file.PromptFormatNoDB,
	}
	if theme.Name == "" {
		theme.Name = "custom"
	}
	if len(theme.Styles) == 0 && len(file.Styles) > 0 {
		theme.Styles = make(map[string]Style, len(file.Styles))
	}
	for key, style := range file.Styles {
		converted, err := validateStyle(style)
		if err != nil {
			return Theme{}, fmt.Errorf("theme file %s: style %q: %w", path, key, err)
		}
		theme.Styles[key] = converted
	}
	if err := validatePromptFormat(theme.PromptFormat, theme.Styles); err != nil {
		return Theme{}, fmt.Errorf("theme file %s: prompt_format: %w", path, err)
	}
	if err := validatePromptFormat(theme.PromptNoDB, theme.Styles); err != nil {
		return Theme{}, fmt.Errorf("theme file %s: prompt_format_nodb: %w", path, err)
	}
	return theme, nil
}

// validateStyle checks a style's colours: each must be #rrggbb or absent.
func validateStyle(style StyleYAML) (Style, error) {
	out := Style{}
	if style.FG != nil {
		if !validHex(*style.FG) {
			return Style{}, fmt.Errorf("fg %q is not #rrggbb", *style.FG)
		}
		out.FG = *style.FG
	}
	if style.BG != nil {
		if !validHex(*style.BG) {
			return Style{}, fmt.Errorf("bg %q is not #rrggbb", *style.BG)
		}
		out.BG = *style.BG
	}
	return out, nil
}

// validHex reports whether s is exactly #rrggbb with hex digits.
func validHex(s string) bool {
	if len(s) != 7 || s[0] != '#' {
		return false
	}
	for _, r := range s[1:] {
		if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
			return false
		}
	}
	return true
}

// validatePromptFormat checks the format grammar: spans must be closed and
// name a known style, and the brace/percent escapes must balance.
func validatePromptFormat(format string, styles map[string]Style) error {
	for i := 0; i < len(format); {
		switch format[i] {
		case '{':
			end := strings.IndexByte(format[i:], '}')
			if end < 0 {
				return errors.New("unclosed brace")
			}
			inner := format[i+1 : i+end]
			colon := strings.IndexByte(inner, ':')
			if colon < 0 {
				return fmt.Errorf("span %q has no style name", inner)
			}
			styleName := inner[:colon]
			if _, ok := styles[styleName]; !ok {
				return fmt.Errorf("unknown style %q", styleName)
			}
			// The span's text is not validated: it may itself contain braces
			// escaped as {{ }}, and its expansion is rendered later.
			i += end + 1
		case '}':
			// A lone closing brace is a literal? No: the grammar says {{ and
			// }} are escaped braces, so a lone } is an error.
			return errors.New("unescaped closing brace")
		default:
			i++
		}
	}
	return nil
}

// renderPrompt renders the theme's format for the current database. When
// colour is off the styles are ignored and the spans print their text only.
// A span whose expansion is empty is omitted entirely, which is what keeps
// the [ ] around an absent database from appearing.
func (t *Theme) renderPrompt(dbName string) string {
	format := t.PromptFormat
	if dbName == "" && t.PromptNoDB != "" {
		format = t.PromptNoDB
	}
	return renderFormat(format, dbName, t.Styles, t.Colour)
}

// renderFormat expands one format string: %database becomes the database,
// {style:text} becomes the text in that style (or plain when colour is off),
// %% is a literal %, and {{ }} are literal braces.
func renderFormat(format, dbName string, styles map[string]Style, colour bool) string {
	var out strings.Builder
	for i := 0; i < len(format); {
		switch format[i] {
		case '%':
			if i+1 < len(format) && format[i+1] == '%' {
				out.WriteByte('%')
				i += 2
				continue
			}
			if strings.HasPrefix(format[i:], "%database") {
				out.WriteString(dbName)
				i += len("%database")
				continue
			}
			out.WriteByte('%')
			i++
		case '{':
			if i+1 < len(format) && format[i+1] == '{' {
				out.WriteByte('{')
				i += 2
				continue
			}
			end := strings.IndexByte(format[i:], '}')
			if end < 0 {
				out.WriteByte('{')
				i++
				continue
			}
			inner := format[i+1 : i+end]
			colon := strings.IndexByte(inner, ':')
			if colon < 0 {
				out.WriteString(inner)
				i += end + 1
				continue
			}
			styleName, text := inner[:colon], inner[colon+1:]
			expanded := expandText(text, dbName)
			if expanded == "" {
				// An empty span vanishes entirely, keeping [ ] off the
				// prompt when there is no database.
				i += end + 1
				continue
			}
			out.WriteString(coloured(expanded, styles[styleName], colour))
			i += end + 1
		case '}':
			if i+1 < len(format) && format[i+1] == '}' {
				out.WriteByte('}')
				i += 2
				continue
			}
			out.WriteByte('}')
			i++
		default:
			out.WriteByte(format[i])
			i++
		}
	}
	return out.String()
}

// expandText expands %database inside a span's text.
func expandText(text, dbName string) string {
	return strings.ReplaceAll(text, "%database", dbName)
}

// coloured wraps text in the ANSI escapes for a style, when colour is on.
// With a truecolor-capable terminal the #rrggbb values go straight into
// 24-bit escapes; otherwise they fall back to the nearest xterm-256 index,
// or no colour at all when the terminal cannot show either.
func coloured(text string, style Style, colour bool) string {
	if !colour || text == "" {
		return text
	}
	fg := fgEscape(style.FG)
	bg := bgEscape(style.BG)
	return fg + bg + text + "\x1b[0m"
}

// fgEscape builds a foreground escape for a hex colour, or none if empty.
func fgEscape(hex string) string {
	if hex == "" {
		return ""
	}
	if truecolor() {
		return "\x1b[38;2;" + hexToANSI(hex) + "m"
	}
	return "\x1b[38;5;" + strconv.Itoa(xterm256(hex)) + "m"
}

// bgEscape builds a background escape for a hex colour, or none if empty.
func bgEscape(hex string) string {
	if hex == "" {
		return ""
	}
	if truecolor() {
		return "\x1b[48;2;" + hexToANSI(hex) + "m"
	}
	return "\x1b[48;5;" + strconv.Itoa(xterm256(hex)) + "m"
}

// truecolor reports whether COLORTERM advertises truecolor support.
func truecolor() bool {
	ct := os.Getenv("COLORTERM")
	return strings.EqualFold(ct, "truecolor") || strings.EqualFold(ct, "24bit")
}

// hexToANSI converts #rrggbb to "r;g;b".
func hexToANSI(hex string) string {
	r, _ := strconv.ParseUint(hex[1:3], 16, 8)
	g, _ := strconv.ParseUint(hex[3:5], 16, 8)
	b, _ := strconv.ParseUint(hex[5:7], 16, 8)
	return fmt.Sprintf("%d;%d;%d", r, g, b)
}

// xterm256 maps a hex colour to the nearest xterm-256 palette index using the
// standard 6×6×6 colour cube plus the gray ramp, then a linear scan of the
// worst-case 256 candidates for true nearest-neighbour. The set is small
// enough that a linear scan is simpler and just as fast as a lookup table.
func xterm256(hex string) int {
	r, _ := strconv.ParseUint(hex[1:3], 16, 8)
	g, _ := strconv.ParseUint(hex[3:5], 16, 8)
	b, _ := strconv.ParseUint(hex[5:7], 16, 8)
	best, bestDist := 16, 1<<30
	for i := 16; i < 256; i++ {
		pr, pg, pb, err := xterm256RGB(i)
		if err != nil {
			continue
		}
		dist := int(r-pr)*int(r-pr) + int(g-pg)*int(g-pg) + int(b-pb)*int(b-pb)
		if dist < bestDist {
			best, bestDist = i, dist
		}
	}
	return best
}

// xterm256RGB returns the RGB of xterm-256 index i. Indices 16..231 are the
// 6×6×6 cube; 232..255 the 24-step gray ramp.
func xterm256RGB(i int) (uint64, uint64, uint64, error) {
	if i >= 16 && i <= 231 {
		v := i - 16
		r := v / 36
		g := (v % 36) / 6
		b := v % 6
		toRGB := func(n int64) uint64 { return uint64(55 + n*40) }
		return toRGB(int64(r)), toRGB(int64(g)), toRGB(int64(b)), nil
	}
	if i >= 232 && i <= 255 {
		v := 8 + (i-232)*10
		return uint64(v), uint64(v), uint64(v), nil
	}
	return 0, 0, 0, fmt.Errorf("not an xterm-256 index: %d", i)
}

// colourAllowed implements the colour policy (spec §2.3): escapes only when
// stdout is a TTY, NO_COLOR is unset or empty, and TERM is set and not dumb. The TTY
// test is on stdout, not on the stdin interactive() predicate, because a
// piped session still prints the prompt to the writer.
func colourAllowed(themeFlag string, stdoutIsTTY bool) bool {
	if _, ok := os.LookupEnv("NO_COLOR"); ok && os.Getenv("NO_COLOR") != "" {
		return false
	}
	if !terminalCanTakeEscapes() {
		return false
	}
	if !stdoutIsTTY {
		return false
	}
	return themeFlag != "plain" && os.Getenv("HOARDDB_THEME") != "plain"
}

// queryBackground runs the OSC 11 query (spec §2.4): write "ESC ] 11 ; ? BEL"
// to the terminal, read the reply with a 50 ms budget, and return "dark",
// "light" or "". It runs once per session. A terminal that does not answer —
// old tmux, some ssh paths — yields "no reply", and HOARDDB_BACKGROUND pins
// the answer for those terminals.
//
// The reply arrives on stdin and readLineRaw reads stdin one byte at a time,
// so anything left unread lands in the first line the user types. The query
// therefore runs in raw mode with echo off and drains everything until the
// terminator or the deadline.
func queryBackground() string {
	if pinned := os.Getenv("HOARDDB_BACKGROUND"); pinned == "dark" || pinned == "light" {
		return pinned
	}

	fd := int(os.Stdin.Fd())
	if _, err := unix.IoctlGetTermios(fd, termiosGetRequest); err != nil {
		// Not a terminal: a query sent to a pipe is pure noise.
		return ""
	}
	original, err := unix.IoctlGetTermios(fd, termiosGetRequest)
	if err != nil {
		return ""
	}
	raw := *original
	raw.Lflag &^= unix.ICANON | unix.ECHO
	if err := unix.IoctlSetTermios(fd, termiosSetRequest, &raw); err != nil {
		return ""
	}
	defer func() { _ = unix.IoctlSetTermios(fd, termiosSetRequest, original) }()

	out := os.Stdout
	_, _ = fmt.Fprintf(out, "\x1b]11;?\x07")
	_ = out.Sync()

	// Read with a 50 ms budget via unix.Poll. VTIME counts deciseconds, so
	// termios cannot express 50 ms; a poll timeout can.
	var buf []byte
	deadline := time.Now().Add(50 * time.Millisecond)
	var pollFds = []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		n, err := unix.Poll(pollFds, int(remaining.Milliseconds()))
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return ""
		}
		if n == 0 {
			break
		}
		var b [256]byte
		readN, readErr := os.Stdin.Read(b[:])
		if readErr != nil {
			return ""
		}
		buf = append(buf, b[:readN]...)
		// A BEL or ST ends the reply.
		if bytesContainsAny(buf, []byte{0x07, 0x1B}) {
			break
		}
	}

	return parseOSC11Reply(buf)
}

// bytesContainsAny reports whether buf holds any of the terminator bytes.
func bytesContainsAny(buf []byte, markers []byte) bool {
	for _, m := range markers {
		for _, b := range buf {
			if b == m {
				return true
			}
		}
	}
	return false
}

// parseOSC11Reply interprets what the terminal answered: BEL or ST
// terminators, rgb:RRRR/GGGG/BBBB (1-4 hex digits per component), or
// #rrggbb. Anything else — including a partial read — is "did not answer".
func parseOSC11Reply(reply []byte) string {
	text := strings.TrimLeft(string(reply), "\x1b]")
	text = strings.TrimPrefix(text, "11;")
	for _, term := range []string{"\x07", "\x1b\\"} {
		if i := strings.Index(text, term); i >= 0 {
			text = text[:i]
		}
	}
	text = strings.TrimSpace(text)

	if strings.HasPrefix(text, "rgb:") {
		parts := strings.Split(text[4:], "/")
		if len(parts) == 3 {
			r, ok1 := hexComponent(parts[0])
			g, ok2 := hexComponent(parts[1])
			b, ok3 := hexComponent(parts[2])
			if ok1 && ok2 && ok3 {
				return backgroundName(r, g, b)
			}
		}
	} else if strings.HasPrefix(text, "#") && len(text) == 7 {
		if validHex(text) {
			r, _ := strconv.ParseUint(text[1:3], 16, 8)
			g, _ := strconv.ParseUint(text[3:5], 16, 8)
			b, _ := strconv.ParseUint(text[5:7], 16, 8)
			return backgroundName(uint64(r), uint64(g), uint64(b))
		}
	}
	return ""
}

// hexComponent parses one rgb: component, 1-4 hex digits, scaled to 8 bits.
func hexComponent(s string) (uint64, bool) {
	if len(s) < 1 || len(s) > 4 {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, false
	}
	switch len(s) {
	case 1:
		return v * 0x11, true // f -> ff
	case 2:
		return v, true
	case 3:
		return v / 16, true
	default:
		return v / 256, true
	}
}

// backgroundName picks dark or light from a background colour. The split is
// on relative luminance, per WCAG: a luminance of 0.5 or more is light.
func backgroundName(r, g, b uint64) string {
	toLinear := func(c uint64) float64 {
		v := float64(c) / 255.0
		if v <= 0.03928 {
			return v / 12.92
		}
		return math.Pow((v+0.055)/1.055, 2.4)
	}
	luminance := 0.2126*toLinear(r) + 0.7152*toLinear(g) + 0.0722*toLinear(b)
	if luminance >= 0.5 {
		return "light"
	}
	return "dark"
}

// lightVariant adapts a theme to a light terminal background: the tile's fg/bg
// swap (off-white block, indigo mark). It runs only when the OSC 11 query
// answered "light".
//
// The default prompt no longer paints a tile, so this is a no-op for it — the
// swap exists for a theme file that paints its own block, which is why tile
// stays a declared style. Removing the block from the default was also what
// made the prompt legible on a light terminal without a theme file: an indigo
// background block under white text is 8.93:1 on dark and unreadable on
// light, and no amount of swapping helps a block that is simply there.
func lightVariant(t *Theme) {
	tile, ok := t.Styles["tile"]
	if !ok {
		return
	}
	tile.FG, tile.BG = tile.BG, tile.FG
	t.Styles["tile"] = tile
}
