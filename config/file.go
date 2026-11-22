package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// PathEnv is the environment variable that names the YAML config file. The
// server's --config flag is the same setting and wins when both are given.
const PathEnv = "HOARDDB_CONFIG"

// ResolvePath returns the config file path the server should load: the
// --config flag value, else HOARDDB_CONFIG, else "" (no file, defaults and
// environment only). No search path is consulted on purpose: a file read
// without being asked for is the silent behaviour this removed.
func ResolvePath(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	return os.Getenv(PathEnv)
}

// readConfigFile reads the file and words a failure with the path and the way
// out (AGENTS.md rule 3).
func readConfigFile(path string) ([]byte, error) {
	content, err := os.ReadFile(path)
	if err == nil {
		return content, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("config file %q not found; check the path given to --config or %s, or unset %s to start with the built-in defaults", path, PathEnv, PathEnv)
	}
	return nil, fmt.Errorf("config file %q cannot be read: %w; check its permissions, or unset %s to start with the built-in defaults", path, err, PathEnv)
}

// decodeConfigFile decodes the YAML into cfg and refuses any key the Config
// struct does not define: a typo or a documented-but-unbuilt
// setting would otherwise look applied.
func decodeConfigFile(path string, content []byte, cfg *Config) error {
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true)
	err := decoder.Decode(cfg)
	if err == nil || errors.Is(err, io.EOF) {
		// io.EOF is an empty file: nothing to apply, defaults stand.
		return nil
	}
	var typeErr *yaml.TypeError
	if !errors.As(err, &typeErr) {
		return fmt.Errorf("config file %q is not valid YAML: %w", path, err)
	}
	return describeTypeErrors(path, typeErr)
}

// unknownFieldMessage matches yaml.v3's KnownFields error, for example
// "line 4: field bucket_limits not found in type config.Config".
var unknownFieldMessage = regexp.MustCompile(`^line (\d+): field (\S+) not found in type (\S+)$`)

func describeTypeErrors(path string, typeErr *yaml.TypeError) error {
	sections := configSections()
	var problems []string
	for _, message := range typeErr.Errors {
		match := unknownFieldMessage.FindStringSubmatch(message)
		if match == nil {
			problems = append(problems, fmt.Sprintf("%s (config file %q)", message, path))
			continue
		}
		line, key, typeName := match[1], match[2], match[3]
		section := sections[typeName]
		if section.name == "cluster" {
			if removed := removedClusterFileKey(path, line, key); removed != "" {
				problems = append(problems, removed)
				continue
			}
		}
		problems = append(problems, unknownKeyProblem(path, line, key, section))
	}
	return errors.New(strings.Join(problems, "\n"))
}

// configSection is one level of the config: the YAML section name ("" for the
// top level) and the keys it accepts.
type configSection struct {
	name string
	keys []string
}

func unknownKeyProblem(path, line, key string, section configSection) string {
	where := "at the top level"
	if section.name != "" {
		where = "in the " + section.name + " section"
	}
	problem := fmt.Sprintf("config file %q has an unknown key %q at line %s (%s)", path, key, line, where)
	if nearest := nearestKey(key, section.keys); nearest != "" {
		return fmt.Sprintf("%s; did you mean %q? Fix the spelling or remove the key. %s", problem, nearest, exampleHint)
	}
	return fmt.Sprintf("%s; valid keys there: %s. Remove the key or move it to the right section. %s", problem, strings.Join(section.keys, ", "), exampleHint)
}

const exampleHint = "A complete example file is in the \"YAML Config File\" section of the configuration docs."

// configSections maps each config struct's type name, as yaml.v3 prints it, to
// its section name and accepted keys, by reflection over Config so the list
// can never drift from the struct.
func configSections() map[string]configSection {
	sections := map[string]configSection{}
	var walk func(structType reflect.Type, name string)
	walk = func(structType reflect.Type, name string) {
		section := configSection{name: name}
		for index := 0; index < structType.NumField(); index++ {
			field := structType.Field(index)
			key, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
			if key == "-" || key == "" {
				continue
			}
			section.keys = append(section.keys, key)
			if field.Type.Kind() == reflect.Struct {
				walk(field.Type, key)
			}
		}
		sections[structType.String()] = section
	}
	walk(reflect.TypeOf(Config{}), "")
	return sections
}

// nearestKey returns the accepted key closest to key, or "" when none is
// close enough to be a plausible typo.
func nearestKey(key string, accepted []string) string {
	limit := 1
	if len(key) >= 5 {
		limit = 2
	}
	best, bestDistance := "", limit+1
	sorted := append([]string(nil), accepted...)
	sort.Strings(sorted)
	for _, candidate := range sorted {
		if distance := editDistance(key, candidate); distance < bestDistance {
			best, bestDistance = candidate, distance
		}
	}
	return best
}

// editDistance is the Levenshtein distance between two short ASCII keys.
func editDistance(left, right string) int {
	previous := make([]int, len(right)+1)
	for column := range previous {
		previous[column] = column
	}
	for row := 1; row <= len(left); row++ {
		current := make([]int, len(right)+1)
		current[0] = row
		for column := 1; column <= len(right); column++ {
			substitution := previous[column-1]
			if left[row-1] != right[column-1] {
				substitution++
			}
			current[column] = min(substitution, previous[column]+1, current[column-1]+1)
		}
		previous = current
	}
	return previous[len(right)]
}
