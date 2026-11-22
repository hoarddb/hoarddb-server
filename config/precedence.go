package config

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// warnEnvOverridesFile logs one warning for each key that the config file sets
// and a HOARDDB_* variable then replaces with a different value. Environment
// beats file by design, but the container image sets variables of its own
// (BUG-0317), so without this line a bind an operator narrowed in the file
// widens again and nothing says so. fromFile is the configuration after the
// file was applied and merged is the result after the environment was; yamlData
// is the raw file, which is how a key the file wrote is told from a default.
func warnEnvOverridesFile(fromFile, merged *Config, yamlData []byte) {
	if len(yamlData) == 0 {
		return
	}
	var fileKeys map[string]any
	if err := yaml.Unmarshal(yamlData, &fileKeys); err != nil || fileKeys == nil {
		// decodeConfigFile already accepted this file, so only a non-mapping
		// document lands here; it sets no keys.
		return
	}
	walkEnvKeys(reflect.ValueOf(fromFile).Elem(), reflect.ValueOf(merged).Elem(), fileKeys, "")
}

// walkEnvKeys visits every field of a config section that carries both a yaml
// and an env tag, descending into nested sections. fileSection is the part of
// the raw file at the same level.
func walkEnvKeys(fromFile, merged reflect.Value, fileSection map[string]any, prefix string) {
	sectionType := fromFile.Type()
	for fieldIndex := 0; fieldIndex < sectionType.NumField(); fieldIndex++ {
		field := sectionType.Field(fieldIndex)
		yamlName, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
		if yamlName == "" || yamlName == "-" {
			continue
		}
		fileEntry, setInFile := fileSection[yamlName]
		if !setInFile || fileEntry == nil {
			continue
		}
		key := prefix + yamlName
		if field.Type.Kind() == reflect.Struct {
			nestedSection, isSection := fileEntry.(map[string]any)
			if isSection {
				walkEnvKeys(fromFile.Field(fieldIndex), merged.Field(fieldIndex), nestedSection, key+".")
			}
			continue
		}
		variable := field.Tag.Get("env")
		if variable == "" || variable == "-" || os.Getenv(variable) == "" {
			continue
		}
		fileValue, mergedValue := fromFile.Field(fieldIndex).Interface(), merged.Field(fieldIndex).Interface()
		if reflect.DeepEqual(fileValue, mergedValue) {
			continue
		}
		slog.Warn(fmt.Sprintf("%s is set in the config file to %v, but %s=%v overrides it. Unset %s to use the file's value, or remove the key from the file to make the variable the only source",
			key, fileValue, variable, mergedValue, variable),
			"key", key, "variable", variable)
	}
}
