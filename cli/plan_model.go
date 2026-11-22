package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// PlanModelVersion is the only model file version this build reads; a file
// with another value is refused, so an old file never changes a plan silently.
const PlanModelVersion = 1

// planSourceWords are the producers a model file may name in "source".
var planSourceWords = []string{"dump", "hand", "agent"}

// planRoleWords are the roles a field may carry; an empty role means none.
var planRoleWords = []string{"counter", "period", "ttl_at", "soft_delete"}

// PlanModel is the one seam: every producer writes it, the deterministic core
// reads it (spec section 17.1).
type PlanModel struct {
	Version int         `json:"version" doc:"must be 1"`
	Source  string      `json:"source" doc:"dump, hand or agent"`
	App     string      `json:"app,omitempty" doc:"the application name, for the plan heading"`
	Kinds   []ModelKind `json:"kinds" doc:"one per source collection or table"`
	Links   []ModelLink `json:"links,omitempty" doc:"link tables read from both sides"`
	Fans    []ModelFan  `json:"fans,omitempty" doc:"children per parent, such as containers per principal"`
	Trace   []TraceLine `json:"trace,omitempty" doc:"the access trace: what the application does with the data"`
}

// ModelKind is one source collection or table.
type ModelKind struct {
	Name        string       `json:"name"`
	Documents   int64        `json:"documents" doc:"documents in the source"`
	Sampled     int64        `json:"sampled" doc:"documents the counts were taken from"`
	ExpireWhole bool         `json:"expire_whole,omitempty" doc:"the whole collection expires"`
	Fields      []ModelField `json:"fields" doc:"every field of the kind"`
	Indexes     [][]string   `json:"indexes,omitempty" doc:"indexes the source declares, each a list of field names"`
}

// ModelField is one field of a kind. Every fraction is an integer per-mille
// (0 to 1000), so two runs on two platforms print the same bytes.
type ModelField struct {
	Name             string      `json:"name"`
	Type             string      `json:"type"`
	PresentPermille  int         `json:"present_permille" doc:"share of documents that carry the field, 0 to 1000"`
	DistinctPermille int         `json:"distinct_permille,omitempty" doc:"share of distinct values, 0 to 1000"`
	BytesP50         int64       `json:"bytes_p50,omitempty" doc:"median size in bytes"`
	Role             string      `json:"role,omitempty" doc:"counter, period, ttl_at or soft_delete"`
	Ref              *ModelRef   `json:"ref,omitempty" doc:"the field holds the key of another kind"`
	Array            *ModelArray `json:"array,omitempty" doc:"facts about an array field"`
}

// ModelRef says a field holds the key of another kind's document.
type ModelRef struct {
	To              string `json:"to"`
	ResolvePermille int    `json:"resolve_permille"`
}

// ModelArray carries the length and element facts of an array field.
type ModelArray struct {
	LenP50              int    `json:"len_p50"`
	LenP95              int    `json:"len_p95"`
	LenMax              int    `json:"len_max"`
	ElemIsDocument      bool   `json:"elem_is_document"`
	OrderField          string `json:"order_field,omitempty"`
	MonotonePermille    int    `json:"monotone_permille,omitempty"`
	KeyField            string `json:"key_field,omitempty"`
	KeyDistinctPermille int    `json:"key_distinct_permille,omitempty"`
	KeyIsTime           bool   `json:"key_is_time,omitempty"`
}

// ModelLink is a link table read from both sides (spec section 16.1): Kind is
// its kind name, A and B the two kinds it joins.
type ModelLink struct {
	Kind string `json:"kind"`
	A    string `json:"a"`
	B    string `json:"b"`
}

// ModelFan is the children per parent (containers per principal, comments per
// post), spec section 16.2.
type ModelFan struct {
	Parent string `json:"parent"`
	Child  string `json:"child"`
	P50    int    `json:"p50"`
	P95    int    `json:"p95"`
	Max    int    `json:"max"`
}

// LoadPlanModel reads one model strictly. An unknown field is refused, naming
// the field and the nearest known one, so a misspelt key cannot read as zero.
func LoadPlanModel(src io.Reader) (*PlanModel, error) {
	decoder := json.NewDecoder(src)
	decoder.DisallowUnknownFields()
	var model PlanModel
	if err := decoder.Decode(&model); err != nil {
		return nil, planDecodeError(err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("plan model: the file holds more than one JSON object. Fix it so the file is one object")
	}
	for index := range model.Trace {
		if model.Trace[index].Ordinal == 0 {
			model.Trace[index].Ordinal = index + 1
		}
	}
	return &model, nil
}

// planDecodeError turns the standard unknown-field error into a sentence that
// names the nearest known JSON key; other decode errors keep their wrapping.
func planDecodeError(err error) error {
	const prefix = "json: unknown field "
	text := err.Error()
	if !strings.HasPrefix(text, prefix) {
		return fmt.Errorf("plan model: %w", err)
	}
	name, unquoteErr := strconv.Unquote(strings.TrimPrefix(text, prefix))
	if unquoteErr != nil {
		return fmt.Errorf("plan model: %w", err)
	}
	known := planModelKeys()
	best, bestDistance := "", 3
	for _, key := range known {
		if distance := ringEditDistance(name, key); distance < bestDistance {
			best, bestDistance = key, distance
		}
	}
	if best != "" {
		return fmt.Errorf("plan model: unknown field %q. Did you mean %q? Fix the name, or delete the field", name, best)
	}
	return fmt.Errorf("plan model: unknown field %q. The known fields are %s. Fix the name, or delete the field",
		name, strings.Join(known, ", "))
}

// planModelKeys lists every JSON key of the model types, sorted, once per call.
func planModelKeys() []string {
	types := []reflect.Type{
		reflect.TypeOf(PlanModel{}), reflect.TypeOf(ModelKind{}), reflect.TypeOf(ModelField{}),
		reflect.TypeOf(ModelRef{}), reflect.TypeOf(ModelArray{}), reflect.TypeOf(ModelLink{}),
		reflect.TypeOf(ModelFan{}),
	}
	var keys []string
	for _, typ := range types {
		for index := range typ.NumField() {
			tag := typ.Field(index).Tag.Get("json")
			name, _, _ := strings.Cut(tag, ",")
			if name != "" && name != "-" {
				keys = append(keys, name)
			}
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// ValidatePlanModel returns the first problem as an error that names the kind
// or field and the fix, or nil when the model is one this build can plan.
func ValidatePlanModel(model *PlanModel) error {
	if model.Version != PlanModelVersion {
		return fmt.Errorf("plan model: version is %d, this build reads version %d. Fix the file by setting \"version\": %d",
			model.Version, PlanModelVersion, PlanModelVersion)
	}
	if !slices.Contains(planSourceWords, model.Source) {
		return fmt.Errorf("plan model: source %q is not one of dump, hand or agent. Fix the source to name the producer",
			model.Source)
	}
	kindNames := make(map[string]bool, len(model.Kinds))
	for _, kind := range model.Kinds {
		if kind.Name == "" {
			return errors.New("plan model: a kind has an empty name. Fix it by naming every kind after its source collection or table")
		}
		if kindNames[kind.Name] {
			return fmt.Errorf("plan model: kind %q is declared twice. Fix it by merging the two entries or renaming one", kind.Name)
		}
		kindNames[kind.Name] = true
	}
	for _, kind := range model.Kinds {
		if err := validatePlanKind(kind, kindNames); err != nil {
			return err
		}
	}
	return nil
}

// validatePlanKind checks one kind's fields against the kind names of the model.
func validatePlanKind(kind ModelKind, kindNames map[string]bool) error {
	for _, field := range kind.Fields {
		where := fmt.Sprintf("kind %q field %q", kind.Name, field.Name)
		if err := planCheckPermille(where, "present_permille", field.PresentPermille); err != nil {
			return err
		}
		if err := planCheckPermille(where, "distinct_permille", field.DistinctPermille); err != nil {
			return err
		}
		if field.Role != "" && !slices.Contains(planRoleWords, field.Role) {
			return fmt.Errorf("plan model: %s: role %q is not one of counter, period, ttl_at or soft_delete. Fix the role, or leave it empty",
				where, field.Role)
		}
		if field.Ref != nil {
			if err := planCheckPermille(where, "ref.resolve_permille", field.Ref.ResolvePermille); err != nil {
				return err
			}
			if !kindNames[field.Ref.To] {
				return fmt.Errorf("plan model: %s: ref.to %q is not a kind in this model (kinds: %s). Fix the name, or add the kind",
					where, field.Ref.To, strings.Join(planSortedKindNames(kindNames), ", "))
			}
		}
		if field.Array != nil {
			if err := planCheckArray(where, field.Array); err != nil {
				return err
			}
		}
	}
	return nil
}

// planCheckArray checks the permilles and the length order of an array field.
func planCheckArray(where string, array *ModelArray) error {
	if err := planCheckPermille(where, "array.monotone_permille", array.MonotonePermille); err != nil {
		return err
	}
	if err := planCheckPermille(where, "array.key_distinct_permille", array.KeyDistinctPermille); err != nil {
		return err
	}
	if array.LenP50 > array.LenP95 || array.LenP95 > array.LenMax {
		return fmt.Errorf("plan model: %s: array lengths must satisfy len_p50 <= len_p95 <= len_max, got %d, %d, %d. Fix the three numbers so they are in that order",
			where, array.LenP50, array.LenP95, array.LenMax)
	}
	return nil
}

// planCheckPermille refuses a fraction outside the per-mille range.
func planCheckPermille(where, name string, permille int) error {
	if permille < 0 || permille > 1000 {
		return fmt.Errorf("plan model: %s: %s is %d, but a fraction is a per-mille count from 0 to 1000. Fix the count, or drop the field",
			where, name, permille)
	}
	return nil
}

// planSortedKindNames lists the kind names in order, so a refusal prints the same text every run.
func planSortedKindNames(kindNames map[string]bool) []string {
	names := make([]string, 0, len(kindNames))
	for name := range kindNames {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
