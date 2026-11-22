package cli

import (
	"slices"
	"strings"
	"unicode"
)

// PlanGrade says how a suggestion is supported (spec section 2).
type PlanGrade string

// The four grades of spec section 6.3.
const (
	GradeShape    PlanGrade = "shape"
	GradeTrace    PlanGrade = "trace"
	GradeAnswered PlanGrade = "answered"
	GradeConflict PlanGrade = "conflict"
)

// PlanKind is one bucket the plan suggests; StoreFields are its owned collections.
type PlanKind struct {
	Name        string           `json:"name"`
	Sources     []string         `json:"sources"`
	Fields      []PlanField      `json:"fields"`
	StoreFields []PlanStoreField `json:"store_fields,omitempty"`
	Grade       PlanGrade        `json:"grade"`
	Evidence    []PlanSignal     `json:"evidence,omitempty"`
	Open        []string         `json:"open,omitempty"`
}

// PlanField is one scalar field; Planned is a marked note such as "unique".
type PlanField struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Planned string `json:"planned,omitempty"`
	Source  string `json:"source"`
}

// PlanStoreField is one owned collection. An empty Store with Orderless set is
// an open choice, and Alternatives then lists the store words that fit.
type PlanStoreField struct {
	Name         string       `json:"name"`
	Store        string       `json:"store,omitempty"`
	Source       string       `json:"source"`
	Orderless    bool         `json:"order_open,omitempty"`
	Alternatives []string     `json:"alternatives,omitempty"`
	Placements   []string     `json:"placement_alternatives,omitempty"`
	Grade        PlanGrade    `json:"grade"`
	Evidence     []PlanSignal `json:"evidence,omitempty"`
}

// planAnswers maps a choice name ("comments.placement") to the answer word.
type planAnswers map[string]string

var (
	planHeapAlternative = "btreeStore keyed by inverted priority"
	planBlobAlternative = "the pointer layout of large-files.md"
	planOrderWords      = []string{"fifoStore", "lifoStore", "hashStore", "btreeStore"}
)

// planTitle turns snake_case or kebab-case into TitleCase, letters and digits only.
func planTitle(name string) string {
	var out strings.Builder
	upper := true
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			upper = true
			continue
		}
		if upper {
			r = unicode.ToUpper(r)
			upper = false
		}
		out.WriteRune(r)
	}
	return out.String()
}

// planBucketName is the singular TitleCase of a source kind.
func planBucketName(kind string) string {
	name := planTitle(kind)
	switch {
	case strings.HasSuffix(name, "ies") && len(name) > 3:
		return name[:len(name)-3] + "y"
	case strings.HasSuffix(name, "ss"), strings.HasSuffix(name, "us"):
		return name
	case strings.HasSuffix(name, "s") && len(name) > 1:
		return name[:len(name)-1]
	}
	return name
}

func planSignalsAbout(signals []PlanSignal, kind string) []PlanSignal {
	var found []PlanSignal
	for _, signal := range signals {
		if signal.Kind == kind {
			found = append(found, signal)
		}
	}
	return found
}

func planHas(signals []PlanSignal, detectors ...string) bool {
	return slices.ContainsFunc(signals, func(s PlanSignal) bool { return slices.Contains(detectors, s.Detector) })
}

// gradeOf applies spec section 6.3 steps 1 to 4 to one candidate's signals. A
// read across owners (T6, or a key read of an owned thing) beside an
// ownership signal is a conflict; an answer is final.
func gradeOf(signals []PlanSignal, answer string) PlanGrade {
	if answer != "" {
		return GradeAnswered
	}
	owned := planHas(signals, "S6", "T2", "T3", "T4", "T5")
	if owned && planHas(signals, "T6", "T1") {
		return GradeConflict
	}
	if planHas(signals, "T1", "T2", "T3", "T4", "T5", "T6", "T7") {
		return GradeTrace
	}
	return GradeShape
}

// planOverrule copies signals, marking the ones the answer contradicts.
func planOverrule(signals []PlanSignal, contradicts ...string) []PlanSignal {
	out := make([]PlanSignal, len(signals))
	for index, signal := range signals {
		out[index] = signal
		if slices.Contains(contradicts, signal.Detector) {
			out[index].Note = "overruled: " + signal.Note
		}
	}
	return out
}

// planStoreWord reads the store word the owned signals agree on; more than one
// word, or none, leaves the choice open and returns the candidates.
func planStoreWord(signals []PlanSignal, heapAllowed bool) (string, []string) {
	words := map[string]bool{}
	for _, signal := range signals {
		switch signal.Detector {
		case "T2":
			switch signal.Pattern {
			case "P2":
				words["fifoStore"] = true
			case "P3":
				words["lifoStore"] = true
			}
		case "T3":
			words["hashStore"] = true
		case "T4":
			words["btreeStore"] = true
		case "T5":
			switch signal.Pattern {
			case "P2":
				words["fifoStore"] = true
			case "P3":
				words["lifoStore"] = true
			case "P6":
				if heapAllowed {
					words["heapStore"] = true
				}
			}
		}
	}
	found := planSortedNames(words)
	if len(found) == 1 {
		return found[0], nil
	}
	if len(found) == 0 {
		return "", planOrderWords
	}
	return "", found
}

// placeKinds applies spec section 6.4 (first match wins) and section 6.3
// (grades). answers may be nil. The model's kinds are the ones placed.
func placeKinds(model *PlanModel, signals []PlanSignal, answers planAnswers) []PlanKind {
	var shape []PlanSignal
	for _, signal := range signals {
		if strings.HasPrefix(signal.Detector, "S") {
			shape = append(shape, signal)
		}
	}
	owners := planOwners(model, shape)
	names := make([]string, 0, len(model.Kinds))
	byName := map[string]ModelKind{}
	for _, kind := range model.Kinds {
		names = append(names, kind.Name)
		byName[kind.Name] = kind
	}
	slices.Sort(names)

	folded := map[string]bool{} // placed as a store field of its owner
	both := map[string]bool{}   // also kept as a bucket
	for _, name := range names {
		if _, ok := owners[name]; !ok {
			continue
		}
		switch answers[name+".placement"] {
		case "bucket":
		case "both":
			folded[name], both[name] = true, true
		default:
			folded[name] = true
		}
	}
	depth := map[string]bool{}
	for _, name := range names {
		if folded[name] && folded[owners[name].To] {
			folded[name], both[name] = false, false
			depth[name] = true
		}
	}

	taken := map[string]bool{}
	bucketNames := map[string]string{}
	for _, name := range names {
		if folded[name] && !both[name] {
			continue
		}
		unique := planBucketName(name)
		for taken[unique] {
			unique += "2"
		}
		taken[unique] = true
		bucketNames[name] = unique
	}

	var kinds []PlanKind
	for _, name := range names {
		bucket, isBucket := bucketNames[name]
		if !isBucket {
			continue
		}
		kinds = append(kinds, buildPlanKind(buildPlanKindRequest{Kind: byName[name], Bucket: bucket, Signals: signals, Answers: answers, Flattened: depth[name], Answered: answers[name+".placement"] != ""}))
	}
	for index := range kinds {
		for _, name := range names {
			if !folded[name] || bucketNames[kinds[index].Sources[0]] == "" {
				continue
			}
			if owners[name].To != kinds[index].Sources[0] {
				continue
			}
			field, open := buildOwnedStoreField(byName[name], signals, answers)
			kinds[index].StoreFields = append(kinds[index].StoreFields, field)
			kinds[index].Open = append(kinds[index].Open, open...)
		}
		finishPlanKind(&kinds[index])
	}
	return kinds
}

// planColumnType maps a model type word onto a ring type word.
func planColumnType(word string) string {
	if slices.Contains(ringTypeWords(), word) {
		return word
	}
	return "any"
}

// buildPlanKind builds the bucket for one model kind: its scalar fields, its
// embedded lists and its large bytes as store fields. Owned kinds are added later.
// buildPlanKindRequest is what one kind's bucket is built from: the kind, the
// bucket it becomes, the signals and answers that decide its fields, and whether
// it sits inside another kind (Flattened) or the developer placed it (Answered).
type buildPlanKindRequest struct {
	Kind      ModelKind
	Bucket    string
	Signals   []PlanSignal
	Answers   planAnswers
	Flattened bool
	Answered  bool
}

func buildPlanKind(req buildPlanKindRequest) PlanKind {
	kind, bucket, signals, answers, flattened, answered := req.Kind, req.Bucket, req.Signals, req.Answers, req.Flattened, req.Answered
	own := planSignalsAbout(signals, kind.Name)
	planned := PlanKind{Name: bucket, Sources: []string{kind.Name}, Evidence: own}
	fieldSignals := func(field string) []PlanSignal {
		var found []PlanSignal
		for _, signal := range own {
			if signal.Field == field {
				found = append(found, signal)
			}
		}
		return found
	}
	for _, field := range kind.Fields {
		if field.Name == "_id" {
			continue
		}
		sigs := fieldSignals(field.Name)
		switch {
		case planHas(sigs, "S2"):
			planned.StoreFields = append(planned.StoreFields, embeddedStoreField(kind, field, sigs, answers, &planned))
		case planHas(sigs, "S4", "T7") && slices.ContainsFunc(sigs, func(s PlanSignal) bool { return s.Pattern == "P7" }):
			planned.StoreFields = append(planned.StoreFields, PlanStoreField{Name: planTitle(field.Name), Store: "blobStore", Source: kind.Name + "." + field.Name,
				Alternatives: []string{planBlobAlternative}, Grade: gradeOf(sigs, ""), Evidence: sigs})
		default:
			scalar := PlanField{Name: planTitle(field.Name), Type: planColumnType(field.Type), Source: field.Name}
			if slices.ContainsFunc(sigs, func(s PlanSignal) bool { return s.Detector == "S1" && s.Pattern == "P8" }) {
				scalar.Planned = "unique"
			}
			planned.Fields = append(planned.Fields, scalar)
		}
	}
	if flattened {
		planned.Evidence = append(planned.Evidence, PlanSignal{Detector: "S9", Kind: kind.Name, Note: kind.Name +
			" would sit inside a store field of a store field (level L3); L3 nesting is kept shallow by design, so it is its own bucket here"})
	}
	if answered {
		planned.Grade = GradeAnswered
		if answers[kind.Name+".placement"] == "bucket" {
			planned.Evidence = planOverrule(own, "S6", "T2", "T3", "T4", "T5")
		}
	}
	return planned
}

// embeddedStoreField builds the store field for a growing list inside the
// kind's own documents. The data alone cannot pick the order, so the choice
// stays open unless the answers file settles it.
func embeddedStoreField(kind ModelKind, field ModelField, sigs []PlanSignal, answers planAnswers, planned *PlanKind) PlanStoreField {
	choice := kind.Name + "." + field.Name + ".order"
	store := PlanStoreField{Name: planTitle(field.Name), Source: kind.Name + "." + field.Name, Evidence: sigs}
	allowed := []string{"fifoStore", "lifoStore"}
	if planHas(sigs, "S3") {
		allowed = append(allowed, "hashStore", "btreeStore")
	}
	if answer := answers[choice]; slices.Contains(allowed, answer) {
		store.Store, store.Grade = answer, GradeAnswered
		return store
	}
	store.Orderless, store.Alternatives, store.Grade = true, allowed, GradeShape
	planned.Open = append(planned.Open, choice)
	return store
}

// buildOwnedStoreField builds the store field that a folded kind becomes on its
// owner, and the choices still open about it.
func buildOwnedStoreField(kind ModelKind, signals []PlanSignal, answers planAnswers) (PlanStoreField, []string) {
	own := planSignalsAbout(signals, kind.Name)
	placement, order := answers[kind.Name+".placement"], answers[kind.Name+".order"]
	field := PlanStoreField{Name: planTitle(kind.Name), Source: kind.Name, Evidence: own}
	word, candidates := planStoreWord(own, true)
	var open []string
	if slices.Contains(planOrderWords, order) {
		word, candidates = order, nil
	}
	field.Store = word
	if word == "" {
		field.Orderless, field.Alternatives = true, candidates
		if answers[kind.Name+".order"] == "" {
			open = append(open, kind.Name+".order")
		}
	}
	answer := placement
	if answer == "" {
		answer = order
	}
	field.Grade = gradeOf(own, answer)
	if placement != "" {
		field.Evidence = planOverrule(own, "T6", "T1")
	}
	if field.Grade == GradeConflict {
		field.Placements = []string{"bucket " + planBucketName(kind.Name), "both: " + field.Name + " and a " + planBucketName(kind.Name) + " bucket written twice"}
		open = append(open, kind.Name+".placement")
	}
	switch word {
	case "heapStore":
		field.Alternatives = append(field.Alternatives, planHeapAlternative)
	}
	return field, open
}

// finishPlanKind sets the bucket's grade from its own signals and its store fields.
func finishPlanKind(kind *PlanKind) {
	if kind.Grade == GradeAnswered {
		return
	}
	grade := gradeOf(kind.Evidence, "")
	if grade == GradeConflict {
		grade = GradeTrace
	}
	for _, store := range kind.StoreFields {
		switch {
		case store.Grade == GradeConflict:
			grade = GradeConflict
		case store.Grade == GradeAnswered && grade != GradeConflict:
			grade = GradeAnswered
		case store.Grade == GradeTrace && grade == GradeShape:
			grade = GradeTrace
		}
		if grade == GradeConflict {
			break
		}
	}
	kind.Grade = grade
	slices.Sort(kind.Open)
}
