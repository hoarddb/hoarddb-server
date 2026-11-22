package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// planQuestion is one open choice; Options are numbered from 1 in this order.
type planQuestion struct {
	Choice, Title, Data, Trace string
	Options                    []planOption
}

type planOption struct{ Answer, Label, Cost string }

// planOrderCosts says, one line each, what a store word buys for an owned list.
var planOrderCosts = map[string]string{
	"fifoStore":  "oldest first; consume from the front",
	"lifoStore":  "newest first; the latest is one peek",
	"hashStore":  "one element by its own key",
	"btreeStore": "elements in key order, or a key range",
	"heapStore":  "most important first; read only today",
}

// planQuestions returns one question per open choice, never more than one per
// choice and none whose answer the evidence already gives.
func planQuestions(kinds []PlanKind, signals []PlanSignal) []planQuestion {
	var questions []planQuestion
	for _, kind := range kinds {
		for _, choice := range kind.Open {
			for _, store := range kind.StoreFields {
				switch {
				case choice == store.Source+".placement" && store.Grade == GradeConflict:
					questions = append(questions, planPlacementQuestion(choice, kind, store, signals))
				case choice == store.Source+".order":
					questions = append(questions, planOrderQuestion(choice, kind, store, signals))
				}
			}
		}
	}
	return questions
}

func planEvidenceText(signals []PlanSignal, kind string, detectors ...string) string {
	var notes []string
	for _, signal := range signals {
		if signal.Kind == kind && slices.Contains(detectors, signal.Detector) {
			notes = append(notes, signal.Note)
		}
	}
	return strings.Join(notes, "; ")
}

func planPlacementQuestion(choice string, owner PlanKind, store PlanStoreField, signals []PlanSignal) planQuestion {
	bucket := planBucketName(store.Source)
	word := store.Store
	if word == "" {
		word = "store"
	}
	return planQuestion{
		Choice: choice,
		Title:  fmt.Sprintf("%s: store field of %s, or its own bucket?", store.Source, owner.Name),
		Data:   planEvidenceText(signals, store.Source, "S6"),
		Trace:  planEvidenceText(signals, store.Source, "T1", "T2", "T3", "T4", "T5", "T6"),
		Options: []planOption{
			{"store-field", fmt.Sprintf("%s %s on %s", store.Name, word, owner.Name), "one seek per owner; a read of " + store.Source + " across owners is not possible"},
			{"bucket", "bucket " + bucket, "both reads work; each owner's list is a scan: indexes are not available yet"},
			{"both", fmt.Sprintf("%s %s on %s, and a %s bucket the application writes twice", store.Name, word, owner.Name, bucket), "two writes per element; the application keeps them consistent"},
		},
	}
}

func planOrderQuestion(choice string, owner PlanKind, store PlanStoreField, signals []PlanSignal) planQuestion {
	question := planQuestion{
		Choice: choice,
		Title:  fmt.Sprintf("%s on %s: which store?", store.Name, owner.Name),
		Data:   planEvidenceText(signals, store.Source, "S2a", "S3", "S6"),
		Trace:  planEvidenceText(signals, store.Source, "T2", "T3", "T4", "T5"),
	}
	for _, word := range store.Alternatives {
		question.Options = append(question.Options, planOption{word, fmt.Sprintf("%s %s", store.Name, word), planOrderCosts[word]})
	}
	return question
}

// planAsk prints each question and reads one line per question: a number, "s"
// to leave it open, or "q" to stop asking. A bad line is asked again; the end
// of input leaves everything remaining open.
func planAsk(in *bufio.Reader, out io.Writer, questions []planQuestion) (planAnswers, error) {
	answers := planAnswers{}
	for _, question := range questions {
		if err := planPrint(out, question); err != nil {
			return nil, fmt.Errorf("print a question: %w", err)
		}
		for {
			line, err := in.ReadString('\n')
			text := strings.TrimSpace(line)
			if text == "" && err != nil {
				if errors.Is(err, io.EOF) {
					return answers, nil
				}
				return nil, fmt.Errorf("read an answer to %s: %w", question.Choice, err)
			}
			if text == "q" {
				return answers, nil
			}
			if text == "s" {
				break
			}
			if number, convErr := strconv.Atoi(text); convErr == nil && number >= 1 && number <= len(question.Options) {
				answers[question.Choice] = question.Options[number-1].Answer
				break
			}
			if _, err := fmt.Fprintf(out, "Type a number from 1 to %d, s to skip or q to stop: ", len(question.Options)); err != nil {
				return nil, fmt.Errorf("write the prompt: %w", err)
			}
		}
	}
	return answers, nil
}

func planPrint(out io.Writer, question planQuestion) error {
	var text strings.Builder
	fmt.Fprintf(&text, "\n%s\n", question.Title)
	if question.Data != "" {
		fmt.Fprintf(&text, "  The data says: %s\n", question.Data)
	}
	if question.Trace != "" {
		fmt.Fprintf(&text, "  The trace says: %s\n", question.Trace)
	}
	choices := make([]string, len(question.Options))
	for index, option := range question.Options {
		fmt.Fprintf(&text, "  %d) %s  - %s\n", index+1, option.Label, option.Cost)
		choices[index] = strconv.Itoa(index + 1)
	}
	fmt.Fprintf(&text, "  [%s/s/q]: ", strings.Join(choices, "/"))
	if _, err := io.WriteString(out, text.String()); err != nil {
		return fmt.Errorf("write question %s: %w", question.Choice, err)
	}
	return nil
}

type planAnswerLine struct {
	Choice string `json:"choice"`
	Answer string `json:"answer"`
}

// LoadPlanAnswers reads the JSON Lines answers file of spec section 4.3. A line
// that names a choice this run does not have is refused with the real choices,
// and so is a choice named twice. A nil choices list skips the membership check.
func LoadPlanAnswers(src io.Reader, choices []string) (planAnswers, error) {
	answers := planAnswers{}
	firstSeen := map[string]int{}
	scanner := bufio.NewScanner(src)
	for number := 1; scanner.Scan(); number++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.DisallowUnknownFields()
		var entry planAnswerLine
		if err := decoder.Decode(&entry); err != nil || entry.Choice == "" || entry.Answer == "" {
			return nil, fmt.Errorf("answers line %d: each line is {\"choice\": \"...\", \"answer\": \"...\"}. Fix the line, or delete it", number)
		}
		if earlier, dup := firstSeen[entry.Choice]; dup {
			return nil, fmt.Errorf("answers line %d: %q is already answered on line %d. Delete one of the two lines", number, entry.Choice, earlier)
		}
		firstSeen[entry.Choice] = number
		if choices != nil && !slices.Contains(choices, entry.Choice) {
			return nil, fmt.Errorf("answers line %d: %q is not a choice in this plan (choices: %s). Fix the name, or delete the line", number, entry.Choice, strings.Join(choices, ", "))
		}
		answers[entry.Choice] = entry.Answer
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read answers: %w", err)
	}
	return answers, nil
}

// SavePlanAnswers writes the answers one JSON object per line, sorted by choice.
func SavePlanAnswers(dst io.Writer, answers planAnswers) error {
	var buffer bytes.Buffer
	for _, choice := range planSortedNames(answers) {
		encoded, err := json.Marshal(planAnswerLine{choice, answers[choice]})
		if err != nil {
			return fmt.Errorf("encode answer %s: %w", choice, err)
		}
		buffer.Write(encoded)
		buffer.WriteByte('\n')
	}
	if _, err := dst.Write(buffer.Bytes()); err != nil {
		return fmt.Errorf("write answers: %w", err)
	}
	return nil
}
