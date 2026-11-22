package cli

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func blogQuestions(t *testing.T) []planQuestion {
	t.Helper()
	model := blogWithTrace(t, true)
	th := DefaultPlanThresholds()
	signals := detectAll(model, th)
	return planQuestions(placeKinds(model, signals, nil), signals)
}

// TestPlanQuestionConflictHasThreeOptions is spec Q6: the conflict offers
// store-field, bucket and both, in that order.
func TestPlanQuestionConflictHasThreeOptions(t *testing.T) {
	var found *planQuestion
	questions := blogQuestions(t)
	for index := range questions {
		if questions[index].Choice == "comments.placement" {
			found = &questions[index]
		}
	}
	if found == nil {
		t.Fatalf("no comments.placement question in %+v", questions)
	}
	var answers []string
	for _, option := range found.Options {
		answers = append(answers, option.Answer)
	}
	if strings.Join(answers, ",") != "store-field,bucket,both" {
		t.Fatalf("options %v", answers)
	}
	if !strings.Contains(found.Options[2].Cost, "two writes") {
		t.Fatalf("option 3 cost %q does not name the double write", found.Options[2].Cost)
	}
	if !strings.Contains(found.Trace, "author_id") {
		t.Fatalf("trace evidence %q does not name the cross-owner read", found.Trace)
	}
}

// TestPlanNoQuestionWhenTheEvidenceAnswers: a trace-graded store field asks nothing.
func TestPlanNoQuestionWhenTheEvidenceAnswers(t *testing.T) {
	model := blogWithTrace(t, false)
	th := DefaultPlanThresholds()
	signals := detectAll(model, th)
	if got := planQuestions(placeKinds(model, signals, nil), signals); len(got) != 0 {
		t.Fatalf("questions %+v, want none", got)
	}
}

func twoQuestions() []planQuestion {
	options := []planOption{{"a", "first", "x"}, {"b", "second", "y"}}
	return []planQuestion{{Choice: "one", Title: "one?", Options: options}, {Choice: "two", Title: "two?", Options: options}}
}

// TestPlanAskReadsNumberSkipAndQuit covers the three inputs.
func TestPlanAskReadsNumberSkipAndQuit(t *testing.T) {
	var out bytes.Buffer
	answers, err := planAsk(bufio.NewReader(strings.NewReader("2\ns\n")), &out, twoQuestions())
	if err != nil || answers["one"] != "b" || len(answers) != 1 {
		t.Fatalf("2 then s: %v, %v", answers, err)
	}
	answers, err = planAsk(bufio.NewReader(strings.NewReader("q\n")), &out, twoQuestions())
	if err != nil || len(answers) != 0 {
		t.Fatalf("q: %v, %v", answers, err)
	}
	answers, err = planAsk(bufio.NewReader(strings.NewReader("9\n1\n1\n")), &out, twoQuestions())
	if err != nil || answers["one"] != "a" || answers["two"] != "a" {
		t.Fatalf("a bad number is asked again: %v, %v", answers, err)
	}
	if !strings.Contains(out.String(), "[1/2/s/q]") {
		t.Fatalf("prompt missing:\n%s", out.String())
	}
}

// TestPlanAnswersRefuseUnknownChoice: the message names the choice and lists the real ones.
func TestPlanAnswersRefuseUnknownChoice(t *testing.T) {
	_, err := LoadPlanAnswers(strings.NewReader(`{"choice":"posts.placement","answer":"bucket"}`+"\n"), []string{"comments.order", "comments.placement"})
	if err == nil {
		t.Fatal("an unknown choice was accepted")
	}
	for _, want := range []string{"posts.placement", "comments.order, comments.placement"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
}

// TestPlanAnswersRoundTrip: saved answers load back, sorted by choice.
func TestPlanAnswersRoundTrip(t *testing.T) {
	var buffer bytes.Buffer
	if err := SavePlanAnswers(&buffer, planAnswers{"b.order": "fifoStore", "a.placement": "bucket"}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(buffer.String(), `{"choice":"a.placement"`) {
		t.Fatalf("not sorted: %s", buffer.String())
	}
	got, err := LoadPlanAnswers(&buffer, []string{"a.placement", "b.order"})
	if err != nil || got["b.order"] != "fifoStore" || len(got) != 2 {
		t.Fatalf("%v, %v", got, err)
	}
}

// TestPlanAnswersRefuseADuplicateChoice (m4): the second line does not silently win.
func TestPlanAnswersRefuseADuplicateChoice(t *testing.T) {
	src := `{"choice":"a","answer":"x"}` + "\n" + `{"choice":"a","answer":"y"}` + "\n"
	_, err := LoadPlanAnswers(strings.NewReader(src), []string{"a"})
	if err == nil || !strings.Contains(err.Error(), "line 2") || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("error %v does not name both lines", err)
	}
}
