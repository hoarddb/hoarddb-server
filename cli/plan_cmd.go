package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// planSynopsis is printed, with the example, when `hoarddb plan` has nothing to read.
const planSynopsis = `usage: hoarddb plan <file-or-dir>... [--trace <file>] [--answers <file>] [--save-answers <file>]
                   [--sample <n>|all] [--out <file>|-] [--json] [--no-input] [--force]
       hoarddb plan <model.json> [--trace <file>] ...
       hoarddb plan --thresholds
       hoarddb plan guide | schema           (rules and model schema, for an AI agent)

hoarddb plan reads a sample of your data, or a model file, and prints a design
you read, edit and run yourself. It connects to nothing and creates nothing.

Examples:
  hoarddb plan dump/ --trace app.trace.jsonl --out -
  hoarddb plan users.bson posts.bson comments.bson --no-input
  hoarddb plan model.json --json
`

// planEnv is the streams and the terminal fact, so a test needs no process.
type planEnv struct {
	stdin           io.Reader
	stdout, stderr  io.Writer
	stdinIsTerminal bool
}

// RunPlanCommand implements `hoarddb plan`. Like the ingest verb it uses the
// process streams; runPlan is what tests drive.
func RunPlanCommand(args []string) int {
	return runPlan(args, planEnv{stdin: os.Stdin, stdout: os.Stdout, stderr: os.Stderr, stdinIsTerminal: isTerminalFile(os.Stdin)})
}

// planOptions are the parsed flags and positionals.
type planOptions struct {
	paths                                []string
	trace, answers, saveAnswers, out     string
	sample, maxBuckets                   int
	asJSON, noInput, force, showThreshes bool
}

// planRefusal is an error that carries its own fix sentence for --json.
type planRefusal struct{ message, fix string }

func (r *planRefusal) Error() string { return r.message }

func refuse(message, fix string) error { return &planRefusal{message: message, fix: fix} }

// runPlan returns ExitOK on a printed plan and ExitUsage for every refusal.
func runPlan(args []string, env planEnv) int {
	if len(args) == 0 {
		say(env.stderr, "%s", planSynopsis)
		return ExitUsage
	}
	if len(args) == 1 && planIsAgentWord(args[0]) {
		if args[0] == "guide" {
			say(env.stdout, "%s", renderGuide())
		} else {
			say(env.stdout, "%s", planSchemaJSON())
		}
		return ExitOK
	}
	options, err := parsePlanFlags(args, env)
	if errors.Is(err, errPlanHelp) {
		say(env.stdout, "%s", planSynopsis)
		say(env.stdout, "\nFlags:\n")
		planFlagSet(&planOptions{}, new(string)).VisitAll(func(f *flag.Flag) {
			say(env.stdout, "  --%s  %s (default %q)\n", f.Name, f.Usage, f.DefValue)
		})
		return ExitOK
	}
	asJSON := options.asJSON
	if err == nil {
		err = executePlan(options, env)
	} else {
		asJSON = planArgsAskJSON(args)
	}
	if err == nil {
		return ExitOK
	}
	reportPlanRefusal(err, asJSON, env)
	return ExitUsage
}

// errPlanHelp is -h or --help: not a refusal.
var errPlanHelp = errors.New("help requested")

// planArgsAskJSON says whether a command line that failed to parse asked for
// JSON, in any of the spellings the flag package accepts.
func planArgsAskJSON(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "--json", "-json", "--json=true", "-json=true", "--json=1", "-json=1":
			return true
		}
	}
	return false
}

// reportPlanRefusal prints the refusal once on stderr; with --json the only
// stdout is {"error":{"message","fix"}}.
func reportPlanRefusal(err error, asJSON bool, env planEnv) {
	message, fix := err.Error(), ""
	var refusal *planRefusal
	if errors.As(err, &refusal) {
		message, fix = refusal.message, refusal.fix
	}
	var usage *ingestUsageError
	if errors.As(err, &usage) {
		message = usage.Message
	}
	shown := message
	if fix != "" {
		shown = strings.TrimRight(message, ".") + ". " + strings.TrimRight(fix, ".") + "."
	}
	say(env.stderr, "%s\n", shown)
	if asJSON {
		payload := map[string]map[string]string{"error": {"message": message, "fix": fix}}
		encoded, marshalErr := json.Marshal(payload)
		if marshalErr == nil {
			say(env.stdout, "%s\n", encoded)
		}
	}
}

// parsePlanFlags reads flags and positionals in any order, the way the spec's
// grammar shows them.
func parsePlanFlags(args []string, env planEnv) (planOptions, error) {
	for _, arg := range args {
		if arg == "--from" || strings.HasPrefix(arg, "--from=") || arg == "--address" {
			return planOptions{}, refuse("hoarddb plan reads files only; "+arg+" is not available yet", "Pass the dump files or a model file instead, as in 'hoarddb plan dump/ --no-input'")
		}
	}
	var options planOptions
	var sample string
	fs := planFlagSet(&options, &sample)
	fs.SetOutput(io.Discard)
	rest := args
	for len(rest) > 0 {
		if err := fs.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return options, errPlanHelp
			}
			return options, refuse("hoarddb plan: "+err.Error(), "Run 'hoarddb plan' with no arguments to see the flags")
		}
		rest = fs.Args()
		if len(rest) > 0 {
			options.paths = append(options.paths, rest[0])
			rest = rest[1:]
		}
	}
	if sample == "all" {
		options.sample = 0
	} else {
		count, err := strconv.Atoi(sample)
		if err != nil || count < 1 {
			return options, refuse(fmt.Sprintf("--sample %q is not a number", sample), "Pass --sample 10000, or --sample all")
		}
		options.sample = count
	}
	return options, nil
}

// planFlagSet declares the flags once, for parsing and for -h.
func planFlagSet(options *planOptions, sample *string) *flag.FlagSet {
	fs := flag.NewFlagSet("hoarddb plan", flag.ContinueOnError)
	fs.StringVar(&options.trace, "trace", "", "access trace, JSON Lines")
	fs.StringVar(&options.answers, "answers", "", "answers file, JSON Lines")
	fs.StringVar(&options.saveAnswers, "save-answers", "", "write the answers given to this file")
	fs.StringVar(sample, "sample", "10000", "documents read per file, or all")
	fs.StringVar(&options.out, "out", "hoarddb-plan.md", "plan file, or - for stdout")
	fs.BoolVar(&options.asJSON, "json", false, "write the plan as JSON on stdout")
	fs.BoolVar(&options.noInput, "no-input", false, "ask nothing; leave every choice open")
	fs.BoolVar(&options.force, "force", false, "overwrite an --out or --save-answers file the advisor did not write")
	fs.BoolVar(&options.showThreshes, "thresholds", false, "print the thresholds and exit")
	fs.IntVar(&options.maxBuckets, "max-buckets", defaultMaxBuckets, "most files read from one directory")
	return fs
}

// planModelFromInputs builds the model from a model file or from dump files, and
// attaches the trace.
func planModelFromInputs(options planOptions, env planEnv) (*PlanModel, error) {
	var model *PlanModel
	if len(options.paths) == 1 && strings.HasSuffix(options.paths[0], ".json") {
		content, err := os.ReadFile(options.paths[0])
		if err != nil {
			return nil, refuse(fmt.Sprintf("cannot read %s: %v", options.paths[0], describeFSError(err)), "Check the path")
		}
		if planLooksLikeModel(content) {
			loaded, err := LoadPlanModel(bytes.NewReader(content))
			if err != nil {
				return nil, err
			}
			if err := ValidatePlanModel(loaded); err != nil {
				return nil, err
			}
			model = loaded
		}
	}
	if model == nil {
		collected, notes, err := planCollect(options.paths, options.sample, options.maxBuckets)
		if err != nil {
			return nil, err
		}
		for _, note := range notes {
			say(env.stderr, "%s\n", note)
		}
		model = collected
	}
	if options.trace != "" {
		file, err := os.Open(options.trace)
		if err != nil {
			return nil, refuse(fmt.Sprintf("cannot open the trace %s: %v", options.trace, describeFSError(err)), "Check the path, or leave out --trace")
		}
		defer closeQuietly(file)
		names := make([]string, len(model.Kinds))
		for index, kind := range model.Kinds {
			names[index] = kind.Name
		}
		lines, err := LoadTrace(file, names)
		if err != nil {
			return nil, err
		}
		model.Trace = lines
	}
	return model, nil
}

func executePlan(options planOptions, env planEnv) error {
	th := DefaultPlanThresholds()
	if options.showThreshes {
		return WriteThresholds(env.stdout, th)
	}
	if len(options.paths) == 0 {
		return refuse("hoarddb plan needs a file, a directory or a model file to read", "Run 'hoarddb plan dump/ --no-input', or 'hoarddb plan' for the usage")
	}
	model, err := planModelFromInputs(options, env)
	if err != nil {
		return fmt.Errorf("read the plan inputs: %w", err)
	}
	signals := detectAll(model, th)
	first := placeKinds(model, signals, nil)
	var choices []string
	for _, kind := range first {
		choices = append(choices, kind.Open...)
	}
	answers := planAnswers{}
	if options.answers != "" {
		answers, err = planLoadAnswersFile(options.answers, choices)
		if err != nil {
			return fmt.Errorf("load the answers: %w", err)
		}
	}
	if !options.noInput && env.stdinIsTerminal && !options.asJSON {
		asked, err := planAsk(bufio.NewReader(env.stdin), env.stderr, planQuestions(placeKinds(model, signals, answers), signals))
		if err != nil {
			return fmt.Errorf("ask the plan questions: %w", err)
		}
		for choice, answer := range asked {
			answers[choice] = answer
		}
	}
	if options.saveAnswers != "" {
		if err := planSaveAnswersFile(options.saveAnswers, answers, options.force); err != nil {
			return fmt.Errorf("save the answers: %w", err)
		}
	}
	kinds := placeKinds(model, signals, answers)
	plan := buildPlan(model, signals, kinds, buildScorecard(model, signals, th), th)
	return planWriteOutput(options, env, plan)
}

func planLoadAnswersFile(path string, choices []string) (planAnswers, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, refuse(fmt.Sprintf("cannot open the answers file %s: %v", path, describeFSError(err)), "Check the path, or leave out --answers")
	}
	defer closeQuietly(file)
	return LoadPlanAnswers(file, choices)
}

// planLooksLikeModel reports whether the first JSON value of a .json file has a
// "version" key, which no document of a JSON Lines dump is expected to carry as
// its first key set. A dump named users.json is therefore read as a dump.
func planLooksLikeModel(content []byte) bool {
	var first map[string]json.RawMessage
	if err := json.NewDecoder(bytes.NewReader(content)).Decode(&first); err != nil {
		return false
	}
	_, hasVersion := first["version"]
	_, hasKinds := first["kinds"]
	return hasVersion && hasKinds
}

func planSaveAnswersFile(path string, answers planAnswers, force bool) error {
	if existing, err := os.ReadFile(path); err == nil && len(bytes.TrimSpace(existing)) > 0 && !force {
		if _, parseErr := LoadPlanAnswers(bytes.NewReader(existing), nil); parseErr != nil {
			return refuse(fmt.Sprintf("%s exists and is not an answers file this command wrote, so it was left alone", path),
				"Pick another file with --save-answers, or overwrite it with --force")
		}
	}
	file, err := os.Create(path)
	if err != nil {
		return refuse(fmt.Sprintf("cannot write the answers file %s: %v", path, describeFSError(err)), "Pick a path you can write with --save-answers")
	}
	if writeErr := SavePlanAnswers(file, answers); writeErr != nil {
		// The write error is the one to report, but a close error on the same
		// file is joined, not dropped: it can be the first sign the bytes never
		// reached the disk.
		var closeErr error
		if err := file.Close(); err != nil {
			closeErr = fmt.Errorf("close %s: %w", path, err)
		}
		return errors.Join(fmt.Errorf("write the answers file %s: %w", path, writeErr), closeErr)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

// planWriteOutput prints JSON on stdout, or writes the Markdown plan to --out,
// refusing to replace a file the advisor did not write.
func planWriteOutput(options planOptions, env planEnv, plan Plan) error {
	if options.asJSON {
		rendered, err := renderJSON(plan)
		if err != nil {
			return err
		}
		if _, err := env.stdout.Write(rendered); err != nil {
			return fmt.Errorf("write the plan: %w", err)
		}
		return nil
	}
	text := renderMarkdown(plan)
	if options.out == "-" {
		_, err := io.WriteString(env.stdout, text)
		if err != nil {
			return fmt.Errorf("write the plan: %w", err)
		}
		return nil
	}
	if existing, err := os.ReadFile(options.out); err == nil && !options.force && !strings.HasPrefix(string(existing), planMarker) {
		return refuse(fmt.Sprintf("%s exists and is not a plan this command wrote, so it was left alone", options.out),
			"Pick another file with --out, print to the terminal with '--out -', or overwrite it with --force")
	}
	if err := os.WriteFile(options.out, []byte(text), 0o644); err != nil {
		return refuse(fmt.Sprintf("cannot write %s: %v", options.out, describeFSError(err)), "Pick a path you can write with --out, or use '--out -'")
	}
	say(env.stderr, "wrote %s: %d buckets, %s open; verdict: %s\n", options.out, len(plan.Kinds), planChoices(plan.OpenChoices), plan.Scorecard.Verdict)
	return nil
}

// closeQuietly closes a file opened for reading; a failed close of a read-only
// file loses nothing.
func closeQuietly(file *os.File) { _ = file.Close() }

// say writes chatter. A failed write to the terminal has nowhere better to be
// reported, so the error is dropped on purpose.
func say(out io.Writer, format string, args ...any) { _, _ = fmt.Fprintf(out, format, args...) }

func planChoices(count int) string {
	if count == 1 {
		return "1 choice"
	}
	return fmt.Sprintf("%d choices", count)
}
