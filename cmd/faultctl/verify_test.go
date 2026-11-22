package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustLoad(t *testing.T, name string) []attempt {
	t.Helper()
	rows, err := loadAttempts("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func verdictNamed(t *testing.T, verdicts []verdict, name string) verdict {
	t.Helper()
	for _, found := range verdicts {
		if found.Name == name {
			return found
		}
	}
	t.Fatalf("no verdict named %q in %+v", name, verdicts)
	return verdict{}
}

func TestVerifyPassesTheGoldenRun(t *testing.T) {
	verdicts := verify(mustLoad(t, "acks_ok.csv"), mustLoad(t, "obs_ok.csv"), verifyOptions{RF: 1, MaxRefused: 50})
	if len(verdicts) != 5 {
		t.Fatalf("want one verdict per invariant I1..I5, got %+v", verdicts)
	}
	for _, found := range verdicts {
		if !found.OK {
			t.Errorf("%s failed on the golden run: %s", found.Name, found.Detail)
		}
	}
}

// Break: make the I1 loop skip ids whose last conclusive read is NOT_FOUND.
func TestVerifyFlagsAMissingAckedWrite(t *testing.T) {
	verdicts := verify(mustLoad(t, "acks_ok.csv"), mustLoad(t, "obs_missing.csv"), verifyOptions{RF: 1, MaxRefused: 50})
	found := verdictNamed(t, verdicts, "no-acked-write-lost")
	if found.OK || !strings.Contains(found.Detail, "a-2") {
		t.Fatalf("a-2 is acked and gone; want a failing no-acked-write-lost naming it, got %+v", found)
	}
}

// Break: print the declared set but still fail the verdict, or drop it from Excused.
func TestVerifyExcusesTheDeclaredLossSetAndPrintsIt(t *testing.T) {
	lost := map[string]string{"a-2": "minority side of a partition"}
	verdicts := verify(mustLoad(t, "acks_ok.csv"), mustLoad(t, "obs_missing.csv"), verifyOptions{RF: 1, MaxRefused: 50, Lost: lost})
	found := verdictNamed(t, verdicts, "no-acked-write-lost")
	if !found.OK {
		t.Fatalf("a-2 is declared lost; want OK, got %+v", found)
	}
	if len(found.Excused) != 1 || !strings.Contains(found.Excused[0], "a-2: acked applied/1 via 192.0.2.1:7433") ||
		!strings.Contains(found.Excused[0], "excused: minority side of a partition") {
		t.Fatalf("the excused id must be printed with its ack and reason, got %q", found.Excused)
	}
}

// Break: compare with > instead of treating NOT_FOUND after a served read as a regression.
func TestVerifyFlagsAStaleReadPerClient(t *testing.T) {
	verdicts := verify(mustLoad(t, "acks_ok.csv"), mustLoad(t, "obs_stale.csv"), verifyOptions{RF: 1, MaxRefused: 50})
	found := verdictNamed(t, verdicts, "reads-monotone-per-client")
	if found.OK || !strings.Contains(found.Detail, "a-1") {
		t.Fatalf("a-1 was served and then not found; want a failing reads-monotone-per-client naming it, got %+v", found)
	}
}

// Break: accept any non-empty err_code as named.
func TestVerifyFlagsAnUnnamedRefusal(t *testing.T) {
	verdicts := verify(mustLoad(t, "acks_unnamed.csv"), mustLoad(t, "obs_ok.csv"), verifyOptions{RF: 1, MaxRefused: 50})
	found := verdictNamed(t, verdicts, "refusals-are-named")
	if found.OK || !strings.Contains(found.Detail, "TIMEOUT") {
		t.Fatalf("a bare deadline from a live node is a violation, got %+v", found)
	}
	down := verifyOptions{RF: 1, MaxRefused: 50, Down: []window{{From: 3500, To: 4500}}}
	if excused := verdictNamed(t, verify(mustLoad(t, "acks_unnamed.csv"), mustLoad(t, "obs_ok.csv"), down), "refusals-are-named"); !excused.OK {
		t.Fatalf("the same deadline inside a declared down window is expected, got %+v", excused)
	}
}

func TestVerifyFlagsAVocabularyBreach(t *testing.T) {
	acks := mustLoad(t, "acks_ok.csv")
	acks[0].Copies = 0
	if found := verdictNamed(t, verify(acks, mustLoad(t, "obs_ok.csv"), verifyOptions{RF: 1, MaxRefused: 50}), "ack-vocabulary-sound"); found.OK {
		t.Fatalf("applied with zero copies must fail, got %+v", found)
	}
}

func TestVerifyFlagsTwoOwnersAckingOneIDAtOneEpoch(t *testing.T) {
	acks := mustLoad(t, "acks_ok.csv")
	twin := acks[0]
	twin.ServedBy = "n2"
	acks = append(acks, twin)
	if found := verdictNamed(t, verify(acks, mustLoad(t, "obs_ok.csv"), verifyOptions{RF: 1, MaxRefused: 50}), "one-tail-per-pair"); found.OK {
		t.Fatalf("two owners acking a-0 at epoch 1 must fail, got %+v", found)
	}
}

func TestVerifyRefusesAHandCorruptedCSV(t *testing.T) {
	if _, err := readAttempts(strings.NewReader("client,id\nonly,two\n")); err == nil {
		t.Fatal("a file without the faultctl header must be an error, not an empty run")
	}
	bad := strings.Join(csvHeader, ",") + "\na,a-0,zero,1,1,applied,1,n1,x,1000,\n"
	if _, err := readAttempts(strings.NewReader(bad)); err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("a non-numeric seq must name its line, got %v", err)
	}
}

func writeTemp(t *testing.T, name string, rows []attempt) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	var buf strings.Builder
	if err := writeAttempts(&buf, rows); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(buf.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Break: delete the "no attempted writes" refusal in runVerify.
func TestVerifyRefusesAHeaderOnlyRun(t *testing.T) {
	empty := writeTemp(t, "acks.csv", nil)
	var out strings.Builder
	err := runVerify([]string{"--acks", empty, "--obs", writeTemp(t, "obs.csv", nil)}, &out)
	if err == nil || !strings.Contains(err.Error(), empty) {
		t.Fatalf("a header-only acks file is not a clean run; want an error naming %s, got %v (output %q)", empty, err, out.String())
	}
}

// Break: delete the "no acked writes and no --lost" refusal in runVerify.
func TestVerifyRefusesARunWithNothingAcked(t *testing.T) {
	refusedOnly := writeTemp(t, "acks.csv", []attempt{{Client: "a", ID: "a-0", Seq: 0, Via: "192.0.2.1:7433", AtNs: 1000, ErrCode: "LEASE_LAPSED"}})
	obs := writeTemp(t, "obs.csv", nil)
	var out strings.Builder
	err := runVerify([]string{"--acks", refusedOnly, "--obs", obs}, &out)
	if err == nil || !strings.Contains(err.Error(), refusedOnly) {
		t.Fatalf("zero acked writes without --lost checked nothing; want an error naming %s, got %v", refusedOnly, err)
	}
}

// Break: restore `seen.Seq >= 0` as the only test in checkNoAckedWriteLost.
func TestVerifyFlagsAReadBelowTheAckedSeq(t *testing.T) {
	obs := mustLoad(t, "obs_ok.csv")
	obs[2].Seq = 0 // a-2 was acked at seq 2 and is read back as seq 0
	found := verdictNamed(t, verify(mustLoad(t, "acks_ok.csv"), obs, verifyOptions{RF: 1, MaxRefused: 50}), "no-acked-write-lost")
	if found.OK || !strings.Contains(found.Detail, "a-2") || !strings.Contains(found.Detail, "below") {
		t.Fatalf("a-2 read back at seq 0 after an ack of seq 2 is a lost write; want a failure naming it, got %+v", found)
	}
}

// Break: make isAcked accept only "applied".
func TestVerifyChecksEveryRecordedAckNotOnlyApplied(t *testing.T) {
	for _, outcome := range []string{"applied_degraded", "unknown"} {
		acks := mustLoad(t, "acks_ok.csv")
		acks[2].Outcome = outcome
		found := verdictNamed(t, verify(acks, mustLoad(t, "obs_missing.csv"), verifyOptions{RF: 3, MaxRefused: 50}), "no-acked-write-lost")
		if found.OK || !strings.Contains(found.Detail, "a-2") {
			t.Errorf("a %s ack that is not readable is a lost write like H1's ledger says; got %+v", outcome, found)
		}
	}
}

// Break: change "read back as not found" to the "never read back" wording for a seq of -1.
func TestVerifyNamesAnIdReadBackAsNotFound(t *testing.T) {
	found := verdictNamed(t, verify(mustLoad(t, "acks_ok.csv"), mustLoad(t, "obs_missing.csv"), verifyOptions{RF: 1, MaxRefused: 50}), "no-acked-write-lost")
	if !strings.Contains(found.Detail, "a-2 (read back as not found)") {
		t.Fatalf("a-2 was read and not found; want that said, got %q", found.Detail)
	}
}

// Break: drop the code test in checkRefusalsAreNamed so any code inside a down window is excused.
func TestVerifyExcusesOnlyTimeoutAndConnInsideADownWindow(t *testing.T) {
	down := verifyOptions{RF: 1, MaxRefused: 50, Down: []window{{From: 3500, To: 4500}}}
	for code, wantOK := range map[string]bool{"TIMEOUT": true, "CONN": true, "INTERNAL": false} {
		acks := mustLoad(t, "acks_unnamed.csv")
		acks[3].ErrCode = code
		found := verdictNamed(t, verify(acks, mustLoad(t, "obs_ok.csv"), down), "refusals-are-named")
		if found.OK != wantOK {
			t.Errorf("%s inside a down window: want OK=%v, got %+v", code, wantOK, found)
		}
	}
}

func vocabulary(t *testing.T, rows ...attempt) verdict {
	t.Helper()
	return verdictNamed(t, verify(rows, mustLoad(t, "obs_ok.csv"), verifyOptions{RF: 3, MaxRefused: 50}), "ack-vocabulary-sound")
}

// Break: key the unknown and applied sets by id alone, as before.
func TestVerifyPairsUnknownWithAppliedPerRequest(t *testing.T) {
	unknown := attempt{Client: "a", ID: "a-0", Seq: 0, Outcome: "unknown", Copies: 0, Via: "192.0.2.1:7433", AtNs: 1000}
	applied := attempt{Client: "a", ID: "a-0", Seq: 0, Outcome: "applied", Copies: 2, Via: "192.0.2.1:7433", AtNs: 2000}
	if found := vocabulary(t, unknown, applied); found.OK || !strings.Contains(found.Detail, "a-0") {
		t.Fatalf("one request answered unknown and then applied must fail, got %+v", found)
	}
	otherRequest := applied
	otherRequest.Seq = 1
	if found := vocabulary(t, unknown, otherRequest); !found.OK {
		t.Fatalf("an unknown and an applied of different requests on one id are not a contradiction, got %+v", found)
	}
}

// Break: restore `case "":` with an empty body.
func TestVerifyFlagsARowThatIsNeitherAckedNorRefused(t *testing.T) {
	blank := attempt{Client: "a", ID: "a-0", Seq: 0, Via: "192.0.2.1:7433", AtNs: 1000}
	if found := vocabulary(t, blank); found.OK || !strings.Contains(found.Detail, "a-0") {
		t.Fatalf("an empty outcome with an empty err_code answered nothing; want a violation, got %+v", found)
	}
	refused := blank
	refused.ErrCode = "LEASE_LAPSED"
	if found := vocabulary(t, refused); !found.OK {
		t.Fatalf("a refusal (empty outcome, a code) is not an ack and not a breach, got %+v", found)
	}
}

func refusals(client string, count int) []attempt {
	rows := make([]attempt, count)
	for index := range rows {
		rows[index] = attempt{Client: client, ID: client + "-x", Seq: int64(index), Via: "192.0.2.1:7433", AtNs: int64(10000 + index), ErrCode: "LEASE_LAPSED"}
	}
	return rows
}

func refusedVerdict(t *testing.T, rows []attempt, maxRefused int) verdict {
	t.Helper()
	return verdictNamed(t, verify(append(mustLoad(t, "acks_ok.csv"), rows...), mustLoad(t, "obs_ok.csv"), verifyOptions{RF: 1, MaxRefused: maxRefused}), "refusals-are-named")
}

// Break: compare with `>= opts.MaxRefused` (3 in a row fails) or `> opts.MaxRefused+1` (4 in a row passes).
func TestVerifyFlagsMoreThanMaxRefusedInARow(t *testing.T) {
	if found := refusedVerdict(t, refusals("c", 3), 3); !found.OK {
		t.Fatalf("exactly --max-refused refusals in a row is allowed, got %+v", found)
	}
	if found := refusedVerdict(t, refusals("c", 4), 3); found.OK || !strings.Contains(found.Detail, "client c was refused more than 3 times in a row") {
		t.Fatalf("one more than --max-refused in a row must fail naming the client, got %+v", found)
	}
}

// --max-refused 0 means no bound (a deliberately full disk refuses forever).
// Break: drop the `opts.MaxRefused > 0 &&` guard.
func TestVerifyMaxRefusedZeroMeansUnbounded(t *testing.T) {
	if found := refusedVerdict(t, refusals("c", 500), 0); !found.OK {
		t.Fatalf("--max-refused 0 is unbounded; 500 named refusals in a row must pass, got %+v", found)
	}
}

// Break: key the I2 stream by id alone instead of (client, id).
func TestVerifyReadsAreMonotonePerClientNotPerID(t *testing.T) {
	obs := []attempt{
		{Client: "a", ID: "a-0", Seq: 2, Via: "192.0.2.1:7433", AtNs: 5000},
		{Client: "b", ID: "a-0", Seq: 1, Via: "192.0.2.1:7433", AtNs: 6000}, // another client's older view is not a regression
		{Client: "a", ID: "a-0", Seq: 2, Via: "192.0.2.1:7433", AtNs: 7000},
	}
	if found := verdictNamed(t, verify(mustLoad(t, "acks_ok.csv"), obs, verifyOptions{RF: 1, MaxRefused: 50}), "reads-monotone-per-client"); !found.OK {
		t.Fatalf("two clients reading one id at different seqs is not a per-client regression, got %+v", found)
	}
	obs[2].Seq = 0
	if found := verdictNamed(t, verify(mustLoad(t, "acks_ok.csv"), obs, verifyOptions{RF: 1, MaxRefused: 50}), "reads-monotone-per-client"); found.OK {
		t.Fatalf("client a going 2 -> 0 on a-0 must still fail, got %+v", found)
	}
}

// Break: return OK from checkNoAckedWriteLost when obs is empty.
func TestVerifyFlagsAcksWithAHeaderOnlyReadFile(t *testing.T) {
	found := verdictNamed(t, verify(mustLoad(t, "acks_ok.csv"), nil, verifyOptions{RF: 1, MaxRefused: 50}), "no-acked-write-lost")
	if found.OK || !strings.Contains(found.Detail, "never read back") {
		t.Fatalf("acked writes and no reads at all are not clean, got %+v", found)
	}
}
