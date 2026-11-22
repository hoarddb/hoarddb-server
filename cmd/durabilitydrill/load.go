package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// target is the node and bucket a subcommand talks to.
type target struct {
	addr        string
	database    string
	bucket      string
	user        string
	password    string
	fingerprint string
	insecure    bool
	timeout     time.Duration
}

// register adds the flags every subcommand that reaches a node shares.
func (t *target) register(flags *flag.FlagSet) {
	flags.StringVar(&t.addr, "addr", "", "node address host:port (required)")
	flags.StringVar(&t.database, "db", "drilldb", "database name")
	flags.StringVar(&t.bucket, "bucket", "Writes", "bucket name")
	flags.StringVar(&t.fingerprint, "fingerprint", "", "pin the node's certificate fingerprint (SHA256:...)")
	flags.BoolVar(&t.insecure, "insecure", false, "skip certificate verification (a node you started yourself on loopback)")
	flags.DurationVar(&t.timeout, "timeout", 5*time.Second, "deadline of one request")
}

func (t *target) validate(command string) error {
	t.user = envOr("DURABILITY_DRILL_USER", "admin")
	t.password = envOr("DURABILITY_DRILL_PASSWORD", os.Getenv("HOARDDB_ROOT_PASSWORD"))
	switch {
	case t.addr == "":
		return fmt.Errorf("--addr is required, for example: durabilitydrill %s --addr 127.0.0.1:7433 --insecure", command)
	case t.password == "":
		return errors.New("no password: export DURABILITY_DRILL_PASSWORD (or HOARDDB_ROOT_PASSWORD) before running")
	case !t.insecure && t.fingerprint == "":
		return errors.New("pass --fingerprint SHA256:... from the node's startup log, or --insecure for a node you started yourself on loopback")
	}
	return nil
}

func (t *target) dial(ctx context.Context) (*ringclient.Client, error) {
	dialCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	client, err := ringclient.Dial(dialCtx, ringclient.Options{
		Addr: t.addr, User: t.user, Password: t.password, Fingerprint: t.fingerprint, Insecure: t.insecure,
		DialTimeout: t.timeout, RequestTimeout: t.timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", t.addr, err)
	}
	return client, nil
}

// say prints one line of a subcommand's output; a line that could not be written is an error,
// because the wrapper script reads these lines.
func say(out io.Writer, format string, args ...any) error {
	if _, err := fmt.Fprintf(out, format, args...); err != nil {
		return fmt.Errorf("writing the output: %w", err)
	}
	return nil
}

func envOr(name, fallback string) string {
	if found := os.Getenv(name); found != "" {
		return found
	}
	return fallback
}

// errorCode names a failed request: the ring code of a refusal, TIMEOUT for a passed
// deadline, CONN for a connection that could not be made or was lost.
func errorCode(err error) string {
	var refusal *ringclient.Error
	var named *cluster.Error
	switch {
	case errors.As(err, &refusal):
		return refusal.Code.String()
	case errors.As(err, &named):
		return named.Code.String()
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return "TIMEOUT"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "TIMEOUT"
	}
	return "CONN"
}

// classifyWrite turns an Insert's answer into a ledger outcome. Only "applied" with no
// error is an acknowledgement; applied_degraded, unknown, a refusal, a deadline and a lost
// connection all leave the write's fate open, so they are recorded as unknown.
func classifyWrite(answer ringclient.WriteResult, err error) (outcome, detail string) {
	if err != nil {
		return outcomeUnknown, errorCode(err)
	}
	if answer.Applied() {
		return outcomeAcked, ""
	}
	return outcomeUnknown, "outcome=" + answer.Outcome
}

// classifyReadError says what a failed Get means: NOT_FOUND is a conclusive absent; anything
// else is no answer yet, worth another try until the read's deadline.
func classifyReadError(err error) (state string, retry bool) {
	if errors.Is(err, ringclient.ErrNotFound) {
		return stateAbsent, false
	}
	return stateUnreadable, true
}

// isRefusal reports whether err is an answer from a live server (the connection still works).
func isRefusal(err error) bool {
	var refusal *ringclient.Error
	return errors.As(err, &refusal)
}

// runReady waits until the node answers DescribeNode with Serving "ok".
func runReady(args []string, out io.Writer) error {
	var node target
	flags := flag.NewFlagSet("ready", flag.ContinueOnError)
	node.register(flags)
	wait := flags.Duration("wait", 120*time.Second, "how long to keep trying")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing ready flags: %w", err)
	}
	if err := node.validate("ready"); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	deadline := time.Now().Add(*wait)
	last := "no attempt was made"
	for time.Now().Before(deadline) && ctx.Err() == nil {
		serving, err := describeServing(ctx, &node)
		if err == nil && serving == "ok" {
			return say(out, "ready: %s is serving\n", node.addr)
		}
		if err != nil {
			last = err.Error()
		} else {
			last = "the node answered Serving: " + serving
		}
		select {
		case <-ctx.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
	return fmt.Errorf("%s was not serving within %s (last: %s). Read the server's log, then start it again on the same data directory", node.addr, *wait, last)
}

func describeServing(ctx context.Context, node *target) (string, error) {
	client, err := node.dial(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = client.Close() }() // one probe; the answer is already read
	requestCtx, cancel := context.WithTimeout(ctx, node.timeout)
	defer cancel()
	described, err := client.DescribeNode(requestCtx)
	if err != nil {
		return "", fmt.Errorf("describing the node: %w", err)
	}
	return described.Serving, nil
}

// createBucket makes the drill's database and bucket; both already existing is not an error.
func createBucket(ctx context.Context, node *target) error {
	client, err := node.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }() // the create answers were already read
	requestCtx, cancel := context.WithTimeout(ctx, 6*node.timeout)
	defer cancel()
	if _, err := client.CreateDatabase(requestCtx, node.database, 0); err != nil && !isConflict(err) {
		return fmt.Errorf("creating database %q: %w", node.database, err)
	}
	fields := []ringclient.FieldDecl{
		{Name: "Round", Type: "int"}, {Name: "Writer", Type: "int"}, {Name: "Seq", Type: "int"},
		{Name: "Check", Type: "string"}, {Name: "Pad", Type: "string"},
	}
	if _, err := client.CreateBucket(requestCtx, node.database, node.bucket, fields); err != nil && !isConflict(err) {
		return fmt.Errorf("creating bucket %q in %q: %w", node.bucket, node.database, err)
	}
	return nil
}

func isConflict(err error) bool {
	var refusal *ringclient.Error
	return errors.As(err, &refusal) && refusal.Code == cluster.CodeConflict
}

// runWrite is the load of one round: --writers connections insert documents no other round
// or writer sends, until every connection is lost (the server was killed) or a signal
// arrives. Every write that was sent gets a ledger row.
func runWrite(args []string, out io.Writer) error {
	var node target
	flags := flag.NewFlagSet("write", flag.ContinueOnError)
	node.register(flags)
	ledgerPath := flags.String("ledger", "", "ledger file to append to (required)")
	round := flags.Int("round", 1, "round number; with the writer and a counter it makes every id unique")
	writers := flags.Int("writers", 4, "concurrent writers, each with its own connection")
	padBytes := flags.Int("pad", 256, "bytes of padding in each document")
	create := flags.Bool("create", false, "create the database and the bucket first if absent")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing write flags: %w", err)
	}
	if err := node.validate("write"); err != nil {
		return err
	}
	switch {
	case *ledgerPath == "":
		return errors.New("write needs --ledger FILE, the file every sent write is recorded in")
	case *writers < 1 || *round < 1 || *padBytes < 0:
		return errors.New("--writers and --round must be at least 1 and --pad at least 0")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *create {
		if err := createBucket(ctx, &node); err != nil {
			return err
		}
	}
	ledger, err := openLedger(*ledgerPath)
	if err != nil {
		return err
	}
	var (
		wait  sync.WaitGroup
		lock  sync.Mutex
		acked int
		sent  int
	)
	for writer := 0; writer < *writers; writer++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			rows := writeUntilLost(ctx, &node, *round, writer, *padBytes)
			lock.Lock()
			defer lock.Unlock()
			for _, row := range rows {
				sent++
				if row.Outcome == outcomeAcked {
					acked++
				}
			}
			if appendErr := writeLedger(ledger, rows, false); appendErr != nil && err == nil {
				err = appendErr
			}
		}()
	}
	wait.Wait()
	if closeErr := ledger.Close(); closeErr != nil && err == nil {
		err = fmt.Errorf("closing the ledger %s: %w", *ledgerPath, closeErr)
	}
	if err != nil {
		return err
	}
	return say(out, "round %d: sent %d, acknowledged %d, unknown %d\n", *round, sent, acked, sent-acked)
}

// openLedger opens the ledger for append and writes the header into a new one.
func openLedger(path string) (*os.File, error) {
	ledger, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening the ledger: %w", err)
	}
	stat, err := ledger.Stat()
	if err == nil && stat.Size() == 0 {
		err = writeLedger(ledger, nil, true)
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("preparing the ledger %s: %w", path, err), ledger.Close())
	}
	return ledger, nil
}

// maxRefusalsInARow ends a writer whose every write a live server refuses: the load is not
// running, and the drill should say so instead of spinning.
const maxRefusalsInARow = 200

// writeUntilLost inserts one document after another on its own connection and returns a row
// for each. It stops when the connection is lost or the context ends.
func writeUntilLost(ctx context.Context, node *target, round, writer, padBytes int) []ledgerRow {
	client, err := node.dial(ctx)
	if err != nil {
		return nil // nothing was sent, so there is nothing to record
	}
	defer func() { _ = client.Close() }() // every answer that will come has been read
	bucket := client.Bucket(node.database, node.bucket)
	var rows []ledgerRow
	refused := 0
	for seq := int64(0); ctx.Err() == nil; seq++ {
		id := drillID(round, writer, seq)
		requestCtx, cancel := context.WithTimeout(ctx, node.timeout)
		answer, err := bucket.Insert(requestCtx, id, drillBody(round, writer, seq, id, padBytes))
		cancel()
		outcome, detail := classifyWrite(answer, err)
		rows = append(rows, ledgerRow{Round: round, Writer: writer, Seq: seq, ID: id, Outcome: outcome, Detail: detail})
		switch {
		case err == nil:
			refused = 0
		case !isRefusal(err):
			return rows // the connection is gone or the deadline passed: the server was killed
		default:
			refused++
			if refused >= maxRefusalsInARow {
				return rows
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return rows
}

// runRead reads back every id of the ledger, acknowledged or not, scans the whole bucket,
// asks the server for its count, and writes what it saw.
func runRead(args []string, out io.Writer) error {
	var node target
	flags := flag.NewFlagSet("read", flag.ContinueOnError)
	node.register(flags)
	ledgerPath := flags.String("ledger", "", "the ledger durabilitydrill write appended to (required)")
	outPath := flags.String("out", "", "observations file to write (required)")
	readers := flags.Int("readers", 8, "concurrent readers")
	patience := flags.Duration("patience", 30*time.Second, "how long one id is retried before it is recorded unreadable")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing read flags: %w", err)
	}
	if err := node.validate("read"); err != nil {
		return err
	}
	if *ledgerPath == "" || *outPath == "" || *readers < 1 {
		return errors.New("read needs --ledger FILE and --out FILE, and --readers of at least 1")
	}
	rows, err := loadLedger(*ledgerPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	seen := observations{Gets: readEvery(ctx, &node, rows, *readers, *patience), ServerCount: -1}
	client, err := node.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }() // the scan's answers were already read
	bucket := client.Bucket(node.database, node.bucket)
	if seen.Listed, err = scanBucket(ctx, &node, bucket); err != nil {
		return err
	}
	countCtx, cancel := context.WithTimeout(ctx, 6*node.timeout)
	defer cancel()
	if count, err := bucket.Count(countCtx, nil); err == nil {
		seen.ServerCount = count
	} else if err := say(out, "note: the server's own count was not read (%v); the scan and the Gets are the evidence\n", err); err != nil {
		return err
	}
	file, err := os.OpenFile(*outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating the observations file: %w", err)
	}
	if err := errors.Join(writeObservations(file, seen), file.Close()); err != nil {
		return fmt.Errorf("writing %s: %w", *outPath, err)
	}
	return say(out, "read %d ledger ids and listed %d ids in %s.%s\n", len(seen.Gets), len(seen.Listed), node.database, node.bucket)
}

// readEvery reads each ledger id with a Get and returns its state.
func readEvery(ctx context.Context, node *target, rows []ledgerRow, readers int, patience time.Duration) map[string]string {
	states := make(map[string]string, len(rows))
	var (
		lock sync.Mutex
		wait sync.WaitGroup
	)
	ids := make(chan string)
	for reader := 0; reader < readers; reader++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			var client *ringclient.Client
			defer func() {
				if client != nil {
					_ = client.Close() // every answer was already read
				}
			}()
			for id := range ids {
				var state string
				state, client = readOne(ctx, node, client, id, patience)
				lock.Lock()
				states[id] = state
				lock.Unlock()
			}
		}()
	}
	for _, row := range rows {
		ids <- row.ID
	}
	close(ids)
	wait.Wait()
	return states
}

// readOne reads id until a conclusive answer or the deadline, redialling after a lost
// connection. It returns the connection it ended with, nil when that one is broken.
func readOne(ctx context.Context, node *target, client *ringclient.Client, id string, patience time.Duration) (string, *ringclient.Client) {
	deadline := time.Now().Add(patience)
	for {
		state, retry, kept := readAttempt(ctx, node, client, id)
		client = kept
		if !retry || time.Now().After(deadline) || ctx.Err() != nil {
			return state, client
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func readAttempt(ctx context.Context, node *target, client *ringclient.Client, id string) (state string, retry bool, kept *ringclient.Client) {
	if client == nil {
		dialled, err := node.dial(ctx)
		if err != nil {
			return stateUnreadable, true, nil
		}
		client = dialled
	}
	requestCtx, cancel := context.WithTimeout(ctx, node.timeout)
	defer cancel()
	body, err := client.Bucket(node.database, node.bucket).Get(requestCtx, id)
	if err != nil {
		state, retry = classifyReadError(err)
		if retry && !isRefusal(err) {
			_ = client.Close() // broken; the next attempt dials again
			client = nil
		}
		return state, retry, client
	}
	check, checkOK := body.Lookup("Check").StringValueOK()
	pad, padOK := body.Lookup("Pad").StringValueOK()
	if !checkOK || !padOK || !bodyMatches(id, check, pad) {
		return stateMismatch, false, client
	}
	return statePresent, false, client
}

// scanBucket lists the whole bucket page by page and counts how often each id came back.
func scanBucket(ctx context.Context, node *target, bucket *ringclient.Bucket) (map[string]int, error) {
	listed := map[string]int{}
	page := ringclient.PageRequest{Limit: 1000, Fields: []string{"Seq"}}
	for {
		requestCtx, cancel := context.WithTimeout(ctx, 6*node.timeout)
		answer, err := bucket.List(requestCtx, nil, page)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("scanning %s.%s after %d ids: %w", node.database, node.bucket, len(listed), err)
		}
		for _, entry := range answer.Entries {
			listed[entry.ID]++
		}
		if !answer.More {
			return listed, nil
		}
		if len(answer.Cursor) == 0 {
			return nil, fmt.Errorf("scanning %s.%s: the server said there is more after %d ids and gave no cursor", node.database, node.bucket, len(listed))
		}
		page.Cursor = answer.Cursor
	}
}

// runJudge compares a ledger with an observations file and prints the report. It reaches no
// server. The error it returns after a failing report is errDrillFailed.
func runJudge(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("judge", flag.ContinueOnError)
	ledgerPath := flags.String("ledger", "", "the ledger durabilitydrill write appended to (required)")
	seenPath := flags.String("observations", "", "the file durabilitydrill read wrote (required)")
	database := flags.String("db", "drilldb", "database the drill wrote to, for the commands a failure prints")
	bucket := flags.String("bucket", "Writes", "bucket the drill wrote to, for the commands a failure prints")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing judge flags: %w", err)
	}
	if *ledgerPath == "" || *seenPath == "" {
		return errors.New("judge needs --ledger FILE and --observations FILE, for example: durabilitydrill judge --ledger ledger.csv --observations seen.csv")
	}
	rows, err := loadLedger(*ledgerPath)
	if err != nil {
		return err
	}
	seen, err := loadObservations(*seenPath)
	if err != nil {
		return err
	}
	found, err := judge(rows, seen)
	if err != nil {
		return err
	}
	found.Database, found.Bucket = *database, *bucket
	if err := found.print(out); err != nil {
		return err
	}
	if len(found.failures()) > 0 {
		return errDrillFailed
	}
	return nil
}

// errDrillFailed is a report that did not pass; the report itself was already printed.
var errDrillFailed = errors.New("the durability drill failed")

// runDelete removes one acknowledged document through the client. It exists to prove the
// drill can fail: after it, read and judge must report exactly that id missing.
func runDelete(args []string, out io.Writer) error {
	var node target
	flags := flag.NewFlagSet("delete-acknowledged", flag.ContinueOnError)
	node.register(flags)
	ledgerPath := flags.String("ledger", "", "the ledger to take the first acknowledged id from (required)")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing delete-acknowledged flags: %w", err)
	}
	if err := node.validate("delete-acknowledged"); err != nil {
		return err
	}
	if *ledgerPath == "" {
		return errors.New("delete-acknowledged needs --ledger FILE")
	}
	rows, err := loadLedger(*ledgerPath)
	if err != nil {
		return err
	}
	victim := ""
	for _, row := range rows {
		if row.Outcome == outcomeAcked {
			victim = row.ID
			break
		}
	}
	if victim == "" {
		return errors.New("the ledger holds no acknowledged write to delete")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*node.timeout)
	defer cancel()
	client, err := node.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }() // the delete's answer was already read
	if _, err := client.Bucket(node.database, node.bucket).Delete(ctx, victim); err != nil {
		return fmt.Errorf("deleting %s: %w", victim, err)
	}
	return say(out, "deleted %s\n", victim)
}
