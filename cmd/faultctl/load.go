package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// Error codes faultctl writes for failures that are not a ring refusal. A node that is up
// never gives either; verify treats them as expected only inside a declared down window.
const (
	codeTimeout = "TIMEOUT" // the attempt's deadline passed with no answer
	codeConn    = "CONN"    // the connection could not be made or was lost
)

type loadConfig struct {
	command     string
	addr        string
	database    string
	bucket      string
	clients     int
	rate        float64 // attempts per second per client; 0 means as fast as the node answers
	duration    time.Duration
	seed        uint64
	timeout     time.Duration
	create      bool
	replication int
	idsPath     string
	passes      int
	wait        time.Duration
	user        string
	password    string
	fingerprint string
	insecure    bool
}

// plannedWrite is one write of a client's stream.
type plannedWrite struct {
	ID  string
	Seq int64
	Pad string
}

// writeStream yields the writes of one client. The stream depends only on the seed and the
// client number, so a re-run with the same seed sends the same ids, seqs and bodies in the
// same order (the interleaving between clients is not reproduced).
type writeStream struct {
	client int
	next   int64
	random *rand.Rand
}

func newWriteStream(seed uint64, client int) *writeStream {
	return &writeStream{client: client, random: rand.New(rand.NewPCG(seed, uint64(client)))}
}

func (s *writeStream) take() plannedWrite {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	pad := make([]byte, 16+s.random.IntN(49))
	for i := range pad {
		pad[i] = alphabet[s.random.IntN(len(alphabet))]
	}
	write := plannedWrite{ID: clientName(s.client) + "-" + strconv.FormatInt(s.next, 10), Seq: s.next, Pad: string(pad)}
	s.next++
	return write
}

func clientName(client int) string { return "c" + strconv.Itoa(client) }

func parseLoadFlags(command string, args []string) (loadConfig, error) {
	config := loadConfig{command: command}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.StringVar(&config.addr, "addr", "", "node address host:port (required)")
	flags.StringVar(&config.database, "db", "faultdb", "database name")
	flags.StringVar(&config.bucket, "bucket", "events", "bucket name")
	flags.IntVar(&config.clients, "clients", 4, "concurrent clients, each with its own connection")
	flags.Float64Var(&config.rate, "rate", 0, "attempts per second per client; 0 for unthrottled")
	flags.DurationVar(&config.duration, "duration", 30*time.Second, "how long to write")
	flags.Uint64Var(&config.seed, "seed", 1, "seed of the write streams")
	flags.DurationVar(&config.timeout, "timeout", 5*time.Second, "deadline of one attempt")
	flags.BoolVar(&config.create, "create", false, "write only: create the database and bucket first if absent")
	flags.IntVar(&config.replication, "rf", 0, "write --create only: replication factor, 0 for the server default")
	flags.StringVar(&config.idsPath, "ids", "", "read only: the CSV written by faultctl write; every id it attempted is read")
	flags.IntVar(&config.passes, "passes", 1, "read only: how many times to read every id")
	flags.DurationVar(&config.wait, "wait", 0, "read only: how long to keep trying to connect (a node that is still booting)")
	flags.StringVar(&config.fingerprint, "fingerprint", "", "pin the node's certificate fingerprint (SHA256:...)")
	flags.BoolVar(&config.insecure, "insecure", false, "skip certificate verification (a test VM)")
	if err := flags.Parse(args); err != nil {
		return config, fmt.Errorf("parsing %s flags: %w", command, err)
	}
	config.user = envOr("FAULTCTL_USER", "root")
	config.password = envOr("FAULTCTL_PASSWORD", os.Getenv("HOARDDB_ROOT_PASSWORD"))
	switch {
	case config.addr == "":
		return config, errors.New("--addr is required, for example: faultctl " + command + " --addr 192.0.2.1:7433 --insecure")
	case config.password == "":
		return config, errors.New("no password: export FAULTCTL_PASSWORD (or HOARDDB_ROOT_PASSWORD) before running")
	case !config.insecure && config.fingerprint == "":
		return config, errors.New("pass --fingerprint SHA256:... from the node's startup log, or --insecure on a test VM")
	case config.clients < 1:
		return config, errors.New("--clients must be at least 1")
	case command == "read" && config.idsPath == "":
		return config, errors.New("read needs --ids acks.csv, the file faultctl write printed")
	}
	return config, nil
}

func envOr(name, fallback string) string {
	if found := os.Getenv(name); found != "" {
		return found
	}
	return fallback
}

// runLoad is faultctl write and faultctl read: it runs the load and prints the CSV.
func runLoad(command string, args []string, out io.Writer) error {
	config, err := parseLoadFlags(command, args)
	if err != nil {
		return err
	}
	// SIGINT or SIGTERM ends the run early but still prints what was recorded.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var rows []attempt
	if command == "write" {
		rows, err = runWrites(ctx, config)
	} else {
		rows, err = runReads(ctx, config)
	}
	if err != nil {
		return err
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].AtNs < rows[j].AtNs })
	return writeAttempts(out, rows)
}

func (c loadConfig) dial(ctx context.Context) (*ringclient.Client, error) {
	return ringclient.Dial(ctx, ringclient.Options{
		Addr: c.addr, User: c.user, Password: c.password, Fingerprint: c.fingerprint, Insecure: c.insecure,
		DialTimeout: c.timeout, RequestTimeout: c.timeout,
	})
}

// errorCode names a failed attempt: the ring code of a refusal, TIMEOUT for a passed deadline,
// CONN for everything else (a connection that could not be made or was lost).
func errorCode(err error) string {
	var refusal *ringclient.Error
	var named *cluster.Error
	switch {
	case errors.As(err, &refusal):
		return refusal.Code.String()
	case errors.As(err, &named):
		return named.Code.String()
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return codeTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return codeTimeout
	}
	return codeConn
}

// session is one client's connection, redialled after a connection failure.
type session struct {
	config loadConfig
	conn   *ringclient.Client
}

func (s *session) bucket(ctx context.Context) (*ringclient.Bucket, error) {
	if s.conn == nil {
		dialCtx, cancel := context.WithTimeout(ctx, s.config.timeout)
		defer cancel()
		conn, err := s.config.dial(dialCtx)
		if err != nil {
			return nil, fmt.Errorf("dialling %s: %w", s.config.addr, err)
		}
		s.conn = conn
	}
	return s.conn.Bucket(s.config.database, s.config.bucket), nil
}

// drop forgets the connection after a failure that is not a ring refusal.
func (s *session) drop(code string) {
	if code == codeConn && s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

func (s *session) close() {
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func runWrites(ctx context.Context, config loadConfig) ([]attempt, error) {
	if config.create {
		if err := createBucket(ctx, config); err != nil {
			return nil, err
		}
	}
	var (
		lock sync.Mutex
		rows []attempt
		wait sync.WaitGroup
	)
	deadline := time.Now().Add(config.duration)
	for client := 0; client < config.clients; client++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			mine := writeClient(ctx, config, client, deadline)
			lock.Lock()
			rows = append(rows, mine...)
			lock.Unlock()
		}()
	}
	wait.Wait()
	return rows, nil
}

func writeClient(ctx context.Context, config loadConfig, client int, deadline time.Time) []attempt {
	stream := newWriteStream(config.seed, client)
	conn := &session{config: config}
	defer conn.close()
	var rows []attempt
	for next := time.Now(); time.Now().Before(deadline) && ctx.Err() == nil; {
		planned := stream.take()
		row := attempt{Client: clientName(client), ID: planned.ID, Seq: planned.Seq, Via: config.addr}
		bucket, err := conn.bucket(ctx)
		if err == nil {
			attemptCtx, cancel := context.WithTimeout(ctx, config.timeout)
			var result ringclient.WriteResult
			result, err = bucket.Insert(attemptCtx, planned.ID, ringclient.Doc{"name": row.Client, "seq": planned.Seq, "pad": planned.Pad})
			cancel()
			if err == nil {
				row.Outcome, row.Copies = result.Outcome, result.Copies
				row.PosEpoch, row.PosSeq = parsePosition(result.Position)
			}
		}
		row.AtNs = time.Now().UnixNano()
		if err != nil {
			row.ErrCode = errorCode(err)
			conn.drop(row.ErrCode)
			if row.ErrCode == codeConn {
				pause(ctx, 200*time.Millisecond)
			}
		}
		rows = append(rows, row)
		if config.rate > 0 {
			next = next.Add(time.Duration(float64(time.Second) / config.rate))
			pause(ctx, time.Until(next))
		}
	}
	return rows
}

// pause sleeps for span or until ctx ends.
func pause(ctx context.Context, span time.Duration) {
	if span <= 0 {
		return
	}
	timer := time.NewTimer(span)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func parsePosition(position string) (epoch, seq uint64) {
	first, second, ok := strings.Cut(position, ":")
	if !ok {
		return 0, 0
	}
	epoch, _ = strconv.ParseUint(first, 10, 64)
	seq, _ = strconv.ParseUint(second, 10, 64)
	return epoch, seq
}

// createBucket makes the database and bucket the write streams need; one that exists is fine.
func createBucket(ctx context.Context, config loadConfig) error {
	conn, err := config.dial(ctx)
	if err != nil {
		return fmt.Errorf("connecting to %s to create %s.%s: %w", config.addr, config.database, config.bucket, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.CreateDatabase(ctx, config.database, int32(config.replication)); err != nil {
		var refusal *ringclient.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeConflict {
			return fmt.Errorf("creating database %q: %w", config.database, err)
		}
	}
	fields := []ringclient.FieldDecl{{Name: "name", Type: "string"}, {Name: "seq", Type: "int"}, {Name: "pad", Type: "string"}}
	if _, err := conn.CreateBucket(ctx, config.database, config.bucket, fields); err != nil {
		return fmt.Errorf("creating bucket %q in %q: %w", config.bucket, config.database, err)
	}
	return nil
}

func runReads(ctx context.Context, config loadConfig) ([]attempt, error) {
	written, err := loadAttempts(config.idsPath)
	if err != nil {
		return nil, err
	}
	// Every attempted id is read, not only the acked ones: an unacked write that is present
	// is information, and one that is absent is expected.
	var (
		lock sync.Mutex
		rows []attempt
		wait sync.WaitGroup
	)
	for worker := 0; worker < config.clients; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			mine := readWorker(ctx, config, written, worker)
			lock.Lock()
			rows = append(rows, mine...)
			lock.Unlock()
		}()
	}
	wait.Wait()
	return rows, nil
}

func readWorker(ctx context.Context, config loadConfig, written []attempt, worker int) []attempt {
	conn := &session{config: config}
	defer conn.close()
	waitUntil := time.Now().Add(config.wait)
	var rows []attempt
	for pass := 0; pass < max(config.passes, 1); pass++ {
		for index := worker; index < len(written) && ctx.Err() == nil; index += config.clients {
			target := written[index]
			row := attempt{Client: target.Client, ID: target.ID, Via: config.addr}
			bucket, err := conn.bucket(ctx)
			for err != nil && time.Now().Before(waitUntil) && ctx.Err() == nil {
				pause(ctx, time.Second)
				bucket, err = conn.bucket(ctx)
			}
			if err == nil {
				attemptCtx, cancel := context.WithTimeout(ctx, config.timeout)
				raw, getErr := bucket.Get(attemptCtx, target.ID)
				cancel()
				err = getErr
				if err == nil {
					row.Seq = raw.Lookup("seq").AsInt64()
				}
			}
			row.AtNs = time.Now().UnixNano()
			switch {
			case err == nil:
			case errors.Is(err, ringclient.ErrNotFound):
				row.Seq = -1
			default:
				row.Seq = -1
				row.ErrCode = errorCode(err)
				conn.drop(row.ErrCode)
			}
			rows = append(rows, row)
			if config.rate > 0 {
				pause(ctx, time.Duration(float64(time.Second)/config.rate))
			}
		}
	}
	return rows
}
