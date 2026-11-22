//go:build bsonoracle

package tap

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	libbsontype "github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/oracle/compare"
	vendorbson "go.mongodb.org/mongo-driver/bson"
	vendorbsontype "go.mongodb.org/mongo-driver/bson/bsontype"
	vendorbsoncore "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/internal/bsonoracle"
)

// outputVariable names the directory the tap records (Go type, bytes) pairs
// into. Unset, nothing is recorded.
const outputVariable = "BSONORACLE_OUT"

func init() {
	bson.SetOracleTap(Observe)
}

// Observe is the tap. Called with a call's inputs before the call runs, it
// prepares the vendor's side (a fresh decode target of the same Go type), and
// the function it returns compares the library's results with the vendor's.
func Observe(before bson.OracleEvent) func(bson.OracleEvent) error {
	vendorTarget := prepareVendorTarget(before)
	site := callSite()
	return func(after bson.OracleEvent) error {
		divergence := compareWithVendor(after, vendorTarget)
		if divergence != nil {
			divergences.add(divergence)
			logDivergence(site, after, divergence)
		}
		recordingError := record(after, site)
		return errors.Join(divergence, recordingError)
	}
}

// divergenceList is every divergence the tap has reported, for a test that
// wants to read them without the call's error.
type divergenceList struct {
	mutex   sync.Mutex
	reports []string
	calls   int
}

func (list *divergenceList) add(divergence error) {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	list.reports = append(list.reports, divergence.Error())
}

func (list *divergenceList) countCall() {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	list.calls++
}

var divergences divergenceList

// Divergences returns the divergences reported since the last Reset.
func Divergences() []string {
	divergences.mutex.Lock()
	defer divergences.mutex.Unlock()
	return append([]string(nil), divergences.reports...)
}

// Calls returns how many calls the tap has compared since the last Reset.
func Calls() int {
	divergences.mutex.Lock()
	defer divergences.mutex.Unlock()
	return divergences.calls
}

// Reset clears the divergences and the call count.
func Reset() {
	divergences.mutex.Lock()
	defer divergences.mutex.Unlock()
	divergences.reports = nil
	divergences.calls = 0
}

// vendorSide is the vendor's fresh decode target for one decode call, or the
// reason there is none.
type vendorSide struct {
	target reflect.Value
	skip   string
}

// prepareVendorTarget makes the vendor's decode target before the library
// writes into its own. A target that already holds data would merge, and the
// tap cannot copy a value of any type, so such a call is skipped and counted.
func prepareVendorTarget(before bson.OracleEvent) vendorSide {
	switch before.Function {
	case "Unmarshal", "RawValue.Unmarshal", "Document.Decode", "UnmarshalExtJSON":
	default:
		return vendorSide{}
	}
	target := reflect.ValueOf(before.Value)
	switch target.Kind() {
	case reflect.Pointer:
		if target.IsNil() {
			return vendorSide{skip: "a nil pointer target"}
		}
		if !target.Elem().IsZero() {
			return vendorSide{skip: "a target that already holds data"}
		}
		return vendorSide{target: reflect.New(target.Elem().Type())}
	case reflect.Map:
		if target.IsNil() || target.Len() != 0 {
			return vendorSide{skip: "a nil or non-empty map target"}
		}
		return vendorSide{target: reflect.New(target.Type())}
	}
	return vendorSide{skip: fmt.Sprintf("a target of kind %v", target.Kind())}
}

// compareWithVendor runs the vendor on the call's inputs and compares.
func compareWithVendor(after bson.OracleEvent, side vendorSide) error {
	divergences.countCall()
	if errors.Is(after.Err, bson.ErrNestingTooDeep) {
		// The library refuses past its ceiling of 200; the vendor has none.
		return nil
	}
	switch after.Function {
	case "Marshal":
		return compareMarshal(after)
	case "MarshalValue":
		return compareMarshalValue(after)
	case "Unmarshal", "Document.Decode":
		return compareUnmarshal(after, side)
	case "RawValue.Unmarshal":
		return compareRawValueUnmarshal(after, side)
	case "MarshalExtJSON":
		return compareMarshalExtJSON(after)
	case "UnmarshalExtJSON":
		return compareUnmarshalExtJSON(after, side)
	}
	return fmt.Errorf("oracle tap: no comparison for the entry point %q", after.Function)
}

// failureRecorder lets compare's helpers report to the tap as to a testing.TB.
// Helper and Errorf are all the helpers call; any other method panics on the
// nil embedded interface, which is the signal that a helper changed.
type failureRecorder struct {
	testing.TB
	failures []string
}

func (recorder *failureRecorder) Helper() {}

func (recorder *failureRecorder) Errorf(format string, args ...any) {
	recorder.failures = append(recorder.failures, fmt.Sprintf(format, args...))
}

func (recorder *failureRecorder) err(function string) error {
	if len(recorder.failures) == 0 {
		return nil
	}
	return fmt.Errorf("oracle tap: %s diverges from the vendor: %s", function, strings.Join(recorder.failures, "; "))
}

func errorParity(function string, libraryErr, vendorErr error) (bothFailed bool, divergence error) {
	switch {
	case libraryErr == nil && vendorErr == nil:
		return false, nil
	case libraryErr != nil && vendorErr != nil:
		return true, nil
	case libraryErr != nil:
		return true, fmt.Errorf("oracle tap: %s: the library refused (%v) what the vendor accepts", function, libraryErr)
	}
	return true, fmt.Errorf("oracle tap: %s: the library accepted what the vendor refuses (%v)", function, vendorErr)
}

func compareMarshal(after bson.OracleEvent) error {
	vendorBytes, vendorErr := bsonoracle.Marshal(after.Value)
	if bothFailed, divergence := errorParity("Marshal", after.Err, vendorErr); bothFailed {
		return divergence
	}
	recorder := &failureRecorder{}
	compare.Encoded(recorder, after.Value, vendorBytes, after.Bytes)
	return recorder.err("Marshal")
}

// compareMarshalValue puts the value in a one-element document on the vendor's
// side, which gives the registry a place to write a scalar, and reads the
// element back as the library's pair of type and bytes is compared.
func compareMarshalValue(after bson.OracleEvent) error {
	if after.Value == nil {
		// An untyped nil has no encoder in either library; inside a document
		// element the vendor would write a null, which is not the question.
		_, _, vendorErr := vendorbson.MarshalValue(nil)
		_, divergence := errorParity("MarshalValue", after.Err, vendorErr)
		return divergence
	}
	vendorDocument, vendorErr := bsonoracle.Marshal(bson.D{{Key: "v", Value: after.Value}})
	if bothFailed, divergence := errorParity("MarshalValue", after.Err, vendorErr); bothFailed {
		return divergence
	}
	libraryDocument := vendorbsoncore.BuildDocument(nil,
		vendorbsoncore.AppendValueElement(nil, "v", vendorbsoncore.Value{Type: vendorbsoncoreType(after.Type), Data: after.Bytes}))
	recorder := &failureRecorder{}
	compare.Encoded(recorder, bson.D{{Key: "v", Value: after.Value}}, vendorDocument, libraryDocument)
	return recorder.err("MarshalValue")
}

func compareUnmarshal(after bson.OracleEvent, side vendorSide) error {
	if side.skip != "" {
		return nil
	}
	vendorErr := bsonoracle.Unmarshal(after.Bytes, side.target.Interface())
	if bothFailed, divergence := errorParity(after.Function, after.Err, vendorErr); bothFailed {
		return divergence
	}
	return compareDecoded(after, side.target)
}

// compareMarshalExtJSON writes the same value's Extended JSON with the vendor
// and compares the two texts with compare.ExtJSON, which tolerates only the
// vendor's random map order.
func compareMarshalExtJSON(after bson.OracleEvent) error {
	vendorText, vendorErr := vendorbson.MarshalExtJSONWithRegistry(bsonoracle.Registry(), after.Value, after.Canonical, after.EscapeHTML) //nolint:staticcheck // SA1019: the registry argument is the point
	if bothFailed, divergence := errorParity("MarshalExtJSON", after.Err, vendorErr); bothFailed {
		return divergence
	}
	recorder := &failureRecorder{}
	compare.ExtJSON(recorder, after.Value, vendorText, after.Bytes)
	return recorder.err("MarshalExtJSON")
}

// compareUnmarshalExtJSON decodes the same text with the vendor into a fresh
// target of the library's target type and compares what the two decoded.
func compareUnmarshalExtJSON(after bson.OracleEvent, side vendorSide) error {
	if side.skip != "" {
		return nil
	}
	vendorErr := vendorbson.UnmarshalExtJSONWithRegistry(bsonoracle.Registry(), after.Bytes, after.Canonical, side.target.Interface()) //nolint:staticcheck // SA1019: the registry argument is the point
	if bothFailed, divergence := errorParity("UnmarshalExtJSON", after.Err, vendorErr); bothFailed {
		return divergence
	}
	return compareDecoded(after, side.target)
}

// compareRawValueUnmarshal decodes the value on the vendor's side as the field
// of a one-field document, so the same registry and the same target type
// apply.
func compareRawValueUnmarshal(after bson.OracleEvent, side vendorSide) error {
	if side.skip != "" {
		return nil
	}
	targetType := side.target.Type().Elem()
	holderType := reflect.StructOf([]reflect.StructField{{Name: "V", Type: targetType, Tag: `bson:"v"`}})
	holder := reflect.New(holderType)
	document := vendorbsoncore.BuildDocument(nil,
		vendorbsoncore.AppendValueElement(nil, "v", vendorbsoncore.Value{Type: vendorbsoncoreType(after.Type), Data: after.Bytes}))
	vendorErr := bsonoracle.Unmarshal(document, holder.Interface())
	if bothFailed, divergence := errorParity("RawValue.Unmarshal", after.Err, vendorErr); bothFailed {
		return divergence
	}
	side.target.Elem().Set(holder.Elem().Field(0))
	return compareDecoded(after, side.target)
}

// compareDecoded converts what the vendor decoded to the library's container
// types where an interface holds them, then compares with what the library
// decoded. vendorTarget is a pointer to the vendor's result.
func compareDecoded(after bson.OracleEvent, vendorTarget reflect.Value) error {
	bsonoracle.ConvertTarget(vendorTarget.Interface())
	libraryResult := reflect.ValueOf(after.Value)
	if libraryResult.Kind() == reflect.Pointer {
		libraryResult = libraryResult.Elem()
	}
	recorder := &failureRecorder{}
	compare.Decoded(recorder, vendorTarget.Elem().Interface(), libraryResult.Interface())
	return recorder.err(after.Function)
}

// --- recording ---

// recorded is the set of (site, function, type, bytes) already written, so a test
// that makes the same call a million times writes it once.
var recorded = struct {
	mutex sync.Mutex
	seen  map[[sha256.Size]byte]struct{}
	files map[string]*os.File
}{seen: map[[sha256.Size]byte]struct{}{}, files: map[string]*os.File{}}

// maximumRecordedPairs bounds the set a long run keeps in memory.
const maximumRecordedPairs = 2_000_000

type recordedPair struct {
	Site     string `json:"site"`
	Function string `json:"function"`
	GoType   string `json:"goType"`
	Bytes    []byte `json:"bytes"`
}

// record appends the call's (Go type, bytes) pair to a file in
// $BSONORACLE_OUT, one file per test binary and process. A call that failed
// has no bytes worth seeding from and is not recorded.
func record(after bson.OracleEvent, site string) error {
	directory := os.Getenv(outputVariable)
	if directory == "" || after.Err != nil || len(after.Bytes) == 0 {
		return nil
	}
	goType := "nil"
	if after.Value != nil {
		valueType := reflect.TypeOf(after.Value)
		if valueType.Kind() == reflect.Pointer && after.Function != "Marshal" && after.Function != "MarshalValue" {
			valueType = valueType.Elem()
		}
		goType = valueType.String()
	}
	key := sha256.Sum256([]byte(site + "\x00" + after.Function + "\x00" + goType + "\x00" + string(after.Bytes)))

	recorded.mutex.Lock()
	defer recorded.mutex.Unlock()
	if _, already := recorded.seen[key]; already || len(recorded.seen) >= maximumRecordedPairs {
		return nil
	}
	recorded.seen[key] = struct{}{}
	outputFile, err := outputFileIn(directory)
	if err != nil {
		return err
	}
	line, err := json.Marshal(recordedPair{Site: site, Function: after.Function, GoType: goType, Bytes: after.Bytes})
	if err != nil {
		return fmt.Errorf("oracle tap: encoding a record for %s: %w", goType, err)
	}
	if _, err := outputFile.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("oracle tap: writing to %s: %w", outputFile.Name(), err)
	}
	return nil
}

func outputFileIn(directory string) (*os.File, error) {
	if opened, ok := recorded.files[directory]; ok {
		return opened, nil
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("oracle tap: creating %s: %w", directory, err)
	}
	fileName := fmt.Sprintf("%s-%d.jsonl", filepath.Base(os.Args[0]), os.Getpid())
	opened, err := os.OpenFile(filepath.Join(directory, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("oracle tap: opening a record file in %s: %w", directory, err)
	}
	recorded.files[directory] = opened
	return opened, nil
}

// vendorbsoncoreType is the vendor's name for the library's BSON type byte.
func vendorbsoncoreType(libraryType libbsontype.Type) vendorbsontype.Type {
	return vendorbsontype.Type(libraryType)
}

// callSite is the first frame of the caller outside the library, its oracle
// module and this package: the hoardDB file and line that made the call. It
// lets a recorded pair or a divergence be attributed to a call site, which is
// what the per-site classes of the API inventory need.
func callSite() string {
	programCounters := make([]uintptr, 32)
	count := runtime.Callers(2, programCounters)
	frames := runtime.CallersFrames(programCounters[:count])
	for {
		frame, more := frames.Next()
		inLibrary := strings.HasPrefix(frame.Function, "github.com/hoarddb/hoarddb-bson")
		inTap := strings.HasPrefix(frame.Function, "github.com/hoarddb/hoarddb-server/internal/bsonoracle/tap.") && !strings.HasSuffix(frame.File, "_test.go")
		if !inLibrary && !inTap {
			file := frame.File
			if index := strings.LastIndex(file, "/hoardDB/"); index >= 0 {
				file = file[index+len("/hoardDB/"):]
			}
			return fmt.Sprintf("%s:%d", file, frame.Line)
		}
		if !more {
			return "unknown"
		}
	}
}

var divergenceLog sync.Mutex

// logDivergence appends one JSON line per divergence to divergences.jsonl in
// $BSONORACLE_OUT, with the call site, so that a run of thousands of tests
// leaves one attributable list beside the test failures. Unset, nothing is
// written. A log that cannot be written is reported on stderr: the divergence
// itself still fails the call.
func logDivergence(site string, after bson.OracleEvent, divergence error) {
	directory := os.Getenv(outputVariable)
	if directory == "" {
		return
	}
	divergenceLog.Lock()
	defer divergenceLog.Unlock()
	line, err := json.Marshal(map[string]any{
		"site": site, "function": after.Function, "goType": fmt.Sprintf("%T", after.Value),
		"bytes": after.Bytes, "divergence": divergence.Error(), "binary": filepath.Base(os.Args[0]),
	})
	if err == nil {
		err = appendLine(filepath.Join(directory, "divergences.jsonl"), line)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "oracle tap: logging a divergence in %s: %v\n", directory, err)
	}
}

func appendLine(path string, line []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", filepath.Dir(path), err)
	}
	logFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("opening %s: %w", path, err)
	}
	if _, err := logFile.Write(append(line, '\n')); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := logFile.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", path, err)
	}
	return nil
}
