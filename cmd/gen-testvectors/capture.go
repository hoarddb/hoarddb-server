package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// requestIDPrefix fills the first 12 bytes of every vector's request ID; the
// last four carry the exchange's number.
var requestIDPrefix = []byte("vector-id-v1")

// fixedRequestID is the request ID of exchange number n.
func fixedRequestID(n int) [transport.RequestIDLen]byte {
	var id [transport.RequestIDLen]byte
	copy(id[:], requestIDPrefix)
	binary.BigEndian.PutUint32(id[12:], uint32(n))
	return id
}

// call is one request: an opcode and the server's own request value, or raw
// payload bytes for the payloads that are not BSON.
type call struct {
	op      transport.Opcode
	payload any
	raw     []byte
	// hook, in a setup, is a step that is not a request: it runs between the
	// requests around it (an alteration through the server's own API, after a
	// token was minted and before it is presented) and is not recorded.
	hook func(ctx context.Context, g *generation, db string) error
}

// encode produces the request payload with the server's encoder, nested
// documents sorted (normaliseStored).
func (c call) encode() ([]byte, error) {
	if c.payload == nil {
		return c.raw, nil
	}
	encoded, err := bson.Marshal(c.payload)
	if err != nil {
		return nil, fmt.Errorf("encoding the %s request: %w", c.op, err)
	}
	return canonicalRequest(encoded)
}

// Connection modes of an exchange.
const (
	connAuthenticated = "authenticated"
	connFresh         = "fresh"
)

// exchangeCase is one live capture.
type exchangeCase struct {
	// retired marks a table slot whose vector is gone: the run skips it and keeps its number.
	retired   bool
	name      string
	group     string
	storeType string
	// status is empty for frozen, or "provisional: <what it waits for>".
	status string
	notes  string
	// server names the server the exchange runs on (see generation.target).
	server string
	// fresh runs the exchange on a new server of its profile, alone, because
	// its answer depends on everything the server holds.
	fresh      bool
	connection string
	// username and password authenticate the connection instead of root.
	username string
	password string
	noReplay bool
	// prepare runs before the connection is made: a step through the
	// server's own API (creating a user) that is not a request.
	prepare func(ctx context.Context, g *generation, db string) error
	setup   []call
	// before runs after the setup and before the request: a step that is not
	// a request (stopping a node, waiting out a token).
	before  func(ctx context.Context, g *generation, db string) error
	request call
	// useSetupToken puts the token of the setup's last AUTH_OK into the request's
	// AuthRequest.Token before it is sent.
	useSetupToken bool

	wantOpcode transport.Opcode
	wantCode   transport.ErrorCode
	wantReason transport.AuthFailReason
	// wantMessage are stable substrings of the answer's message.
	wantMessage []string
	// wantField are top-level answer fields and the value each must have
	// (compared through Extended JSON), checked at capture so a behaviour
	// change stops the generator instead of freezing.
	wantField map[string]any

	vary      []string
	varyPaths []string
	drop      []string
	additive  []string
	absent    []string
}

// resumedTokenAttempts bounds how often a capture whose request resumes the
// setup's one-second token is retried when the token ran out first.
const resumedTokenAttempts = 5

// errResumedTokenExpired reports that the token a case minted in its setup had
// expired by the time the case presented it, so the capture measured the
// clock rather than the server.
var errResumedTokenExpired = errors.New("the token minted in the setup expired before the request presented it")

// capture runs one exchange on its server and returns its vector.
//
// A case that resumes its setup's token on the auth profile (TOKEN_TTL=1)
// races the clock: the server stamps exp as the whole unix second of
// mint-time plus one second and refuses the token once that second has passed
// (server/auth.go), so the token lives only for what is left of the second it
// was minted in, anywhere from the full second down to a few milliseconds,
// and a loaded host stretches the gap between the mint and the request past
// it. The capture is retried from a fresh setup, starting just after a second
// boundary, so a clock that ate the token costs a retry; a server that
// refuses the token every time still fails the generation.
func (g *generation) capture(ctx context.Context, number int, db string, c exchangeCase) (vector, error) {
	if !c.useSetupToken || c.wantOpcode != transport.OpAuthOK {
		return g.captureOnce(ctx, number, db, c)
	}
	var lastErr error
	for attempt := 1; attempt <= resumedTokenAttempts; attempt++ {
		if err := waitForSecondStart(ctx, time.Now, 100*time.Millisecond); err != nil {
			return vector{}, err
		}
		built, err := g.captureOnce(ctx, number, db, c)
		if !errors.Is(err, errResumedTokenExpired) {
			return built, err
		}
		lastErr = err
		g.progress.Printf("%s: attempt %d of %d: %v", c.name, attempt, resumedTokenAttempts, err)
	}
	return vector{}, fmt.Errorf("%d attempts: %w", resumedTokenAttempts, lastErr)
}

// waitForSecondStart returns once the wall clock is within the first window
// of a second, so a token minted next has most of its second left.
func waitForSecondStart(ctx context.Context, now func() time.Time, window time.Duration) error {
	for {
		sinceBoundary := time.Duration(now().Nanosecond())
		if sinceBoundary < window {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second - sinceBoundary):
		}
	}
}

// captureOnce is one attempt at capture.
func (g *generation) captureOnce(ctx context.Context, number int, db string, c exchangeCase) (vector, error) {
	target, release, err := g.target(ctx, c)
	if err != nil {
		return vector{}, err
	}
	defer release()

	if c.prepare != nil {
		if err := c.prepare(ctx, g, db); err != nil {
			return vector{}, fmt.Errorf("preparing: %w", err)
		}
	}
	conn, err := dial(ctx, target)
	if err != nil {
		return vector{}, err
	}
	defer conn.close()
	if c.connection == "" || c.connection == connAuthenticated {
		username, password := rootUser, rootPassword
		if c.username != "" {
			username, password = c.username, c.password
		}
		if err := conn.authenticateAs(username, password); err != nil {
			return vector{}, fmt.Errorf("authenticating: %w", err)
		}
	}

	var setupSteps []setupStep
	var setupToken, setupLease string
	for position, step := range c.setup {
		if step.hook != nil {
			if err := step.hook(ctx, g, db); err != nil {
				return vector{}, fmt.Errorf("setup step %d (a hook): %w", position+1, err)
			}
			continue
		}
		payload, err := withSetupToken(withSetupLease(step, setupLease), setupToken).encode()
		if err != nil {
			return vector{}, err
		}
		recorded, err := withSetupToken(withSetupLease(step, fixedString), fixedString).encode()
		if err != nil {
			return vector{}, err
		}
		answer, err := conn.roundTrip(step.op, fixedRequestID(0), payload)
		if err != nil {
			return vector{}, fmt.Errorf("setup step %d (%s): %w", position+1, step.op, err)
		}
		if err := requireSetupSuccess(step.op, answer); err != nil {
			return vector{}, fmt.Errorf("setup step %d: %w", position+1, err)
		}
		if answer.Opcode == transport.OpAuthOK {
			var authOK struct {
				Token string `bson:"token"`
			}
			if err := bson.Unmarshal(answer.Payload, &authOK); err != nil {
				return vector{}, fmt.Errorf("reading the setup AUTH_OK: %w", err)
			}
			setupToken = authOK.Token
		}
		if step.op == transport.OpQueueClaim {
			var claimed struct {
				LeaseID string `bson:"lease_id"`
			}
			if err := bson.Unmarshal(answer.Payload, &claimed); err != nil {
				return vector{}, fmt.Errorf("reading the setup claim's lease: %w", err)
			}
			setupLease = claimed.LeaseID
		}
		stepJSON, err := extendedJSON(recorded)
		if err != nil {
			return vector{}, err
		}
		setupSteps = append(setupSteps, setupStep{Opcode: uint8(step.op), OpcodeName: step.op.String(), PayloadJSON: stepJSON})
	}
	if c.before != nil {
		if err := c.before(ctx, g, db); err != nil {
			return vector{}, fmt.Errorf("preparing: %w", err)
		}
	}

	request := withSetupLease(c.request, fixedString)
	sent := withSetupLease(c.request, setupLease)
	if c.useSetupToken {
		withToken, ok := request.payload.(authRequestWithToken)
		if !ok {
			return vector{}, fmt.Errorf("useSetupToken on a %T request", request.payload)
		}
		// The recorded request carries the token normalised; the sent one
		// carries the real token.
		request.payload = withToken.with(fixedString)
		sent.payload = withToken.with(setupToken)
	}
	requestPayload, err := request.encode()
	if err != nil {
		return vector{}, err
	}
	sentPayload, err := sent.encode()
	if err != nil {
		return vector{}, err
	}
	requestID := fixedRequestID(number)
	answer, err := conn.roundTrip(request.op, requestID, sentPayload)
	if err != nil {
		return vector{}, fmt.Errorf("sending %s: %w", request.op, err)
	}

	if c.useSetupToken && c.wantOpcode == transport.OpAuthOK && answer.Opcode == transport.OpAuthFail {
		var refusal struct {
			Reason uint8 `bson:"reason"`
		}
		if err := bson.Unmarshal(answer.Payload, &refusal); err == nil && transport.AuthFailReason(refusal.Reason) == transport.AuthFailTokenExpired {
			return vector{}, errResumedTokenExpired
		}
	}

	built, err := g.record(c, requestID, request, requestPayload, answer)
	if err != nil {
		return vector{}, err
	}
	built.Setup = setupSteps
	return built, nil
}

// setupLeaseMarker, as the lease_id of a lease verb in an exchange, stands for the
// lease the exchange's last setup claim returned, which only the server mints.
// The step is sent with the real id and recorded with the normalised one, as a
// token is, and a replaying harness fills it from the setup claim's answer.
const setupLeaseMarker = "lease of the setup's last claim"

// withSetupLease returns step with its lease id, when it names the setup's
// lease, set to lease.
func withSetupLease(step call, lease string) call {
	if request, ok := step.payload.(server.LeaseRequest); ok && request.LeaseID == setupLeaseMarker {
		request.LeaseID = lease
		step.payload = request
	}
	return step
}

// withSetupToken returns step with its token, when it is an AUTH that presents
// the setup's token, set to token.
func withSetupToken(step call, token string) call {
	if withToken, ok := step.payload.(authRequestWithToken); ok {
		step.payload = withToken.with(token)
	}
	return step
}

// authRequestWithToken is a request whose token is filled at capture time.
type authRequestWithToken interface {
	with(token string) any
}

// record checks the answer against the case and builds its vector.
func (g *generation) record(c exchangeCase, requestID [transport.RequestIDLen]byte, request call, requestPayload []byte, answer *transport.Frame) (vector, error) {
	if answer.RequestID != requestID && answer.RequestID != ([transport.RequestIDLen]byte{}) {
		return vector{}, fmt.Errorf("the answer carries request ID %x, not the request's %x", answer.RequestID, requestID)
	}
	if answer.Opcode != c.wantOpcode {
		return vector{}, fmt.Errorf("%s answered %s, the case expects %s: %s", request.op, answer.Opcode, c.wantOpcode, describePayload(answer.Payload))
	}
	rules := normaliseRules{
		vary:      append(slices.Clone(varyNames), c.vary...),
		varyPaths: c.varyPaths,
		drop:      c.drop,
		replacer:  g.replacer(),
		sortTop:   answer.Opcode == transport.OpError && mapEncodedError(answer.Payload),
	}
	if c.useSetupToken {
		rules.vary = append(rules.vary, "token")
	}
	var norm normalised
	var err error
	if answer.Opcode == transport.OpGetFastResponse {
		// A status byte, then the stored document when found.
		norm.payload, err = normaliseGetFast(answer.Payload)
	} else {
		norm, err = normaliseAnswer(answer.Payload, rules)
	}
	if err != nil {
		return vector{}, err
	}

	built := vector{
		Name:       c.name,
		Group:      c.group,
		Kind:       "exchange",
		Status:     statusOf(c.status),
		Notes:      c.notes,
		StoreType:  c.storeType,
		Topology:   g.topologyOf(c.server),
		Replay:     boolPointer(!c.noReplay),
		Connection: connectionOf(c.connection),
		ServerEnv:  g.serverEnvOf(c.server),
	}

	// A raw payload (GET_FAST's, or bytes that are not BSON on purpose) has
	// no Extended JSON form; payload_hex alone carries it.
	var requestJSON json.RawMessage
	if request.payload != nil {
		requestJSON, err = extendedJSON(requestPayload)
		if err != nil {
			return vector{}, err
		}
	}
	allowed, known := allowedFieldsFor(request.op)
	if !known {
		allowed = []string{}
	}
	sent, err := topLevelNames(hex.EncodeToString(requestPayload))
	if request.payload != nil && err == nil {
		for _, name := range sent {
			if !slices.Contains(allowed, name) {
				return vector{}, fmt.Errorf("the request sends %q, which %s's request type does not allow (allowed: %v)", name, request.op, allowed)
			}
		}
	}
	built.Request = &vectorRequest{
		Opcode:        uint8(request.op),
		OpcodeName:    request.op.String(),
		RequestIDHex:  hex.EncodeToString(requestID[:]),
		PayloadJSON:   requestJSON,
		PayloadHex:    hex.EncodeToString(requestPayload),
		AllowedFields: allowed,
	}

	answerJSON := json.RawMessage(nil)
	if answer.Opcode != transport.OpGetFastResponse && len(norm.payload) > 0 {
		answerJSON, err = extendedJSON(norm.payload)
		if err != nil {
			return vector{}, err
		}
	}
	additive := slices.Clone(c.additive)
	additive = append(additive, norm.dropped...)
	if g.topologyOf(c.server) == "single" && isMutationAnswer(answer.Opcode, request.op) {
		additive = append(additive, "copies_confirmed", "replication_factor", "write_outcome")
	}
	slices.Sort(additive)
	built.Response = &vectorResponse{
		Opcode:         uint8(answer.Opcode),
		OpcodeName:     answer.Opcode.String(),
		RequestIDHex:   hex.EncodeToString(answer.RequestID[:]),
		PayloadHex:     hex.EncodeToString(norm.payload),
		PayloadJSON:    answerJSON,
		Fields:         answerFieldsFor(answer.Opcode, request.op),
		Vary:           nonNil(norm.vary),
		AdditiveFields: nonNil(slices.Compact(additive)),
		AbsentFields:   nonNil(c.absent),
	}

	message, err := answerMessage(answer)
	if err != nil {
		return vector{}, err
	}
	message = g.replacer().Replace(message)
	for _, want := range c.wantMessage {
		if !strings.Contains(message, want) {
			return vector{}, fmt.Errorf("the answer's message %q does not contain %q", message, want)
		}
	}
	if err := checkFields(norm.payload, c.wantField); err != nil {
		return vector{}, err
	}
	for _, name := range c.absent {
		if fieldPresent(norm.payload, name) {
			return vector{}, fmt.Errorf("the answer carries %q, which the case says is absent", name)
		}
	}

	switch answer.Opcode {
	case transport.OpError:
		var errorAnswer struct {
			Code         int    `bson:"code"`
			WriteOutcome string `bson:"write_outcome"`
		}
		if err := bson.Unmarshal(answer.Payload, &errorAnswer); err != nil {
			return vector{}, fmt.Errorf("reading the error answer: %w", err)
		}
		if transport.ErrorCode(errorAnswer.Code) != c.wantCode {
			return vector{}, fmt.Errorf("the error code is %d (%s), the case expects %d (%s): %s",
				errorAnswer.Code, transport.ErrorCode(errorAnswer.Code), c.wantCode, c.wantCode, message)
		}
		built.Error = &vectorError{
			Code:            errorAnswer.Code,
			Name:            transport.ErrorCode(errorAnswer.Code).String(),
			WriteOutcome:    errorAnswer.WriteOutcome,
			MessageContains: nonNil(c.wantMessage),
		}
	case transport.OpAuthFail:
		var refusal struct {
			Reason int `bson:"reason"`
		}
		if len(answer.Payload) > 0 {
			if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
				return vector{}, fmt.Errorf("reading the AUTH_FAIL answer: %w", err)
			}
		}
		if transport.AuthFailReason(refusal.Reason) != c.wantReason {
			return vector{}, fmt.Errorf("the AUTH_FAIL reason is %d, the case expects %d (%s): %s", refusal.Reason, c.wantReason, c.wantReason, message)
		}
		built.AuthFail = &vectorAuthFail{
			Reason:          refusal.Reason,
			Name:            transport.AuthFailReason(refusal.Reason).String(),
			MessageContains: nonNil(c.wantMessage),
		}
	}
	return built, nil
}

// requireSetupSuccess refuses a setup answer that is an error, a refusal, or
// an in-band success:false.
func requireSetupSuccess(op transport.Opcode, answer *transport.Frame) error {
	switch answer.Opcode {
	case transport.OpError, transport.OpAuthFail:
		return fmt.Errorf("%s was refused with %s: %s", op, answer.Opcode, describePayload(answer.Payload))
	}
	if len(answer.Payload) == 0 || answer.Opcode == transport.OpGetFastResponse {
		return nil
	}
	var inBand struct {
		Success *bool `bson:"success"`
	}
	if err := bson.Unmarshal(answer.Payload, &inBand); err != nil {
		return fmt.Errorf("reading the %s answer: %w", op, err)
	}
	if inBand.Success != nil && !*inBand.Success {
		return fmt.Errorf("%s answered success:false: %s", op, describePayload(answer.Payload))
	}
	return nil
}

// answerMessage is the message field of an answer, or "".
func answerMessage(answer *transport.Frame) (string, error) {
	if len(answer.Payload) == 0 || answer.Opcode == transport.OpGetFastResponse {
		return "", nil
	}
	var withMessage struct {
		Message string `bson:"message"`
	}
	if err := bson.Unmarshal(answer.Payload, &withMessage); err != nil {
		return "", fmt.Errorf("reading the answer's message: %w", err)
	}
	return withMessage.Message, nil
}

// checkFields compares top-level answer fields with wanted values through
// relaxed Extended JSON, so the comparison does not depend on Go types.
func checkFields(payload []byte, want map[string]any) error {
	if len(want) == 0 {
		return nil
	}
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		got, err := bson.Raw(payload).LookupErr(name)
		if err != nil {
			return fmt.Errorf("the answer has no %q; the case expects %v: %s", name, want[name], describePayload(payload))
		}
		wantValue, err := bson.Marshal(bson.M{name: want[name]})
		if err != nil {
			return fmt.Errorf("encoding the expected %q: %w", name, err)
		}
		gotValue, err := bson.Marshal(bson.D{{Key: name, Value: got}})
		if err != nil {
			return fmt.Errorf("encoding the answer's %q: %w", name, err)
		}
		wantJSON, err := bson.MarshalExtJSON(bson.Raw(wantValue), false, false)
		if err != nil {
			return fmt.Errorf("rendering the expected %q: %w", name, err)
		}
		gotJSON, err := bson.MarshalExtJSON(bson.Raw(gotValue), false, false)
		if err != nil {
			return fmt.Errorf("rendering the answer's %q: %w", name, err)
		}
		if string(wantJSON) != string(gotJSON) {
			return fmt.Errorf("the answer's %s, the case expects %s: %s", gotJSON, wantJSON, describePayload(payload))
		}
	}
	return nil
}

// fieldPresent reports whether a top-level field is present.
func fieldPresent(payload []byte, name string) bool {
	if len(payload) == 0 {
		return false
	}
	_, err := bson.Raw(payload).LookupErr(name)
	return err == nil
}

// mutationRequests are the requests whose success answers carry the write
// outcome on a cluster (handbook operations.md: the † responses).
var mutationRequests = []transport.Opcode{
	transport.OpPut, transport.OpUpsert, transport.OpBatchPut, transport.OpBatchUpsert, transport.OpFIFOPush, transport.OpLIFOPush, transport.OpHeapPush,
	transport.OpFIFOPop, transport.OpLIFOPop, transport.OpHeapPop, transport.OpDelete, transport.OpUpdate,
	transport.OpBlobWrite, transport.OpBTreeInsert,
	transport.OpFieldPush, transport.OpFieldPop, transport.OpFieldPut, transport.OpFieldDelete,
	transport.OpQueueClaim, transport.OpQueueAck, transport.OpQueueRelease, transport.OpQueueExtend,
	transport.OpDeadRequeue, transport.OpDeadDiscard,
}

// isMutationAnswer says whether an answer is a data mutation's: a success, or
// an error that a cluster stamps with the write outcome.
func isMutationAnswer(answer, request transport.Opcode) bool {
	return answer != transport.OpAuthFail && slices.Contains(mutationRequests, request)
}

// extendedJSON is canonical Extended JSON v2 of a BSON document.
func extendedJSON(payload []byte) (json.RawMessage, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	encoded, err := bson.MarshalExtJSON(bson.Raw(payload), true, false)
	if err != nil {
		return nil, fmt.Errorf("rendering Extended JSON: %w", err)
	}
	return encoded, nil
}

func statusOf(status string) string {
	if status == "" {
		return "frozen"
	}
	return status
}

func connectionOf(connection string) string {
	if connection == "" {
		return connAuthenticated
	}
	return connection
}

func boolPointer(value bool) *bool { return &value }

func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
