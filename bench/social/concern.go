package main

import (
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// parseWriteConcern checks -write-concern, which hoardDB takes per request:
// one, majority or all. Empty keeps the node's default for its replication
// factor, so a run that says nothing records nothing.
func parseWriteConcern(name string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "":
		return "", nil
	case "1", "one":
		return "one", nil
	case "majority":
		return "majority", nil
	case "all":
		return "all", nil
	default:
		return "", fmt.Errorf("-write-concern %q is not one, majority or all (or empty for the node's default)", name)
	}
}

// concernCaller sends the run's write concern with every hoardDB mutation, so
// a leg's durability does not depend on the node's configured default.
type concernCaller struct {
	wireCaller
	level string
}

func (c concernCaller) call(op transport.Opcode, request, response interface{}) error {
	return c.wireCaller.call(op, c.stamped(op, request), response)
}

// stamped returns request with the write concern set when the request can
// carry one. An update, a claim and an ack have no write-concern field on the
// wire, so they run at the node's default; TestWriteConcernSentOnEveryHoardMutation
// pins that list so a new mutation cannot go without one unnoticed.
func (c concernCaller) stamped(op transport.Opcode, request interface{}) interface{} {
	switch typed := request.(type) {
	case server.FieldRequest:
		if op == transport.OpFieldPut || op == transport.OpFieldPush || op == transport.OpFieldDelete {
			typed.WriteConcern = c.level
		}
		return typed
	case server.FieldPopRequest:
		typed.WriteConcern = c.level
		return typed
	case server.PushRequest:
		typed.WriteConcern = c.level
		return typed
	case server.InsertRequest:
		typed.WriteConcern = c.level
		return typed
	case server.BatchRequest:
		// One level on the envelope covers every write item, an update's
		// included; an item that named its own would be refused.
		typed.WriteConcern = c.level
		return typed
	}
	return request
}
