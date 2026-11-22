package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

func TestLoginReplyError(t *testing.T) {
	refusalText := "connection limit for 192.0.2.9 reached (64, HOARDDB_TCP_MAX_CONNS_PER_IP); retry shortly"
	refusalPayload, err := bson.Marshal(&server.ErrorResponse{Code: int(transport.ErrInternal), Message: refusalText})
	if err != nil {
		t.Fatal(err)
	}
	rateLimited, err := bson.Marshal(&server.AuthFailResponse{Reason: uint8(transport.AuthFailRateLimited), Message: "too many failed logins from this address"})
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := bson.Marshal(&server.AuthFailResponse{Reason: uint8(transport.AuthFailCredentials)})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name         string
		reply        *transport.Frame
		wantContains string
		wantAbsent   string
	}{
		{"rate limit is named and not blamed on the password", transport.NewFrame(transport.OpAuthFail, rateLimited), "RATE_LIMITED", "-password"},
		{"rate limit says no password was checked", transport.NewFrame(transport.OpAuthFail, rateLimited), "no password was checked", ""},
		{"rate limit carries the server text", transport.NewFrame(transport.OpAuthFail, rateLimited), "too many failed logins from this address", ""},
		{"credentials reason suggests the password", transport.NewFrame(transport.OpAuthFail, credentials), "-password", ""},
		{"undecodable auth failure payload is not blamed on the password", transport.NewFrame(transport.OpAuthFail, []byte{1, 2, 3}), "undecodable", "-password"},
		{"server refusal printed verbatim", transport.NewFrame(transport.OpError, refusalPayload), refusalText, "-password"},
		{"genuine auth failure suggests password", transport.NewFrame(transport.OpAuthFail, nil), "-password", ""},
		{"undecodable refusal still not blamed on password", transport.NewFrame(transport.OpError, []byte{1, 2, 3}), "undecodable", "-password"},
		{"login accepted", transport.NewFrame(transport.OpAuthOK, nil), "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := loginReplyError("host:1", tc.reply)
			if tc.wantContains == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Fatalf("error %q lacks %q", err, tc.wantContains)
			}
			if retried := errors.Is(err, errLoginRateLimited); retried != strings.Contains(tc.name, "rate limit") {
				t.Fatalf("errors.Is(err, errLoginRateLimited) = %v; only the RATE_LIMITED reason may be retried", retried)
			}
			if tc.wantAbsent != "" && strings.Contains(err.Error(), tc.wantAbsent) {
				t.Fatalf("error %q must not contain %q", err, tc.wantAbsent)
			}
		})
	}
}
