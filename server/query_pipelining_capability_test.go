package server

import "testing"

// TestLoginAdvertisesQueryPipelining is query pipelining's step 11 (W-1): a
// login's capabilities name query_pipelining, now that every promise it makes
// to a driver holds — queries in flight on one connection run independently,
// each request gets one answer, an oversized result leaves the connection
// up, a query is abandoned when its client goes away, and a connection with
// a request in flight is never closed for idleness.
func TestLoginAdvertisesQueryPipelining(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	client := dialTCPServer(t, addr)
	login := client.login()
	if !containsString(login.Capabilities, "query_pipelining") {
		t.Fatalf("AUTH_OK advertised %v without query_pipelining", login.Capabilities)
	}
}
