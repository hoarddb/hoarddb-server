package ringclient

import (
	"testing"
	"time"
)

func TestExpiresInIsTheClientsNowPlusTheDuration(t *testing.T) {
	before := time.Now()
	got := ExpiresIn(30 * time.Minute)
	after := time.Now()
	if got.Before(before.Add(30*time.Minute)) || got.After(after.Add(30*time.Minute)) {
		t.Fatalf("ExpiresIn(30m) = %v, want the client's now plus 30 minutes (between %v and %v)", got, before.Add(30*time.Minute), after.Add(30*time.Minute))
	}
}
