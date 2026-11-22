package transport

import (
	"context"
	"testing"
	"time"
)

// TestTheTurnHolderIsRecorded is spec lease-renewer-cadence O5: a lease call that waits for the
// control connection's turn behind a probe reports how long it waited and that a probe held it.
func TestTheTurnHolderIsRecorded(t *testing.T) {
	release := make(chan struct{})
	probing := make(chan struct{}, 1)
	server := startChannelServer(t, ChannelControl, func(_ context.Context, _ Handshake, req Message) (*Message, error) {
		if req.Env.Kind == KindProbe {
			probing <- struct{}{}
			<-release
		}
		reply := Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindProbeReply}, Body: make([]byte, 17)}
		return &reply, nil
	})
	client := NewChannelClient(server.Addr().String(), ChannelControl, testHello("caller"))
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	probe := Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindProbe}, Body: make([]byte, 8)}
	lease := Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindLeaseReq}, Body: make([]byte, 8)}
	probeDone := make(chan error, 1)
	go func() { _, err := client.Call(ctx, probe); probeDone <- err }()
	<-probing

	type result struct {
		wait TurnWait
		err  error
	}
	leased := make(chan result, 1)
	go func() {
		_, wait, err := client.CallTimed(ctx, lease)
		leased <- result{wait, err}
	}()
	time.Sleep(60 * time.Millisecond) // the lease call is queued behind the probe
	close(release)
	got := <-leased
	if got.err != nil {
		t.Fatal(got.err)
	}
	if err := <-probeDone; err != nil {
		t.Fatal(err)
	}
	if got.wait.Holder != KindProbe || HolderName(got.wait.Holder) != "probe" {
		t.Errorf("holder = %#x (%s), want the probe", got.wait.Holder, HolderName(got.wait.Holder))
	}
	if got.wait.Waited < 50*time.Millisecond {
		t.Errorf("waited = %v, want at least the 60 ms the probe held the turn", got.wait.Waited)
	}

	_, quiet, err := client.CallTimed(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if quiet.Holder != 0 || quiet.Waited > 20*time.Millisecond {
		t.Errorf("a call that found the turn free reports %+v", quiet)
	}
}
