package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// fakeConn is a net.Conn that records whether it was closed.
type fakeConn struct {
	closed bool
}

func (c *fakeConn) Read([]byte) (int, error)         { return 0, nil }
func (c *fakeConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *fakeConn) Close() error                     { c.closed = true; return nil }
func (c *fakeConn) LocalAddr() net.Addr              { return nil }
func (c *fakeConn) RemoteAddr() net.Addr             { return nil }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }

func TestSessionRegistryCloseSessionsFor(t *testing.T) {
	reg := NewSessionRegistry()

	alice1 := &Session{}
	alice1.SetIdentity(Identity{Username: "alice"})
	alice2 := &Session{}
	alice2.SetIdentity(Identity{Username: "alice"})
	bob := &Session{}
	bob.SetIdentity(Identity{Username: "bob"})

	c1, c2, c3 := &fakeConn{}, &fakeConn{}, &fakeConn{}
	reg.Add("alice", alice1, c1)
	reg.Add("alice", alice2, c2)
	reg.Add("bob", bob, c3)

	if closed := reg.CloseSessionsFor("alice"); closed != 2 {
		t.Fatalf("CloseSessionsFor(alice) = %d, want 2", closed)
	}
	if !c1.closed || !c2.closed {
		t.Fatal("alice's connections were not closed")
	}
	if c3.closed {
		t.Fatal("bob's connection was closed by alice's close")
	}
	if closed := reg.CloseSessionsFor("alice"); closed != 0 {
		t.Fatalf("second CloseSessionsFor(alice) = %d, want 0 (idempotent)", closed)
	}

	// Remove after a close must not panic or resurrect the entry.
	reg.Remove(alice1)
	reg.Remove(bob)
	if closed := reg.CloseSessionsFor("bob"); closed != 0 {
		t.Fatalf("bob close after Remove = %d, want 0", closed)
	}
}

func TestSessionRegistrySameSessionReplacedNotDuplicated(t *testing.T) {
	reg := NewSessionRegistry()
	sess := &Session{}
	sess.SetIdentity(Identity{Username: "alice"})
	c1, c2 := &fakeConn{}, &fakeConn{}
	reg.Add("alice", sess, c1)
	reg.Add("alice", sess, c2) // a token refresh rebinds the same session

	if closed := reg.CloseSessionsFor("alice"); closed != 1 {
		t.Fatalf("CloseSessionsFor = %d, want 1 (the session is replaced, not duplicated)", closed)
	}
	if c1.closed {
		t.Fatal("the replaced connection was closed instead of the current one")
	}
	if !c2.closed {
		t.Fatal("the current connection was not closed")
	}
}

// standingAuthHandler answers every AUTH with AUTH_OK for username under the
// given standing, and echoes any other frame.
func standingAuthHandler(username string, standing *LoginStanding) RequestHandler {
	return func(ctx context.Context, frame *Frame) (*Frame, error) {
		if frame.Opcode == OpAuth {
			if sess, ok := SessionFromContext(ctx); ok {
				sess.SetIdentity(Identity{Username: username, Standing: standing})
			}
			return NewFrameWithID(OpAuthOK, frame.RequestID, nil), nil
		}
		return NewFrameWithID(frame.Opcode, frame.RequestID, frame.Payload), nil
	}
}

// closedByTheNode reports whether conn is closed by its peer within the wait,
// with nothing arriving first. The wait is shorter than the listener's idle
// timeout, so an idle close cannot pass for the close under test.
func closedByTheNode(conn net.Conn, wait time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(wait))
	var one [1]byte
	_, err := conn.Read(one[:])
	if err == nil {
		return false
	}
	var timeout net.Error
	return !errors.As(err, &timeout) || !timeout.Timeout()
}

// TestARevocationBetweenAuthOKAndRegistrationClosesTheConnection: an account
// is dropped, disabled or given new credentials after its login was answered
// and before the session was registered. The close that change makes finds no
// session, so the listener itself must close the connection once it registers
// it; the client sends nothing more and must not be left with an open socket.
func TestARevocationBetweenAuthOKAndRegistrationClosesTheConnection(t *testing.T) {
	standing := &LoginStanding{}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{IdleTimeout: 30 * time.Second}), standingAuthHandler("alice", standing), nil)
	closedInWindow := make(chan int, 1)
	ln.SetAfterAuthOKTestHook(func() {
		// What the server does for a committed drop, in its order: revoke
		// the standing, then close the account's registered sessions.
		standing.Revoke()
		closedInWindow <- ln.CloseSessionsFor("alice")
	})

	conn := dialTestTCP(t, ln.Addr().String())
	if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
		t.Fatal(err)
	}
	if answer := mustDecode(t, conn); answer.Opcode != OpAuthOK {
		t.Fatalf("login answered %s, want AUTH_OK", answer.Opcode)
	}
	if closed := <-closedInWindow; closed != 0 {
		t.Fatalf("the close inside the window closed %d sessions, want 0: the session was already registered and this test no longer covers the window", closed)
	}
	if !closedByTheNode(conn, 3*time.Second) {
		t.Fatal("a login revoked between its AUTH_OK and its registration was left with an open connection")
	}
	// The closed connection leaves nothing behind in the registry.
	deadline := time.Now().Add(5 * time.Second)
	for {
		ln.sessions.mu.Lock()
		registered := len(ln.sessions.byUser)
		ln.sessions.mu.Unlock()
		if registered == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the registry still holds %d users after the connection was closed", registered)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestARevocationJustBeforeRegistrationClosesTheConnection: the revocation and
// its close both complete inside the registration, after everything the
// listener did ahead of it and before the session is recorded. The close
// finds nothing, so only a standing check made after the registration can end
// this connection: a check made before it has already passed.
func TestARevocationJustBeforeRegistrationClosesTheConnection(t *testing.T) {
	standing := &LoginStanding{}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{IdleTimeout: 30 * time.Second}), standingAuthHandler("alice", standing), nil)
	closedInGap := make(chan int, 1)
	ln.SetBeforeRegistrationTestHook(func() {
		standing.Revoke()
		closedInGap <- ln.CloseSessionsFor("alice")
	})

	conn := dialTestTCP(t, ln.Addr().String())
	if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
		t.Fatal(err)
	}
	if answer := mustDecode(t, conn); answer.Opcode != OpAuthOK {
		t.Fatalf("login answered %s, want AUTH_OK", answer.Opcode)
	}
	if closed := <-closedInGap; closed != 0 {
		t.Fatalf("the close just before the registration closed %d sessions, want 0", closed)
	}
	if !closedByTheNode(conn, 3*time.Second) {
		t.Fatal("a login revoked just before its registration was left with an open connection")
	}
}

// TestALoginInGoodStandingIsRegisteredAndStaysOpen: the check that closes a
// revoked login at registration leaves every other login alone, and a later
// revocation still closes it through the registry.
func TestALoginInGoodStandingIsRegisteredAndStaysOpen(t *testing.T) {
	standing := &LoginStanding{}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{IdleTimeout: 30 * time.Second}), standingAuthHandler("alice", standing), nil)
	registering := make(chan struct{}, 1)
	ln.SetAfterAuthOKTestHook(func() { registering <- struct{}{} })

	conn := dialTestTCP(t, ln.Addr().String())
	if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
		t.Fatal(err)
	}
	if answer := mustDecode(t, conn); answer.Opcode != OpAuthOK {
		t.Fatalf("login answered %s, want AUTH_OK", answer.Opcode)
	}
	<-registering
	deadline := time.Now().Add(5 * time.Second)
	for {
		ln.sessions.mu.Lock()
		registered := len(ln.sessions.byUser["alice"])
		ln.sessions.mu.Unlock()
		if registered == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a login in good standing was never registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := conn.Write(NewFrame(OpGet, []byte("still served")).Encode()); err != nil {
		t.Fatal(err)
	}
	if answer := mustDecode(t, conn); answer.Opcode != OpGet {
		t.Fatalf("a frame after registration answered %s, want the echo", answer.Opcode)
	}
	standing.Revoke()
	if closed := ln.CloseSessionsFor("alice"); closed != 1 {
		t.Fatalf("the registry closed %d sessions, want 1", closed)
	}
	if !closedByTheNode(conn, 3*time.Second) {
		t.Fatal("the registry's close did not close the connection")
	}
}
