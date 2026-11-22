package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

const ringComposeDB = "app"

// ringComposeServer starts the real server on loopback port 0, logs in as the bootstrap
// root, creates one database and bucket THROUGH the wire (RING_ADMIN: no seeding through the
// node), and returns the service, the bucket's header values as the reply told them and the
// logged-in connection.
func ringComposeServer(t *testing.T) (*Service, wire.ClientBucket, *tcpServerClient) {
	t.Helper()
	qs, addr := newTCPEnabledServer(t, nil)
	if qs.ringNode == nil {
		t.Fatal("the server opened no entry-ring node: a client can reach no ring opcode")
	}
	client := dialTCPServer(t, addr)
	client.login()
	ringAdmin(t, client, wire.RingAdminRequest{Cmd: wire.AdminCreateDatabase, Name: ringComposeDB})
	created := ringAdmin(t, client, wire.RingAdminRequest{Cmd: wire.AdminCreateBucket, Database: ringComposeDB, Name: "User",
		Fields: []wire.RingAdminField{{Name: "Name", Type: "string"}, {Name: "Purchases", Type: "fifoStore"}, {Name: "Friends", Type: "hashStore"}}})
	bucket := wire.ClientBucket{BucketID: created.BucketID, DeclVersion: created.DeclVersion,
		SchemaVersionSeen: created.SchemaVersionSeen, FieldIDs: map[string]uint16{}}
	for _, field := range created.Fields {
		bucket.FieldIDs[field.Name] = field.ID
	}
	return qs, bucket, client
}

// ringAdminFrame sends one RING_ADMIN command and returns the frame that answers it.
func ringAdminFrame(t *testing.T, c *tcpServerClient, req wire.RingAdminRequest) *transport.Frame {
	t.Helper()
	body, err := wire.EncodeRingAdmin(req)
	if err != nil {
		t.Fatal(err)
	}
	return c.ringSend(transport.OpRingAdmin, body)
}

// ringAdmin sends one RING_ADMIN command and returns its reply, failing on a refusal.
func ringAdmin(t *testing.T, c *tcpServerClient, req wire.RingAdminRequest) wire.RingAdminReply {
	t.Helper()
	frame := ringAdminFrame(t, c, req)
	if frame.Opcode != transport.OpRingReply {
		t.Fatalf("%s was answered %s %q, want RING_REPLY", req.Cmd, frame.Opcode, frame.Payload)
	}
	reply, err := wire.DecodeRingAdminReply(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

// ringSend writes one ring frame and returns the frame that answers it.
func (c *tcpServerClient) ringSend(op transport.Opcode, payload []byte) *transport.Frame {
	c.t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(20 * time.Second))
	id := [16]byte{0xCC, byte(len(payload))}
	if _, err := c.conn.Write(transport.NewFrameWithID(op, id, payload).Encode()); err != nil {
		c.t.Fatal(err)
	}
	frame, err := transport.DecodeFrame(c.br)
	if err != nil {
		c.t.Fatal(err)
	}
	return frame
}

func ringTyped(text string) []byte {
	out := append([]byte{0x02}, binary.LittleEndian.AppendUint32(nil, uint32(len(text)+1))...)
	return append(append(out, text...), 0)
}

func ringKeyed(t *testing.T, c *tcpServerClient, req wire.RingKeyedRequest) wire.RingReply {
	t.Helper()
	payload, err := wire.EncodeRingKeyed(req)
	if err != nil {
		t.Fatal(err)
	}
	frame := c.ringSend(transport.OpRingKeyed, payload)
	if frame.Opcode != transport.OpRingReply {
		t.Fatalf("a ring request was answered %s %q, want RING_REPLY", frame.Opcode, frame.Payload)
	}
	reply, err := wire.DecodeRingReply(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

func ringHeader(b wire.ClientBucket, class byte, id string) cluster.Op {
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		panic(err) // test-only: an id this package wrote itself
	}
	return cluster.Op{BucketID: b.BucketID, DeclVersion: b.DeclVersion, SchemaVersionSeen: b.SchemaVersionSeen,
		Class: class, EncID: encoded}
}

func ringItem(t *testing.T, label string) []byte {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "item", Value: label}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestRingOpcodesWorkThroughTheRunningServer is the first-five-minutes path over the real
// TLS listener: AUTH as the bootstrap root, then insert, get, fifo push and pop, hash put
// and take, with a legacy opcode on the same connection afterwards.
func TestRingOpcodesWorkThroughTheRunningServer(t *testing.T) {
	_, bucket, c := ringComposeServer(t)

	insert := ringHeader(bucket, cluster.OpInsert, "ada")
	insert.IDValue = ringTyped("ada")
	insert.Fields = []cluster.FieldOp{{FieldID: bucket.FieldIDs["Name"], Value: ringTyped("Ada")}}
	if reply := ringKeyed(t, c, wire.RingKeyedRequest{Op: insert}); reply.WriteOutcome != "applied" {
		t.Fatalf("insert reply %+v", reply)
	}
	read := wire.RingRead{Class: wire.RingClassGet, BucketID: bucket.BucketID, DeclVersion: bucket.DeclVersion,
		SchemaVersionSeen: bucket.SchemaVersionSeen, EncID: insert.EncID}
	got := ringKeyed(t, c, wire.RingKeyedRequest{Read: &read})
	if got.Found == nil || !*got.Found || len(got.Elements) != 1 || bson.Raw(got.Elements[0]).Lookup("Name").StringValue() != "Ada" {
		t.Fatalf("get reply %+v", got)
	}

	arg, err := wire.ElementArg(ringItem(t, "coffee"))
	if err != nil {
		t.Fatal(err)
	}
	push := ringHeader(bucket, cluster.OpStoreVerb, "ada")
	push.Fields = []cluster.FieldOp{{FieldID: bucket.FieldIDs["Purchases"], Verb: cluster.VerbPush, Value: arg}}
	if reply := ringKeyed(t, c, wire.RingKeyedRequest{Op: push}); reply.WriteOutcome != "applied" {
		t.Fatalf("push reply %+v", reply)
	}
	pop := ringHeader(bucket, cluster.OpStoreVerb, "ada")
	pop.Fields = []cluster.FieldOp{{FieldID: bucket.FieldIDs["Purchases"], Verb: cluster.VerbPop}}
	popped := ringKeyed(t, c, wire.RingKeyedRequest{Op: pop})
	if popped.Found == nil || !*popped.Found || len(popped.Elements) != 1 || !bytes.Equal(popped.Elements[0], ringItem(t, "coffee")) {
		t.Fatalf("pop reply %+v", popped)
	}

	put, err := wire.PutArg("bob", ringItem(t, "friend"))
	if err != nil {
		t.Fatal(err)
	}
	friends := ringHeader(bucket, cluster.OpStoreVerb, "ada")
	friends.Fields = []cluster.FieldOp{{FieldID: bucket.FieldIDs["Friends"], Verb: cluster.VerbPut, Value: put}}
	if reply := ringKeyed(t, c, wire.RingKeyedRequest{Op: friends}); reply.WriteOutcome != "applied" {
		t.Fatalf("put reply %+v", reply)
	}
	take, err := wire.TakeKeyArg("bob")
	if err != nil {
		t.Fatal(err)
	}
	friends.Fields = []cluster.FieldOp{{FieldID: bucket.FieldIDs["Friends"], Verb: cluster.VerbTake, Value: take}}
	taken := ringKeyed(t, c, wire.RingKeyedRequest{Op: friends})
	if taken.Found == nil || !*taken.Found || len(taken.Elements) != 1 || !bytes.Equal(taken.Elements[0], ringItem(t, "friend")) {
		t.Fatalf("take reply %+v", taken)
	}

	if frame := c.ringSend(transport.OpPing, nil); frame.Opcode == transport.OpError {
		t.Fatalf("a legacy opcode on the same connection answered %s", frame.Opcode)
	}
}

// TestRingNodeIsConfiguredFromTheServersOwnSettings: the node lives in its own directory
// under data_dir, takes the table-sync mode from storage.badger_table_sync and its name from
// server.node_id, and adds no setting.
func TestRingNodeIsConfiguredFromTheServersOwnSettings(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = "/data"
	cfg.Storage.BadgerTableSync = config.TableSyncStepwise
	cfg.Server.NodeID = "node-a"
	got := ringNodeConfig(cfg)
	if got.Dir != "/data/ring" || got.TableSync != config.TableSyncStepwise || got.NodeID != "node-a" {
		t.Fatalf("ring node config %+v", got)
	}
	if got.Dir == filepath.Join(cfg.Storage.DataDir, nodeStoreDirName) {
		t.Fatal("the ring node shares the legacy store's directory")
	}
}

// TestRingNodeClosesWithTheServerAndKeepsItsDataAcrossARestart: Stop releases the node's
// directory, so the same data_dir starts again and still holds the bucket.
func TestRingNodeClosesWithTheServerAndKeepsItsDataAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	first, _ := newTCPEnabledServer(t, func(cfg *config.Config) { cfg.Storage.DataDir = dir })
	ctx := context.Background()
	if _, err := first.ringNode.CreateDatabase(ctx, ringComposeDB); err != nil {
		t.Fatal(err)
	}
	if _, err := first.ringNode.CreateBucket(ctx, ringComposeDB, "User", wire.Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}
	if first.ringNode != nil {
		t.Fatal("Stop left the ring node open")
	}
	second := newCapacityService(t, func(cfg *config.Config) {
		cfg.Storage.DataDir = dir
		cfg.TCP.Listen = "127.0.0.1:0"
	}, nil)
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Stop() })
	if _, err := second.ringNode.ClientBucket(ctx, ringComposeDB, "User"); err != nil {
		t.Fatalf("the restarted server lost the bucket: %v", err)
	}
}

// TestAStartThatFailsAfterTheRingNodeOpenedClosesIt: a bind failure after the node opened
// must not leave its directory locked for the retry.
func TestAStartThatFailsAfterTheRingNodeOpenedClosesIt(t *testing.T) {
	taken := listenLoopback(t, "internode")
	// Held to the end of the test: an unreferenced listener is closed by the garbage collector.
	t.Cleanup(func() { _ = taken.Close() })
	dir := t.TempDir()
	qs := newCapacityService(t, func(cfg *config.Config) {
		cfg.Storage.DataDir = dir
		cfg.TCP.Listen = "127.0.0.1:0"
		cfg.TCP.InternodeListen = taken.Addr().String()
	}, nil)
	if err := qs.Start(); err == nil {
		t.Fatal("Start bound an internode address that was taken")
	}
	if qs.ringNode != nil {
		t.Fatal("a failed Start left the ring node open")
	}
	node, err := wire.OpenNode(context.Background(), ringNodeConfig(qs.config))
	if err != nil {
		t.Fatalf("the ring directory is still locked after a failed Start: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestRingOpcodesAreDeniedToAUserWithNoGrants: the composed handler asks the server's own
// authorizer, so a user who holds no grant gets PERMISSION_DENIED, not an answer.
func TestRingOpcodesAreDeniedToAUserWithNoGrants(t *testing.T) {
	qs, bucket, _ := ringComposeServer(t)
	credential, err := NewCredential("pw-for-nobody", DefaultArgon2Params())
	if err != nil {
		t.Fatal(err)
	}
	if err := qs.core.auth.CreateUser("nobody", credential, nil, true); err != nil {
		t.Fatal(err)
	}
	addr := qs.tcpListener.Addr().String()
	c := dialTCPServer(t, addr)
	var auth AuthResponse
	if frame := c.call(transport.OpAuth, &LoginRequest{Username: "nobody", Password: "pw-for-nobody"}, &auth); frame.Opcode != transport.OpAuthOK {
		t.Fatalf("login answered %s", frame.Opcode)
	}
	read := wire.RingRead{Class: wire.RingClassGet, BucketID: bucket.BucketID, DeclVersion: bucket.DeclVersion,
		SchemaVersionSeen: bucket.SchemaVersionSeen, EncID: ringHeader(bucket, cluster.OpInsert, "ada").EncID}
	payload, err := wire.EncodeRingKeyed(wire.RingKeyedRequest{Read: &read})
	if err != nil {
		t.Fatal(err)
	}
	frame := c.ringSend(transport.OpRingKeyed, payload)
	if frame.Opcode != transport.OpError {
		t.Fatalf("a user with no grants was answered %s", frame.Opcode)
	}
	refusal, err := wire.DecodeRingError(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if refusal.Code != cluster.CodePermissionDenied {
		t.Fatalf("code %s (%q), want PERMISSION_DENIED", refusal.Code, refusal.Message)
	}
}

// TestU76RingNodeRefusesTheStorageDirectoryNames: [GUARD the ring node is told the same
// storage-directory names the legacy door reserves, so create_database over RING_ADMIN
// refuses them].
func TestU76RingNodeRefusesTheStorageDirectoryNames(t *testing.T) {
	qs, _ := newTCPEnabledServer(t, nil)
	for _, name := range []string{nodeStoreDirName, filepath.Base(qs.core.blobRoot()), "_system"} {
		if _, err := qs.ringNode.CreateDatabase(context.Background(), name); err == nil {
			t.Errorf("the ring node created database %q, which the legacy door refuses", name)
		}
	}
}
