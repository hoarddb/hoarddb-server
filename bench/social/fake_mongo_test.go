package main

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// fakeMongod is a server that speaks just enough of the wire protocol for the
// driver to connect and for one publish to run: it answers the handshake,
// answers a find of the follows collection with the followers it was given and
// acknowledges every insert, recording each command with the number of
// documents it carried. No mongod is needed, which is what the live tests
// (SOCIAL_LIVE_MONGO_URI) cannot give a unit run.
type fakeMongod struct {
	mu        sync.Mutex
	followers []int
	commands  []fakeMongoCommand
	listener  net.Listener
}

// fakeMongoCommand is one command the fake received on the test database.
type fakeMongoCommand struct {
	Name       string
	Collection string
	Documents  int
}

const (
	opQuery = 2004
	opReply = 1
	opMsg   = 2013
)

func newFakeMongod(t *testing.T, followers []int) *fakeMongod {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for the fake mongod: %v", err)
	}
	server := &fakeMongod{followers: followers, listener: listener}
	go server.accept()
	t.Cleanup(func() { _ = listener.Close() })
	return server
}

func (s *fakeMongod) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.serve(conn)
	}
}

func (s *fakeMongod) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	for {
		header := make([]byte, 16)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		length, requestID, opcode := binary.LittleEndian.Uint32(header[0:4]), binary.LittleEndian.Uint32(header[4:8]), binary.LittleEndian.Uint32(header[12:16])
		body := make([]byte, length-16)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		var reply []byte
		switch opcode {
		case opQuery:
			reply = opReplyTo(requestID, s.handshake())
		case opMsg:
			reply = opMsgTo(requestID, s.command(body))
		default:
			return
		}
		if _, err := conn.Write(reply); err != nil {
			return
		}
	}
}

func (s *fakeMongod) handshake() bson.D {
	return bson.D{{Key: "ismaster", Value: true}, {Key: "isWritablePrimary", Value: true}, {Key: "maxBsonObjectSize", Value: int32(16 << 20)},
		{Key: "maxMessageSizeBytes", Value: int32(48000000)}, {Key: "maxWriteBatchSize", Value: int32(100000)},
		{Key: "minWireVersion", Value: int32(0)}, {Key: "maxWireVersion", Value: int32(17)}, {Key: "ok", Value: 1.0}}
}

// command answers one OP_MSG: a hello, a find or an insert.
func (s *fakeMongod) command(body []byte) bson.D {
	// body: flagBits(4), then sections; section 0 is the command document and
	// section 1 a document sequence (an insert's documents).
	offset := 4
	var command bson.Raw
	sequenced := 0
	for offset < len(body) {
		kind := body[offset]
		offset++
		size := int(binary.LittleEndian.Uint32(body[offset : offset+4]))
		switch kind {
		case 0:
			command = bson.Raw(body[offset : offset+size])
		case 1:
			identifier := offset + 4
			for body[identifier] != 0 {
				identifier++
			}
			for cursor := identifier + 1; cursor < offset+size; {
				sequenced++
				cursor += int(binary.LittleEndian.Uint32(body[cursor : cursor+4]))
			}
		}
		offset += size
	}
	elements, _ := command.Elements()
	if len(elements) == 0 {
		return bson.D{{Key: "ok", Value: 0.0}}
	}
	name := elements[0].Key()
	collection, _ := elements[0].Value().StringValueOK()
	database, _ := command.Lookup("$db").StringValueOK()
	documents := sequenced
	if inline, ok := command.Lookup("documents").ArrayOK(); ok {
		inlined, _ := inline.Values()
		documents += len(inlined)
	}
	if database != "admin" {
		s.mu.Lock()
		s.commands = append(s.commands, fakeMongoCommand{Name: name, Collection: collection, Documents: documents})
		s.mu.Unlock()
	}
	switch name {
	case "find":
		batch := bson.A{}
		for _, follower := range s.followers {
			batch = append(batch, bson.D{{Key: "_id", Value: userID(follower) + ">x"}, {Key: "follower", Value: userID(follower)}})
		}
		return bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: database + "." + collection}, {Key: "firstBatch", Value: batch}}}, {Key: "ok", Value: 1.0}}
	case "insert":
		return bson.D{{Key: "n", Value: int32(documents)}, {Key: "ok", Value: 1.0}}
	default:
		return s.handshake()
	}
}

func opReplyTo(requestID uint32, document bson.D) []byte {
	encoded, _ := bson.Marshal(document)
	message := make([]byte, 16+20, 16+20+len(encoded))
	binary.LittleEndian.PutUint32(message[0:], uint32(16+20+len(encoded)))
	binary.LittleEndian.PutUint32(message[8:], requestID)
	binary.LittleEndian.PutUint32(message[12:], opReply)
	binary.LittleEndian.PutUint32(message[16:], 8) // AwaitCapable
	binary.LittleEndian.PutUint32(message[32:], 1) // one document returned
	return append(message, encoded...)
}

func opMsgTo(requestID uint32, document bson.D) []byte {
	encoded, _ := bson.Marshal(document)
	message := make([]byte, 16+5, 16+5+len(encoded))
	binary.LittleEndian.PutUint32(message[0:], uint32(16+5+len(encoded)))
	binary.LittleEndian.PutUint32(message[8:], requestID)
	binary.LittleEndian.PutUint32(message[12:], opMsg)
	return append(message, encoded...) // flag bits 0, section kind 0
}

// received returns the commands seen so far, in order.
func (s *fakeMongod) received() []fakeMongoCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.commands)
}

// newFakeMongoEngine connects the production engine type to server, with no
// handshake beyond what the driver does itself.
func newFakeMongoEngine(t *testing.T, server *fakeMongod, cfg *config) *mongoEngine {
	t.Helper()
	cfg.Engine, cfg.Database, cfg.Conc, cfg.OpTimeout = engineMongo, "social", 2, 5*time.Second
	client, err := mongo.Connect(context.Background(), options.Client().SetHosts([]string{server.listener.Addr().String()}).SetDirect(true).SetMaxPoolSize(2))
	if err != nil {
		t.Fatalf("connecting to the fake mongod: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return &mongoEngine{cfg: cfg, client: client, ids: newTimeOrderedIDs(), facts: map[string]string{}}
}
