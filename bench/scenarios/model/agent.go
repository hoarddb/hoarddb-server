package model

import (
	"encoding/binary"
	"fmt"
	"math"
)

// VectorLen is how many float32 values a memory's embedding holds.
const VectorLen = 64

func sessionID(rank int) string { return fmt.Sprintf("s%d", rank) }
func runID(rank int) string     { return fmt.Sprintf("run%d", rank) }

// EncodeVector packs values as little-endian float32 bytes, the body of a BSON binary of
// subtype 0 (and of a SQL BLOB).
func EncodeVector(values []float32) []byte {
	packed := make([]byte, 4*len(values))
	for i, value := range values {
		binary.LittleEndian.PutUint32(packed[4*i:], math.Float32bits(value))
	}
	return packed
}

// DecodeVector is the inverse of EncodeVector. A length that is not a multiple of 4 is an error.
func DecodeVector(packed []byte) ([]float32, error) {
	if len(packed)%4 != 0 {
		return nil, fmt.Errorf("a vector of %d bytes is not a whole number of float32 values", len(packed))
	}
	values := make([]float32, len(packed)/4)
	for i := range values {
		values[i] = math.Float32frombits(binary.LittleEndian.Uint32(packed[4*i:]))
	}
	return values, nil
}

// agentSchema is section 2.4.
func agentSchema() []BucketDecl {
	return []BucketDecl{
		{Name: "Session", Fields: decl("Owner", "string", "Messages", "fifoStore")},
		{Name: "Run", Fields: decl("Session", "string", "Scratch", "hashStore", "Tasks", "fifoStore")},
		{Name: "User", Fields: decl("Name", "string", "Memories", "hashStore")},
	}
}

// Agent density: a session page of 20 messages and a memory list of 100, each with room for a
// page offset, so every session holds twice the first and every user twice the second.
const (
	messagesPerSession = 40
	memoriesPerUser    = 200
)

// buildAgent makes 40 users with 200 memories each, 60 sessions of 40 messages and 60 runs
// with 6 scratch keys and 10 tasks, per scale step. A memory carries a 64-value vector derived
// from the seed and a text.
func buildAgent(g *Generator) ([]BucketDecl, []Unit) {
	users, sessions, runs := 40*g.scale, 60*g.scale, 60*g.scale
	words := newVocabulary(g.seed)
	b := newBuilder()
	for rank := range users {
		rng := stream(g.seed, "agent-user", rank)
		b.document("User", userID(rank), []Field{{"Name", words.sentence(rng, 2)}}, nil)
		for memory := range memoriesPerUser {
			rng := stream(g.seed, "agent-memory", rank*memoriesPerUser+memory)
			values := make([]float32, VectorLen)
			for i := range values {
				values[i] = rng.Float32()*2 - 1
			}
			b.element("User", userID(rank), "Memories", fmt.Sprintf("m%03d", memory),
				Field{"text", words.sentence(rng, 12)}, Field{"vec", EncodeVector(values)})
		}
		b.setCounters("User", userID(rank), Field{"memory_n", int64(memoriesPerUser)})
	}
	for rank := range sessions {
		rng := stream(g.seed, "agent-session", rank)
		b.document("Session", sessionID(rank), []Field{{"Owner", userID(rng.IntN(users))}}, nil)
		for message := range messagesPerSession {
			role := []string{"user", "assistant"}[message%2]
			b.element("Session", sessionID(rank), "Messages", "", Field{"role", role},
				Field{"text", words.sentence(rng, 5+rng.IntN(20))}, Field{"at", epoch + int64(rank*100+message)*10})
		}
		b.setCounters("Session", sessionID(rank), Field{"message_n", int64(messagesPerSession)})
	}
	for rank := range runs {
		rng := stream(g.seed, "agent-run", rank)
		b.document("Run", runID(rank), []Field{{"Session", sessionID(rank % sessions)}}, nil)
		for key := range 6 {
			b.element("Run", runID(rank), "Scratch", fmt.Sprintf("k%d", key), Field{"value", words.sentence(rng, 4)})
		}
		for task := range 10 {
			b.element("Run", runID(rank), "Tasks", "", Field{"task", fmt.Sprintf("t%d", task)}, Field{"payload", words.sentence(rng, 8)})
		}
		b.setCounters("Run", runID(rank), Field{"task_n", int64(10)})
	}
	return agentSchema(), b.finish()
}
