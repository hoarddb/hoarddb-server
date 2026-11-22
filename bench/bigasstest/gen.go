package main

import (
	"hash/fnv"
	"math/rand/v2"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// writeEpoch is the instant (seconds) the times of written content count from. It is later
// than every time the model seeds, so a written element sorts after a seeded one.
const writeEpoch int64 = 1_800_000_000

// contentGen makes the content of writes: text, amounts, times and vectors. Everything is a
// function of the run seed and the write id, so every engine is given the same bytes. The
// model package keeps its vocabulary private, so run-time content has its own.
type contentGen struct {
	seed  uint64
	words []string
}

// newContentGen builds the generator of a seed.
func newContentGen(seed uint64) *contentGen {
	g := &contentGen{seed: seed}
	rng := g.rng("vocabulary", "words")
	g.words = make([]string, 2048)
	for i := range g.words {
		letters := make([]byte, 2+rng.IntN(8))
		for j := range letters {
			letters[j] = byte('a' + rng.IntN(26))
		}
		g.words[i] = string(letters)
	}
	return g
}

// rng returns the random stream of one purpose of one write id.
func (g *contentGen) rng(id, purpose string) *rand.Rand {
	digest := fnv.New64a()
	_, _ = digest.Write([]byte(id + "/" + purpose))
	return rand.New(rand.NewPCG(g.seed, digest.Sum64()))
}

// sentence returns count words for the purpose of write id.
func (g *contentGen) sentence(id, purpose string, count int) string {
	rng := g.rng(id, purpose)
	words := make([]string, count)
	for i := range words {
		words[i] = g.words[rng.IntN(len(g.words))]
	}
	return strings.Join(words, " ")
}

// vector returns the 64-value embedding of write id as its stored bytes.
func (g *contentGen) vector(id string) []byte {
	rng := g.rng(id, "vector")
	values := make([]float32, model.VectorLen)
	for i := range values {
		values[i] = rng.Float32()*2 - 1
	}
	return model.EncodeVector(values)
}

// writeAt is the time stamp of a write. It is unique to the write because the ordinal is.
func writeAt(ordinal int64) int64 { return writeEpoch + ordinal }
