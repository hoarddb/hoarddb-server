package ringload

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

// This file is the part of ../gen.go and ../engine_hoard.go the graph needs, copied
// verbatim (the vocabulary, the tag pool, the word generator and UserID) because the harness
// is package main and cannot be imported. graph.go is ../graph.go without posts(), which this
// seeder does not use.
// TestGeneratorMatchesTheHarness here and TestRingSeedParity in ../ pin the same figures, so
// a change to either generator fails one of them.

const vocabularySize = 8192

var (
	vocabulary = buildVocabulary()
	tagPool    = buildTagPool()
)

func buildVocabulary() []string {
	rng := rand.New(rand.NewPCG(0x50c1a1, 1))
	words := make([]string, vocabularySize)
	for i := range words {
		letters := make([]byte, 2+rng.IntN(8))
		for j := range letters {
			letters[j] = byte('a' + rng.IntN(26))
		}
		words[i] = string(letters)
	}
	return words
}

func buildTagPool() []string {
	tags := make([]string, 64)
	for i := range tags {
		word := vocabulary[i*7]
		tags[i] = word[:min(len(word), 6)]
	}
	return tags
}

type postGenerator struct {
	rng *rand.Rand
}

// words returns exactly length bytes of space-separated random words.
func (g *postGenerator) words(length int) string {
	var text strings.Builder
	text.Grow(length + 10)
	for text.Len() < length {
		if text.Len() > 0 {
			text.WriteByte(' ')
		}
		text.WriteString(vocabulary[g.rng.IntN(vocabularySize)])
	}
	return text.String()[:length]
}

func UserID(rank int) string { return fmt.Sprintf("%d", rank) }

// DatasetSeed fixes the social graph; it is ../book.go's constant, so the data matches.
const DatasetSeed uint64 = 20261001
