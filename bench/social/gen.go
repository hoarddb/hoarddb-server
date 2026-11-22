package main

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// vocabularySize is large on purpose: a small vocabulary makes the post text
// far more compressible than real prose, and the storage layer compresses, so
// disk and cache numbers would flatter hoardDB.
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

// postGenerator builds documents for one worker. It is not safe for
// concurrent use; every worker owns one so the RNG needs no lock.
type postGenerator struct {
	rng              *rand.Rand
	textMin, textMax int
	epoch            time.Time
}

func newPostGenerator(rng *rand.Rand, textMin, textMax int) *postGenerator {
	return &postGenerator{
		rng:     rng,
		textMin: textMin,
		textMax: textMax,
		epoch:   time.Now().UTC().Truncate(time.Millisecond),
	}
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

// post builds one timeline entry. seq is the per-author sequence number the
// verifier reads back; a LIFO bucket has no keys, so the post's own key
// (author:seq) is implied and is what reply_to refers to.
func (g *postGenerator) post(author string, seq int64) map[string]interface{} {
	textLength := g.textMin + g.rng.IntN(g.textMax-g.textMin+1)
	tags := make([]string, g.rng.IntN(4))
	for i := range tags {
		tags[i] = tagPool[g.rng.IntN(len(tagPool))]
	}
	replyTo := ""
	if seq > 0 && g.rng.IntN(5) == 0 {
		replyTo = fmt.Sprintf("%s:%d", author, g.rng.Int64N(seq))
	}
	return map[string]interface{}{
		"text":       g.words(textLength),
		"author":     author,
		"seq":        seq,
		"created_at": g.epoch.Add(-time.Duration(g.rng.Int64N(int64(365 * 24 * time.Hour)))).Truncate(time.Millisecond),
		"tags":       tags,
		"likes":      int64(g.rng.IntN(5000)),
		"reply_to":   replyTo,
	}
}

// profile builds the ~300 B users-bucket document.
func (g *postGenerator) profile(userID string) map[string]interface{} {
	return map[string]interface{}{
		"_id_":      userID,
		"userid":    userID,
		"name":      g.words(14),
		"bio":       g.words(150),
		"joined":    g.epoch.Add(-time.Duration(g.rng.Int64N(int64(5 * 365 * 24 * time.Hour)))).Truncate(time.Millisecond),
		"followers": int64(g.rng.IntN(100000)),
		"following": int64(g.rng.IntN(2000)),
	}
}

// newUserPicker returns a function choosing user ranks in [0, users) with a
// Zipf distribution: rank 0 is the hottest user.
func newUserPicker(rng *rand.Rand, users int, zipfS float64) func() int {
	zipf := rand.NewZipf(rng, zipfS, 1, uint64(users-1))
	return func() int { return int(zipf.Uint64()) }
}
