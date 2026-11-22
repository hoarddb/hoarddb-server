package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"
)

// idGenerator issues UUIDv7-style ids: 48 bits of millisecond clock, the
// version nibble 7, a 12-bit counter, the variant bits 10 and 62 random bits.
// Every engine gets ids from the same generator, so they sort by time and a
// B-tree primary key sees the same insertion pattern everywhere. It is safe
// for concurrent use.
type idGenerator struct {
	now    func() time.Time
	random io.Reader

	mu      sync.Mutex
	lastMS  int64
	counter uint16
}

// newIDGenerator takes the clock and entropy source as arguments so a test
// can freeze both.
func newIDGenerator(now func() time.Time, random io.Reader) *idGenerator {
	return &idGenerator{now: now, random: random}
}

// next returns a new id. Ids are strictly ascending as strings: a clock that
// stands still or steps back keeps the last millisecond and counts up, and a
// counter that runs out borrows the next millisecond.
func (g *idGenerator) next() (string, error) {
	var tail [8]byte
	if _, err := io.ReadFull(g.random, tail[:]); err != nil {
		return "", fmt.Errorf("reading entropy for an id: %w", err)
	}
	g.mu.Lock()
	ms := g.now().UnixMilli()
	if ms <= g.lastMS {
		ms = g.lastMS
		g.counter++
		if g.counter > 0x0fff {
			ms++
			g.counter = 0
		}
	} else {
		g.counter = 0
	}
	g.lastMS = ms
	counter := g.counter
	g.mu.Unlock()

	var raw [16]byte
	binary.BigEndian.PutUint64(raw[:8], uint64(ms)<<16)
	raw[6] = 0x70 | byte(counter>>8)
	raw[7] = byte(counter)
	copy(raw[8:], tail[:])
	raw[8] = raw[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}
