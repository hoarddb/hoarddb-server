/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Modified by the hoardDB project, 2026-10-05: a child iterator that stops on
 * an error stops the merge, which reports it through Error. See
 * hoardDB/third_party/README.md.
 */

package table

import (
	"bytes"

	"github.com/dgraph-io/badger/v4/y"
)

// MergeIterator merges multiple iterators.
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	left  node
	right node
	small *node

	curKey  []byte
	reverse bool
	// err is the error a child stopped on. It stops the merge: running on
	// with the other child would return keys from past the damaged range,
	// and older versions of keys whose newer ones it could not read.
	err error
}

type node struct {
	valid bool
	key   []byte
	iter  y.Iterator
	// err is the error iter stopped on, set when valid is false.
	err error

	// The two iterators are type asserted from `y.Iterator`, used to inline more function calls.
	// Calling functions on concrete types is much faster (about 25-30%) than calling the
	// interface's function.
	merge  *MergeIterator
	concat *ConcatIterator
}

func (n *node) setIterator(iter y.Iterator) {
	n.iter = iter
	// It's okay if the type assertion below fails and n.merge/n.concat are set to nil.
	// We handle the nil values of merge and concat in all the methods.
	n.merge, _ = iter.(*MergeIterator)
	n.concat, _ = iter.(*ConcatIterator)
}

func (n *node) setKey() {
	n.err = nil
	switch {
	case n.merge != nil:
		n.valid = n.merge.Valid()
		if n.valid {
			n.key = n.merge.small.key
		} else {
			n.err = n.merge.err
		}
	case n.concat != nil:
		n.valid = n.concat.Valid()
		if n.valid {
			n.key = n.concat.Key()
		} else {
			n.err = n.concat.Error()
		}
	default:
		n.valid = n.iter.Valid()
		if n.valid {
			n.key = n.iter.Key()
		} else {
			n.err = y.IteratorError(n.iter)
		}
	}
}

func (n *node) next() {
	switch {
	case n.merge != nil:
		n.merge.Next()
	case n.concat != nil:
		n.concat.Next()
	default:
		n.iter.Next()
	}
	n.setKey()
}

func (n *node) rewind() {
	n.iter.Rewind()
	n.setKey()
}

func (n *node) seek(key []byte) {
	n.iter.Seek(key)
	n.setKey()
}

// childFailed records the error a child stopped on, if either did.
func (mi *MergeIterator) childFailed() bool {
	mi.err = mi.left.err
	if mi.err == nil {
		mi.err = mi.right.err
	}
	return mi.err != nil
}

func (mi *MergeIterator) fix() {
	if mi.childFailed() {
		return
	}
	if !mi.bigger().valid {
		return
	}
	if !mi.small.valid {
		mi.swapSmall()
		return
	}
	cmp := y.CompareKeys(mi.small.key, mi.bigger().key)
	switch {
	case cmp == 0: // Both the keys are equal.
		// In case of same keys, move the right iterator ahead.
		mi.right.next()
		if &mi.right == mi.small {
			mi.swapSmall()
		}
		mi.childFailed()
		return
	case cmp < 0: // Small is less than bigger().
		if mi.reverse {
			mi.swapSmall()
		} else { //nolint:staticcheck
			// we don't need to do anything. Small already points to the smallest.
		}
		return
	default: // bigger() is less than small.
		if mi.reverse {
			// Do nothing since we're iterating in reverse. Small currently points to
			// the bigger key and that's okay in reverse iteration.
		} else {
			mi.swapSmall()
		}
		return
	}
}

func (mi *MergeIterator) bigger() *node {
	if mi.small == &mi.left {
		return &mi.right
	}
	return &mi.left
}

func (mi *MergeIterator) swapSmall() {
	if mi.small == &mi.left {
		mi.small = &mi.right
		return
	}
	if mi.small == &mi.right {
		mi.small = &mi.left
		return
	}
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mi *MergeIterator) Next() {
	for mi.Valid() {
		if !bytes.Equal(mi.small.key, mi.curKey) {
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}

func (mi *MergeIterator) setCurrent() {
	mi.curKey = append(mi.curKey[:0], mi.small.key...)
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mi *MergeIterator) Rewind() {
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
}

// Seek brings us to element with key >= given key.
func (mi *MergeIterator) Seek(key []byte) {
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mi *MergeIterator) Valid() bool {
	return mi.small.valid && mi.err == nil
}

// Error returns the error a child iterator stopped on, or nil. Valid is false
// once a child has failed.
func (mi *MergeIterator) Error() error {
	return mi.err
}

// Key returns the key associated with the current iterator.
func (mi *MergeIterator) Key() []byte {
	return mi.small.key
}

// Value returns the value associated with the iterator.
func (mi *MergeIterator) Value() y.ValueStruct {
	return mi.small.iter.Value()
}

// Close implements y.Iterator.
func (mi *MergeIterator) Close() error {
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return y.Wrap(err1, "MergeIterator")
	}
	return y.Wrap(err2, "MergeIterator")
}

// NewMergeIterator creates a merge iterator.
func NewMergeIterator(iters []y.Iterator, reverse bool) y.Iterator {
	switch len(iters) {
	case 0:
		return nil
	case 1:
		return iters[0]
	case 2:
		mi := &MergeIterator{
			reverse: reverse,
		}
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		// Assign left iterator randomly. This will be fixed when user calls rewind/seek.
		mi.small = &mi.left
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator(
		[]y.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}
