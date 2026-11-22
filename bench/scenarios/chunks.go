package main

import "iter"

// chunks groups an iterator into slices of at most size items.
func chunks[T any](batches iter.Seq[T], size int) iter.Seq[[]T] {
	return func(yield func([]T) bool) {
		var batch []T
		for entry := range batches {
			batch = append(batch, entry)
			if len(batch) == size {
				if !yield(batch) {
					return
				}
				batch = nil
			}
		}
		if len(batch) > 0 {
			yield(batch)
		}
	}
}
