package storage

import "fmt"

// closedBatch is the batch a closed store hands out: every call says so.
type closedBatch struct{}

func (closedBatch) Set(key, value []byte) error {
	return fmt.Errorf("adding a key to a batch: %w", ErrStoreClosed)
}

func (closedBatch) Delete(key []byte) error {
	return fmt.Errorf("adding a delete to a batch: %w", ErrStoreClosed)
}

func (closedBatch) Commit(sync bool) error {
	return fmt.Errorf("committing a batch: %w", ErrStoreClosed)
}

func (closedBatch) Discard() {}

// closedSnapshot is the snapshot a closed store hands out: it holds nothing
// and every read says why.
type closedSnapshot struct{}

func (closedSnapshot) Get(key []byte) ([]byte, error) {
	return nil, &KeyError{Operation: "reading a key", Key: append([]byte(nil), key...), Cause: ErrStoreClosed}
}

func (closedSnapshot) NewIterator(opts IterOptions) KVIterator { return closedIterator{} }

func (closedSnapshot) Discard() {}

// closedIterator is never positioned, and Err says the store is closed so a
// walk cannot take the empty range for the end of the data.
type closedIterator struct{}

func (closedIterator) Seek(key []byte)        {}
func (closedIterator) Valid() bool            { return false }
func (closedIterator) Key() []byte            { return nil }
func (closedIterator) Value() ([]byte, error) { return nil, ErrStoreClosed }
func (closedIterator) Next()                  {}
func (closedIterator) Visits() int64          { return 0 }
func (closedIterator) Err() error             { return fmt.Errorf("walking the node store: %w", ErrStoreClosed) }
func (closedIterator) Close()                 {}
