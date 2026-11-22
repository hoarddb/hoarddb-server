package main

import (
	"context"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringReads is the part of the ring client the adapter's read paths call. It exists so a test
// can count the calls an operation makes; the real implementation is clientReads.
type ringReads interface {
	// StoreList reads one page of a store of the entity id in bucket.
	StoreList(ctx context.Context, bucket, id, store string, limit int, after []byte) (ringclient.ElementPage, error)
	// GetFields reads the named fields of one document.
	GetFields(ctx context.Context, bucket, id string, fields []string) (bson.Raw, error)
	// GetMany reads the same fields of many documents of one bucket in one call and answers
	// every id in order. The implementation splits a long list into frames; the adapter does not.
	// The returned error is a failure of the whole call; a document's own failure is its answer's Err.
	GetMany(ctx context.Context, bucket string, ids, fields []string) ([]ringAnswer, error)
}

// ringAnswer is one document's answer to GetMany. A document that does not exist is
// Found == false with no error, as the client reports it.
type ringAnswer struct {
	Doc   bson.Raw
	Found bool
	Err   error
}

// ringGetManyMax is the most keys one get-many frame carries.
const ringGetManyMax = 255

// ringGetManyFrames is how many frames the client sends for n ids: one per ringGetManyMax,
// the round trips a get-many read is charged.
func ringGetManyFrames(n int) int { return (n + ringGetManyMax - 1) / ringGetManyMax }

// clientReads serves ringReads from a ringclient connection.
type clientReads struct {
	client   *ringclient.Client
	database string
}

func (c clientReads) StoreList(ctx context.Context, bucket, id, store string, limit int, after []byte) (ringclient.ElementPage, error) {
	return c.client.Bucket(c.database, bucket).StoreList(ctx, id, store, limit, after)
}

func (c clientReads) GetFields(ctx context.Context, bucket, id string, fields []string) (bson.Raw, error) {
	return c.client.Bucket(c.database, bucket).GetFields(ctx, id, fields)
}

func (c clientReads) GetMany(ctx context.Context, bucket string, ids, fields []string) ([]ringAnswer, error) {
	got, err := c.client.Bucket(c.database, bucket).GetMany(ctx, ids, fields)
	if err != nil {
		return nil, err
	}
	answers := make([]ringAnswer, len(got))
	for i, answer := range got {
		answers[i] = ringAnswer{Doc: answer.Doc, Found: answer.Found, Err: answer.Err}
	}
	return answers, nil
}
