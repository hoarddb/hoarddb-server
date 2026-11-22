package store

import (
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/internal/bsonoracle"
)

// treeStructRows is every wire and storage struct of this package: the
// struct targets of the API inventory's marshal and unmarshal sites (item 3)
// and every struct type here that carries a bson tag (bson-codec-spec 9.1
// rows 1 and 2, build doc step 6a).
var treeStructRows = []bsonoracle.TreeStruct{
	{Name: "store.BucketState", Zero: BucketState{}},
	{Name: "store.Refusal", Zero: Refusal{}},
	{Name: "store.blobEnumeratedEntry", Zero: blobEnumeratedEntry{}},
	{Name: "store.blobMeta", Zero: blobMeta{}},
	{Name: "store.deadRecord", Zero: deadRecord{}},
	{Name: "store/heap.go:heapEntryDocument", Zero: struct {
		ID       string   `bson:"_id_"`
		Priority int64    `bson:"priority"`
		Value    bson.Raw `bson:"value"`
	}{}},
}

func TestTreeStructsEncodeLikeTheVendor(t *testing.T) {
	bsonoracle.EncodeLikeTheVendor(t, treeStructRows)
}

func TestTreeStructsDecodeLikeTheVendor(t *testing.T) {
	bsonoracle.DecodeLikeTheVendor(t, treeStructRows)
}
