package main

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/internal/bsonoracle"
)

// treeStructRows is every wire and storage struct of this package: the
// struct targets of the API inventory's marshal and unmarshal sites (item 3)
// and every struct type here that carries a bson tag (bson-codec-spec 9.1
// rows 1 and 2, build doc step 6a).
var treeStructRows = []bsonoracle.TreeStruct{
	{Name: "capture.go:token", Zero: struct {
		Token string `bson:"token"`
	}{}},
	{Name: "capture.go:lease_id", Zero: struct {
		LeaseID string `bson:"lease_id"`
	}{}},
	{Name: "capture.go:reason uint8", Zero: struct {
		Reason uint8 `bson:"reason"`
	}{}},
	{Name: "capture.go:code and write_outcome", Zero: struct {
		Code         int    `bson:"code"`
		WriteOutcome string `bson:"write_outcome"`
	}{}},
	{Name: "capture.go:reason int", Zero: struct {
		Reason int `bson:"reason"`
	}{}},
	{Name: "capture.go:requireSetupSuccess", Zero: struct {
		Success *bool `bson:"success"`
	}{}},
	{Name: "capture.go:answerMessage", Zero: struct {
		Message string `bson:"message"`
	}{}},
}

func TestTreeStructsEncodeLikeTheVendor(t *testing.T) {
	bsonoracle.EncodeLikeTheVendor(t, treeStructRows)
}

func TestTreeStructsDecodeLikeTheVendor(t *testing.T) {
	bsonoracle.DecodeLikeTheVendor(t, treeStructRows)
}
