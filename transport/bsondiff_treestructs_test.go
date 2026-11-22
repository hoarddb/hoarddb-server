package transport

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/internal/bsonoracle"
)

// treeStructRows is every wire and storage struct of this package: the
// struct targets of the API inventory's marshal and unmarshal sites (item 3)
// and every struct type here that carries a bson tag (bson-codec-spec 9.1
// rows 1 and 2, build doc step 6a).
var treeStructRows = []bsonoracle.TreeStruct{
	{Name: "transport/serve.go:authFailReasonOf", Zero: struct {
		Reason uint8 `bson:"reason"`
	}{}},
	{Name: "transport/tcp.go:(*TCPListener).serveConn", Zero: struct {
		Reason  uint8  `bson:"reason"`
		Message string `bson:"message,omitempty"`
	}{}},
}

func TestTreeStructsEncodeLikeTheVendor(t *testing.T) {
	bsonoracle.EncodeLikeTheVendor(t, treeStructRows)
}

func TestTreeStructsDecodeLikeTheVendor(t *testing.T) {
	bsonoracle.DecodeLikeTheVendor(t, treeStructRows)
}
