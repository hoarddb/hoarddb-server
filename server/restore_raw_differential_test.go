package server

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The restore differential (bson-codec-spec 7.2, build doc step 7a, path
// "restore"). A dump's documents are not this node's stored bytes, so restore
// reads them through Validate, never TrustStored (build trap 4); step 7b moved
// server/restore.go:decodeRestoreDocument onto validateAt at
// maxDocumentDepth, decoding the map from the Document. The oracle below is
// that function as it stood before (next at 8fc40221), a depth check then a
// decode into a map, with the depth scan it ran replaced by Validate at the
// same limit, since build step 9 removed the scan (the two were held equal
// until then). Over valid documents both must give the same map; over
// framed documents with changed bytes and the probe classes, both must accept
// and refuse alike.
//
// Restore reads no single field of a dumped document: the whole map becomes a
// write request's Data, and the request is decoded again by the handler that
// applies it. So the path keeps its map decode (it is not a resolver read),
// and this test pins what it accepts.

// oracleDecodeRestoreDocument is decodeRestoreDocument at 8fc40221, with
// Validate in place of the depth scan.
func oracleDecodeRestoreDocument(raw []byte) (map[string]interface{}, error) {
	if _, err := bson.Validate(raw, bson.Limits{Depth: maxDocumentDepth}); err != nil {
		return nil, fmt.Errorf("the dumped document: %w", err)
	}
	var doc map[string]interface{}
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// TestRestoreFieldReadsMatchTheMapPath compares the two decodes of a dumped
// document: the same map, by its encoding (maps encode with sorted keys, and
// a NaN is equal to itself there), for every document both accept, and the
// same answer, accept or refuse, for every document.
func TestRestoreFieldReadsMatchTheMapPath(t *testing.T) {
	documents := append([]probeDocument{}, handBuiltDocuments()...)
	documents = append(documents, existingTestDocuments(t)...)
	documents = append(documents, generatedDocuments(31, 400)...)
	documents = append(documents, longStringDocuments()...)
	stride := 5
	if raceEnabled {
		stride = 100
	}
	for position, record := range v1CorpusInputs(t) {
		if position%stride == 0 {
			documents = append(documents, record)
		}
	}
	random := rand.New(rand.NewSource(32))
	sources := len(documents)
	for _, source := range documents[:sources] {
		documents = append(documents, mutatedDocuments(source, random, 3)...)
	}
	documents = append(documents, probeInputs()...)
	documents = append(documents, probeDocument{"nesting at the limit", nestedChain(maxDocumentDepth, "doc")},
		probeDocument{"nesting past the limit", nestedChain(maxDocumentDepth+1, "doc")})

	accepted, refused := 0, 0
	for _, document := range documents {
		want, wantErr := oracleDecodeRestoreDocument(document.raw)
		got, gotErr := decodeRestoreDocument(document.raw)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("%s: the map path says %v, restore says %v", document.name, wantErr, gotErr)
		}
		if wantErr != nil {
			refused++
			continue
		}
		wantBytes, err := bson.Marshal(want)
		if err != nil {
			t.Fatalf("%s: encoding the map path's map: %v", document.name, err)
		}
		gotBytes, err := bson.Marshal(got)
		if err != nil {
			t.Fatalf("%s: encoding restore's map: %v", document.name, err)
		}
		if !bytes.Equal(wantBytes, gotBytes) {
			t.Fatalf("%s: the map path decodes %v, restore %v", document.name, want, got)
		}
		accepted++
	}
	if accepted == 0 || refused == 0 {
		t.Fatalf("%d accepted and %d refused; the comparison needs both", accepted, refused)
	}
	t.Logf("%d dumped documents: %d decoded alike, %d refused alike", len(documents), accepted, refused)
}
