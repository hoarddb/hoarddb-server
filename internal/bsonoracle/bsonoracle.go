// Package bsonoracle is the vendor shim of the BSON codec differentials
// (bson-codec-spec §9.1): it encodes and decodes through the MongoDB driver's
// codec (the vendor) on a registry of its own, so a test can put one value
// through the vendor and through hoarddb-bson and compare the two.
//
// Only _test.go files import this package. It links the vendor codec, which
// must never reach cmd/server or cmd/cli (spec §9.3, O14);
// TestNoProductionPackageImportsTheShim fails on a non-test importer.
//
// Every vendor import here and in hoardDB test code carries a vendor* alias
// (vendorbson, vendorprimitive, ...). The step 6 swap script never rewrites an
// aliased import and skips this package whole, so the oracle stays the vendor
// after the server moves to the library (build doc §2, trap 1).
//
// The registry holds the vendor's defaults and, from step 3c, a codec for each
// library scalar and container type (librarytypes.go), so one library-typed
// value goes through both libraries and the results compare.
package bsonoracle

import (
	"bytes"
	"fmt"

	vendorbson "go.mongodb.org/mongo-driver/bson"
	vendorbsoncodec "go.mongodb.org/mongo-driver/bson/bsoncodec"
	vendorbsonrw "go.mongodb.org/mongo-driver/bson/bsonrw"
)

// vendorRegistry is private to the shim, so the library types later steps
// register on it never reach the vendor's DefaultRegistry, which the
// production code still uses until step 6.
var vendorRegistry = vendorbson.NewRegistry()

// encodeDocument and decodeDocument are the shim's two codec calls. They are
// variables only so TestShimRefusesToBeTheLibrary can put a library encoder in
// the shim and watch the round-trip check fail.
var (
	encodeDocument = vendorEncodeDocument
	decodeDocument = vendorDecodeDocument
)

// Registry returns the shim's vendor registry.
func Registry() *vendorbsoncodec.Registry {
	return vendorRegistry
}

// Marshal encodes goValue as a BSON document with the vendor, on the shim's
// registry.
func Marshal(goValue any) ([]byte, error) {
	return encodeDocument(goValue)
}

// Unmarshal decodes the BSON document documentBytes into target with the
// vendor, on the shim's registry. target must be a non-nil pointer.
func Unmarshal(documentBytes []byte, target any) error {
	return decodeDocument(documentBytes, target)
}

// vendorEncodeDocument is the vendor's bson.Marshal on the shim's registry,
// written through an Encoder because MarshalWithRegistry is deprecated.
func vendorEncodeDocument(goValue any) ([]byte, error) {
	var encoded bytes.Buffer
	valueWriter, err := vendorbsonrw.NewBSONValueWriter(&encoded)
	if err != nil {
		return nil, fmt.Errorf("bsonoracle: creating the vendor value writer: %w", err)
	}
	encoder, err := vendorbson.NewEncoder(valueWriter)
	if err != nil {
		return nil, fmt.Errorf("bsonoracle: creating the vendor encoder: %w", err)
	}
	if err := encoder.SetRegistry(vendorRegistry); err != nil {
		return nil, fmt.Errorf("bsonoracle: setting the vendor registry: %w", err)
	}
	if err := encoder.Encode(goValue); err != nil {
		return nil, fmt.Errorf("bsonoracle: vendor encoding of %T: %w", goValue, err)
	}
	return encoded.Bytes(), nil
}

// vendorDecodeDocument is the vendor's bson.Unmarshal on the shim's registry.
func vendorDecodeDocument(documentBytes []byte, target any) error {
	decoder, err := vendorbson.NewDecoder(vendorbsonrw.NewBSONDocumentReader(documentBytes))
	if err != nil {
		return fmt.Errorf("bsonoracle: creating the vendor decoder: %w", err)
	}
	if err := decoder.SetRegistry(vendorRegistry); err != nil {
		return fmt.Errorf("bsonoracle: setting the vendor registry: %w", err)
	}
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("bsonoracle: vendor decoding into %T: %w", target, err)
	}
	return nil
}
