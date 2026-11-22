// Package tap is the runtime oracle tap of bson-codec-spec 9.1.7 (hoardDB's
// half, build doc step 4f). Built with the tag bsonoracle, it installs
// bson.SetOracleTap, so that every Marshal, MarshalValue, Unmarshal,
// RawValue.Unmarshal and Document.Decode of the library also runs the vendor
// through the shim (hoardDB/internal/bsonoracle) and compares. A divergence is
// returned as the call's own error, which fails the test that made the call.
// It also records (Go type, bytes) pairs to $BSONORACLE_OUT.
//
// Without the tag this package has no code, so nothing of it, and nothing of
// the library's hook, is in a normal build. Only _test.go files import it
// (a blank import in a zz_bsonoracle_tap_test.go of each package the tap run
// covers), because it links the vendor codec.
package tap
