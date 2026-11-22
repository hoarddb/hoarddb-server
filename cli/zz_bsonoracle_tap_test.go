//go:build bsonoracle

package cli

// Installs the runtime oracle tap (bson-codec-spec 9.1.7): with the
// bsonoracle tag, every library codec call made by this package's tests is
// repeated on the vendor and compared. Without the tag the file is not built.
import _ "github.com/hoarddb/hoarddb-server/internal/bsonoracle/tap"
