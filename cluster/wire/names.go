package wire

import (
	"fmt"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/namepolicy"
)

// echoedNameBytes is how much of a refused name a refusal repeats, so the text does not grow
// with what the client sent.
const echoedNameBytes = 64

// echoName quotes at most echoedNameBytes of name, cut on a rune boundary.
func echoName(name string) string {
	if len(name) <= echoedNameBytes {
		return fmt.Sprintf("%q", name)
	}
	cut := echoedNameBytes
	for cut > 0 && !utf8.RuneStart(name[cut]) {
		cut--
	}
	return fmt.Sprintf("%q…", name[:cut])
}

// databaseNameRefusal is the refusal for a database name the shared policy (package
// namepolicy, the rules the legacy door applies) rejects, or nil when the name is allowed.
func databaseNameRefusal(name string, storage []namepolicy.StorageDir) error {
	violation := namepolicy.CheckDatabase(name, storage)
	if violation == nil {
		return nil
	}
	switch violation.Rule {
	case namepolicy.RuleEmpty:
		return refuse(cluster.CodeInvalidArgument, "database name is empty. Run 'create database <name>' with a name of 1 to %d bytes", namepolicy.MaxBytes)
	case namepolicy.RuleReserved:
		return refuse(cluster.CodeReservedName, "database name %s is reserved for internal use (the auth master key lives there). Run 'create database <name>' with another name", echoName(name))
	case namepolicy.RuleStorageDir:
		return refuse(cluster.CodeReservedName, "%s", violation.Detail)
	case namepolicy.RuleTooLong:
		return refuse(cluster.CodeInvalidArgument, "database name %s is %d bytes and the limit is %d. Run 'create database <name>' with a shorter name", echoName(name), len(name), namepolicy.MaxBytes)
	}
	return refuse(cluster.CodeInvalidArgument, "invalid database name: %v. Run 'create database <name>' with a name that has no '/', '\\' or NUL and is not '.' or '..'", violation.Err)
}

// bucketNameRefusal is the refusal for a bucket name the shared policy rejects, or nil.
func bucketNameRefusal(dbName, name string) error {
	violation := namepolicy.CheckBucket(dbName, name)
	if violation == nil {
		return nil
	}
	if violation.Rule == namepolicy.RuleTooLong {
		return refuse(cluster.CodeInvalidArgument, "%s name %s is %d bytes and the limit is %d. Run 'create bucket <name> in %s { … }' with a shorter name",
			violation.Kind, echoName(violation.Name), len(violation.Name), namepolicy.MaxBytes, dbName)
	}
	return refuse(cluster.CodeInvalidArgument, "invalid %s name: %v. Run 'create bucket <name> in %s { … }' with a name that has no '/', '\\' or NUL and is not '.' or '..'",
		violation.Kind, violation.Err, dbName)
}
