// Package namepolicy is the one definition of which database and bucket names hoardDB
// accepts. The legacy door (package server) and the entry-ring door (package cluster/wire)
// both call it, so the two cannot disagree about a name. It imports nothing from hoardDB,
// which is what lets both of them import it without a cycle.
package namepolicy

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// MaxBytes is the longest database name and the longest bucket name, in UTF-8 bytes.
const MaxBytes = 255

// ReservedDatabase is the database name that must never be created: the auth master key
// lives under it.
const ReservedDatabase = "_system"

// ErrUnsafeComponent marks a name that could traverse as a path component.
var ErrUnsafeComponent = errors.New("restore path escapes the data directory")

// Rule names the rule a name broke.
type Rule int

// The rules, in the order CheckDatabase applies them.
const (
	// RuleEmpty is an empty database name.
	RuleEmpty Rule = iota + 1
	// RuleReserved is the reserved database name.
	RuleReserved
	// RuleStorageDir is a database named like one of the node's storage directories.
	RuleStorageDir
	// RuleTooLong is a name over MaxBytes.
	RuleTooLong
	// RuleUnsafe is a name with a NUL, an absolute path, a separator, "." or "..".
	RuleUnsafe
)

// StorageDir is a name the node's own storage uses, with the full refusal text to give a
// database that takes it. The caller owns the text because it names the configured path.
type StorageDir struct {
	Name    string
	Refusal string
}

// Violation is one broken rule.
type Violation struct {
	Rule Rule
	// Kind is "database" or "bucket": which name broke the rule.
	Kind string
	// Name is the offending name; Database is the database a bucket name was checked in.
	Name     string
	Database string
	// Detail is the rule's own sentence for RuleStorageDir (the StorageDir.Refusal) and
	// RuleUnsafe (the wrapped ErrUnsafeComponent text).
	Detail string
	// Err is the error behind RuleUnsafe, wrapping ErrUnsafeComponent.
	Err error
}

// IsReservedDatabase reports whether name is the reserved database name.
func IsReservedDatabase(name string) bool {
	return name == ReservedDatabase
}

// PathComponent rejects a name that is unsafe as a single path component: empty, a NUL
// byte, an absolute path, a path separator, or "." / "..". kind is "database" or "bucket".
func PathComponent(kind, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: %s name is empty", ErrUnsafeComponent, kind)
	case strings.ContainsRune(name, 0):
		return fmt.Errorf("%w: %s name %q contains a NUL byte", ErrUnsafeComponent, kind, name)
	case filepath.IsAbs(name):
		return fmt.Errorf("%w: %s name %q is an absolute path", ErrUnsafeComponent, kind, name)
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf("%w: %s name %q contains a path separator", ErrUnsafeComponent, kind, name)
	case name == "." || name == "..":
		return fmt.Errorf("%w: %s name %q is a reserved path component", ErrUnsafeComponent, kind, name)
	}
	return nil
}

// CheckDatabase applies every database-name rule and returns the first one broken, or nil.
func CheckDatabase(name string, storage []StorageDir) *Violation {
	if name == "" {
		return &Violation{Rule: RuleEmpty, Kind: "database", Name: name}
	}
	if IsReservedDatabase(name) {
		return &Violation{Rule: RuleReserved, Kind: "database", Name: name}
	}
	for _, dir := range storage {
		if name == dir.Name {
			return &Violation{Rule: RuleStorageDir, Kind: "database", Name: name, Detail: dir.Refusal}
		}
	}
	if len(name) > MaxBytes {
		return &Violation{Rule: RuleTooLong, Kind: "database", Name: name}
	}
	if err := PathComponent("database", name); err != nil {
		return &Violation{Rule: RuleUnsafe, Kind: "database", Name: name, Detail: err.Error(), Err: err}
	}
	return nil
}

// CheckBucket applies every bucket-name rule to bucket in database, the database first, and
// returns the first rule broken, or nil. The database is checked only for being a safe,
// short name: a database already created has passed CheckDatabase.
func CheckBucket(database, bucket string) *Violation {
	if err := PathComponent("database", database); err != nil {
		return &Violation{Rule: RuleUnsafe, Kind: "database", Name: database, Detail: err.Error(), Err: err}
	}
	if err := PathComponent("bucket", bucket); err != nil {
		return &Violation{Rule: RuleUnsafe, Kind: "bucket", Name: bucket, Database: database, Detail: err.Error(), Err: err}
	}
	if len(database) > MaxBytes {
		return &Violation{Rule: RuleTooLong, Kind: "database", Name: database}
	}
	if len(bucket) > MaxBytes {
		return &Violation{Rule: RuleTooLong, Kind: "bucket", Name: bucket, Database: database}
	}
	return nil
}
