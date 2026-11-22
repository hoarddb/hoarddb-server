package server

import (
	"fmt"
	"strconv"
	"unicode"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// A database name, a bucket name and a field name are printed by every client
// that lists them, and a terminal acts on a control character instead of
// showing it. A name that holds one is therefore refused when a client creates
// it. Only the client-facing create paths ask (the owner's precheck, the
// coordinator's precheck and the door of a node that does not route): a name
// that already exists, an entry another node logged, a replay and a catalog
// load stay permissive, so a name made before this rule is still listed, read
// and dropped. A restore goes through the same create paths, so a dump that
// holds such a name is refused for that bucket, and says how to keep its
// documents.

// controlCharacterIn returns the first control character in name (C0, DEL and
// C1) and its byte offset.
func controlCharacterIn(name string) (rune, int, bool) {
	for offset, character := range name {
		if unicode.IsControl(character) {
			return character, offset, true
		}
	}
	return 0, 0, false
}

// controlCharacterRefusal is the sentence for a name that holds one: the thing
// (subject, which already carries the quoted name), then what to run instead.
func controlCharacterRefusal(subject, name, fix string) error {
	character, offset, _ := controlCharacterIn(name)
	return &sentencedError{cause: errCreateInputRefused, message: fmt.Sprintf(
		"%s contains a control character (U+%04X at byte %d), so it was refused. "+
			"A terminal acts on control characters when it prints a name, so use printable characters only: %s",
		subject, character, offset, fix)}
}

// checkUsername is the same rules for an account name, applied where a name
// enters (create, and the root user's seed): no control character and no more
// than maxNameBytes. An alter names an account that exists already, so it is
// not checked: an account made before the rules must stay alterable and
// droppable.
func checkUsername(username string) error {
	const fix = "create user <name> roles [reader@<database>]"
	if len(username) > maxNameBytes {
		return &sentencedError{cause: ErrUsernameInvalid, message: fmt.Sprintf(
			"Username %s is too long: it is %s bytes and the limit is %d. Use a shorter username: %s",
			echoName(username), groupThousands(len(username)), maxNameBytes, fix)}
	}
	if _, _, found := controlCharacterIn(username); found {
		refusal := controlCharacterRefusal("Username "+echoName(username), username, fix)
		return &sentencedError{message: refusal.Error(), cause: ErrUsernameInvalid}
	}
	return nil
}

func checkDatabaseNameCharacters(database string) error {
	if _, _, found := controlCharacterIn(database); found {
		return controlCharacterRefusal("Database name "+echoName(database), database, "create database <name>")
	}
	return nil
}

func checkBucketNameCharacters(database, bucket string) error {
	if err := checkDatabaseNameCharacters(database); err != nil {
		return err
	}
	if _, _, found := controlCharacterIn(bucket); found {
		return controlCharacterRefusal("Bucket name "+echoName(bucket)+" in database "+echoName(database), bucket,
			"create bucket <name> in "+echoName(database)+" { type: hash }")
	}
	return nil
}

// checkFieldNameCharacters walks a create's or a declare's field declarations,
// typed fields at every level and store fields with their nested ones, and
// refuses the first name that holds a control character. The recorded forms a
// node writes for itself are lists of Go structs, which optionList does not
// read, so they are not walked.
func checkFieldNameCharacters(bucket string, options map[string]interface{}) error {
	for _, key := range []string{"fields", "stores"} {
		if err := checkDeclarationNames(bucket, options[key]); err != nil {
			return err
		}
	}
	return checkIndexNames(bucket, options["indices"])
}

// checkIndexNames refuses a control character in an index declaration's name
// or fields, in both forms: a bare field name, and an object with field,
// fields and name. An index's name and fields reach clients in the bucket's
// statistics. The recorded form a node writes for itself is a list of
// structs, which is not walked.
func checkIndexNames(bucket string, raw interface{}) error {
	var elements []interface{}
	switch declared := raw.(type) {
	case []string:
		for _, name := range declared {
			elements = append(elements, name)
		}
	default:
		list, err := optionList(raw)
		if err != nil {
			list = []interface{}{raw}
		}
		elements = list
	}
	for _, element := range elements {
		var names []string
		switch declared := element.(type) {
		case string:
			names = []string{declared}
		default:
			object, err := optionMap(element)
			if err != nil {
				continue
			}
			for _, key := range []string{"name", "field"} {
				if text, ok := object[key].(string); ok {
					names = append(names, text)
				}
			}
			if fields, err := optionList(object["fields"]); err == nil {
				for _, field := range fields {
					if text, ok := field.(string); ok {
						names = append(names, text)
					}
				}
			}
		}
		for _, name := range names {
			if _, _, found := controlCharacterIn(name); found {
				return controlCharacterRefusal("Index name or field "+echoName(name)+" on bucket "+echoName(bucket), name,
					"name it like a document field, for example: create bucket "+echoName(bucket)+` { indices: ["Email"] }`)
			}
		}
	}
	return nil
}

func checkDeclarationNames(bucket string, raw interface{}) error {
	entries, err := optionList(raw)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		declared, err := optionMap(entry)
		if err != nil {
			continue
		}
		if name, _ := declared["name"].(string); name != "" {
			if _, _, found := controlCharacterIn(name); found {
				return controlCharacterRefusal("Field name "+echoName(name)+" on bucket "+echoName(bucket), name,
					"name it like a document field, for example: alter bucket "+echoName(bucket)+" { Items hashStore }")
			}
		}
		for _, nested := range []string{"members", "fields"} {
			if err := checkDeclarationNames(bucket, declared[nested]); err != nil {
				return err
			}
		}
		if err := checkDeclarationNames(bucket, []interface{}{declared["elem"]}); err != nil {
			return err
		}
	}
	return nil
}

// clientNameRefusal is the control-character rule for one client-facing frame:
// a create database, a create bucket or a declare of store fields. It returns
// nil for any other frame and for a payload the handler reads and refuses in
// its own words.
func clientNameRefusal(opcode transport.Opcode, payload []byte) error {
	switch opcode {
	case transport.OpCreateDB:
		var req CreateDatabaseRequest
		if bson.Unmarshal(payload, &req) != nil {
			return nil
		}
		return checkDatabaseNameCharacters(req.Name)
	case transport.OpCreateBucket:
		var req CreateBucketRequest
		if bson.Unmarshal(payload, &req) != nil {
			return nil
		}
		return createNameRefusal(req)
	case transport.OpDeclareStoreFields:
		var req DeclareStoreFieldsRequest
		if bson.Unmarshal(payload, &req) != nil {
			return nil
		}
		if err := checkBucketNameCharacters(req.Database, req.Name); err != nil {
			return err
		}
		stores := make([]interface{}, 0, len(req.Stores))
		for _, entry := range req.Stores {
			stores = append(stores, entry)
		}
		return checkFieldNameCharacters(req.Name, map[string]interface{}{"stores": stores})
	}
	return nil
}

// createNameRefusal is the rule for a create bucket: its database, its name,
// and every field it declares.
//
// Exactly one name shape is exempt from the control-character rule: a part
// name, collection ‖ 0x1F ‖ hex4(part), in a create that carries part_of (B7).
// part and part_of on any other create are refused, so a client cannot make a
// bucket that claims to be a part.
func createNameRefusal(req CreateBucketRequest) error {
	_, _, shaped := partNameParts(req.Name)
	if req.PartOf != 0 && shaped {
		if err := checkDatabaseNameCharacters(req.Database); err != nil {
			return err
		}
		return checkFieldNameCharacters(req.Name, req.Options)
	}
	if req.PartOf != 0 || req.Part != 0 {
		return &sentencedError{cause: errCreateInputRefused, message: fmt.Sprintf(
			"Bucket %s in database %s was refused: part and part_of are set by the cluster for the parts of a spread collection, and this name is not one. "+
				"Create the bucket without them: create bucket <name> in %s { type: hash }",
			echoName(displayBucket(req.Name)), echoName(req.Database), echoName(req.Database))}
	}
	if err := checkBucketNameCharacters(req.Database, req.Name); err != nil {
		return err
	}
	return checkFieldNameCharacters(req.Name, req.Options)
}

// restoreNameRefusal is the rule for a restore. A restore creates its database
// and bucket through the same door a client's create uses, so a dump that
// holds a name with a control character is refused for that bucket, before
// anything is created or dropped. The dump's files are untouched, and its
// documents can still be loaded under a printable name with ingest.
func restoreNameRefusal(database, bucket string) error {
	character, _, found := controlCharacterIn(database)
	if !found {
		character, _, found = controlCharacterIn(bucket)
	}
	if !found {
		return nil
	}
	return &sentencedError{cause: errCreateInputRefused, message: fmt.Sprintf(
		"Restore of bucket %s in database %s was refused: a name in it holds a control character (U+%04X), and hoardDB no longer creates names "+
			"that a terminal acts on when it prints them. Nothing was changed. To keep the documents, load the dump's file into a bucket "+
			"with a printable name: hoarddb ingest <dump dir>/<database>/<bucket>.bson --into <database>.<new name>",
		echoName(bucket), echoName(database), character)}
}

// inertName is name as it is, or quoted and escaped when it holds a control
// character, for a sentence that repeats a name without quoting it.
func inertName(name string) string {
	if _, _, found := controlCharacterIn(name); found {
		return strconv.Quote(name)
	}
	return name
}
