package wire

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// GetStatement is the one statement that reads a document: db.<bucket>["<id>"].get()
// (queries §1). Nothing else reads one document.
type GetStatement struct {
	// Bucket is the bucket name.
	Bucket string
	// ID is the text of the id: the unescaped string when Quoted, the decimal digits otherwise.
	ID string
	// Quoted says the id was written as a string rather than an integer.
	Quoted bool
}

func statementRefusal(format string, args ...any) error {
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(format, args...)}
}

func isBucketRune(r byte, first bool) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		return true
	case r >= '0' && r <= '9':
		return !first
	}
	return false
}

// ParseGetStatement parses db.<bucket>["<id>"].get() or db.<bucket>[<integer>].get().
// It accepts exactly the text String prints, so a statement means one thing. find,
// get("<id>") and a store verb with an id are refused with the one spelling to use.
func ParseGetStatement(text string) (GetStatement, error) {
	rest, ok := strings.CutPrefix(text, "db.")
	if !ok {
		return GetStatement{}, statementRefusal("%q is not a read statement. Run 'db.<bucket>[\"<id>\"].get()' to read one document", text)
	}
	end := 0
	for end < len(rest) && isBucketRune(rest[end], end == 0) {
		end++
	}
	bucket, rest := rest[:end], rest[end:]
	if bucket == "" {
		return GetStatement{}, statementRefusal("%q names no bucket. Run 'db.<bucket>[\"<id>\"].get()' with the bucket's name after 'db.'", text)
	}
	one := fmt.Sprintf(`db.%s["<id>"].get()`, bucket)
	if verb, ok := strings.CutPrefix(rest, ".find"); ok && strings.HasPrefix(verb, "(") {
		return GetStatement{}, statementRefusal("find is not a statement: reading one document has one spelling. Run '%s' to read a document by id, or 'db.%s.list({limit: 10})' to see what exists", one, bucket)
	}
	if strings.HasPrefix(rest, ".get(") {
		return GetStatement{}, statementRefusal("get(\"<id>\") is not a statement: the id goes in the brackets, so the placement key and the element key cannot disagree. Run '%s'", one)
	}
	if !strings.HasPrefix(rest, "[") {
		return GetStatement{}, statementRefusal("%q is not a read statement. Run '%s' to read one document, or 'db.%s.list({limit: 10})' to see what exists", text, one, bucket)
	}
	id, quoted, after, err := parseStatementID(rest[1:])
	if err != nil {
		return GetStatement{}, statementRefusal("%q has a bad id: %v. Run '%s'", text, err, one)
	}
	if after != "].get()" {
		return GetStatement{}, statementRefusal("%q is not a read statement: after the id comes '].get()' and nothing else. Run '%s'", text, one)
	}
	return GetStatement{Bucket: bucket, ID: id, Quoted: quoted}, nil
}

// parseStatementID reads one id literal and returns what follows it.
func parseStatementID(rest string) (id string, quoted bool, after string, err error) {
	if strings.HasPrefix(rest, `"`) {
		var out strings.Builder
		for i := 1; i < len(rest); i++ {
			switch c := rest[i]; {
			case c == '"':
				return out.String(), true, rest[i+1:], nil
			case c == '\\' && i+1 < len(rest) && (rest[i+1] == '"' || rest[i+1] == '\\'):
				i++
				out.WriteByte(rest[i])
			case c == '\\':
				return "", false, "", fmt.Errorf(`a backslash escapes only " and \`)
			case c < 0x20:
				return "", false, "", fmt.Errorf("a control character is not allowed in an id")
			default:
				out.WriteByte(c)
			}
		}
		return "", false, "", fmt.Errorf("the string is not closed")
	}
	end := 0
	for end < len(rest) && (rest[end] == '-' || (rest[end] >= '0' && rest[end] <= '9')) {
		end++
	}
	number, err := strconv.ParseInt(rest[:end], 10, 64)
	if err != nil || strconv.FormatInt(number, 10) != rest[:end] {
		return "", false, "", fmt.Errorf("write a string in double quotes or an integer without leading zeros")
	}
	return rest[:end], false, rest[end:], nil
}

// String is the statement as ParseGetStatement accepts it.
func (s GetStatement) String() string {
	if !s.Quoted {
		return fmt.Sprintf("db.%s[%s].get()", s.Bucket, s.ID)
	}
	escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s.ID)
	return fmt.Sprintf(`db.%s["%s"].get()`, s.Bucket, escaped)
}
