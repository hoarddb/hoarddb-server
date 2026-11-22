package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// ringLiteralDoc parses a put whose document is literal and returns that document.
// The step's tests reach the value through Parse, so they need no name the
// parser has not added yet.
func ringLiteralDoc(t *testing.T, literal string) Value {
	t.Helper()
	st, err := Parse(`db.User["ada"].Friends.put("g", `+literal+`)`, "app")
	if err != nil {
		t.Fatalf("Parse of %s: %v", literal, err)
	}
	for _, a := range st.Args {
		if a.Kind == ArgDoc && a.Doc.Kind == ValueDoc {
			return a.Doc
		}
		for _, v := range a.Values {
			if v.Kind == ValueDoc {
				return v
			}
		}
	}
	t.Fatalf("no document in the statement for %s", literal)
	return Value{}
}

// ringBinary finds the first binary in a literal, as ringBSON reads it.
func ringBinary(v Value) (primitive.Binary, bool) {
	if b, ok := ringBSON(v).(primitive.Binary); ok {
		return b, true
	}
	for _, entry := range v.Entries {
		if b, ok := ringBinary(entry.Value); ok {
			return b, true
		}
	}
	for _, element := range v.Elems {
		if b, ok := ringBinary(element); ok {
			return b, true
		}
	}
	return primitive.Binary{}, false
}

// TestParseBinaryLiteral: {"$binary": {"base64": ..., "subType": ...}} is a binary
// and every wrong body is refused with the fix in the message.
func TestParseBinaryLiteral(t *testing.T) {
	cases := []struct {
		name       string
		literal    string
		wantBytes  []byte
		wantSubtyp byte
		wantErr    string
		wantBinary bool
	}{
		{
			name:       "subtype 00",
			literal:    `{Vector: {"$binary": {"base64": "AACAPwAAAAA=", "subType": "00"}}}`,
			wantBytes:  []byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0},
			wantSubtyp: 0,
			wantBinary: true,
		},
		{
			name:       "subtype 05",
			literal:    `{Vector: {"$binary": {"base64": "AACAPwAAAAA=", "subType": "05"}}}`,
			wantBytes:  []byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0},
			wantSubtyp: 5,
			wantBinary: true,
		},
		{
			name:    "base64 is not standard",
			literal: `{Vector: {"$binary": {"base64": "!!", "subType": "00"}}}`,
			wantErr: "base64 is not standard base64",
		},
		{
			name:    "subType is one digit",
			literal: `{Vector: {"$binary": {"base64": "AA==", "subType": "0"}}}`,
			wantErr: "subType is not two hex digits",
		},
		{
			name:    "a missing key",
			literal: `{Vector: {"$binary": {"base64": "AA=="}}}`,
			wantErr: "it needs exactly the keys base64 and subType",
		},
		{
			name:    "an extra key",
			literal: `{Vector: {"$binary": {"base64": "AA==", "subType": "00", "x": 1}}}`,
			wantErr: "it needs exactly the keys base64 and subType",
		},
		{
			name:       "$binary beside another key is an ordinary document",
			literal:    `{Vector: {"$binary": 1, "x": 2}}`,
			wantBinary: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantErr != "" {
				_, err := Parse(`db.User["ada"].Friends.put("g", `+tc.literal+`)`, "app")
				if err == nil {
					t.Fatalf("accepted %s, want a refusal containing %q", tc.literal, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("refusal %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			doc := ringLiteralDoc(t, tc.literal)
			got, found := ringBinary(doc)
			if found != tc.wantBinary {
				t.Fatalf("binary found %v, want %v", found, tc.wantBinary)
			}
			if !tc.wantBinary {
				return
			}
			if got.Subtype != tc.wantSubtyp || !bytes.Equal(got.Data, tc.wantBytes) {
				t.Fatalf("binary subtype %d data %v, want subtype %d data %v", got.Subtype, got.Data, tc.wantSubtyp, tc.wantBytes)
			}
		})
	}
}

// TestBinaryLiteralRendersAndParsesBack: a binary prints as the literal it was
// written as, two-digit subtype included, and that text parses back to the same bytes.
func TestBinaryLiteralRendersAndParsesBack(t *testing.T) {
	for _, subtype := range []string{"00", "05"} {
		literal := `{"$binary": {"base64": "AACAPwAAAAA=", "subType": "` + subtype + `"}}`
		doc := ringLiteralDoc(t, `{Vector: `+literal+`}`)
		vector := doc.Entries[0].Value
		rendered := ringRenderValue(vector)
		if rendered != literal {
			t.Fatalf("rendered %s, want %s", rendered, literal)
		}
		back := ringLiteralDoc(t, `{Vector: `+rendered+`}`)
		want, ok := ringBinary(doc)
		if !ok {
			t.Fatalf("no binary in %s", literal)
		}
		got, ok := ringBinary(back)
		if !ok {
			t.Fatalf("no binary after rendering %s back", rendered)
		}
		if got.Subtype != want.Subtype || !bytes.Equal(got.Data, want.Data) {
			t.Fatalf("parsed back subtype %d data %v, want subtype %d data %v", got.Subtype, got.Data, want.Subtype, want.Data)
		}
	}
}

// TestBinaryLiteralReachesBSON: the parsed binary is the primitive.Binary the
// driver writes, so the field is stored, not dropped.
func TestBinaryLiteralReachesBSON(t *testing.T) {
	doc := ringLiteralDoc(t, `{Vector: {"$binary": {"base64": "AACAPwAAAAA=", "subType": "00"}}}`)
	got, ok := ringBSON(doc.Entries[0].Value).(primitive.Binary)
	if !ok {
		t.Fatalf("ringBSON of the vector is not a primitive.Binary")
	}
	want := primitive.Binary{Subtype: 0, Data: []byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0}}
	if got.Subtype != want.Subtype || !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("ringBSON gave subtype %d data %v, want subtype %d data %v", got.Subtype, got.Data, want.Subtype, want.Data)
	}
}
