package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// TestReceivedWordsReplaceTypeLabel is spec §5.4's two-codes rule made one
// vocabulary: the operand refusals on an undeclared field use the
// <received> words of spec §6.2, the words a declared field's refusal will
// use, and not the words typeLabel had ("a decimal128", "a bool", "an int32").
func TestReceivedWordsReplaceTypeLabel(t *testing.T) {
	rows := []struct {
		name     string
		stored   interface{}
		want     string
		oldWords string
	}{
		{"a string", "Ada", "holds a string", ""},
		{"a Decimal128", primitive.NewDecimal128(0, 5), "holds a 128-bit decimal", "holds a decimal128,"},
		{"a bool", true, "holds a boolean", "holds a bool,"},
	}
	for _, row := range rows {
		s := numericServer(t, "a", map[string]interface{}{"Name": row.stored})
		for _, update := range []UpdateRequest{
			{Database: "app", Bucket: "P", Key: "a", Inc: map[string]int64{"Name": 1}},
			{Database: "app", Bucket: "P", Key: "a", Mul: map[string]float64{"Name": 2}},
		} {
			_, err := s.Update(update)
			if err == nil {
				t.Fatalf("%s: the update was acknowledged", row.name)
			}
			if !strings.Contains(err.Error(), row.want) {
				t.Errorf("%s: the refusal is %q; want it to say %q", row.name, err, row.want)
			}
			if row.oldWords != "" && strings.Contains(err.Error(), row.oldWords) {
				t.Errorf("%s: the refusal still says %q", row.name, row.oldWords)
			}
		}
	}
}

// TestOverflowRefusalUsesTheReceivedWords: the overflow refusal names the
// stored type with the same words.
func TestOverflowRefusalUsesTheReceivedWords(t *testing.T) {
	s := numericServer(t, "a", map[string]interface{}{"Count": int64(9223372036854775807)})
	_, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Inc: map[string]int64{"Count": 1}})
	if err == nil {
		t.Fatal("an overflowing $inc was acknowledged")
	}
	if !strings.Contains(err.Error(), "the field holds a 64-bit integer") {
		t.Errorf("the overflow refusal is %q; want it to say the field holds a 64-bit integer", err)
	}
}
