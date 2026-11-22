package main

import (
	"math/rand/v2"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

func TestPostGeneratorRespectsBounds(t *testing.T) {
	gen := newPostGenerator(rand.New(rand.NewPCG(7, 7)), 280, 1800)
	smallest, largest := 1<<30, 0
	for seq := int64(0); seq < 2000; seq++ {
		post := gen.post("42", seq)
		text := post["text"].(string)
		if len(text) < 280 || len(text) > 1800 {
			t.Fatalf("post %d text is %d bytes, want 280..1800", seq, len(text))
		}
		smallest, largest = min(smallest, len(text)), max(largest, len(text))
		if tags := post["tags"].([]string); len(tags) > 3 {
			t.Fatalf("post %d has %d tags, want at most 3", seq, len(tags))
		}
		encoded, err := bson.Marshal(post)
		if err != nil {
			t.Fatalf("encoding post %d: %v", seq, err)
		}
		if len(encoded) < 300 || len(encoded) > 2200 {
			t.Fatalf("post %d is %d BSON bytes, outside the 0.4-2 KB model", seq, len(encoded))
		}
	}
	if smallest > 330 || largest < 1750 {
		t.Fatalf("text lengths span only %d..%d, not uniform over 280..1800", smallest, largest)
	}
}

func TestProfileIsAboutThreeHundredBytes(t *testing.T) {
	gen := newPostGenerator(rand.New(rand.NewPCG(1, 1)), 280, 1800)
	encoded, err := bson.Marshal(gen.profile("9"))
	if err != nil {
		t.Fatalf("encoding profile: %v", err)
	}
	if len(encoded) < 250 || len(encoded) > 400 {
		t.Fatalf("profile is %d BSON bytes, want about 300", len(encoded))
	}
}

func TestUserPickerStaysInRangeAndIsSkewed(t *testing.T) {
	const users = 1000
	pick := newUserPicker(rand.New(rand.NewPCG(3, 3)), users, 1.1)
	counts := make([]int, users)
	for i := 0; i < 200000; i++ {
		rank := pick()
		if rank < 0 || rank >= users {
			t.Fatalf("picked user %d outside [0,%d)", rank, users)
		}
		counts[rank]++
	}
	if counts[0] <= counts[users/2]*20 {
		t.Fatalf("user 0 got %d picks and user %d got %d: not Zipf-skewed", counts[0], users/2, counts[users/2])
	}
}
