package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"slices"
	"strings"
	"testing"
)

func hashf(digest hash.Hash, format string, args ...any) {
	_, _ = fmt.Fprintf(digest, format, args...)
}

func mustNew(t *testing.T, layout Layout, seed uint64, scale int) *Generator {
	t.Helper()
	g, err := New(layout, seed, scale)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// fingerprint hashes every unit of the generator, so any change to a document, an element, a
// key or the order shows.
func fingerprint(g *Generator) string {
	digest := sha256.New()
	for unit := range g.Units() {
		hashf(digest, "doc %s %s %v %v\n", unit.Doc.Bucket, unit.Doc.ID, unit.Doc.Fields, unit.Doc.Counters)
		for _, element := range unit.Elements {
			hashf(digest, "el %s %s %s %q %d %v\n", element.Bucket, element.Owner, element.Store, element.Key, element.Seq, element.Body)
		}
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// goldenFingerprints pin the generator at seed 1, scale 1. A change that moves one means the
// three engines load different data than the last run did; update them only on purpose, with
// the reason in the commit.
var goldenFingerprints = map[Layout]string{
	Social: "fc2b42e5e182e76c038e918baa4e95de81395f8cafc5c2eb9ef424e37223e05f",
	Chat:   "683dc712311908690c3531541283c01b7292bdf8ea290d4b5417ee582a05fb94",
	Ledger: "86813dd5199c12366e06e4d4bf2609fb4cc0d7107cd0f97923fb1d2677d2158b",
	Agent:  "47d5d7793729934384a2318a0e4e1ad16b3674f716681fe839805ed4366f171f",
	Video:  "cf97fc350cec521b93cfc82abdf11f76bdcdab203028dc6bfe47171ca852b756",
}

func TestGeneratorIsPinnedByAGoldenFingerprint(t *testing.T) {
	for _, layout := range Layouts {
		if got := fingerprint(mustNew(t, layout, 1, 1)); got != goldenFingerprints[layout] {
			t.Errorf("%s: fingerprint %s, want %s", layout, got, goldenFingerprints[layout])
		}
	}
}

func TestGeneratorIsDeterministicAndSeedSensitive(t *testing.T) {
	for _, layout := range Layouts {
		first := fingerprint(mustNew(t, layout, 7, 1))
		if second := fingerprint(mustNew(t, layout, 7, 1)); first != second {
			t.Errorf("%s: two generators from one seed differ", layout)
		}
		if other := fingerprint(mustNew(t, layout, 8, 1)); first == other {
			t.Errorf("%s: seeds 7 and 8 give the same data", layout)
		}
	}
}

func TestCountsAtScaleOne(t *testing.T) {
	want := map[Layout]Counts{
		Social: {"User": 40, "Post": 1600, "Group": 10, "Follow": 74, "User.Feed": 1600, "User.Inbox": 2960, "User.Followers": 74, "User.Following": 74,
			"User.Friends": 120, "User.Settings": 120, "User.Photos": 80, "User.Drafts": 0, "Group.Members": 40, "Group.Wall": 50},
		Chat:   {"User": 40, "Room": 145, "Room.Messages": 14500, "Member": 803, "User.Rooms": 803},
		Ledger: {"Account": 30, "Account.Expenses": 3000, "Account.Invoices": 1200},
		Agent: {"User": 40, "User.Memories": 8000, "Session": 60, "Session.Messages": 2400, "Run": 60,
			"Run.Scratch": 360, "Run.Tasks": 600},
		Video: {"Video": 100, "Channel": 20, "Viewer": 40, "Video.Comments": 800, "Video.Likes": 600, "Channel.Uploads": 100,
			"Channel.Subscribers": 160, "Viewer.Subscriptions": 160, "Viewer.Progress": 400, "Viewer.History": 800, "Viewer.Playlists": 400},
	}
	for layout, expected := range want {
		got := mustNew(t, layout, 1, 1).Counts()
		for name, count := range expected {
			if got[name] != count {
				t.Errorf("%s: %s is %d, want %d", layout, name, got[name], count)
			}
		}
	}
}

func TestScaleMultipliesFixedCounts(t *testing.T) {
	one, three := mustNew(t, Ledger, 1, 1).Counts(), mustNew(t, Ledger, 1, 3).Counts()
	for name, count := range one {
		if three[name] != 3*count {
			t.Errorf("ledger %s: scale 3 holds %d, want %d", name, three[name], 3*count)
		}
	}
	if _, err := New(Social, 1, 0); err == nil {
		t.Error("scale 0 was accepted")
	}
}

func TestSocialFollowGraphHasTheStatedShape(t *testing.T) {
	g := mustNew(t, Social, 1, 1)
	perUser := map[string]int{}
	for unit := range g.Units() {
		if unit.Doc.Bucket == "User" {
			perUser[unit.Doc.ID] = 0
		}
	}
	for follow := range g.Documents("Follow") {
		if follow.Fields[0].Value == follow.Fields[1].Value {
			t.Errorf("%s follows itself", follow.ID)
		}
		perUser[follow.Fields[1].Value.(string)]++
	}
	counts := make([]int, 0, len(perUser))
	for _, n := range perUser {
		counts = append(counts, n)
	}
	slices.Sort(counts)
	if busiest, median := counts[len(counts)-1], (counts[19]+counts[20])/2; busiest != 8 || median != 2 {
		t.Errorf("busiest user has %d followers and the median is %d, want 8 and 2", busiest, median)
	}
}

// TestTheCopiesOfOneFactAgree: the same follow, like or join is written in more than one place
// (two stores and a Membership bucket), and a verdict that compares them needs equal counts.
func TestTheCopiesOfOneFactAgree(t *testing.T) {
	social := mustNew(t, Social, 1, 2).Counts()
	if social["Follow"] != social["User.Followers"] || social["Follow"] != social["User.Following"] || social["Post.Likes"] != social["User.Likes"] {
		t.Errorf("social copies differ: %v", social)
	}
	chat := mustNew(t, Chat, 1, 2).Counts()
	if chat["Member"] != chat["Room.Members"] || chat["Member"] != chat["User.Rooms"] {
		t.Errorf("chat copies differ: %v", chat)
	}
	video := mustNew(t, Video, 1, 2).Counts()
	if video["Channel.Subscribers"] != video["Viewer.Subscriptions"] || video["Channel.Uploads"] != video["Video"] {
		t.Errorf("video copies differ: %v", video)
	}
	counts := mustNew(t, Chat, 1, 1).Counts()
	if members, rooms := counts["Member"], counts["Room"]; members < 3*rooms || members > 8*rooms {
		t.Errorf("%d members in %d rooms; each room has 3 to 8", members, rooms)
	}
}

func TestEveryElementBelongsToAnEarlierDocumentInOrder(t *testing.T) {
	for _, layout := range Layouts {
		g := mustNew(t, layout, 1, 1)
		declared := map[string]string{}
		for _, bucket := range g.Schema() {
			for _, field := range bucket.Fields {
				if field.IsStore() {
					declared[bucket.Name+"."+field.Name] = field.Type
				}
			}
		}
		ids := map[string]bool{}
		for unit := range g.Units() {
			key := unit.Doc.Bucket + "/" + unit.Doc.ID
			if ids[key] {
				t.Errorf("%s: duplicate document %s", layout, key)
			}
			ids[key] = true
			next := map[string]int64{}
			keys := map[string]bool{}
			for _, element := range unit.Elements {
				kind, ok := declared[element.StoreName()]
				if !ok || element.Owner != unit.Doc.ID {
					t.Fatalf("%s: element %v is not in a declared store of its unit", layout, element)
				}
				if element.Seq != next[element.Store] {
					t.Errorf("%s: %s/%s seq %d, want %d", layout, key, element.Store, element.Seq, next[element.Store])
				}
				next[element.Store]++
				keyed := kind == "hashStore" || kind == "btreeStore"
				if keyed == (element.Key == "") {
					t.Errorf("%s: %s element key %q in a %s", layout, element.StoreName(), element.Key, kind)
				}
				if keyed {
					if name := element.Store + "/" + element.Key; keys[name] {
						t.Errorf("%s: %s repeats key %s", layout, key, name)
					} else {
						keys[name] = true
					}
				}
			}
		}
	}
}

func TestMemoriesCarryA64ValueVector(t *testing.T) {
	g := mustNew(t, Agent, 1, 1)
	seen := 0
	for element := range g.Elements("User.Memories") {
		var packed []byte
		text := ""
		for _, field := range element.Body {
			switch field.Name {
			case "vec":
				packed = field.Value.([]byte)
			case "text":
				text = field.Value.(string)
			}
		}
		values, err := DecodeVector(packed)
		if err != nil || len(values) != VectorLen || text == "" {
			t.Fatalf("memory %s/%s: %d values, text %q, %v", element.Owner, element.Key, len(values), text, err)
		}
		seen++
	}
	if seen != 8000 {
		t.Errorf("%d memories, want 8000", seen)
	}
	if _, err := DecodeVector([]byte{1, 2, 3}); err == nil {
		t.Error("a 3-byte vector decoded")
	}
}

func TestParseLayoutNamesTheChoices(t *testing.T) {
	if layout, err := ParseLayout("chat"); err != nil || layout != Chat {
		t.Fatalf("%v %v", layout, err)
	}
	if layout, err := ParseLayout("video"); err != nil || layout != Video {
		t.Fatalf("%v %v", layout, err)
	}
	if _, err := ParseLayout("blog"); err == nil {
		t.Fatal("an unknown layout was accepted")
	}
}

// TestVideoPointsAtItsBytesAndKeepsItsReferencesWhole: a video stores a path and never bytes,
// every reference names a document that exists, and a subscription is written on both sides
// with the same time.
func TestVideoPointsAtItsBytesAndKeepsItsReferencesWhole(t *testing.T) {
	g := mustNew(t, Video, 1, 1)
	exists := map[string]bool{}
	for unit := range g.Units() {
		exists[unit.Doc.Bucket+"/"+unit.Doc.ID] = true
	}
	since := map[string]int64{}
	for unit := range g.Units() {
		if unit.Doc.Bucket != "Video" {
			continue
		}
		fields := map[string]any{}
		for _, field := range unit.Doc.Fields {
			fields[field.Name] = field.Value
		}
		path, _ := fields["Path"].(string)
		views, _ := fields["Views"].(int64)
		if !strings.HasPrefix(path, "library/") || !strings.HasSuffix(path, ".mp4") || views < 0 || views > 100000 {
			t.Errorf("video %s: path %q, views %v", unit.Doc.ID, path, fields["Views"])
		}
		if !exists["Channel/"+fields["Owner"].(string)] {
			t.Errorf("video %s: owner %v is not a channel", unit.Doc.ID, fields["Owner"])
		}
	}
	for element := range g.Elements("Channel.Subscribers") {
		since[element.Key+">"+element.Owner] = element.Body[0].Value.(int64)
		if !exists["Viewer/"+element.Key] {
			t.Errorf("subscriber %s is not a viewer", element.Key)
		}
	}
	for element := range g.Elements("Viewer.Subscriptions") {
		if got, ok := since[element.Owner+">"+element.Key]; !ok || got != element.Body[0].Value.(int64) {
			t.Errorf("subscription %s>%s has no matching subscriber entry", element.Owner, element.Key)
		}
	}
	for element := range g.Elements("Viewer.Playlists") {
		if !strings.Contains(element.Key, "/") || !exists["Video/"+element.Body[0].Value.(string)] {
			t.Errorf("playlist element %s %q names %v", element.Owner, element.Key, element.Body[0].Value)
		}
	}
	var last int64
	for element := range g.Elements("Channel.Uploads") {
		if element.Owner == "c0" {
			at := element.Body[1].Value.(int64)
			if at <= last {
				t.Errorf("uploads of c0 are not pushed oldest first")
			}
			last = at
		}
	}
}
