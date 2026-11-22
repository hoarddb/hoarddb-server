package storage

import (
	"errors"
	"strings"
	"testing"
)

// bannedForAMember is what no refusal of a ring node's directory may tell the operator: a cluster
// member put on an empty directory keeps its id and forgets its Raft term and log, which can lose
// committed cluster state (store-vouched-cut review R3-2).
var bannedForAMember = []string{"re-seed", "reseed", "remove-node", "empty its data directory", "leave it empty", "start the node with no data"}

func requireNoEmptyingAdvice(t *testing.T, text string) {
	t.Helper()
	for _, banned := range bannedForAMember {
		if strings.Contains(text, banned) {
			t.Errorf("the refusal says %q: %s", banned, text)
		}
	}
}

// TestAGoneDirectoryRefusalNeverTellsAnyoneToStartEmpty: [GUARD DirectoryGoneError ends in no advice
// to empty the directory; red when it offers 'leave it empty to start the node with no data']. With
// no advice registered it is the single node's: restore from a backup or keep the directory.
func TestAGoneDirectoryRefusalNeverTellsAnyoneToStartEmpty(t *testing.T) {
	text := (&DirectoryGoneError{Dir: "/ring/badger", Cause: errors.New("gone")}).Error()
	requireNoEmptyingAdvice(t, text)
	if !strings.Contains(text, "from a backup") {
		t.Errorf("the single node's text does not say to restore from a backup: %s", text)
	}
}

// TestADamagedIncarnationFileRefusalNeverTellsAnyoneToStartEmpty: [GUARD incarnationFileDamaged
// offers no remove-node and no emptied directory; red when it does].
func TestADamagedIncarnationFileRefusalNeverTellsAnyoneToStartEmpty(t *testing.T) {
	text := incarnationFileDamaged("/ring/wal/incarnation", errors.New("bad")).Error()
	requireNoEmptyingAdvice(t, text)
	if !strings.Contains(text, "copy of this data directory") {
		t.Errorf("the single node's text does not say to restore the file: %s", text)
	}
}

// TestAHoleInTheLogRefusalNeverTellsAnyoneToReseed: [GUARD holeInLog offers no re-seed; red when it
// does].
func TestAHoleInTheLogRefusalNeverTellsAnyoneToReseed(t *testing.T) {
	text := holeInLog("/ring/wal", 5, 9).Error()
	requireNoEmptyingAdvice(t, text)
	if !strings.Contains(text, "Restore the segment from a backup") {
		t.Errorf("the single node's text does not say to restore the segment: %s", text)
	}
}

// TestARegisteredAdviceReplacesTheDefaultInEachDirectoryRefusal: [GUARD a ring node's advice for its
// directory ends each of the three refusals instead of the default].
func TestARegisteredAdviceReplacesTheDefaultInEachDirectoryRefusal(t *testing.T) {
	const advice = "THE NODE'S ADVICE"
	for _, dir := range []string{"/ring/badger", "/ring/wal"} {
		SetDirectoryAdvice(dir, advice)
		t.Cleanup(func() { ClearDirectoryAdvice(dir) })
	}
	texts := map[string]string{
		"gone":        (&DirectoryGoneError{Dir: "/ring/badger", Cause: errors.New("gone")}).Error(),
		"incarnation": incarnationFileDamaged("/ring/wal/incarnation", errors.New("bad")).Error(),
		"hole":        holeInLog("/ring/wal", 5, 9).Error(),
	}
	for name, text := range texts {
		if !strings.Contains(text, advice) {
			t.Errorf("%s: the registered advice is not in the text: %s", name, text)
		}
		requireNoEmptyingAdvice(t, text)
	}
}
