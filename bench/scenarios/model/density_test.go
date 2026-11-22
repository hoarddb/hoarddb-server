package model

import "testing"

// pagedStores is every store a mix read pages through, with the fewest elements each owner must
// be seeded with (owner ruling 2026-11-07): twice the spec page, so a page offset of up to one
// page still ends inside the seeded elements. User.Rooms of the chat layout is a hash store, which
// has no order to take an offset in, so it only needs the one page of 20.
var pagedStores = map[Layout][]struct {
	Bucket, Store string
	Min           int
}{
	Social: {{"User", "Feed", 2 * 10}, {"User", "Inbox", 2 * 20}},
	Chat:   {{"Room", "Messages", 2 * 50}, {"User", "Rooms", 20}},
	Ledger: {{"Account", "Expenses", 2 * 50}, {"Account", "Invoices", 2 * 20}},
	Agent:  {{"Session", "Messages", 2 * 20}, {"User", "Memories", 2 * 100}},
}

// TestEveryOwnerOfAPagedStoreHoldsTwoPages fails on a generator that seeds an owner with fewer
// elements than the read's page needs: the minimum, not the average, is what every read can use.
func TestEveryOwnerOfAPagedStoreHoldsTwoPages(t *testing.T) {
	scales := []int{1, 20, 300}
	if testing.Short() {
		scales = scales[:2]
	}
	for _, layout := range []Layout{Social, Chat, Ledger, Agent} {
		for _, scale := range scales {
			g := mustNew(t, layout, 1, scale)
			for _, paged := range pagedStores[layout] {
				owners := map[string]int{}
				for unit := range g.Units() {
					if unit.Doc.Bucket == paged.Bucket {
						owners[unit.Doc.ID] += 0
						for _, element := range unit.Elements {
							if element.Store == paged.Store {
								owners[unit.Doc.ID]++
							}
						}
					}
				}
				if len(owners) == 0 {
					t.Fatalf("%s scale %d: no %s documents", layout, scale, paged.Bucket)
				}
				fewest, short := -1, 0
				for _, n := range owners {
					if fewest < 0 || n < fewest {
						fewest = n
					}
					if n < paged.Min {
						short++
					}
				}
				if short > 0 {
					t.Errorf("%s scale %d: %d of %d %s owners hold fewer than %d %s elements (fewest %d)",
						layout, scale, short, len(owners), paged.Bucket, paged.Min, paged.Store, fewest)
				}
			}
		}
	}
}

// TestEveryUserFollowsSomeoneSoEveryInboxFills: the inbox is filled by the posts of the users
// one follows, so a user who follows nobody could never hold a page.
func TestEveryUserFollowsSomeoneSoEveryInboxFills(t *testing.T) {
	g := mustNew(t, Social, 1, 1)
	following := map[string]int{}
	for follow := range g.Documents("Follow") {
		following[follow.Fields[0].Value.(string)]++
	}
	for user := range g.Documents("User") {
		if following[user.ID] == 0 {
			t.Errorf("%s follows nobody", user.ID)
		}
	}
}

// TestEveryChatUserIsInEnoughRoomsAtAnySeed: the guarantee is by construction, not by luck of one seed.
func TestEveryChatUserIsInEnoughRoomsAtAnySeed(t *testing.T) {
	for seed := uint64(1); seed <= 12; seed++ {
		g := mustNew(t, Chat, seed, 1)
		for unit := range g.Units() {
			if unit.Doc.Bucket != "User" {
				continue
			}
			rooms := 0
			for _, element := range unit.Elements {
				if element.Store == "Rooms" {
					rooms++
				}
			}
			if rooms < 20 {
				t.Errorf("seed %d: %s is in %d rooms", seed, unit.Doc.ID, rooms)
			}
		}
	}
}
