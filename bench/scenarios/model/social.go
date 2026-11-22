package model

import (
	"fmt"
	"slices"
)

// followerTemplate is the number of followers of 40 users, sorted: the busiest has 8 and the
// median (the 20th and 21st value) is 2, as section 2.1 of the layouts document states. Larger
// scales repeat it per block of 40 users, so the shape holds at every scale.
var followerTemplate = [40]int{8, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0}

func userID(rank int) string  { return fmt.Sprintf("u%d", rank) }
func postID(rank int) string  { return fmt.Sprintf("p%d", rank) }
func groupID(rank int) string { return fmt.Sprintf("g%d", rank) }

// socialSchema is section 2.1: four buckets, the widened social layout.
func socialSchema() []BucketDecl {
	return []BucketDecl{
		{Name: "User", Fields: decl("Name", "string", "Handle", "string", "JoinedAt", "int", "Bio", "string",
			"Feed", "fifoStore", "Inbox", "fifoStore", "Notifications", "lifoStore", "Photos", "lifoStore",
			"Friends", "hashStore", "Following", "hashStore", "Followers", "hashStore", "Likes", "hashStore",
			"Settings", "hashStore", "Drafts", "heapStore")},
		{Name: "Post", Fields: decl("Author", "string", "Text", "string", "At", "int", "Comments", "fifoStore", "Likes", "hashStore")},
		{Name: "Group", Fields: decl("Name", "string", "Owner", "string", "Members", "hashStore", "Wall", "fifoStore")},
		{Name: "Follow", Fields: decl("From", "string", "To", "string", "At", "int")},
	}
}

// postsPerUser is how many posts each user writes. A user's inbox holds the posts of the users
// they follow and the follow graph (section 2.1: median 2 followers, so under 2 followees per
// user on average) gives some users a single followee, so a single author's 40 posts are what
// guarantee every inbox the 2 x 20 elements a page and a page offset need. It also seeds every
// feed with 40, twice the 2 x 10 it needs.
const postsPerUser = 40

// buildSocial makes 40 users, 10 groups, a follow graph and 1,600 posts (40 per user) per scale step. The
// Drafts heapStore is declared and left empty: the ring client has no push that takes a
// priority, so a heap cannot be seeded (the layouts document drops that row too).
func buildSocial(g *Generator) ([]BucketDecl, []Unit) {
	users, groups, posts := 40*g.scale, 10*g.scale, postsPerUser*40*g.scale
	words := newVocabulary(g.seed)
	b := newBuilder()
	at := func(minutes int) int64 { return epoch + int64(minutes)*60 }

	for rank := range users {
		rng := stream(g.seed, "social-user", rank)
		b.document("User", userID(rank), []Field{
			{"Name", words.sentence(rng, 2)}, {"Handle", fmt.Sprintf("%s%d", words[rng.IntN(len(words))], rank)},
			{"JoinedAt", at(rank)}, {"Bio", words.sentence(rng, 10)},
		}, nil)
	}
	for rank := range groups {
		rng := stream(g.seed, "social-group", rank)
		b.document("Group", groupID(rank), []Field{{"Name", words.sentence(rng, 2)}, {"Owner", userID(rng.IntN(users))}}, nil)
	}

	// The follow graph: user v gets the follower count of the template slot its permuted
	// position falls in, and that many distinct followers other than v.
	order := stream(g.seed, "social-follow-order", 0).Perm(users)
	followers := make([][]int, users)
	followerCount := make([]int, users)
	for slot, rank := range order {
		count := followerTemplate[slot%len(followerTemplate)]
		rng := stream(g.seed, "social-followers", rank)
		followers[rank] = distinct(rng, users, count, rank)
		followerCount[rank] = len(followers[rank])
	}
	following := make([][]int, users)
	for followee := range users {
		for _, follower := range followers[followee] {
			following[follower] = append(following[follower], followee)
		}
	}
	followers, following = ensureEveryUserFollows(followers, following)
	for followee := range users {
		for _, follower := range followers[followee] {
			stamp := at(followee*7 + follower)
			b.element("User", userID(followee), "Followers", userID(follower), Field{"user", userID(follower)}, Field{"at", stamp})
			b.element("User", userID(follower), "Following", userID(followee), Field{"user", userID(followee)}, Field{"at", stamp})
		}
	}

	// Posts, their comments and likes, the author's feed and the followers' inboxes.
	// Authors come round a seeded permutation, so every user writes exactly postsPerUser posts.
	authors := stream(g.seed, "social-authors", 0).Perm(users)
	for rank := range posts {
		rng := stream(g.seed, "social-post", rank)
		author := authors[rank%users]
		stamp := at(users + rank)
		b.document("Post", postID(rank), []Field{{"Author", userID(author)}, {"Text", words.sentence(rng, 14)}, {"At", stamp}}, nil)
		b.element("User", userID(author), "Feed", "", Field{"post", postID(rank)}, Field{"at", stamp})
		for _, follower := range followers[author] {
			b.element("User", userID(follower), "Inbox", "", Field{"post", postID(rank)}, Field{"author", userID(author)}, Field{"at", stamp})
		}
		for comment := range rng.IntN(4) {
			commenter := rng.IntN(users)
			b.element("Post", postID(rank), "Comments", "", Field{"author", userID(commenter)}, Field{"text", words.sentence(rng, 6)}, Field{"at", stamp + int64(comment+1)})
			b.element("User", userID(author), "Notifications", "", Field{"kind", "comment"}, Field{"actor", userID(commenter)}, Field{"post", postID(rank)}, Field{"at", stamp + int64(comment+1)})
		}
		for _, liker := range distinct(rng, users, rng.IntN(4), -1) {
			b.element("Post", postID(rank), "Likes", userID(liker), Field{"user", userID(liker)}, Field{"at", stamp})
			b.element("User", userID(liker), "Likes", postID(rank), Field{"post", postID(rank)}, Field{"at", stamp})
		}
		b.setCounters("Post", postID(rank),
			Field{"likes_n", b.held("Post", postID(rank), "Likes")}, Field{"comments_n", b.held("Post", postID(rank), "Comments")})
	}

	// Per-user keyed stores: friends, settings, photos.
	for rank := range users {
		rng := stream(g.seed, "social-user-stores", rank)
		for _, friend := range distinct(rng, users, 3, rank) {
			b.element("User", userID(rank), "Friends", userID(friend), Field{"user", userID(friend)}, Field{"since", at(rank + friend)})
		}
		for _, setting := range []struct{ key, value string }{{"theme", []string{"light", "dark"}[rng.IntN(2)]}, {"lang", []string{"en", "de", "fr"}[rng.IntN(3)]}, {"digest", []string{"daily", "weekly", "off"}[rng.IntN(3)]}} {
			b.element("User", userID(rank), "Settings", setting.key, Field{"value", setting.value})
		}
		for photo := range 2 {
			b.element("User", userID(rank), "Photos", "", Field{"name", fmt.Sprintf("%s-%d.jpg", userID(rank), photo)}, Field{"bytes", int64(20000 + rng.IntN(80000))})
		}
		b.setCounters("User", userID(rank),
			Field{"followers_n", int64(followerCount[rank])}, Field{"following_n", int64(len(following[rank]))})
	}

	// Groups: members and a wall.
	for rank := range groups {
		rng := stream(g.seed, "social-group-stores", rank)
		for _, member := range distinct(rng, users, 4, -1) {
			b.element("Group", groupID(rank), "Members", userID(member), Field{"user", userID(member)}, Field{"role", []string{"member", "admin"}[rng.IntN(2)]})
		}
		for post := range 5 {
			b.element("Group", groupID(rank), "Wall", "", Field{"author", userID(rng.IntN(users))}, Field{"text", words.sentence(rng, 8)}, Field{"at", at(users + posts + post)})
		}
	}

	// The Membership bucket the advisor recommends for "who follows v".
	for followee := range users {
		for _, follower := range followers[followee] {
			id := userID(follower) + ">" + userID(followee)
			b.document("Follow", id, []Field{{"From", userID(follower)}, {"To", userID(followee)}, {"At", at(followee*7 + follower)}}, nil)
		}
	}
	return socialSchema(), b.finish()
}

// ensureEveryUserFollows gives every user at least one followee without changing how many
// followers any user has, so the follower shape of section 2.1 holds and every inbox can fill.
// A user who follows nobody takes over a follower slot of a user who follows two or more.
func ensureEveryUserFollows(followers, following [][]int) ([][]int, [][]int) {
	users := len(followers)
	for lonely := range users {
		if len(following[lonely]) > 0 {
			continue
		}
	search:
		for step := 1; step < users; step++ {
			followee := (lonely + step) % users
			if slices.Contains(followers[followee], lonely) {
				continue
			}
			for slot, donor := range followers[followee] {
				if len(following[donor]) < 2 {
					continue
				}
				followers[followee][slot] = lonely
				following[donor] = slices.DeleteFunc(following[donor], func(other int) bool { return other == followee })
				following[lonely] = append(following[lonely], followee)
				break search
			}
		}
	}
	return followers, following
}
