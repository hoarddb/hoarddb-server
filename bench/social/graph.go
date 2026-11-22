package main

import (
	"fmt"
	"iter"
	"math"
	"math/rand/v2"
	"slices"
	"time"
)

// datasetSpec is one row of the dataset table of spec 6.4. Counts are exact:
// the generator produces precisely these numbers for a given seed.
type datasetSpec struct {
	Name        string
	Users       int
	Posts       int
	FollowEdges int
	FeedEntries int
	Likes       int
	Comments    int
	Inbox       int
	Avatars     int
	Photos      int
}

var datasetSpecs = []datasetSpec{
	{Name: "S-10k", Users: 10_000, Posts: 100_000, FollowEdges: 200_000, FeedEntries: 200_000, Likes: 100_000, Comments: 50_000, Inbox: 50_000, Avatars: 1_000, Photos: 100},
	{Name: "S-100k", Users: 100_000, Posts: 1_000_000, FollowEdges: 2_000_000, FeedEntries: 2_000_000, Likes: 1_000_000, Comments: 500_000, Inbox: 500_000, Avatars: 10_000, Photos: 1_000},
	{Name: "S-1M", Users: 1_000_000, Posts: 2_000_000, FollowEdges: 5_000_000, FeedEntries: 5_000_000, Likes: 1_000_000, Comments: 0, Inbox: 1_000_000, Avatars: 10_000, Photos: 0},
}

// lookupDataset finds a dataset by its spec 6.4 name.
func lookupDataset(name string) (datasetSpec, error) {
	for _, spec := range datasetSpecs {
		if spec.Name == name {
			return spec, nil
		}
	}
	return datasetSpec{}, fmt.Errorf("dataset %q is not one of S-10k, S-100k, S-1M", name)
}

const (
	followingMax      = 100 // spec 6.1: following is geometric with minimum 1 and maximum 100
	followerCap       = 500 // the celebrity cap, so fan-out on write is bounded
	zipfExponent      = 1.1
	postAgeSpan       = 90 * 24 * time.Hour
	postTextMin       = 80
	postTextMax       = 480
	avatarBytes       = 16 << 10
	photoBytes        = 200 << 10
	zipfRetriesBefore = 8 // Zipf draws a saturated celebrity this often before the picker falls back to a uniform draw
)

// graphEpoch is the fixed "now" of generated data, so the same seed gives the
// same bytes on any day.
var graphEpoch = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// Seed domains keep the random streams of unrelated entities independent.
const (
	domainFollowing uint64 = iota + 1
	domainFollowees
	domainFeedQuota
	domainProfile
	domainPost
	domainLike
	domainComment
	domainInbox
	domainMedia
)

// socialGraph is the deterministic social network of spec 6.1 and 6.4. Only
// the follow graph is held in memory (4 bytes per edge, 20 MB for S-1M): it has
// to be, because the follower cap makes each user's choices depend on the ones
// before. Posts, reactions, notifications and media stream from per-entity
// random generators and are never held.
type socialGraph struct {
	spec          datasetSpec
	seed          uint64
	postsPerUser  int
	followOffsets []int32  // user u follows followees[followOffsets[u]:followOffsets[u+1]]
	followees     []int32  // flat list of followed user ranks
	followerCount []uint16 // followers of each user
	feedQuota     []uint16 // feed entries of each user
}

func newSocialGraph(spec datasetSpec, seed uint64) *socialGraph {
	graph := &socialGraph{spec: spec, seed: seed, postsPerUser: spec.Posts / spec.Users}
	graph.buildFollowGraph()
	graph.planFeedQuotas()
	return graph
}

func (g *socialGraph) rng(domain, major, minor uint64) *rand.Rand {
	return rand.New(rand.NewPCG(g.seed^domain*0x9e3779b97f4a7c15, major<<24|minor))
}

// followingCounts draws each user's following count from a geometric
// distribution (minimum 1, maximum 100) with the mean the dataset table
// implies, then nudges single users up or down until the total is exactly the
// table's edge count.
func (g *socialGraph) followingCounts() []int {
	rng := g.rng(domainFollowing, 0, 0)
	successProbability := float64(g.spec.Users) / float64(g.spec.FollowEdges)
	counts := make([]int, g.spec.Users)
	total := 0
	for user := range counts {
		failures := math.Floor(math.Log1p(-rng.Float64()) / math.Log1p(-successProbability))
		counts[user] = min(1+int(failures), followingMax)
		total += counts[user]
	}
	step := 1
	if total > g.spec.FollowEdges {
		step = -1
	}
	for user := rng.IntN(g.spec.Users); total != g.spec.FollowEdges; user = (user + 1) % g.spec.Users {
		if next := counts[user] + step; next >= 1 && next <= followingMax {
			counts[user] = next
			total += step
		}
	}
	return counts
}

// buildFollowGraph picks, for every user in rank order, who they follow: Zipf
// over users, never themselves, never twice, never a user already at the
// follower cap. A saturated celebrity is redrawn, and after a few failed Zipf
// draws the pick becomes uniform so the cap bounds fan-out without stalling.
func (g *socialGraph) buildFollowGraph() {
	counts := g.followingCounts()
	rng := g.rng(domainFollowees, 0, 0)
	zipf := rand.NewZipf(rng, zipfExponent, 1, uint64(g.spec.Users-1))
	g.followOffsets = make([]int32, g.spec.Users+1)
	g.followees = make([]int32, 0, g.spec.FollowEdges)
	g.followerCount = make([]uint16, g.spec.Users)
	for user, want := range counts {
		chosen := g.followees[len(g.followees):]
		for len(chosen) < want {
			var candidate int32
			for attempt := 0; ; attempt++ {
				if attempt < zipfRetriesBefore {
					candidate = int32(zipf.Uint64())
				} else {
					candidate = int32(rng.IntN(g.spec.Users))
				}
				if int(candidate) != user && g.followerCount[candidate] < followerCap && !slices.Contains(chosen, candidate) {
					break
				}
			}
			chosen = append(chosen, candidate)
			g.followerCount[candidate]++
		}
		g.followees = g.followees[:len(g.followees)+want]
		g.followOffsets[user+1] = int32(len(g.followees))
	}
}

func (g *socialGraph) following(user int) []int32 {
	return g.followees[g.followOffsets[user]:g.followOffsets[user+1]]
}

// planFeedQuotas gives every user feedEntries/users feed entries, capped by the
// posts available (followed users times posts per user). Users with room take
// one more at a time until the table's total is exact, so a user who follows
// one person with ten posts does not leave the dataset short.
func (g *socialGraph) planFeedQuotas() {
	perUser := g.spec.FeedEntries / g.spec.Users
	g.feedQuota = make([]uint16, g.spec.Users)
	total := 0
	for user := range g.feedQuota {
		g.feedQuota[user] = uint16(min(perUser, len(g.following(user))*g.postsPerUser))
		total += int(g.feedQuota[user])
	}
	for user := g.rng(domainFeedQuota, 0, 0).IntN(g.spec.Users); total < g.spec.FeedEntries; user = (user + 1) % g.spec.Users {
		if int(g.feedQuota[user]) < len(g.following(user))*g.postsPerUser {
			g.feedQuota[user]++
			total++
		}
	}
}

// userProfile is the roughly 300 byte User document.
type userProfile struct {
	ID        string
	Name      string
	Bio       string
	Joined    time.Time
	Followers int64
	Following int64
}

// feedRef names the post a feed entry is a copy of; post(Author, Seq) rebuilds
// the document.
type feedRef struct {
	Author int32
	Seq    int32
}

// notification is one inbox entry waiting for its user.
type notification struct {
	Kind  string
	Actor int
}

// graphUser is everything that belongs to one user's own stores. Feed is in
// delivery order, oldest first. Following is shared with the graph: read it,
// never modify it.
type graphUser struct {
	Rank      int
	Profile   userProfile
	Following []int32
	Feed      []feedRef
	Inbox     []notification
}

// users streams every user with their follows, feed and inbox, in rank order.
func (g *socialGraph) users() iter.Seq[graphUser] {
	inboxPerUser := g.spec.Inbox / g.spec.Users
	kinds := []string{"like", "comment", "follow"}
	return func(yield func(graphUser) bool) {
		for rank := range g.spec.Users {
			rng := g.rng(domainProfile, uint64(rank), 0)
			words := &postGenerator{rng: rng}
			user := graphUser{
				Rank: rank,
				Profile: userProfile{
					ID: userID(rank), Name: words.words(14), Bio: words.words(150),
					Joined:    graphEpoch.Add(-time.Duration(rng.Int64N(int64(5 * 365 * 24 * time.Hour)))).Truncate(time.Millisecond),
					Followers: int64(g.followerCount[rank]), Following: int64(len(g.following(rank))),
				},
				Following: g.following(rank),
				Feed:      g.feedOf(rank),
				Inbox:     make([]notification, inboxPerUser),
			}
			inboxRNG := g.rng(domainInbox, uint64(rank), 0)
			for i := range user.Inbox {
				user.Inbox[i] = notification{Kind: kinds[inboxRNG.IntN(len(kinds))], Actor: inboxRNG.IntN(g.spec.Users)}
			}
			if !yield(user) {
				return
			}
		}
	}
}

// feedOf takes the newest post of each followed user first, then the next
// newest, until the user's quota is met, and returns them oldest first.
func (g *socialGraph) feedOf(user int) []feedRef {
	quota := int(g.feedQuota[user])
	newestFirst := make([]feedRef, 0, quota)
	followed := g.following(user)
	for round := 0; len(newestFirst) < quota; round++ {
		for _, author := range followed {
			if len(newestFirst) == quota {
				break
			}
			newestFirst = append(newestFirst, feedRef{Author: author, Seq: int32(g.postsPerUser - 1 - round)})
		}
	}
	slices.Reverse(newestFirst)
	return newestFirst
}

// graphPost is one post, about 350 bytes stored.
type graphPost struct {
	Author  int
	Seq     int
	Text    string
	Created time.Time
	Tags    []string
	ReplyTo string
}

// post rebuilds the document of a user's seq-th post. The same arguments always
// give the same bytes, which is what lets a feed entry be a reference.
func (g *socialGraph) post(author, seq int) graphPost {
	rng := g.rng(domainPost, uint64(author), uint64(seq))
	text := (&postGenerator{rng: rng}).words(postTextMin + rng.IntN(postTextMax-postTextMin+1))
	// Later posts are newer: each post sits in its own slice of the 90 days.
	age := time.Duration((float64(g.postsPerUser-seq) - rng.Float64()) / float64(g.postsPerUser) * float64(postAgeSpan))
	tags := make([]string, rng.IntN(4))
	for i := range tags {
		tags[i] = tagPool[rng.IntN(len(tagPool))]
	}
	replyTo := ""
	if seq > 0 && rng.IntN(5) == 0 {
		replyTo = fmt.Sprintf("%d:%d", author, rng.IntN(seq))
	}
	return graphPost{Author: author, Seq: seq, Text: text, Created: graphEpoch.Add(-age).Truncate(time.Millisecond), Tags: tags, ReplyTo: replyTo}
}

// posts streams every post, a user's posts together, oldest first.
func (g *socialGraph) posts() iter.Seq[graphPost] {
	return func(yield func(graphPost) bool) {
		for author := range g.spec.Users {
			for seq := range g.postsPerUser {
				if !yield(g.post(author, seq)) {
					return
				}
			}
		}
	}
}

// reaction is a like or a comment: the actor is uniform over users, the post
// is a Zipf-chosen author's uniformly chosen post.
type reaction struct {
	Actor      int
	PostAuthor int
	PostSeq    int
}

func (g *socialGraph) reactions(domain uint64, count int) iter.Seq[reaction] {
	return func(yield func(reaction) bool) {
		rng := g.rng(domain, 0, 0)
		zipf := rand.NewZipf(rng, zipfExponent, 1, uint64(g.spec.Users-1))
		for range count {
			reacted := reaction{Actor: rng.IntN(g.spec.Users), PostAuthor: int(zipf.Uint64()), PostSeq: rng.IntN(g.postsPerUser)}
			if !yield(reacted) {
				return
			}
		}
	}
}

func (g *socialGraph) likes() iter.Seq[reaction] { return g.reactions(domainLike, g.spec.Likes) }
func (g *socialGraph) comments() iter.Seq[reaction] {
	return g.reactions(domainComment, g.spec.Comments)
}

type mediaKind int

const (
	mediaAvatar mediaKind = iota + 1
	mediaPhoto
)

// mediaItem is one blob: a 16 KiB avatar or a 200 KiB photo. The photo is above
// the 64 KiB at which hoardDB writes a blob as a file.
type mediaItem struct {
	Owner int
	Kind  mediaKind
	Bytes int
}

// media streams the avatars and photos. Owners are spread evenly: every
// (users/avatars)-th user has an avatar and every (users/photos)-th a photo.
func (g *socialGraph) media() iter.Seq[mediaItem] {
	return func(yield func(mediaItem) bool) {
		for rank := range g.spec.Users {
			if g.spec.Avatars > 0 && rank%(g.spec.Users/g.spec.Avatars) == 0 && !yield(mediaItem{Owner: rank, Kind: mediaAvatar, Bytes: avatarBytes}) {
				return
			}
			if g.spec.Photos > 0 && rank%(g.spec.Users/g.spec.Photos) == 0 && !yield(mediaItem{Owner: rank, Kind: mediaPhoto, Bytes: photoBytes}) {
				return
			}
		}
	}
}

// content returns the blob's bytes: pseudo-random, so they do not compress.
func (g *socialGraph) content(item mediaItem) []byte {
	rng := g.rng(domainMedia, uint64(item.Owner), uint64(item.Kind))
	blob := make([]byte, item.Bytes)
	for i := range blob {
		blob[i] = byte(rng.Uint32())
	}
	return blob
}
