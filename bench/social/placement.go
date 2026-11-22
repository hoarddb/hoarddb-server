package main

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// placement is where a hoardDB run puts its users (spec 6.2). One shard is the
// primary form H-1, a single User bucket. More shards are the same User
// declaration created once per shard as User_00, User_01 and so on, a user
// living in shard xxHash64(user id) mod shards, the hash the ring uses; the
// client computes it. The form is a shard count, so H-256 is a number away
// from H-64; only the forms flagged built in placementForms can be asked for.
type placement struct {
	shards int
	// perUser is the H-U form: one bucket per user, named User_<id>. shards is
	// then the dataset's user count, and bucketOf indexes names by the user
	// itself rather than by a hash of the id, so every user's whole record has
	// a bucket of its own and the ring spreads users by bucket name.
	perUser bool
	// names are the bucket names, one per shard (or per user, under H-U).
	names []string
}

// placementOfShards is the placement of that many shards; fewer than two is H-1.
func placementOfShards(shards int) placement {
	if shards < 2 {
		return placement{shards: 1, names: []string{userBucketName}}
	}
	width := max(len(strconv.Itoa(shards-1)), 2)
	names := make([]string, shards)
	for shard := range names {
		names[shard] = fmt.Sprintf("%s_%0*d", userBucketName, width, shard)
	}
	return placement{shards: shards, names: names}
}

// placementOfPerUser is H-U: one User_<id> bucket per user, each declaring the
// same stores, so a user's document and stores live on the bucket's owner.
func placementOfPerUser(users int) placement {
	names := make([]string, max(users, 1))
	for user := range names {
		names[user] = fmt.Sprintf("%s_%d", userBucketName, user)
	}
	return placement{shards: len(names), perUser: true, names: names}
}

// placementOfConfig is the placement -placement names. The flag was validated
// when the flags were parsed; a form with no shard count of its own (hu) takes
// the dataset's user count instead.
func placementOfConfig(cfg *config) placement {
	if strings.EqualFold(cfg.Placement, "hu") {
		return placementOfPerUser(cfg.Users)
	}
	return placementOfShards(placementForms[strings.ToLower(cfg.Placement)].shards)
}

// shardOf is the shard a user id lives in: xxHash64 of its decimal text, mod shards.
func shardOf(id string, shards int) int {
	return int(xxhash.Sum64String(id) % uint64(shards))
}

// bucketOf is the bucket holding user's document and stores.
func (p placement) bucketOf(user int) string {
	if p.perUser {
		return p.names[user]
	}
	if p.shards == 1 {
		return p.names[0]
	}
	return p.names[shardOf(userID(user), p.shards)]
}

// shardOfUser is the shard of user, 0 under H-1 and under H-U (where a user's
// own bucket is its unit and the shard ordering would mean nothing).
func (p placement) shardOfUser(user int) int {
	if p.shards == 1 || p.perUser {
		return 0
	}
	return shardOf(userID(user), p.shards)
}

// buckets are every User bucket of the form, in shard order. The first is the
// primary bucket whose holders a leg record names.
func (p placement) buckets() []string { return p.names }

// byShard orders the indexes of users so that those in one shard are adjacent,
// shards ascending and each user's original order kept within its shard. The
// requests of an operation that touches several users go out in this order,
// one after another.
func (p placement) byShard(users []int) []int {
	order := make([]int, len(users))
	for index := range order {
		order[index] = index
	}
	if p.shards > 1 {
		slices.SortStableFunc(order, func(a, b int) int { return p.shardOfUser(users[a]) - p.shardOfUser(users[b]) })
	}
	return order
}

// placementMap is the owner and holder map a sharded run records (spec 9.2).
type placementMap struct {
	// NodeIDs are the server's node ids, sorted; Owners counts, for each, the
	// shard buckets it owns. Holders are the nodes holding the primary bucket.
	NodeIDs []string
	Owners  []int
	Holders []string
}

// standaloneNode names the one node of a server that reports no cluster.
const standaloneNode = "standalone"

// placementMapper is an engine that can report who owns its shard buckets.
type placementMapper interface {
	placementMap(ctx context.Context) (placementMap, error)
}

// placementMap asks the server, bucket by bucket, which nodes hold each shard
// (describe bucket: the replica set, owner first) and tallies the owners. It is
// empty for H-1, which has one owner by construction. A server that names no
// nodes is a standalone server owning every shard itself.
func (h *hoardEngine) placementMap(context.Context) (placementMap, error) {
	place := h.placement()
	// H-U is skipped: one describe per bucket would be the dataset's user count
	// in round trips per leg, and where the buckets landed is visible on the
	// nodes themselves (status, show buckets). The record still names the form.
	if place.shards == 1 || place.perUser {
		return placementMap{}, nil
	}
	caller, release, err := h.openCaller()
	if err != nil {
		return placementMap{}, fmt.Errorf("opening a connection to describe the shard buckets: %w", err)
	}
	defer release()
	ownerOf := map[string]int{}
	var primary []string
	for index, bucket := range place.buckets() {
		var described server.BucketStatsResponse
		if err := caller.call(transport.OpBucketStats, server.BucketStatsRequest{Database: h.cfg.Database, Bucket: bucket}, &described); err != nil {
			return placementMap{}, fmt.Errorf("describing bucket %s: %w", bucket, err)
		}
		if !described.Found {
			return placementMap{}, fmt.Errorf("describing bucket %s: %s; the dataset is not seeded in the %d-shard form, seed it with -placement %s", bucket, described.Message, place.shards, h.cfg.Placement)
		}
		nodes := described.Nodes
		if len(nodes) == 0 {
			nodes = []string{standaloneNode}
		}
		ownerOf[nodes[0]]++
		if index == 0 {
			primary = nodes
		}
	}
	mapped := placementMap{Holders: primary}
	for node := range ownerOf {
		mapped.NodeIDs = append(mapped.NodeIDs, node)
	}
	slices.Sort(mapped.NodeIDs)
	for _, node := range mapped.NodeIDs {
		mapped.Owners = append(mapped.Owners, ownerOf[node])
	}
	return mapped, nil
}
