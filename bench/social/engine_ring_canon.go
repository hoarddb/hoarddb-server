package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
)

// canonicalUser reads one user's whole content (spec P4): the profile, every post with its
// likers and comments, the feed, the inbox, both follow lists and the media.
func (o *ringOps) canonicalUser(ctx context.Context, rank int) (canonicalUser, error) {
	profile, err := o.profile(ctx, rank)
	if err != nil {
		return canonicalUser{}, err
	}
	raw := rawUser{Profile: profile, Media: map[string][]byte{}}
	if err := o.readMedia(ctx, rank, raw.Media); err != nil {
		return canonicalUser{}, err
	}
	var refs []ringPostRef
	err = o.listAll(ctx, userStore(rank, ringload.UserPosts), func(element bson.Raw) error {
		var ref ringPostRef
		if err := bson.Unmarshal(element, &ref); err != nil {
			return err
		}
		refs = append(refs, ref)
		return nil
	})
	if err != nil {
		return canonicalUser{}, err
	}
	raw.Posts = make([]rawPost, len(refs))
	err = o.gather(len(refs), func(index int) error { return o.readPost(ctx, refs[index], &raw.Posts[index]) })
	if err != nil {
		return canonicalUser{}, err
	}
	if raw.Feed, err = readWhole(ctx, o, userStore(rank, ringload.UserFeed), decodeFeed); err != nil {
		return canonicalUser{}, err
	}
	// A lifoStore lists oldest first and the canonical form wants the order a pop would take, newest first.
	slices.Reverse(raw.Feed)
	if raw.Inbox, err = readWhole(ctx, o, userStore(rank, ringload.UserInbox), decodeNotes); err != nil {
		return canonicalUser{}, err
	}
	if raw.Following, err = o.userIDsOf(ctx, userStore(rank, ringload.UserFollowing)); err != nil {
		return canonicalUser{}, err
	}
	if raw.Followers, err = o.userIDsOf(ctx, userStore(rank, ringload.UserFollowers)); err != nil {
		return canonicalUser{}, err
	}
	return canonicalize(raw), nil
}

// readMedia fills media with the avatar and the photo the user's document holds.
func (o *ringOps) readMedia(ctx context.Context, rank int, media map[string][]byte) error {
	raw, err := o.api.get(ctx, ringload.UserBucket, ringload.UserID(rank))
	if err != nil {
		return fmt.Errorf("reading the media of user %d: %w", rank, err)
	}
	var doc ringUserDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("decoding user %d: %w", rank, err)
	}
	if len(doc.Avatar) > 0 {
		media["avatar"] = doc.Avatar
	}
	if len(doc.Photo) > 0 {
		media["photo"] = doc.Photo
	}
	return nil
}

// readPost fills one post with its document, likers and comments.
func (o *ringOps) readPost(ctx context.Context, ref ringPostRef, into *rawPost) error {
	posts, err := o.posts(ctx, []ringPostRef{ref})
	if err != nil {
		return err
	}
	into.Post = posts[0]
	likes := storeRef{bucket: ringload.PostBucket, id: ref.Post, field: ringload.PostLikes}
	err = o.listAll(ctx, likes, func(element bson.Raw) error {
		var like ringLikeElement
		if err := bson.Unmarshal(element, &like); err != nil {
			return err
		}
		into.Likers = append(into.Likers, like.Liker)
		return nil
	})
	if err != nil {
		return fmt.Errorf("post %s: %w", ref.Post, err)
	}
	comments := storeRef{bucket: ringload.PostBucket, id: ref.Post, field: ringload.PostComments}
	return o.listAll(ctx, comments, func(element bson.Raw) error {
		var comment rawComment
		if err := bson.Unmarshal(element, &comment); err != nil {
			return err
		}
		into.Comments = append(into.Comments, comment)
		return nil
	})
}

func decodeNotes(elements []bson.Raw) ([]rawNote, error) {
	notes := make([]rawNote, len(elements))
	for index, element := range elements {
		if err := bson.Unmarshal(element, &notes[index]); err != nil {
			return nil, fmt.Errorf("decoding notification %d: %w", index, err)
		}
	}
	return notes, nil
}

// readWhole reads a store field whole by paging through its list and decodes the elements. The server caps a peek at 100 elements, so a peek cannot
// be the whole of a feed; a list has a cursor.
func readWhole[T any](ctx context.Context, o *ringOps, store storeRef, decode func([]bson.Raw) ([]T, error)) ([]T, error) {
	var elements []bson.Raw
	err := o.listAll(ctx, store, func(element bson.Raw) error {
		elements = append(elements, element)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return decode(elements)
}
