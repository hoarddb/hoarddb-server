package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// The native layout variant of the ring adapter (-param native=on). The data is seeded with
// scenarios -layout-variant native: a User.Feed element also holds the post's author and text,
// a Room.Members element also holds the member's name (model/native.go is the one definition).
// In native mode a feed page is one StoreList and a room page is two, and the adapter turns the
// copies back into the Post and User documents the common reads return, so the shared verifier
// runs unchanged on them. Nothing here reads a document: a copy that is wrong fails the verifier
// exactly as a wrong Post or User document does, and the post-run audit checks the copies of the
// elements the run wrote (audit_native.go).

// ringNativeMode is the word the settings and the record carry for the native flag.
func ringNativeMode(native bool) string {
	if native {
		return "on"
	}
	return "off"
}

// ringProbeTimeout bounds the open-time probe; Session.Layout has no context of its own.
const ringProbeTimeout = 30 * time.Second

// probeElements names the element the probe reads for each layout that has a copy: the first
// feed of the first user and the first member of the first room, which every seed holds at any
// scale (the model numbers users u0 up and rooms r0 up).
var probeElements = map[model.Layout]struct{ bucket, owner, store, copied string }{
	model.Social: {"User", "u0", "Feed", "text"},
	model.Chat:   {"Room", "r0", "Members", "name"},
}

// probeVariant checks that the server holds the layout variant -param native asks for. The
// seeded schemas have no room for a marker, so it reads one element that can carry a copy: it
// must carry it exactly when native is on. A layout with no copy, and a store that is empty
// (checkFresh reports an unseeded database better), say nothing.
func probeVariant(ctx context.Context, reads ringReads, layout model.Layout, native bool) error {
	probe, ok := probeElements[layout]
	if !ok {
		return nil
	}
	page, err := reads.StoreList(ctx, probe.bucket, probe.owner, probe.store, 1, nil)
	if err != nil {
		return fmt.Errorf("ring: reading the first %s of %s %s to probe the layout variant: %w", probe.store, probe.bucket, probe.owner, err)
	}
	if len(page.Elements) == 0 {
		return nil
	}
	_, holds := page.Elements[0].Lookup(probe.copied).StringValueOK()
	if holds == native {
		return nil
	}
	have, flag, fix := "common", "off", "common"
	if holds {
		have = "native"
	}
	if native {
		flag, fix = "on", "native"
	}
	return fmt.Errorf("this server holds the %s layout but -param native=%s was given. Reseed with -layout-variant %s or change the flag", have, flag, fix)
}

// nativeSplit turns the elements of a store read in native mode into the common elements and
// the documents their copies stand for. A store that carries no copy comes back unchanged with
// no documents.
func nativeSplit(elements []model.Element) ([]model.Element, []model.Document, error) {
	common := make([]model.Element, 0, len(elements))
	var docs []model.Document
	for _, element := range elements {
		stripped, doc, err := model.Strip(element)
		if err != nil {
			return nil, nil, fmt.Errorf("ring: native read: %w", err)
		}
		common = append(common, stripped)
		if doc != nil {
			docs = append(docs, *doc)
		}
	}
	return common, docs, nil
}

// embedFor returns the fields a native write appends to the body of element: the author and
// text of the Post document of the same write for a User.Feed element, the name in the User
// document of the write's Refs for a Room.Members element, nil for every other element. The
// adapter stores them and the audit expects them, so both call this one function.
func embedFor(op WriteOp, element *model.Element) ([]model.Field, error) {
	var source []model.Document
	var bucket, idField string
	var embed func(model.Document) ([]model.Field, error)
	switch {
	case element.Bucket == "User" && element.Store == "Feed":
		source, bucket, idField, embed = op.Documents, "Post", "post", model.FeedEmbed
	case element.Bucket == "Room" && element.Store == "Members":
		source, bucket, idField, embed = op.Refs, "User", "user", model.MemberEmbed
	default:
		return nil, nil
	}
	id, err := textField(*element, idField)
	if err != nil {
		return nil, fmt.Errorf("native write %s: %w", op.ID, err)
	}
	for _, doc := range source {
		if doc.Bucket != bucket || doc.ID != id {
			continue
		}
		extra, err := embed(doc)
		if err != nil {
			return nil, fmt.Errorf("native write %s: %w", op.ID, err)
		}
		return extra, nil
	}
	return nil, fmt.Errorf("native write %s: the %s %s that the %s element of %s copies from is not part of the write, so it cannot carry the copy", op.ID, bucket, id, element.Store, element.Owner)
}

// withCopy returns the element of a write as native mode stores it, with its copy appended. The
// op's own element is not changed, because the registry and the audit read its common body.
func withCopy(op WriteOp, element *model.Element) (*model.Element, error) {
	extra, err := embedFor(op, element)
	if err != nil {
		return nil, fmt.Errorf("ring: %w", err)
	}
	if extra == nil {
		return element, nil
	}
	copied := *element
	copied.Body = append(append([]model.Field(nil), element.Body...), extra...)
	return &copied, nil
}

// ReadEmbeds implements EmbedReader: every element of one store with the body exactly as the
// server holds it, the copies included, for the native audit.
func (o *ringOps) ReadEmbeds(ctx context.Context, ref StoreRef) ([]model.Element, error) {
	elements, _, err := o.readStore(ctx, ref, 0)
	return elements, err
}
