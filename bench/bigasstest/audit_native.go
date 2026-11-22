package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// The audit of the native layout variant. A page never reaches an element the run wrote (a paged
// read ends inside the seeded elements), so a writer that stored a wrong copy would pass every
// read. After the run, for every User.Feed and Room.Members store the run wrote to, the engine's
// bodies are read back with their copies and every copy must equal the document it was taken from:
// the Post of the same write, the member's User document, or for a seeded element the seeded one.

// copyID identifies an element of a store that carries a copy within its store: a feed element by
// the post it names (every post is written once), a member by its key.
func copyID(e model.Element) string {
	if e.Store == "Feed" {
		return bodyString(e, "post")
	}
	return e.Key
}

// noteCopy records the copy a write gave an element of a store that carries one: as acknowledged,
// or as a failed write that may have landed. It holds b.mu through its caller.
func (b *book) noteCopy(op *WriteOp, e model.Element, st *refState, failed bool) {
	if !b.native || !model.IsNativeStore(e.Bucket, e.Store) {
		return
	}
	extra, err := embedFor(*op, &e)
	if err != nil {
		b.embedProblems = append(b.embedProblems, err.Error())
		return
	}
	id, rendered := copyID(e), canonBody(extra)
	if failed {
		if st.copiesTried == nil {
			st.copiesTried = map[string][]string{}
		}
		st.copiesTried[id] = append(st.copiesTried[id], rendered)
		return
	}
	if st.copies == nil {
		st.copies = map[string]string{}
	}
	st.copies[id] = rendered
}

// seededCopies is the copy every seeded element of a store should carry, by copyID, taken from the
// seeded document the copy stands for.
func seededCopies(ds *Dataset, ref StoreRef) (map[string]string, error) {
	out := map[string]string{}
	for _, e := range ds.elements(ref) {
		var extra []model.Field
		var err error
		if ref.Store == "Feed" {
			post, ok := ds.doc("Post", bodyString(e, "post"))
			if !ok {
				return nil, fmt.Errorf("the seeded feed of %s names post %q, which the model does not hold", ref.Owner, bodyString(e, "post"))
			}
			extra, err = model.FeedEmbed(post)
		} else {
			user, ok := ds.doc("User", e.Key)
			if !ok {
				return nil, fmt.Errorf("the seeded members of %s name user %q, which the model does not hold", ref.Owner, e.Key)
			}
			extra, err = model.MemberEmbed(user)
		}
		if err != nil {
			return nil, err
		}
		out[copyID(e)] = canonBody(extra)
	}
	return out, nil
}

// auditEmbeds reads back every store with copies that the run wrote to and returns what is wrong
// with its copies: a body that is not the native shape, a copy that is not the one its source
// gives (a seeded element's, an acknowledged write's, or a failed write's), and an acknowledged
// write whose element is gone.
func (b *book) auditEmbeds(ctx context.Context, layout model.Layout, ops LayoutOps) ([]string, error) {
	b.mu.Lock()
	problems := append([]string(nil), b.embedProblems...)
	var refs []StoreRef
	for ref, st := range b.state {
		if st.layout == layout && model.IsNativeStore(ref.Bucket, ref.Store) && (len(st.copies) > 0 || len(st.copiesTried) > 0) {
			refs = append(refs, ref)
		}
	}
	b.mu.Unlock()
	if len(refs) == 0 {
		return problems, nil
	}
	reader, ok := ops.(EmbedReader)
	if !ok {
		return append(problems, fmt.Sprintf("the adapter does not implement EmbedReader, so the embedded copies of the %d stores written in %s cannot be read back", len(refs), layout)), nil
	}
	sort.Slice(refs, func(i, j int) bool { return fmt.Sprint(refs[i]) < fmt.Sprint(refs[j]) })
	ds := b.w.sets[layout]
	for _, ref := range refs {
		call, cancel := b.bounded(ctx)
		got, err := reader.ReadEmbeds(call, ref)
		err = b.deadlineHint(call, err)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("audit of %s: reading back the embedded copies of %s/%s/%s: %w", layout, ref.Bucket, ref.Owner, ref.Store, err)
		}
		seeded, err := seededCopies(ds, ref)
		if err != nil {
			return nil, fmt.Errorf("audit of %s: %w", layout, err)
		}
		b.mu.Lock()
		st := b.state[ref]
		copies, tried, uncertain := st.copies, st.copiesTried, st.uncertain
		b.mu.Unlock()
		name := fmt.Sprintf("%s/%s/%s", ref.Bucket, ref.Owner, ref.Store)
		present := map[string]bool{}
		for _, e := range got {
			common, _, err := model.Strip(e)
			if err != nil {
				problems = append(problems, fmt.Sprintf("%s holds an element that is not native: %v", name, err))
				continue
			}
			id := copyID(common)
			present[id] = true
			have := canonBody(e.Body[len(common.Body):])
			want, wasWritten := copies[id]
			switch {
			case wasWritten && have == want:
			case seeded[id] == have && seeded[id] != "":
			case containsString(tried[id], have):
			case wasWritten:
				problems = append(problems, fmt.Sprintf("%s element %q holds the embedded copy %s, the document it was taken from gives %s", name, id, have, want))
			case seeded[id] != "":
				problems = append(problems, fmt.Sprintf("%s seeded element %q holds the embedded copy %s, the seeded document gives %s", name, id, have, seeded[id]))
			default:
				problems = append(problems, fmt.Sprintf("%s element %q holds the embedded copy %s and nobody wrote it", name, id, have))
			}
		}
		if !uncertain {
			for id := range copies {
				if !present[id] {
					problems = append(problems, fmt.Sprintf("%s lost the element %q, which a write acknowledged with its embedded copy", name, id))
				}
			}
		}
	}
	sort.Strings(problems)
	return problems, nil
}

func containsString(list []string, want string) bool {
	for _, have := range list {
		if have == want {
			return true
		}
	}
	return false
}
