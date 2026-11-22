package server

import (
	"fmt"
	"slices"
)

// compiledProjection is a projection whose keys are parsed into paths once
// per query (typed-fields spec §4.9.6). Projection is a map of key to 1
// (include) or 0 (exclude); a projection mixing the two is read as the
// inclusion of its 1s, as it always was.
//
// An included path is copied with its parents: {Address: {City: …}}. An
// excluded path removes that member only. Several positions of one array
// merge into one array of those elements in ascending position order, each
// holding only its projected members; positions are not kept, so the answer's
// Lines[0] is the stored Lines[1] when only Lines[1] is projected.
type compiledProjection struct {
	include bool
	root    *projectionNode
}

// projectionNode is one level of the projected paths: the value whole, or
// the members and positions below it that the projection names.
type projectionNode struct {
	whole     bool
	members   map[string]*projectionNode
	positions map[int]*projectionNode
	// order is the positions in ascending order, for the merged array.
	order []int
}

// compileProjection parses a projection's keys once for a query. A key that
// is not a path is refused before the bucket is walked.
func compileProjection(projection map[string]int) (*compiledProjection, error) {
	if len(projection) == 0 {
		return nil, nil
	}
	hasInclusion := false
	for _, value := range projection {
		if value > 0 {
			hasInclusion = true
		}
	}
	compiled := &compiledProjection{include: hasInclusion, root: &projectionNode{}}
	for key, value := range projection {
		if hasInclusion && value <= 0 {
			continue
		}
		path, err := parseFieldPath(key)
		if err != nil {
			return nil, fmt.Errorf("the projection's key %w. Write a path as names joined by \".\", with 0-based positions in brackets, for example Address.City or Lines[3].Qty", err)
		}
		segs := path.segs
		if segs == nil {
			segs = []pathSeg{{name: path.text}}
		}
		compiled.root.add(segs)
	}
	compiled.root.sortPositions()
	return compiled, nil
}

// add records one path below the node. A value projected whole absorbs every
// deeper path into it.
func (n *projectionNode) add(segs []pathSeg) {
	if n.whole {
		return
	}
	if len(segs) == 0 {
		n.whole, n.members, n.positions = true, nil, nil
		return
	}
	var child *projectionNode
	if segs[0].isIndex {
		if n.positions == nil {
			n.positions = make(map[int]*projectionNode)
		}
		if child = n.positions[segs[0].index]; child == nil {
			child = &projectionNode{}
			n.positions[segs[0].index] = child
		}
	} else {
		if n.members == nil {
			n.members = make(map[string]*projectionNode)
		}
		if child = n.members[segs[0].name]; child == nil {
			child = &projectionNode{}
			n.members[segs[0].name] = child
		}
	}
	child.add(segs[1:])
}

func (n *projectionNode) sortPositions() {
	for position, child := range n.positions {
		n.order = append(n.order, position)
		child.sortPositions()
	}
	slices.Sort(n.order)
	for _, child := range n.members {
		child.sortPositions()
	}
}

// apply returns the projection of doc as a new document. doc is never
// changed: an excluded member is removed from copies of the values on its
// path.
func (p *compiledProjection) apply(doc map[string]interface{}) map[string]interface{} {
	if p.include {
		projected, _ := p.root.include(doc)
		document, _ := projected.(map[string]interface{})
		if document == nil {
			document = map[string]interface{}{}
		}
		return document
	}
	return p.root.exclude(doc).(map[string]interface{})
}

// include is the part of value the node names, and whether any of it is
// present. A parent appears only when a projected value below it does.
func (n *projectionNode) include(value interface{}) (interface{}, bool) {
	if n.whole {
		return value, true
	}
	if elements, isArray := decodedArray(value); isArray && len(n.order) > 0 {
		var merged []interface{}
		for _, position := range n.order {
			if position >= len(elements) {
				continue
			}
			if element, present := n.positions[position].include(elements[position]); present {
				merged = append(merged, element)
			}
		}
		return merged, merged != nil
	}
	if len(n.members) == 0 {
		return nil, false
	}
	var projected map[string]interface{}
	for name, child := range n.members {
		member, found := pathStep(value, pathSeg{name: name})
		if !found {
			continue
		}
		if kept, present := child.include(member); present {
			if projected == nil {
				projected = make(map[string]interface{}, len(n.members))
			}
			projected[name] = kept
		}
	}
	return projected, projected != nil
}

// exclude is value without the members and positions the node names. It
// copies only the documents and arrays on an excluded path.
func (n *projectionNode) exclude(value interface{}) interface{} {
	if elements, isArray := decodedArray(value); isArray && len(n.order) > 0 {
		kept := make([]interface{}, 0, len(elements))
		for position, element := range elements {
			child, named := n.positions[position]
			switch {
			case !named:
				kept = append(kept, element)
			case !child.whole:
				kept = append(kept, child.exclude(element))
			}
		}
		return kept
	}
	document, isDocument := value.(map[string]interface{})
	if !isDocument || len(n.members) == 0 {
		return value
	}
	copied := make(map[string]interface{}, len(document))
	for name, member := range document {
		child, named := n.members[name]
		switch {
		case !named:
			copied[name] = member
		case !child.whole:
			copied[name] = child.exclude(member)
		}
	}
	return copied
}
