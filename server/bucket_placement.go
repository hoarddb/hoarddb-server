package server

import "fmt"

// placementLabel is the placement a bucket reports: "spread" or "whole" for a
// hash bucket, and "" for every other type, which is always whole and records
// nothing.
func placementLabel(bucketType BucketType, spread bool) string {
	if bucketType != BucketTypeHash {
		return ""
	}
	if spread {
		return placementSpread
	}
	return placementWhole
}

// defaultHashPlacement is the placement a hash bucket gets when its create
// names none. It is whole for now: the router routes keyed operations of a
// spread bucket into parts but refuses count, find, distinct, search, describe,
// dump, alter, declare-stores and migrate on it until the fan-in reads land.
// MVP step 7 of the spread collection work flips it back to
// placementSpread, which is the intended end state. This is the only place the
// default is decided.
const defaultHashPlacement = placementWhole

// createPlacement reads the placement a create asks for from its options
// (document placement spec §3.2). An absent option is defaultHashPlacement for
// a hash bucket, applied here so that a client, a replica applying the logged create
// and a replay all reach the same value. The label is "" for a bucket that is
// not a hash bucket: placement whole is accepted there and records nothing,
// placement spread is refused naming the type. A non-nil response is the
// refusal.
func createPlacement(req CreateBucketRequest) (string, *CreateBucketResponse) {
	if req.PartOf != 0 {
		return partPlacement(req)
	}
	asked, present := req.Options["placement"]
	word, isText := asked.(string)
	if present && (!isText || (word != placementSpread && word != placementWhole)) {
		return "", &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
			"bucket %q: placement must be spread or whole, got %v. "+
				"Name spread or whole: create bucket %s { placement: spread }",
			req.Name, asked, req.Name)}
	}
	if req.Type != BucketTypeHash {
		if word == placementSpread {
			return "", &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
				"bucket %q: placement spread is refused on a %s bucket; only hash buckets spread. "+
					"A %s bucket is always whole. Create it without placement, or as a hash bucket: create bucket %s { type: hash }",
				req.Name, req.Type, req.Type, req.Name)}
		}
		return "", nil
	}
	if word == "" {
		word = defaultHashPlacement
	}
	if word == placementSpread && len(req.Name) > maxSpreadNameBytes {
		return "", &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
			"bucket name %s in database %s is %s bytes, and a spread bucket's name is at most %d (a whole bucket's is %d). "+
				"Use a shorter name, or keep the bucket on one node: create bucket <name> { placement: whole }",
			echoName(req.Name), echoName(req.Database), groupThousands(len(req.Name)), maxSpreadNameBytes, maxNameBytes)}
	}
	return word, nil
}

// partPlacement is the placement of a part's create: always whole, because a
// part is an ordinary bucket that holds its documents itself. It also holds
// the shape of a part: a hash bucket, named collection ‖ 0x1F ‖ hex4(part)
// with the part number the request carries. Asking for spread is refused. Only
// the router sends this create, so the refusal is a backstop for a replica or a
// replay that is sent a malformed one.
func partPlacement(req CreateBucketRequest) (string, *CreateBucketResponse) {
	_, p, shaped := partNameParts(req.Name)
	switch {
	case req.Type != BucketTypeHash:
		return "", &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
			"bucket %q: only a hash bucket can be a part of a spread collection", displayBucket(req.Name))}
	case !shaped || int32(p) != req.Part:
		return "", &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
			"bucket %q: a part's name is its collection and its part number, and the number must be the request's (%d)",
			displayBucket(req.Name), req.Part)}
	}
	if asked, present := req.Options["placement"]; present && asked != placementWhole {
		return "", &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
			"bucket %q: a part of a spread collection is always whole, not %v", displayBucket(req.Name), asked)}
	}
	return placementWhole, nil
}
