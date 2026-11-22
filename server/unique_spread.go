package server

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/hoarddb/hoarddb-server/store"
)

// spreadPlacementWord finds a body's own "placement: spread", which the
// create line to run turns into whole instead of writing the option twice.
var spreadPlacementWord = regexp.MustCompile(`placement\s*:\s*spread`)

// uniqueOnSpreadRefusal refuses a create that declares a unique field other
// than _id_ on a bucket recorded spread (document placement spec 8.3): the
// parts of a spread bucket are separate stores on separate nodes, and no part
// can see another's values, so the rule could not be checked. The text names
// the create that works, built from this declaration. Nil when specs declare
// no such field. The unique index of a path is unusable and never enforced,
// so it is not refused here.
func uniqueOnSpreadRefusal(req CreateBucketRequest, specs []store.IndexSpec) *CreateBucketResponse {
	for _, spec := range usableIndexSpecs(specs) {
		if !spec.Unique {
			continue
		}
		names := spec.FieldNames()
		if len(names) == 1 && names[0] == "_id_" {
			continue
		}
		state := "declared spread (placement: spread)"
		// Reachable only while defaultHashPlacement is spread (MVP step 7);
		// until then a create naming no placement is whole and never gets here.
		if _, named := req.Options["placement"]; !named {
			state = "spread by default (this create named no placement)"
		}
		return &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
			"Cannot declare %s unique on bucket %q (database %q): %s is %s, "+
				"and a unique field other than _id_ is refused on a spread bucket, because no part of it can see the values in the others. "+
				"Declare it whole:\n  create bucket %s in %s %s",
			strings.Join(names, ", "), req.Name, req.Database, req.Name, state, req.Name, req.Database, wholeCreateBody(req, specs))}
	}
	return nil
}

// wholeCreateBody is the body of req's create with placement whole: the text
// the caller wrote, when the shell sent one, so the line runs as the caller
// would have typed it, and otherwise the declaration rebuilt from the indexes.
func wholeCreateBody(req CreateBucketRequest, specs []store.IndexSpec) string {
	body := strings.TrimSpace(req.SchemaJSON)
	if strings.HasPrefix(body, "{") && strings.HasSuffix(body, "}") {
		inner := strings.TrimSpace(body[1 : len(body)-1])
		switch {
		case spreadPlacementWord.MatchString(inner):
			inner = spreadPlacementWord.ReplaceAllString(inner, "placement: whole")
		case inner == "":
			inner = "placement: whole"
		default:
			inner = "placement: whole, " + inner
		}
		return "{ " + inner + " }"
	}
	indices := make([]string, 0, len(specs))
	for _, spec := range specs {
		names := spec.FieldNames()
		quoted := make([]string, len(names))
		for position, name := range names {
			quoted[position] = fmt.Sprintf("%q", name)
		}
		field := fmt.Sprintf("field: %s", quoted[0])
		if len(names) > 1 {
			field = fmt.Sprintf("fields: [%s]", strings.Join(quoted, ", "))
		}
		switch {
		case spec.Unique:
			indices = append(indices, "{ "+field+", unique: true }")
		case len(names) == 1:
			indices = append(indices, quoted[0])
		default:
			indices = append(indices, "{ "+field+" }")
		}
	}
	return "{ placement: whole, indices: [" + strings.Join(indices, ", ") + "] }"
}

// existingPlacementNote is the sentence a create on an existing hash bucket
// adds when it asked for a placement the bucket does not have: the create
// still answers success, as a replay's does, but must not read as if it had
// been applied. Empty when the create asked for none or for the recorded one.
func existingPlacementNote(req CreateBucketRequest, recorded string) string {
	asked, _ := req.Options["placement"].(string)
	if recorded == "" || asked == "" || asked == recorded {
		return ""
	}
	return fmt.Sprintf("\nPlacement is %s; this create asked for %s and it was not changed. A bucket's placement is fixed when it is created. "+
		"To change it, in this shell (the create line below names only the placement: add the bucket's fields and indexes inside the braces, "+
		"because a restore into an existing bucket does not declare them):\n  dump --out ./dump-%s %s %s\n  drop bucket %s in %s\n  create bucket %s in %s { placement: %s }\n  restore --dir ./dump-%s\n"+
		"A dump does not carry blob payloads or blob store contents; copy those out first.",
		recorded, asked, req.Name, req.Database, req.Name, req.Name, req.Database, req.Name, req.Database, asked, req.Name)
}
