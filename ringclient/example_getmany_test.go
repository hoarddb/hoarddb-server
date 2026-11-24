package ringclient_test

import (
	"context"
	"fmt"
	"log"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ExampleBucket_GetMany reads a user's feed the way an application does: one page of the user's own
// feed store (post ids), then every post of the page in one frame instead of one Get each. It needs
// a server with the social layout of the how-to page (`hoarddb seed social`), so it is compiled by
// go test and not run.
func ExampleBucket_GetMany() {
	ctx := context.Background()
	client, err := ringclient.Dial(ctx, ringclient.Options{
		Addr: "localhost:7433", User: "root", Password: "the root password", Insecure: true, // local development only
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("closing the client: %v", err)
		}
	}()

	page, err := client.Bucket("scenarios_social", "User").StoreList(ctx, "u7", "Feed", 20, nil)
	if err != nil {
		log.Fatal(err)
	}
	ids := make([]string, len(page.Elements))
	for i, entry := range page.Elements {
		ids[i] = entry.Lookup("post").StringValue()
	}

	answers, err := client.Bucket("scenarios_social", "Post").GetMany(ctx, ids, nil)
	if err != nil {
		log.Fatal(err) // the whole call failed: the connection, or a refusal that names no key
	}
	for i, answer := range answers { // one answer per id, in the order of ids
		switch {
		case answer.Err != nil:
			fmt.Println(ids[i], "was refused:", answer.Err) // that key only; the others are answered
		case !answer.Found:
			fmt.Println(ids[i], "is gone") // a missing document is data, not an error
		default:
			fmt.Println(ids[i], answer.Doc.Lookup("Text").StringValue())
		}
	}
}
