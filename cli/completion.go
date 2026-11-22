package cli

import (
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/termio"
)

// completionWords is every top-level word a command can start with, plus the
// words that follow them. It is deliberately a flat list of the vocabulary the
// parser accepts rather than a grammar: completion's job is to save typing a
// word that exists, and a wrong suggestion costs a Backspace.
var completionWords = append([]string{
	// Global
	"use", "show", "describe", "status", "encryption", "nodes", "help", "exit", "quit",
	"connect", "master", "migrate", "write-concern",
	"consistent-get", "join", "remove-node", "set", "batch",
	"databases", "buckets", "bucket", "database", "in", "banner",
	"pretty", "extjson", "pager", "on", "off",
	// Schema
	"create", "drop", "alter",
	// Users
	"user", "users", "role", "grant", "revoke", "to", "from", "password",
	"disable", "enable", "roles", "list",
	// Bucket verbs
	"insert", "upsert", "find", "get", "put", "delete", "update", "search", "count",
	"length", "push", "pop", "peek", "range", "first", "last", "stats", "info",
	"claim", "ack", "release", "extend", "dead", "requeue", "discard",
	// Store fields: the per-document verbs the bucket verbs lack, and the
	// six engine words a declaration names. The engine words are derived from
	// the table in store_types.go — they are the keyword plus the Store
	// suffix, which is what the declaration parser requires, so writing them
	// out here was a second list that could drift from the parser.
	"len", "list",
	// Types and literals that appear as bare words. The six store-type
	// keywords come from the table in store_types.go, so completion and the
	// parser cannot disagree about a type's spelling.
	//
	// `with` is deliberately absent: no parser path accepts a
	// `with` clause, and its only use was to finish `create database <name>
	// with replication_factor N` — the clause that was accepted and discarded
	// and is now refused. `replication_factor` stays, because the bucket form
	// `create bucket X { replication_factor: N }` does parse.
	"type", "indices", "replication_factor", "one", "majority", "all",
}, append(storeTypeKeywords(), storeFieldNames()...)...)

// completionTopics are the arguments `help <topic>` accepts. Derived from the
// store-type table plus the non-store topics, matching the help-topic list.
var completionTopics = helpTopicList()

// completer returns the Tab-completion function for this session. It completes
// the word under the cursor against the command vocabulary, and knows two
// context-sensitive cases: the topic after `help`, and the bucket name after
// `db.`.
func (c *CLI) completer() termio.Completer {
	return func(line string, pos int) (int, []string) {
		if pos > len(line) {
			pos = len(line)
		}
		start := wordStart(line, pos)
		prefix := line[start:pos]

		// `help <tab>` completes topics, not verbs.
		if fields := strings.Fields(line[:start]); len(fields) == 1 && fields[0] == "help" {
			return start, filterPrefix(completionTopics, prefix)
		}

		// A bucket name is only known to the server, so `db.<tab>` lists the
		// buckets in the current database rather than guessing. A failure here
		// (no session, no database selected) simply yields no candidates.
		if strings.HasPrefix(prefix, "db.") {
			return start, c.bucketCandidates(prefix)
		}

		return start, filterPrefix(completionWords, prefix)
	}
}

// wordStart returns the rune offset where the word ending at pos begins. A word
// ends at a space or at one of the delimiters a command can carry.
func wordStart(line string, pos int) int {
	for i := pos; i > 0; i-- {
		switch line[i-1] {
		case ' ', '\t', '(', ')', '{', '}', ',', ';':
			return i
		}
	}
	return 0
}

// filterPrefix returns the candidates that start with prefix, sorted and
// de-duplicated. An empty prefix matches everything, which is what makes a bare
// Tab list the vocabulary.
func filterPrefix(candidates []string, prefix string) []string {
	seen := make(map[string]struct{}, len(candidates))
	var out []string
	for _, candidate := range candidates {
		if !strings.HasPrefix(candidate, prefix) {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	sort.Strings(out)
	return out
}

// bucketCandidates completes `db.<partial>` with the buckets in the current
// database. It asks the server, so a session with no connection or no selected
// database returns nothing instead of an error the user did not ask for.
func (c *CLI) bucketCandidates(prefix string) []string {
	if c.handler == nil {
		return nil
	}
	buckets, err := c.handler.BucketNames(c.currentDB)
	if err != nil {
		return nil
	}
	partial := strings.TrimPrefix(prefix, "db.")
	var out []string
	for _, bucket := range buckets {
		candidate := "db." + bucket
		if strings.HasPrefix(candidate, prefix) {
			out = append(out, candidate)
			continue
		}
		// A dotted bucket name (`db.Post.Comments`, one flat bucket named
		// "Post.Comments" — not a sub-bucket) completes on the leaf too.
		if strings.HasPrefix(bucket, partial) {
			out = append(out, candidate)
		}
	}
	sort.Strings(out)
	return out
}
