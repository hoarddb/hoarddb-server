package main

import (
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// layoutDatabase is the database a layout is seeded into. Layouts share the bucket name User
// with different fields, so each gets its own database: <base>_<layout>.
func layoutDatabase(base string, layout model.Layout) string { return base + "_" + string(layout) }

// hoardDDL is the hoardDB schema text of a layout: the create database and the create bucket
// statements exactly as the test scenarios and layouts spec section 2 declares them.
func hoardDDL(gen *model.Generator, database string, rf int) string {
	var out strings.Builder
	fmt.Fprintf(&out, "create database %s { replication_factor: %d };\n", database, rf)
	for _, bucket := range gen.Schema() {
		fields := make([]string, len(bucket.Fields))
		for i, field := range bucket.Fields {
			fields[i] = field.Name + " " + field.Type
		}
		fmt.Fprintf(&out, "create bucket %s in %s { %s };\n", bucket.Name, database, strings.Join(fields, ", "))
	}
	return out.String()
}

// createTable renders one table as MySQL DDL: InnoDB, utf8mb4, the primary key and the
// secondary keys, no foreign keys (the application keeps its references, as it does on
// hoardDB).
func createTable(spec table) string {
	lines := make([]string, 0, len(spec.Cols)+1+len(spec.Keys))
	for _, col := range spec.Cols {
		lines = append(lines, fmt.Sprintf("  %s %s", col.Name, col.SQLType))
	}
	lines = append(lines, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(spec.PK, ", ")))
	for _, key := range spec.Keys {
		unique := ""
		if key.Unique {
			unique = "UNIQUE "
		}
		lines = append(lines, fmt.Sprintf("  %sKEY %s (%s)", unique, key.Name, strings.Join(key.Cols, ", ")))
	}
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", spec.Name, strings.Join(lines, ",\n"))
}

// mysqlDDL is the MySQL schema text of a layout, each table under a comment that says which
// read its keys serve.
func mysqlDDL(layout model.Layout, database string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "CREATE DATABASE %s DEFAULT CHARACTER SET utf8mb4;\nUSE %s;\n", database, database)
	for _, spec := range tables(layout) {
		fmt.Fprintf(&out, "-- %s: %s.\n%s\n", spec.Name, spec.Why, createTable(spec))
	}
	return out.String()
}

// mongoIndex is one index a collection needs besides _id.
type mongoIndex struct {
	Keys   []string
	Unique bool
}

// mongoIdentity says whether a collection takes its _id from the id column. Any other table
// is keyed by a unique index on its primary key columns.
func mongoIdentity(spec table) bool { return len(spec.PK) == 1 && spec.PK[0] == "id" }

// mongoIndexes lists the indexes of a collection: the primary key as a unique index unless it
// is _id, then the secondary keys.
func mongoIndexes(spec table) []mongoIndex {
	var out []mongoIndex
	if !mongoIdentity(spec) {
		out = append(out, mongoIndex{Keys: spec.PK, Unique: true})
	}
	for _, key := range spec.Keys {
		out = append(out, mongoIndex{Keys: key.Cols, Unique: key.Unique})
	}
	return out
}

// indexText is the createIndex call of one index.
func (m mongoIndex) text(collection string) string {
	keys := make([]string, len(m.Keys))
	for i, name := range m.Keys {
		keys[i] = name + ": 1"
	}
	options := ""
	if m.Unique {
		options = ", { unique: true }"
	}
	return fmt.Sprintf("db.%s.createIndex({ %s }%s);", collection, strings.Join(keys, ", "), options)
}

// mongoDDL is the MongoDB schema text of a layout: one collection per entity and per store,
// never an array inside the owner's document (an unbounded array grows past the 16 MB document
// limit and rewrites the whole document on every push).
func mongoDDL(layout model.Layout, database string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "use %s;\n", database)
	for _, spec := range tables(layout) {
		fmt.Fprintf(&out, "// %s: %s.\ndb.createCollection(%q);\n", spec.Name, spec.Why, spec.Name)
		for _, idx := range mongoIndexes(spec) {
			fmt.Fprintln(&out, idx.text(spec.Name))
		}
	}
	return out.String()
}

// redisDDL is the key scheme of a layout. Redis has no DDL, so the schema is the list of key
// families, each with the read it serves. database is the key prefix.
func redisDDL(gen *model.Generator, database string) string {
	plan := newRedisPlan(gen, database)
	var out strings.Builder
	fmt.Fprintf(&out, "-- key prefix %s: every key below starts with it; each layout has a prefix of its own inside one logical database\n", database)
	for _, bucket := range gen.Schema() {
		fmt.Fprintf(&out, "-- %s:%s:<id> hash of the scalar fields and the denormalised counters: a read by id is one HGETALL\n", database, bucket.Name)
		for _, field := range bucket.Fields {
			if !field.IsStore() {
				continue
			}
			name := bucket.Name + "." + field.Name
			key := fmt.Sprintf("%s:%s:<id>:%s", database, bucket.Name, field.Name)
			fmt.Fprintf(&out, "-- %s %s\n", key, redisFamily(plan, gen, name))
		}
	}
	return out.String()
}

// redisFamily says which Redis type holds a store and why, in one line.
func redisFamily(plan redisPlan, gen *model.Generator, store string) string {
	var line string
	switch kind := plan.kinds[store]; {
	case plan.sets[store]:
		line = "set of member ids, one set per side of the pair as the application keeps it: SISMEMBER answers membership and SMEMBERS the list; the other side of the pair is a set of its own, and the time of the join is not kept in either"
	case kind == "fifoStore":
		line = "list, RPUSH appends a JSON body and LRANGE reads from the head: oldest first, the order a fifoStore pops"
	case kind == "lifoStore":
		line = "list, LPUSH puts the newest at the head so LRANGE 0 n reads newest first, the order a lifoStore pops"
	case kind == "hashStore":
		line = "hash, element key to JSON body: HGET is the keyed read and HGETALL the list"
	case kind == "btreeStore":
		line = "sorted set, score 0 and the element key as member, so ZRANGEBYLEX walks the keys in order; the bodies are the hash <key>:bodies (element key to JSON)"
	default:
		line = "sorted set scored by priority: ZPOPMIN takes the lowest score first"
	}
	for element := range gen.Elements(store) {
		for _, field := range element.Body {
			if packed, ok := field.Value.([]byte); ok {
				line += fmt.Sprintf("; field %s is the raw %d-byte binary string in the hash <key>:%s (element key to bytes), no index because no verb searches by vector yet", field.Name, len(packed), field.Name)
			}
		}
		break
	}
	return line
}

// kvrocksNote is Kvrocks's section: it takes the Redis keys unchanged.
const kvrocksNote = "-- Kvrocks is the Redis scheme above on RocksDB, same keys and same commands; only the port (6666) and the engine underneath differ"

// ddlText prints the schemas of a layout, one engine after another.
func ddlText(gen *model.Generator, database string, rf int) string {
	layout := gen.Layout()
	var out strings.Builder
	fmt.Fprintf(&out, "-- layout %s, database %s\n\n-- hoardDB\n%s\n-- MySQL\n%s\n-- PostgreSQL\n%s\n-- MongoDB\n%s\n-- Redis\n%s\n-- Kvrocks\n%s\n", layout, database, hoardDDL(gen, database, rf), mysqlDDL(layout, database), postgresDDL(layout, database), mongoDDL(layout, database), redisDDL(gen, database), kvrocksNote)
	return out.String()
}
