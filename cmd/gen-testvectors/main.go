// Command gen-testvectors writes clients/testdata/driver-vectors/v1.json, the
// frozen wire vectors every hoardDB driver is built against
// (clients/handbook/testing.md section 1; the driver-go-build record Step 1).
//
// It drives real servers — one in this process, and a three-node cluster of
// processes built from this commit — with the server's own request types and
// its own BSON encoder, and records what they answer. The output is protocol
// data, licensed Apache-2.0 under clients/; this program imports server
// types and stays in the server's tree.
//
//	go run ./cmd/gen-testvectors -out ../clients/testdata/driver-vectors
//	go run ./cmd/gen-testvectors -check ../clients/testdata/driver-vectors/v1.json
//
// -bson-out and -bson-check do the same for the BSON codec corpus,
// clients/testdata/bson-vectors/v1.json (bsoncorpus.go), which needs no server.
// -trust-out and -trust-check do the same for the known_servers trust corpus,
// clients/testdata/trust-vectors/v1.json (trustcorpus.go), which needs no
// server either.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// vectorFileName is the one file the generator writes.
const vectorFileName = "v1.json"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "gen-testvectors: %v\n", err)
		os.Exit(1)
	}
}

// run parses the flags and either writes the file or checks a committed one.
func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("gen-testvectors", flag.ContinueOnError)
	flags.SetOutput(stderr)
	outDir := flags.String("out", "", "directory to write "+vectorFileName+" into (clients/testdata/driver-vectors)")
	checkPath := flags.String("check", "", "regenerate in memory and compare with this committed file instead of writing")
	bsonOutDir := flags.String("bson-out", "", "directory to write the BSON codec corpus "+bsonCorpusFileName+" into (clients/testdata/bson-vectors); needs no server")
	bsonCheckPath := flags.String("bson-check", "", "regenerate the BSON codec corpus in memory and compare with this committed file instead of writing")
	trustOutDir := flags.String("trust-out", "", "directory to write the known_servers trust corpus "+trustCorpusFileName+" into (clients/testdata/trust-vectors); needs no server")
	trustCheckPath := flags.String("trust-check", "", "regenerate the known_servers trust corpus in memory and compare with this committed file instead of writing")
	keepGoing := flags.Bool("keep-going", false, "capture every exchange and report every failure, instead of stopping at the first")
	if err := flags.Parse(args); err != nil {
		return err
	}
	modes := 0
	for _, given := range []string{*outDir, *checkPath, *bsonOutDir, *bsonCheckPath, *trustOutDir, *trustCheckPath} {
		if given != "" {
			modes++
		}
	}
	if modes > 1 {
		return fmt.Errorf("give exactly one of -out, -check, -bson-out, -bson-check, -trust-out or -trust-check")
	}
	if *bsonOutDir != "" || *bsonCheckPath != "" {
		return runBSONCorpus(*bsonOutDir, *bsonCheckPath, stdout)
	}
	if *trustOutDir != "" || *trustCheckPath != "" {
		return runTrustCorpus(*trustOutDir, *trustCheckPath, stdout)
	}
	if (*outDir == "") == (*checkPath == "") {
		return fmt.Errorf("give exactly one of -out <dir> or -check <file>")
	}
	if *outDir != "" {
		if err := refuseURIVectors(*outDir); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	generated, err := generateFile(ctx, generateOptions{log: stderr, keepGoing: *keepGoing})
	if err != nil {
		return err
	}

	if *checkPath != "" {
		committed, err := os.ReadFile(*checkPath)
		if err != nil {
			return fmt.Errorf("reading the committed vectors: %w", err)
		}
		differences, err := compareVectorFiles(committed, generated)
		if err != nil {
			return err
		}
		if len(differences) > 0 {
			return fmt.Errorf("%s does not match the server at this commit:\n  %s\nregenerate with 'make vectors' and review the diff: a payload change is a change to every driver",
				*checkPath, joinLines(differences))
		}
		if err := checkConsumeConstants(*checkPath); err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s and %s match the server\n", *checkPath, constantsFileName)
		return err
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", *outDir, err)
	}
	target := filepath.Join(*outDir, vectorFileName)
	if err := os.WriteFile(target, generated, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", target, err)
	}
	constants, err := writeConsumeConstants(*outDir)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "wrote %s (%d bytes) and %s\n", target, len(generated), constants)
	return err
}

// refuseURIVectors is the guard driver-spec section 4.1 asks for: the URI
// vectors are hand-maintained, and a generator pointed at them is one
// mistyped flag from destroying them.
func refuseURIVectors(outDir string) error {
	resolved, err := filepath.Abs(outDir)
	if err != nil {
		return fmt.Errorf("resolving -out %q: %w", outDir, err)
	}
	if strings.Contains(resolved, "uri-vectors") {
		return fmt.Errorf("refusing -out %s: clients/testdata/uri-vectors is hand-maintained (a URI never crosses the wire, so there is nothing to generate it from) and this generator never writes there; use -out clients/testdata/driver-vectors", outDir)
	}
	return nil
}
