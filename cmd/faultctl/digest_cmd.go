package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// runDigest is faultctl digest: it digests one stopped node's store and writes the file.
func runDigest(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("digest", flag.ContinueOnError)
	storeDir := flags.String("store", "", "the stopped node's store directory, <data_dir>/ring/badger (required)")
	node := flags.String("node", "", "name to give this node in a compare (default: the host name)")
	outPath := flags.String("out", "", "digest file to write; - for standard output (required)")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing digest flags: %w", err)
	}
	if *storeDir == "" || *outPath == "" {
		return fmt.Errorf("digest needs --store and --out, for example: faultctl digest --store /var/lib/hoarddb/ring/badger --node node1 --out node1.json")
	}
	if outsideStore, err := insideDirectory(*outPath, *storeDir); err != nil {
		return err
	} else if outsideStore {
		return fmt.Errorf("--out %s is inside the store directory %s, and this tool never writes there. Write the digest file somewhere else, for example --out /tmp/node1.json", *outPath, *storeDir)
	}
	nodeName := *node
	if nodeName == "" {
		hostName, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("finding this host's name for --node: %w. Pass --node NAME", err)
		}
		nodeName = hostName
	}
	digest, err := digestStore(*storeDir, nodeName)
	if err != nil {
		return err
	}
	if *outPath == "-" {
		return printDigestJSON(out, digest)
	}
	if err := writeDigestFile(*outPath, digest); err != nil {
		return err
	}
	var liveKeys uint64
	for _, pair := range digest.Pairs {
		liveKeys += pair.LiveKeys
	}
	_, err = fmt.Fprintf(out, "digested %d pairs and %d live keys of node %s into %s%s\n",
		len(digest.Pairs), liveKeys, nodeName, *outPath, describeSkipped(digest.SkippedKeys))
	return err
}

// insideDirectory reports whether path lies under dir. "-" (standard output) never does.
func insideDirectory(path, dir string) (bool, error) {
	if path == "-" {
		return false, nil
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, fmt.Errorf("resolving --out %s: %w", path, err)
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return false, fmt.Errorf("resolving --store %s: %w", dir, err)
	}
	return strings.HasPrefix(absPath, absDir+string(filepath.Separator)), nil
}

func printDigestJSON(out io.Writer, digest digestFile) error {
	encoded, err := encodeDigest(digest)
	if err != nil {
		return err
	}
	if _, err := out.Write(encoded); err != nil {
		return fmt.Errorf("printing the digest: %w", err)
	}
	return nil
}

func describeSkipped(skipped map[string]uint64) string {
	if len(skipped) == 0 {
		return ""
	}
	labels := make([]string, 0, len(skipped))
	for label := range skipped {
		labels = append(labels, label)
	}
	slices.Sort(labels)
	parts := make([]string, len(labels))
	for index, label := range labels {
		parts[index] = fmt.Sprintf("%s %d", label, skipped[label])
	}
	return "; keys left out as node-local: " + strings.Join(parts, ", ")
}

// dirFlags collects repeated --dir NAME=DIR values.
type dirFlags map[string]string

func (dirs dirFlags) String() string { return fmt.Sprint(map[string]string(dirs)) }

func (dirs dirFlags) Set(value string) error {
	name, dir, found := strings.Cut(value, "=")
	if !found || name == "" || dir == "" {
		return fmt.Errorf("--dir wants NAME=DIR, for example --dir node1=/var/lib/hoarddb/ring/badger, got %q", value)
	}
	dirs[name] = dir
	return nil
}

// runCompare is faultctl compare: it judges two or three digest files and returns an error
// unless every shared pair is equal at the same applied position.
func runCompare(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	keyLimit := flags.Int("keys", 0, "with --dir, list up to N differing keys of each different pair")
	showAll := flags.Bool("all", false, "list the equal pairs too")
	dirs := dirFlags{}
	flags.Var(dirs, "dir", "NAME=DIR: the stopped store directory of a node, for --keys; repeat for each node")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing compare flags: %w", err)
	}
	files, err := loadDigestFiles(flags.Args())
	if err != nil {
		return err
	}
	verdicts := compareDigests(files)
	if len(verdicts) == 0 {
		return fmt.Errorf("no pair is held by two of %s, so there is nothing to compare: check that the digests are of nodes of the same cluster", describeNodes(files))
	}
	counts := map[verdictKind]int{}
	for _, verdict := range verdicts {
		counts[verdict.Kind]++
		if verdict.Kind == verdictEqual && !*showAll {
			continue
		}
		if _, err := fmt.Fprintf(out, "slot %d database %d: %s: %s\n", verdict.Slot, verdict.Database, verdict.Kind, verdict.Detail); err != nil {
			return fmt.Errorf("printing the verdict: %w", err)
		}
		if verdict.Kind == verdictDifferent && *keyLimit > 0 {
			if err := printDifferences(out, verdict, dirs, *keyLimit); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintf(out, "%d pairs equal, %d different, %d not comparable\n",
		counts[verdictEqual], counts[verdictDifferent], counts[verdictNotComparable]); err != nil {
		return fmt.Errorf("printing the summary: %w", err)
	}
	if single := singleHolderPairs(files); single > 0 {
		if _, err := fmt.Fprintf(out, "%d pairs are held by only one of these nodes and were not compared\n", single); err != nil {
			return fmt.Errorf("printing the summary: %w", err)
		}
	}
	if counts[verdictDifferent]+counts[verdictNotComparable] > 0 {
		return fmt.Errorf("%d pairs differ and %d are not comparable", counts[verdictDifferent], counts[verdictNotComparable])
	}
	return nil
}

func loadDigestFiles(paths []string) ([]digestFile, error) {
	if len(paths) < 2 || len(paths) > 3 {
		return nil, fmt.Errorf("compare needs two or three digest files, got %d. For example: faultctl compare node1.json node2.json", len(paths))
	}
	files := make([]digestFile, 0, len(paths))
	named := map[string]string{}
	for _, path := range paths {
		file, err := readDigestFile(path)
		if err != nil {
			return nil, err
		}
		if earlier, found := named[file.Node]; found {
			return nil, fmt.Errorf("%s and %s are both digests of node %q: digest each node with its own --node name", earlier, path, file.Node)
		}
		named[file.Node] = path
		files = append(files, file)
	}
	return files, nil
}

func describeNodes(files []digestFile) string {
	names := make([]string, len(files))
	for index, file := range files {
		names[index] = file.Node
	}
	return strings.Join(names, ", ")
}

// printDifferences lists the first keys the first node of a different pair does not share with
// each other node, when the directories of both are known.
func printDifferences(out io.Writer, verdict pairVerdict, dirs dirFlags, limit int) error {
	reference := verdict.Nodes[0]
	for _, other := range verdict.Nodes[1:] {
		referenceDir, haveReference := dirs[reference]
		otherDir, haveOther := dirs[other]
		if !haveReference || !haveOther {
			if _, err := fmt.Fprintf(out, "  to list differing keys of %s and %s give --dir %s=DIR --dir %s=DIR\n", reference, other, reference, other); err != nil {
				return fmt.Errorf("printing the verdict: %w", err)
			}
			continue
		}
		differences, err := firstDifferences(referenceDir, otherDir, verdict.Slot, verdict.Database, limit)
		if err != nil {
			return err
		}
		for _, difference := range differences {
			if _, err := fmt.Fprintf(out, "  %s vs %s: %s\n", reference, other, difference.Describe(reference, other)); err != nil {
				return fmt.Errorf("printing the verdict: %w", err)
			}
		}
	}
	return nil
}
