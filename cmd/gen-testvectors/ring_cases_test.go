package main

import (
	"bytes"
	"encoding/hex"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	hc "github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringOpcodes are the opcodes each of which needs a request, a reply, an error and a refusal.
var ringOpcodes = []transport.Opcode{transport.OpClusterProbe, transport.OpRingList, transport.OpRingCount, transport.OpRingKeyed,
	transport.OpRingBatch, transport.OpRingGetMany, transport.OpRingAdmin, transport.OpRingTopology}

func ringVectorBytes(t *testing.T, name, text string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(text)
	if err != nil {
		t.Fatalf("vector %q: %v", name, err)
	}
	return raw
}

// refusalOpcode is the opcode whose decoder a reply refusal is read by: a reply refusal is
// named for the opcode it answers, and the RING_REPLY ones ride under RING_LIST.
func refusalOpcode(name string) transport.Opcode {
	switch {
	case strings.HasPrefix(name, "cluster probe"):
		return transport.OpClusterProbe
	case strings.HasPrefix(name, "ring batch reply"):
		return transport.OpRingBatch
	case strings.HasPrefix(name, "ring reply"):
		return transport.OpRingList
	}
	return transport.OpRingTopology
}

// TestRingVectorsReplayThroughTheServersDecoders: [GUARD every committed ring request and reply is read by the
// server's own strict decoder, every refusal vector is refused by it, and every ring opcode has a
// request, a reply, an error and a refusal].
func TestRingVectorsReplayThroughTheServersDecoders(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	type coverage struct{ reply, failure, refusal int }
	seen := map[transport.Opcode]*coverage{}
	for _, op := range ringOpcodes {
		seen[op] = &coverage{}
	}
	for _, v := range file.Vectors {
		if v.Group != ringGroup {
			continue
		}
		switch v.Kind {
		case "exchange":
			op := transport.Opcode(v.Request.Opcode)
			decoder, ok := ringDecoders[op]
			if !ok {
				t.Errorf("vector %q: no decoder for %s", v.Name, op)
				continue
			}
			// A RING_BATCH or RING_GET_MANY answered INVALID_ARGUMENT is the refusal of a whole frame that does not decode;
			// every other exchange answers a request the decoder accepts.
			refusedWhole := (op == transport.OpRingBatch || op == transport.OpRingGetMany) && v.Error != nil && v.Error.Code == int(hc.CodeInvalidArgument)
			if err := decoder.request(ringVectorBytes(t, v.Name, v.Request.PayloadHex)); (err != nil) != refusedWhole {
				t.Errorf("vector %q: the request decodes with error %v, want it refused only for a RING_BATCH or RING_GET_MANY answered INVALID_ARGUMENT", v.Name, err)
			}
			if v.Response.RequestIDHex != v.Request.RequestIDHex {
				t.Errorf("vector %q: the answer carries another request id", v.Name)
			}
			if transport.Opcode(v.Response.Opcode) == transport.OpError {
				seen[op].failure++
				if v.Error == nil || v.Error.Code == 0 {
					t.Errorf("vector %q: an error answer without its error", v.Name)
				}
				if _, err := wire.DecodeRingError(ringVectorBytes(t, v.Name, v.Response.PayloadHex)); err != nil {
					t.Errorf("vector %q: the error payload is refused: %v", v.Name, err)
				}
				continue
			}
			seen[op].reply++
			if err := decoder.reply(ringVectorBytes(t, v.Name, v.Response.PayloadHex)); err != nil {
				t.Errorf("vector %q: the reply is refused: %v", v.Name, err)
			}
		case "refuse":
			switch v.Refuse.Direction {
			case "request":
				op := transport.Opcode(v.Request.Opcode)
				if err := ringDecoders[op].request(ringVectorBytes(t, v.Name, v.Request.PayloadHex)); err == nil {
					t.Errorf("vector %q: the request is accepted", v.Name)
				}
				seen[op].refusal++
			case "reply":
				op := refusalOpcode(v.Name)
				if err := ringDecoders[op].reply(ringVectorBytes(t, v.Name, v.Response.PayloadHex)); err == nil {
					t.Errorf("vector %q: the reply is accepted", v.Name)
				}
				seen[op].refusal++
			case "error":
				if _, err := wire.DecodeRingError(ringVectorBytes(t, v.Name, v.Response.PayloadHex)); err == nil {
					t.Errorf("vector %q: the error payload is accepted", v.Name)
				}
			default:
				t.Errorf("vector %q: direction %q", v.Name, v.Refuse.Direction)
			}
		default:
			t.Errorf("ring vector %q has kind %q", v.Name, v.Kind)
		}
	}
	for _, op := range ringOpcodes {
		got := seen[op]
		if got.reply == 0 || got.refusal == 0 || got.failure == 0 && op != transport.OpClusterProbe {
			t.Errorf("%s: %d replies, %d errors, %d refusals; each needs at least one", op, got.reply, got.failure, got.refusal)
		}
	}
}

// ringFieldNamed is the field a FIELD_TYPE message names in its first quotes.
var ringFieldNamed = regexp.MustCompile(`field "([^"]+)"`)

// TestRingFieldTypeRefusalsNameAFieldTheRequestNames: [GUARD every ring refusal whose message names a field carries
// a request that names that field, so a driver replaying the vector sees the refusal the message describes].
func TestRingFieldTypeRefusalsNameAFieldTheRequestNames(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, v := range file.Vectors {
		if v.Group != ringGroup || v.Kind != "exchange" || v.Error == nil || v.Error.Code != int(hc.CodeFieldType) {
			continue
		}
		failure, err := wire.DecodeRingError(ringVectorBytes(t, v.Name, v.Response.PayloadHex))
		if err != nil {
			t.Fatalf("vector %q: the error payload is refused: %v", v.Name, err)
		}
		match := ringFieldNamed.FindStringSubmatch(failure.Message)
		if match == nil {
			t.Errorf("vector %q: a FIELD_TYPE message that names no field: %q", v.Name, failure.Message)
			continue
		}
		checked++
		request := ringVectorBytes(t, v.Name, v.Request.PayloadHex)
		if !bytes.Contains(request, []byte(match[1])) {
			t.Errorf("vector %q: the message names %q but the request does not name it", v.Name, match[1])
		}
	}
	if checked == 0 {
		t.Fatal("no ring FIELD_TYPE refusal vector was checked")
	}
}

// TestTopologyVectorsCarryTheRankingNotTheCopySet: [GUARD the committed 3-member reply lists the down member and
// every slot ranks all three, so a driver must truncate by RF itself].
func TestTopologyVectorsCarryTheRankingNotTheCopySet(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range file.Vectors {
		if v.Name != "ring topology: the first call after AUTH" {
			continue
		}
		reply, err := wire.DecodeRingTopologyReply(ringVectorBytes(t, v.Name, v.Response.PayloadHex))
		if err != nil {
			t.Fatal(err)
		}
		members := reply.Table.Members()
		if len(members) != 3 || members[1].State != hc.MemberDown {
			t.Fatalf("members %+v", members)
		}
		for slot := range uint16(8192) {
			if got := len(reply.Table.Ranking(slot)); got != 3 {
				t.Fatalf("slot %d ranks %d names, want all 3", slot, got)
			}
		}
		return
	}
	t.Fatal("the topology vector is missing")
}

// TestClientsTreeImportsNoBSLPackage is the check CLAUDE.md asks for: nothing under clients/
// (Apache-2.0) imports a package of the BSL server module. A Go source file is parsed for its
// imports, and a Go module there is asked for its dependencies with go list -deps.
func TestClientsTreeImportsNoBSLPackage(t *testing.T) {
	clients := filepath.Join(filepath.Dir(committedVectorsPath), "..", "..")
	if _, err := os.Stat(clients); err != nil {
		t.Skipf("published server tree: no clients/ tree to check (%v)", err)
	}
	const serverModule = "hoardDB"
	checked := 0
	err := filepath.WalkDir(clients, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		switch {
		case strings.HasSuffix(path, ".go"):
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Errorf("%s: %v", path, err)
				return nil
			}
			for _, spec := range parsed.Imports {
				imported := strings.Trim(spec.Path.Value, `"`)
				if imported == serverModule || strings.HasPrefix(imported, serverModule+"/") {
					t.Errorf("%s imports %s: clients/ is Apache-2.0 and must never import the BSL server module", path, imported)
				}
			}
			checked++
		case entry.Name() == "go.mod":
			command := exec.Command("go", "list", "-deps", "./...")
			command.Dir = filepath.Dir(path)
			deps, err := command.Output()
			if err != nil {
				t.Errorf("%s: go list -deps: %v", path, err)
				return nil
			}
			for _, dep := range strings.Fields(string(deps)) {
				if dep == serverModule || strings.HasPrefix(dep, serverModule+"/") {
					t.Errorf("%s depends on %s: clients/ must never import a BSL package", path, dep)
				}
			}
			checked++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("checked %d Go files and modules under clients/", checked)
}

// TestDriverVectorsCarryTheExpandedPage: [GUARD the second driver is held to an expanded page, a row
// whose far document is gone, and the options trailer of a get_fields].
func TestDriverVectorsCarryTheExpandedPage(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"ring list: expand an author field": false, "ring keyed: a get_fields with expand and local": false}
	for _, v := range file.Vectors {
		if _, ok := want[v.Name]; ok {
			want[v.Name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("the committed vectors have no %q: run make vectors", name)
		}
	}
}
