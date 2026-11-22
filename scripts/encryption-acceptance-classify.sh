#!/usr/bin/env bash
#
# encryption-acceptance-classify.sh — the sealed-directory allowlist of
# at-rest-encryption-spec.md §1.2 and §7.2 as one piece of code, sourced by
# scripts/encryption-acceptance.sh and driven by
# server/wal_allowlist_test.go:TestASealedClusterNodeWritesOnlyWALFilesTheSpecLists.
#
# Why it is its own file: the allowlist lived in three forms — the spec table,
# classify() in the acceptance script, and a plain list of names in the Go test.
# The test compared names only, so a file added to the spec and the test passed
# while the acceptance run, the one check that reaches every real file, called
# it UNMATCHED; and a non-empty wal/origin-format passed the test and failed the
# script. With one definition both the script and the test read, the three
# cannot disagree.
#
# classify REL TYPE echoes the allowlist class of one path, or UNMATCHED. The
# classes are rev 4 §1.2 and §7.2 rule 1; each path must match exactly one.
# See the spec for what each class means.

# sealed_class FILE PURPOSE: the file opens with the rev 2+ magic, version 0x02
# and the purpose byte.
sealed_class() {
  local want="0048444202$2" got
  got=$(head -c 6 "$1" | od -An -tx1 | tr -d ' \n')
  [ "$got" = "$want" ]
}

# classify REL TYPE echoes the allowlist class of one path, or UNMATCHED. The
# classes are rev 4 §1.2 and §7.2 rule 1; each path must match exactly one.
classify() {
  local rel=$1 type=$2 path="$sealed_dir/$1"
  if [ "$type" = d ]; then
    case "$rel" in
      keys|store|wal|blobs) echo "dir"; return ;;
      blobs/??|blobs/??/??) echo "dir"; return ;;
      keys/*) [ -f "$path/authorized_keys" ] && { echo "dir"; return; } ;;
    esac
    echo UNMATCHED; return
  fi
  case "$rel" in
    *.tmp|.users-*.json|catalog.json|replication_state.bson|replication_state.bson.*) echo "absent-violated"; return ;;
    users.json|users.json.prev) sealed_class "$path" 01 && echo "sealed:0x01" || echo "sealed:0x01-VIOLATED"; return ;;
    keys/node-master.key.wrapped) sealed_class "$path" 02 && echo "sealed:0x02" || echo "sealed:0x02-VIOLATED"; return ;;
    keys/store.wrapped) sealed_class "$path" 03 && echo "sealed:0x03" || echo "sealed:0x03-VIOLATED"; return ;;
    wal/keys.wrapped) sealed_class "$path" 08 && echo "sealed:0x08" || echo "sealed:0x08-VIOLATED"; return ;;
    keys/blobs.wrapped) sealed_class "$path" 08 && echo "sealed:0x08" || echo "sealed:0x08-VIOLATED"; return ;;
    blobs/??/??/???-????????????????-????????-????-????-????-????????????) sealed_class "$path" 06 && echo "sealed:0x06" || echo "sealed:0x06-VIOLATED"; return ;;
    wal/wal_[0-9]*.log) sealed_class "$path" 04 && echo "sealed:0x04" || echo "sealed:0x04-VIOLATED"; return ;;
    store/MANIFEST|store/KEYREGISTRY|store/DISCARD|store/LOCK|store/*.sst|store/*.vlog|store/*.mem) echo "badger"; return ;;
    auth.token.key) sealed_class "$path" 09 && echo "sealed:0x09" || echo "sealed:0x09-VIOLATED"; return ;;
    cluster.token) sealed_class "$path" 0a && echo "sealed:0x0a" || echo "sealed:0x0a-VIOLATED"; return ;;
    keys/server.key) sealed_class "$path" 0b && echo "sealed:0x0b" || echo "sealed:0x0b-VIOLATED"; return ;;
    root.password|keys/server.crt|keys/*/authorized_keys) echo "keymaterial"; return ;;
    membership.json|wal/state.bson|store/memtable-size|store/memtable-choice.json) echo "metadata"; return ;;
    wal/origin-format|wal/segment-format-2) [ -s "$path" ] && echo "metadata-NOT-EMPTY-VIOLATED" || echo "metadata"; return ;;
    wal/incarnation.bson) echo "metadata"; return ;;
  esac
  echo UNMATCHED
}
