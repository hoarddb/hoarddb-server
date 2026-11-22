#!/usr/bin/env bash
#
# ring-encryption-acceptance-lib.sh - the markers and the checks of the ring
# encryption acceptance run, sourced by scripts/ring-encryption-acceptance.sh and
# by its selftest, scripts/ring-encryption-acceptance-selftest.sh, so the
# selftest exercises the very functions the run trusts. Source it; do not run it.
#
# The marker helpers are copied from the earlier engine's
# encryption-acceptance-markers.sh, which goes with that engine's acceptance
# script (ring encryption at rest spec section 9.3).
#
# Why markers are not just random: a store compresses its blocks, and a marker is
# only visible to grep if it is stored contiguously. Two strings that share a
# prefix or a suffix can be stored as a back-reference, so grep -F for the whole
# marker would find nothing and read as "sealed". The markers are built to be
# uncompressible against each other: upper-case letters only (nothing else the
# run stores has three upper-case letters in a row), pairwise distinct first and
# last characters, and no three-character run shared between any two markers or
# repeated inside one.

# uppercase_marker prints 18 random upper-case letters.
uppercase_marker() {
  local letters
  letters=$(head -c 4096 /dev/urandom | LC_ALL=C tr -dc 'A-Z')
  printf '%s' "${letters:0:18}"
}

# markers_are_apart MARKER... succeeds when no two markers can be merged by
# block compression (see above).
markers_are_apart() {
  local -A first_seen=() last_seen=() run_seen=()
  local marker index run
  for marker in "$@"; do
    [ -z "${first_seen[${marker:0:1}]:-}" ] || return 1
    [ -z "${last_seen[${marker: -1}]:-}" ] || return 1
    first_seen[${marker:0:1}]=1
    last_seen[${marker: -1}]=1
    for ((index = 0; index <= ${#marker} - 3; index++)); do
      run=${marker:index:3}
      [ -z "${run_seen[$run]:-}" ] || return 1
      run_seen[$run]=1
    done
  done
}

# marker_set COUNT prints COUNT markers, space separated, that satisfy
# markers_are_apart.
marker_set() {
  local count=$1 candidate index
  while :; do
    candidate=""
    for ((index = 0; index < count; index++)); do
      candidate="$candidate $(uppercase_marker)"
    done
    # shellcheck disable=SC2086 # the markers are the words
    markers_are_apart $candidate && { printf '%s\n' "${candidate# }"; return 0; }
  done
}

# marker_files DIR MARKER... prints, one per line, "<path relative to DIR>
# <marker>" for every file under DIR that holds one of the markers. grep -a,
# because the store's files are binary and a grep that skips binary files proves
# nothing. Prints nothing when no file holds any.
marker_files() {
  local dir=$1 marker file
  shift
  for marker in "$@"; do
    while IFS= read -r file; do
      [ -n "$file" ] && printf '%s %s\n' "${file#"$dir"/}" "$marker"
    done < <(LC_ALL=C grep -a -r -l -F -- "$marker" "$dir" 2>/dev/null || true)
  done
}

# check_sealed_ring LEG DATA_DIR MARKER...: the ring of a sealed node holds no
# marker in its store or its log, and nothing else in the data directory holds
# one either (the credentials are sealed and the ring metadata holds names and
# ids, not the markers). Prints PASS, or a FAIL that names every file and marker,
# and returns non-zero on a FAIL. A data directory without ring/badger or
# ring/wal fails: a scan of a directory the node never wrote proves nothing.
check_sealed_ring() {
  local leg=$1 data_dir=$2 found part
  shift 2
  for part in ring/badger ring/wal; do
    if [ ! -d "$data_dir/$part" ] || [ -z "$(ls -A "$data_dir/$part" 2>/dev/null)" ]; then
      fail "$leg" "a ring store and log to scan under $data_dir" "$part is missing or empty"
      return 1
    fi
  done
  found=$(marker_files "$data_dir" "$@")
  if [ -n "$found" ]; then
    fail "$leg" "no marker in any file under $data_dir" "$(printf '%s' "$found" | tr '\n' ';')"
    return 1
  fi
  pass "$leg" "no marker in $data_dir ($(find "$data_dir/ring/badger" "$data_dir/ring/wal" -type f | wc -l) store and log files scanned, and the rest of the data directory)"
}

# check_plaintext_ring LEG DATA_DIR MARKER...: the positive control. The ring of
# a plaintext node holds the first marker in its store and in its log, each on
# its own; if it does not, the scans of the sealed nodes are blind and the run
# fails.
check_plaintext_ring() {
  local leg=$1 data_dir=$2 marker=$3 part
  for part in ring/badger ring/wal; do
    if [ -z "$(marker_files "$data_dir/$part" "$marker")" ]; then
      fail "$leg" "the marker $marker in the plaintext node's $part, or the scans are blind" "no file under $data_dir/$part holds it"
      return 1
    fi
  done
  pass "$leg" "the plaintext node's ring/badger and ring/wal each hold the marker: the scans can see"
}

# sealed_file_purpose FILE prints the purpose byte (two hex digits) of a file
# sealed under the node master (magic 00 48 44 42, version 02, purpose), or
# nothing when the file is not sealed.
sealed_file_purpose() {
  local header
  header=$(head -c 6 "$1" 2>/dev/null | od -An -tx1 | tr -d ' \n')
  case "$header" in
    0048444202??) printf '%s' "${header:10:2}" ;;
  esac
}

# check_sealed_file LEG FILE PURPOSE: FILE exists and is sealed for PURPOSE.
check_sealed_file() {
  local leg=$1 file=$2 want=$3 got
  if [ ! -f "$file" ]; then
    fail "$leg" "$file sealed (purpose 0x$want)" "the file does not exist"
    return 1
  fi
  got=$(sealed_file_purpose "$file")
  if [ "$got" != "$want" ]; then
    fail "$leg" "$file sealed (purpose 0x$want)" "${got:+sealed for purpose 0x$got}${got:-not sealed: its first bytes are $(head -c 6 "$file" | od -An -tx1 | tr -s ' ')}"
    return 1
  fi
  pass "$leg" "$(basename "$file") is sealed under the node master (purpose 0x$want)"
}

# metrics_encryption METRICS_TEXT prints "<mode> <sealed>" from a node's
# /metrics: the mode label of hoarddb_node_encryption_info and the value of
# hoarddb_node_encryption_sealed. Prints "missing missing" for what it cannot find.
metrics_encryption() {
  local text=$1 mode sealed
  mode=$(printf '%s\n' "$text" | sed -n 's/^hoarddb_node_encryption_info{.*mode="\([a-z]*\)".*} 1$/\1/p' | head -1)
  sealed=$(printf '%s\n' "$text" | sed -n 's/^hoarddb_node_encryption_sealed \([0-9]\)$/\1/p' | head -1)
  printf '%s %s\n' "${mode:-missing}" "${sealed:-missing}"
}

# tree_digest DIR prints one digest of every file's path, size and content under
# DIR, so a refused start can be shown to leave the ring as it was.
tree_digest() {
  (cd "$1" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 -r sha256sum | sha256sum | cut -d' ' -f1)
}

# check_exported_key LEG FILE DATA_DIR: FILE is what -export-key must leave: a
# regular file (not a link) of 32 bytes, readable by its owner only, outside
# DATA_DIR. A copy of the node master inside the data directory, or one other
# users can read, is a key anyone with the disk can use.
check_exported_key() {
  local leg=$1 file=$2 data_dir=$3 size mode
  if [ -L "$file" ]; then
    fail "$leg" "the exported node master at $file, a regular file" "a symbolic link"
    return 1
  fi
  if [ ! -f "$file" ]; then
    fail "$leg" "the exported node master at $file" "no such file"
    return 1
  fi
  case "$(realpath "$file")" in
    "$(realpath "$data_dir")"/*)
      fail "$leg" "the exported node master outside the data directory" "$file is inside $data_dir"
      return 1
      ;;
  esac
  size=$(stat -c %s "$file")
  mode=$(stat -c %a "$file")
  if [ "$size" != 32 ] || [ "$mode" != 600 ]; then
    fail "$leg" "an export of 32 bytes with mode 600" "$size bytes, mode $mode"
    return 1
  fi
  pass "$leg" "-export-key wrote the 32-byte node master, mode 600, outside the data directory"
}

# read_back_fault EXPECTED FILE prints what is wrong with a read-back, or nothing
# when it is whole. EXPECTED lists the documents written, one "<id> <note>" per
# line. FILE is the CLI's answers: the bucket's count() on the first line, then
# one line per document, then the store reads. Whole means: no error, the count
# equals the number of documents written, and every document is there once with
# its own note. An empty EXPECTED or an empty FILE is a fault: nothing compared
# is nothing proved.
read_back_fault() {
  local expected=$1 file=$2 want id note line
  want=$(grep -c . "$expected" || true)
  if [ "$want" -eq 0 ]; then echo "no document was expected, so nothing would be compared"; return 0; fi
  if [ ! -s "$file" ]; then echo "nothing was read back"; return 0; fi
  if grep -q 'CLI error' "$file"; then
    echo "an error in place of an answer: $(grep -m1 'CLI error' "$file" | cut -c1-200)"
    return 0
  fi
  if [ "$(head -1 "$file")" != "$want" ]; then
    echo "count() answered \"$(head -1 "$file" | cut -c1-80)\", and $want documents were written"
    return 0
  fi
  while read -r id note; do
    line=$(grep -F -- "\"_id_\": \"$id\"" "$file" || true)
    if [ -z "$line" ]; then echo "document $id was not read back"; return 0; fi
    if [ "$(printf '%s\n' "$line" | wc -l)" -ne 1 ]; then echo "document $id was read back more than once"; return 0; fi
    case "$line" in
      *"\"Note\": \"$note\""*) ;;
      *) echo "document $id does not carry its note $note: ${line:0:200}"; return 0 ;;
    esac
  done <"$expected"
}

# check_read_back_whole LEG EXPECTED BEFORE AFTER: the read-back taken AFTER a
# recovery is whole (read_back_fault) and is, byte for byte, the one taken
# BEFORE the loss, which must be whole too: the same count, the same bodies and
# the same store values.
check_read_back_whole() {
  local leg=$1 expected=$2 before=$3 after=$4 fault want
  fault=$(read_back_fault "$expected" "$before")
  if [ -n "$fault" ]; then
    fail "$leg" "a whole read-back before the loss, to compare with" "$fault"
    return 1
  fi
  fault=$(read_back_fault "$expected" "$after")
  if [ -n "$fault" ]; then
    fail "$leg" "the count and every document body read back after the recovery" "$fault"
    return 1
  fi
  if ! cmp -s "$before" "$after"; then
    fail "$leg" "the same answers after the recovery as before the loss" \
      "$(diff "$before" "$after" | grep -m2 '^[<>]' | cut -c1-150 | tr '\n' ' ')"
    return 1
  fi
  want=$(grep -c . "$expected")
  pass "$leg" "count() $want, all $want document bodies and the store reads are the same after the recovery as before the loss"
}
