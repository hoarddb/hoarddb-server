#!/usr/bin/env bash
#
# encryption-acceptance-markers.sh — marker generation for the encryption
# acceptance run, sourced by encryption-acceptance.sh and its selftest.
#
# Why markers are not just random: the plaintext store compresses its blocks,
# and a marker is only visible to grep if it is stored contiguously. A BSON
# string carries a 4-byte length prefix (\x13\0\0\0 for 18 characters), so two
# 18-character strings that start with the same character share five bytes,
# and the compressor replaces those with a back-reference: the second string
# loses its first character and grep -F for the whole marker finds nothing
# (seen in a kept scratch: "V0MgMoRQ..." stored as "0MgMoRQ..."). The same
# holds at the other end, where a shared last character is followed by the
# same NUL and field bytes. With base64 markers that hid one of four markers
# in roughly 1 run in 20 and read as "the greps are blind".
#
# So the markers are built to be uncompressible against each other: upper-case
# letters only (nothing else stored here, field names and ids included, has
# three upper-case letters in a row), pairwise distinct first characters,
# pairwise distinct last characters, and no three-character run shared
# between any two markers or repeated inside one.

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

# marker_set prints four markers, space separated, that satisfy markers_are_apart.
marker_set() {
  local candidate
  while :; do
    candidate="$(uppercase_marker) $(uppercase_marker) $(uppercase_marker) $(uppercase_marker)"
    # shellcheck disable=SC2086 # the four markers are the words
    markers_are_apart $candidate && { printf '%s\n' "$candidate"; return 0; }
  done
}
