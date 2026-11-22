#!/usr/bin/env bash
#
# durability-drill-lib.sh - the checks of the durability drill that do not need
# a server, sourced by scripts/durability-drill.sh and by its selftest,
# scripts/durability-drill-selftest.sh, so the selftest exercises the very
# functions the drill trusts. Source it; do not run it.

# drill_fstype DIR: the filesystem type DIR is on, or "missing".
drill_fstype() {
  stat -f -c %T "$1" 2>/dev/null || echo missing
}

# drill_fstype_name DIR: the same, for a report line. stat -f says ext2/ext3
# for ext2, ext3 and ext4 alike, so the line says that instead of guessing.
drill_fstype_name() {
  local fstype
  fstype=$(drill_fstype "$1")
  case "$fstype" in
    ext2/ext3) echo "ext2/ext3/ext4 (stat -f does not tell them apart)" ;;
    *) echo "$fstype" ;;
  esac
}

# drill_on_disk DIR: DIR exists, is writable, and is not on tmpfs or ramfs. A
# data directory in memory survives kill -9 whatever the server does, so a drill
# on one proves nothing about what the server wrote to a disk.
drill_on_disk() {
  [ -d "$1" ] && [ -w "$1" ] || return 1
  case "$(drill_fstype "$1")" in
    tmpfs | ramfs | missing) return 1 ;;
  esac
  return 0
}

# drill_disk_refusal DIR: the text that refuses DIR, naming the fix.
drill_disk_refusal() {
  printf '%s\n' "--dir $1 is not a writable directory on a real disk (filesystem: $(drill_fstype "$1")). A data directory on tmpfs or ramfs lives in memory, so every write survives kill -9 whether or not the server made it durable, and the drill would pass without testing anything. Name a directory on a disk: scripts/durability-drill.sh --dir /var/tmp"
}

# drill_is_count VALUE: VALUE is a whole number of at least 1.
drill_is_count() {
  case "$1" in
    '' | *[!0-9]*) return 1 ;;
  esac
  [ "$1" -ge 1 ]
}

# drill_delay_tenths MIN MAX: a delay between MIN and MAX seconds, in tenths of
# a second, drawn afresh each call so the kill lands at a different point of the
# load in every round.
drill_delay_tenths() {
  local low=$(($1 * 10)) high=$(($2 * 10))
  echo $((low + RANDOM % (high - low + 1)))
}

# drill_writer_count FIELD TEXT: one count of the line 'durabilitydrill write'
# prints ("round 2: sent 120, acknowledged 117, unknown 3"); FIELD is sent,
# acknowledged or unknown. Prints nothing when the line is absent.
drill_writer_count() {
  sed -n "s/^round [0-9]*:.* $1 \\([0-9][0-9]*\\).*/\\1/p" <<<"$2" | tail -n 1
}

# drill_result_count FIELD TEXT: one count of the judge's report, read from the
# line that starts with FIELD ("missing", "acknowledged", "never sent", ...).
# Prints nothing when the line is absent, so a caller comparing with 0 fails.
drill_result_count() {
  sed -n "s/^$1  *\\([0-9][0-9]*\\).*/\\1/p" <<<"$2" | head -n 1
}

# drill_report_passes STATUS TEXT: the judge exited 0, printed RESULT PASS and
# no RESULT FAIL, and its report carries the counts a pass needs: at least one
# acknowledged write, every one present, none missing, no duplicate, nothing
# never sent. The exit status alone is not trusted, and neither is the word.
drill_report_passes() {
  local status=$1 text=$2 acknowledged present missing duplicates never_sent
  [ "$status" -eq 0 ] || return 1
  grep -q '^RESULT PASS' <<<"$text" || return 1
  ! grep -q '^RESULT FAIL\|^FAIL' <<<"$text" || return 1
  acknowledged=$(drill_result_count acknowledged "$text")
  present=$(drill_result_count present "$text")
  missing=$(drill_result_count missing "$text")
  duplicates=$(drill_result_count duplicates "$text")
  never_sent=$(drill_result_count 'never sent' "$text")
  drill_is_count "$acknowledged" || return 1
  [ "$present" = "$acknowledged" ] && [ "$missing" = 0 ] && [ "$duplicates" = 0 ] && [ "$never_sent" = 0 ]
}
