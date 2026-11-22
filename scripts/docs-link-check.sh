#!/usr/bin/env bash
# docs-link-check.sh — every relative markdown link in docs/ must resolve.
#
# Moving 30 documents at once is exactly the situation where "I think the links
# are fine" stops being good enough: a link that used to resolve and now does not
# fails silently for the reader, and nothing else in the toolchain notices.
#
# DOCS_LINK_EXTRA="README.md SECURITY.md" also checks those files (paths relative
# to the tree root). The published tree uses it for its root files; the private
# repository leaves it unset.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

broken="$(mktemp)"
checked=0

while IFS= read -r doc; do
  dir="$(dirname "$doc")"
  # Local markdown targets only: skip URLs, anchors-only links and mailto.
  while IFS= read -r target; do
    [ -n "$target" ] || continue
    case "$target" in
      http://* | https://* | mailto:* | "#"*) continue ;;
    esac
    target="${target%%#*}"          # drop any #anchor
    [ -n "$target" ] || continue
    checked=$((checked + 1))
    if [ ! -e "$dir/$target" ]; then
      printf '%s -> %s\n' "$doc" "$target" >>"$broken"
    fi
  done < <(grep -oE '\]\([^)]+\)' "$doc" 2>/dev/null | sed -e 's/^](//' -e 's/)$//' || true)
done < <({ find docs -name '*.md'; for extra in ${DOCS_LINK_EXTRA:-}; do [ -f "$extra" ] && echo "$extra"; done; } | sort)

if [ -s "$broken" ]; then
  printf 'broken links:\n'
  sed 's/^/  /' "$broken"
  printf '\ndocs-link-check: FAIL (%s broken, %s checked)\n' "$(wc -l <"$broken")" "$checked"
  rm -f "$broken"
  exit 1
fi

rm -f "$broken"
printf 'docs-link-check: ok (%s links checked)\n' "$checked"
