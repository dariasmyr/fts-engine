#!/bin/sh
set -eu

DEST=${1:-./data/msmarco}

mkdir -p "$DEST"

fetch() {
  url=$1
  out=$2
  if [ -f "$out" ]; then
    return 0
  fi
  tmp="$out.tmp"
  rm -f "$tmp"
  curl -fL "$url" -o "$tmp"
  mv "$tmp" "$out"
}

has_queries_tsv() {
  [ -f "$1" ] && awk -F '\t' 'NF >= 2 { found = 1; exit } END { exit found ? 0 : 1 }' "$1"
}

has_qrels_tsv() {
  [ -f "$1" ] && awk -F '\t' 'NF >= 4 { found = 1; exit } END { exit found ? 0 : 1 }' "$1"
}

build_queries_dev_small() {
  tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/msmarco-queries.XXXXXX")
  archive="$tmpdir/queries.tar.gz"

  curl -fL "https://msmarco.z22.web.core.windows.net/msmarcoranking/queries.tar.gz" -o "$archive"
  tar -xzf "$archive" -C "$tmpdir" queries.dev.tsv
  awk -F '\t' 'NR == FNR { keep[$1] = 1; next } ($1 in keep) { print }' \
    "$DEST/qrels.dev.small.tsv" \
    "$tmpdir/queries.dev.tsv" > "$DEST/queries.dev.small.tsv"

  rm -rf "$tmpdir"
}

fetch "https://msmarco.z22.web.core.windows.net/msmarcoranking/collection.tar.gz" "$DEST/collection.tar.gz"

if ! has_qrels_tsv "$DEST/qrels.dev.small.tsv"; then
  rm -f "$DEST/qrels.dev.small.tsv"
  fetch "https://msmarco.z22.web.core.windows.net/msmarcoranking/qrels.dev.small.tsv" "$DEST/qrels.dev.small.tsv"
fi

if ! has_queries_tsv "$DEST/queries.dev.small.tsv"; then
  build_queries_dev_small
fi

if [ ! -f "$DEST/collection.tsv" ]; then
  tar -xzf "$DEST/collection.tar.gz" -C "$DEST"
fi

printf 'MS MARCO files are ready in %s\n' "$DEST"
