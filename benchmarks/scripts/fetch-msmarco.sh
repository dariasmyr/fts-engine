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
  curl -L "$url" -o "$out"
}

fetch "https://msmarco.z22.web.core.windows.net/msmarcoranking/collection.tar.gz" "$DEST/collection.tar.gz"
fetch "https://msmarco.z22.web.core.windows.net/msmarcoranking/queries.dev.small.tsv" "$DEST/queries.dev.small.tsv"
fetch "https://msmarco.z22.web.core.windows.net/msmarcoranking/qrels.dev.small.tsv" "$DEST/qrels.dev.small.tsv"

if [ ! -f "$DEST/collection.tsv" ]; then
  tar -xzf "$DEST/collection.tar.gz" -C "$DEST"
fi

printf 'MS MARCO files are ready in %s\n' "$DEST"
