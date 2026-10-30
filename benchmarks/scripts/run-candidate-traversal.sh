#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
LABEL=${1:-candidate-traversal-baseline}
OUT_DIR=${OUT_DIR:-"$ROOT/benchmarks/results/$LABEL"}
BENCHTIME=${BENCHTIME:-200ms}
COUNT=${COUNT:-6}

mkdir -p "$OUT_DIR"

{
  printf 'label: %s\n' "$LABEL"
  printf 'timestamp: '
  date -u '+%Y-%m-%dT%H:%M:%SZ'
  printf 'commit: '
  git -C "$ROOT" rev-parse HEAD
  printf 'go: '
  go version
  printf 'system: '
  uname -a
  printf 'working-tree:\n'
  git -C "$ROOT" status --short
} > "$OUT_DIR/environment.txt"

run_benchmark() {
  name=$1
  shift
  printf 'running %s\n' "$name"
  (
    cd "$ROOT"
    go test "$@" -benchmem -benchtime="$BENCHTIME" -count="$COUNT"
  ) > "$OUT_DIR/$name.txt"
}

run_benchmark service-e2e \
  ./pkg/fts -run '^$' -bench '^BenchmarkServiceCandidateSelectivity$'

run_benchmark service-prepared \
  ./pkg/fts -run '^$' -bench '^BenchmarkServicePreparedCandidateSelectivity$'

run_benchmark candidate-preparation \
  ./pkg/fts -run '^$' -bench '^BenchmarkCandidateSetPreparation$'

run_benchmark direct-and \
  ./pkg/fts -run '^$' -bench '^BenchmarkBooleanAndPostingIntersect$'

for candidates in 4 100 1000 5000; do
  run_benchmark "ftse-fpr1-$candidates" \
    ./pkg/presence -run '^$' \
    -bench "BenchmarkCandidateMatrix/postings=10000$/candidates=${candidates}$/terms=(1|2|5)$/(ftse_reader_reused|ftse_reader_reopened|fpr1)$"
done

printf 'candidate traversal results written to %s\n' "$OUT_DIR"
