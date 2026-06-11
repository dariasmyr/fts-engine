#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
OUT_DIR=${OUT_DIR:-"$ROOT/results/full"}
WORK_DIR=${WORK_DIR:-"$ROOT/work/full"}
ENGINES=${ENGINES:-"fts-engine,bleve,bluge"}
MSMARCO_DIR=${MSMARCO_DIR:-""}
BENCH_CMD=${BENCH_CMD:-"go run ./cmd/bench"}

mkdir -p "$OUT_DIR" "$WORK_DIR"

run_bench() {
  name=$1
  shift
  (
    cd "$ROOT"
    sh -c "$BENCH_CMD \"\$@\"" sh "$@" -engines="$ENGINES" -out="$OUT_DIR/$name.json"
  )
}

if [ -n "$MSMARCO_DIR" ]; then
  run_bench var-1 -dataset=msmarco -msmarco-dir="$MSMARCO_DIR" -max-docs=100000 -max-queries=2000 -k=10 -work="$WORK_DIR/var-1"
  run_bench var-2 -dataset=msmarco -msmarco-dir="$MSMARCO_DIR" -max-docs=100000 -max-queries=2000 -k=10 -work="$WORK_DIR/var-2"
  run_bench var-3 -dataset=msmarco -msmarco-dir="$MSMARCO_DIR" -max-docs=100000 -max-queries=2000 -k=10 -work="$WORK_DIR/var-3"

  run_bench conc-c1 -dataset=msmarco -msmarco-dir="$MSMARCO_DIR" -max-docs=100000 -max-queries=2000 -k=10 -concurrency=1 -work="$WORK_DIR/conc-c1"
  run_bench conc-c4 -dataset=msmarco -msmarco-dir="$MSMARCO_DIR" -max-docs=100000 -max-queries=2000 -k=10 -concurrency=4 -work="$WORK_DIR/conc-c4"
  run_bench conc-c8 -dataset=msmarco -msmarco-dir="$MSMARCO_DIR" -max-docs=100000 -max-queries=2000 -k=10 -concurrency=8 -work="$WORK_DIR/conc-c8"

  run_bench scale-25k -dataset=msmarco -msmarco-dir="$MSMARCO_DIR" -max-docs=25000 -max-queries=2000 -k=10 -work="$WORK_DIR/scale-25k"
  run_bench scale-100k -dataset=msmarco -msmarco-dir="$MSMARCO_DIR" -max-docs=100000 -max-queries=2000 -k=10 -work="$WORK_DIR/scale-100k"
  run_bench scale-250k -dataset=msmarco -msmarco-dir="$MSMARCO_DIR" -max-docs=250000 -max-queries=2000 -k=10 -work="$WORK_DIR/scale-250k"
fi

run_bench synthetic -dataset=synthetic -synth-docs=100000 -synth-queries=2000 -k=10 -work="$WORK_DIR/synthetic"

printf 'suite results written to %s\n' "$OUT_DIR"
