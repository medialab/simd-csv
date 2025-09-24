#!/bin/bash
set -uoe pipefail

FILES=(
  series.csv
  blogs.csv
  articles.csv
  mediapart.tsv
  numbers.csv
  random.csv
  range.csv
  worst-case.csv
  ndjson-scam.csv
)

# Building
cargo build --release --example count
PROG=target/release/examples/count

for file in ${FILES[@]};
do
  path=data/$file

  echo "Bench for $file"
  echo

  hyperfine \
    --warmup 1 \
    "$PROG baseline $path" \
    "$PROG simd $path" \
    "$PROG zero-copy $path"

  printf %"$(tput cols)"s | tr " " "-"
  echo
  echo
done