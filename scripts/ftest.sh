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
)

# Building
cargo build --release --example count
PROG=target/release/examples/count

for file in ${FILES[@]};
do
  path=data/$file

  echo $file

  echo `$PROG baseline $path` -- baseline
  echo `$PROG simd $path` -- simd
  echo `$PROG split $path` -- splits
  echo `$PROG mmap $path` -- mmap
  echo `$PROG zero-copy $path` -- zero-copy

  echo
done