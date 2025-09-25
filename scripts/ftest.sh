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
cargo build --release --example integrity
COUNT=target/release/examples/count
INTEGRITY=target/release/examples/integrity

for file in ${FILES[@]};
do
  path=data/$file

  echo $file

  echo `$COUNT baseline $path` -- baseline
  echo `$COUNT simd $path` -- simd
  echo `$COUNT split $path` -- splits
  echo `$COUNT mmap $path` -- mmap
  echo `$COUNT zero-copy --check-alignment $path` -- zero-copy
  echo `$COUNT copy $path` -- copy

  echo `$INTEGRITY $path | md5sum`
  echo `$INTEGRITY --simd $path | md5sum`

  echo
done