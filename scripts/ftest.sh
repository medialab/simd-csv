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
cargo build --release --example passthrough

COUNT=target/release/examples/count
INTEGRITY=target/release/examples/integrity
PASSTHROUGH=target/release/examples/passthrough

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
  echo
  echo `$INTEGRITY $path | md5sum` baseline reader checksum
  echo `$INTEGRITY --simd $path | md5sum` simd reader checksum
  echo `cat $path | md5sum` baseline passthrough checksum
  echo `$PASSTHROUGH --simd $path | md5sum` simd passthrough checksum

  echo
done