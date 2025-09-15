#!/bin/bash
set -uoe pipefail

FILES=(blogs)

# Building engines
cd benchmark/engines

echo "Building rust_csv"
cd rust_csv
cargo build --release

echo "Building rust_simd_csv"
cd ../rust_simd_csv
cargo build --release
cd ../../..

for file in ${FILES[@]};
do
  path=benchmark/data/$file.csv

  echo "Bench for $file"
  echo

  hyperfine \
    --warmup 1 \
    "cat $path > /dev/null"

  echo
done