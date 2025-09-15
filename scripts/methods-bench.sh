#!/bin/bash
set -uoe pipefail

# Building
cargo build --release --example count
PROG=target/release/examples/count

# Warmup
cat $1 > /dev/null

echo Baseline
time $PROG $1
echo

echo SIMD
time $PROG --simd $1
echo

echo Split
time $PROG --simd --split $1
echo

echo Mmap
time $PROG --mmap $1
echo