#!/bin/bash
set -uoe pipefail

# Building
cargo build --release --example count
PROG=target/release/examples/count

# Warmup
cat $1 > /dev/null

echo Baseline
time $PROG baseline $1
echo

echo SIMD
time $PROG simd $1
echo

echo Split
time $PROG split $1
echo

echo Mmap
time $PROG mmap $1
echo

echo Zero-copy
time $PROG zero-copy $1
echo

echo Copy
time $PROG copy $1
echo

echo Mmap Copy
time $PROG mmap-copy $1
echo