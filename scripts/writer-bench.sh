#!/bin/bash
set -uoe pipefail

# Building
cargo build --release --example passthrough
PROG=target/release/examples/passthrough

# Warmup
cat $1 > /dev/null

echo Read only
time $PROG --only-read $1
echo

echo Read+Write
time $PROG $1 > /dev/null
echo

echo SIMD Read only
time $PROG --simd --only-read $1
echo

echo SIMD Read+Write
time $PROG --simd $1 > /dev/null
echo