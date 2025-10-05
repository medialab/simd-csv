#!/bin/bash
set -uoe pipefail

# Building
cargo build --release --example count
PROG=target/release/examples/count

TIMEFORMAT=%3lR
TIME="/usr/bin/time -f %e"

# Warmup
cat $1 > /dev/null

echo Baseline
$TIME $PROG baseline $1 > /dev/null
echo

echo SIMD
$TIME $PROG simd $1 > /dev/null
echo

echo Split
$TIME $PROG split $1 > /dev/null
echo

# echo Mmap
# $TIME $PROG mmap $1 > /dev/null
# echo

echo Zero-copy
$TIME $PROG zero-copy $1 > /dev/null
echo

echo Copy
$TIME $PROG copy $1 > /dev/null
echo

# echo Mmap Copy
# $TIME $PROG mmap-copy $1 > /dev/null
# echo

echo Lines
$TIME $PROG lines $1 > /dev/null
echo