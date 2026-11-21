#!/bin/bash
set -uoe pipefail

sep() {
    printf %"$(tput cols)"s | tr " " "-"
    echo
}

BINARIES=$(ls scripts/binaries/*.bin)

for file in $(ls data | grep -v articles);
do
    echo Benching $file
    sep
    echo

    hyperfine \
        --warmup 1 \
        -L bin "$BINARIES" \
        "{bin} copy data/$file > /dev/null"
done
