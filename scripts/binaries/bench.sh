#!/bin/bash
set -uoe pipefail

sep() {
    printf %"$(tput cols)"s | tr " " "-"
    echo
}

BINARIES=$(ls scripts/binaries/*.bin | paste -sd ,)

for file in $(ls data | grep -v articles);
do
    sep
    echo Benching $file
    sep
    echo

    hyperfine \
        --warmup 1 \
        -L bin "$BINARIES" \
        "{bin} copy data/$file > /dev/null"

    sep
    echo
    echo
done
