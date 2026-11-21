#!/bin/bash
set -uoe pipefail

sep() {
    printf %"$(tput cols)"s | tr " " "-"
    echo
}

rm -rf scripts/binaries/results
mkdir -p scripts/binaries/results

BINARIES=$(ls scripts/binaries/*.bin | paste -sd ,)

FILES=(
    # articles.csv
    blogs.csv
    crlf.csv
    mediapart.tsv
    ndjson-scam.csv
    numbers.csv
    quote-always.csv
    random.csv
    range.csv
    religions.csv
    # series.csv
    tweets.csv
    worldcitiespop.csv
    worst-case.csv
)

for file in ${FILES[@]};
do
    sep
    echo $file
    sep
    echo

    wc -l data/$file > /dev/null

    hyperfine \
        --warmup 1 \
        -L bin "$BINARIES" \
        --export-csv "scripts/binaries/results/$file" \
        "{bin} copy data/$file > /dev/null"

    sep
    echo
    echo
done
