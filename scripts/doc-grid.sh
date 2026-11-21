#!/bin/bash
set -uoe pipefail

# cat <(ls data/*.csv) <(ls data/*.tsv) | \
# xan from -f txt -c path | \
# xan search -v series -P ndjson -P crlf | \
# xan map -f scripts/bench.moonblade | \
# xan progress --smooth > grid.csv

xan search -v series -P ndjson grid.csv | \
xan transform split,zero_copy,copy 'baseline / _ | to_fixed(_, 1) | fmt("~{}x", _)' | \
xan select -e 'path.split("/")[-1] as file, split, zero_copy, copy'