#!/bin/bash
set -uoe pipefail

xan cat rows -d, scripts/binaries/results/*.csv scripts/binaries/results/*.tsv | \
xan separate command -rc 'data/(?<file>\S+)' | \
xan separate parameter_bin -rc 'binaries/(?<bin>[^\.]+)\.bin' | \
xan select file,bin,mean | \
xan top -Rg file mean | \
xan v -Ig file
