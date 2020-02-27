#!/bin/bash -ex

CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "current dir=$CRNT_DIR"
DXDA_ROOT=$CRNT_DIR/../..

dxda=$GOPATH/bin/dxfuse
manifest=$CRNT_DIR/manifest_symlinks.json.bz2

# make sure we have the dx-download-agent executable in hand
go build -o $dxda $DXDA_ROOT/cmd/dx-download-agent/dx-download-agent.go

# download and check
$dxda download $manifest
$dxda inspect $manifest

# intentionally corrupt one file, this should be detected when
# running inspect
echo "heelo" >> symlinks/1000G_2504_high_coverage.sequence.index
$dxda inspect manifest_symlinks.json.bz2

# now we re-download just that file
$dxda download manifest_symlinks.json.bz2
$dxda inspect manifest_symlinks.json.bz2
