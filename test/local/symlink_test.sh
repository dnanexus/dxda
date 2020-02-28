#!/bin/bash -e

CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "current dir=$CRNT_DIR"
DXDA_ROOT=$CRNT_DIR/../..

dxda=$GOPATH/bin/dxda
manifest=$CRNT_DIR/manifest_symlinks.json.bz2

# make sure we have the dx-download-agent executable in hand
go build -o $dxda $DXDA_ROOT/cmd/dx-download-agent/dx-download-agent.go

# download and check
$dxda download $manifest
$dxda inspect $manifest

# intentionally corrupt one file, this should be detected when
# running inspect
echo "heelo" >> symlinks/1000G_2504_high_coverage.sequence.index

set +e
$dxda inspect $manifest
rc=$?
set -e
if [[ $rc == 0 ]]; then
    echo "Error, should detect file corruption"
    exit 1
fi

# now we re-download just that file
$dxda download $manifest

set +e
$dxda inspect $manifest
rc=$?
set -e
if [[ $rc != 0 ]]; then
    echo "The corruption was not fixed"
    exit 1
fi

echo "Symlink test was successful"
