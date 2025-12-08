#!/bin/bash -e

# This test checks handling of the download of symlink 2.0 files which have parts with out md5 checksums.
CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "current dir=$CRNT_DIR"
DXDA_ROOT=$CRNT_DIR/../..

dxda=${GOPATH:-$HOME/go}/bin/dxda

# make sure we have the dx-download-agent executable in hand
go build -o $dxda $DXDA_ROOT/cmd/dx-download-agent/dx-download-agent.go

echo "creating manifest from the dxfuse_test_data:/symlinks2 directory"
manifest=$CRNT_DIR/symlinks2.manifest.json.bz2
if [[ ! -f $manifest ]]; then
    python ${DXDA_ROOT}/scripts/create_manifest.py -r /symlinks2 --output_file $manifest
fi

# download and check
$dxda download $manifest
$dxda inspect $manifest

echo "Symlinks2 test was successful"
rm -rf symlinks2
rm -f *.db *.log
