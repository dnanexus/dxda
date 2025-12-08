#!/bin/bash -e

CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "current dir=$CRNT_DIR"
DXDA_ROOT=$CRNT_DIR/../..

dxda=$GOPATH/bin/dxda

# make sure we have the dx-download-agent executable in hand
go build -o $dxda $DXDA_ROOT/cmd/dx-download-agent/dx-download-agent.go

echo "creating manifest from the dxfuse_test_data:/correctness directory"
manifest=$CRNT_DIR/regular.manifest.json.bz2
if [[ ! -f $manifest ]]; then
    python ${DXDA_ROOT}/scripts/create_manifest.py -r /correctness --output_file $manifest
fi

# download and check
$dxda download -gc_info $manifest
$dxda inspect $manifest

# intentionally corrupt one file, this should be detected when
# running inspect
echo "hello world" > correctness/dxWDL_source_code/test/bugs/argument_list_too_long.wdl

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

echo "Checking large files"
manifest2=$CRNT_DIR/large_files.manifest.json.bz2
if [[ ! -f $manifest2 ]]; then
    python ${DXDA_ROOT}/scripts/create_manifest.py -r /large_files --output_file $manifest2
fi

$dxda download $manifest2
$dxda inspect $manifest2

echo "Regular files test was successful"
rm -rf large_files correctness
rm -f *.db *.log
