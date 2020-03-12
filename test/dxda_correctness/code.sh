#!/bin/bash

# The following line causes bash to exit at any point if there is any error
# and to output each line as it is executed -- useful for debugging
set -e -x -o pipefail

######################################################################
## constants

baseDir="$HOME/dxda_test"
dxTrgDir="${baseDir}/dxCopy"
dxdaTrgDir="${baseDir}/dxda2Copy"
projId="project-FbZ25gj04J9B8FJ3Gb5fVP41"

dxDirOnProject="/correctness"

######################################################################

main() {
    dx-download-all-inputs
    mkdir -p ${baseDir}

    # Get all the DX environment variables, so that dxda can use them
    echo "loading the dx environment"

    # we want to avoid outputing the token
    source environment >& /dev/null

    # build manifest
    echo "downloading with dx-download-agent"
    mkdir $dxdaTrgDir
    cd $dxdaTrgDir
    mv ${HOME}/in/manifest/*.json.bz2 manifest.json.bz2
    dx-download-agent download manifest.json.bz2
    rm -f manifest*
    cd $HOME

    echo "download recursively with dx download"
    mkdir -p $dxTrgDir
    cd $dxTrgDir
    dx download --no-progress -r  "$projId:$dxDirOnProject"
    cd $HOME

    echo "comparing results"
    mkdir -p $HOME/out/result

    # don't exit if there is a discrepency; we want to see the file differences.
    diff -r --brief $dxTrgDir $dxdaTrgDir > $HOME/out/result/results.txt  || true

    # If the diff is non empty, declare that the results
    # are not equivalent.
    equivalent="true"
    if [[ -s $HOME/out/result/results.txt ]]; then
        equivalent="false"
    fi

    ls -lh $HOME/out/result/results.txt
    echo "equivalent = $equivalent"

    dx-jobutil-add-output --class=boolean equality $equivalent

    # There was a difference, upload diff files.
    if [[ $equivalent == "false" ]]; then
        dx-upload-all-outputs
    fi
}
