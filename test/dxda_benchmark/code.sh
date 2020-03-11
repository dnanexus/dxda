#!/bin/bash

# The following line causes bash to exit at any point if there is any error
# and to output each line as it is executed -- useful for debugging
set -e -x -o pipefail

main() {
    dx-download-all-inputs

    # Get all the DX environment variables, so that dxda can use them
    echo "loading the dx environment"

    # we want to avoid outputing the token
    source environment >& /dev/null

    # build manifest
    echo "downloading with dx-download-agent"

    mv ${HOME}/in/manifest/*.json.bz2 manifest.json.bz2

    start=`date +%s`
    dx-download-agent download manifest.json.bz2
    end=`date +%s`
    runtime=$((end-start))

    dx-jobutil-add-output --class=string runtime $runtime
}
