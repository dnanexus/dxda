#!/bin/bash -ex

CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TAG=$1

docker pull dnanexus/dxda:$TAG

echo "extracting version from util.go"
VERSION=$(cat $CRNT_DIR/util.go | grep Version | cut --delimiter='"' --fields=2)
echo "version=$VERSION"

mkdir -p builds/
#docker run --entrypoint='' dnanexus/dxda:$TAG cat /builds/dx-download-agent-osx.tar > builds/dx-download-agent-osx.tar
docker run --entrypoint='' dnanexus/dxda:$TAG cat /builds/dx-download-agent-linux.tar > builds/dx-download-agent-linux.tar

docker tag dnanexus/dxda:$TAG dnanexus/dxda:$VERSION
docker push dnanexus/dxda:$VERSION

echo "SUCCESS. Builds placed in builds/ directory.  To finish creating a release add these to a Github release page on Github"
