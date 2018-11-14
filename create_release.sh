#!/bin/bash -ex

TAG=$1

docker pull dnanexus/dxda:$TAG

VERSION=$2
mkdir -p builds/
docker run --entrypoint='' dnanexus/dxda:$TAG cat /go/bin/dx-download-agent > builds/dx-download-agent-linux
docker run --entrypoint='' dnanexus/dxda:$TAG cat /dx-download-agent-osx > builds/dx-download-agent-osx

docker tag dnanexus/dxda:$TAG dnanexus/dxda:$VERSION
docker push dnanexus/dxda:$VERSION

echo "SUCCESS. Builds placed in builds/ directory.  To finish creating a release add these to a Github release page on Github"