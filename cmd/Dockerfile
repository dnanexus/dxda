# Development Dockerfile for dxda (Linux)
FROM ubuntu:16.04

# Get dependencies
RUN apt-get update && apt-get install -y wget git build-essential && \
    wget https://dl.google.com/go/go1.11.1.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.11.1.linux-amd64.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"

# Assume we bind mount dxda in path below
RUN go get github.com/google/subcommands && \
    go get github.com/dnanexus/dxda

RUN cd /go/src/github.com/dnanexus/dxda/cmd && make build

ENTRYPOINT ["/go/src/github.com/dnanexus/dxda/cmd/dx-download-agent"]