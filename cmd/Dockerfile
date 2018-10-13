# Development Dockerfile for dxda (Linux)
FROM ubuntu:16.04

# Get dependencies
RUN apt-get update && apt-get install -y wget git && \
    wget https://dl.google.com/go/go1.11.1.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.11.1.linux-amd64.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="${HOME}/go"

# Assume we bind mount dxda in path below
RUN mkdir -p ${HOME}/go/src/github.com/dnanexus/dxda/ && \ 
    go get github.com/google/subcommands && \
    go get github.com/dnanexus/dxda