# Development Dockerfile for dxda (Linux)
FROM ubuntu:22.04

ARG TARGETARCH

# Get dependencies for running Go
ENV GOVERSION="1.22.12"
RUN apt-get update && apt-get install -y wget git build-essential && \
    wget https://dl.google.com/go/go${GOVERSION}.linux-${TARGETARCH}.tar.gz && \
    tar -C /usr/local -xzf go${GOVERSION}.linux-${TARGETARCH}.tar.gz && \
    rm go${GOVERSION}.linux-${TARGETARCH}.tar.gz

# Set environment variables for Go
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"

WORKDIR /dxda
COPY . .

RUN cd cmd/dx-download-agent && \
    go install .

ENTRYPOINT ["/go/bin/dx-download-agent"]
