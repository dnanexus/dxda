# Development Dockerfile for dxda (Linux)
FROM ubuntu:22.04

ARG TARGETARCH

# Get dependencies for running Go
ENV GOVERSION="1.16.6"
RUN apt-get update && apt-get install -y wget git build-essential && \
    wget https://dl.google.com/go/go${GOVERSION}.linux-${TARGETARCH}.tar.gz && \
    tar -C /usr/local -xzf go${GOVERSION}.linux-${TARGETARCH}.tar.gz

# Set environment variables for Go
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"

WORKDIR /dxda
COPY . .

RUN cd cmd/dx-download-agent && \
    go install .

# Build architecture-specific binaries and packages
RUN mkdir -p /builds/dx-download-agent-osx /builds/dx-download-agent-linux && \
    #CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -o /builds/dx-download-agent-osx/dx-download-agent /go/src/github.com/dnanexus/dxda/cmd/dx-download-agent && \
    cp /go/bin/dx-download-agent /builds/dx-download-agent-linux/ && \
    cd /builds/ && \
    chmod a+x dx-download-agent-linux/dx-download-agent && \
    #chmod a+x dx-download-agent-osx/dx-download-agent && \
    tar -cvf dx-download-agent-linux.tar dx-download-agent-linux && \
    #tar -cvf dx-download-agent-osx.tar dx-download-agent-osx
    cd ..

ENTRYPOINT ["/go/bin/dx-download-agent"]
