# Development Dockerfile for dxda (Linux)
FROM ubuntu:16.04

# Get dependencies for running Go
RUN apt-get update && apt-get install -y wget git build-essential && \
    wget https://dl.google.com/go/go1.11.1.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.11.1.linux-amd64.tar.gz

# Set environment variables for Go
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"

# Install Go packages and Download agent executables
RUN go get github.com/google/subcommands && go install github.com/google/subcommands && \
    go get github.com/dnanexus/dxda && go install github.com/dnanexus/dxda && \
    go install github.com/dnanexus/dxda/cmd/dx-download-agent

# Build architecture-specific binaries
RUN GOOS=darwin GOARCH=amd64 go build -o dx-download-agent-osx /go/src/github.com/dnanexus/dxda/cmd/dx-download-agent 

ENTRYPOINT ["/go/bin/dx-download-agent"]