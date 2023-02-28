# Development Dockerfile for dxda (Linux)
FROM --platform=$BUILDPLATFORM ubuntu:22.04 AS build

ARG BUILDARCH TARGETOS TARGETARCH

# Get dependencies for running Go
ENV GOVERSION="1.16.6"
RUN apt-get update && apt-get install -y wget git build-essential && \
    wget https://dl.google.com/go/go${GOVERSION}.linux-$BUILDARCH.tar.gz && \
    tar -C /usr/local -xzf go${GOVERSION}.linux-${BUILDARCH}.tar.gz

# Set environment variables for Go
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"

WORKDIR /src
COPY . .

RUN cd cmd/dx-download-agent && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /build/dx-download-agent .


FROM ubuntu:22.04

COPY --from=build /build/dx-download-agent /go/bin/

ENTRYPOINT ["/go/bin/dx-download-agent"]