FROM --platform=amd64 ubuntu:20.04

ENV GO_VERSION=1.22.12

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOOS=linux
ENV GOARCH=amd64
ENV CGO_ENABLED=1

RUN apt-get update && \
    apt-get --yes dist-upgrade && \
    apt-get install --yes build-essential curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl --fail --location https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz | tar -C /usr/local -xzf -
