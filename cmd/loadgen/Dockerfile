# Build the manager binary
FROM docker.io/library/golang:1.20 as builder

WORKDIR /workspace/apm-queue

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

WORKDIR /workspace/apm-queue/cmd/loadgen
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Build
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o loadgen .

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/apm-queue/cmd/loadgen/loadgen .
USER 65532:65532
ENTRYPOINT ["/loadgen"]

# Add Metadata.
ARG BUILD_DATE
ARG SOURCE_COMMIT
LABEL build-date="${BUILD_DATE}"
LABEL source-commit="${SOURCE_COMMIT}"
