# syntax=docker/dockerfile:1.4
FROM --platform=$BUILDPLATFORM golang:1.18 as builder

WORKDIR /src/
COPY go.* /src/
# Cache mod downloads
RUN go mod download -x

COPY cmd /src/cmd
COPY pkg /src/pkg

ARG GOOS=linux
ARG VERSION=v0.0.0-0.unknown

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-X github.com/piraeusdatastore/linstor-affinity-controller/pkg/version.Version=${VERSION} -extldflags=-static" -v ./cmd/linstor-affinity-controller

FROM gcr.io/distroless/static

COPY --from=builder /src/linstor-affinity-controller /linstor-affinity-controller
USER nobody
CMD ["/linstor-affinity-controller", "-v=2"]
