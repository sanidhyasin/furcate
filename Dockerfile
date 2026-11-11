# furcate in a container: the collector and a read-only web UI next to the
# stores (see the app profile in docker-compose.yml). Runs and forks need a
# sandbox driver, so they use the furcate binary on the host instead.
#
# Built for linux/amd64 and linux/arm64 by the release workflow; Go
# cross-compiles, so no emulation is needed.

FROM --platform=$BUILDPLATFORM golang:1.26.3 AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o /out/furcate ./cmd/furcate \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/furcate /usr/local/bin/furcate
# The collector's write-ahead log goes here; a new named volume takes this
# directory's owner, the image's non-root user.
COPY --from=build --chown=65532:65532 /out/data /data
ENV FURCATE_WAL_DIR=/data/wal
EXPOSE 4317 4318 7070
ENTRYPOINT ["furcate"]
CMD ["serve", "-listen", "0.0.0.0:7070"]
