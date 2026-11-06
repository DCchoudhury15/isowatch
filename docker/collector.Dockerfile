# syntax=docker/dockerfile:1

# The IsoWatch collector: an OpenTelemetry Collector with the IsoWatch
# processor, built with the collector builder (ocb).
#
#   docker build -f docker/collector.Dockerfile --build-arg VERSION=0.1.0 -t isowatch-collector .
#   docker run --rm -p 4317:4317 -p 4318:4318 -p 8888:8888 -p 13133:13133 isowatch-collector
#
# Ports:
#   4317   OTLP gRPC
#   4318   OTLP HTTP
#   8888   the isowatch_* metrics for Prometheus, at /metrics
#   13133  health check extension: GET / returns 200 when the collector is ready
#
# The runtime image has no shell, so there is no Docker HEALTHCHECK. Point
# your orchestrator's HTTP probe (for example a Kubernetes readinessProbe)
# at port 13133. Put your own config at /etc/isowatch/config.yaml.

# The build stage runs on the build machine's own platform and
# cross-compiles, so a multi-arch build needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS build
ARG BUILDER_VERSION=v0.162.0
RUN go install go.opentelemetry.io/collector/cmd/builder@${BUILDER_VERSION}
WORKDIR /src
COPY . .
ARG VERSION=0.0.0-dev
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    sed -e "s|REPO_DIR|/src|g" -e "s|DIST_VERSION|${VERSION}|g" \
        collector/builder-config.yaml > /tmp/builder.yaml \
 && cd collector \
 && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} builder --config=/tmp/builder.yaml

# Static binary on distroless: no shell, no package manager, CA
# certificates and time zones included, runs as uid 65532 (nonroot).
FROM gcr.io/distroless/static-debian13:nonroot
ARG VERSION=0.0.0-dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="isowatch-collector" \
      org.opencontainers.image.description="OpenTelemetry Collector with the IsoWatch processor. Finds lost updates and write skew in PostgreSQL app traffic." \
      org.opencontainers.image.source="https://github.com/DCchoudhury15/isowatch" \
      org.opencontainers.image.url="https://github.com/DCchoudhury15/isowatch" \
      org.opencontainers.image.documentation="https://github.com/DCchoudhury15/isowatch/blob/main/docs/install.md" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=build /src/collector/build/isowatch-collector /usr/local/bin/isowatch-collector
COPY LICENSE /usr/share/doc/isowatch/LICENSE
COPY docker/collector-config.yaml /etc/isowatch/config.yaml
USER 65532:65532
EXPOSE 4317 4318 8888 13133
ENTRYPOINT ["/usr/local/bin/isowatch-collector"]
CMD ["--config", "/etc/isowatch/config.yaml"]
