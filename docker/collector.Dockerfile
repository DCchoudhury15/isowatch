# Builds the OpenTelemetry Collector with the IsoWatch processor.
FROM golang:1.26-bookworm AS build
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.162.0
WORKDIR /src
COPY . .
RUN sed "s|REPO_DIR|/src|g" collector/builder-config.yaml > /tmp/builder.yaml \
 && cd collector \
 && CGO_ENABLED=0 builder --config=/tmp/builder.yaml

FROM debian:bookworm-slim
COPY --from=build /src/collector/build/isowatch-collector /usr/local/bin/isowatch-collector
COPY collector/config.docker.yaml /etc/isowatch/config.yaml
EXPOSE 4317 4318
ENTRYPOINT ["isowatch-collector"]
CMD ["--config", "/etc/isowatch/config.yaml"]
