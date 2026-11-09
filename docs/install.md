# Installing IsoWatch

IsoWatch has two parts:

- **The Go library** goes into your app. It records each transaction.
- **The collector** checks the transactions of all your apps together. It is an OpenTelemetry Collector with the IsoWatch processor.

Releases are on the [releases page](https://github.com/DCchoudhury15/isowatch/releases). The library, the processor and the collector image of one release have the same version. [CHANGELOG.md](../CHANGELOG.md) lists what changed.

## What you need

- PostgreSQL 18 or newer. IsoWatch reads the old row version of each write with `RETURNING old.xmin`, which older versions do not have.
- An app in Go that uses [pgx v5](https://github.com/jackc/pgx).
- Go 1.26 or newer.
- **cgo and a C compiler** (for example `gcc`) to build your app. IsoWatch parses SQL with the real PostgreSQL parser ([pg_query_go](https://github.com/pganalyze/pg_query_go)), which is C code. A build with `CGO_ENABLED=0` fails with `undefined: pg_query.Parse`. The first build compiles the parser and takes about a minute; later builds are cached.

The collector is pure Go and needs none of this.

## The library

```bash
go get github.com/DCchoudhury15/isowatch@latest
```

Then follow [Using it in your app](../README.md#using-it-in-your-app), or [Adding IsoWatch to an existing app](adopting.md) to change as little code as possible.

## The collector image

```bash
docker run --rm -p 4317:4317 -p 4318:4318 -p 8888:8888 -p 13133:13133 \
  ghcr.io/dcchoudhury15/isowatch-collector:latest
```

| Port | What |
|---|---|
| 4317 | OTLP gRPC |
| 4318 | OTLP HTTP (`capture.NewOTLPSink` uses this one) |
| 8888 | The `isowatch_*` metrics for Prometheus, at `/metrics`. See [Metrics and alerts](alerts.md). |
| 13133 | Health check. `GET /` returns 200 when the collector is ready. |

The image's own config ([docker/collector-config.yaml](../docker/collector-config.yaml)) checks the transactions and prints each anomaly to the log. To send the anomaly spans on to your tracing backend, or to turn on the webhook or protection, mount your own config over it:

```bash
docker run --rm -p 4318:4318 -p 8888:8888 -p 13133:13133 \
  -v "$PWD/config.yaml:/etc/isowatch/config.yaml:ro" \
  -e ISOWATCH_WEBHOOK_URL \
  ghcr.io/dcchoudhury15/isowatch-collector:0.1.0
```

Start from [collector/config.yaml](../collector/config.yaml), which lists every option, and copy the `health_check` extension and the `metrics` part of `service.telemetry` from the image's config if you want those ports. Put secrets in environment variables and use `${env:NAME}` in the config.

About the image:

- Tags: one per release (`0.1.0`), and `latest` for the newest release that is not a pre-release. Use a fixed version in production.
- Platforms: linux/amd64 and linux/arm64.
- It is built on distroless, runs as user 65532 (not root) and has no shell. So there is no Docker `HEALTHCHECK`. Point your orchestrator's HTTP check at port 13133.

### Kubernetes

Run **one** replica. One checker has to see every transaction, or it misses the anomalies between the replicas (see [Running the collector](collector.md#run-one-checker-see-every-transaction)). The `Recreate` strategy also stops two copies from running at once during an update.

```yaml
spec:
  replicas: 1
  strategy:
    type: Recreate
  template:
    spec:
      containers:
        - name: isowatch-collector
          image: ghcr.io/dcchoudhury15/isowatch-collector:0.1.0
          ports:
            - containerPort: 4318
            - containerPort: 8888
            - containerPort: 13133
          readinessProbe:
            httpGet:
              path: /
              port: 13133
          livenessProbe:
            httpGet:
              path: /
              port: 13133
```

## Collector binaries

Each release has the collector for Linux and macOS, on amd64 and arm64, and a `checksums.txt`:

```bash
v=0.1.0
base=https://github.com/DCchoudhury15/isowatch/releases/download/v$v
curl -LO "$base/isowatch-collector_${v}_linux_amd64.tar.gz"
curl -LO "$base/checksums.txt"
sha256sum --ignore-missing -c checksums.txt
tar -xzf "isowatch-collector_${v}_linux_amd64.tar.gz"
cd "isowatch-collector_${v}_linux_amd64"
./isowatch-collector --config config.yaml
```

The archive has the binary, the license and the commented example config. The binaries are not signed for macOS. If you download one with a browser, macOS may block it. `xattr -d com.apple.quarantine isowatch-collector` allows it to run. Files downloaded with `curl` are not blocked.

## The proxy, for apps in any language

```bash
go install github.com/DCchoudhury15/isowatch/cmd/isowatch-proxy@latest
```

It needs cgo and a C compiler to build, like the library. See [The IsoWatch proxy](proxy.md) for how to run it.

## Adding the processor to your own collector

If you already build a collector with the [OpenTelemetry Collector Builder](https://opentelemetry.io/docs/collector/custom-collector/) (ocb), add the processor to your builder config:

```yaml
processors:
  - gomod: github.com/DCchoudhury15/isowatch/processor/isowatchprocessor v0.1.0
```

IsoWatch is built and tested with collector components v0.162.0 (v1.68.0 for the stable modules). Use the same versions, or expect to fix version conflicts. Then put `isowatch` first in your traces pipeline, before `batch` and any sampling, as [Running the collector](collector.md) explains.

## From source

```bash
git clone https://github.com/DCchoudhury15/isowatch
cd isowatch
make collector     # collector/build/isowatch-collector
docker build -f docker/collector.Dockerfile -t isowatch-collector .
```

`make help` lists the other tasks, like running the tests and the demo.
