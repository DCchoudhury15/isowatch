# Builds the bank demo. It uses cgo (the PostgreSQL SQL parser is C code).
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY . .
RUN cd examples/bank && CGO_ENABLED=1 go build -o /out/bank .

FROM debian:bookworm-slim
COPY --from=build /out/bank /usr/local/bin/bank
EXPOSE 8090
ENTRYPOINT ["bank"]
