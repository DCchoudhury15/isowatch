// Command isowatch-proxy is a PostgreSQL proxy that records transactions for
// IsoWatch, so apps in any language can use it. Point the app's connection
// string at the proxy instead of PostgreSQL.
//
//	isowatch-proxy -upstream db:5432 -dsn postgres://isowatch@db:5432/app -tables accounts,orders
//
// Every flag can also be set with an environment variable (shown in -help).
// See docs/proxy.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/DCchoudhury15/isowatch/capture"
	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/proxy"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

type options struct {
	listen, upstream, dsn       string
	tables, exclude             string
	watchAll                    bool
	sink, otlpEndpoint, groupBy string
	otlpInsecure                bool
	keys, httpAddr              string
	showVersion                 bool
}

// env returns the environment variable, or def.
func env(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

func envBool(name string) bool {
	b, _ := strconv.ParseBool(os.Getenv(name))
	return b
}

func parseFlags(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("isowatch-proxy", flag.ContinueOnError)
	fs.StringVar(&o.listen, "listen", env("ISOWATCH_LISTEN", ":6432"), "address to accept client connections on (ISOWATCH_LISTEN)")
	fs.StringVar(&o.upstream, "upstream", env("ISOWATCH_UPSTREAM", ""), "PostgreSQL server as host:port (ISOWATCH_UPSTREAM, required)")
	fs.StringVar(&o.dsn, "dsn", env("ISOWATCH_DSN", ""), "connection string used at start to read the watched tables (ISOWATCH_DSN, required)")
	fs.StringVar(&o.tables, "tables", env("ISOWATCH_TABLES", ""), "tables to watch, comma separated, like accounts,billing.ledger (ISOWATCH_TABLES)")
	fs.BoolVar(&o.watchAll, "watch-all", envBool("ISOWATCH_WATCH_ALL"), "watch every table with a primary key (ISOWATCH_WATCH_ALL)")
	fs.StringVar(&o.exclude, "exclude", env("ISOWATCH_EXCLUDE", ""), "tables never watched: schema.table, table or schema.* (ISOWATCH_EXCLUDE)")
	fs.StringVar(&o.sink, "sink", env("ISOWATCH_SINK", "log"), `"log": check here and log anomalies; "otlp": send transactions to an IsoWatch collector (ISOWATCH_SINK)`)
	fs.StringVar(&o.otlpEndpoint, "otlp-endpoint", env("ISOWATCH_OTLP_ENDPOINT", ""), "OTLP/HTTP host:port of the collector, for -sink otlp (ISOWATCH_OTLP_ENDPOINT)")
	fs.BoolVar(&o.otlpInsecure, "otlp-insecure", envBool("ISOWATCH_OTLP_INSECURE"), "use plain HTTP for -sink otlp (ISOWATCH_OTLP_INSECURE)")
	fs.StringVar(&o.groupBy, "group-by", env("ISOWATCH_GROUP_BY", "transaction"), `for -sink log: "transaction" or "trace" (ISOWATCH_GROUP_BY)`)
	fs.StringVar(&o.keys, "keys", env("ISOWATCH_KEYS", "hash"), `"hash" stores a hash of primary keys, "raw" the values (ISOWATCH_KEYS)`)
	fs.StringVar(&o.httpAddr, "http", env("ISOWATCH_HTTP", ""), "address for /coverage and /stats, like :9187; off when empty (ISOWATCH_HTTP)")
	fs.BoolVar(&o.showVersion, "version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if o.showVersion {
		return o, nil
	}
	switch {
	case o.upstream == "":
		return o, errors.New("-upstream is required")
	case o.dsn == "":
		return o, errors.New("-dsn is required")
	case o.tables == "" && !o.watchAll:
		return o, errors.New("set -tables or -watch-all")
	case o.sink != "log" && o.sink != "otlp":
		return o, fmt.Errorf("-sink must be log or otlp, not %q", o.sink)
	case o.groupBy != "transaction" && o.groupBy != "trace":
		return o, fmt.Errorf("-group-by must be transaction or trace, not %q", o.groupBy)
	case o.keys != "hash" && o.keys != "raw":
		return o, fmt.Errorf("-keys must be hash or raw, not %q", o.keys)
	}
	return o, nil
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func main() {
	o, err := parseFlags(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "isowatch-proxy:", err)
		os.Exit(2)
	}
	if o.showVersion {
		fmt.Println("isowatch-proxy", version)
		return
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(o, log); err != nil {
		log.Error("isowatch-proxy stopped", "err", err)
		os.Exit(1)
	}
}

func run(o options, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var sink capture.Sink
	var flush func(context.Context) error
	var lostWrites func() int // only with the log sink
	switch o.sink {
	case "log":
		opts := checker.Options{}
		if o.groupBy == "trace" {
			opts.GroupBy = checker.ByTrace
		}
		cs := capture.NewCheckerSink(opts, func(a checker.Anomaly) { logAnomaly(log, a) })
		defer cs.Close()
		sink = cs
		lostWrites = func() int { return cs.Checker().LostWrites() }
	case "otlp":
		es, err := capture.NewOTLPSink(ctx, capture.OTLPOptions{Endpoint: o.otlpEndpoint, Insecure: o.otlpInsecure, Gzip: true})
		if err != nil {
			return err
		}
		sink, flush = es, es.Shutdown
	}

	// The catalog is read once, with a short-lived connection.
	conn, err := pgx.Connect(ctx, o.dsn)
	if err != nil {
		return fmt.Errorf("connecting with -dsn: %w", err)
	}
	keyMode := capture.HashKeys
	if o.keys == "raw" {
		keyMode = capture.RawKeys
	}
	mon, err := capture.New(ctx, conn, capture.Config{
		Tables:        split(o.tables),
		WatchAll:      o.watchAll,
		ExcludeTables: split(o.exclude),
		Sink:          sink,
		KeyMode:       keyMode,
		Logger:        log,
	})
	var searchPath string
	if err == nil {
		err = conn.QueryRow(ctx, "SHOW search_path").Scan(&searchPath)
	}
	conn.Close(context.Background())
	if err != nil {
		return err
	}
	defer mon.Close()

	px, err := proxy.New(proxy.Config{
		Upstream:   o.upstream,
		Monitor:    mon,
		SearchPath: searchPath,
		Logger:     log,
	})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", o.listen)
	if err != nil {
		return err
	}
	log.Info("isowatch-proxy started", "version", version, "listen", ln.Addr().String(),
		"upstream", o.upstream, "tables", strings.Join(mon.Tables(), ","), "sink", o.sink)

	if o.httpAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/coverage", mon.Handler())
		mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			out := map[string]any{"proxy": px.Stats(), "capture": mon.Stats()}
			if lostWrites != nil {
				out["lost_writes"] = lostWrites()
			}
			_ = json.NewEncoder(w).Encode(out)
		})
		srv := &http.Server{Addr: o.httpAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("isowatch-proxy: http server", "err", err)
			}
		}()
		defer srv.Close()
	}

	errc := make(chan error, 1)
	go func() { errc <- px.Serve(ln) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("isowatch-proxy stopping: waiting up to 10s for connections to end")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = px.Shutdown(sctx)
	if flush != nil {
		fctx, fcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer fcancel()
		_ = flush(fctx)
	}
	return nil
}

func logAnomaly(log *slog.Logger, a checker.Anomaly) {
	var names, traces []string
	for _, t := range a.Txns {
		if t.Name != "" {
			names = append(names, t.Name)
		}
		if t.TraceID != "" {
			traces = append(traces, t.TraceID)
		}
	}
	var steps []string
	for _, s := range a.Steps {
		steps = append(steps, fmt.Sprintf("%s %s %s key=%s xmin=%d: %s", s.Txn, s.Action, s.Table, s.Key, s.Xmin, s.SQL))
	}
	log.Warn("isolation anomaly",
		"kind", string(a.Kind), "unexpected", a.Unexpected, "summary", a.Summary, "fix", a.Fix,
		"names", strings.Join(names, ","), "traces", strings.Join(traces, ","), "steps", strings.Join(steps, " | "))
}
