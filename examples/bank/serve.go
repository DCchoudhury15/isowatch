package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/DCchoudhury15/isowatch/capture"
	"github.com/DCchoudhury15/isowatch/checker"
)

type server struct {
	pool   *pgxpool.Pool
	mon    *capture.Monitor
	tracer trace.Tracer
	window time.Duration

	mu        sync.Mutex
	anomalies map[checker.Kind]int
	embedded  *capture.CheckerSink
}

const schema = `
CREATE TABLE IF NOT EXISTS accounts (id int PRIMARY KEY, balance int NOT NULL);
CREATE TABLE IF NOT EXISTS doctors (id text PRIMARY KEY, shift int NOT NULL, on_call bool NOT NULL);
`

const seed = `
TRUNCATE accounts, doctors;
INSERT INTO accounts VALUES (1, 0), (2, 0);
INSERT INTO doctors VALUES ('alice', 1, true), ('bob', 1, true);
`

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	dsn := fs.String("dsn", envOr("DATABASE_URL", "postgres://postgres:postgres@localhost:55432/postgres"), "PostgreSQL 18 connection string")
	addr := fs.String("addr", ":8090", "HTTP listen address")
	mode := fs.String("mode", "embedded", `"embedded" checks inside this process, "otlp" sends spans to a collector, "both" does both`)
	endpoint := fs.String("otlp", "localhost:4318", "collector OTLP/HTTP endpoint (otlp mode)")
	window := fs.Duration("race-window", 5*time.Millisecond, "pause between read and write, to make races easy to see")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}
	if _, err := pool.Exec(ctx, seed); err != nil {
		return fmt.Errorf("seeding: %w", err)
	}

	s := &server{pool: pool, window: *window, anomalies: map[checker.Kind]int{}}

	// Tracing: every request gets a span, so every alert points at real traces.
	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName("bank-demo"))),
	}
	if *mode != "embedded" && *mode != "otlp" && *mode != "both" {
		return fmt.Errorf("unknown mode %q", *mode)
	}
	var sinks capture.MultiSink
	if *mode == "embedded" || *mode == "both" {
		s.embedded = capture.NewCheckerSink(checker.Options{}, s.report)
		defer s.embedded.Close()
		sinks = append(sinks, s.embedded)
	}
	if *mode == "otlp" || *mode == "both" {
		exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(*endpoint), otlptracehttp.WithInsecure())
		if err != nil {
			return err
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp))
		sinks = append(sinks, capture.NewOTelSink(nil))
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(c)
	}()
	otel.SetTracerProvider(tp)
	s.tracer = tp.Tracer("bank-demo")

	s.mon, err = capture.New(ctx, pool, capture.Config{
		Tables:  []string{"accounts", "doctors"},
		Sink:    sinks,
		KeyMode: capture.RawKeys, // demo keys are not sensitive
	})
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /deposit", s.traced("deposit", s.deposit("deposit", "")))
	mux.HandleFunc("POST /deposit-safe", s.traced("deposit-safe", s.deposit("deposit-safe", "FOR UPDATE")))
	mux.HandleFunc("POST /deposit-auto", s.traced("deposit-auto", s.deposit("auto-deposit", "")))
	mux.HandleFunc("POST /leave", s.traced("leave", s.leave("leave", false)))
	mux.HandleFunc("POST /leave-safe", s.traced("leave-safe", s.leave("leave-safe", true)))
	mux.HandleFunc("POST /leave-auto", s.traced("leave-auto", s.leave("auto-leave", false)))
	mux.HandleFunc("GET /state", s.state)
	mux.HandleFunc("POST /reset", s.reset)

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Printf("bank demo on %s (mode %s)", *addr, *mode)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// report prints an anomaly found by the embedded checker. For the "auto"
// endpoints it also tells IsoWatch to protect those transactions from now on.
func (s *server) report(a checker.Anomaly) {
	s.mu.Lock()
	s.anomalies[a.Kind]++
	s.mu.Unlock()
	// Build the whole alert first and log it once, so alerts from
	// concurrent requests do not interleave.
	var b strings.Builder
	fmt.Fprintf(&b, "ANOMALY %s: %s traces=%v", a.Kind, a.Summary, a.TraceIDs())
	for _, st := range a.Steps {
		fmt.Fprintf(&b, "\n    %s", st)
	}
	fmt.Fprintf(&b, "\n    fix: %s", a.Fix)
	for _, n := range a.Names() {
		if strings.HasPrefix(n, "auto-") {
			if added, _ := s.mon.ProtectFrom(a); len(added) > 0 {
				fmt.Fprintf(&b, "\n    now protecting %v: they run at SERIALIZABLE with retries from here on", added)
			}
			break
		}
	}
	log.Print(b.String())
}

// traced wraps a handler in a span, like any instrumented web framework would.
func (s *server) traced(name string, h func(context.Context, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, span := s.tracer.Start(r.Context(), "POST /"+name, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		if err := h(ctx, r); err != nil {
			span.RecordError(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// deposit is the classic lost update: read, compute in the app, write back.
// With lock = "FOR UPDATE" the read locks the row and the bug is gone.
func (s *server) deposit(name, lock string) func(context.Context, *http.Request) error {
	return func(ctx context.Context, r *http.Request) error {
		id, _ := strconv.Atoi(r.URL.Query().Get("account"))
		amount, _ := strconv.Atoi(r.URL.Query().Get("amount"))
		if id == 0 {
			id = 1
		}
		if amount == 0 {
			amount = 1
		}
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int("account", id), attribute.Int("amount", amount))
		return s.mon.Do(ctx, s.pool, pgx.TxOptions{}, func(tx *capture.Tx) error {
			var balance int
			if err := tx.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = $1 "+lock, id).Scan(&balance); err != nil {
				return err
			}
			time.Sleep(s.window) // real apps do work here: call a service, render, log...
			_, err := tx.Exec(ctx, "UPDATE accounts SET balance = $1 WHERE id = $2", balance+amount, id)
			return err
		}, capture.WithName(name))
	}
}

// leave is the classic write skew: each doctor checks that someone else is
// still on call, then goes off call. Two at once leave nobody on call.
// The safe version runs at SERIALIZABLE and retries when PostgreSQL says so.
func (s *server) leave(name string, safe bool) func(context.Context, *http.Request) error {
	return func(ctx context.Context, r *http.Request) error {
		doctor := r.URL.Query().Get("doctor")
		opts := pgx.TxOptions{}
		if safe {
			opts.IsoLevel = pgx.Serializable
		}
		for attempt := 0; ; attempt++ {
			err := s.mon.Do(ctx, s.pool, opts, func(tx *capture.Tx) error {
				rows, err := tx.Query(ctx, "SELECT id FROM doctors WHERE shift = 1 AND on_call")
				if err != nil {
					return err
				}
				onCall, err := pgx.CollectRows(rows, pgx.RowTo[string])
				if err != nil {
					return err
				}
				if len(onCall) < 2 {
					return nil // must stay, nobody else is on call
				}
				time.Sleep(s.window)
				_, err = tx.Exec(ctx, "UPDATE doctors SET on_call = false WHERE id = $1", doctor)
				return err
			}, capture.WithName(name))
			var pgErr *pgconn.PgError
			if safe && attempt < 10 && errors.As(err, &pgErr) && pgErr.Code == "40001" {
				continue // serialization failure: safe to retry the whole transaction
			}
			return err
		}
	}
}

type stateJSON struct {
	Mode      string               `json:"mode"`
	Balances  map[int]int          `json:"balances"`
	OnCall    []string             `json:"on_call"`
	Anomalies map[checker.Kind]int `json:"anomalies"`
	LostWrite int                  `json:"lost_writes,omitempty"`
	Capture   capture.Stats        `json:"capture"`
}

func (s *server) state(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	out := stateJSON{Mode: "otlp", Balances: map[int]int{}}
	rows, err := s.pool.Query(ctx, "SELECT id, balance FROM accounts ORDER BY id")
	if err == nil {
		for rows.Next() {
			var id, b int
			_ = rows.Scan(&id, &b)
			out.Balances[id] = b
		}
		rows.Close()
	}
	rows, err = s.pool.Query(ctx, "SELECT id FROM doctors WHERE on_call ORDER BY id")
	if err == nil {
		out.OnCall, _ = pgx.CollectRows(rows, pgx.RowTo[string])
	}
	s.mu.Lock()
	if s.embedded != nil {
		out.Mode = "embedded"
		out.Anomalies = map[checker.Kind]int{}
		for k, v := range s.anomalies {
			out.Anomalies[k] = v
		}
		out.LostWrite = s.embedded.Checker().LostWrites()
	}
	s.mu.Unlock()
	out.Capture = s.mon.Stats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *server) reset(w http.ResponseWriter, r *http.Request) {
	if _, err := s.pool.Exec(r.Context(), seed); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.mu.Lock()
	s.anomalies = map[checker.Kind]int{}
	if s.embedded != nil {
		s.embedded.Reset()
	}
	for _, n := range s.mon.Protected() {
		s.mon.Unprotect(n)
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}
