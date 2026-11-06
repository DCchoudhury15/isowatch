package isowatchtest

import (
	"context"
	"crypto/rand"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

// RunOption configures Concurrently.
type RunOption func(*runConfig)

type runConfig struct {
	rounds int
}

// Repeat runs the whole burst of calls this many times, one round after
// another, each round starting all its calls at the same moment. Races
// depend on timing, so more rounds make a race that only sometimes happens
// very likely to happen at least once.
func Repeat(rounds int) RunOption {
	return func(c *runConfig) { c.rounds = rounds }
}

// Concurrently calls fn n times at the same moment, each call on its own
// goroutine, and waits for all of them. Every goroutine is started first and
// waits at a barrier; only when all n are ready are they released together.
// That makes the calls overlap as much as possible, so races in the code
// under test actually happen.
//
// i is the index of the call: 0 to n-1 in the first round, n to 2n-1 in the
// second (see Repeat), and so on.
//
// If any call returns an error or panics, Concurrently fails the test with
// the errors once the round is over, and runs no further rounds. Call it
// from the test's own goroutine.
//
// For the most overlap, give the pool at least n connections
// (pgxpool.Config.MaxConns), or calls wait for a free connection instead of
// racing.
func Concurrently(t testing.TB, n int, fn func(i int) error, opts ...RunOption) {
	t.Helper()
	if n < 1 {
		t.Fatalf("isowatchtest.Concurrently: n must be at least 1, got %d", n)
	}
	c := runConfig{rounds: 1}
	for _, o := range opts {
		o(&c)
	}
	if c.rounds < 1 {
		c.rounds = 1
	}
	for round := 0; round < c.rounds; round++ {
		errs := startTogether(n, round*n, fn)
		if msg := describeErrors(errs, round*n, round, c.rounds); msg != "" {
			t.Fatal(msg)
		}
	}
}

// startTogether runs fn(base) to fn(base+n-1) at the same moment and returns
// their errors, by call.
func startTogether(n, base int, fn func(i int) error) []error {
	errs := make([]error, n)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(n)
	done.Add(n)
	for w := 0; w < n; w++ {
		go func(w int) {
			defer done.Done()
			defer func() {
				if p := recover(); p != nil {
					errs[w] = fmt.Errorf("panic: %v\n%s", p, debug.Stack())
				}
			}()
			ready.Done()
			<-start
			errs[w] = fn(base + w)
		}(w)
	}
	ready.Wait()
	close(start)
	done.Wait()
	return errs
}

func describeErrors(errs []error, base, round, rounds int) string {
	failed := 0
	for _, err := range errs {
		if err != nil {
			failed++
		}
	}
	if failed == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "isowatchtest.Concurrently: %d of %d calls failed", failed, len(errs))
	if rounds > 1 {
		fmt.Fprintf(&b, " in round %d of %d", round+1, rounds)
	}
	b.WriteString(":")
	const show = 5
	shown := 0
	for w, err := range errs {
		if err == nil {
			continue
		}
		if shown == show {
			fmt.Fprintf(&b, "\n  ... and %d more", failed-show)
			break
		}
		fmt.Fprintf(&b, "\n  call %d: %v", base+w, err)
		shown++
	}
	return b.String()
}

// NewTrace returns ctx with a new, random trace id, as if a new request had
// started. Transactions begun with this context carry that trace id. Use it
// with GroupByTrace, so each simulated request is its own node:
//
//	isowatchtest.Concurrently(t, 10, func(i int) error {
//		ctx := isowatchtest.NewTrace(t.Context())
//		return handleTransfer(ctx, ...)
//	})
//
// No OpenTelemetry SDK is needed. If one is set up, spans started from ctx
// join this trace.
func NewTrace(ctx context.Context) context.Context {
	var tid trace.TraceID
	var sid trace.SpanID
	_, _ = rand.Read(tid[:])
	_, _ = rand.Read(sid[:])
	return trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	}))
}
