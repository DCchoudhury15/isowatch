package isowatchprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"

	"github.com/DCchoudhury15/isowatch/history"
)

// hookServer records the bodies posted to it.
type hookServer struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []webhookMessage
	got    chan struct{}
}

func newHookServer(t *testing.T) *hookServer {
	t.Helper()
	h := &hookServer{got: make(chan struct{}, 1000)}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("bad request: %s %s", r.Method, r.Header.Get("Content-Type"))
		}
		var m webhookMessage
		if err := json.Unmarshal(b, &m); err != nil {
			t.Errorf("body is not JSON: %v: %s", err, b)
		}
		h.mu.Lock()
		h.bodies = append(h.bodies, m)
		h.mu.Unlock()
		w.Write([]byte("ok")) // what Slack answers
		h.got <- struct{}{}
	}))
	t.Cleanup(h.Close)
	return h
}

// wait waits for n more posts.
func (h *hookServer) wait(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-h.got:
		case <-time.After(5 * time.Second):
			t.Fatalf("waited for %d posts, got %d", n, i)
		}
	}
}

func (h *hookServer) messages() []webhookMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]webhookMessage(nil), h.bodies...)
}

func TestWebhookPostsOncePerNewIssue(t *testing.T) {
	srv := newHookServer(t)
	cfg := createDefaultConfig()
	cfg.Webhook.URL = configopaque.String(srv.URL)
	p, sink, clock := startProcessor(t, nopSettings(), cfg)

	for n := 0; n < 10; n++ {
		sendRace(t, p, n, depositRace)
		clock.Add(time.Second)
	}
	srv.wait(t, 1)
	// Give a wrongly sent second message time to arrive.
	time.Sleep(100 * time.Millisecond)
	msgs := srv.messages()
	if len(msgs) != 1 {
		t.Fatalf("10 occurrences of one issue must post once, got %d", len(msgs))
	}
	m := msgs[0]
	if m.Event != "new_issue" || m.Issue.Kind != "lost-update" || m.Issue.Count != 1 {
		t.Fatalf("message = %+v", m)
	}
	for _, want := range []string{"lost-update", "public.acct", "deposit", m.Issue.ID, "FOR UPDATE"} {
		if !strings.Contains(m.Text, want) {
			t.Fatalf("text %q should contain %q", m.Text, want)
		}
	}
	span := spansNamed(sink, history.AnomalySpan)[0]
	if strAttr(t, span, "isowatch.issue.id") != m.Issue.ID {
		t.Fatal("the message and the span must name the same issue")
	}
	tr := strings.Join(m.Anomaly.Traces, " ")
	if len(m.Anomaly.Traces) != 2 || !strings.Contains(tr, traceID(0, 1).String()) || !strings.Contains(tr, traceID(0, 2).String()) {
		t.Fatalf("traces = %v, want both requests", m.Anomaly.Traces)
	}

	// A different bug is a new issue and gets its own message.
	other := depositRace
	other.nameA, other.nameB = "withdraw", "withdraw"
	sendRace(t, p, 100, other)
	srv.wait(t, 1)
	if msgs := srv.messages(); len(msgs) != 2 || msgs[1].Issue.ID == m.Issue.ID {
		t.Fatalf("want a second message for the second issue, got %+v", msgs)
	}

	// Without repeat, later occurrences never post, even after the interval.
	clock.Add(time.Hour)
	sendRace(t, p, 200, depositRace)
	time.Sleep(100 * time.Millisecond)
	if n := len(srv.messages()); n != 2 {
		t.Fatalf("repeat is off, want 2 messages, got %d", n)
	}
}

func TestWebhookRepeatSendsTheCount(t *testing.T) {
	srv := newHookServer(t)
	cfg := createDefaultConfig()
	cfg.Webhook.URL = configopaque.String(srv.URL)
	cfg.Webhook.Repeat = true
	p, _, clock := startProcessor(t, nopSettings(), cfg)

	for n := 0; n < 5; n++ {
		sendRace(t, p, n, depositRace)
		clock.Add(time.Second)
	}
	srv.wait(t, 1)
	clock.Add(time.Minute)
	sendRace(t, p, 5, depositRace)
	srv.wait(t, 1)
	sendRace(t, p, 6, depositRace) // within the interval again: no message
	time.Sleep(100 * time.Millisecond)
	msgs := srv.messages()
	if len(msgs) != 2 {
		t.Fatalf("want new + 1 repeat, got %d", len(msgs))
	}
	r := msgs[1]
	if r.Event != "repeat" || r.Issue.Count != 6 || r.Issue.ID != msgs[0].Issue.ID {
		t.Fatalf("repeat = %+v", r)
	}
	if !strings.Contains(r.Text, "6 times") {
		t.Fatalf("repeat text %q should give the count", r.Text)
	}
}

// A webhook that never answers must not slow the pipeline. Messages that do
// not fit in the queue are dropped and counted.
func TestWebhookNeverBlocksThePipeline(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	tel, set := telemetrySettings(t)
	cfg := createDefaultConfig()
	cfg.Webhook.URL = configopaque.String(srv.URL)
	cfg.Webhook.Timeout = time.Minute
	p, _, _ := startProcessor(t, set, cfg)

	const issues = webhookQueue + 50
	start := time.Now()
	sent := make(chan error, 1)
	go func() { // a pipeline that blocks fails the test below, not by hanging
		for n := 0; n < issues; n++ {
			r := depositRace
			r.nameA = fmt.Sprintf("op-%d", n) // a new issue each time
			r.nameB = r.nameA
			if err := p.ConsumeTraces(context.Background(), raceTraces(n, r)); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the pipeline is blocked on the webhook")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the pipeline waited for the webhook: %v for %d batches", d, issues)
	}
	// One message is in flight, webhookQueue are queued, the rest dropped.
	dropped := sumOf(t, tel, metricDropped)
	if dropped < issues-webhookQueue-1 || dropped > issues-webhookQueue {
		t.Fatalf("dropped = %d, want about %d", dropped, issues-webhookQueue)
	}

	// Shutdown gives up when its context ends, even with a stuck webhook.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = p.Shutdown(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hung on the webhook")
	}
}

func TestWebhookErrorHidesTheURL(t *testing.T) {
	err := redact(&testErr{`Post "https://hooks.example.com/services/team/hook/private-part": timeout`},
		"https://hooks.example.com/services/team/hook/private-part")
	if strings.Contains(err.Error(), "private-part") {
		t.Fatalf("error leaks the URL: %v", err)
	}
}

type testErr struct{ s string }

func (e *testErr) Error() string { return e.s }
