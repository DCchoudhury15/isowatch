package isowatchprocessor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/DCchoudhury15/isowatch/checker"
)

// webhookQueue is how many messages can wait to be sent. New issues are
// rare, so this only fills up if the webhook is down or very slow.
const webhookQueue = 100

// webhookMessage is the JSON body posted to the webhook. "text" is what a
// Slack (or Mattermost, or Rocket.Chat) incoming webhook shows. The other
// fields are for programs.
type webhookMessage struct {
	Text    string         `json:"text"`
	Event   string         `json:"event"` // "new_issue" or "repeat"
	Issue   issueView      `json:"issue"`
	Anomaly webhookAnomaly `json:"anomaly"`
}

type webhookAnomaly struct {
	Kind       string   `json:"kind"`
	Unexpected bool     `json:"unexpected"`
	Summary    string   `json:"summary"`
	Fix        string   `json:"fix"`
	Traces     []string `json:"traces"`
	Names      []string `json:"names,omitempty"`
}

type webhook struct {
	url    string
	client *http.Client
	logger *zap.Logger
	tel    *telemetry
	w      *worker[[]byte]
}

func newWebhook(cfg WebhookConfig, logger *zap.Logger, tel *telemetry) *webhook {
	h := &webhook{
		url:    string(cfg.URL),
		client: &http.Client{Timeout: cfg.Timeout},
		logger: logger,
		tel:    tel,
	}
	h.w = newWorker(webhookQueue, h.post)
	return h
}

// notify queues a message for one occurrence. It never blocks.
func (h *webhook) notify(ctx context.Context, s seen, a checker.Anomaly) {
	msg := webhookMessage{
		Event: "repeat",
		Issue: s.issue,
		Anomaly: webhookAnomaly{
			Kind: string(a.Kind), Unexpected: a.Unexpected, Summary: a.Summary, Fix: a.Fix,
			Traces: a.TraceIDs(), Names: a.Names(),
		},
	}
	if s.isNew {
		msg.Event = "new_issue"
	}
	msg.Text = messageText(msg)
	b, err := json.Marshal(msg)
	if err != nil {
		return
	}
	if !h.w.offer(b) {
		h.tel.droppedNotification(ctx, "webhook")
		h.logger.Warn("isowatch webhook queue is full, message dropped", zap.String("issue", s.issue.ID))
	}
}

// messageText is the human readable part of a message.
func messageText(m webhookMessage) string {
	where := strings.Join(m.Issue.Tables, ", ")
	if len(m.Issue.Names) > 0 {
		where += " in " + strings.Join(m.Issue.Names, ", ")
	}
	var b strings.Builder
	if m.Event == "new_issue" {
		fmt.Fprintf(&b, "IsoWatch found a new %s issue on %s (issue %s).\n", m.Issue.Kind, where, m.Issue.ID)
		b.WriteString(m.Anomaly.Summary + "\n")
		b.WriteString("Fix: " + m.Anomaly.Fix + "\n")
		if len(m.Anomaly.Traces) > 0 {
			b.WriteString("Traces: " + strings.Join(m.Anomaly.Traces, ", "))
		}
		return strings.TrimRight(b.String(), "\n")
	}
	fmt.Fprintf(&b, "IsoWatch issue %s (%s on %s) has happened %d times since %s.",
		m.Issue.ID, m.Issue.Kind, where, m.Issue.Count, m.Issue.FirstSeen.UTC().Format(time.RFC3339))
	if m.Issue.LostWrites > 0 {
		fmt.Fprintf(&b, " Lost writes so far: %d.", m.Issue.LostWrites)
	}
	if len(m.Anomaly.Traces) > 0 {
		b.WriteString(" Latest traces: " + strings.Join(m.Anomaly.Traces, ", "))
	}
	return b.String()
}

func (h *webhook) post(ctx context.Context, body []byte) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		h.logger.Warn("isowatch webhook request failed", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		h.logger.Warn("isowatch webhook request failed", zap.Error(redact(err, h.url)))
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		h.logger.Warn("isowatch webhook returned an error", zap.Int("status", resp.StatusCode))
	}
}

// redact removes the webhook URL from an error. Slack webhook URLs contain
// the secret token.
func redact(err error, url string) error {
	return fmt.Errorf("%s", strings.ReplaceAll(err.Error(), url, "<webhook url>"))
}
