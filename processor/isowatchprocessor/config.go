package isowatchprocessor

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/collector/config/configopaque"

	"github.com/DCchoudhury15/isowatch/protection"
)

// Config is the processor configuration.
//
//	processors:
//	  isowatch:
//	    window: 2m            # keep transactions this long (longer than your longest transaction)
//	    group_by: transaction # or "trace" to also find anomalies across a whole request
//	    max_depth: 16         # longest cycle to look for
//	    evict_interval: 5s    # how often to forget old transactions
//	    drop_txn_spans: true  # remove the isowatch.txn spans after checking them
//	    ignore_tables: []     # anomalies only on these tables are acceptable
//	    ignore_names: []      # transactions with these names may race
//	    report: per_issue     # or "every" for one anomaly span per occurrence
//	    issue_interval: 1m    # per_issue: at most one more span per issue in this time
//	    max_issues: 1000      # issues kept in memory, least recently seen are forgotten
//	    webhook:
//	      url: https://hooks.slack.com/services/...
//	      timeout: 5s
//	      repeat: false       # also post the count once per issue_interval
//	    protection:
//	      dsn: postgres://isowatch@db:5432/app
//	      table: isowatch_protected
type Config struct {
	Window        time.Duration `mapstructure:"window"`
	GroupBy       string        `mapstructure:"group_by"`
	MaxDepth      int           `mapstructure:"max_depth"`
	EvictInterval time.Duration `mapstructure:"evict_interval"`
	DropTxnSpans  bool          `mapstructure:"drop_txn_spans"`
	IgnoreTables  []string      `mapstructure:"ignore_tables"`
	IgnoreNames   []string      `mapstructure:"ignore_names"`

	// Report decides how many anomaly spans are added. "per_issue" adds one
	// for the first occurrence of each issue and then at most one per
	// IssueInterval. "every" adds one per occurrence. Metrics always count
	// every occurrence.
	Report        string        `mapstructure:"report"`
	IssueInterval time.Duration `mapstructure:"issue_interval"`
	MaxIssues     int           `mapstructure:"max_issues"`

	Webhook    WebhookConfig    `mapstructure:"webhook"`
	Protection ProtectionConfig `mapstructure:"protection"`
}

// WebhookConfig sends a JSON message for each new issue. It is off when URL
// is empty. The message has a "text" field, so a Slack incoming webhook
// works without changes.
type WebhookConfig struct {
	URL     configopaque.String `mapstructure:"url"`
	Timeout time.Duration       `mapstructure:"timeout"`
	// Repeat also sends the current count when an issue happens again, at
	// most once per issue_interval.
	Repeat bool `mapstructure:"repeat"`
}

// ProtectionConfig writes the names of the transactions in each anomaly to
// a table that the apps read, so they can run those transactions at
// SERIALIZABLE. It is off when DSN is empty.
type ProtectionConfig struct {
	DSN   configopaque.String `mapstructure:"dsn"`
	Table string              `mapstructure:"table"`
}

const (
	reportPerIssue = "per_issue"
	reportEvery    = "every"
)

func createDefaultConfig() *Config {
	return &Config{
		Window:        2 * time.Minute,
		GroupBy:       "transaction",
		MaxDepth:      16,
		EvictInterval: 5 * time.Second,
		DropTxnSpans:  true,
		Report:        reportPerIssue,
		IssueInterval: time.Minute,
		MaxIssues:     1000,
		Webhook:       WebhookConfig{Timeout: 5 * time.Second},
		Protection:    ProtectionConfig{Table: "isowatch_protected"},
	}
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if c.Window <= 0 {
		return errors.New("window must be positive")
	}
	if c.GroupBy != "transaction" && c.GroupBy != "trace" {
		return errors.New(`group_by must be "transaction" or "trace"`)
	}
	if c.MaxDepth < 2 {
		return errors.New("max_depth must be at least 2")
	}
	if c.EvictInterval <= 0 {
		return errors.New("evict_interval must be positive")
	}
	if c.Report != reportPerIssue && c.Report != reportEvery {
		return errors.New(`report must be "per_issue" or "every"`)
	}
	if c.IssueInterval <= 0 {
		return errors.New("issue_interval must be positive")
	}
	if c.MaxIssues < 1 {
		return errors.New("max_issues must be at least 1")
	}
	if c.Webhook.URL != "" {
		u, err := url.Parse(string(c.Webhook.URL))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return errors.New("webhook.url must be an http or https URL")
		}
		if c.Webhook.Timeout <= 0 {
			return errors.New("webhook.timeout must be positive")
		}
	}
	if c.Protection.DSN != "" {
		if _, err := pgx.ParseConfig(string(c.Protection.DSN)); err != nil {
			return fmt.Errorf("protection.dsn: %w", err)
		}
		// The same rule as the apps' protection.NewPostgres, so both sides
		// name the same table.
		if err := protection.ValidTable(c.Protection.Table); err != nil {
			return fmt.Errorf("protection.table: %w", err)
		}
	}
	return nil
}
