package isowatchprocessor

import (
	"errors"
	"time"
)

// Config is the processor configuration.
//
//	processors:
//	  isowatch:
//	    window: 2m            # keep transactions this long (longer than your longest transaction)
//	    group_by: transaction # or "trace" to also find anomalies across a whole request
//	    max_depth: 16         # longest cycle to look for
//	    evict_interval: 5s    # how often to forget old transactions
//	    drop_txn_spans: false # true removes the isowatch.txn spans after checking them
//	    ignore_tables: []     # anomalies only on these tables are acceptable
//	    ignore_names: []      # transactions with these names may race
type Config struct {
	Window        time.Duration `mapstructure:"window"`
	GroupBy       string        `mapstructure:"group_by"`
	MaxDepth      int           `mapstructure:"max_depth"`
	EvictInterval time.Duration `mapstructure:"evict_interval"`
	DropTxnSpans  bool          `mapstructure:"drop_txn_spans"`
	IgnoreTables  []string      `mapstructure:"ignore_tables"`
	IgnoreNames   []string      `mapstructure:"ignore_names"`
}

func createDefaultConfig() *Config {
	return &Config{
		Window:        2 * time.Minute,
		GroupBy:       "transaction",
		MaxDepth:      16,
		EvictInterval: 5 * time.Second,
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
	return nil
}
