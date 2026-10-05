package history

import (
	"encoding/json"
	"fmt"
)

// Span and attribute names used when a transaction travels as an
// OpenTelemetry span. The capture library writes them and the collector
// processor reads them.
const (
	SpanName      = "isowatch.txn"
	AnomalySpan   = "isowatch.anomaly"
	AttrSchema    = "isowatch.schema"
	AttrTxnID     = "isowatch.txn.id"
	AttrTxnName   = "isowatch.txn.name"
	AttrIsolation = "isowatch.txn.isolation"
	AttrXids      = "isowatch.txn.xids"
	AttrHistory   = "isowatch.txn.history"
	SchemaVersion = 1
)

// payload is the part of a Txn that travels as one JSON attribute.
type payload struct {
	Reads      []Read   `json:"r,omitempty"`
	Writes     []Write  `json:"w,omitempty"`
	Statements []string `json:"q,omitempty"`
}

// EncodeHistory returns the JSON for the reads, writes and statements.
func EncodeHistory(t *Txn) (string, error) {
	b, err := json.Marshal(payload{Reads: t.Reads, Writes: t.Writes, Statements: t.Statements})
	return string(b), err
}

// DecodeHistory fills the reads, writes and statements of t from JSON.
func DecodeHistory(s string, t *Txn) error {
	var p payload
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		return fmt.Errorf("isowatch: bad %s attribute: %w", AttrHistory, err)
	}
	t.Reads, t.Writes, t.Statements = p.Reads, p.Writes, p.Statements
	return nil
}
