// Package history defines what IsoWatch records about one database
// transaction: which row versions it read and which row versions it wrote.
//
// A row version is named by the table, the row's primary key and the xmin of
// that version. In PostgreSQL, xmin is the id of the transaction that wrote
// the version, so it tells us exactly which write a read saw.
package history

import (
	"strconv"
	"time"
)

// Version names one version of one row.
type Version struct {
	Table string `json:"t"`
	Key   string `json:"k"`
	Xmin  uint32 `json:"x"`
}

// String returns a compact, stable form used as a map key.
func (v Version) String() string {
	return v.Table + "\x00" + v.Key + "\x00" + strconv.FormatUint(uint64(v.Xmin), 10)
}

// Read is one row version a transaction saw.
type Read struct {
	Table string `json:"t"`
	Key   string `json:"k"`
	Xmin  uint32 `json:"x"`
	// Stmt is the position of the statement in the transaction (0, 1, 2...).
	// It orders reads and writes. Q indexes Txn.Statements for the SQL text.
	Stmt int `json:"s"`
	Q    int `json:"q"`
}

// Version returns the row version this read saw.
func (r Read) Version() Version { return Version{Table: r.Table, Key: r.Key, Xmin: r.Xmin} }

// Write is one row change made by a transaction.
//
// Insert: OldXmin is 0. Delete: NewXmin is 0. Update: both are set.
// An update may change the primary key, so the old and new keys are kept apart.
type Write struct {
	Table   string `json:"t"`
	OldKey  string `json:"ok,omitempty"`
	OldXmin uint32 `json:"ox,omitempty"`
	NewKey  string `json:"nk,omitempty"`
	NewXmin uint32 `json:"nx,omitempty"`
	Stmt    int    `json:"s"`
	Q       int    `json:"q"`
}

// Replaced returns the version this write replaced, if any.
func (w Write) Replaced() (Version, bool) {
	if w.OldXmin == 0 {
		return Version{}, false
	}
	return Version{Table: w.Table, Key: w.OldKey, Xmin: w.OldXmin}, true
}

// Created returns the version this write created, if any.
func (w Write) Created() (Version, bool) {
	if w.NewXmin == 0 {
		return Version{}, false
	}
	return Version{Table: w.Table, Key: w.NewKey, Xmin: w.NewXmin}, true
}

// Txn is one committed transaction as seen by IsoWatch.
type Txn struct {
	// ID is unique per transaction. It is not the PostgreSQL xid, because
	// read-only transactions never get an xid.
	ID string `json:"id"`
	// Name is an optional label for the kind of transaction, like "deposit".
	// Names let you ignore or protect a kind of transaction.
	Name string `json:"name,omitempty"`
	// Xids are every PostgreSQL transaction id this transaction wrote with:
	// the top level id plus any savepoint (subtransaction) ids.
	Xids      []uint32  `json:"xids,omitempty"`
	Isolation string    `json:"iso,omitempty"` // "read committed", "repeatable read" or "serializable"
	Start     time.Time `json:"start"`
	Commit    time.Time `json:"commit"`

	// Trace context of the request that ran the transaction.
	TraceID string `json:"trace,omitempty"`
	SpanID  string `json:"span,omitempty"`

	Reads      []Read   `json:"reads,omitempty"`
	Writes     []Write  `json:"writes,omitempty"`
	Statements []string `json:"stmts,omitempty"` // distinct normalized SQL, no literal values
}

// Owns reports whether the given xmin was written by this transaction.
func (t *Txn) Owns(xmin uint32) bool {
	for _, x := range t.Xids {
		if x == xmin {
			return true
		}
	}
	return false
}

// Statement returns the normalized SQL for a Q index, or "".
func (t *Txn) Statement(i int) string {
	if i < 0 || i >= len(t.Statements) {
		return ""
	}
	return t.Statements[i]
}
