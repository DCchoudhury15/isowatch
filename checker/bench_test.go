package checker

import (
	"fmt"
	"testing"
	"time"

	"github.com/DCchoudhury15/isowatch/history"
)

// BenchmarkAddContended: every transaction reads and writes the same hot row
// (worst case: long version chains and many edges).
func BenchmarkAddContended(b *testing.B) {
	c := New(Options{Window: time.Hour})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		x := uint32(i + 2)
		c.Add(&history.Txn{
			ID: fmt.Sprint(i), Xids: []uint32{x}, Commit: t0,
			Reads:  []history.Read{{Table: "acct", Key: "1", Xmin: x - 1, Stmt: 0}},
			Writes: []history.Write{{Table: "acct", OldKey: "1", OldXmin: x - 1, NewKey: "1", NewXmin: x, Stmt: 1}},
		})
	}
}

// BenchmarkAddSpread: transactions touch 10,000 different rows (typical case).
func BenchmarkAddSpread(b *testing.B) {
	c := New(Options{Window: time.Hour})
	last := map[string]uint32{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		x := uint32(i + 1_000_000)
		key := fmt.Sprint(i % 10000)
		prev, ok := last[key]
		if !ok {
			prev = 1
		}
		last[key] = x
		c.Add(&history.Txn{
			ID: fmt.Sprint(i), Xids: []uint32{x}, Commit: t0,
			Reads:  []history.Read{{Table: "acct", Key: key, Xmin: prev, Stmt: 0}},
			Writes: []history.Write{{Table: "acct", OldKey: key, OldXmin: prev, NewKey: key, NewXmin: x, Stmt: 1}},
		})
	}
}
