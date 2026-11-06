package checker

import (
	"testing"
	"time"

	"github.com/DCchoudhury15/isowatch/history"
)

// One request per trace: a read in one transaction, then a write in a later
// one, like two statements run outside a transaction.
func splitRequest(trace string, readXmin, oldXmin, newXmin uint32, readAt, writeAt int) (*history.Txn, *history.Txn) {
	r := txn(trace+"-read", nil, []history.Read{rd("acct", "1", readXmin, 0)}, nil)
	r.TraceID, r.Start, r.Commit = trace, at(readAt), at(readAt).Add(time.Millisecond)
	w := txn(trace+"-write", []uint32{newXmin}, nil, []history.Write{upd("acct", "1", oldXmin, newXmin, 0)})
	w.TraceID, w.Start, w.Commit = trace, at(writeAt), at(writeAt).Add(time.Millisecond)
	return r, w
}

// In ByTrace mode the read of an earlier transaction in the same request is
// what the write was based on, so lost writes and skipped versions must be
// counted across transactions too.
func TestByTraceCountsLostWritesAcrossTransactions(t *testing.T) {
	ra, wa := splitRequest("reqA", 700, 700, 701, 1, 3)
	rb, wb := splitRequest("reqB", 700, 701, 702, 2, 4) // B read 700 but replaced A's 701
	c := New(Options{GroupBy: ByTrace})
	var all []Anomaly
	for _, x := range []*history.Txn{ra, rb, wa, wb} {
		all = append(all, c.Add(x)...)
	}
	a := mustOne(t, all, LostUpdate)
	if a.SkippedVersions != 1 {
		t.Fatalf("B replaced 1 version it never saw, got SkippedVersions=%d", a.SkippedVersions)
	}
	if lost := c.LostWrites(); lost != 1 {
		t.Fatalf("A's write was lost, LostWrites = %d, want 1", lost)
	}
}

// Writes based on an up to date read lose nothing.
func TestByTraceSequentialRequestsLoseNothing(t *testing.T) {
	ra, wa := splitRequest("reqA", 700, 700, 701, 1, 2)
	rb, wb := splitRequest("reqB", 701, 701, 702, 3, 4)
	c := New(Options{GroupBy: ByTrace})
	for _, x := range []*history.Txn{ra, wa, rb, wb} {
		if got := c.Add(x); len(got) != 0 {
			t.Fatalf("no anomaly expected, got %+v", got)
		}
	}
	if lost := c.LostWrites(); lost != 0 {
		t.Fatalf("LostWrites = %d, want 0", lost)
	}
}

// A read that was still running when the write started may not be what the
// write was based on, so it is not used. The anomaly is still reported (it
// comes from the graph), only the count is left out.
func TestByTraceIgnoresOverlappingReads(t *testing.T) {
	ra, wa := splitRequest("reqA", 700, 700, 701, 1, 3)
	rb, wb := splitRequest("reqB", 700, 701, 702, 2, 4)
	rb.Commit = wb.Start.Add(time.Millisecond) // the read finished after the write began
	c := New(Options{GroupBy: ByTrace})
	var all []Anomaly
	for _, x := range []*history.Txn{ra, rb, wa, wb} {
		all = append(all, c.Add(x)...)
	}
	mustOne(t, all, LostUpdate)
	if lost := c.LostWrites(); lost != 0 {
		t.Fatalf("an overlapping read must not count as a base, LostWrites = %d", lost)
	}
}

// Per transaction there is no base read across transactions.
func TestByTransactionKeepsReadsInsideTheTransaction(t *testing.T) {
	ra, wa := splitRequest("reqA", 700, 700, 701, 1, 3)
	rb, wb := splitRequest("reqB", 700, 701, 702, 2, 4)
	c := New(Options{})
	for _, x := range []*history.Txn{ra, rb, wa, wb} {
		c.Add(x)
	}
	if lost := c.LostWrites(); lost != 0 {
		t.Fatalf("LostWrites = %d, want 0 when grouping by transaction", lost)
	}
}
