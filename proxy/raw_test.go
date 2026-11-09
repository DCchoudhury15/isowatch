package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// hijack connects with pgconn (which does the authentication) and then
// takes over the connection to send protocol messages by hand.
func hijack(t *testing.T, host string, port uint16) *pgconn.HijackedConn {
	t.Helper()
	cfg, err := pgconn.ParseConfig(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	if host != "" {
		cfg.Host, cfg.Port = host, port
	}
	cfg.Fallbacks, cfg.TLSConfig = nil, nil
	pc, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	hc, err := pc.Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hc.Conn.Close() })
	return hc
}

// exchange sends messages and returns every reply up to ReadyForQuery,
// encoded, so two runs can be compared byte for byte.
func exchange(t *testing.T, hc *pgconn.HijackedConn, msgs ...pgproto3.FrontendMessage) []string {
	t.Helper()
	for _, m := range msgs {
		hc.Frontend.Send(m)
	}
	if err := hc.Frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	var out []string
	_ = hc.Conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		m, err := hc.Frontend.Receive()
		if err != nil {
			t.Fatalf("after %d replies %v: %v", len(out), out, err)
		}
		b, err := m.Encode(nil)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%T %q", m, b))
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			return out
		}
	}
}

// TestSameMessagesAsDirect sends the same protocol messages directly and
// through the proxy. Every reply must be the same, byte for byte: no extra
// or missing message, no hidden column, nothing reordered.
func TestSameMessagesAsDirect(t *testing.T) {
	tb := tableName("px_raw")
	schema := tableName("px_rawschema")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int, v int"})
	h.exec(fmt.Sprintf("INSERT INTO %s SELECT g, g * 10, g FROM generate_series(1, 6) g", tb))
	h.exec(fmt.Sprintf("CREATE SCHEMA %s; CREATE TABLE %s.%s (k int PRIMARY KEY, v int); INSERT INTO %s.%s VALUES (1, 99)", schema, schema, tb, schema, tb))
	t.Cleanup(func() { _, _ = h.direct.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })

	steps := [][]pgproto3.FrontendMessage{
		{
			&pgproto3.Parse{Name: "s1", Query: fmt.Sprintf("SELECT id, n FROM %s WHERE id <= $1 ORDER BY id", tb)},
			&pgproto3.Describe{ObjectType: 'S', Name: "s1"},
			&pgproto3.Parse{Name: "s2", Query: fmt.Sprintf("SELECT v FROM %s WHERE v > 0 /*route='x'*/", tb)},
			&pgproto3.Sync{},
		},
		{&pgproto3.Query{String: "BEGIN"}},
		{
			&pgproto3.Bind{DestinationPortal: "p1", PreparedStatement: "s1", Parameters: [][]byte{[]byte("3")}, ResultFormatCodes: []int16{1}},
			&pgproto3.Describe{ObjectType: 'P', Name: "p1"},
			&pgproto3.Execute{Portal: "p1", MaxRows: 2},
			&pgproto3.Execute{Portal: "p1", MaxRows: 2},
			&pgproto3.Sync{},
		},
		{
			&pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("5")}, ResultFormatCodes: []int16{0, 1}},
			&pgproto3.Execute{},
			&pgproto3.Sync{},
		},
		{
			&pgproto3.Parse{Query: fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = $1", tb)},
			&pgproto3.Describe{ObjectType: 'S'},
			&pgproto3.Bind{Parameters: [][]byte{[]byte("1")}},
			&pgproto3.Describe{ObjectType: 'P'},
			&pgproto3.Execute{},
			&pgproto3.Sync{},
		},
		{
			&pgproto3.Parse{Query: fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = $1 RETURNING n, id", tb)},
			&pgproto3.Bind{Parameters: [][]byte{[]byte("2")}, ResultFormatCodes: []int16{1, 0}},
			&pgproto3.Describe{ObjectType: 'P'},
			&pgproto3.Execute{},
			&pgproto3.Sync{},
		},
		{&pgproto3.Query{String: fmt.Sprintf("SELECT n FROM %s WHERE id = 2; SELECT 1", tb)}},
		{&pgproto3.Query{String: fmt.Sprintf("UPDATE %s SET n = n WHERE id = 3", tb)}},
		{&pgproto3.Query{String: fmt.Sprintf("SELECT * FROM %s WHERE id = 1", tb)}},
		{&pgproto3.Query{String: ""}},
		{&pgproto3.Query{String: "ROLLBACK"}},
		{
			&pgproto3.Parse{Name: "bad", Query: "SELEC 1"},
			&pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("1")}},
			&pgproto3.Execute{},
			&pgproto3.Sync{},
		},
		{&pgproto3.Query{String: fmt.Sprintf("SET search_path = %s, public", schema)}},
		{
			&pgproto3.Bind{PreparedStatement: "s2", ResultFormatCodes: []int16{1}},
			&pgproto3.Execute{},
			&pgproto3.Sync{},
		},
		{&pgproto3.Query{String: "RESET search_path"}},
		{
			&pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("2")}},
			&pgproto3.Execute{},
			&pgproto3.Sync{},
		},
		{
			&pgproto3.Close{ObjectType: 'S', Name: "s1"},
			&pgproto3.Sync{},
		},
	}
	// All steps directly first: its transaction holds row locks until its
	// ROLLBACK, so the two runs cannot overlap.
	direct := hijack(t, "", 0)
	var want [][]string
	for _, step := range steps {
		want = append(want, exchange(t, direct, step...))
	}
	through := hijack(t, "127.0.0.1", h.port)
	for i, step := range steps {
		got := exchange(t, through, step...)
		if fmt.Sprint(got) != fmt.Sprint(want[i]) {
			t.Fatalf("step %d: replies differ\nthrough the proxy:\n%v\ndirectly:\n%v", i, got, want[i])
		}
	}
	// Rolled back, failed, or untracked because of search_path: only the
	// last read of s1 is reported.
	txns, _ := h.reported()
	if len(txns) != 1 || len(txns[0].Reads) != 2 {
		t.Fatalf("want one transaction with 2 reads, got %+v", txns)
	}
}
