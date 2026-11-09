package proxy

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func u16(v int) []byte { return binary.BigEndian.AppendUint16(nil, uint16(v)) }
func u32(v int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(v)) }

// bindMsg builds a Bind body with one int parameter, one NULL parameter
// and the given result format codes.
func bindMsg(results ...int) []byte {
	var b []byte
	b = append(b, "p1\x00s1\x00"...)
	b = append(b, u16(1)...)
	b = append(b, u16(1)...) // binary parameters
	b = append(b, u16(2)...)
	b = append(b, u32(4)...)
	b = append(b, u32(7)...)
	b = append(b, u32(-1&0xffffffff)...) // NULL
	b = append(b, u16(len(results))...)
	for _, r := range results {
		b = append(b, u16(r)...)
	}
	return b
}

func TestAddResultFormats(t *testing.T) {
	for _, n := range []int{0, 1} {
		in := bindMsg(make([]int, n)...)
		out, ok := addResultFormats(in, 3)
		if !ok || !bytes.Equal(out, in) {
			t.Fatalf("%d codes: the message must not change", n)
		}
	}
	out, ok := addResultFormats(bindMsg(1, 0, 1), 2)
	if !ok {
		t.Fatal("not read")
	}
	if want := bindMsg(1, 0, 1, 0, 0); !bytes.Equal(out, want) {
		t.Fatalf("got %v\nwant %v", out, want)
	}
	if _, ok := addResultFormats(bindMsg(1, 1)[:20], 2); ok {
		t.Fatal("a cut message must not be read")
	}
	bad := append(bindMsg(1, 1), 0)
	if _, ok := addResultFormats(bad, 2); ok {
		t.Fatal("a message with extra bytes must not be read")
	}
}

func field(name string) []byte {
	b := append([]byte(name), 0)
	return append(b, make([]byte, 18)...)
}

func rowDesc(names ...string) []byte {
	b := u16(len(names))
	for _, n := range names {
		b = append(b, field(n)...)
	}
	return b
}

func TestStripRowDescription(t *testing.T) {
	out, keep, ok := stripRowDescription(rowDesc("id", "name", "__isowatch_0", "__isowatch_1"), 2)
	if !ok || keep != 2 || !bytes.Equal(out, rowDesc("id", "name")) {
		t.Fatalf("ok=%v keep=%d out=%v", ok, keep, out)
	}
	out, keep, ok = stripRowDescription(rowDesc("__isowatch_0", "__isowatch_1"), 2)
	if !ok || keep != 0 || !bytes.Equal(out, rowDesc()) {
		t.Fatalf("all hidden: ok=%v keep=%d", ok, keep)
	}
	if _, _, ok := stripRowDescription(rowDesc("id", "name", "xmin"), 2); ok {
		t.Fatal("columns that are not IsoWatch's own must not be removed")
	}
	if _, _, ok := stripRowDescription(rowDesc("__isowatch_0"), 2); ok {
		t.Fatal("fewer columns than hidden must fail")
	}
	if _, _, ok := stripRowDescription(rowDesc("id", "__isowatch_0")[:10], 1); ok {
		t.Fatal("a cut message must fail")
	}
}

func dataRow(vals ...[]byte) []byte {
	b := u16(len(vals))
	for _, v := range vals {
		if v == nil {
			b = append(b, u32(-1&0xffffffff)...)
			continue
		}
		b = append(b, u32(len(v))...)
		b = append(b, v...)
	}
	return b
}

func TestSplitRow(t *testing.T) {
	row := dataRow([]byte("1"), nil, []byte("hello"), []byte("123"), nil)
	off, tail, err := splitRow(row, 3)
	if err != nil {
		t.Fatal(err)
	}
	if want := dataRow([]byte("1"), nil, []byte("hello")); !bytes.Equal(append(u16(3), row[2:off]...), want) {
		t.Fatal("visible part is wrong")
	}
	if !reflect.DeepEqual(tail, [][]byte{[]byte("123"), nil}) {
		t.Fatalf("hidden values %q", tail)
	}
	if _, _, err := splitRow(row[:len(row)-2], 3); err == nil {
		t.Fatal("a cut row must fail")
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		sql  string
		want control
	}{
		{"SELECT 1", control{}},
		{"BEGIN", control{kind: ctlBegin}},
		{"begin isolation level serializable", control{kind: ctlBegin, iso: "serializable"}},
		{"START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", control{kind: ctlBegin, iso: "repeatable read"}},
		{"BEGIN ISOLATION LEVEL READ UNCOMMITTED", control{kind: ctlBegin, iso: "read committed"}},
		{"COMMIT", control{kind: ctlCommit}},
		{"END", control{kind: ctlCommit}},
		{"COMMIT AND CHAIN", control{kind: ctlCommit, chain: true}},
		{"ABORT", control{kind: ctlRollback}},
		{"ROLLBACK", control{kind: ctlRollback}},
		{"PREPARE TRANSACTION 'x'", control{kind: ctlPrepare}},
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", control{kind: ctlTxnIso, iso: "serializable"}},
		{"SET transaction_isolation = 'repeatable read'", control{kind: ctlTxnIso, iso: "repeatable read"}},
		{"SET default_transaction_isolation TO 'serializable'", control{kind: ctlSessionIso, iso: "serializable"}},
		{"SET LOCAL default_transaction_isolation TO 'serializable'", control{kind: ctlSessionIso, iso: "serializable", local: true}},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ", control{kind: ctlSessionIso, iso: "repeatable read"}},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", control{kind: ctlSessionIso}},
		{"RESET default_transaction_isolation", control{kind: ctlResetIso}},
		{"SET default_transaction_isolation TO DEFAULT", control{kind: ctlResetIso}},
		{"RESET ALL", control{kind: ctlResetIso, searchPath: true}},
		{"DISCARD ALL", control{kind: ctlResetIso, deallocAll: true, searchPath: true}},
		{"DEALLOCATE ALL", control{deallocAll: true}},
		{"DEALLOCATE stmt_1", control{dealloc: []string{"stmt_1"}}},
		{"SET search_path = tenant, public", control{searchPath: true}},
		{"RESET search_path", control{searchPath: true}},
		{"SET work_mem = '64MB'", control{}},
	}
	for _, c := range cases {
		if got := classify(c.sql, false); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s\n got %+v\nwant %+v", c.sql, got, c.want)
		}
	}
	m := classify("SET search_path = x; DEALLOCATE a; SET default_transaction_isolation = 'serializable'", true)
	if m.kind != ctlMulti || !m.searchPath || !m.sessionIso || !reflect.DeepEqual(m.dealloc, []string{"a"}) {
		t.Fatalf("multi: %+v", m)
	}
}

func TestStartupIso(t *testing.T) {
	cases := []struct {
		params map[string]string
		iso    string
		ok     bool
	}{
		{map[string]string{"user": "app"}, "", false},
		{map[string]string{"default_transaction_isolation": "serializable"}, "serializable", true},
		{map[string]string{"options": "-c default_transaction_isolation=serializable"}, "serializable", true},
		{map[string]string{"options": `-cdefault_transaction_isolation=repeatable\ read -c work_mem=1MB`}, "repeatable read", true},
		{map[string]string{"options": "--default-transaction-isolation=serializable"}, "serializable", true},
		{map[string]string{"options": "-c default_transaction_isolation=strange"}, isoUnknown, true},
		{map[string]string{"options": "-c work_mem=1MB"}, "", false},
	}
	for _, c := range cases {
		iso, ok := startupIso(c.params)
		if iso != c.iso || ok != c.ok {
			t.Errorf("%v: got %q %v, want %q %v", c.params, iso, ok, c.iso, c.ok)
		}
	}
}

func TestComments(t *testing.T) {
	sql, comments := splitComments("SELECT '/* not a comment */' FROM t /*traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/")
	if len(comments) != 1 || sql != "SELECT '/* not a comment */' FROM t  " {
		t.Fatalf("sql %q comments %q", sql, comments)
	}
	m := parseMeta(comments)
	if m == nil || m.sc.TraceID().String() != "0af7651916cd43dd8448eb211c80319c" {
		t.Fatalf("trace not read: %+v", m)
	}
	if got := withComments("SELECT 1", []string{"-- end", "/*a*/"}); got != "SELECT 1 -- end\n /*a*/" {
		t.Fatalf("withComments %q", got)
	}

	names := []struct {
		comment, name string
	}{
		{"/*controller='users',action='show'*/", "users#show"},
		{"/*controller:users,action:show*/", "users#show"},
		{"/*action='show'*/", "show"},
		{"/*route='%2Fapi%2Fdeposit',controller='x'*/", "/api/deposit"},
		{"/*isowatch_name='deposit',route='/x'*/", "deposit"},
		{"/*application='app'*/", ""},
	}
	for _, n := range names {
		m := parseMeta([]string{n.comment})
		got := ""
		if m != nil {
			got = m.name
		}
		if got != n.name {
			t.Errorf("%s: name %q, want %q", n.comment, got, n.name)
		}
	}
	if m := parseMeta([]string{"/*traceparent='00-zzzz-1234-01'*/"}); m != nil {
		t.Fatalf("a broken traceparent must be ignored: %+v", m)
	}
}
