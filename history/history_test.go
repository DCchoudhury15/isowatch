package history

import (
	"reflect"
	"testing"
)

func TestHistoryRoundTrip(t *testing.T) {
	in := &Txn{
		Xids:       []uint32{7, 8},
		Reads:      []Read{{Table: "public.a", Key: "1", Xmin: 5, Stmt: 0, Q: 0}},
		Writes:     []Write{{Table: "public.a", OldKey: "1", OldXmin: 5, NewKey: "1", NewXmin: 7, Stmt: 1, Q: 1}},
		Statements: []string{"SELECT n FROM a WHERE id = $1", "UPDATE a SET n = $1 WHERE id = $2"},
	}
	s, err := EncodeHistory(in)
	if err != nil {
		t.Fatal(err)
	}
	out := &Txn{}
	if err := DecodeHistory(s, out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in.Reads, out.Reads) || !reflect.DeepEqual(in.Writes, out.Writes) || !reflect.DeepEqual(in.Statements, out.Statements) {
		t.Fatalf("round trip changed data:\n in  %+v\n out %+v", in, out)
	}
	if err := DecodeHistory("{bad", out); err == nil {
		t.Fatal("bad JSON must fail")
	}
}

func TestVersionsOfInsertUpdateDelete(t *testing.T) {
	ins := Write{Table: "t", NewKey: "1", NewXmin: 9}
	if _, ok := ins.Replaced(); ok {
		t.Fatal("an insert replaces nothing")
	}
	del := Write{Table: "t", OldKey: "1", OldXmin: 9}
	if _, ok := del.Created(); ok {
		t.Fatal("a delete creates nothing")
	}
	upd := Write{Table: "t", OldKey: "1", OldXmin: 9, NewKey: "2", NewXmin: 10}
	o, _ := upd.Replaced()
	n, _ := upd.Created()
	if o.Key != "1" || n.Key != "2" {
		t.Fatal("an update can change the key")
	}
	txn := &Txn{Xids: []uint32{10}}
	if !txn.Owns(10) || txn.Owns(9) {
		t.Fatal("Owns is wrong")
	}
}
