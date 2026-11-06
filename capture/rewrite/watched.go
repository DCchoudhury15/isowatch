package rewrite

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// usesWatched reports whether any statement in the tree can read or write
// table rows and names a watched table anywhere: directly in FROM, inside a
// subquery or CTE, as a COPY target, and so on. It is used for coverage
// counts only. It never decides whether a statement is rewritten.
//
// Commands that do not read or write rows (CREATE INDEX, LOCK, ALTER, SET,
// ...) are left out, so they do not show up as gaps in the coverage report.
// A CTE that has the same name as a watched table is counted as the table.
func usesWatched(tree *pg_query.ParseResult, cat Catalog) bool {
	for _, raw := range tree.Stmts {
		if s := raw.GetStmt(); readsOrWritesRows(s) && namesWatched(s, cat) {
			return true
		}
	}
	return false
}

// readsOrWritesRows reports whether a statement can read or write rows.
func readsOrWritesRows(s *pg_query.Node) bool {
	switch {
	case s == nil:
		return false
	case s.GetSelectStmt() != nil, s.GetInsertStmt() != nil, s.GetUpdateStmt() != nil,
		s.GetDeleteStmt() != nil, s.GetMergeStmt() != nil, s.GetCopyStmt() != nil,
		s.GetTruncateStmt() != nil, s.GetCreateTableAsStmt() != nil,
		s.GetDeclareCursorStmt() != nil, s.GetPrepareStmt() != nil:
		return true
	case s.GetExplainStmt() != nil:
		// Plain EXPLAIN only plans the query. EXPLAIN ANALYZE runs it.
		for _, o := range s.GetExplainStmt().Options {
			if d := o.GetDefElem(); d != nil && strings.EqualFold(d.Defname, "analyze") {
				return true
			}
		}
	}
	return false
}

// namesWatched walks a statement and reports whether any table reference
// resolves to a watched table.
func namesWatched(s *pg_query.Node, cat Catalog) bool {
	found := false
	var walk func(protoreflect.Message)
	walk = func(msg protoreflect.Message) {
		if found {
			return
		}
		if rv, ok := msg.Interface().(*pg_query.RangeVar); ok {
			if _, ok := cat.Lookup(rv.Schemaname, rv.Relname); ok {
				found = true
				return
			}
		}
		msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			switch {
			case fd.IsList() && fd.Message() != nil:
				l := v.List()
				for i := 0; i < l.Len(); i++ {
					walk(l.Get(i).Message())
				}
			case fd.Message() != nil && !fd.IsMap():
				walk(v.Message())
			}
			return !found
		})
	}
	walk(s.ProtoReflect())
	return found
}

// refineReason makes the reason for an untracked statement that uses a
// watched table more precise than the generic one the planner gave.
func refineReason(s *pg_query.Node, reason string) string {
	switch reason {
	case reasonNoWatched:
		if s.GetSelectStmt() != nil {
			return "watched table only in a subquery or an aliased join"
		}
		return "watched table only read by a write to another table"
	case reasonStatementType:
		switch {
		case s.GetCopyStmt() != nil:
			return "COPY"
		case s.GetTruncateStmt() != nil:
			return "TRUNCATE"
		case s.GetExplainStmt() != nil:
			return "EXPLAIN ANALYZE"
		case s.GetCreateTableAsStmt() != nil:
			return "CREATE TABLE AS"
		case s.GetDeclareCursorStmt() != nil:
			return "cursor (DECLARE)"
		case s.GetPrepareStmt() != nil:
			return "PREPARE"
		}
	}
	return reason
}
