// Package rewrite adds hidden columns to SQL statements so IsoWatch can see
// which row versions a statement read or wrote.
//
// For a SELECT on a watched table it appends, per table:
//
//	alias.xmin::text, alias.pk1::text, ...
//
// For INSERT, UPDATE and DELETE on a watched table it appends to RETURNING
// (this needs PostgreSQL 18 or newer):
//
//	old.xmin::text, old.pk1::text, ..., new.xmin::text, new.pk1::text, ...
//
// The rule is: when in doubt, do not rewrite. A statement that is not
// rewritten still runs exactly as written, it is just not tracked. A wrong
// rewrite could make a query fail, which would abort the user's transaction.
package rewrite

import (
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Table describes one watched table.
type Table struct {
	Schema string
	Name   string
	PK     []string // primary key columns, in order
	// Unqualified is true when the bare table name resolves to this table
	// with the default search_path, so queries may omit the schema.
	Unqualified bool
}

// QualifiedName returns schema.name.
func (t *Table) QualifiedName() string { return t.Schema + "." + t.Name }

// Catalog finds watched tables.
type Catalog interface {
	// Lookup returns the watched table for a name as written in SQL.
	// schema is "" when the SQL did not give one.
	Lookup(schema, name string) (*Table, bool)
}

// Kind tells the caller what to do with a statement.
type Kind int

const (
	// Untracked: run the original SQL and record nothing.
	Untracked Kind = iota
	// Read: a SELECT with hidden version columns.
	Read
	// Write: an INSERT, UPDATE or DELETE with hidden RETURNING columns.
	Write
	// Savepoint, Release, RollbackTo: savepoint commands to track.
	Savepoint
	Release
	RollbackTo
	// Forbidden: BEGIN, COMMIT, ROLLBACK and friends must go through the
	// IsoWatch transaction methods instead.
	Forbidden
)

// Group describes one block of hidden columns.
// Read groups have 1+len(PK) columns (xmin, pk...).
// Write groups have 2*(1+len(PK)) columns (old xmin, old pk..., new xmin, new pk...).
type Group struct {
	Table *Table
}

// Plan is the result of planning one statement.
type Plan struct {
	Kind   Kind
	SQL    string  // SQL to send (rewritten, or the original)
	Groups []Group // hidden column groups, in order
	Hidden int     // total hidden columns at the end of each row
	Name   string  // savepoint name, for savepoint kinds
	Reason string  // why a statement is untracked (for metrics and logs)
	// SetsIsolation is true for SET TRANSACTION style statements. After one,
	// IsoWatch no longer trusts the isolation level it was given.
	SetsIsolation bool
}

// Make plans one SQL string.
func Make(sql string, cat Catalog) *Plan {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return &Plan{Kind: Untracked, SQL: sql, Reason: "parse error"}
	}
	if len(tree.Stmts) != 1 {
		return &Plan{Kind: Untracked, SQL: sql, Reason: "multiple statements"}
	}
	stmt := tree.Stmts[0].Stmt

	if ts := stmt.GetTransactionStmt(); ts != nil {
		switch ts.Kind {
		case pg_query.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
			return &Plan{Kind: Savepoint, SQL: sql, Name: strings.ToLower(ts.SavepointName)}
		case pg_query.TransactionStmtKind_TRANS_STMT_RELEASE:
			return &Plan{Kind: Release, SQL: sql, Name: strings.ToLower(ts.SavepointName)}
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
			return &Plan{Kind: RollbackTo, SQL: sql, Name: strings.ToLower(ts.SavepointName)}
		default:
			return &Plan{Kind: Forbidden, SQL: sql, Reason: "transaction control must use Tx methods"}
		}
	}

	if vs := stmt.GetVariableSetStmt(); vs != nil {
		name := strings.ToLower(vs.Name)
		return &Plan{Kind: Untracked, SQL: sql, Reason: "SET statement",
			SetsIsolation: name == "transaction" || name == "transaction_isolation"}
	}

	var p *Plan
	switch {
	case stmt.GetSelectStmt() != nil:
		p = planSelect(stmt.GetSelectStmt(), cat)
	case stmt.GetUpdateStmt() != nil:
		u := stmt.GetUpdateStmt()
		p = planWrite(u.Relation, &u.ReturningList, u.FromClause, cat)
	case stmt.GetDeleteStmt() != nil:
		d := stmt.GetDeleteStmt()
		p = planWrite(d.Relation, &d.ReturningList, d.UsingClause, cat)
	case stmt.GetInsertStmt() != nil:
		i := stmt.GetInsertStmt()
		p = planWrite(i.Relation, &i.ReturningList, nil, cat)
	case stmt.GetMergeStmt() != nil:
		if _, ok := lookup(stmt.GetMergeStmt().Relation, cat); ok {
			return &Plan{Kind: Untracked, SQL: sql, Reason: "MERGE is not supported yet"}
		}
		return &Plan{Kind: Untracked, SQL: sql, Reason: "no watched table"}
	default:
		return &Plan{Kind: Untracked, SQL: sql, Reason: "statement type not tracked"}
	}
	if p.Kind == Untracked {
		p.SQL = sql
		return p
	}
	out, err := pg_query.Deparse(tree)
	if err != nil {
		return &Plan{Kind: Untracked, SQL: sql, Reason: "deparse error"}
	}
	p.SQL = out
	return p
}

func lookup(rv *pg_query.RangeVar, cat Catalog) (*Table, bool) {
	if rv == nil || rv.Catalogname != "" {
		return nil, false
	}
	return cat.Lookup(rv.Schemaname, rv.Relname)
}

// refName is the name SQL uses to refer to a table in FROM: the alias if
// there is one, otherwise the bare table name.
func refName(rv *pg_query.RangeVar) string {
	if rv.Alias != nil && rv.Alias.Aliasname != "" {
		return rv.Alias.Aliasname
	}
	return rv.Relname
}

// colText builds "qualifier.column::text AS __isowatch_N". The unique name
// keeps hidden columns from clashing with yours, for example in ORDER BY id.
func colText(qualifier, column string, n int) *pg_query.Node {
	ref := pg_query.MakeColumnRefNode([]*pg_query.Node{
		pg_query.MakeStrNode(qualifier),
		pg_query.MakeStrNode(column),
	}, -1)
	cast := &pg_query.TypeCast{
		Arg: ref,
		TypeName: &pg_query.TypeName{
			Names:    []*pg_query.Node{pg_query.MakeStrNode("text")},
			Typemod:  -1,
			Location: -1,
		},
		Location: -1,
	}
	return pg_query.MakeResTargetNodeWithNameAndVal(hiddenName(n), &pg_query.Node{Node: &pg_query.Node_TypeCast{TypeCast: cast}}, -1)
}

func hiddenName(n int) string {
	return "__isowatch_" + strconv.Itoa(n)
}

// aggregates are built-in aggregate functions. If one appears in a SELECT
// list, adding a plain column would break the query, so we skip it.
var aggregates = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
	"array_agg": true, "string_agg": true, "json_agg": true, "jsonb_agg": true,
	"json_object_agg": true, "jsonb_object_agg": true, "bool_and": true,
	"bool_or": true, "every": true, "bit_and": true, "bit_or": true, "bit_xor": true,
	"xmlagg": true, "stddev": true, "stddev_pop": true, "stddev_samp": true,
	"variance": true, "var_pop": true, "var_samp": true, "corr": true,
	"covar_pop": true, "covar_samp": true, "regr_avgx": true, "regr_avgy": true,
	"regr_count": true, "regr_intercept": true, "regr_r2": true, "regr_slope": true,
	"regr_sxx": true, "regr_sxy": true, "regr_syy": true, "mode": true,
	"percentile_cont": true, "percentile_disc": true, "rank": true,
	"dense_rank": true, "percent_rank": true, "cume_dist": true,
	"any_value": true, "range_agg": true, "range_intersect_agg": true,
	"json_arrayagg": true, "json_objectagg": true,
}

// hasAggregate walks any protobuf message and reports whether it contains
// an aggregate or window function call.
func hasAggregate(m proto.Message) bool {
	found := false
	var walk func(protoreflect.Message)
	walk = func(msg protoreflect.Message) {
		if found {
			return
		}
		if fc, ok := msg.Interface().(*pg_query.FuncCall); ok {
			if fc.Over != nil || fc.AggStar || fc.AggDistinct || fc.AggFilter != nil || len(fc.AggOrder) > 0 || fc.AggWithinGroup {
				found = true
				return
			}
			if n := len(fc.Funcname); n > 0 {
				if s := fc.Funcname[n-1].GetString_(); s != nil && aggregates[strings.ToLower(s.Sval)] {
					found = true
					return
				}
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
	walk(m.ProtoReflect())
	return found
}

// collectTables gathers watched tables that appear directly in FROM,
// including inside plain joins. Subqueries and functions are skipped.
func collectTables(items []*pg_query.Node, cat Catalog, out *[]*pg_query.RangeVar) {
	for _, it := range items {
		switch {
		case it.GetRangeVar() != nil:
			if _, ok := lookup(it.GetRangeVar(), cat); ok {
				*out = append(*out, it.GetRangeVar())
			}
		case it.GetJoinExpr() != nil:
			j := it.GetJoinExpr()
			// A join with its own alias hides the inner table names.
			if j.Alias != nil {
				continue
			}
			collectTables([]*pg_query.Node{j.Larg, j.Rarg}, cat, out)
		}
	}
}

func planSelect(s *pg_query.SelectStmt, cat Catalog) *Plan {
	switch {
	case s.Op != pg_query.SetOperation_SET_OPERATION_UNDEFINED && s.Op != pg_query.SetOperation_SETOP_NONE:
		return &Plan{Kind: Untracked, Reason: "UNION, INTERSECT or EXCEPT"}
	case len(s.ValuesLists) > 0:
		return &Plan{Kind: Untracked, Reason: "VALUES list"}
	case s.IntoClause != nil:
		return &Plan{Kind: Untracked, Reason: "SELECT INTO"}
	case s.WithClause != nil:
		return &Plan{Kind: Untracked, Reason: "WITH query"}
	case len(s.GroupClause) > 0 || s.GroupDistinct || s.HavingClause != nil:
		return &Plan{Kind: Untracked, Reason: "GROUP BY or HAVING"}
	case len(s.DistinctClause) > 0:
		return &Plan{Kind: Untracked, Reason: "DISTINCT"}
	case len(s.WindowClause) > 0:
		return &Plan{Kind: Untracked, Reason: "WINDOW clause"}
	}
	for _, t := range s.TargetList {
		if hasAggregate(t) {
			return &Plan{Kind: Untracked, Reason: "aggregate or window function"}
		}
	}
	for _, o := range s.SortClause {
		if hasAggregate(o) {
			return &Plan{Kind: Untracked, Reason: "aggregate in ORDER BY"}
		}
	}

	var rvs []*pg_query.RangeVar
	collectTables(s.FromClause, cat, &rvs)
	if len(rvs) == 0 {
		return &Plan{Kind: Untracked, Reason: "no watched table"}
	}
	// Two references with the same name would be ambiguous.
	names := map[string]bool{}
	for _, rv := range rvs {
		n := strings.ToLower(refName(rv))
		if names[n] {
			return &Plan{Kind: Untracked, Reason: "ambiguous table reference"}
		}
		names[n] = true
	}

	p := &Plan{Kind: Read}
	for _, rv := range rvs {
		tbl, _ := lookup(rv, cat)
		ref := refName(rv)
		s.TargetList = append(s.TargetList, colText(ref, "xmin", p.Hidden))
		for i, pk := range tbl.PK {
			s.TargetList = append(s.TargetList, colText(ref, pk, p.Hidden+1+i))
		}
		p.Groups = append(p.Groups, Group{Table: tbl})
		p.Hidden += 1 + len(tbl.PK)
	}
	return p
}

func aliasIsOldOrNew(rv *pg_query.RangeVar) bool {
	if rv == nil || rv.Alias == nil {
		return false
	}
	a := strings.ToLower(rv.Alias.Aliasname)
	return a == "old" || a == "new"
}

func planWrite(rel *pg_query.RangeVar, returning *[]*pg_query.Node, from []*pg_query.Node, cat Catalog) *Plan {
	tbl, ok := lookup(rel, cat)
	if !ok {
		return &Plan{Kind: Untracked, Reason: "no watched table"}
	}
	// RETURNING old.x and new.x would be ambiguous if a table is aliased
	// "old" or "new".
	if aliasIsOldOrNew(rel) {
		return &Plan{Kind: Untracked, Reason: "table alias named old or new"}
	}
	for _, f := range from {
		if aliasIsOldOrNew(f.GetRangeVar()) {
			return &Plan{Kind: Untracked, Reason: "table alias named old or new"}
		}
	}
	n := 0
	for _, side := range []string{"old", "new"} {
		*returning = append(*returning, colText(side, "xmin", n))
		n++
		for _, pk := range tbl.PK {
			*returning = append(*returning, colText(side, pk, n))
			n++
		}
	}
	return &Plan{Kind: Write, Groups: []Group{{Table: tbl}}, Hidden: 2 * (1 + len(tbl.PK))}
}

// Normalize replaces literal values with placeholders so statements can be
// stored without data. It returns "" if the SQL cannot be parsed.
func Normalize(sql string) string {
	n, err := pg_query.Normalize(sql)
	if err != nil {
		return ""
	}
	return n
}
