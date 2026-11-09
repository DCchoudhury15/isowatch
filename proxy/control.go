package proxy

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// ctlKind is what a statement does to the transaction or the session,
// beyond reading and writing rows.
type ctlKind int

const (
	ctlNone       ctlKind = iota
	ctlBegin              // BEGIN, START TRANSACTION
	ctlCommit             // COMMIT, END (the command tag says if it really committed)
	ctlRollback           // ROLLBACK, ABORT
	ctlPrepare            // PREPARE TRANSACTION: not committed yet
	ctlTxnIso             // SET TRANSACTION ISOLATION LEVEL, SET transaction_isolation
	ctlSessionIso         // SET default_transaction_isolation, SET SESSION CHARACTERISTICS
	ctlResetIso           // RESET ALL, RESET default_transaction_isolation, DISCARD ALL
	ctlMulti              // several statements in one string
)

// isoUnknown marks an isolation level the proxy could not work out.
const isoUnknown = "?"

// control is what classify found out about one SQL string.
type control struct {
	kind  ctlKind
	iso   string // for ctlBegin, ctlTxnIso, ctlSessionIso: "" = not given, isoUnknown = unclear
	chain bool   // COMMIT AND CHAIN, ROLLBACK AND CHAIN
	local bool   // SET LOCAL

	// Effects on prepared statements (DEALLOCATE, DISCARD ALL).
	deallocAll bool
	dealloc    []string

	// searchPath is true when the string may change search_path.
	searchPath bool
	// sessionIso is true when a multi statement string may change the
	// session's isolation level.
	sessionIso bool
}

// mayControl reports whether a statement could be one classify cares
// about, from its first word. It keeps the parser away from ordinary
// statements.
func mayControl(sql string) bool {
	s := strings.TrimLeft(sql, " \t\r\n(")
	i := strings.IndexAny(s, " \t\r\n;(")
	if i >= 0 {
		s = s[:i]
	}
	switch strings.ToLower(s) {
	case "begin", "start", "commit", "end", "rollback", "abort", "prepare",
		"set", "reset", "discard", "deallocate":
		return true
	}
	return false
}

// classify reads transaction control, isolation settings and prepared
// statement effects from SQL. multi says the string has several
// statements.
func classify(sql string, multi bool) control {
	if !multi && !mayControl(sql) {
		return control{}
	}
	tree, err := pg_query.Parse(sql)
	if err != nil || len(tree.Stmts) == 0 {
		if multi {
			return control{kind: ctlMulti, searchPath: true, sessionIso: true}
		}
		return control{}
	}
	if len(tree.Stmts) > 1 || multi {
		out := control{kind: ctlMulti}
		for _, raw := range tree.Stmts {
			c := classifyStmt(raw.Stmt)
			out.deallocAll = out.deallocAll || c.deallocAll
			out.dealloc = append(out.dealloc, c.dealloc...)
			out.searchPath = out.searchPath || c.searchPath
			if c.kind == ctlSessionIso || c.kind == ctlResetIso {
				out.sessionIso = true
			}
		}
		return out
	}
	return classifyStmt(tree.Stmts[0].Stmt)
}

func classifyStmt(n *pg_query.Node) control {
	switch {
	case n.GetTransactionStmt() != nil:
		ts := n.GetTransactionStmt()
		switch ts.Kind {
		case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN, pg_query.TransactionStmtKind_TRANS_STMT_START:
			return control{kind: ctlBegin, iso: isoFromOptions(ts.Options)}
		case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
			return control{kind: ctlCommit, chain: ts.Chain}
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			return control{kind: ctlRollback, chain: ts.Chain}
		case pg_query.TransactionStmtKind_TRANS_STMT_PREPARE:
			return control{kind: ctlPrepare}
		}
	case n.GetVariableSetStmt() != nil:
		vs := n.GetVariableSetStmt()
		name := strings.ToLower(vs.Name)
		switch vs.Kind {
		case pg_query.VariableSetKind_VAR_RESET_ALL:
			return control{kind: ctlResetIso, searchPath: true}
		case pg_query.VariableSetKind_VAR_RESET, pg_query.VariableSetKind_VAR_SET_DEFAULT:
			switch name {
			case "default_transaction_isolation":
				return control{kind: ctlResetIso, local: vs.IsLocal}
			case "search_path":
				return control{searchPath: true}
			}
			return control{}
		}
		switch name {
		case "transaction":
			return control{kind: ctlTxnIso, iso: isoFromOptions(vs.Args)}
		case "session characteristics":
			return control{kind: ctlSessionIso, iso: isoFromOptions(vs.Args), local: vs.IsLocal}
		case "transaction_isolation":
			return control{kind: ctlTxnIso, iso: isoFromArgs(vs.Args)}
		case "default_transaction_isolation":
			return control{kind: ctlSessionIso, iso: isoFromArgs(vs.Args), local: vs.IsLocal}
		case "search_path":
			return control{searchPath: true}
		}
	case n.GetDiscardStmt() != nil:
		if n.GetDiscardStmt().Target == pg_query.DiscardMode_DISCARD_ALL {
			return control{kind: ctlResetIso, deallocAll: true, searchPath: true}
		}
	case n.GetDeallocateStmt() != nil:
		ds := n.GetDeallocateStmt()
		if ds.Isall || ds.Name == "" {
			return control{deallocAll: true}
		}
		return control{dealloc: []string{ds.Name}}
	}
	return control{}
}

// isoFromOptions finds transaction_isolation in BEGIN or SET TRANSACTION
// options. It returns "" when no level is given.
func isoFromOptions(opts []*pg_query.Node) string {
	for _, o := range opts {
		d := o.GetDefElem()
		if d == nil || d.Defname != "transaction_isolation" {
			continue
		}
		return isoFromArgs([]*pg_query.Node{d.Arg})
	}
	return ""
}

// isoFromArgs reads one string value, as in SET x = 'value'.
func isoFromArgs(args []*pg_query.Node) string {
	if len(args) != 1 || args[0] == nil {
		return isoUnknown
	}
	c := args[0].GetAConst()
	if c == nil || c.GetSval() == nil {
		return isoUnknown
	}
	return normIso(c.GetSval().Sval)
}

// normIso turns a level as PostgreSQL accepts it into the names IsoWatch
// uses.
func normIso(s string) string {
	switch strings.Join(strings.Fields(strings.ToLower(s)), " ") {
	case "read committed", "read uncommitted":
		return "read committed"
	case "repeatable read":
		return "repeatable read"
	case "serializable":
		return "serializable"
	}
	return isoUnknown
}

// startupIso reads default_transaction_isolation from startup parameters:
// directly, or from "-c name=value" in options. ok is false when it is not
// set there.
func startupIso(params map[string]string) (iso string, ok bool) {
	if v, found := params["default_transaction_isolation"]; found {
		return normIso(v), true
	}
	opts := params["options"]
	// PostgreSQL also accepts dashes in place of underscores here.
	if !strings.Contains(strings.ReplaceAll(opts, "-", "_"), "default_transaction_isolation") {
		return "", false
	}
	for _, kv := range splitOptions(opts) {
		k, v, found := strings.Cut(kv, "=")
		if found && strings.ReplaceAll(k, "-", "_") == "default_transaction_isolation" {
			iso, ok = normIso(v), true
		}
	}
	if !ok {
		return isoUnknown, true // mentioned, but not in a form we understand
	}
	return iso, true
}

// splitOptions splits the options startup parameter into name=value
// settings. Spaces separate arguments, and a backslash escapes the next
// character. Both "-c name=value" and "--name=value" are accepted.
func splitOptions(s string) []string {
	var args []string
	var cur strings.Builder
	escaped, have := false, false
	for _, r := range s {
		switch {
		case escaped:
			cur.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped, have = true, true
		case r == ' ' || r == '\t':
			if have {
				args = append(args, cur.String())
				cur.Reset()
				have = false
			}
		default:
			cur.WriteRune(r)
			have = true
		}
	}
	if have {
		args = append(args, cur.String())
	}
	var out []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-c" && i+1 < len(args):
			out = append(out, args[i+1])
			i++
		case strings.HasPrefix(a, "-c"):
			out = append(out, a[2:])
		case strings.HasPrefix(a, "--"):
			out = append(out, a[2:])
		}
	}
	return out
}
