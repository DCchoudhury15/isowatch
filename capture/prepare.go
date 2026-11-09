package capture

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
)

// prepare expands pgx query options and decides how to run a statement.
// conn is handed to a pgx.QueryRewriter (for example pgx.NamedArgs), the
// same way pgx would do it.
//
// It returns the SQL and arguments to send, and the plan. For a statement
// that is run unchanged because of its options, the original arguments are
// returned as they are, so pgx sees exactly what the caller passed.
func (m *Monitor) prepare(ctx context.Context, conn *pgx.Conn, sql string, args []any) (string, []any, *rewrite.Plan, error) {
	orig := args
	var lead []any
	var rewriter pgx.QueryRewriter
	for len(args) > 0 {
		switch a := args[0].(type) {
		case pgx.QueryExecMode:
			lead = append(lead, a)
		case pgx.QueryRewriter:
			rewriter = a
		case pgx.QueryResultFormats, pgx.QueryResultFormatsByOID:
			// Custom result formats would not cover our hidden columns, so
			// run this statement as is.
			return sql, orig, m.unchangedPlan(sql, "custom result formats"), nil
		default:
			goto done
		}
		args = args[1:]
	}
done:
	if rewriter != nil {
		var err error
		sql, args, err = rewriter.RewriteQuery(ctx, conn, sql, args)
		if err != nil {
			return "", nil, nil, fmt.Errorf("rewrite query failed: %w", err)
		}
	}
	p := m.plan(sql)
	return p.SQL, append(lead, args...), p, nil
}

// unchangedPlan is the plan for a statement IsoWatch runs unchanged for a
// reason the planner cannot see (options, batches, COPY). It keeps the
// coverage fields of the real plan.
func (m *Monitor) unchangedPlan(sql, reason string) *rewrite.Plan {
	p := m.plan(sql)
	return &rewrite.Plan{Kind: rewrite.Untracked, SQL: sql, Reason: reason, Watched: p.Watched, Normalized: p.Normalized}
}

// tracks reports whether a plan is rewritten and recorded.
func tracks(p *rewrite.Plan) bool {
	return p.Kind == rewrite.Read || p.Kind == rewrite.AggRead || p.Kind == rewrite.Write
}
