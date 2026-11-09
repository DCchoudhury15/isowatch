package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
)

// SkippedTable is a table that Config.WatchAll found but could not watch.
type SkippedTable struct {
	Table  string `json:"table"` // schema.name
	Reason string `json:"reason"`
}

// allTablesSQL lists every user table in one row, as JSON, so it works with
// the small Querier interface (QueryRow only). Partitions are left out: the
// partitioned parent is listed instead, and reported as skipped. So are
// temporary tables and tables that belong to an extension.
const allTablesSQL = `
SELECT coalesce(json_agg(json_build_array(n.nspname, c.relname, c.relkind::text, k.pk,
         coalesce(to_regclass(quote_ident(c.relname)) = c.oid, false))
       ORDER BY n.nspname, c.relname), '[]')::text
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL (
  SELECT coalesce((SELECT array_agg(a.attname::text ORDER BY k.ord)
                   FROM pg_index i
                   CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                   JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
                   WHERE i.indrelid = c.oid AND i.indisprimary), '{}'::text[]) AS pk
) k
WHERE c.relkind IN ('r', 'p')
  AND NOT c.relispartition
  AND c.relpersistence <> 't'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname !~ '^pg_(toast|temp_)'
  AND NOT EXISTS (SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')`

// loadTables adds every table when WatchAll is set, then removes the
// excluded ones.
func (m *Monitor) loadTables(ctx context.Context, db Querier) error {
	if m.cfg.WatchAll {
		var raw string
		if err := db.QueryRow(ctx, allTablesSQL).Scan(&raw); err != nil {
			return fmt.Errorf("isowatch: listing tables: %w", err)
		}
		var list [][]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &list); err != nil {
			return fmt.Errorf("isowatch: listing tables: %w", err)
		}
		for _, row := range list {
			var schema, name, kind string
			var pk []string
			var unqualified bool
			if len(row) != 5 ||
				json.Unmarshal(row[0], &schema) != nil || json.Unmarshal(row[1], &name) != nil ||
				json.Unmarshal(row[2], &kind) != nil || json.Unmarshal(row[3], &pk) != nil ||
				json.Unmarshal(row[4], &unqualified) != nil {
				return fmt.Errorf("isowatch: listing tables: unexpected row %s", raw)
			}
			q := schema + "." + name
			if _, ok := m.tables[q]; ok || excluded(m.cfg.ExcludeTables, schema, name) || m.isProtectionTable(schema, name) {
				continue
			}
			switch {
			case kind == "p":
				m.skipped = append(m.skipped, SkippedTable{Table: q, Reason: "partitioned table (not supported yet)"})
			case len(pk) == 0:
				m.skipped = append(m.skipped, SkippedTable{Table: q, Reason: "no primary key"})
			default:
				m.tables[q] = &rewrite.Table{Schema: schema, Name: name, PK: pk, Unqualified: unqualified}
			}
		}
	}
	for q, t := range m.tables {
		if excluded(m.cfg.ExcludeTables, t.Schema, t.Name) {
			delete(m.tables, q)
		}
	}
	if m.cfg.Logger != nil && len(m.skipped) > 0 {
		parts := make([]string, len(m.skipped))
		for i, s := range m.skipped {
			parts[i] = s.Table + " (" + s.Reason + ")"
		}
		m.cfg.Logger.Info("isowatch: some tables are not watched", "tables", strings.Join(parts, ", "))
	}
	return nil
}

// isProtectionTable reports whether a table is the ProtectionStore's own
// table. WatchAll leaves it out: its statements must never be tracked.
func (m *Monitor) isProtectionTable(schema, name string) bool {
	t, ok := m.cfg.ProtectionStore.(interface{ Table() string })
	return ok && excluded([]string{t.Table()}, schema, name)
}

// excluded matches a table against "schema.table", "table" and "schema.*".
func excluded(list []string, schema, name string) bool {
	for _, x := range list {
		if x == schema+"."+name || x == name || x == schema+".*" {
			return true
		}
	}
	return false
}

// Tables returns the watched tables as "schema.name", sorted.
func (m *Monitor) Tables() []string {
	out := make([]string, 0, len(m.tables))
	for q := range m.tables {
		out = append(out, q)
	}
	sort.Strings(out)
	return out
}

// SkippedTables returns the tables Config.WatchAll found but could not
// watch, and why. Statements on them are never tracked.
func (m *Monitor) SkippedTables() []SkippedTable {
	return append([]SkippedTable(nil), m.skipped...)
}
