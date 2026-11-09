package proxy

import (
	"context"
	"net/url"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// splitComments removes SQL comments and returns the SQL without them plus
// the comments, in order. Each comment is replaced by one space, which is
// what PostgreSQL itself treats a comment as. Removing comments lets
// statements that only differ in their sqlcommenter tags share one plan.
//
// It uses PostgreSQL's own scanner, so "/*" inside a string literal or a
// quoted name is not mistaken for a comment. If the SQL cannot be scanned,
// it is returned unchanged.
func splitComments(sql string) (string, []string) {
	if !strings.Contains(sql, "/*") && !strings.Contains(sql, "--") {
		return sql, nil
	}
	res, err := pg_query.Scan(sql)
	if err != nil {
		return sql, nil
	}
	var b strings.Builder
	var comments []string
	last := 0
	for _, tok := range res.Tokens {
		if tok.Token != pg_query.Token_C_COMMENT && tok.Token != pg_query.Token_SQL_COMMENT {
			continue
		}
		start, end := int(tok.Start), int(tok.End)
		if start < last || end > len(sql) || start > end {
			return sql, nil
		}
		b.WriteString(sql[last:start])
		b.WriteByte(' ')
		comments = append(comments, sql[start:end])
		last = end
	}
	if comments == nil {
		return sql, nil
	}
	b.WriteString(sql[last:])
	return b.String(), comments
}

// withComments puts the comments back at the end of a rewritten statement,
// so tools that read them from the database logs (for example query
// insights) still see them.
func withComments(sql string, comments []string) string {
	if len(comments) == 0 {
		return sql
	}
	var b strings.Builder
	b.WriteString(sql)
	for _, c := range comments {
		b.WriteByte(' ')
		b.WriteString(c)
		if strings.HasPrefix(c, "--") {
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// meta is what the comments of one statement say about the request that
// sent it.
type meta struct {
	sc       trace.SpanContext
	name     string
	nameRank int // which key the name came from; a higher rank wins
}

const (
	rankNone = iota
	rankAction
	rankController
	rankControllerAction
	rankRoute
	rankExplicit
)

// parseMeta reads sqlcommenter style tags from block comments. It accepts
// the sqlcommenter form key='value' (values URL encoded), plain key=value,
// and the key:value form used by Rails' Marginalia and query log tags.
func parseMeta(comments []string) *meta {
	var tags map[string]string
	for _, c := range comments {
		if !strings.HasPrefix(c, "/*") || !strings.HasSuffix(c, "*/") || len(c) < 4 {
			continue
		}
		for k, v := range parseTags(c[2 : len(c)-2]) {
			if tags == nil {
				tags = map[string]string{}
			}
			if _, seen := tags[k]; !seen {
				tags[k] = v
			}
		}
	}
	if tags == nil {
		return nil
	}
	m := &meta{}
	if tp := tags["traceparent"]; tp != "" {
		carrier := propagation.MapCarrier{"traceparent": tp}
		if ts := tags["tracestate"]; ts != "" {
			carrier["tracestate"] = ts
		}
		ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			m.sc = sc
		}
	}
	switch {
	case tags["isowatch_name"] != "":
		m.name, m.nameRank = tags["isowatch_name"], rankExplicit
	case tags["route"] != "":
		m.name, m.nameRank = tags["route"], rankRoute
	case tags["controller"] != "" && tags["action"] != "":
		m.name, m.nameRank = tags["controller"]+"#"+tags["action"], rankControllerAction
	case tags["controller"] != "":
		m.name, m.nameRank = tags["controller"], rankController
	case tags["action"] != "":
		m.name, m.nameRank = tags["action"], rankAction
	}
	if !m.sc.IsValid() && m.name == "" {
		return nil
	}
	return m
}

// parseTags splits "a='x',b='y'" (or a=x, a:x) into a map. Commas inside
// quotes do not split.
func parseTags(s string) map[string]string {
	out := map[string]string{}
	var parts []string
	inQuote, start := false, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++ // skip the escaped character
		case '\'':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	for _, p := range parts {
		p = strings.TrimSpace(p)
		sep := strings.IndexAny(p, "=:")
		if sep <= 0 {
			continue
		}
		k := strings.TrimSpace(p[:sep])
		v := strings.TrimSpace(p[sep+1:])
		if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
			v = strings.ReplaceAll(v[1:len(v)-1], `\'`, `'`)
		}
		if u, err := url.PathUnescape(k); err == nil {
			k = u
		}
		if u, err := url.PathUnescape(v); err == nil {
			v = u
		}
		if k != "" {
			out[strings.ToLower(k)] = v
		}
	}
	return out
}
