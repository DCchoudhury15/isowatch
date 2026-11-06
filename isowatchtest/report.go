package isowatchtest

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/DCchoudhury15/isowatch/checker"
)

// wrapWidth is where long sentences (summaries and fixes) are wrapped.
const wrapWidth = 96

// group is a set of anomalies with the same shape: the same kind, the same
// transaction names and the same SQL. A race that happens 19 times in a test
// is shown once, with a count.
type group struct {
	example checker.Anomaly // the shortest cycle in the group, easiest to read
	count   int
	rows    []string
}

// groupAnomalies groups anomalies by shape, in the order first seen.
func groupAnomalies(as []checker.Anomaly) []*group {
	byKey := map[string]*group{}
	var out []*group
	for _, a := range as {
		k := shapeKey(a)
		g := byKey[k]
		if g == nil {
			g = &group{example: a}
			byKey[k] = g
			out = append(out, g)
		}
		if len(a.Steps) < len(g.example.Steps) {
			g.example = a
		}
		g.count++
		for _, r := range rowsOf(a) {
			if !contains(g.rows, r) {
				g.rows = append(g.rows, r)
			}
		}
	}
	return out
}

// shapeKey is the kind plus the set of distinct (name, action, table, SQL)
// steps. It ignores transaction ids, versions, where the cycle starts and
// how long it is: 16 deposits racing on one row give lost update cycles of
// many lengths, but they are all the same bug.
func shapeKey(a checker.Anomaly) string {
	var parts []string
	for _, s := range a.Steps {
		if p := s.Name + "\x00" + s.Action + "\x00" + s.Table + "\x00" + s.SQL; !contains(parts, p) {
			parts = append(parts, p)
		}
	}
	sort.Strings(parts)
	return string(a.Kind) + "\x01" + strconv.FormatBool(a.Unexpected) + "\x01" + strings.Join(parts, "\x01")
}

func rowsOf(a checker.Anomaly) []string {
	var out []string
	for _, e := range a.Edges {
		r := fmt.Sprintf("%s(%s)", e.Version.Table, e.Version.Key)
		if !contains(out, r) {
			out = append(out, r)
		}
	}
	return out
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// kindIdent is how a test names each kind in Go code.
var kindIdent = map[checker.Kind]string{
	checker.LostUpdate: "isowatchtest.LostUpdate",
	checker.WriteSkew:  "isowatchtest.WriteSkew",
	checker.GSingle:    "isowatchtest.GSingle",
	checker.G2Item:     "isowatchtest.G2Item",
	checker.G1c:        "checker.G1c",
	checker.G0:         "checker.G0",
}

// report renders anomalies. headline has one %s for "N isolation anomalies
// (kinds)". failure adds what to do next.
func (m *Monitor) report(as []checker.Anomaly, headline string, failure bool) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf(headline, countPhrase(as)))
	b.WriteString("\nEach one is a race that really happened in this test: PostgreSQL returned the row versions that prove it.\n")

	groups := groupAnomalies(as)
	for i, g := range groups {
		b.WriteString("\n")
		writeGroup(&b, i+1, g)
	}

	if lost := m.LostWrites(); lost > 0 {
		fmt.Fprintf(&b, "\nIsoWatch counted %d lost write(s): changes that were overwritten by a transaction that never saw them.\n", lost)
	}
	b.WriteString("\n" + m.statsLine())
	if failure {
		b.WriteString("\n" + nextSteps(as))
	}
	return b.String()
}

func countPhrase(as []checker.Anomaly) string {
	byKind := map[checker.Kind]int{}
	var order []checker.Kind
	for _, a := range as {
		if byKind[a.Kind] == 0 {
			order = append(order, a.Kind)
		}
		byKind[a.Kind]++
	}
	parts := make([]string, len(order))
	for i, k := range order {
		parts[i] = fmt.Sprintf("%d %s", byKind[k], k)
	}
	noun := "isolation anomalies"
	if len(as) == 1 {
		noun = "an isolation anomaly"
		return fmt.Sprintf("%s (%s)", noun, strings.Join(parts, ", "))
	}
	return fmt.Sprintf("%d %s (%s)", len(as), noun, strings.Join(parts, ", "))
}

func writeGroup(b *strings.Builder, n int, g *group) {
	a := g.example
	times := "once"
	if g.count > 1 {
		times = fmt.Sprintf("%d times", g.count)
	}
	fmt.Fprintf(b, "%d) %s, %s, %s\n", n, a.Kind, times, namesPhrase(a))
	writeWrapped(b, "   ", a.Summary)
	fmt.Fprintf(b, "   Rows: %s\n", listCapped(g.rows, 5))
	b.WriteString("   What happened (one example):\n")

	who := make([]string, len(a.Steps))
	width := 0
	for i, s := range a.Steps {
		who[i] = stepWho(s)
		width = max(width, len(who[i]))
	}
	for i, s := range a.Steps {
		fmt.Fprintf(b, "     %-*s  %-8s  %s(%s) version %d\n", width, who[i], s.Action, s.Table, s.Key, s.Xmin)
		if s.SQL != "" {
			fmt.Fprintf(b, "       %s\n", s.SQL)
		}
	}
	writeWrapped(b, "   ", "Fix: "+a.Fix)
	if g.count > 1 {
		fmt.Fprintf(b, "   %d more like this one (same kind, transaction names and SQL).\n", g.count-1)
	}
}

// namesPhrase lists the names of every transaction in the steps. With
// GroupByTrace a node holds several transactions, and a.Names() has only
// the first of each.
func namesPhrase(a checker.Anomaly) string {
	names := a.Names()
	for _, s := range a.Steps {
		if s.Name != "" && !contains(names, s.Name) {
			names = append(names, s.Name)
		}
	}
	unnamed := 0
	for _, t := range a.Txns {
		if t.Name == "" {
			unnamed++
		}
	}
	switch {
	case len(names) == 0:
		return "between unnamed transactions (name them with capture.WithName to see which code ran them)"
	case unnamed > 0:
		return fmt.Sprintf("between transactions named %s and %d unnamed one(s)", quoteAll(names), unnamed)
	default:
		return "between transactions named " + quoteAll(names)
	}
}

func stepWho(s checker.Step) string {
	who := "txn " + short(s.Txn)
	if s.Name != "" {
		who = s.Name + " (" + who + ")"
	}
	if s.Trace != "" {
		who += " in trace " + short(s.Trace)
	}
	return who
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func quoteAll(xs []string) string {
	q := make([]string, len(xs))
	for i, x := range xs {
		q[i] = strconv.Quote(x)
	}
	return strings.Join(q, " and ")
}

func listCapped(xs []string, n int) string {
	if len(xs) <= n {
		return strings.Join(xs, ", ")
	}
	return strings.Join(xs[:n], ", ") + fmt.Sprintf(" and %d more", len(xs)-n)
}

// writeWrapped writes text wrapped at wrapWidth, each line indented.
func writeWrapped(b *strings.Builder, indent, text string) {
	line := indent
	for _, w := range strings.Fields(text) {
		if len(line) > len(indent) && len(line)+1+len(w) > wrapWidth {
			b.WriteString(line + "\n")
			line = indent
		}
		if len(line) > len(indent) {
			line += " "
		}
		line += w
	}
	if len(line) > len(indent) {
		b.WriteString(line + "\n")
	}
}

// nextSteps says how to fix or accept these races, using the real kinds,
// names and tables from the report.
func nextSteps(as []checker.Anomaly) string {
	var kinds, names, tables []string
	for _, a := range as {
		if id := kindIdent[a.Kind]; !contains(kinds, id) {
			kinds = append(kinds, id)
		}
		for _, n := range a.Names() { // the names IgnoreNames matches on
			if !contains(names, n) {
				names = append(names, n)
			}
		}
		for _, e := range a.Edges {
			t := e.Version.Table
			if i := strings.LastIndexByte(t, '.'); i >= 0 {
				t = t[i+1:]
			}
			if !contains(tables, t) {
				tables = append(tables, t)
			}
		}
	}
	calls := make([]string, len(kinds))
	for i, k := range kinds {
		calls[i] = "m.ExpectAnomaly(" + k + ")"
	}
	var b strings.Builder
	b.WriteString("\nNext: apply the fix above and run the test again.\n")
	b.WriteString("If this test is meant to show the race, call " + strings.Join(calls, " and ") + ".\n")
	b.WriteString("If the race is acceptable, pass ")
	if len(names) > 0 {
		b.WriteString("isowatchtest.IgnoreNames(" + strings.Join(quoted(names), ", ") + ") or ")
	}
	b.WriteString("isowatchtest.IgnoreTables(" + strings.Join(quoted(tables), ", ") + ") to isowatchtest.New.")
	return b.String()
}

func quoted(xs []string) []string {
	q := make([]string, len(xs))
	for i, x := range xs {
		q[i] = strconv.Quote(x)
	}
	return q
}
