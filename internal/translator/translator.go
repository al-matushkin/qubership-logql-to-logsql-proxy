// Package translator converts a parsed LogQL AST (from package parser) into
// a LogsQL query string understood by VictoriaLogs.
//
// Translation rules (LogQL → LogsQL):
//
//	{app="api"}          → app:="api"          (exact field match)
//	{app!="api"}         → NOT app:="api"
//	{app=~"api.*"}       → app:~"api.*"         (RE2 regex)
//	{app!~"bot.*"}       → NOT app:~"bot.*"
//	{}                   → *                    (match-all)
//	|= "error"           → _msg:"error"         (substring search)
//	!= "error"           → NOT _msg:"error"
//	|~ "err.*"           → _msg:~"err.*"        (regex)
//	!~ "err.*"           → NOT _msg:~"err.*"
//	| json               → (hint only; not added to the query string)
//	| logfmt             → | unpack_logfmt
//
// Multiple terms are joined with AND:
//
//	{app="api", level!="debug"} |= "error"
//	→ app:="api" AND NOT level:="debug" AND _msg:"error"
//
// Metric queries (count_over_time / rate) are signalled via Result.IsMetric;
// the caller is responsible for routing them to the /select/logsql/hits
// endpoint instead of /select/logsql/query.
package translator

import (
	"fmt"
	"strings"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
)

// ────────────────────────────────────────────────────────────────────────────
// Public types
// ────────────────────────────────────────────────────────────────────────────

// Result is the output of a successful translation.
type Result struct {
	// LogsQL is the translated filter string for VictoriaLogs.
	// For log queries it goes to POST /select/logsql/query.
	// For metric queries it goes to POST /select/logsql/hits.
	LogsQL string

	// HasJSONParser is true when the LogQL pipeline contained "| json".
	// VictoriaLogs auto-parses JSON fields, so no extra clause is added to the
	// query string; this flag lets the VL client pass a relevant hint.
	HasJSONParser bool

	// IsMetric is true for count_over_time / rate queries (including those
	// wrapped in a vector aggregation like sum by (...)).
	IsMetric bool

	// MetricFunc identifies the aggregation (CountOverTime or Rate).
	// Only meaningful when IsMetric is true.
	MetricFunc parser.MetricFunction

	// MetricRange is the range window requested by the metric query (e.g. 5m).
	// Only meaningful when IsMetric is true.
	MetricRange time.Duration

	// IsAggregation is true when the query is wrapped in a vector aggregation
	// operator (sum, count, avg, min, max). The handler streams records and
	// groups them in memory instead of using the /hits endpoint.
	IsAggregation bool

	// AggregateBy is the list of label names from the "by (...)" clause.
	// An empty slice means aggregate across all series into one (no grouping).
	// Only meaningful when IsAggregation is true.
	AggregateBy []string
}

// Options controls optional translation behaviour.
type Options struct {
	// LabelRemap maps LogQL label names to their VictoriaLogs equivalents.
	// Applied to stream-selector matchers, label-filter stages, and the
	// "by (...)" grouping clause before emitting the LogsQL string.
	// A nil map disables all remapping.
	LabelRemap map[string]string
}

// TranslationError is returned when an AST node cannot be expressed in LogsQL.
type TranslationError struct {
	Msg string
}

func (e *TranslationError) Error() string {
	return "translation error: " + e.Msg
}

// ────────────────────────────────────────────────────────────────────────────
// Public API
// ────────────────────────────────────────────────────────────────────────────

// Translate converts a parsed LogQL AST into a Result containing the LogsQL
// query string and any metadata needed by the VL client.
//
// Returns *TranslationError if the AST contains a node with no LogsQL mapping.
func Translate(q parser.Query, opts Options) (Result, error) {
	switch v := q.(type) {
	case *parser.LogQuery:
		logsql, hasJSON, err := translateLogQuery(v, opts)
		if err != nil {
			return Result{}, err
		}
		return Result{LogsQL: logsql, HasJSONParser: hasJSON}, nil

	case *parser.MetricQuery:
		logsql, hasJSON, err := translateLogQuery(&v.Inner, opts)
		if err != nil {
			return Result{}, err
		}
		return Result{
			LogsQL:        logsql,
			HasJSONParser: hasJSON,
			IsMetric:      true,
			MetricFunc:    v.Function,
			MetricRange:   v.Range,
		}, nil

	case *parser.AggregationQuery:
		logsql, hasJSON, err := translateLogQuery(&v.Inner.Inner, opts)
		if err != nil {
			return Result{}, err
		}
		return Result{
			LogsQL:        logsql,
			HasJSONParser: hasJSON,
			IsMetric:      true,
			IsAggregation: true,
			MetricFunc:    v.Inner.Function,
			MetricRange:   v.Inner.Range,
			AggregateBy:   remapNames(v.By, opts.LabelRemap),
		}, nil

	default:
		return Result{}, &TranslationError{
			Msg: fmt.Sprintf("unknown query type %T", q),
		}
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Internal translation helpers
// ────────────────────────────────────────────────────────────────────────────

func translateLogQuery(lq *parser.LogQuery, opts Options) (logsql string, hasJSON bool, err error) {
	var terms []string
	var pipes []string

	for _, m := range lq.Selector.Matchers {
		t, terr := translateMatcher(m, opts)
		if terr != nil {
			return "", false, terr
		}
		terms = append(terms, t)
	}

	for _, stage := range lq.Pipeline {
		switch s := stage.(type) {
		case *parser.LineFilter:
			terms = append(terms, translateLineFilter(s))
		case *parser.LabelFilter:
			t, terr := translateLabelFilter(s, opts)
			if terr != nil {
				return "", false, terr
			}
			if t != "" {
				terms = append(terms, t)
			}
		case *parser.JSONParser:
			hasJSON = true // not added to the query string; VL auto-parses JSON
		case *parser.LogfmtParser:
			pipes = append(pipes, "unpack_logfmt")
		default:
			return "", false, &TranslationError{
				Msg: fmt.Sprintf("unknown pipeline stage type %T", stage),
			}
		}
	}

	filter := "*"
	if len(terms) > 0 {
		filter = strings.Join(terms, " AND ")
	}
	if len(pipes) == 0 {
		return filter, hasJSON, nil
	}
	return filter + " | " + strings.Join(pipes, " | "), hasJSON, nil
}

func translateMatcher(m parser.LabelMatcher, opts Options) (string, error) {
	f := quoteLabelName(remapName(m.Name, opts.LabelRemap))
	switch m.Type {
	case parser.Eq:
		return fmt.Sprintf(`%s:="%s"`, f, escapeLit(m.Value)), nil
	case parser.Neq:
		return fmt.Sprintf(`NOT %s:="%s"`, f, escapeLit(m.Value)), nil
	case parser.Re:
		return fmt.Sprintf(`%s:~"%s"`, f, escapeRe(m.Value)), nil
	case parser.Nre:
		return fmt.Sprintf(`NOT %s:~"%s"`, f, escapeRe(m.Value)), nil
	default:
		return "", &TranslationError{Msg: fmt.Sprintf("unknown match type %d", m.Type)}
	}
}

// vlInternalFields lists VictoriaLogs field names that use non-standard filter
// syntax and therefore cannot be expressed with the :=/:~ operators. Label
// filter stages that target these fields are silently dropped.
//
//   - _stream / _stream_id: Loki stream-selector representation; VictoriaLogs
//     uses {label="value"} syntax for _stream, not :=. The filter
//     "| _stream!=""" that Grafana Drilldown appends is always true anyway.
var vlInternalFields = map[string]bool{
	"_stream":    true,
	"_stream_id": true,
}

func translateLabelFilter(f *parser.LabelFilter, opts Options) (string, error) {
	name := remapName(f.Name, opts.LabelRemap)
	if vlInternalFields[name] {
		return "", nil // drop — these fields use non-standard VL syntax
	}
	field := quoteLabelName(name)
	switch f.Type {
	case parser.Eq:
		return fmt.Sprintf(`%s:="%s"`, field, escapeLit(f.Value)), nil
	case parser.Neq:
		return fmt.Sprintf(`NOT %s:="%s"`, field, escapeLit(f.Value)), nil
	case parser.Re:
		return fmt.Sprintf(`%s:~"%s"`, field, escapeRe(f.Value)), nil
	case parser.Nre:
		return fmt.Sprintf(`NOT %s:~"%s"`, field, escapeRe(f.Value)), nil
	default:
		return "", &TranslationError{Msg: fmt.Sprintf("unknown match type %d", f.Type)}
	}
}

func translateLineFilter(f *parser.LineFilter) string {
	switch f.Op {
	case parser.Contains:
		// LogsQL uses _msg:"text" for substring/word search (no := needed)
		return fmt.Sprintf(`_msg:"%s"`, escapeLit(f.Value))
	case parser.NotContains:
		return fmt.Sprintf(`NOT _msg:"%s"`, escapeLit(f.Value))
	case parser.Regex:
		return fmt.Sprintf(`_msg:~"%s"`, escapeRe(f.Value))
	case parser.NotRegex:
		return fmt.Sprintf(`NOT _msg:~"%s"`, escapeRe(f.Value))
	default:
		// Defensive fallback; the parser never produces an unknown FilterOp.
		return fmt.Sprintf(`_msg:"%s"`, escapeLit(f.Value))
	}
}

// ────────────────────────────────────────────────────────────────────────────
// String escaping and field-name quoting
// ────────────────────────────────────────────────────────────────────────────

// remapName returns the VictoriaLogs equivalent of a LogQL label name by
// looking it up in the provided map. Returns name unchanged when the map is
// nil or contains no entry for name.
func remapName(name string, remap map[string]string) string {
	if mapped, ok := remap[name]; ok {
		return mapped
	}
	return name
}

// remapNames applies remapName to each element of a slice, returning a new
// slice. Returns the original slice unchanged when remap is nil or empty.
func remapNames(names []string, remap map[string]string) []string {
	if len(remap) == 0 || len(names) == 0 {
		return names
	}
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = remapName(n, remap)
	}
	return out
}

// quoteLabelName returns a LogsQL-safe representation of a field name.
// Simple names consisting only of ASCII letters, digits, and underscores are
// returned unchanged.  Any name that contains dots, slashes, hyphens, or
// other special characters (e.g. Kubernetes labels like
// "labels.app.kubernetes.io/name") is wrapped in double quotes as required by
// the LogsQL filter syntax documented at
// https://docs.victoriametrics.com/victorialogs/logsql/
func quoteLabelName(name string) string {
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') || ch == '_') {
			return `"` + strings.ReplaceAll(name, `"`, `\"`) + `"`
		}
	}
	return name
}

// escapeLit escapes a literal (non-regex) value for a LogsQL double-quoted
// string. Backslashes are doubled first, then double-quotes are escaped.
func escapeLit(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

// escapeRe escapes a regex value for a LogsQL double-quoted string.
// Only double-quotes are escaped; backslashes retain their regex-engine
// meaning (e.g. \d, \s, \w are preserved unchanged).
func escapeRe(s string) string {
	return strings.ReplaceAll(s, `"`, `\"`)
}
