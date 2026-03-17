// Package parser implements a token-based LogQL parser for the subset of LogQL
// that Grafana Logs Drilldown emits.
//
// Supported constructs:
//   - Stream selectors: {label="val", other!="x", re=~"pat.*", nre!~"bad.*"}
//   - Empty selector: {}
//   - Line filters:  |= "text"  |~ "re"  != "text"  !~ "re"
//   - JSON parser stage: | json
//   - Logfmt parser stage: | logfmt
//   - Metric wrappers: count_over_time({...}[5m])  rate({...}[5m])
//
// Unsupported constructs (line_format, label_format, topk, etc.) return
// *UnsupportedError so HTTP handlers can return HTTP 400 with a clear message.
package parser

import (
	"fmt"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Query interface
// ────────────────────────────────────────────────────────────────────────────

// Query is the top-level result of a Parse call.
type Query interface {
	queryNode()
}

// LogQuery is a stream selector with an optional filter/parser pipeline.
type LogQuery struct {
	Selector StreamSelector
	Pipeline []PipelineStage
}

func (*LogQuery) queryNode() {}

// MetricQuery wraps a LogQuery with a metric aggregation function and a range.
type MetricQuery struct {
	Function MetricFunction
	Inner    LogQuery
	Range    time.Duration
}

func (*MetricQuery) queryNode() {}

// MetricFunction identifies which metric aggregation is applied.
type MetricFunction int

const (
	CountOverTime MetricFunction = iota
	Rate
)

// AggregationQuery wraps a MetricQuery with a vector aggregation operator such
// as sum, count, avg, min, or max. Grafana Logs Drilldown uses this form to
// produce per-label log-volume bar charts, e.g.:
//
//	sum by (detected_level) (count_over_time({app="api"}[2s]))
type AggregationQuery struct {
	Function AggregationFunction
	By       []string    // labels in the "by (...)" clause; nil means aggregate all into one series
	Inner    MetricQuery // the wrapped count_over_time / rate query
}

func (*AggregationQuery) queryNode() {}

// AggregationFunction identifies the vector aggregation operator.
type AggregationFunction int

const (
	AggSum   AggregationFunction = iota
	AggCount                     // treated the same as AggSum for integer counts
	AggAvg
	AggMin
	AggMax
)

// ────────────────────────────────────────────────────────────────────────────
// Stream selector
// ────────────────────────────────────────────────────────────────────────────

// StreamSelector is the {label="value", ...} part of a LogQL query.
type StreamSelector struct {
	Matchers []LabelMatcher
}

// MatchType identifies the operator in a label matcher.
type MatchType int

const (
	Eq  MatchType = iota // =
	Neq                  // !=
	Re                   // =~
	Nre                  // !~
)

// LabelMatcher is a single label filter inside {}.
type LabelMatcher struct {
	Name  string
	Type  MatchType
	Value string
}

// ────────────────────────────────────────────────────────────────────────────
// Pipeline stages
// ────────────────────────────────────────────────────────────────────────────

// PipelineStage is a single step in the log pipeline (after the selector).
type PipelineStage interface {
	stageNode()
}

// FilterOp identifies the operator in a line filter.
type FilterOp int

const (
	Contains    FilterOp = iota // |= "text"
	NotContains                 // != "text"
	Regex                       // |~ "re"
	NotRegex                    // !~ "re"
)

// LineFilter filters log lines by content or regex.
type LineFilter struct {
	Op    FilterOp
	Value string
}

func (*LineFilter) stageNode() {}

// LabelFilter is a pipeline stage that filters log lines by the value of a
// specific label field.  Grafana Logs Drilldown emits these to exclude empty
// values from aggregation series, e.g.:
//
//	| labels.app.kubernetes.io/name!=""
//
// LogQL label filter operators mirror the stream-selector operators (=, !=,
// =~, !~) and are represented with the same MatchType constants.
type LabelFilter struct {
	Name  string
	Type  MatchType
	Value string
}

func (*LabelFilter) stageNode() {}

// JSONParser represents the `| json` pipeline stage.
// VictoriaLogs auto-parses JSON fields, so this is conveyed as a hint rather
// than an explicit query clause.
type JSONParser struct{}

func (*JSONParser) stageNode() {}

// LogfmtParser represents the `| logfmt` pipeline stage.
// Translated to `| unpack_logfmt` in LogsQL.
type LogfmtParser struct{}

func (*LogfmtParser) stageNode() {}

// ────────────────────────────────────────────────────────────────────────────
// Error types
// ────────────────────────────────────────────────────────────────────────────

// ParseError is returned for syntactically invalid LogQL input.
// Pos is the byte offset of the offending character in the original query.
type ParseError struct {
	Pos int
	Msg string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at position %d: %s", e.Pos, e.Msg)
}

// UnsupportedError is returned for valid LogQL constructs that this proxy
// does not translate. HTTP handlers must convert this to a 400 response.
type UnsupportedError struct {
	Pos       int
	Construct string
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("unsupported LogQL construct %q at position %d", e.Construct, e.Pos)
}
