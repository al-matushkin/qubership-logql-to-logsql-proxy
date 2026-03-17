package parser_test

import (
	"errors"
	"testing"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
)

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func mustParse(t *testing.T, query string) parser.Query {
	t.Helper()
	q, err := parser.Parse(query)
	if err != nil {
		t.Fatalf("Parse(%q) unexpected error: %v", query, err)
	}
	return q
}

func asLogQuery(t *testing.T, q parser.Query) *parser.LogQuery {
	t.Helper()
	lq, ok := q.(*parser.LogQuery)
	if !ok {
		t.Fatalf("expected *LogQuery, got %T", q)
	}
	return lq
}

func asMetricQuery(t *testing.T, q parser.Query) *parser.MetricQuery {
	t.Helper()
	mq, ok := q.(*parser.MetricQuery)
	if !ok {
		t.Fatalf("expected *MetricQuery, got %T", q)
	}
	return mq
}

// ────────────────────────────────────────────────────────────────────────────
// Success cases
// ────────────────────────────────────────────────────────────────────────────

func TestParseEmptySelector(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{}`))
	if len(lq.Selector.Matchers) != 0 {
		t.Errorf("expected 0 matchers, got %d", len(lq.Selector.Matchers))
	}
	if len(lq.Pipeline) != 0 {
		t.Errorf("expected empty pipeline, got %d stages", len(lq.Pipeline))
	}
}

func TestParseSimpleSelector(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="api"}`))
	if len(lq.Selector.Matchers) != 1 {
		t.Fatalf("expected 1 matcher, got %d", len(lq.Selector.Matchers))
	}
	m := lq.Selector.Matchers[0]
	if m.Name != "app" || m.Type != parser.Eq || m.Value != "api" {
		t.Errorf("got matcher %+v, want {Name:app Type:Eq Value:api}", m)
	}
}

func TestParseMultipleMatchers(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="api", level!="debug"}`))
	matchers := lq.Selector.Matchers
	if len(matchers) != 2 {
		t.Fatalf("expected 2 matchers, got %d", len(matchers))
	}
	if matchers[0].Name != "app" || matchers[0].Type != parser.Eq || matchers[0].Value != "api" {
		t.Errorf("matcher[0] = %+v", matchers[0])
	}
	if matchers[1].Name != "level" || matchers[1].Type != parser.Neq || matchers[1].Value != "debug" {
		t.Errorf("matcher[1] = %+v", matchers[1])
	}
}

func TestParseRegexMatchers(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app=~"api.*", ns!~"test.*"}`))
	matchers := lq.Selector.Matchers
	if len(matchers) != 2 {
		t.Fatalf("expected 2 matchers, got %d", len(matchers))
	}
	if matchers[0].Type != parser.Re || matchers[0].Value != "api.*" {
		t.Errorf("matcher[0] = %+v", matchers[0])
	}
	if matchers[1].Type != parser.Nre || matchers[1].Value != "test.*" {
		t.Errorf("matcher[1] = %+v", matchers[1])
	}
}

func TestParseWithLineFilter(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="api"} |= "error"`))
	if len(lq.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(lq.Pipeline))
	}
	lf, ok := lq.Pipeline[0].(*parser.LineFilter)
	if !ok {
		t.Fatalf("expected *LineFilter, got %T", lq.Pipeline[0])
	}
	if lf.Op != parser.Contains || lf.Value != "error" {
		t.Errorf("got LineFilter %+v, want {Op:Contains Value:error}", lf)
	}
}

// TestParseWithNotFilter covers the pipeline `!= "text"` syntax (not-contains).
func TestParseWithNotFilter(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="api"} != "debug"`))
	if len(lq.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(lq.Pipeline))
	}
	lf, ok := lq.Pipeline[0].(*parser.LineFilter)
	if !ok {
		t.Fatalf("expected *LineFilter, got %T", lq.Pipeline[0])
	}
	if lf.Op != parser.NotContains || lf.Value != "debug" {
		t.Errorf("got LineFilter %+v, want {Op:NotContains Value:debug}", lf)
	}
}

func TestParseWithRegexFilter(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="api"} |~ "err.*"`))
	lf, ok := lq.Pipeline[0].(*parser.LineFilter)
	if !ok {
		t.Fatalf("expected *LineFilter, got %T", lq.Pipeline[0])
	}
	if lf.Op != parser.Regex || lf.Value != "err.*" {
		t.Errorf("got LineFilter %+v, want {Op:Regex Value:err.*}", lf)
	}
}

func TestParseWithNotRegexFilter(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="api"} !~ "err.*"`))
	lf, ok := lq.Pipeline[0].(*parser.LineFilter)
	if !ok {
		t.Fatalf("expected *LineFilter, got %T", lq.Pipeline[0])
	}
	if lf.Op != parser.NotRegex || lf.Value != "err.*" {
		t.Errorf("got LineFilter %+v, want {Op:NotRegex Value:err.*}", lf)
	}
}

func TestParseJSONStage(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="api"} | json`))
	if len(lq.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(lq.Pipeline))
	}
	if _, ok := lq.Pipeline[0].(*parser.JSONParser); !ok {
		t.Errorf("expected *JSONParser, got %T", lq.Pipeline[0])
	}
}

func TestParseLogfmtStage(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="api"} | logfmt`))
	if len(lq.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(lq.Pipeline))
	}
	if _, ok := lq.Pipeline[0].(*parser.LogfmtParser); !ok {
		t.Errorf("expected *LogfmtParser, got %T", lq.Pipeline[0])
	}
}

func TestParseCountOverTime(t *testing.T) {
	mq := asMetricQuery(t, mustParse(t, `count_over_time({app="api"}[5m])`))
	if mq.Function != parser.CountOverTime {
		t.Errorf("expected CountOverTime, got %d", mq.Function)
	}
	if mq.Range != 5*time.Minute {
		t.Errorf("expected 5m range, got %v", mq.Range)
	}
	if len(mq.Inner.Selector.Matchers) != 1 || mq.Inner.Selector.Matchers[0].Value != "api" {
		t.Errorf("unexpected inner query: %+v", mq.Inner)
	}
}

func TestParseRate(t *testing.T) {
	mq := asMetricQuery(t, mustParse(t, `rate({app="api"}[1h])`))
	if mq.Function != parser.Rate {
		t.Errorf("expected Rate, got %d", mq.Function)
	}
	if mq.Range != time.Hour {
		t.Errorf("expected 1h range, got %v", mq.Range)
	}
}

func TestParseBacktickStringLabel(t *testing.T) {
	// Grafana Logs Drilldown sends backtick-quoted strings, e.g.
	// {service_name=~`.+`}
	lq := asLogQuery(t, mustParse(t, "{service_name=~`.+`}"))
	if len(lq.Selector.Matchers) != 1 {
		t.Fatalf("expected 1 matcher, got %d", len(lq.Selector.Matchers))
	}
	m := lq.Selector.Matchers[0]
	if m.Name != "service_name" || m.Type != parser.Re || m.Value != `.+` {
		t.Errorf("got matcher %+v, want {Name:service_name Type:Re Value:.+}", m)
	}
}

func TestParseBacktickStringMixed(t *testing.T) {
	// Backtick and double-quote strings are interchangeable per LogQL spec.
	lq := asLogQuery(t, mustParse(t, "{app=`api`, level!=\"debug\"}"))
	matchers := lq.Selector.Matchers
	if len(matchers) != 2 {
		t.Fatalf("expected 2 matchers, got %d", len(matchers))
	}
	if matchers[0].Value != "api" {
		t.Errorf("matcher[0].Value = %q, want api", matchers[0].Value)
	}
	if matchers[1].Value != "debug" {
		t.Errorf("matcher[1].Value = %q, want debug", matchers[1].Value)
	}
}

func TestParseBacktickLineFilter(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, "{app=`api`} |= `error`"))
	lf, ok := lq.Pipeline[0].(*parser.LineFilter)
	if !ok {
		t.Fatalf("expected *LineFilter, got %T", lq.Pipeline[0])
	}
	if lf.Op != parser.Contains || lf.Value != "error" {
		t.Errorf("got LineFilter %+v, want {Op:Contains Value:error}", lf)
	}
}

func TestParseLabelWithEscapedQuote(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="my \"app\""}`))
	if len(lq.Selector.Matchers) != 1 {
		t.Fatalf("expected 1 matcher, got %d", len(lq.Selector.Matchers))
	}
	if got := lq.Selector.Matchers[0].Value; got != `my "app"` {
		t.Errorf("expected value %q, got %q", `my "app"`, got)
	}
}

func TestParseMultiplePipelineStages(t *testing.T) {
	lq := asLogQuery(t, mustParse(t, `{app="api"} |= "error" | json`))
	if len(lq.Pipeline) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(lq.Pipeline))
	}
	if _, ok := lq.Pipeline[0].(*parser.LineFilter); !ok {
		t.Errorf("stage[0]: expected *LineFilter, got %T", lq.Pipeline[0])
	}
	if _, ok := lq.Pipeline[1].(*parser.JSONParser); !ok {
		t.Errorf("stage[1]: expected *JSONParser, got %T", lq.Pipeline[1])
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Aggregation queries
// ────────────────────────────────────────────────────────────────────────────

func TestParseAggregationSumBy(t *testing.T) {
	// The exact form Grafana Logs Drilldown sends for log-volume bar charts.
	q := mustParse(t, "sum by (detected_level) (count_over_time({app=`api`}[2s]))")
	aq, ok := q.(*parser.AggregationQuery)
	if !ok {
		t.Fatalf("expected *AggregationQuery, got %T", q)
	}
	if aq.Function != parser.AggSum {
		t.Errorf("function = %d, want AggSum", aq.Function)
	}
	if len(aq.By) != 1 || aq.By[0] != "detected_level" {
		t.Errorf("by = %v, want [detected_level]", aq.By)
	}
	if aq.Inner.Function != parser.CountOverTime {
		t.Errorf("inner function = %d, want CountOverTime", aq.Inner.Function)
	}
	if aq.Inner.Range != 2*time.Second {
		t.Errorf("inner range = %v, want 2s", aq.Inner.Range)
	}
	if len(aq.Inner.Inner.Selector.Matchers) != 1 {
		t.Errorf("inner selector matchers = %d, want 1", len(aq.Inner.Inner.Selector.Matchers))
	}
}

func TestParseAggregationSumNoBy(t *testing.T) {
	// sum without "by" clause produces a single series.
	q := mustParse(t, `sum(count_over_time({app="api"}[5m]))`)
	aq, ok := q.(*parser.AggregationQuery)
	if !ok {
		t.Fatalf("expected *AggregationQuery, got %T", q)
	}
	if aq.Function != parser.AggSum {
		t.Errorf("function = %d, want AggSum", aq.Function)
	}
	if len(aq.By) != 0 {
		t.Errorf("by = %v, want empty", aq.By)
	}
}

func TestParseAggregationMultipleByLabels(t *testing.T) {
	q := mustParse(t, "count by (service, level) (rate({ns=`prod`}[1m]))")
	aq, ok := q.(*parser.AggregationQuery)
	if !ok {
		t.Fatalf("expected *AggregationQuery, got %T", q)
	}
	if aq.Function != parser.AggCount {
		t.Errorf("function = %d, want AggCount", aq.Function)
	}
	if len(aq.By) != 2 || aq.By[0] != "service" || aq.By[1] != "level" {
		t.Errorf("by = %v, want [service level]", aq.By)
	}
	if aq.Inner.Function != parser.Rate {
		t.Errorf("inner function = %d, want Rate", aq.Inner.Function)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Error cases
// ────────────────────────────────────────────────────────────────────────────

func TestParseErrorMissingBrace(t *testing.T) {
	_, err := parser.Parse(`{app="api"`)
	if err == nil {
		t.Fatal("expected error for missing closing brace, got nil")
	}
	var pe *parser.ParseError
	if !errors.As(err, &pe) {
		t.Errorf("expected *ParseError, got %T: %v", err, err)
	}
}

func TestParseUnsupportedConstruct(t *testing.T) {
	_, err := parser.Parse(`{app="api"} | line_format "{{.msg}}"`)
	if err == nil {
		t.Fatal("expected error for unsupported construct, got nil")
	}
	var ue *parser.UnsupportedError
	if !errors.As(err, &ue) {
		t.Errorf("expected *UnsupportedError, got %T: %v", err, err)
	}
	if ue.Construct != "| line_format" {
		t.Errorf("expected construct %q, got %q", "| line_format", ue.Construct)
	}
}

func TestParseErrorEmptyInput(t *testing.T) {
	_, err := parser.Parse(``)
	if err == nil {
		t.Fatal("expected error for empty input, got nil")
	}
}

func TestParseErrorInvalidOperator(t *testing.T) {
	_, err := parser.Parse(`{app>"api"}`)
	if err == nil {
		t.Fatal("expected error for invalid operator '>', got nil")
	}
}
