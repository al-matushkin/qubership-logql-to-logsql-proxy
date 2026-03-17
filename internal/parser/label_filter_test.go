package parser_test

import (
	"testing"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
)

// TestDropStage validates that "| drop field1, field2" and
// "| keep field1, field2" pipeline stages are silently ignored, so that
// queries like
//
//	{namespace="ingress-nginx"} | json | logfmt | drop __error__, __error_details__
//
// no longer return HTTP 400.
func TestDropStage(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantLogsQL string
	}{
		{
			name:       "log query with drop",
			query:      `{namespace="ingress-nginx"} | json | logfmt | drop __error__, __error_details__`,
			wantLogsQL: `namespace:="ingress-nginx" | unpack_logfmt`,
		},
		{
			name:       "sum without by, drop stage",
			query:      `sum(count_over_time({namespace="ingress-nginx"} | json | logfmt | drop __error__, __error_details__ [900s]))`,
			wantLogsQL: `namespace:="ingress-nginx" | unpack_logfmt`,
		},
		{
			name:       "keep stage",
			query:      `{app="api"} | keep message, level`,
			wantLogsQL: `app:="api"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := parser.Parse(tc.query)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			res, err := translator.Translate(ast, translator.Options{})
			if err != nil {
				t.Fatalf("Translate error: %v", err)
			}
			if res.LogsQL != tc.wantLogsQL {
				t.Errorf("LogsQL mismatch\n  got:  %q\n  want: %q", res.LogsQL, tc.wantLogsQL)
			}
		})
	}
}

// TestStreamLabelFilterDropped validates that "| _stream!=""" (and the related
// _stream_id variant) are silently dropped during translation. VictoriaLogs
// uses special {} syntax for the _stream field and rejects the := operator,
// so passing the filter through would cause a VL 400 error.
func TestStreamLabelFilterDropped(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantLogsQL string
	}{
		{
			name:       "_stream neq empty",
			query:      `{namespace="alty1224-consul-service"} | _stream!=""`,
			wantLogsQL: `namespace:="alty1224-consul-service"`,
		},
		{
			name:       "_stream eq value",
			query:      `{app="api"} | _stream="something"`,
			wantLogsQL: `app:="api"`,
		},
		{
			name:       "_stream_id neq empty",
			query:      `{app="api"} | _stream_id!=""`,
			wantLogsQL: `app:="api"`,
		},
		{
			name:       "other label filter still works",
			query:      `{namespace="ns"} | level!=""`,
			wantLogsQL: `namespace:="ns" AND NOT level:=""`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := parser.Parse(tc.query)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			res, err := translator.Translate(ast, translator.Options{})
			if err != nil {
				t.Fatalf("Translate error: %v", err)
			}
			if res.LogsQL != tc.wantLogsQL {
				t.Errorf("LogsQL mismatch\n  got:  %q\n  want: %q", res.LogsQL, tc.wantLogsQL)
			}
		})
	}
}

// TestPostfixByClauseAndLabelRemap validates:
//  1. The postfix "by" form — sum(expr) by (label) — is parsed correctly.
//  2. "detected_level" is remapped to "level" when a LabelRemap is provided.
func TestPostfixByClauseAndLabelRemap(t *testing.T) {
	query := `sum(count_over_time({namespace="ingress-nginx"}       [2s])) by (detected_level)`
	opts := translator.Options{
		LabelRemap: map[string]string{"detected_level": "level"},
	}

	ast, err := parser.Parse(query)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	res, err := translator.Translate(ast, opts)
	if err != nil {
		t.Fatalf("Translate error: %v", err)
	}
	if !res.IsAggregation {
		t.Fatalf("expected IsAggregation=true")
	}
	if want := `namespace:="ingress-nginx"`; res.LogsQL != want {
		t.Errorf("LogsQL: got %q, want %q", res.LogsQL, want)
	}
	if len(res.AggregateBy) != 1 || res.AggregateBy[0] != "level" {
		t.Errorf("AggregateBy: got %v, want [level]", res.AggregateBy)
	}
}

// TestLabelFilterKubernetesLabels validates that the two queries that were
// returning HTTP 400 from Grafana Logs Drilldown now parse and translate
// successfully.
func TestLabelFilterKubernetesLabels(t *testing.T) {
	cases := []struct {
		query       string
		wantLogsQL  string
		wantAggBy   string
	}{
		{
			query: `sum by (labels.app.kubernetes.io/name) (count_over_time({namespace="ingress-nginx"} | labels.app.kubernetes.io/name!=""       [2s]))`,
			wantLogsQL: `namespace:="ingress-nginx" AND NOT "labels.app.kubernetes.io/name":=""`,
			wantAggBy:  "labels.app.kubernetes.io/name",
		},
		{
			query: `sum by (labels.app.kubernetes.io/part-of) (count_over_time({namespace="ingress-nginx"} | labels.app.kubernetes.io/part-of!=""       [2s]))`,
			wantLogsQL: `namespace:="ingress-nginx" AND NOT "labels.app.kubernetes.io/part-of":=""`,
			wantAggBy:  "labels.app.kubernetes.io/part-of",
		},
	}

	for _, tc := range cases {
		ast, err := parser.Parse(tc.query)
		if err != nil {
			t.Fatalf("Parse(%q) error: %v", tc.query, err)
		}

		res, err := translator.Translate(ast, translator.Options{})
		if err != nil {
			t.Fatalf("Translate(%q) error: %v", tc.query, err)
		}

		if res.LogsQL != tc.wantLogsQL {
			t.Errorf("LogsQL mismatch\n  got:  %q\n  want: %q", res.LogsQL, tc.wantLogsQL)
		}
		if !res.IsAggregation {
			t.Errorf("expected IsAggregation=true")
		}
		if len(res.AggregateBy) != 1 || res.AggregateBy[0] != tc.wantAggBy {
			t.Errorf("AggregateBy mismatch: got %v, want [%q]", res.AggregateBy, tc.wantAggBy)
		}
	}
}
