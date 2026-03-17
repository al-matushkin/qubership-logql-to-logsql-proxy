package handler_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/config"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/handler"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/limits"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// ────────────────────────────────────────────────────────────────────────────
// Mock VLogsClient
// ────────────────────────────────────────────────────────────────────────────

type mockVL struct {
	queryLogsErr  error
	queryLogsRecs []vlogs.Record
	queryHitsBkts []vlogs.HitBucket
	queryHitsErr  error
	fieldNames    []string
	fieldNamesErr error
	fieldValues   []string
	fieldValsErr  error
}

func (m *mockVL) QueryLogs(_ context.Context, _ vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
	if m.queryLogsErr != nil {
		return m.queryLogsErr
	}
	for _, r := range m.queryLogsRecs {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockVL) QueryHits(_ context.Context, _ vlogs.HitsQueryRequest) ([]vlogs.HitBucket, error) {
	return m.queryHitsBkts, m.queryHitsErr
}

func (m *mockVL) FieldNames(_ context.Context, _ vlogs.FieldNamesRequest) ([]string, error) {
	return m.fieldNames, m.fieldNamesErr
}

func (m *mockVL) FieldValues(_ context.Context, _ vlogs.FieldValuesRequest) ([]string, error) {
	return m.fieldValues, m.fieldValsErr
}

// ────────────────────────────────────────────────────────────────────────────
// Test helpers
// ────────────────────────────────────────────────────────────────────────────

func defaultConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Server.ListenAddr = ":3100"
	cfg.VLogs.URL = "http://localhost:9428"
	cfg.VLogs.Timeout = 30 * time.Second
	cfg.Limits.MaxConcurrentQueries = 50
	cfg.Limits.MaxQueueDepth = 100
	cfg.Limits.MaxResponseBodyBytes = 64 * 1024 * 1024
	cfg.Limits.MaxStreamsPerResponse = 5000
	cfg.Limits.MaxMemoryMB = 512
	cfg.Limits.MaxQueryRangeHours = 24
	cfg.Limits.MaxLimit = 5000
	cfg.Limits.DefaultLimit = 1000
	cfg.Labels.MetadataCacheTTL = 5 * time.Minute
	cfg.Labels.MetadataCacheSize = 256
	cfg.Log.Level = "info"
	cfg.Log.Format = "json"
	return cfg
}

func newDeps(vl vlogs.VLogsClient) *handler.Deps {
	cfg := defaultConfig()
	return &handler.Deps{
		Cfg:   cfg,
		VL:    vl,
		Lim:   limits.New(50, 100),
		Cache: vlogs.NewMetadataCache(256),
	}
}

// buildHandler constructs a fasthttp.RequestHandler with all routes registered,
// delegating to deps.BuildHandler() to mirror production routing exactly.
func buildHandler(deps *handler.Deps) fasthttp.RequestHandler {
	return deps.BuildHandler()
}

// newTestServer starts a real fasthttp server on an ephemeral localhost port and
// returns its base URL and a cleanup function. Tests use the standard http.Get /
// http.Client to send requests, exactly as in production.
func newTestServer(t *testing.T, h fasthttp.RequestHandler) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := &fasthttp.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	return "http://" + ln.Addr().String(), func() { _ = srv.Shutdown() }
}

// ────────────────────────────────────────────────────────────────────────────
// /ready
// ────────────────────────────────────────────────────────────────────────────

func TestReady(t *testing.T) {
	addr, cleanup := newTestServer(t, buildHandler(newDeps(&mockVL{})))
	defer cleanup()

	resp, err := http.Get(addr + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/query_range — log queries
// ────────────────────────────────────────────────────────────────────────────

func TestQueryRangeSuccess(t *testing.T) {
	vl := &mockVL{
		queryLogsRecs: []vlogs.Record{
			{"_time": "2024-01-15T12:00:00Z", "_msg": "log line 1", "app": "api"},
			{"_time": "2024-01-15T12:00:01Z", "_msg": "log line 2", "app": "api"},
		},
	}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr +
		"/loki/api/v1/query_range?query={app=\"api\"}&start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body loki.StreamsResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Status != "success" {
		t.Errorf("status = %q, want %q", body.Status, "success")
	}
	if body.Data.ResultType != "streams" {
		t.Errorf("resultType = %q, want %q", body.Data.ResultType, "streams")
	}
	if len(body.Data.Result) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(body.Data.Result))
	}
	if len(body.Data.Result[0].Values) != 2 {
		t.Errorf("expected 2 values, got %d", len(body.Data.Result[0].Values))
	}
}

func TestQueryRangeMissingQuery(t *testing.T) {
	addr, cleanup := newTestServer(t, buildHandler(newDeps(&mockVL{})))
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/query_range?start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestQueryRangeBadLogQL(t *testing.T) {
	addr, cleanup := newTestServer(t, buildHandler(newDeps(&mockVL{})))
	defer cleanup()

	resp, err := http.Get(addr +
		"/loki/api/v1/query_range?query=not-valid-logql&start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestQueryRangeUnsupportedConstruct(t *testing.T) {
	addr, cleanup := newTestServer(t, buildHandler(newDeps(&mockVL{})))
	defer cleanup()

	// line_format is explicitly unsupported.
	resp, err := http.Get(addr +
		`/loki/api/v1/query_range?query={app="api"}|line_format "{{.msg}}"&start=1705320000&end=1705323600`)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestQueryRangeNanosecondTimestamps(t *testing.T) {
	// Grafana Drilldown sends start/end as Unix nanosecond integers, e.g.
	// start=1772721369477000000&end=1772722269477000000 (15-minute window).
	// These must not be misinterpreted as seconds (which would make them
	// year ~56 billion and trip the MaxQueryRangeHours guard).
	vl := &mockVL{
		queryLogsRecs: []vlogs.Record{
			{"_time": "2026-03-05T14:00:00Z", "_msg": "ok", "app": "api"},
		},
	}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	// 15-minute window expressed in nanoseconds
	start := "1772721369477000000"
	end := "1772722269477000000"
	resp, err := http.Get(addr +
		"/loki/api/v1/query_range?query={app=`api`}&start=" + start + "&end=" + end)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("nanosecond timestamps: status = %d, want 200 (got misinterpreted as seconds?)", resp.StatusCode)
	}
}

func TestQueryRangeMillisecondTimestamps(t *testing.T) {
	// Some Grafana versions send start/end as Unix millisecond integers.
	vl := &mockVL{
		queryLogsRecs: []vlogs.Record{
			{"_time": "2024-01-15T12:00:00Z", "_msg": "ok", "app": "api"},
		},
	}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	// 1-hour window: 1705320000000–1705323600000 ms
	resp, err := http.Get(addr +
		"/loki/api/v1/query_range?query={app=`api`}&start=1705320000000&end=1705323600000")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("millisecond timestamps: status = %d, want 200", resp.StatusCode)
	}
}

func TestQueryRangeTimeRangeExceeded(t *testing.T) {
	deps := newDeps(&mockVL{})
	deps.Cfg.Limits.MaxQueryRangeHours = 1
	addr, cleanup := newTestServer(t, buildHandler(deps))
	defer cleanup()

	// 25-hour range — should be rejected.
	resp, err := http.Get(addr +
		"/loki/api/v1/query_range?query={}&start=1705320000&end=1705410000")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestQueryRangeVLError(t *testing.T) {
	vl := &mockVL{queryLogsErr: &testVLError{}}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr +
		"/loki/api/v1/query_range?query={}&start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
}

type testVLError struct{}

func (e *testVLError) Error() string { return "simulated VL failure" }

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/query_range — metric queries
// ────────────────────────────────────────────────────────────────────────────

func TestQueryRangeMetricCountOverTime(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	vl := &mockVL{
		queryHitsBkts: []vlogs.HitBucket{
			{Timestamp: now, Count: 42},
			{Timestamp: now.Add(time.Minute), Count: 58},
		},
	}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr +
		"/loki/api/v1/query_range?query=count_over_time({app=%22api%22}[5m])&start=1705320000&end=1705323600&step=60")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body loki.MatrixResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Data.ResultType != "matrix" {
		t.Errorf("resultType = %q, want %q", body.Data.ResultType, "matrix")
	}
	if len(body.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(body.Data.Result))
	}
	if len(body.Data.Result[0].Values) != 2 {
		t.Errorf("expected 2 values, got %d", len(body.Data.Result[0].Values))
	}
}

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/query_range — aggregation queries (sum by (...) (...))
// ────────────────────────────────────────────────────────────────────────────

func TestQueryRangeAggregationSumBy(t *testing.T) {
	// Three records: two "info" in bucket 0, one "error" in bucket 0,
	// one "info" in bucket 1 (2 s later).
	base := time.Unix(1705320000, 0).UTC()
	vl := &mockVL{
		queryLogsRecs: []vlogs.Record{
			{"_time": base.Format(time.RFC3339Nano), "_msg": "r1", "detected_level": "info"},
			{"_time": base.Format(time.RFC3339Nano), "_msg": "r2", "detected_level": "error"},
			{"_time": base.Add(2 * time.Second).Format(time.RFC3339Nano), "_msg": "r3", "detected_level": "info"},
		},
	}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr +
		`/loki/api/v1/query_range?query=sum+by+(detected_level)+(count_over_time({app=%22api%22}[2s]))&start=1705320000&end=1705323600&step=2`)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body loki.MatrixResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Data.ResultType != "matrix" {
		t.Errorf("resultType = %q, want matrix", body.Data.ResultType)
	}
	// Expect 2 series: one for "error", one for "info" (sorted by key).
	if len(body.Data.Result) != 2 {
		t.Fatalf("expected 2 series, got %d: %+v", len(body.Data.Result), body.Data.Result)
	}

	// Series are sorted by their series key (label map string). "error" < "info".
	errSeries := body.Data.Result[0]
	if errSeries.Metric["detected_level"] != "error" {
		t.Errorf("series[0] detected_level = %q, want error", errSeries.Metric["detected_level"])
	}
	if len(errSeries.Values) != 1 {
		t.Errorf("error series values = %d, want 1", len(errSeries.Values))
	}

	infoSeries := body.Data.Result[1]
	if infoSeries.Metric["detected_level"] != "info" {
		t.Errorf("series[1] detected_level = %q, want info", infoSeries.Metric["detected_level"])
	}
	// "info" appears in two different buckets.
	if len(infoSeries.Values) != 2 {
		t.Errorf("info series values = %d, want 2", len(infoSeries.Values))
	}
	// Count in each bucket is 1.
	if countStr, ok := infoSeries.Values[0][1].(string); !ok || countStr != "1" {
		t.Errorf("info bucket[0] count = %v, want \"1\"", infoSeries.Values[0][1])
	}
}

func TestQueryRangeAggregationSumNoBy(t *testing.T) {
	base := time.Unix(1705320000, 0).UTC()
	vl := &mockVL{
		queryLogsRecs: []vlogs.Record{
			{"_time": base.Format(time.RFC3339Nano), "_msg": "r1", "app": "api"},
			{"_time": base.Format(time.RFC3339Nano), "_msg": "r2", "app": "api"},
		},
	}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr +
		`/loki/api/v1/query_range?query=sum(count_over_time({app=%22api%22}[2s]))&start=1705320000&end=1705323600&step=2`)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body loki.MatrixResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// No "by" clause → single series with empty metric.
	if len(body.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(body.Data.Result))
	}
	if len(body.Data.Result[0].Metric) != 0 {
		t.Errorf("metric = %v, want empty (no grouping)", body.Data.Result[0].Metric)
	}
	if countStr, ok := body.Data.Result[0].Values[0][1].(string); !ok || countStr != "2" {
		t.Errorf("bucket count = %v, want \"2\"", body.Data.Result[0].Values[0][1])
	}
}

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/labels
// ────────────────────────────────────────────────────────────────────────────

func TestLabelsSuccess(t *testing.T) {
	vl := &mockVL{fieldNames: []string{"app", "level", "host"}}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/labels?start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body loki.LabelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Data) != 3 {
		t.Errorf("expected 3 labels, got %d: %v", len(body.Data), body.Data)
	}
}

func TestLabelsFromStaticConfig(t *testing.T) {
	deps := newDeps(&mockVL{})
	deps.Cfg.Labels.KnownLabels = []string{"app", "env"}
	addr, cleanup := newTestServer(t, buildHandler(deps))
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/labels")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var body loki.LabelsResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if len(body.Data) != 2 {
		t.Errorf("expected 2 known labels, got %d: %v", len(body.Data), body.Data)
	}
	if body.Data[0] != "app" || body.Data[1] != "env" {
		t.Errorf("known labels = %v, want [app env]", body.Data)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/detected_labels
// ────────────────────────────────────────────────────────────────────────────

func TestDetectedLabelsSuccess(t *testing.T) {
	vl := &mockVL{fieldNames: []string{"app", "level", "host"}}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/detected_labels?start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// detected_labels does NOT use the {"status","data"} envelope — it returns
	// DetectedLabelsData directly.
	var body loki.DetectedLabelsData
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.DetectedLabels) != 3 {
		t.Fatalf("detectedLabels count = %d, want 3", len(body.DetectedLabels))
	}
	if body.DetectedLabels[0].Label != "app" {
		t.Errorf("first label = %q, want app", body.DetectedLabels[0].Label)
	}
}

func TestDetectedLabelsFromStaticConfig(t *testing.T) {
	deps := newDeps(&mockVL{})
	deps.Cfg.Labels.KnownLabels = []string{"app", "env"}
	addr, cleanup := newTestServer(t, buildHandler(deps))
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/detected_labels")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var body loki.DetectedLabelsData
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.DetectedLabels) != 2 {
		t.Errorf("detectedLabels count = %d, want 2", len(body.DetectedLabels))
	}
	if body.DetectedLabels[0].Label != "app" || body.DetectedLabels[1].Label != "env" {
		t.Errorf("labels = %v, want [app env]", body.DetectedLabels)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/label/:name/values
// ────────────────────────────────────────────────────────────────────────────

func TestLabelValuesSuccess(t *testing.T) {
	vl := &mockVL{fieldValues: []string{"api", "worker", "nginx"}}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/label/app/values?start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body loki.LabelValuesResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Data) != 3 {
		t.Errorf("expected 3 values, got %d: %v", len(body.Data), body.Data)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/series
// ────────────────────────────────────────────────────────────────────────────

func TestSeriesSuccess(t *testing.T) {
	vl := &mockVL{
		queryLogsRecs: []vlogs.Record{
			{"_time": "2024-01-15T12:00:00Z", "_msg": "m", "app": "api", "env": "prod"},
			{"_time": "2024-01-15T12:00:01Z", "_msg": "m", "app": "worker", "env": "prod"},
		},
	}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/series?start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body loki.SeriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Data) != 2 {
		t.Errorf("expected 2 series, got %d: %v", len(body.Data), body.Data)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Concurrency limiter — HTTP 429
// ────────────────────────────────────────────────────────────────────────────

func TestConcurrencyLimitReturns429(t *testing.T) {
	// Create a limiter that is already saturated (0 slots, 0 queue).
	saturated := limits.New(1, 0)
	// Acquire the only slot to saturate it.
	if err := saturated.Acquire(context.Background()); err != nil {
		t.Fatalf("pre-acquire: %v", err)
	}
	defer saturated.Release()

	deps := newDeps(&mockVL{})
	deps.Lim = saturated
	h := handler.ConcurrencyMiddleware(saturated, 30*time.Second)(buildHandler(deps))
	addr, cleanup := newTestServer(t, h)
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/labels?start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("expected Retry-After header")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/index/stats stub
// ────────────────────────────────────────────────────────────────────────────

func TestIndexStatsStub(t *testing.T) {
	addr, cleanup := newTestServer(t, buildHandler(newDeps(&mockVL{})))
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/index/stats")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/index/volume
// ────────────────────────────────────────────────────────────────────────────

func TestIndexVolumeEmptyResult(t *testing.T) {
	// No records returned by VL → empty vector response.
	addr, cleanup := newTestServer(t, buildHandler(newDeps(&mockVL{})))
	defer cleanup()

	for _, path := range []string{
		"/loki/api/v1/index/volume",
		"/loki/api/v1/index/volume_range",
	} {
		resp, err := http.Get(addr + path + "?start=1705320000&end=1705323600")
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}

		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: status = %d, want 200", path, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
			t.Errorf("%s: Content-Type = %q, want application/json", path, ct)
		}

		var body loki.IndexVolumeResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("%s: decode: %v", path, err)
		}
		resp.Body.Close()

		if body.Status != "success" {
			t.Errorf("%s: status = %q, want success", path, body.Status)
		}
		if body.Data.ResultType != "vector" {
			t.Errorf("%s: resultType = %q, want vector", path, body.Data.ResultType)
		}
		if len(body.Data.Result) != 0 {
			t.Errorf("%s: expected 0 result entries, got %d", path, len(body.Data.Result))
		}
	}
}

func TestIndexVolumeWithQuery(t *testing.T) {
	vl := &mockVL{
		queryLogsRecs: []vlogs.Record{
			{"_time": "2024-01-15T12:00:00Z", "_msg": "r1", "service_name": "frontend"},
			{"_time": "2024-01-15T12:00:01Z", "_msg": "r2", "service_name": "frontend"},
			{"_time": "2024-01-15T12:00:02Z", "_msg": "r3", "service_name": "backend"},
		},
	}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr +
		`/loki/api/v1/index/volume?query={service_name=~".+"}&start=1705320000&end=1705323600`)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body loki.IndexVolumeResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if body.Status != "success" {
		t.Errorf("status = %q, want success", body.Status)
	}
	if body.Data.ResultType != "vector" {
		t.Errorf("resultType = %q, want vector", body.Data.ResultType)
	}
	if len(body.Data.Result) != 2 {
		t.Fatalf("expected 2 result entries, got %d", len(body.Data.Result))
	}

	// Results are sorted by count descending: frontend (2) before backend (1).
	first := body.Data.Result[0]
	if first.Metric["service_name"] != "frontend" {
		t.Errorf("first entry service_name = %q, want frontend", first.Metric["service_name"])
	}
	if len(first.Value) != 2 {
		t.Fatalf("first entry Value len = %d, want 2", len(first.Value))
	}
	// Value[1] is the count as a JSON string.
	if countStr, ok := first.Value[1].(string); !ok || countStr != "2" {
		t.Errorf("first entry count = %v, want \"2\"", first.Value[1])
	}

	second := body.Data.Result[1]
	if second.Metric["service_name"] != "backend" {
		t.Errorf("second entry service_name = %q, want backend", second.Metric["service_name"])
	}
	if countStr, ok := second.Value[1].(string); !ok || countStr != "1" {
		t.Errorf("second entry count = %v, want \"1\"", second.Value[1])
	}
}

func TestIndexVolumeMultiLabelGrouping(t *testing.T) {
	vl := &mockVL{
		queryLogsRecs: []vlogs.Record{
			{"_time": "2024-01-15T12:00:00Z", "_msg": "r1", "service_name": "api", "env": "prod"},
			{"_time": "2024-01-15T12:00:01Z", "_msg": "r2", "service_name": "api", "env": "prod"},
			{"_time": "2024-01-15T12:00:02Z", "_msg": "r3", "service_name": "api", "env": "staging"},
		},
	}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr +
		`/loki/api/v1/index/volume?query={service_name=~".+",env=~".+"}&start=1705320000&end=1705323600`)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body loki.IndexVolumeResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Two unique (service_name, env) combinations.
	if len(body.Data.Result) != 2 {
		t.Fatalf("expected 2 result entries, got %d: %+v", len(body.Data.Result), body.Data.Result)
	}
	// api/prod has 2 entries and should be first.
	first := body.Data.Result[0]
	if first.Metric["service_name"] != "api" || first.Metric["env"] != "prod" {
		t.Errorf("first entry metric = %v, want {service_name:api, env:prod}", first.Metric)
	}
	if countStr, ok := first.Value[1].(string); !ok || countStr != "2" {
		t.Errorf("first entry count = %v, want \"2\"", first.Value[1])
	}
}

func TestIndexVolumeVLError(t *testing.T) {
	vl := &mockVL{queryLogsErr: &testVLError{}}
	addr, cleanup := newTestServer(t, buildHandler(newDeps(vl)))
	defer cleanup()

	resp, err := http.Get(addr +
		"/loki/api/v1/index/volume?start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
}

func TestIndexVolumeBadQuery(t *testing.T) {
	addr, cleanup := newTestServer(t, buildHandler(newDeps(&mockVL{})))
	defer cleanup()

	resp, err := http.Get(addr +
		"/loki/api/v1/index/volume?query=not-valid-logql&start=1705320000&end=1705323600")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// /loki/api/v1/drilldown-limits stub
// ────────────────────────────────────────────────────────────────────────────

func TestDrilldownLimitsStub(t *testing.T) {
	addr, cleanup := newTestServer(t, buildHandler(newDeps(&mockVL{})))
	defer cleanup()

	resp, err := http.Get(addr + "/loki/api/v1/drilldown-limits")
	if err != nil {
		t.Fatalf("GET /loki/api/v1/drilldown-limits: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Metadata cache
// ────────────────────────────────────────────────────────────────────────────

func TestMetadataCacheHit(t *testing.T) {
	callCount := 0
	vl := &countingVL{
		names:        []string{"app"},
		onFieldNames: func() { callCount++ },
	}
	deps := newDeps(vl)
	addr, cleanup := newTestServer(t, buildHandler(deps))
	defer cleanup()

	url := addr + "/loki/api/v1/labels?start=1705320000&end=1705323600"
	for i := 0; i < 3; i++ {
		resp, _ := http.Get(url)
		resp.Body.Close()
	}

	// All three requests land in the same minute bucket; only the first should
	// hit VictoriaLogs — the other two should be served from cache.
	if callCount != 1 {
		t.Errorf("FieldNames called %d times, want 1 (cache should serve subsequent requests)", callCount)
	}
}

// countingVL wraps mockVL and invokes a callback on FieldNames.
type countingVL struct {
	names        []string
	onFieldNames func()
}

func (c *countingVL) QueryLogs(_ context.Context, _ vlogs.LogQueryRequest, _ func(vlogs.Record) error) error {
	return nil
}

func (c *countingVL) QueryHits(_ context.Context, _ vlogs.HitsQueryRequest) ([]vlogs.HitBucket, error) {
	return nil, nil
}

func (c *countingVL) FieldNames(_ context.Context, _ vlogs.FieldNamesRequest) ([]string, error) {
	c.onFieldNames()
	return c.names, nil
}

func (c *countingVL) FieldValues(_ context.Context, _ vlogs.FieldValuesRequest) ([]string, error) {
	return nil, nil
}
