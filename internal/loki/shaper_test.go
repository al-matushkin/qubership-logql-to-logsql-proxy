package loki_test

import (
	"testing"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// ────────────────────────────────────────────────────────────────────────────
// StreamGrouper tests
// ────────────────────────────────────────────────────────────────────────────

func TestGroupSingleStream(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	_ = g.Add(vlogs.Record{
		"_time": "2024-01-15T12:00:00Z",
		"_msg":  "hello world",
		"app":   "api",
	})

	streams := g.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if streams[0].Stream["app"] != "api" {
		t.Errorf("stream.app = %q, want %q", streams[0].Stream["app"], "api")
	}
	if len(streams[0].Values) != 1 {
		t.Fatalf("expected 1 value, got %d", len(streams[0].Values))
	}
	if streams[0].Values[0][1] != "hello world" {
		t.Errorf("log line = %q, want %q", streams[0].Values[0][1], "hello world")
	}
}

func TestGroupMultipleStreams(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	records := []vlogs.Record{
		{"_time": "2024-01-15T12:00:00Z", "_msg": "line1", "app": "api"},
		{"_time": "2024-01-15T12:00:01Z", "_msg": "line2", "app": "worker"},
		{"_time": "2024-01-15T12:00:02Z", "_msg": "line3", "app": "api"},
	}
	for _, r := range records {
		_ = g.Add(r)
	}

	streams := g.Streams()
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(streams))
	}
	// Streams are sorted by key, so "api" < "worker" alphabetically.
	if streams[0].Stream["app"] != "api" {
		t.Errorf("streams[0].app = %q, want %q", streams[0].Stream["app"], "api")
	}
	if len(streams[0].Values) != 2 {
		t.Errorf("api stream: expected 2 values, got %d", len(streams[0].Values))
	}
	if streams[1].Stream["app"] != "worker" {
		t.Errorf("streams[1].app = %q, want %q", streams[1].Stream["app"], "worker")
	}
}

func TestGroupTimestampNano(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	_ = g.Add(vlogs.Record{
		"_time": "2024-01-15T12:00:00.123456789Z",
		"_msg":  "precise timestamp",
		"app":   "api",
	})

	streams := g.Streams()
	ts := streams[0].Values[0][0]

	// The timestamp should be a nanosecond Unix timestamp string.
	// 2024-01-15T12:00:00.123456789Z → should contain "123456789" at the end.
	if len(ts) < 10 {
		t.Errorf("timestamp too short: %q", ts)
	}
	// The last 9 digits should be 123456789.
	if len(ts) >= 9 && ts[len(ts)-9:] != "123456789" {
		t.Errorf("nanosecond part = %q, want %q", ts[len(ts)-9:], "123456789")
	}
}

func TestGroupMaxStreamsEnforced(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 2)

	for i := 0; i < 5; i++ {
		_ = g.Add(vlogs.Record{
			"_time": "2024-01-15T12:00:00Z",
			"_msg":  "msg",
			"svc":   string(rune('a' + i)), // distinct label value per record
		})
	}

	streams := g.Streams()
	if len(streams) != 2 {
		t.Errorf("expected max 2 streams, got %d", len(streams))
	}
	if !g.Truncated() {
		t.Error("expected Truncated() == true after cap exceeded")
	}
}

func TestGroupMaxStreamsNotTruncatedWhenUnderCap(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 10)
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "_msg": "m", "a": "1"})
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:01Z", "_msg": "m", "a": "1"})

	if g.Truncated() {
		t.Error("expected Truncated() == false")
	}
	if len(g.Streams()) != 1 {
		t.Errorf("expected 1 stream, got %d", len(g.Streams()))
	}
}

func TestGroupKnownLabelsFilter(t *testing.T) {
	// Only "app" is in the known-labels allowlist; "host" should be excluded
	// from the stream key.
	g := loki.NewStreamGrouper([]string{"app"}, 100)
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "_msg": "m", "app": "api", "host": "h1"})
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:01Z", "_msg": "m", "app": "api", "host": "h2"})

	// Both records have the same "app" value → they should be in the same stream.
	streams := g.Streams()
	if len(streams) != 1 {
		t.Errorf("expected 1 stream (host excluded from key), got %d", len(streams))
	}
	if _, ok := streams[0].Stream["host"]; ok {
		t.Error("stream should not contain 'host' label (not in known-labels)")
	}
}

func TestGroupValuesAreSortedByTimestamp(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	// Add records out-of-order; Streams() should return them sorted.
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:02Z", "_msg": "third", "app": "a"})
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "_msg": "first", "app": "a"})
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:01Z", "_msg": "second", "app": "a"})

	values := g.Streams()[0].Values
	if values[0][1] != "first" || values[1][1] != "second" || values[2][1] != "third" {
		t.Errorf("values not sorted: %v", values)
	}
}

func TestGroupEmptyMsgField(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	// Record with no _msg: should still be added with an empty log line.
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "app": "x"})
	streams := g.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if streams[0].Values[0][1] != "" {
		t.Errorf("empty _msg: got %q, want %q", streams[0].Values[0][1], "")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// ShapeMatrix tests
// ────────────────────────────────────────────────────────────────────────────

func TestMatrixResponseFormat(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	buckets := []vlogs.HitBucket{
		{Timestamp: now, Count: 42},
		{Timestamp: now.Add(time.Minute), Count: 58},
	}
	metric := map[string]string{"app": "api"}

	series := loki.ShapeMatrix(buckets, metric, false, 60)
	if len(series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(series))
	}
	s := series[0]
	if s.Metric["app"] != "api" {
		t.Errorf("metric.app = %q, want %q", s.Metric["app"], "api")
	}
	if len(s.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(s.Values))
	}
	// First value: [unix_seconds_float, "42"]
	ts0, ok := s.Values[0][0].(float64)
	if !ok {
		t.Fatalf("Values[0][0] is %T, want float64", s.Values[0][0])
	}
	if int64(ts0) != now.Unix() {
		t.Errorf("ts = %v, want %v", int64(ts0), now.Unix())
	}
	val0, ok := s.Values[0][1].(string)
	if !ok {
		t.Fatalf("Values[0][1] is %T, want string", s.Values[0][1])
	}
	if val0 != "42" {
		t.Errorf("value = %q, want %q", val0, "42")
	}
}

func TestMatrixRateQuery(t *testing.T) {
	now := time.Now().UTC()
	buckets := []vlogs.HitBucket{
		{Timestamp: now, Count: 120}, // 120 hits in 60s = 2/s rate
	}

	series := loki.ShapeMatrix(buckets, nil, true, 60)
	val := series[0].Values[0][1].(string)
	if val != "2" {
		t.Errorf("rate value = %q, want %q", val, "2")
	}
}

func TestMatrixNilMetric(t *testing.T) {
	buckets := []vlogs.HitBucket{{Timestamp: time.Now(), Count: 1}}
	series := loki.ShapeMatrix(buckets, nil, false, 60)
	if series[0].Metric == nil {
		t.Error("metric map should not be nil")
	}
}

func TestMatrixEmptyBuckets(t *testing.T) {
	series := loki.ShapeMatrix(nil, map[string]string{}, false, 60)
	if len(series) != 1 {
		t.Fatalf("expected 1 series even with empty buckets, got %d", len(series))
	}
	if len(series[0].Values) != 0 {
		t.Errorf("expected 0 values, got %d", len(series[0].Values))
	}
}
