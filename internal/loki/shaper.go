package loki

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// StreamGrouper accumulates vlogs.Records into Loki streams grouped by their
// label values. It is designed for streaming use: Add is called once per
// record as records arrive from the VictoriaLogs NDJSON decoder.
type StreamGrouper struct {
	knownLabels []string
	streams     map[string]*streamState
	maxStreams   int
	truncated   bool
}

type streamState struct {
	labels map[string]string
	values [][2]string // [ts_ns_string, log_line]
}

// NewStreamGrouper creates a StreamGrouper.
//
// knownLabels is the label allowlist used to build the stream key. Only fields
// whose name appears in knownLabels are included in the stream label set. If
// knownLabels is empty, all non-special fields (_msg, _time) are used.
//
// maxStreams caps the number of distinct streams that may accumulate; records
// that would create a new stream beyond the cap are silently dropped and
// Truncated returns true.
func NewStreamGrouper(knownLabels []string, maxStreams int) *StreamGrouper {
	return &StreamGrouper{
		knownLabels: knownLabels,
		streams:     make(map[string]*streamState),
		maxStreams:   maxStreams,
	}
}

// Add processes one VL record into the appropriate Loki stream. The method
// satisfies the func(vlogs.Record) error callback signature used by
// VLogsClient.QueryLogs, so it can be passed directly.
func (g *StreamGrouper) Add(rec vlogs.Record) error {
	labels := g.extractLabels(rec)
	key := buildStreamKey(labels)

	st, ok := g.streams[key]
	if !ok {
		if len(g.streams) >= g.maxStreams {
			g.truncated = true
			return nil
		}
		st = &streamState{labels: labels}
		g.streams[key] = st
	}

	ts := parseVLTimestamp(rec["_time"])
	st.values = append(st.values, [2]string{ts, rec["_msg"]})
	return nil
}

// Streams returns the accumulated Loki streams. Values within each stream are
// sorted ascending by nanosecond timestamp. The returned slice itself is sorted
// by stream key for deterministic output.
func (g *StreamGrouper) Streams() []LokiStream {
	result := make([]LokiStream, 0, len(g.streams))
	for _, st := range g.streams {
		sort.Slice(st.values, func(i, j int) bool {
			return st.values[i][0] < st.values[j][0]
		})
		result = append(result, LokiStream{
			Stream: st.labels,
			Values: st.values,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return buildStreamKey(result[i].Stream) < buildStreamKey(result[j].Stream)
	})
	return result
}

// Truncated reports whether any records were dropped because the stream cap
// (maxStreams) was reached. When true, the caller should set the
// X-Proxy-Truncated response header.
func (g *StreamGrouper) Truncated() bool { return g.truncated }

// extractLabels returns a map of only the label fields from rec according to
// the knownLabels allowlist. The _msg and _time fields are always excluded.
func (g *StreamGrouper) extractLabels(rec vlogs.Record) map[string]string {
	out := make(map[string]string)
	if len(g.knownLabels) > 0 {
		for _, k := range g.knownLabels {
			if v, ok := rec[k]; ok {
				out[k] = v
			}
		}
	} else {
		for k, v := range rec {
			if k != "_msg" && k != "_time" {
				out[k] = v
			}
		}
	}
	return out
}

// buildStreamKey returns a canonical JSON-like string representation of the
// label map, used as the grouping key. Keys are sorted for stability.
func buildStreamKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%q:%q", k, labels[k])
	}
	sb.WriteByte('}')
	return sb.String()
}

// parseVLTimestamp converts a VictoriaLogs _time field (RFC3339Nano) to the
// nanosecond Unix timestamp decimal string that Loki uses in its values arrays.
func parseVLTimestamp(s string) string {
	if s == "" {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return strconv.FormatInt(time.Now().UnixNano(), 10)
		}
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}

// ShapeMatrix converts a slice of VL hit buckets into a Loki matrix result
// slice. metric is the label set associated with the series (typically the
// equality matchers extracted from the original LogQL query).
//
// When isRate is true the bucket count is divided by stepSec to produce a
// per-second rate value, matching the semantics of LogQL rate().
func ShapeMatrix(buckets []vlogs.HitBucket, metric map[string]string, isRate bool, stepSec float64) []MatrixSeries {
	values := make([][]interface{}, 0, len(buckets))
	for _, b := range buckets {
		// Matrix timestamps are Unix seconds as a float (not nanoseconds).
		ts := float64(b.Timestamp.UnixNano()) / 1e9
		var valStr string
		if isRate && stepSec > 0 {
			valStr = strconv.FormatFloat(float64(b.Count)/stepSec, 'f', -1, 64)
		} else {
			valStr = strconv.FormatInt(b.Count, 10)
		}
		values = append(values, []interface{}{ts, valStr})
	}
	if metric == nil {
		metric = map[string]string{}
	}
	return []MatrixSeries{{
		Metric: metric,
		Values: values,
	}}
}
