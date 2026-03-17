// Package vlogs provides a streaming HTTP client for the VictoriaLogs query API.
package vlogs

import "time"

// ────────────────────────────────────────────────────────────────────────────
// Domain types
// ────────────────────────────────────────────────────────────────────────────

// Record is a single log record returned by POST /select/logsql/query.
// All field values are strings; the special fields are:
//   - "_msg"  — the raw log line
//   - "_time" — RFC3339Nano timestamp, e.g. "2024-01-15T12:00:00.123456789Z"
//
// All other keys are indexed label fields.
type Record map[string]string

// HitBucket is a single time-bucket entry from POST /select/logsql/hits.
type HitBucket struct {
	Timestamp time.Time
	Count     int64
}

// ────────────────────────────────────────────────────────────────────────────
// Request parameter types
// ────────────────────────────────────────────────────────────────────────────

// LogQueryRequest parameterises POST /select/logsql/query.
type LogQueryRequest struct {
	Query string    // LogsQL filter expression
	Start time.Time // inclusive start of time range
	End   time.Time // inclusive end of time range
	Limit int       // maximum number of records; 0 means server default
}

// HitsQueryRequest parameterises POST /select/logsql/hits.
type HitsQueryRequest struct {
	Query string        // LogsQL filter expression
	Start time.Time     // inclusive start of time range
	End   time.Time     // inclusive end of time range
	Step  time.Duration // bucket width, e.g. time.Minute
}

// FieldNamesRequest parameterises GET /select/logsql/field_names.
type FieldNamesRequest struct {
	Query string    // LogsQL filter to scope which fields are returned
	Start time.Time
	End   time.Time
}

// FieldValuesRequest parameterises GET /select/logsql/field_values.
type FieldValuesRequest struct {
	FieldName string    // the field whose distinct values are requested
	Query     string    // LogsQL filter to scope the results
	Start     time.Time
	End       time.Time
	Limit     int // maximum number of values; 0 means server default
}

// ────────────────────────────────────────────────────────────────────────────
// VictoriaLogs API JSON response shapes (internal)
// ────────────────────────────────────────────────────────────────────────────

// vlFieldsResponse is the JSON body returned by /field_names and /field_values.
//
//	{"values":[{"value":"app","hits":100},{"value":"level","hits":50}]}
//
// Each entry is an object with a string "value" and an integer "hits" count.
// The hits count is ignored by the proxy; only the field name is used.
type vlFieldsResponse struct {
	Values []vlFieldEntry `json:"values"`
}

// vlFieldEntry is one item in a vlFieldsResponse.
type vlFieldEntry struct {
	Value string `json:"value"`
	Hits  int64  `json:"hits"`
}

// vlHitsResponse is the JSON body returned by /select/logsql/hits.
// VictoriaLogs returns a top-level "hits" array where each entry represents
// one time bucket.
//
//	{"hits":[{"timestamp":"2024-01-15T12:00:00Z","hits":42}, ...]}
//
// NOTE: verify this against your VictoriaLogs version; the format changed
// between VL releases. Adjust vlHitEntry if necessary.
type vlHitsResponse struct {
	Hits []vlHitEntry `json:"hits"`
}

// vlHitEntry is one bucket inside vlHitsResponse.
type vlHitEntry struct {
	Timestamp string `json:"timestamp"` // RFC3339
	Hits      int64  `json:"hits"`
}
