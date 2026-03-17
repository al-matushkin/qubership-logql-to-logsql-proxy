// Package loki defines Loki API response types and the response shaper that
// converts VictoriaLogs NDJSON records into Loki streams/matrix JSON.
package loki

// StreamsResponse is the Loki API JSON body for a successful log query
// (resultType: "streams").
type StreamsResponse struct {
	Status string      `json:"status"`
	Data   StreamsData `json:"data"`
}

// StreamsData is the data field of a streams response.
type StreamsData struct {
	ResultType string       `json:"resultType"`
	Result     []LokiStream `json:"result"`
}

// LokiStream is a single stream entry: a label set and its matching log lines.
// Each value is a [timestamp_nanoseconds_string, log_line] pair.
type LokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

// MatrixResponse is the Loki API JSON body for a successful metric query
// (resultType: "matrix").
type MatrixResponse struct {
	Status string     `json:"status"`
	Data   MatrixData `json:"data"`
}

// MatrixData is the data field of a matrix response.
type MatrixData struct {
	ResultType string         `json:"resultType"`
	Result     []MatrixSeries `json:"result"`
}

// MatrixSeries is a single series in a matrix result.
// Each value is a [unix_seconds_float, count_string] pair.
type MatrixSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}

// LabelsResponse is the Loki API JSON body for GET /loki/api/v1/labels.
type LabelsResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// LabelValuesResponse is the Loki API JSON body for
// GET /loki/api/v1/label/{name}/values.
type LabelValuesResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// SeriesResponse is the Loki API JSON body for GET /loki/api/v1/series.
type SeriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

// IndexStatsResponse is a stub response for GET /loki/api/v1/index/stats.
type IndexStatsResponse struct {
	Status string     `json:"status"`
	Data   IndexStats `json:"data"`
}

// IndexStats holds the statistics stub returned by the index/stats endpoint.
type IndexStats struct {
	Streams int64 `json:"streams"`
	Chunks  int64 `json:"chunks"`
	Entries int64 `json:"entries"`
	Bytes   int64 `json:"bytes"`
}

// DetectedLabelsData is the Loki API JSON body for
// GET /loki/api/v1/detected_labels.
//
// Unlike most Loki endpoints, detected_labels does NOT use a
// {"status":"success","data":{…}} envelope — it returns this struct directly.
type DetectedLabelsData struct {
	DetectedLabels []DetectedLabel `json:"detectedLabels"`
}

// DetectedLabel is a single entry in a DetectedLabelsData response.
// Cardinality is the number of unique values seen for this label.
// VictoriaLogs does not expose per-label cardinality, so we always report 0.
type DetectedLabel struct {
	Label       string `json:"label"`
	Cardinality uint64 `json:"cardinality"`
}

// IndexVolumeResponse is the Loki API JSON body for GET /loki/api/v1/index/volume
// and GET /loki/api/v1/index/volume_range.
type IndexVolumeResponse struct {
	Status string          `json:"status"`
	Data   IndexVolumeData `json:"data"`
}

// IndexVolumeData is the data field of a volume response.
type IndexVolumeData struct {
	ResultType string             `json:"resultType"`
	Result     []IndexVolumeEntry `json:"result"`
}

// IndexVolumeEntry is a single result entry in an index/volume vector response.
// Metric is the label set identifying the group; Value is a
// [unix_seconds_float, count_string] pair representing the end timestamp of the
// requested time range and the total log record count for that label group.
type IndexVolumeEntry struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value"`
}

// DetectedFieldsResponse is the Loki API JSON body for
// GET /loki/api/v1/detected_fields.
// Grafana Logs Drilldown calls this endpoint to discover the field names (and
// their types / cardinality) that are present in the selected log streams.
type DetectedFieldsResponse struct {
	Fields []DetectedField `json:"fields"`
}

// DetectedField is a single entry in a DetectedFieldsResponse.
// VictoriaLogs does not expose per-field type or cardinality information from
// the field_names endpoint, so Type is always "string" and Cardinality is 0.
type DetectedField struct {
	Label       string `json:"label"`
	Type        string `json:"type"`
	Cardinality uint64 `json:"cardinality"`
}

// PatternsResponse is the Loki API JSON body for GET /loki/api/v1/patterns.
// Grafana Logs Drilldown uses this endpoint to display clustered log patterns.
// VictoriaLogs has no native pattern-detection feature; the proxy approximates
// it by returning per-query hit-count time series from /select/logsql/hits.
type PatternsResponse struct {
	Status string         `json:"status"`
	Data   []PatternEntry `json:"data"`
}

// PatternEntry is a single detected pattern in a PatternsResponse.
// Pattern is a human-readable template string (Grafana uses <_> as the
// wildcard token). Labels is the stream label set the pattern belongs to.
// Samples is an array of [unix_timestamp_seconds, count] pairs.
type PatternEntry struct {
	Pattern string            `json:"pattern"`
	Labels  map[string]string `json:"labels"`
	Samples [][]interface{}   `json:"samples"`
}

// ErrorResponse is the Loki-compatible error body returned on failure.
type ErrorResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}
