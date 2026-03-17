package vlogs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/config"
)

// ────────────────────────────────────────────────────────────────────────────
// Interface
// ────────────────────────────────────────────────────────────────────────────

// VLogsClient is the interface that wraps all VictoriaLogs query operations.
// The interface makes it trivial to substitute a mock in handler tests.
type VLogsClient interface {
	// QueryLogs streams log records for a log query.
	// fn is called once per record; returning a non-nil error stops streaming.
	QueryLogs(ctx context.Context, req LogQueryRequest, fn func(Record) error) error

	// QueryHits returns hit-count buckets for a metric query
	// (count_over_time / rate). Results are fully buffered because the number
	// of buckets is bounded by (timeRange / step).
	QueryHits(ctx context.Context, req HitsQueryRequest) ([]HitBucket, error)

	// FieldNames returns all indexed field names visible in the given time
	// range that match the optional filter query.
	FieldNames(ctx context.Context, req FieldNamesRequest) ([]string, error)

	// FieldValues returns distinct values for a single field.
	FieldValues(ctx context.Context, req FieldValuesRequest) ([]string, error)
}

// ────────────────────────────────────────────────────────────────────────────
// Concrete implementation
// ────────────────────────────────────────────────────────────────────────────

// Client is the production VLogsClient backed by an HTTP connection pool.
type Client struct {
	cfg    config.VLogsConfig
	httpCl *http.Client
	maxB   int64 // MaxResponseBodyBytes, cached for convenience
}

// NewClient constructs a Client with a tuned HTTP transport derived from cfg.
// The returned client satisfies VLogsClient.
func NewClient(cfg config.VLogsConfig, maxResponseBytes int64) *Client {
	transport := &http.Transport{
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxConnsPerHost:     cfg.MaxConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return &Client{
		cfg: cfg,
		httpCl: &http.Client{
			Transport: transport,
			// No global timeout — per-request context deadline is used instead.
		},
		maxB: maxResponseBytes,
	}
}

// ────────────────────────────────────────────────────────────────────────────
// VLogsClient method implementations
// ────────────────────────────────────────────────────────────────────────────

// QueryLogs calls POST /select/logsql/query and streams the NDJSON response
// record-by-record into fn. The total bytes read from VL are capped at
// c.maxB; on overflow fn is not called and ErrResponseTooLarge is returned.
func (c *Client) QueryLogs(ctx context.Context, req LogQueryRequest, fn func(Record) error) error {
	form := url.Values{}
	form.Set("query", req.Query)
	form.Set("start", formatTime(req.Start))
	form.Set("end", formatTime(req.End))
	if req.Limit > 0 {
		form.Set("limit", strconv.Itoa(req.Limit))
	}

	httpReq, err := http.NewRequestWithContext(
		ctx, http.MethodPost,
		c.cfg.URL+"/select/logsql/query",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return fmt.Errorf("build QueryLogs request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c.decorateRequest(httpReq)

	resp, err := c.httpCl.Do(httpReq)
	if err != nil {
		return fmt.Errorf("QueryLogs HTTP: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("QueryLogs: VL returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return StreamDecoder(ctx, resp.Body, c.maxB, fn)
}

// QueryHits calls POST /select/logsql/hits and returns all hit buckets.
// The response is fully buffered (bounded by timeRange/step).
func (c *Client) QueryHits(ctx context.Context, req HitsQueryRequest) ([]HitBucket, error) {
	form := url.Values{}
	form.Set("query", req.Query)
	form.Set("start", formatTime(req.Start))
	form.Set("end", formatTime(req.End))
	form.Set("step", formatDuration(req.Step))

	httpReq, err := http.NewRequestWithContext(
		ctx, http.MethodPost,
		c.cfg.URL+"/select/logsql/hits",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, fmt.Errorf("build QueryHits request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c.decorateRequest(httpReq)

	resp, err := c.httpCl.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("QueryHits HTTP: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("QueryHits: VL returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var vlResp vlHitsResponse
	if err := json.NewDecoder(resp.Body).Decode(&vlResp); err != nil {
		return nil, fmt.Errorf("decode QueryHits response: %w", err)
	}

	buckets := make([]HitBucket, 0, len(vlResp.Hits))
	for _, e := range vlResp.Hits {
		ts, err := time.Parse(time.RFC3339, e.Timestamp)
		if err != nil {
			// Try RFC3339Nano as a fallback
			ts, err = time.Parse(time.RFC3339Nano, e.Timestamp)
			if err != nil {
				return nil, fmt.Errorf("parse hit timestamp %q: %w", e.Timestamp, err)
			}
		}
		buckets = append(buckets, HitBucket{Timestamp: ts, Count: e.Hits})
	}
	return buckets, nil
}

// FieldNames calls GET /select/logsql/field_names and returns the list of
// indexed field names visible in the given time range.
func (c *Client) FieldNames(ctx context.Context, req FieldNamesRequest) ([]string, error) {
	u, err := url.Parse(c.cfg.URL + "/select/logsql/field_names")
	if err != nil {
		return nil, fmt.Errorf("build FieldNames URL: %w", err)
	}
	q := u.Query()
	q.Set("query", req.Query)
	q.Set("start", formatTime(req.Start))
	q.Set("end", formatTime(req.End))
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build FieldNames request: %w", err)
	}
	c.decorateRequest(httpReq)

	resp, err := c.httpCl.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("FieldNames HTTP: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("FieldNames: VL returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var vlResp vlFieldsResponse
	if err := json.NewDecoder(resp.Body).Decode(&vlResp); err != nil {
		return nil, fmt.Errorf("decode FieldNames response: %w", err)
	}
	names := make([]string, len(vlResp.Values))
	for i, e := range vlResp.Values {
		names[i] = e.Value
	}
	return names, nil
}

// FieldValues calls GET /select/logsql/field_values and returns distinct values
// for the requested field name.
func (c *Client) FieldValues(ctx context.Context, req FieldValuesRequest) ([]string, error) {
	u, err := url.Parse(c.cfg.URL + "/select/logsql/field_values")
	if err != nil {
		return nil, fmt.Errorf("build FieldValues URL: %w", err)
	}
	q := u.Query()
	q.Set("field", req.FieldName)
	q.Set("query", req.Query)
	q.Set("start", formatTime(req.Start))
	q.Set("end", formatTime(req.End))
	if req.Limit > 0 {
		q.Set("limit", strconv.Itoa(req.Limit))
	}
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build FieldValues request: %w", err)
	}
	c.decorateRequest(httpReq)

	resp, err := c.httpCl.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("FieldValues HTTP: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("FieldValues: VL returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var vlResp vlFieldsResponse
	if err := json.NewDecoder(resp.Body).Decode(&vlResp); err != nil {
		return nil, fmt.Errorf("decode FieldValues response: %w", err)
	}
	values := make([]string, len(vlResp.Values))
	for i, e := range vlResp.Values {
		values[i] = e.Value
	}
	return values, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Request decoration
// ────────────────────────────────────────────────────────────────────────────

// decorateRequest applies authentication, extra headers, and extra query
// parameters to every outbound VictoriaLogs request. It is the single place
// where all four VL endpoints pick up auth and tenant configuration.
func (c *Client) decorateRequest(req *http.Request) {
	// Auth — mutually exclusive; validated at config load time.
	if c.cfg.BasicAuth != nil {
		req.SetBasicAuth(c.cfg.BasicAuth.Username, c.cfg.BasicAuth.Password)
	} else if c.cfg.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.BearerToken)
	}

	// Extra headers (e.g. X-Tenant-ID).
	for k, v := range c.cfg.ExtraHeaders {
		req.Header.Set(k, v)
	}

	// Extra query parameters (appended; does not clobber params already set).
	if len(c.cfg.ExtraParams) > 0 {
		q := req.URL.Query()
		for k, v := range c.cfg.ExtraParams {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Formatting helpers
// ────────────────────────────────────────────────────────────────────────────

// formatTime formats a time.Time as RFC3339Nano for VL query parameters.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// formatDuration formats a duration as a Go-style string (e.g. "5m", "1h30m").
// VictoriaLogs accepts Go duration notation for the step parameter.
func formatDuration(d time.Duration) string {
	return d.String()
}
