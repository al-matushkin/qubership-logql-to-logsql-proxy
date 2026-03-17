package handler

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// Patterns handles GET /loki/api/v1/patterns.
//
// Grafana Logs Drilldown uses this endpoint to cluster log lines into patterns
// and display their volume over time. This handler implements pattern detection
// using VictoriaLogs' `| collapse_nums prettify` pipe, which replaces numbers,
// UUIDs, IPv4 addresses, timestamps, and datetimes with typed placeholders
// (<N>, <UUID>, <IP4>, <TIME>, <DATE>, <DATETIME>). Log lines that reduce to
// the same collapsed string are treated as the same pattern.
//
// The implementation:
//  1. Appends `| collapse_nums prettify | fields _msg, _time` to the translated
//     LogsQL query and streams matching records from VictoriaLogs.
//  2. Groups records by their collapsed _msg value (the pattern).
//  3. Counts records per time-step bucket for each pattern.
//  4. Returns the top-N patterns (by total record count) as PatternEntry items
//     with [[unix_seconds, count], ...] samples, matching the Loki wire format.
func (d *Deps) Patterns(ctx *fasthttp.RequestCtx) {
	queryStr := string(ctx.QueryArgs().Peek("query"))
	if queryStr == "" {
		queryStr = "{}"
	}

	start, end, err := parseTimeRange(ctx)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	step := parseDuration(string(ctx.QueryArgs().Peek("step")))

	// Number of top patterns to return (capped by MaxLimit).
	limit := d.Cfg.Limits.DefaultLimit
	if limStr := string(ctx.QueryArgs().Peek("limit")); limStr != "" {
		if n, err := strconv.Atoi(limStr); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > d.Cfg.Limits.MaxLimit {
		limit = d.Cfg.Limits.MaxLimit
	}

	ast, parseErr := parser.Parse(queryStr)
	if parseErr != nil {
		var unsup *parser.UnsupportedError
		if errors.As(parseErr, &unsup) {
			writeError(ctx, fasthttp.StatusBadRequest, "bad_data",
				"unsupported LogQL construct: "+unsup.Construct)
		} else {
			writeError(ctx, fasthttp.StatusBadRequest, "bad_data",
				"invalid LogQL query: "+parseErr.Error())
		}
		return
	}

	xlat, err := translator.Translate(ast, translator.Options{LabelRemap: d.Cfg.Labels.LabelRemap})
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	// Append the collapse_nums prettify pipe so VictoriaLogs normalises numbers
	// and special tokens server-side before we receive the records. Adding
	// `| fields _msg, _time` avoids transmitting all other log fields.
	logsqlQuery := xlat.LogsQL + " | collapse_nums prettify | fields _msg, _time"

	// patternBuckets maps collapsed _msg → (step-bucket index → hit count).
	patternBuckets := make(map[string]map[int64]int64)
	patternTotal := make(map[string]int64)

	scanErr := d.VL.QueryLogs(reqContext(ctx), vlogs.LogQueryRequest{
		Query: logsqlQuery,
		Start: start,
		End:   end,
		Limit: d.Cfg.Limits.MaxLimit,
	}, func(rec vlogs.Record) error {
		msg := rec["_msg"]
		if msg == "" {
			return nil
		}

		// Parse the record timestamp; skip unparseable entries.
		ts, tErr := time.Parse(time.RFC3339Nano, rec["_time"])
		if tErr != nil {
			ts, tErr = time.Parse(time.RFC3339, rec["_time"])
			if tErr != nil {
				return nil
			}
		}

		// Map the record time onto a zero-based step-bucket index.
		idx := int64(ts.Sub(start) / step)
		if idx < 0 {
			idx = 0
		}

		b, ok := patternBuckets[msg]
		if !ok {
			b = make(map[int64]int64)
			patternBuckets[msg] = b
		}
		b[idx]++
		patternTotal[msg]++
		return nil
	})

	switch {
	case scanErr == nil:
	case errors.Is(scanErr, vlogs.ErrResponseTooLarge):
		ctx.Response.Header.Set("X-Proxy-Truncated", "true")
		slog.Warn("Patterns: response truncated by body size limit",
			"logsql", logsqlQuery)
	case errors.Is(scanErr, context.Canceled), errors.Is(scanErr, context.DeadlineExceeded):
		writeError(ctx, fasthttp.StatusGatewayTimeout, "timeout", "query timed out")
		return
	default:
		slog.Error("Patterns QueryLogs failed", "logsql", logsqlQuery, "err", scanErr)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"VictoriaLogs query failed: "+scanErr.Error())
		return
	}

	// Rank patterns by total hit count and apply the caller-requested limit.
	type entry struct {
		pattern string
		total   int64
		buckets map[int64]int64
	}
	ranked := make([]entry, 0, len(patternBuckets))
	for pattern, b := range patternBuckets {
		ranked = append(ranked, entry{pattern: pattern, total: patternTotal[pattern], buckets: b})
	}
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].total > ranked[j].total
	})
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}

	// Stream labels come from the equality-matched label pairs in the selector.
	// Regex / negative matchers have no single representative value so they are
	// omitted, matching Loki's own behaviour.
	labels := selectorEqualityLabels(ast)

	data := make([]loki.PatternEntry, 0, len(ranked))
	for _, e := range ranked {
		// Emit samples sorted by bucket index so Grafana renders them in order.
		idxs := make([]int64, 0, len(e.buckets))
		for idx := range e.buckets {
			idxs = append(idxs, idx)
		}
		sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] })

		samples := make([][]interface{}, 0, len(idxs))
		for _, idx := range idxs {
			bucketTime := start.Add(time.Duration(idx) * step)
			samples = append(samples, []interface{}{
				float64(bucketTime.UnixNano()) / 1e9,
				e.buckets[idx],
			})
		}

		data = append(data, loki.PatternEntry{
			Pattern: e.pattern,
			Labels:  labels,
			Samples: samples,
		})
	}

	writeJSON(ctx, fasthttp.StatusOK, loki.PatternsResponse{
		Status: "success",
		Data:   data,
	})
}

// selectorEqualityLabels extracts only equality-matched labels from the stream
// selector of q. Regex and negative matchers are omitted because they cannot be
// reduced to a single representative value.
func selectorEqualityLabels(q parser.Query) map[string]string {
	var matchers []parser.LabelMatcher
	switch v := q.(type) {
	case *parser.LogQuery:
		matchers = v.Selector.Matchers
	case *parser.MetricQuery:
		matchers = v.Inner.Selector.Matchers
	case *parser.AggregationQuery:
		matchers = v.Inner.Inner.Selector.Matchers
	}
	labels := make(map[string]string, len(matchers))
	for _, m := range matchers {
		if m.Type == parser.Eq {
			labels[m.Name] = m.Value
		}
	}
	return labels
}
