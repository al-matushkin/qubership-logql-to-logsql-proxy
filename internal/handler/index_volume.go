package handler

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// IndexVolume handles GET /loki/api/v1/index/volume and
// GET /loki/api/v1/index/volume_range.
//
// Grafana Logs Drilldown calls these endpoints to render per-stream volume
// histograms. The query parameter is a LogQL stream selector (e.g.
// {service_name=~".+"}). This handler translates it to LogsQL, streams
// matching records from VictoriaLogs, tallies counts grouped by the label
// names present in the selector, and returns a Loki vector response sorted
// by count descending.
//
// The "limit" parameter caps the number of result entries (unique label-value
// groups), not the number of log records scanned.
func (d *Deps) IndexVolume(ctx *fasthttp.RequestCtx) {
	// Query is optional; an absent query defaults to match-all.
	queryStr := string(ctx.QueryArgs().Peek("query"))
	if queryStr == "" {
		queryStr = "{}"
	}

	start, end, err := parseTimeRange(ctx)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	// limit is the maximum number of result entries returned, not the number
	// of log records fetched from VictoriaLogs.
	limit := d.Cfg.Limits.DefaultLimit
	if limStr := string(ctx.QueryArgs().Peek("limit")); limStr != "" {
		if n, err := strconv.Atoi(limStr); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > d.Cfg.Limits.MaxLimit {
		limit = d.Cfg.Limits.MaxLimit
	}

	// Parse LogQL.
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

	// Translate to LogsQL.
	xlat, err := translator.Translate(ast, translator.Options{LabelRemap: d.Cfg.Labels.LabelRemap})
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	// The label names from the stream selector define the grouping dimensions.
	groupLabels := volumeGroupLabels(ast)

	// Stream records from VictoriaLogs and tally counts per label-value group.
	// We cap the number of scanned records at MaxLimit to stay within the body
	// size limit; this means counts may be underrepresented for very large
	// datasets, but is consistent with the Series handler approach.
	counts := make(map[string]int64)
	metricsFor := make(map[string]map[string]string)

	scanErr := d.VL.QueryLogs(reqContext(ctx), vlogs.LogQueryRequest{
		Query: xlat.LogsQL,
		Start: start,
		End:   end,
		Limit: d.Cfg.Limits.MaxLimit,
	}, func(rec vlogs.Record) error {
		k, metric := volumeKey(rec, groupLabels)
		if _, exists := counts[k]; !exists {
			metricsFor[k] = metric
		}
		counts[k]++
		return nil
	})

	switch {
	case scanErr == nil:
		// complete result — nothing to do
	case errors.Is(scanErr, vlogs.ErrResponseTooLarge):
		ctx.Response.Header.Set("X-Proxy-Truncated", "true")
		slog.Warn("IndexVolume: response truncated by body size limit",
			"logsql", xlat.LogsQL)
	case errors.Is(scanErr, context.Canceled), errors.Is(scanErr, context.DeadlineExceeded):
		writeError(ctx, fasthttp.StatusGatewayTimeout, "timeout", "query timed out")
		return
	default:
		slog.Error("IndexVolume QueryLogs failed", "logsql", xlat.LogsQL, "err", scanErr)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"VictoriaLogs query failed: "+scanErr.Error())
		return
	}

	// Collect entries, sort by count descending, and apply the result limit.
	type entry struct {
		metric map[string]string
		count  int64
	}
	entries := make([]entry, 0, len(counts))
	for k, n := range counts {
		entries = append(entries, entry{metric: metricsFor[k], count: n})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].count > entries[j].count
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}

	// Build the Loki vector response. The timestamp is the end of the
	// requested range, matching the Loki index/volume API contract.
	endUnix := float64(end.UnixNano()) / 1e9
	result := make([]loki.IndexVolumeEntry, len(entries))
	for i, e := range entries {
		result[i] = loki.IndexVolumeEntry{
			Metric: e.metric,
			Value:  []interface{}{endUnix, strconv.FormatInt(e.count, 10)},
		}
	}

	writeJSON(ctx, fasthttp.StatusOK, loki.IndexVolumeResponse{
		Status: "success",
		Data: loki.IndexVolumeData{
			ResultType: "vector",
			Result:     result,
		},
	})
}

// volumeGroupLabels returns the deduplicated label names from the stream
// selector of q. These names are the dimensions by which index/volume groups
// its results (e.g. {service_name=~".+"} → ["service_name"]).
func volumeGroupLabels(q parser.Query) []string {
	var matchers []parser.LabelMatcher
	switch v := q.(type) {
	case *parser.LogQuery:
		matchers = v.Selector.Matchers
	case *parser.MetricQuery:
		matchers = v.Inner.Selector.Matchers
	}
	seen := make(map[string]bool, len(matchers))
	names := make([]string, 0, len(matchers))
	for _, m := range matchers {
		if !seen[m.Name] {
			seen[m.Name] = true
			names = append(names, m.Name)
		}
	}
	return names
}

// volumeKey returns a stable map-key string and the metric label map for a
// single log record given the set of group label names. Records that share the
// same values for every group label produce the same key.
func volumeKey(rec vlogs.Record, groupLabels []string) (string, map[string]string) {
	if len(groupLabels) == 0 {
		return "", map[string]string{}
	}
	parts := make([]string, 0, len(groupLabels)*2)
	metric := make(map[string]string, len(groupLabels))
	for _, name := range groupLabels {
		val := rec[name]
		metric[name] = val
		parts = append(parts, name, val)
	}
	return strings.Join(parts, "\x00"), metric
}
