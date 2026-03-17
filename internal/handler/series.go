package handler

import (
	"log/slog"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// Series handles GET /loki/api/v1/series.
//
// Grafana Logs Drilldown calls this endpoint to discover which log streams
// exist in the requested time range so it can populate the streams panel.
//
// Strategy: run a bounded QueryLogs (limit = MaxStreamsPerResponse) with the
// LogsQL filter derived from the first match[] parameter, then collect the
// distinct label sets from the sampled records using a StreamGrouper. This
// accurately reflects the actual streams present in VictoriaLogs rather than
// synthesising a Cartesian product of field values.
func (d *Deps) Series(ctx *fasthttp.RequestCtx) {
	start, end, err := parseTimeRange(ctx)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	logsql := d.seriesFilter(ctx)

	grouper := loki.NewStreamGrouper(d.Cfg.Labels.KnownLabels, d.Cfg.Limits.MaxStreamsPerResponse)

	err = d.VL.QueryLogs(reqContext(ctx), vlogs.LogQueryRequest{
		Query: logsql,
		Start: start,
		End:   end,
		Limit: d.Cfg.Limits.MaxStreamsPerResponse,
	}, grouper.Add)

	if err != nil && !isLargeOrCancelled(err) {
		slog.Error("Series QueryLogs failed", "err", err)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"failed to query streams from VictoriaLogs")
		return
	}

	// Build the series response from the distinct streams collected by the grouper.
	streams := grouper.Streams()
	data := make([]map[string]string, 0, len(streams))
	for _, s := range streams {
		if len(s.Stream) > 0 {
			data = append(data, s.Stream)
		}
	}

	writeJSON(ctx, fasthttp.StatusOK, loki.SeriesResponse{
		Status: "success",
		Data:   data,
	})
}

// seriesFilter derives the LogsQL filter string from the first match[]
// parameter in the request. If absent or unparseable, "*" (match-all) is used.
func (d *Deps) seriesFilter(ctx *fasthttp.RequestCtx) string {
	match := string(ctx.QueryArgs().Peek("match[]"))
	if match == "" {
		return "*"
	}

	ast, err := parser.Parse(match)
	if err != nil {
		slog.Warn("Series: cannot parse match[] selector, using match-all",
			"match", match, "err", err)
		return "*"
	}

	res, err := translator.Translate(ast, translator.Options{LabelRemap: d.Cfg.Labels.LabelRemap})
	if err != nil {
		slog.Warn("Series: cannot translate match[] selector, using match-all",
			"match", match, "err", err)
		return "*"
	}

	return res.LogsQL
}

// isLargeOrCancelled returns true for errors that indicate a partial result is
// acceptable: the body-size cap was hit, or the client cancelled the request.
func isLargeOrCancelled(err error) bool {
	if err == vlogs.ErrResponseTooLarge {
		return true
	}
	switch err {
	case nil:
		return false
	}
	// context.Canceled and context.DeadlineExceeded: treat partial result as OK.
	str := err.Error()
	return str == "context canceled" || str == "context deadline exceeded"
}
