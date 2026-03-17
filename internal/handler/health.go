package handler

import (
	"strings"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
)

// Ready handles GET /ready — a simple liveness/readiness probe that returns
// HTTP 200 once the server is listening.
func Ready(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/plain; charset=utf-8")
	ctx.SetStatusCode(fasthttp.StatusOK)
	_, _ = ctx.WriteString("ready\n")
}

// isVectorExpr reports whether s is a PromQL vector() expression. Grafana
// sends "vector(1)+vector(1)" to both /query and /query_range when testing a
// Loki datasource, and expects a successful (non-error) response.
func isVectorExpr(s string) bool {
	return strings.HasPrefix(strings.TrimSpace(s), "vector(")
}

// IndexStats handles GET /loki/api/v1/index/stats.
// Grafana queries this endpoint on datasource configuration; we return a zero
// stub so it does not error out. Real statistics are not available from
// VictoriaLogs via a compatible API.
func (d *Deps) IndexStats(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, loki.IndexStatsResponse{
		Status: "success",
		Data:   loki.IndexStats{},
	})
}
