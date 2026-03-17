package handler

import (
	"strings"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

// BuildHandler constructs the full fasthttp.RequestHandler with all Loki-compatible
// routes registered. It is shared between main.go and handler_test.go to
// guarantee that the test routing mirrors production.
//
// fasthttp/router (v1.5.4) has a radix-tree bug where a parametric route
// like /loki/api/v1/label/:name/values becomes unreachable when the static
// route /loki/api/v1/labels is also registered (shared prefix "label"). To
// work around this, /loki/api/v1/label/:name/values is NOT registered with
// the router and is instead matched by extracting the name segment manually
// in the returned wrapper handler.
func (d *Deps) BuildHandler() fasthttp.RequestHandler {
	r := router.New()

	// Loki-compatible query endpoints.
	r.GET("/loki/api/v1/query_range", d.QueryRange)
	r.GET("/loki/api/v1/query", d.Query)

	// Loki label discovery endpoints.
	r.GET("/loki/api/v1/labels", d.Labels)
	// NOTE: /loki/api/v1/label/:name/values is handled below via manual extraction.
	r.GET("/loki/api/v1/series", d.Series)
	r.GET("/loki/api/v1/detected_labels", d.DetectedLabels)
	r.GET("/loki/api/v1/detected_fields", d.DetectedFields)

	// Stub / health endpoints.
	r.GET("/loki/api/v1/index/stats", d.IndexStats)
	r.GET("/loki/api/v1/index/volume", d.IndexVolume)
	r.GET("/loki/api/v1/index/volume_range", d.IndexVolume)
	r.GET("/loki/api/v1/drilldown-limits", d.DrilldownLimits)
	r.GET("/loki/api/v1/patterns", d.Patterns)
	r.GET("/ready", Ready)

	inner := r.Handler
	labelValuesHandler := d.LabelValues

	return func(ctx *fasthttp.RequestCtx) {
		// Manual match for /loki/api/v1/label/:name/values to work around the
		// fasthttp/router radix-tree bug with the /labels static sibling route.
		if string(ctx.Method()) == "GET" {
			if name := extractLabelValueName(string(ctx.Path())); name != "" {
				ctx.SetUserValue("name", name)
				labelValuesHandler(ctx)
				return
			}
		}
		inner(ctx)
	}
}

// extractLabelValueName returns the label name segment from a path matching
// /loki/api/v1/label/{name}/values, or "" if the path does not match.
func extractLabelValueName(path string) string {
	const prefix = "/loki/api/v1/label/"
	const suffix = "/values"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return ""
	}
	name := path[len(prefix) : len(path)-len(suffix)]
	// Reject empty names or names containing slashes (not a single segment).
	if name == "" || strings.ContainsRune(name, '/') {
		return ""
	}
	return name
}
