package handler

import "github.com/valyala/fasthttp"

// DrilldownLimits handles GET /loki/api/v1/drilldown-limits.
// Grafana Logs Drilldown queries this endpoint for per-tenant UI configuration
// (max query length, ingestion rates, etc.). We return a static response that
// mirrors a minimal Loki configuration so Logs Drilldown behaves correctly
// without requiring a real Loki backend.
func (d *Deps) DrilldownLimits(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, map[string]interface{}{
		"limits": map[string]interface{}{
			"discover_log_levels": true,
			"discover_service_name": []string{
				"service",
				"app",
				"application",
				"app_name",
				"name",
				"app_kubernetes_io_name",
				"container",
				"container_name",
				"k8s_container_name",
				"component",
				"workload",
				"job",
				"k8s_job_name",
			},
			"log_level_fields": []string{
				"level",
				"LEVEL",
				"Level",
				"log.level",
				"severity",
				"SEVERITY",
				"Severity",
				"SeverityText",
				"lvl",
				"LVL",
				"Lvl",
				"severity_text",
				"Severity_Text",
				"SEVERITY_TEXT",
			},
			"max_entries_limit_per_query": 5000,
			"max_line_size_truncate":      false,
			"max_query_bytes_read":        "0B",
			"max_query_length":            "30d1h",
			"max_query_lookback":          "31d",
			"max_query_range":             "0s",
			"max_query_series":            500,
			"metric_aggregation_enabled":  true,
			"otlp_config": map[string]interface{}{
				"resource_attributes": map[string]interface{}{
					"attributes_config": []map[string]interface{}{
						{
							"action": "index_label",
							"attributes": []string{
								"service.name",
								"service.namespace",
								"service.instance.id",
								"deployment.environment",
								"cloud.region",
								"cloud.availability_zone",
								"k8s.cluster.name",
								"k8s.namespace.name",
								"k8s.pod.name",
								"k8s.container.name",
								"container.name",
								"k8s.replicaset.name",
								"k8s.deployment.name",
								"k8s.statefulset.name",
								"k8s.daemonset.name",
								"k8s.cronjob.name",
								"k8s.job.name",
								"app_id",
								"app_key",
								"kind",
								"deployment.environment.name",
							},
						},
					},
				},
			},
			"pattern_persistence_enabled": false,
			"query_timeout":               "5m",
			"retention_period":            "31d",
			"volume_enabled":              true,
			"volume_max_series":           100000000,
		},
		"pattern_ingester_enabled": true,
		"version":                  "fake",
	})
}
