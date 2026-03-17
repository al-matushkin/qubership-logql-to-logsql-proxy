# AGENT.md — Repository Guide for AI Agents

This file gives AI coding agents the context needed to work effectively in
this repository without having to reconstruct it from scratch.

---

## What this project is

**logql-proxy** is a Go HTTP proxy that translates Grafana's LogQL queries into
VictoriaLogs' LogsQL queries, making VictoriaLogs appear as a Loki datasource
to Grafana Logs Drilldown.

```mermaid
flowchart TD
  G[Grafana\n(Logs Drilldown)] -->|LogQL + Loki HTTP API| P[logql-proxy\n(this project)]
  P -->|LogsQL + VictoriaLogs HTTP API| V[VictoriaLogs]
```

---

## Essential commands

```bash
# Build binary → bin/logql-proxy
go build -ldflags="-s -w" -o bin/logql-proxy ./cmd/logql-proxy

# Run all tests (always use -race)
go test -race -count=1 ./...

# Run tests for a single package
go test -race -count=1 ./internal/parser/...

# Lint (requires golangci-lint)
golangci-lint run ./...

# Build Docker image
docker build -t logql-proxy:latest .

# Start full local stack (Grafana + VictoriaLogs + proxy)
docker compose up

# Helm — render templates without a cluster
helm template my-release charts/logql-proxy --set proxy.vlogs.url=http://vl:9428

# Helm — lint
helm lint charts/logql-proxy --set proxy.vlogs.url=http://vl:9428
```

**Important:** The shell in this repo's CI is **PowerShell** on Windows. Use
`;` as command separator, not `&&`.

---

## Repository layout

```text
cmd/logql-proxy/main.go          Entry point — wires config, client, limiter, cache, HTTP server
internal/
  config/config.go               Config struct, YAML loader, env-var overlay, validation
  parser/
    ast.go                       AST node types (Query, LogQuery, MetricQuery, LabelMatcher, …)
    logql.go                     Token-based recursive-descent LogQL parser
    logql_test.go
  translator/
    translator.go                AST → LogsQL string
    translator_test.go
  vlogs/
    client.go                    VLogsClient interface + HTTP implementation (QueryLogs, QueryHits, FieldNames, FieldValues)
    model.go                     VL request/response types (Record, HitBucket, LogQueryRequest, …)
    stream_decoder.go            NDJSON streaming decoder (bufio.Scanner + io.LimitedReader)
    metadata_cache.go            Bounded TTL in-memory cache (MetadataCache) for label metadata
    client_test.go
  handler/
    router.go                    fasthttp router setup + manual match for /label/{name}/values
    middleware.go                Deps struct, RecoveryMiddleware, LoggingMiddleware, ConcurrencyMiddleware, time/duration parsers
    query_range.go               GET /loki/api/v1/query_range  and  /query
    labels.go                    GET /loki/api/v1/labels  +  /detected_labels  +  /detected_fields
    label_values.go              GET /loki/api/v1/label/{name}/values
    series.go                    GET /loki/api/v1/series
    health.go                    GET /ready  and  GET /loki/api/v1/index/stats (stub)
    index_volume.go              GET /loki/api/v1/index/volume[_range] (computed from sampled records)
    drilldown_limits.go          GET /loki/api/v1/drilldown-limits (static Loki-compatible limits)
    patterns.go                  GET /loki/api/v1/patterns (best-effort clustering using VL hits)
    handler_test.go
  loki/
    model.go                     Loki API response types (StreamsResponse, MatrixResponse, …)
    shaper.go                    StreamGrouper (VL records → Loki streams) + ShapeMatrix
    shaper_test.go
  limits/
    limiter.go                   Semaphore + queue concurrency limiter (Limiter)
charts/logql-proxy/              Helm chart (Kubernetes deployment)
  Chart.yaml
  values.yaml
  custom.values.yaml             Local/personal overrides — NOT committed to shared branches
  templates/
    deployment.yaml
    service.yaml
    configmap.yaml               Renders proxy config.yaml from values (no secrets)
    secret.yaml                  VL credentials Secret (only created when inline creds provided)
    ingress.yaml                 Standard Kubernetes Ingress (opt-in)
    httproute.yaml               Gateway API v1 HTTPRoute (opt-in)
    hpa.yaml                     HorizontalPodAutoscaler (opt-in)
    pdb.yaml                     PodDisruptionBudget (opt-in)
    _helpers.tpl
    NOTES.txt
Dockerfile                       Multi-stage build: golang:1.25-alpine → distroless/static-debian12
docker-compose.yml               Local dev stack: proxy + VictoriaLogs + Grafana
.env.example                     All PROXY_* environment variables with defaults
PLAN.md                          Original detailed implementation plan (12 phases)
IDEA.md                          Background research and design notes
```

---

## Translation pipeline (the core logic)

Every Grafana request flows through five stages:

```text
1. HTTP handler (internal/handler/)
       ↓ raw query string
2. Parser (internal/parser/)
       produces: parser.Query (LogQuery or MetricQuery AST)
       ↓ AST
3. Translator (internal/translator/)
       produces: translator.Result{LogsQL, IsMetric, MetricFunc, MetricRange, …}
       ↓ LogsQL string
4. VL Client (internal/vlogs/)
       calls: POST /select/logsql/query  or  POST /select/logsql/hits
       streams: NDJSON records via StreamDecoder
       ↓ vlogs.Record  or  []vlogs.HitBucket
5. Response shaper (internal/loki/)
       produces: StreamsResponse (streams)  or  MatrixResponse (matrix)
```

### LogQL → LogsQL mapping

| LogQL | LogsQL |
| --- | --- |
| `{app="api"}` | `app:="api"` |
| `{app!="api"}` | `NOT app:="api"` |
| `{app=~"api.*"}` | `app:~"api.*"` |
| `{app!~"bot.*"}` | `NOT app:~"bot.*"` |
| `{}` | `*` |
| `\|= "error"` | `_msg:"error"` |
| `!= "error"` | `NOT _msg:"error"` |
| `\|~ "err.*"` | `_msg:~"err.*"` |
| `!~ "err.*"` | `NOT _msg:~"err.*"` |
| `\| json` | _(hint only; not added to query string)_ |
| `count_over_time({…}[5m])` | route to `/hits` endpoint |
| `rate({…}[5m])` | route to `/hits` + divide by step seconds |

Multiple filters join with `AND`. Unsupported constructs (`line_format`,
`topk`, `label_format`, etc.) return `*UnsupportedError` → HTTP 400.

---

## Key design rules (do not violate)

1. **Never buffer log lines in memory.** The NDJSON decoder streams records
   one at a time through `StreamDecoder → StreamGrouper.Add`. Only label
   metadata (small string lists) is cached.

2. **`VLogsClient` is always an interface.** The production `*Client` and any
   mock in tests both satisfy `vlogs.VLogsClient`. Handler code must never
   import `*vlogs.Client` directly.

3. **Auth credentials never enter the ConfigMap.** In Kubernetes, VL
   credentials live in a Secret and are injected as `PROXY_VLOGS_*` env vars
   that override `config.yaml`. The ConfigMap contains only non-sensitive
   fields.

4. **`go 1.25` features are available.** The `net/http.ServeMux` pattern
   syntax is available, but this project currently uses `fasthttp` +
   `github.com/fasthttp/router` for HTTP routing (see `internal/handler/router.go`).
   Note: `fasthttp/router` has a radix-tree bug for the `/loki/api/v1/labels`
   sibling of `/loki/api/v1/label/{name}/values`, so label values are matched
   manually.

5. **Concurrency is controlled by `limits.Limiter`.** Any code that calls VL
   must run inside an `Acquire` / `Release` pair. The middleware applies this
   automatically for HTTP handlers; custom goroutines (e.g. in tests) must do
   it manually.

6. **Config loading order is sacred:**
   defaults → `config.yaml` → `PROXY_*` env vars → validation →
   password file resolution → `GOMEMLIMIT`.
   Never bypass this chain.

7. **Use `log/slog` (stdlib, Go 1.21) for all structured logging.** No
   external logging library. Call `slog.SetDefault(buildLogger(…))` once at
   startup in `main.go`.

---

## Configuration system

Config flows from three sources (later overrides earlier):

```text
1. Built-in defaults (hardcoded in config.defaultRaw())
2. YAML file  (path from --config flag or CONFIG_FILE env var)
3. PROXY_*    environment variables (uppercase, no separators between words)
```

### Environment variable naming

```text
PROXY_SERVER_LISTENADDR
PROXY_VLOGS_URL
PROXY_VLOGS_BEARERTOKEN
PROXY_VLOGS_BASICAUTH_USERNAME
PROXY_VLOGS_BASICAUTH_PASSWORD
PROXY_VLOGS_BASICAUTH_PASSWORDFILE
PROXY_LIMITS_MAXCONCURRENTQUERIES
PROXY_LIMITS_MAXMEMORYMB
PROXY_LABELS_KNOWNLABELS          (comma-separated list)
PROXY_LOG_LEVEL
PROXY_LOG_FORMAT
```

See `.env.example` for the full list with defaults.

---

## VictoriaLogs API endpoints used

| VL endpoint | Used for |
| --- | --- |
| `POST /select/logsql/query` | Log queries (streams response) |
| `POST /select/logsql/hits` | Metric queries (count_over_time / rate) |
| `GET /select/logsql/field_names` | `/loki/api/v1/labels` |
| `GET /select/logsql/field_values` | `/loki/api/v1/label/{name}/values` |

VictoriaLogs returns **NDJSON** from `/query` (one JSON object per line, not a
JSON array). The `StreamDecoder` in `internal/vlogs/stream_decoder.go` handles
this correctly. Never decode the full body as a JSON array.

VL timestamps are `RFC3339Nano` strings (e.g. `"2024-01-15T12:00:00.123Z"`).
Loki expects nanosecond Unix timestamps as decimal strings. Conversion is in
`internal/loki/shaper.go:parseVLTimestamp`.

---

## Testing conventions

- Test files use the `_test` package suffix (e.g. `package parser_test`).
- Handler tests use `httptest.NewServer` + a `mockVL` struct that implements
  `vlogs.VLogsClient`. See `internal/handler/handler_test.go`.
- VL client tests use `httptest.NewServer` to simulate VictoriaLogs.
- Always run tests with `-race`. The CI gate is `go test -race -count=1 ./...`.
- The `shaper_test.go` and `handler_test.go` files are the primary places to
  add new integration-style tests.

---

## Helm chart (`charts/logql-proxy/`)

The chart produces these Kubernetes resources:

| Resource | Always? | Condition |
| --- | --- | --- |
| ServiceAccount | yes | `serviceAccount.create=true` |
| ConfigMap | yes | always (proxy config.yaml) |
| Secret | conditional | `proxy.vlogs.existingSecret` is empty AND a credential is set |
| Deployment | yes | always |
| Service | yes | always |
| Ingress | opt-in | `ingress.enabled=true` |
| HTTPRoute | opt-in | `httpRoute.enabled=true` (needs Gateway API CRDs) |
| HPA | opt-in | `autoscaling.enabled=true` |
| PDB | opt-in | `podDisruptionBudget.enabled=true` |

The Deployment annotation `checksum/config` triggers rolling restarts when
the ConfigMap content changes.

`charts/logql-proxy/custom.values.yaml` is a personal/local override file —
never commit secrets from it to shared branches.

---

## Loki endpoints implemented

| Endpoint | Status | Notes |
| --- | --- | --- |
| `GET /loki/api/v1/query_range` | Full | log + metric queries |
| `GET /loki/api/v1/query` | Full | instant query (delegates to same pipeline) |
| `GET /loki/api/v1/labels` | Full | static list or VL field_names + cache |
| `GET /loki/api/v1/detected_labels` | Full | same VL field_names call; wrapped in detectedLabels shape |
| `GET /loki/api/v1/detected_fields` | Full | field discovery for Logs Drilldown (label/field shape differences) |
| `GET /loki/api/v1/label/{name}/values` | Full | VL field_values + cache |
| `GET /loki/api/v1/series` | Full | samples real records for stream discovery |
| `GET /loki/api/v1/index/stats` | Stub | returns zeros; Grafana only needs 200 OK |
| `GET /loki/api/v1/index/volume` | Full | computes volume by label(s) from sampled records |
| `GET /loki/api/v1/index/volume_range` | Full | same as index/volume (range form) |
| `GET /loki/api/v1/drilldown-limits` | Full | returns Loki-compatible limits JSON |
| `GET /loki/api/v1/patterns` | Full | best-effort patterns endpoint for Logs Drilldown |
| `GET /ready` | Full | health/liveness probe |

---

## Unsupported LogQL constructs (return HTTP 400)

These are permanently out of scope and must return a clear error:

- `line_format "{{.msg}}"` — Loki-specific template rendering
- `label_format` — label renaming
- `topk()`, `bottomk()` — semantics differ too much
- `quantile_over_time()` — VL model mismatch
- Full LogQL arithmetic expressions
- LogQL recording rules

---

## File to edit for common tasks

| Task | File(s) |
| --- | --- |
| Add a new supported LogQL construct | `internal/parser/{ast.go,logql.go}`, `internal/translator/translator.go` |
| Change LogQL → LogsQL mapping | `internal/translator/translator.go` |
| Add a new Loki endpoint | `internal/handler/` (new file) + `cmd/logql-proxy/main.go` (register route) |
| Tune resource limits or defaults | `internal/config/config.go` (`defaultRaw()`) |
| Add a new config field | `internal/config/config.go` (struct + raw + env + convert + validate) |
| Change VL API interaction | `internal/vlogs/client.go` |
| Change response JSON shape | `internal/loki/model.go` + `internal/loki/shaper.go` |
| Add a Helm chart resource | `charts/logql-proxy/templates/` + `charts/logql-proxy/values.yaml` |
