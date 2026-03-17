// Package handler contains the HTTP handlers for each Loki-compatible endpoint.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/config"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/limits"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// ────────────────────────────────────────────────────────────────────────────
// Dependency container
// ────────────────────────────────────────────────────────────────────────────

// Deps holds the shared dependencies injected into every handler method.
// Constructing it in main and passing it to the mux decouples the handlers
// from global state, making them easy to test with mock implementations.
type Deps struct {
	Cfg   *config.Config
	VL    vlogs.VLogsClient
	Lim   *limits.Limiter
	Cache *vlogs.MetadataCache
}

// ────────────────────────────────────────────────────────────────────────────
// Response helpers
// ────────────────────────────────────────────────────────────────────────────

// writeJSON serialises v as JSON and writes it to ctx with the given HTTP status.
func writeJSON(ctx *fasthttp.RequestCtx, status int, v interface{}) {
	ctx.SetContentType("application/json; charset=utf-8")
	ctx.SetStatusCode(status)
	if err := json.NewEncoder(ctx).Encode(v); err != nil {
		slog.Error("writeJSON encode", "err", err)
	}
}

// writeError sends a Loki-compatible JSON error response.
func writeError(ctx *fasthttp.RequestCtx, status int, errType, msg string) {
	writeJSON(ctx, status, loki.ErrorResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     msg,
	})
}

// ────────────────────────────────────────────────────────────────────────────
// Context helpers
// ────────────────────────────────────────────────────────────────────────────

// reqContext returns the timeout context stored by ConcurrencyMiddleware, or
// falls back to ctx itself (which implements context.Context).
func reqContext(ctx *fasthttp.RequestCtx) context.Context {
	if c, ok := ctx.UserValue("ctx").(context.Context); ok {
		return c
	}
	return ctx
}

// ────────────────────────────────────────────────────────────────────────────
// Middleware
// ────────────────────────────────────────────────────────────────────────────

// RecoveryMiddleware catches panics in downstream handlers and returns HTTP 500,
// preventing a single misbehaving handler from crashing the server.
func RecoveryMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("handler panic", "recover", rec, "path", string(ctx.Path()))
				writeError(ctx, fasthttp.StatusInternalServerError, "server_error", "internal server error")
			}
		}()
		next(ctx)
	}
}

// LoggingMiddleware logs every request with method, path, raw query string,
// status code, and wall-clock latency using the structured slog logger.
// Including the raw query string makes it straightforward to diagnose 4xx
// errors caused by unsupported or malformed LogQL sent by Grafana.
//
// Health-check paths (/ready) are intentionally excluded to avoid filling
// logs with high-frequency probe noise.
func LoggingMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		start := time.Now()
		next(ctx)
		if string(ctx.Path()) == "/ready" {
			return
		}
		slog.Info("request",
			"method", string(ctx.Method()),
			"path", string(ctx.Path()),
			"raw_query", string(ctx.URI().QueryString()),
			"status", ctx.Response.StatusCode(),
			"latency_ms", time.Since(start).Milliseconds(),
			"remote_addr", ctx.RemoteAddr().String(),
		)
	}
}

// ConcurrencyMiddleware wraps the limiter acquisition around every handler.
// Requests that find all slots and the queue full receive HTTP 429.
// The per-request VL timeout is applied to the context stored in the
// "ctx" user value, which handlers retrieve via reqContext().
func ConcurrencyMiddleware(lim *limits.Limiter, requestTimeout time.Duration) func(fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(next fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			if err := lim.Acquire(ctx); err != nil {
				if errors.Is(err, limits.ErrQueueFull) {
					ctx.Response.Header.Set("Retry-After", "1")
					writeError(ctx, fasthttp.StatusTooManyRequests, "execution",
						"too many concurrent queries; try again later")
					return
				}
				writeError(ctx, fasthttp.StatusServiceUnavailable, "cancelled", err.Error())
				return
			}
			defer lim.Release()

			timeoutCtx, cancel := context.WithTimeout(ctx, requestTimeout)
			defer cancel()
			ctx.SetUserValue("ctx", timeoutCtx)

			next(ctx)
		}
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Parameter parsing helpers
// ────────────────────────────────────────────────────────────────────────────

// parseTimeRange extracts and validates the start / end query parameters.
// Accepts Unix nanoseconds, milliseconds, fractional seconds, and RFC3339.
func parseTimeRange(ctx *fasthttp.RequestCtx) (start, end time.Time, err error) {
	startStr := string(ctx.QueryArgs().Peek("start"))
	endStr := string(ctx.QueryArgs().Peek("end"))

	if startStr == "" {
		start = time.Now().Add(-time.Hour)
	} else {
		start, err = parseTime(startStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid start: %w", err)
		}
	}
	if endStr == "" {
		end = time.Now()
	} else {
		end, err = parseTime(endStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end: %w", err)
		}
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("end must be after start")
	}
	return start, end, nil
}

// parseTime parses a time string in one of four formats:
//  1. Unix nanosecond integer  (e.g. "1772722269477000000") — Grafana Drilldown
//  2. Unix millisecond integer (e.g. "1705320000123")
//  3. Unix second float        (e.g. "1705320000.123")       — Prometheus default
//  4. RFC3339(Nano)            (e.g. "2024-01-15T12:00:00.000Z")
//
// Integer strings are parsed with strconv.ParseInt before falling back to
// float to avoid the precision loss that float64 introduces on 19-digit
// nanosecond timestamps. The threshold between second, millisecond, and
// nanosecond representations is based on magnitude:
//
//	> 1e15 → nanoseconds   (divide by 1e9)
//	> 1e12 → milliseconds  (divide by 1e3)
//	else   → seconds
func parseTime(s string) (time.Time, error) {
	// Integer path: handles ns and ms timestamps without float64 precision loss.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		switch {
		case n > 1_000_000_000_000_000: // > 1e15 → nanoseconds
			return time.Unix(n/1_000_000_000, n%1_000_000_000).UTC(), nil
		case n > 1_000_000_000_000: // > 1e12 → milliseconds
			return time.Unix(n/1_000, (n%1_000)*1_000_000).UTC(), nil
		default: // seconds (integer form)
			return time.Unix(n, 0).UTC(), nil
		}
	}
	// Float path: fractional-second timestamps (e.g. "1705320000.123").
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unrecognised time format %q (expected Unix ns/ms/s or RFC3339)", s)
}

// parseDuration parses a duration string in Prometheus/Grafana format.
// It accepts:
//   - A plain number in seconds (e.g. "60" → 1 minute)
//   - A Go duration string (e.g. "1m", "30s", "1h30m")
//
// Falls back to 1 minute on parse failure.
func parseDuration(s string) time.Duration {
	if f, err := strconv.ParseFloat(s, 64); err == nil && f > 0 {
		return time.Duration(f * float64(time.Second))
	}
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	return time.Minute
}
