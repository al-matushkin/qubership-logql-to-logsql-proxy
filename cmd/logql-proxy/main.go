package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/config"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/handler"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/limits"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

func main() {
	cfgPath := flag.String("config", "",
		"path to config YAML file (default: config.yaml if present, otherwise env vars only)")
	flag.Parse()

	// Resolve config file: explicit flag > CONFIG_FILE env var > config.yaml if present.
	path := *cfgPath
	if path == "" {
		path = os.Getenv("CONFIG_FILE")
	}
	if path == "" {
		if _, err := os.Stat("config.yaml"); err == nil {
			path = "config.yaml"
		}
	}

	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logql-proxy: %v\n", err)
		os.Exit(1)
	}

	slog.SetDefault(buildLogger(cfg.Log.Level, cfg.Log.Format))

	slog.Info("logql-proxy starting",
		"listenAddr", cfg.Server.ListenAddr,
		"vlogsURL", cfg.VLogs.URL,
		"maxConcurrentQueries", cfg.Limits.MaxConcurrentQueries,
		"maxMemoryMB", cfg.Limits.MaxMemoryMB,
	)

	// ── Build dependencies ──────────────────────────────────────────────────

	vlClient := vlogs.NewClient(cfg.VLogs, cfg.Limits.MaxResponseBodyBytes)
	lim := limits.New(cfg.Limits.MaxConcurrentQueries, cfg.Limits.MaxQueueDepth)
	cache := vlogs.NewMetadataCache(cfg.Labels.MetadataCacheSize)

	deps := &handler.Deps{
		Cfg:   cfg,
		VL:    vlClient,
		Lim:   lim,
		Cache: cache,
	}

	// ── Build router ────────────────────────────────────────────────────────

	// ── Apply middleware ────────────────────────────────────────────────────
	//
	// Order (outermost to innermost):
	//   RecoveryMiddleware → LoggingMiddleware → ConcurrencyMiddleware → router
	//
	// RecoveryMiddleware is outermost so panics in the logging wrapper itself
	// are also caught.

	h := handler.RecoveryMiddleware(
		handler.LoggingMiddleware(
			handler.ConcurrencyMiddleware(lim, cfg.VLogs.Timeout)(deps.BuildHandler()),
		),
	)

	// ── Start HTTP server ───────────────────────────────────────────────────

	srv := &fasthttp.Server{
		Handler:      h,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	// Start server in a goroutine so we can listen for shutdown signals.
	serverErr := make(chan error, 1)
	go func() {
		slog.Info("HTTP server listening", "addr", cfg.Server.ListenAddr)
		if err := srv.ListenAndServe(cfg.Server.ListenAddr); err != nil {
			serverErr <- err
		}
	}()

	// ── Graceful shutdown ───────────────────────────────────────────────────

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		slog.Error("server error", "err", err)
		os.Exit(1)
	case sig := <-quit:
		slog.Info("shutdown signal received", "signal", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.GracefulTimeout)
	defer cancel()

	if err := srv.ShutdownWithContext(ctx); err != nil {
		slog.Error("graceful shutdown failed", "err", err)
		os.Exit(1)
	}

	slog.Info("server stopped cleanly")
}

// buildLogger constructs a slog.Logger from the configured level and format.
func buildLogger(level, format string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}
	if format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
