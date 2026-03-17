// Package config holds the proxy configuration struct and loading logic.
//
// Loading order:
//  1. Defaults are applied.
//  2. YAML file at the given path is parsed (if non-empty).
//  3. PROXY_* environment variables override matching fields.
//  4. The result is validated.
//  5. BasicAuth.PasswordFile is resolved (content read into Password).
//  6. GOMEMLIMIT is set to Limits.MaxMemoryMB * 1 MiB.
package config

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the complete proxy configuration.
type Config struct {
	Server ServerConfig
	VLogs  VLogsConfig
	Limits LimitsConfig
	Labels LabelsConfig
	Log    LogConfig
}

// ServerConfig controls the inbound HTTP server.
type ServerConfig struct {
	ListenAddr      string        // default: ":3100"
	ReadTimeout     time.Duration // default: 30s
	WriteTimeout    time.Duration // default: 60s
	IdleTimeout     time.Duration // default: 90s
	GracefulTimeout time.Duration // default: 15s
}

// VLogsConfig controls outbound connections to VictoriaLogs.
type VLogsConfig struct {
	// URL is the full base URL of VictoriaLogs, e.g. "http://victorialogs:9428".
	URL             string
	Timeout         time.Duration     // per-request context deadline, default: 30s
	MaxIdleConns    int               // default: 100
	MaxConnsPerHost int               // default: 50
	BasicAuth       *BasicAuthConfig  // optional; mutually exclusive with BearerToken
	BearerToken     string            // optional; mutually exclusive with BasicAuth
	ExtraHeaders    map[string]string // forwarded on every VL request
	ExtraParams     map[string]string // appended to every VL query string
}

// BasicAuthConfig holds HTTP Basic authentication credentials.
// PasswordFile takes precedence over Password when both are set.
type BasicAuthConfig struct {
	Username     string
	Password     string // populated from PasswordFile at load time if set
	PasswordFile string // path to a file containing the password
}

// LimitsConfig controls resource guardrails.
type LimitsConfig struct {
	MaxConcurrentQueries  int   // semaphore size, default: 50
	MaxQueueDepth         int   // waiting requests before 429, default: 100
	MaxResponseBodyBytes  int64 // per-request VL body cap, default: 64 MiB
	MaxStreamsPerResponse int   // Loki stream accumulation cap, default: 5000
	MaxMemoryMB           int64 // GOMEMLIMIT target, default: 512
	MaxQueryRangeHours    int   // reject time ranges wider than this, default: 24
	MaxLimit              int   // cap on ?limit= param, default: 5000
	DefaultLimit          int   // default when ?limit= is absent, default: 1000
}

// LabelsConfig controls label discovery and caching.
type LabelsConfig struct {
	// KnownLabels is a static allowlist for /labels. Empty = query VL dynamically.
	KnownLabels []string

	// LabelRemap translates LogQL label names to their VictoriaLogs equivalents
	// before emitting LogsQL. The default mapping is:
	//   detected_level → level
	// (Grafana Logs Drilldown synthesises "detected_level" from log content;
	// VictoriaLogs stores the equivalent information in the "level" field.)
	// Override in config to add or replace entries; set to {} to disable all
	// remapping.
	LabelRemap map[string]string

	MetadataCacheTTL  time.Duration // default: 5m
	MetadataCacheSize int           // max cache entries, default: 256
}

// LogConfig controls structured logging output.
type LogConfig struct {
	Level  string // "debug"|"info"|"warn"|"error", default: "info"
	Format string // "json"|"text", default: "json"
}

// ────────────────────────────────────────────────────────────────────────────
// Internal YAML representation (durations as strings so yaml.v3 can parse them)
// ────────────────────────────────────────────────────────────────────────────

type rawConfig struct {
	Server rawServerConfig `yaml:"server"`
	VLogs  rawVLogsConfig  `yaml:"vlogs"`
	Limits rawLimitsConfig `yaml:"limits"`
	Labels rawLabelsConfig `yaml:"labels"`
	Log    rawLogConfig    `yaml:"log"`
}

type rawServerConfig struct {
	ListenAddr      string `yaml:"listenAddr"`
	ReadTimeout     string `yaml:"readTimeout"`
	WriteTimeout    string `yaml:"writeTimeout"`
	IdleTimeout     string `yaml:"idleTimeout"`
	GracefulTimeout string `yaml:"gracefulTimeout"`
}

type rawVLogsConfig struct {
	URL             string            `yaml:"url"`
	Timeout         string            `yaml:"timeout"`
	MaxIdleConns    int               `yaml:"maxIdleConns"`
	MaxConnsPerHost int               `yaml:"maxConnsPerHost"`
	BasicAuth       *rawBasicAuth     `yaml:"basicAuth"`
	BearerToken     string            `yaml:"bearerToken"`
	ExtraHeaders    map[string]string `yaml:"extraHeaders"`
	ExtraParams     map[string]string `yaml:"extraParams"`
}

type rawBasicAuth struct {
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	PasswordFile string `yaml:"passwordFile"`
}

type rawLimitsConfig struct {
	MaxConcurrentQueries  int   `yaml:"maxConcurrentQueries"`
	MaxQueueDepth         int   `yaml:"maxQueueDepth"`
	MaxResponseBodyBytes  int64 `yaml:"maxResponseBodyBytes"`
	MaxStreamsPerResponse  int   `yaml:"maxStreamsPerResponse"`
	MaxMemoryMB           int64 `yaml:"maxMemoryMB"`
	MaxQueryRangeHours    int   `yaml:"maxQueryRangeHours"`
	MaxLimit              int   `yaml:"maxLimit"`
	DefaultLimit          int   `yaml:"defaultLimit"`
}

type rawLabelsConfig struct {
	KnownLabels       []string          `yaml:"knownLabels"`
	LabelRemap        map[string]string `yaml:"labelRemap"`
	MetadataCacheTTL  string            `yaml:"metadataCacheTTL"`
	MetadataCacheSize int               `yaml:"metadataCacheSize"`
}

type rawLogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// ────────────────────────────────────────────────────────────────────────────
// Defaults
// ────────────────────────────────────────────────────────────────────────────

func defaultRaw() *rawConfig {
	r := &rawConfig{}
	r.Server.ListenAddr = ":3100"
	r.Server.ReadTimeout = "30s"
	r.Server.WriteTimeout = "60s"
	r.Server.IdleTimeout = "90s"
	r.Server.GracefulTimeout = "15s"
	r.VLogs.Timeout = "30s"
	r.VLogs.MaxIdleConns = 100
	r.VLogs.MaxConnsPerHost = 50
	r.Limits.MaxConcurrentQueries = 50
	r.Limits.MaxQueueDepth = 100
	r.Limits.MaxResponseBodyBytes = 64 * 1024 * 1024 // 64 MiB
	r.Limits.MaxStreamsPerResponse = 5000
	r.Limits.MaxMemoryMB = 512
	r.Limits.MaxQueryRangeHours = 24
	r.Limits.MaxLimit = 5000
	r.Limits.DefaultLimit = 1000
	r.Labels.LabelRemap = map[string]string{
		"detected_level": "level",
	}
	r.Labels.MetadataCacheTTL = "5m"
	r.Labels.MetadataCacheSize = 256
	r.Log.Level = "info"
	r.Log.Format = "json"
	return r
}

// ────────────────────────────────────────────────────────────────────────────
// Public API
// ────────────────────────────────────────────────────────────────────────────

// Load builds a Config by applying defaults, reading the YAML file at path
// (skipped when path is ""), overlaying PROXY_* environment variables,
// validating the result, resolving the password file, and setting GOMEMLIMIT.
func Load(path string) (*Config, error) {
	raw := defaultRaw()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, raw); err != nil {
			return nil, fmt.Errorf("parse config file %q: %w", path, err)
		}
	}

	applyEnv(raw)

	cfg, err := convert(raw)
	if err != nil {
		return nil, fmt.Errorf("config conversion: %w", err)
	}

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if err := resolvePasswordFile(cfg); err != nil {
		return nil, err
	}

	debug.SetMemoryLimit(cfg.Limits.MaxMemoryMB * 1024 * 1024)

	return cfg, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Environment variable overlay
// ────────────────────────────────────────────────────────────────────────────

// applyEnv overlays PROXY_* environment variables onto raw.
// Naming convention: PROXY_<SECTION>_<FIELD> (all uppercase, no separators).
func applyEnv(r *rawConfig) {
	// Server
	envStr("PROXY_SERVER_LISTENADDR", &r.Server.ListenAddr)
	envStr("PROXY_SERVER_READTIMEOUT", &r.Server.ReadTimeout)
	envStr("PROXY_SERVER_WRITETIMEOUT", &r.Server.WriteTimeout)
	envStr("PROXY_SERVER_IDLETIMEOUT", &r.Server.IdleTimeout)
	envStr("PROXY_SERVER_GRACEFULTIMEOUT", &r.Server.GracefulTimeout)

	// VLogs
	envStr("PROXY_VLOGS_URL", &r.VLogs.URL)
	envStr("PROXY_VLOGS_TIMEOUT", &r.VLogs.Timeout)
	envInt("PROXY_VLOGS_MAXIDLECONNS", &r.VLogs.MaxIdleConns)
	envInt("PROXY_VLOGS_MAXCONNSPERHOST", &r.VLogs.MaxConnsPerHost)
	envStr("PROXY_VLOGS_BEARERTOKEN", &r.VLogs.BearerToken)

	// BasicAuth — create the sub-struct lazily if any field is set via env
	baUser := os.Getenv("PROXY_VLOGS_BASICAUTH_USERNAME")
	baPass := os.Getenv("PROXY_VLOGS_BASICAUTH_PASSWORD")
	baFile := os.Getenv("PROXY_VLOGS_BASICAUTH_PASSWORDFILE")
	if baUser != "" || baPass != "" || baFile != "" {
		if r.VLogs.BasicAuth == nil {
			r.VLogs.BasicAuth = &rawBasicAuth{}
		}
		if baUser != "" {
			r.VLogs.BasicAuth.Username = baUser
		}
		if baPass != "" {
			r.VLogs.BasicAuth.Password = baPass
		}
		if baFile != "" {
			r.VLogs.BasicAuth.PasswordFile = baFile
		}
	}

	// Limits
	envInt("PROXY_LIMITS_MAXCONCURRENTQUERIES", &r.Limits.MaxConcurrentQueries)
	envInt("PROXY_LIMITS_MAXQUEUEDEPTH", &r.Limits.MaxQueueDepth)
	envInt64("PROXY_LIMITS_MAXRESPONSEBODYBYTES", &r.Limits.MaxResponseBodyBytes)
	envInt("PROXY_LIMITS_MAXSTREAMSPERRESPONSE", &r.Limits.MaxStreamsPerResponse)
	envInt64("PROXY_LIMITS_MAXMEMORYMB", &r.Limits.MaxMemoryMB)
	envInt("PROXY_LIMITS_MAXQUERYRANGEHOURS", &r.Limits.MaxQueryRangeHours)
	envInt("PROXY_LIMITS_MAXLIMIT", &r.Limits.MaxLimit)
	envInt("PROXY_LIMITS_DEFAULTLIMIT", &r.Limits.DefaultLimit)

	// Labels
	if v := os.Getenv("PROXY_LABELS_KNOWNLABELS"); v != "" {
		parts := strings.Split(v, ",")
		labels := parts[:0]
		for _, p := range parts {
			if t := strings.TrimSpace(p); t != "" {
				labels = append(labels, t)
			}
		}
		r.Labels.KnownLabels = labels
	}
	envStr("PROXY_LABELS_METADATACACHETTL", &r.Labels.MetadataCacheTTL)
	envInt("PROXY_LABELS_METADATACACHESIZE", &r.Labels.MetadataCacheSize)

	// Log
	envStr("PROXY_LOG_LEVEL", &r.Log.Level)
	envStr("PROXY_LOG_FORMAT", &r.Log.Format)
}

// ────────────────────────────────────────────────────────────────────────────
// Conversion from raw → typed Config
// ────────────────────────────────────────────────────────────────────────────

func convert(r *rawConfig) (*Config, error) {
	var errs []error

	dur := func(s, field string) time.Duration {
		if s == "" {
			return 0
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: invalid duration %q: %w", field, s, err))
		}
		return d
	}

	cfg := &Config{
		Server: ServerConfig{
			ListenAddr:      r.Server.ListenAddr,
			ReadTimeout:     dur(r.Server.ReadTimeout, "server.readTimeout"),
			WriteTimeout:    dur(r.Server.WriteTimeout, "server.writeTimeout"),
			IdleTimeout:     dur(r.Server.IdleTimeout, "server.idleTimeout"),
			GracefulTimeout: dur(r.Server.GracefulTimeout, "server.gracefulTimeout"),
		},
		VLogs: VLogsConfig{
			URL:             r.VLogs.URL,
			Timeout:         dur(r.VLogs.Timeout, "vlogs.timeout"),
			MaxIdleConns:    r.VLogs.MaxIdleConns,
			MaxConnsPerHost: r.VLogs.MaxConnsPerHost,
			BearerToken:     r.VLogs.BearerToken,
			ExtraHeaders:    r.VLogs.ExtraHeaders,
			ExtraParams:     r.VLogs.ExtraParams,
		},
		Limits: LimitsConfig{
			MaxConcurrentQueries:  r.Limits.MaxConcurrentQueries,
			MaxQueueDepth:         r.Limits.MaxQueueDepth,
			MaxResponseBodyBytes:  r.Limits.MaxResponseBodyBytes,
			MaxStreamsPerResponse: r.Limits.MaxStreamsPerResponse,
			MaxMemoryMB:           r.Limits.MaxMemoryMB,
			MaxQueryRangeHours:    r.Limits.MaxQueryRangeHours,
			MaxLimit:              r.Limits.MaxLimit,
			DefaultLimit:          r.Limits.DefaultLimit,
		},
		Labels: LabelsConfig{
			KnownLabels:       r.Labels.KnownLabels,
			LabelRemap:        r.Labels.LabelRemap,
			MetadataCacheTTL:  dur(r.Labels.MetadataCacheTTL, "labels.metadataCacheTTL"),
			MetadataCacheSize: r.Labels.MetadataCacheSize,
		},
		Log: LogConfig{
			Level:  r.Log.Level,
			Format: r.Log.Format,
		},
	}

	if r.VLogs.BasicAuth != nil {
		cfg.VLogs.BasicAuth = &BasicAuthConfig{
			Username:     r.VLogs.BasicAuth.Username,
			Password:     r.VLogs.BasicAuth.Password,
			PasswordFile: r.VLogs.BasicAuth.PasswordFile,
		}
	}

	return cfg, errors.Join(errs...)
}

// ────────────────────────────────────────────────────────────────────────────
// Validation
// ────────────────────────────────────────────────────────────────────────────

func validate(cfg *Config) error {
	var errs []error

	if cfg.VLogs.URL == "" {
		errs = append(errs, errors.New("vlogs.url is required"))
	}
	if cfg.VLogs.BasicAuth != nil && cfg.VLogs.BearerToken != "" {
		errs = append(errs, errors.New("vlogs.basicAuth and vlogs.bearerToken are mutually exclusive"))
	}
	if cfg.Limits.MaxConcurrentQueries < 1 {
		errs = append(errs, errors.New("limits.maxConcurrentQueries must be >= 1"))
	}
	if cfg.Limits.MaxQueueDepth < 0 {
		errs = append(errs, errors.New("limits.maxQueueDepth must be >= 0"))
	}
	if cfg.Limits.MaxResponseBodyBytes < 1 {
		errs = append(errs, errors.New("limits.maxResponseBodyBytes must be >= 1"))
	}
	if cfg.Limits.MaxStreamsPerResponse < 1 {
		errs = append(errs, errors.New("limits.maxStreamsPerResponse must be >= 1"))
	}
	if cfg.Limits.MaxMemoryMB < 1 {
		errs = append(errs, errors.New("limits.maxMemoryMB must be >= 1"))
	}
	if cfg.Limits.MaxQueryRangeHours < 1 {
		errs = append(errs, errors.New("limits.maxQueryRangeHours must be >= 1"))
	}
	if cfg.Limits.MaxLimit < 1 {
		errs = append(errs, errors.New("limits.maxLimit must be >= 1"))
	}
	if cfg.Limits.DefaultLimit < 1 {
		errs = append(errs, errors.New("limits.defaultLimit must be >= 1"))
	}
	if cfg.Limits.DefaultLimit > cfg.Limits.MaxLimit {
		errs = append(errs, fmt.Errorf(
			"limits.defaultLimit (%d) must be <= limits.maxLimit (%d)",
			cfg.Limits.DefaultLimit, cfg.Limits.MaxLimit,
		))
	}

	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[cfg.Log.Level] {
		errs = append(errs, fmt.Errorf("log.level must be one of debug|info|warn|error, got %q", cfg.Log.Level))
	}
	validFormats := map[string]bool{"json": true, "text": true}
	if !validFormats[cfg.Log.Format] {
		errs = append(errs, fmt.Errorf("log.format must be one of json|text, got %q", cfg.Log.Format))
	}

	return errors.Join(errs...)
}

// ────────────────────────────────────────────────────────────────────────────
// Password file resolution
// ────────────────────────────────────────────────────────────────────────────

// resolvePasswordFile reads the file at BasicAuth.PasswordFile and populates
// BasicAuth.Password, trimming any trailing newline characters.
func resolvePasswordFile(cfg *Config) error {
	if cfg.VLogs.BasicAuth == nil || cfg.VLogs.BasicAuth.PasswordFile == "" {
		return nil
	}
	data, err := os.ReadFile(cfg.VLogs.BasicAuth.PasswordFile)
	if err != nil {
		return fmt.Errorf("read vlogs.basicAuth.passwordFile %q: %w", cfg.VLogs.BasicAuth.PasswordFile, err)
	}
	cfg.VLogs.BasicAuth.Password = strings.TrimRight(string(data), "\r\n")
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Environment variable helpers
// ────────────────────────────────────────────────────────────────────────────

func envStr(name string, dst *string) {
	if v := os.Getenv(name); v != "" {
		*dst = v
	}
}

func envInt(name string, dst *int) {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			*dst = n
		}
	}
}

func envInt64(name string, dst *int64) {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			*dst = n
		}
	}
}
