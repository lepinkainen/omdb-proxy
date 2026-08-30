// Command omdb-proxy runs a self-hosted caching proxy in front of
// omdbapi.com. See the repository README for the motivation and
// wire-compatibility details; this file is just config loading,
// wiring, and graceful shutdown.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/lepinkainen/omdb-proxy/internal/cache"
	"github.com/lepinkainen/omdb-proxy/internal/proxy"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if err := run(logger); err != nil {
		logger.Error("omdb-proxy exited", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return errors.Wrap(err, "load configuration")
	}

	store, err := cache.Open(cfg.dbPath)
	if err != nil {
		return errors.Wrap(err, "open cache database")
	}
	defer store.Close()

	handler, err := proxy.New(store, proxy.Config{
		UpstreamURL: cfg.upstreamURL,
		APIKey:      cfg.apiKey,
		DailyBudget: cfg.dailyBudget,
		ProxyToken:  cfg.proxyToken,
		NotFoundTTL: cfg.notFoundTTL,
	})
	if err != nil {
		return errors.Wrap(err, "construct proxy handler")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.Healthz)
	mux.HandleFunc("GET /stats", handler.StatsHandler)
	mux.Handle("/", handler)

	server := &http.Server{
		Addr:              cfg.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("omdb-proxy listening", "addr", cfg.addr, "daily_budget", cfg.dailyBudget, "db_path", cfg.dbPath)
		serveErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return errors.Wrap(err, "serve")
		}
		return nil
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return errors.Wrap(err, "graceful shutdown")
		}
		return nil
	}
}

// config holds the fully-resolved environment configuration.
type config struct {
	apiKey      string
	addr        string
	dbPath      string
	dailyBudget int
	upstreamURL string
	proxyToken  string
	notFoundTTL time.Duration
}

// loadConfig reads environment variables, applying defaults. OMDB_API_KEY
// is the only variable required at startup — everything else is safe to
// default. We fail fast on a missing key rather than starting a proxy
// that can never do anything but serve cache hits, which would look
// broken in a confusing way once the cache is empty.
func loadConfig() (config, error) {
	apiKey := os.Getenv("OMDB_API_KEY")
	if apiKey == "" {
		return config{}, errors.New("OMDB_API_KEY is required")
	}

	budget, err := envInt("DAILY_BUDGET", 900)
	if err != nil {
		return config{}, err
	}

	notFoundTTL, err := envDuration("NOTFOUND_TTL", proxy.DefaultNotFoundTTL)
	if err != nil {
		return config{}, err
	}

	return config{
		apiKey:      apiKey,
		addr:        envOr("ADDR", ":8090"),
		dbPath:      envOr("DB_PATH", "/data/cache.db"),
		dailyBudget: budget,
		upstreamURL: envOr("UPSTREAM_URL", "https://www.omdbapi.com"),
		proxyToken:  os.Getenv("PROXY_TOKEN"),
		notFoundTTL: notFoundTTL,
	}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("parse %s as an integer", key))
	}
	return n, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("parse %s as a duration", key))
	}
	return d, nil
}
