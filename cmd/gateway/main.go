package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"cex-backend/internal/gateway"

	"github.com/redis/go-redis/v9"
)

func main() {
	port := getEnv("GATEWAY_PORT", "8081")
	backendURL := getEnv("BACKEND_URL", "http://localhost:8080")

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	redisDB := getEnvInt("REDIS_DB", 0)

	rate := getEnvInt("RATE_LIMIT_RATE", 50)
	burst := getEnvInt("RATE_LIMIT_BURST", 100)
	failOpen := getEnvBool("RATE_LIMIT_FAIL_OPEN", false)
	localFallback := getEnvBool("RATE_LIMIT_LOCAL_FALLBACK", true)
	trustProxy := getEnvBool("TRUST_PROXY", false)

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPassword,
		DB:           redisDB,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		slog.Warn("redis unavailable, gateway will use configured fallback",
			"addr", redisAddr, "error", err)
	} else {
		slog.Info("redis connected", "addr", redisAddr, "db", redisDB)
	}
	pingCancel()

	limiter := gateway.NewLimiter(rdb, gateway.LimiterConfig{
		Rate:          rate,
		Burst:         burst,
		FailOpen:      failOpen,
		LocalFallback: localFallback,
		KeyPrefix:     "ratelimit:tb:",
	})

	proxy, err := gateway.NewProxy(backendURL)
	if err != nil {
		slog.Error("invalid backend url", "url", backendURL, "error", err)
		os.Exit(1)
	}

	metrics := &gateway.Metrics{}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{
			"total":       metrics.Total.Load(),
			"allowed":     metrics.Allowed.Load(),
			"denied":      metrics.Denied.Load(),
			"unavailable": metrics.Unavailable.Load(),
		})
	})
	mux.Handle("/", proxy)

	handler := gateway.Recovery(
		gateway.RequestID(
			gateway.AccessLog(
				gateway.RateLimit(mux, limiter, trustProxy, []string{"/health", "/metrics"}, metrics),
			),
		),
	)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	go func() {
		slog.Info("CEX Gateway 启动",
			"addr", "http://localhost:"+port,
			"backend", backendURL,
			"rate", rate,
			"burst", burst,
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("gateway server error", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down gateway...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("gateway shutdown error", "error", err)
	}
	_ = rdb.Close()
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		switch value {
		case "1", "true", "TRUE", "yes", "YES", "on", "ON":
			return true
		case "0", "false", "FALSE", "no", "NO", "off", "OFF":
			return false
		}
	}
	return defaultValue
}
