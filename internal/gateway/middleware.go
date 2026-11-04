package gateway

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Metrics struct {
	Total       atomic.Int64
	Allowed     atomic.Int64
	Denied      atomic.Int64
	Unavailable atomic.Int64
}

func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("gateway panic", "panic", rec, "path", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

var requestSeq atomic.Uint64

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = fmt.Sprintf("gw-%d-%d", time.Now().UnixNano(), requestSeq.Add(1))
			r.Header.Set("X-Request-ID", id)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

func AccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)

		slog.Info("gateway request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"latency_ms", time.Since(start).Milliseconds(),
			"client_ip", ClientIP(r, false),
			"request_id", r.Header.Get("X-Request-ID"),
		)
	})
}

func RateLimit(next http.Handler, limiter *Limiter, trustProxy bool, skipPaths []string, metrics *Metrics) http.Handler {
	skip := make(map[string]struct{}, len(skipPaths))
	for _, p := range skipPaths {
		skip[p] = struct{}{}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := skip[r.URL.Path]; ok {
			next.ServeHTTP(w, r)
			return
		}

		ip := ClientIP(r, trustProxy)
		r.Header.Set("X-Real-IP", ip)

		res, err := limiter.Allow(r.Context(), ip)
		if metrics != nil {
			metrics.Total.Add(1)
		}
		if err != nil {
			if metrics != nil {
				metrics.Unavailable.Add(1)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "rate limiter unavailable",
			})
			return
		}

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limiter.cfg.Rate))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))

		if !res.Allowed {
			if metrics != nil {
				metrics.Denied.Add(1)
			}
			retryAfter := int(res.RetryAfter.Seconds())
			if retryAfter < 1 {
				retryAfter = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error":       "rate limit exceeded",
				"retry_after": retryAfter,
			})
			return
		}

		if metrics != nil {
			metrics.Allowed.Add(1)
		}
		next.ServeHTTP(w, r)
	})
}

// ClientIP 获取客户端 IP。trustProxy=true 时信任 X-Forwarded-For 第一跳。
func ClientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if ip := strings.TrimSpace(parts[0]); ip != "" {
				return normalizeIP(ip)
			}
		}
		if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
			return normalizeIP(xrip)
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return normalizeIP(r.RemoteAddr)
	}
	return normalizeIP(host)
}

func normalizeIP(ip string) string {
	if parsed := net.ParseIP(ip); parsed != nil {
		return parsed.String()
	}
	return ip
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	return w.ResponseWriter.Write(b)
}

// Flush 支持 SSE / 流式响应
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	return h.Hijack()
}
