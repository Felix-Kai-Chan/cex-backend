package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrLimiterUnavailable = errors.New("rate limiter unavailable")

// LimiterConfig 令牌桶配置
type LimiterConfig struct {
	Rate          int    // 每秒补充令牌数
	Burst         int    // 桶容量
	FailOpen      bool   // Redis 异常时是否直接放行
	LocalFallback bool   // Redis 异常时是否切换本地内存限流
	KeyPrefix     string // Redis key 前缀
}

// LimitResult 限流结果
type LimitResult struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration
}

// Limiter Redis 令牌桶 + 本地降级
type Limiter struct {
	rdb    *redis.Client
	script *redis.Script
	cfg    LimiterConfig
	local  *localLimiter
}

func NewLimiter(rdb *redis.Client, cfg LimiterConfig) *Limiter {
	if cfg.Rate <= 0 {
		cfg.Rate = 50
	}
	if cfg.Burst <= 0 {
		cfg.Burst = cfg.Rate
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "ratelimit:tb:"
	}

	return &Limiter{
		rdb:    rdb,
		script: redis.NewScript(tokenBucketLua),
		cfg:    cfg,
		local:  newLocalLimiter(cfg),
	}
}

const tokenBucketLua = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])

local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil then tokens = capacity end
if ts == nil then ts = now end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + (elapsed / 1000.0) * rate)

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
local ttl = math.ceil((capacity / rate) * 1000) + 1000
redis.call('PEXPIRE', key, ttl)

local retry_ms = 0
if allowed == 0 then
    retry_ms = math.ceil(((1 - tokens) / rate) * 1000)
end

return {allowed, math.floor(tokens), retry_ms}
`

func (l *Limiter) Allow(ctx context.Context, ip string) (LimitResult, error) {
	if l.rdb != nil {
		res, err := l.script.Run(ctx, l.rdb, []string{l.cfg.KeyPrefix + ip},
			l.cfg.Rate, l.cfg.Burst).Result()
		if err == nil {
			return parseRedisResult(res), nil
		}
		if l.cfg.LocalFallback {
			return l.local.Allow(ip), nil
		}
		if l.cfg.FailOpen {
			return LimitResult{Allowed: true, Remaining: l.cfg.Burst}, nil
		}
		return LimitResult{}, fmt.Errorf("%w: %v", ErrLimiterUnavailable, err)
	}

	if l.cfg.LocalFallback {
		return l.local.Allow(ip), nil
	}
	if l.cfg.FailOpen {
		return LimitResult{Allowed: true, Remaining: l.cfg.Burst}, nil
	}
	return LimitResult{}, ErrLimiterUnavailable
}

func parseRedisResult(res interface{}) LimitResult {
	values, ok := res.([]interface{})
	if !ok || len(values) < 3 {
		return LimitResult{Allowed: true}
	}

	allowed := toInt64(values[0]) == 1
	remaining := int(toInt64(values[1]))
	retryMs := toInt64(values[2])

	return LimitResult{
		Allowed:    allowed,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryMs) * time.Millisecond,
	}
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case string:
		var out int64
		_, _ = fmt.Sscan(n, &out)
		return out
	default:
		return 0
	}
}

// ---------- 本地降级限流 ----------

type localBucket struct {
	tokens float64
	last   time.Time
}

type localLimiter struct {
	mu          sync.Mutex
	buckets     map[string]*localBucket
	cfg         LimiterConfig
	lastCleanup time.Time
}

func newLocalLimiter(cfg LimiterConfig) *localLimiter {
	return &localLimiter{
		buckets: make(map[string]*localBucket),
		cfg:     cfg,
	}
}

func (l *localLimiter) Allow(ip string) LimitResult {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	b := l.buckets[ip]
	if b == nil {
		b = &localBucket{tokens: float64(l.cfg.Burst), last: now}
		l.buckets[ip] = b
	}

	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = minFloat(float64(l.cfg.Burst), b.tokens+elapsed*float64(l.cfg.Rate))
		b.last = now
	}

	allowed := false
	if b.tokens >= 1 {
		b.tokens--
		allowed = true
	}

	retry := time.Duration(0)
	if !allowed {
		retry = time.Duration(((1 - b.tokens) / float64(l.cfg.Rate)) * float64(time.Second))
	}

	l.cleanupLocked(now)
	return LimitResult{
		Allowed:    allowed,
		Remaining:  int(b.tokens),
		RetryAfter: retry,
	}
}

func (l *localLimiter) cleanupLocked(now time.Time) {
	if len(l.buckets) < 10000 {
		return
	}
	if now.Sub(l.lastCleanup) < time.Minute {
		return
	}
	l.lastCleanup = now
	ttl := time.Duration(float64(l.cfg.Burst)/float64(l.cfg.Rate)*float64(time.Second)) + time.Second
	for ip, b := range l.buckets {
		if now.Sub(b.last) > ttl {
			delete(l.buckets, ip)
		}
	}
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
