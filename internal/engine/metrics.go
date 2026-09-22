package engine

import (
	"sort"
	"sync"
	"time"
)

// Metrics 撮合耗时统计（滑窗 + 分位 + TPS）
type Metrics struct {
	mu         sync.Mutex
	latencies  []time.Duration // 最近 N 次撮合耗时
	maxSamples int             // 滑窗大小
	totalCount int64           // 累计撮合次数
	totalTime  time.Duration   // 累计耗时

	// TPS 统计
	timestamps   []time.Time // 最近撮合的时间戳
	tpsWindowSec int         // TPS 窗口（秒）
}

// NewMetrics 创建 metrics
// maxSamples: 滑窗大小（如 1000，统计最近 1000 次的 P50/P95/P99）
func NewMetrics(maxSamples int) *Metrics {
	if maxSamples <= 0 {
		maxSamples = 1000
	}
	return &Metrics{
		latencies:    make([]time.Duration, 0, maxSamples),
		maxSamples:   maxSamples,
		timestamps:   make([]time.Time, 0, 10000),
		tpsWindowSec: 10, // 默认 10 秒窗口
	}
}

// Record 记录一次撮合耗时
func (m *Metrics) Record(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.totalCount++
	m.totalTime += d

	// 滑窗
	if len(m.latencies) >= m.maxSamples {
		m.latencies = m.latencies[1:]
	}
	m.latencies = append(m.latencies, d)

	// TPS：记录时间戳 + 清理窗口外
	m.timestamps = append(m.timestamps, now)
	m.cleanupTimestamps(now)
}

// cleanupTimestamps 清理 TPS 窗口外的时间戳（调用方必须持有锁）
func (m *Metrics) cleanupTimestamps(now time.Time) {
	if m.tpsWindowSec <= 0 {
		return
	}
	cutoff := now.Add(-time.Duration(m.tpsWindowSec) * time.Second)

	idx := 0
	for i, t := range m.timestamps {
		if t.After(cutoff) {
			idx = i
			break
		}
		idx = i + 1
	}

	if idx > 0 {
		m.timestamps = m.timestamps[idx:]
	}
}

// MetricsSnapshot 返回当前快照
type MetricsSnapshot struct {
	Count   int64   `json:"count"`
	AvgMs   float64 `json:"avg_ms"`
	P50Ms   float64 `json:"p50_ms"`
	P95Ms   float64 `json:"p95_ms"`
	P99Ms   float64 `json:"p99_ms"`
	MaxMs   float64 `json:"max_ms"`
	Samples int     `json:"samples"`

	// 吞吐
	TPS       float64 `json:"tps"`
	TPSWindow int     `json:"tps_window_s"`
	TPSRecent int     `json:"tps_recent"`
}

// Snapshot 返回统计快照
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.cleanupTimestamps(now)

	snap := MetricsSnapshot{
		Count:     m.totalCount,
		Samples:   len(m.latencies),
		TPSWindow: m.tpsWindowSec,
		TPSRecent: len(m.timestamps),
	}

	if m.tpsWindowSec > 0 {
		snap.TPS = float64(len(m.timestamps)) / float64(m.tpsWindowSec)
	}

	if m.totalCount > 0 {
		snap.AvgMs = float64(m.totalTime.Microseconds()) / 1000.0 / float64(m.totalCount)
	}

	if len(m.latencies) == 0 {
		return snap
	}

	sorted := make([]time.Duration, len(m.latencies))
	copy(sorted, m.latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	snap.P50Ms = percentileMs(sorted, 0.50)
	snap.P95Ms = percentileMs(sorted, 0.95)
	snap.P99Ms = percentileMs(sorted, 0.99)
	snap.MaxMs = float64(sorted[len(sorted)-1].Microseconds()) / 1000.0

	return snap
}

// percentileMs 计算分位耗时（毫秒）
func percentileMs(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx].Microseconds()) / 1000.0
}
