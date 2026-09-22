package engine

import (
	"sort"
	"sync"
	"time"
)

// Metrics 撮合耗时统计（滑窗 + 分位）
type Metrics struct {
	mu         sync.Mutex
	latencies  []time.Duration // 最近 N 次撮合耗时
	maxSamples int             // 滑窗大小
	totalCount int64           // 累计撮合次数
	totalTime  time.Duration   // 累计耗时
}

// NewMetrics 创建 metrics
// maxSamples: 滑窗大小（如 1000，统计最近 1000 次的 P50/P95/P99）
func NewMetrics(maxSamples int) *Metrics {
	if maxSamples <= 0 {
		maxSamples = 1000
	}
	return &Metrics{
		latencies:  make([]time.Duration, 0, maxSamples),
		maxSamples: maxSamples,
	}
}

// Record 记录一次撮合耗时
func (m *Metrics) Record(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalCount++
	m.totalTime += d

	// 滑窗：满了就丢最早的
	if len(m.latencies) >= m.maxSamples {
		// 简单做法：移除第一个元素（切片头部删除 O(n)）
		// 更好的做法是环形缓冲，但为了简单先用这个
		m.latencies = m.latencies[1:]
	}
	m.latencies = append(m.latencies, d)
}

// Snapshot 返回当前快照
type MetricsSnapshot struct {
	Count   int64   `json:"count"`   // 累计撮合次数
	AvgMs   float64 `json:"avg_ms"`  // 平均耗时（毫秒）
	P50Ms   float64 `json:"p50_ms"`  // P50
	P95Ms   float64 `json:"p95_ms"`  // P95
	P99Ms   float64 `json:"p99_ms"`  // P99
	MaxMs   float64 `json:"max_ms"`  // 最大
	Samples int     `json:"samples"` // 当前滑窗样本数
}

// Snapshot 返回统计快照
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap := MetricsSnapshot{
		Count:   m.totalCount,
		Samples: len(m.latencies),
	}

	if m.totalCount > 0 {
		snap.AvgMs = float64(m.totalTime.Microseconds()) / 1000.0 / float64(m.totalCount)
	}

	if len(m.latencies) == 0 {
		return snap
	}

	// 复制一份排序，不影响原数组
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
// sorted 必须已排序
// p: 0.5 / 0.95 / 0.99
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
