package engine

import (
	"sync"
	"time"
)

// CircuitBreaker 单交易对熔断器
// 逻辑：记录最近一笔成交价；每次新成交来，如果相对上一笔波动超过阈值，打开熔断
// 打开后 N 秒内拒绝新单；N 秒后自动闭合
type CircuitBreaker struct {
	mu             sync.RWMutex
	lastPrice      int64     // 上一笔成交价
	openUntil      time.Time // 熔断到期时间；零值表示闭合
	thresholdPct   float64   // 波动阈值（如 0.05 = 5%）
	cooldownSecond int       // 熔断持续秒数
}

// NewCircuitBreaker 创建熔断器
// thresholdPct: 波动阈值（如 0.05 表示 5%）
// cooldownSec: 熔断后冷却秒数
func NewCircuitBreaker(thresholdPct float64, cooldownSec int) *CircuitBreaker {
	return &CircuitBreaker{
		thresholdPct:   thresholdPct,
		cooldownSecond: cooldownSec,
	}
}

// IsOpen 当前是否熔断中（拒绝新单）
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.openUntil.IsZero() {
		return false
	}
	return time.Now().Before(cb.openUntil)
}

// UpdatePrice 更新成交价，检测波动
// 如果相对上一笔波动超过阈值 → 打开熔断
func (cb *CircuitBreaker) UpdatePrice(price int64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.lastPrice == 0 {
		// 第一笔，记录基准
		cb.lastPrice = price
		return
	}

	// 计算波动率
	diff := price - cb.lastPrice
	if diff < 0 {
		diff = -diff
	}
	changePct := float64(diff) / float64(cb.lastPrice)

	// 更新基准价
	cb.lastPrice = price

	// 检查是否超阈值
	if changePct > cb.thresholdPct {
		cb.openUntil = time.Now().Add(time.Duration(cb.cooldownSecond) * time.Second)
	}
}

// Status 返回熔断器状态（用于 /metrics）
func (cb *CircuitBreaker) Status() (isOpen bool, remainSec int, lastPrice int64) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	isOpen = !cb.openUntil.IsZero() && time.Now().Before(cb.openUntil)
	remainSec = 0
	if isOpen {
		remainSec = int(time.Until(cb.openUntil).Seconds())
	}
	lastPrice = cb.lastPrice
	return
}
