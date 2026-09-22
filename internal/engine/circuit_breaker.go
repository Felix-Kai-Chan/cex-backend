package engine

import (
	"fmt"
	"sync"
	"time"
)

// 市价单保护默认参数
const (
	DefaultMaxMarketAmount int64   = 100  // 单个市价单最大数量
	DefaultMaxSlippagePct  float64 = 0.05 // 最大滑点 5%
)

// CircuitBreaker 单交易对熔断器
type CircuitBreaker struct {
	mu             sync.RWMutex
	lastPrice      int64     // 上一笔成交价
	openUntil      time.Time // 熔断到期时间
	thresholdPct   float64   // 波动阈值
	cooldownSecond int       // 熔断持续秒数

	// 市价单保护
	maxMarketAmount int64   // 单个市价单最大数量
	maxSlippagePct  float64 // 最大滑点（0.05 = 5%）
}

// NewCircuitBreaker 创建熔断器
func NewCircuitBreaker(thresholdPct float64, cooldownSec int) *CircuitBreaker {
	return &CircuitBreaker{
		thresholdPct:    thresholdPct,
		cooldownSecond:  cooldownSec,
		maxMarketAmount: DefaultMaxMarketAmount,
		maxSlippagePct:  DefaultMaxSlippagePct,
	}
}

// IsOpen 当前是否熔断中
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.openUntil.IsZero() {
		return false
	}
	return time.Now().Before(cb.openUntil)
}

// UpdatePrice 更新成交价，检测波动
func (cb *CircuitBreaker) UpdatePrice(price int64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.lastPrice == 0 {
		cb.lastPrice = price
		return
	}

	diff := price - cb.lastPrice
	if diff < 0 {
		diff = -diff
	}
	changePct := float64(diff) / float64(cb.lastPrice)

	cb.lastPrice = price

	if changePct > cb.thresholdPct {
		cb.openUntil = time.Now().Add(time.Duration(cb.cooldownSecond) * time.Second)
	}
}

// Status 返回熔断器状态
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

// CheckMarketAmount 检查单个市价单数量上限
func (cb *CircuitBreaker) CheckMarketAmount(amount int64) error {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if amount > cb.maxMarketAmount {
		return fmt.Errorf("市价单数量 %d 超过上限 %d", amount, cb.maxMarketAmount)
	}
	return nil
}

// CheckSlippage 检查市价单滑点
// referencePrice: 订单进来时的 Best Ask（买）/ Best Bid（卖）
// estimatedAvgPrice: 预估成交均价（通过模拟撮合计算）
// 返回错误表示滑点超阈值
func (cb *CircuitBreaker) CheckSlippage(side Side, referencePrice, estimatedAvgPrice int64) error {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if referencePrice <= 0 {
		return nil // 无法估算，跳过
	}

	diff := estimatedAvgPrice - referencePrice
	if diff < 0 {
		diff = -diff
	}
	slippagePct := float64(diff) / float64(referencePrice)

	if slippagePct > cb.maxSlippagePct {
		return fmt.Errorf("市价单滑点 %.2f%% 超过阈值 %.2f%%（参考价 %d，预估均价 %d）",
			slippagePct*100, cb.maxSlippagePct*100, referencePrice, estimatedAvgPrice)
	}
	return nil
}

// SetMaxMarketAmount 动态设置市价单上限（供测试或管理接口用）
func (cb *CircuitBreaker) SetMaxMarketAmount(amount int64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maxMarketAmount = amount
}

// SetMaxSlippagePct 动态设置滑点阈值
func (cb *CircuitBreaker) SetMaxSlippagePct(pct float64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maxSlippagePct = pct
}
