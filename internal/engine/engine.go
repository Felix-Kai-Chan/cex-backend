package engine

import (
	"sync"
)

// Engine 撮合引擎管理器（支持多交易对）
type Engine struct {
	orderBooks   map[string]*OrderBook
	snapshotters map[string]*Snapshotter
	breakers     map[string]*CircuitBreaker // ✅ 每个 symbol 一个熔断器
	metrics      *Metrics                   // ✅ 全局 metrics
	mu           sync.RWMutex
}

// NewEngine 创建引擎
func NewEngine() *Engine {
	return &Engine{
		orderBooks:   make(map[string]*OrderBook),
		snapshotters: make(map[string]*Snapshotter),
		breakers:     make(map[string]*CircuitBreaker),
		metrics:      NewMetrics(1000), // 滑窗 1000
	}
}

// GetOrderBook 获取或创建交易对的订单簿
func (e *Engine) GetOrderBook(symbol string) *OrderBook {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ob, exists := e.orderBooks[symbol]; exists {
		return ob
	}

	ob := NewOrderBook()
	e.orderBooks[symbol] = ob
	return ob
}

// RegisterSnapshotter 注册 Snapshotter
func (e *Engine) RegisterSnapshotter(symbol string, s *Snapshotter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snapshotters[symbol] = s
}

// GetSnapshotter 获取 Snapshotter
func (e *Engine) GetSnapshotter(symbol string) *Snapshotter {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.snapshotters[symbol]
}

// GetBreaker 获取 symbol 的熔断器（不存在则创建）
// 默认：5% 波动触发，冷却 30 秒
func (e *Engine) GetBreaker(symbol string) *CircuitBreaker {
	e.mu.Lock()
	defer e.mu.Unlock()

	if cb, exists := e.breakers[symbol]; exists {
		return cb
	}

	cb := NewCircuitBreaker(0.05, 30)
	e.breakers[symbol] = cb
	return cb
}

// GetMetrics 获取全局 metrics
func (e *Engine) GetMetrics() *Metrics {
	return e.metrics
}

// GetBreakerStatus 获取所有 symbol 的熔断状态（用于 /metrics）
func (e *Engine) GetBreakerStatus() map[string]map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make(map[string]map[string]interface{})
	for symbol, cb := range e.breakers {
		isOpen, remainSec, lastPrice := cb.Status()
		result[symbol] = map[string]interface{}{
			"is_open":    isOpen,
			"remain_sec": remainSec,
			"last_price": lastPrice,
		}
	}
	return result
}

// GetDepth 获取指定交易对的深度
func (e *Engine) GetDepth(symbol string, limit int) Depth {
	ob := e.GetOrderBook(symbol)
	return ob.GetDepth(symbol, limit)
}

// GetAllSymbols 获取所有交易对
func (e *Engine) GetAllSymbols() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	symbols := make([]string, 0, len(e.orderBooks))
	for symbol := range e.orderBooks {
		symbols = append(symbols, symbol)
	}
	return symbols
}
