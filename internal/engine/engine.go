package engine

import (
	"sync"
)

// Engine 撮合引擎管理器（支持多交易对）
type Engine struct {
	orderBooks   map[string]*OrderBook
	snapshotters map[string]*Snapshotter
	mu           sync.RWMutex
}

// NewEngine 创建引擎
func NewEngine() *Engine {
	return &Engine{
		orderBooks:   make(map[string]*OrderBook),
		snapshotters: make(map[string]*Snapshotter),
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
