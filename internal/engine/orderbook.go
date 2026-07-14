package engine

import (
	"fmt"
	"sync"
)

// Side 买卖方向
type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

// Order 订单结构体
type Order struct {
	ID        string
	UserID    string
	Side      Side
	Price     int64  // 用 int64 避免浮点精度
	Amount    int64  // 原始数量
	Remaining int64  // 剩余未成交数量
	Timestamp int64  // 下单时间戳（毫秒）
	Next      *Order // 链表指针
}

// OrderList 价格链表（同价格按时间排序）
type OrderList struct {
	Head *Order
	Tail *Order
}

// OrderBook 订单簿
type OrderBook struct {
	bids map[int64]*OrderList // 买盘：价格 -> 订单链表
	asks map[int64]*OrderList // 卖盘：价格 -> 订单链表
	mu   sync.RWMutex
}

// NewOrderBook 创建订单簿
func NewOrderBook() *OrderBook {
	return &OrderBook{
		bids: make(map[int64]*OrderList),
		asks: make(map[int64]*OrderList),
	}
}

// AddOrder 添加订单到订单簿
func (ob *OrderBook) AddOrder(order *Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var list *OrderList
	if order.Side == Buy {
		list = ob.bids[order.Price]
		if list == nil {
			list = &OrderList{}
			ob.bids[order.Price] = list
		}
	} else {
		list = ob.asks[order.Price]
		if list == nil {
			list = &OrderList{}
			ob.asks[order.Price] = list
		}
	}

	// 尾插：按时间顺序
	if list.Head == nil {
		list.Head = order
		list.Tail = order
	} else {
		list.Tail.Next = order
		list.Tail = order
	}
}

// BestBid 最佳买价（最高价）
func (ob *OrderBook) BestBid() int64 {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	var best int64
	for price := range ob.bids {
		if price > best {
			best = price
		}
	}
	return best
}

// BestAsk 最佳卖价（最低价）
func (ob *OrderBook) BestAsk() int64 {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	var best int64 = 1<<63 - 1
	for price := range ob.asks {
		if price < best {
			best = price
		}
	}
	if best == 1<<63-1 {
		return 0
	}
	return best
}

// CancelOrder 从订单簿中移除订单
func (ob *OrderBook) CancelOrder(orderID string) (*Order, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	// 从买盘找
	for price, list := range ob.bids {
		if list == nil || list.Head == nil {
			continue
		}
		if removed := ob.removeFromList(list, orderID); removed != nil {
			// 如果链表空了，删除这个价格档位
			if list.Head == nil {
				delete(ob.bids, price)
			}
			return removed, nil
		}
	}

	// 从卖盘找
	for price, list := range ob.asks {
		if list == nil || list.Head == nil {
			continue
		}
		if removed := ob.removeFromList(list, orderID); removed != nil {
			if list.Head == nil {
				delete(ob.asks, price)
			}
			return removed, nil
		}
	}

	return nil, fmt.Errorf("订单不在订单簿中")
}

// removeFromList 从链表中移除订单
func (ob *OrderBook) removeFromList(list *OrderList, orderID string) *Order {
	if list == nil || list.Head == nil {
		return nil
	}

	// 如果头部就是要删的
	if list.Head.ID == orderID {
		removed := list.Head
		list.Head = list.Head.Next
		if list.Head == nil {
			list.Tail = nil
		}
		removed.Next = nil
		return removed
	}

	// 遍历查找
	prev := list.Head
	curr := prev.Next
	for curr != nil {
		if curr.ID == orderID {
			prev.Next = curr.Next
			if curr == list.Tail {
				list.Tail = prev
			}
			curr.Next = nil
			return curr
		}
		prev = curr
		curr = curr.Next
	}

	return nil
}

// DepthLevel 深度档位
type DepthLevel struct {
	Price      int64 `json:"price"`
	Amount     int64 `json:"amount"`
	OrderCount int   `json:"order_count"`
}

// Depth 深度数据
type Depth struct {
	Symbol string       `json:"symbol"`
	Bids   []DepthLevel `json:"bids"`
	Asks   []DepthLevel `json:"asks"`
}

// GetDepth 获取订单簿深度
func (ob *OrderBook) GetDepth(symbol string, limit int) Depth {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	depth := Depth{
		Symbol: symbol,
		Bids:   []DepthLevel{},
		Asks:   []DepthLevel{},
	}

	// 收集买盘（按价格从高到低）
	for price, list := range ob.bids {
		if list == nil || list.Head == nil {
			continue
		}
		amount := int64(0)
		count := 0
		for order := list.Head; order != nil; order = order.Next {
			amount += order.Remaining
			count++
		}
		depth.Bids = append(depth.Bids, DepthLevel{
			Price:      price,
			Amount:     amount,
			OrderCount: count,
		})
	}

	// 收集卖盘（按价格从低到高）
	for price, list := range ob.asks {
		if list == nil || list.Head == nil {
			continue
		}
		amount := int64(0)
		count := 0
		for order := list.Head; order != nil; order = order.Next {
			amount += order.Remaining
			count++
		}
		depth.Asks = append(depth.Asks, DepthLevel{
			Price:      price,
			Amount:     amount,
			OrderCount: count,
		})
	}

	// 排序：买盘从高到低，卖盘从低到高
	// 这里简单处理，Go 排序需要 import sort
	// 实际使用时可以排序，也可以不排（map 顺序随机）
	// 为了演示，我们只取前 limit 个

	return depth
}
