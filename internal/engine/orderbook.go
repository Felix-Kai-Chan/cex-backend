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
	bids      map[int64]*OrderList // 买盘：价格 -> 订单链表
	asks      map[int64]*OrderList // 卖盘：价格 -> 订单链表
	bidPrices *SkipList            // ✅ 买盘跳表（降序）
	askPrices *SkipList            // ✅ 卖盘跳表（升序）
	mu        sync.RWMutex
}

// NewOrderBook 创建订单簿
func NewOrderBook() *OrderBook {
	return &OrderBook{
		bids:      make(map[int64]*OrderList),
		asks:      make(map[int64]*OrderList),
		bidPrices: NewSkipList(false), // 降序
		askPrices: NewSkipList(true),  // 升序
	}
}

// AddOrder 添加订单到订单簿
func (ob *OrderBook) AddOrder(order *Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var list *OrderList
	var skip *SkipList

	if order.Side == Buy {
		list = ob.bids[order.Price]
		skip = ob.bidPrices
		if list == nil {
			list = &OrderList{}
			ob.bids[order.Price] = list
			skip.Insert(order.Price, list) // ✅ 插入跳表
		}
	} else {
		list = ob.asks[order.Price]
		skip = ob.askPrices
		if list == nil {
			list = &OrderList{}
			ob.asks[order.Price] = list
			skip.Insert(order.Price, list) // ✅ 插入跳表
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
// ✅ 改从跳表读取，O(1)（跳表头节点就是最优价）
func (ob *OrderBook) BestBid() int64 {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	node := ob.bidPrices.First()
	if node == nil {
		return 0
	}
	return node.Price
}

// BestAsk 最佳卖价（最低价）
// ✅ 改从跳表读取，O(1)（跳表头节点就是最优价）
func (ob *OrderBook) BestAsk() int64 {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	node := ob.askPrices.First()
	if node == nil {
		return 0
	}
	return node.Price
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
			if list.Head == nil {
				delete(ob.bids, price)
				ob.bidPrices.Remove(price) // ✅ 从跳表移除
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
				ob.askPrices.Remove(price) // ✅ 从跳表移除
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

	if list.Head.ID == orderID {
		removed := list.Head
		list.Head = list.Head.Next
		if list.Head == nil {
			list.Tail = nil
		}
		removed.Next = nil
		return removed
	}

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
// ✅ 改从跳表读取，已有序，无需排序
func (ob *OrderBook) GetDepth(symbol string, limit int) Depth {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	depth := Depth{
		Symbol: symbol,
		Bids:   []DepthLevel{},
		Asks:   []DepthLevel{},
	}

	// ✅ 买盘从跳表读取（已按价格降序）
	for _, node := range ob.bidPrices.GetAll() {
		if limit > 0 && len(depth.Bids) >= limit {
			break
		}
		list := node.List
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
			Price:      node.Price,
			Amount:     amount,
			OrderCount: count,
		})
	}

	// ✅ 卖盘从跳表读取（已按价格升序）
	for _, node := range ob.askPrices.GetAll() {
		if limit > 0 && len(depth.Asks) >= limit {
			break
		}
		list := node.List
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
			Price:      node.Price,
			Amount:     amount,
			OrderCount: count,
		})
	}

	return depth
}
