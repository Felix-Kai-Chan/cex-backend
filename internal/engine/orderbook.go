package engine

import (
	"fmt"
	"sync"
	"time"
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
	Price     int64
	Amount    int64
	Remaining int64
	Timestamp int64
	Next      *Order
}

// OrderList 价格链表（同价格按时间排序）
type OrderList struct {
	Head *Order
	Tail *Order
}

// OrderChange 订单变更事件（用于写 WAL）
type OrderChange struct {
	Op        string // "ADD" / "REMOVE"
	OrderID   string
	UserID    string
	Side      string
	Price     int64
	Remaining int64
	Timestamp int64
}

// OrderBook 订单簿
type OrderBook struct {
	bids      map[int64]*OrderList
	asks      map[int64]*OrderList
	bidPrices *SkipList
	askPrices *SkipList
	mu        sync.RWMutex
}

// NewOrderBook 创建订单簿
func NewOrderBook() *OrderBook {
	return &OrderBook{
		bids:      make(map[int64]*OrderList),
		asks:      make(map[int64]*OrderList),
		bidPrices: NewSkipList(false),
		askPrices: NewSkipList(true),
	}
}

// AddOrder 添加订单到订单簿
func (ob *OrderBook) AddOrder(order *Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	ob.addOrderLocked(order)
}

// addOrderLocked 内部使用，不加锁
func (ob *OrderBook) addOrderLocked(order *Order) {
	var list *OrderList
	if order.Side == Buy {
		list = ob.bids[order.Price]
		if list == nil {
			list = &OrderList{}
			ob.bids[order.Price] = list
			ob.bidPrices.Insert(order.Price, list)
		}
	} else {
		list = ob.asks[order.Price]
		if list == nil {
			list = &OrderList{}
			ob.asks[order.Price] = list
			ob.askPrices.Insert(order.Price, list)
		}
	}

	if list.Head == nil {
		list.Head = order
		list.Tail = order
	} else {
		list.Tail.Next = order
		list.Tail = order
	}
}

// BestBid 最佳买价
func (ob *OrderBook) BestBid() int64 {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	node := ob.bidPrices.First()
	if node == nil {
		return 0
	}
	return node.Price
}

// BestAsk 最佳卖价
func (ob *OrderBook) BestAsk() int64 {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	node := ob.askPrices.First()
	if node == nil {
		return 0
	}
	return node.Price
}

// Match 撮合（支持限价单 + 市价单）
// 返回：成交列表 + 订单变更列表（用于写 WAL）
func (ob *OrderBook) Match(order *Order) ([]Trade, []OrderChange) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var trades []Trade
	var changes []OrderChange

	for order.Remaining > 0 {
		var matchPrice int64
		var matchList *OrderList

		if order.Side == Buy {
			matchPrice, matchList = ob.getLowestAsk()
			if matchList == nil {
				break
			}
			if order.Price != 0 && matchPrice > order.Price {
				break
			}
		} else {
			matchPrice, matchList = ob.getHighestBid()
			if matchList == nil {
				break
			}
			if order.Price != 0 && matchPrice < order.Price {
				break
			}
		}

		if matchList == nil || matchList.Head == nil {
			break
		}

		matchOrder := matchList.Head
		qty := min(order.Remaining, matchOrder.Remaining)

		trade := Trade{
			TradeID:   fmt.Sprintf("trade_%d", time.Now().UnixNano()),
			Price:     matchPrice,
			Quantity:  qty,
			Timestamp: time.Now().UnixMilli(),
		}

		if order.Side == Buy {
			trade.BuyOrder = order.ID
			trade.BuyUserID = order.UserID
			trade.SellOrder = matchOrder.ID
			trade.SellUserID = matchOrder.UserID
		} else {
			trade.BuyOrder = matchOrder.ID
			trade.BuyUserID = matchOrder.UserID
			trade.SellOrder = order.ID
			trade.SellUserID = order.UserID
		}

		trades = append(trades, trade)

		order.Remaining -= qty
		matchOrder.Remaining -= qty

		// 对手单被吃完 → 从订单簿移除 + 记录 REMOVE
		if matchOrder.Remaining == 0 {
			matchList.Head = matchOrder.Next
			if matchList.Head == nil {
				matchList.Tail = nil
				if order.Side == Buy {
					delete(ob.asks, matchPrice)
					ob.askPrices.Remove(matchPrice)
				} else {
					delete(ob.bids, matchPrice)
					ob.bidPrices.Remove(matchPrice)
				}
			}
			matchOrder.Next = nil

			changes = append(changes, OrderChange{
				Op:        "REMOVE",
				OrderID:   matchOrder.ID,
				UserID:    matchOrder.UserID,
				Side:      string(matchOrder.Side),
				Price:     matchOrder.Price,
				Remaining: 0,
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}

	// 当前订单还有剩余 → 挂入订单簿 + 记录 ADD
	if order.Remaining > 0 {
		ob.addOrderLocked(order)
		changes = append(changes, OrderChange{
			Op:        "ADD",
			OrderID:   order.ID,
			UserID:    order.UserID,
			Side:      string(order.Side),
			Price:     order.Price,
			Remaining: order.Remaining,
			Timestamp: order.Timestamp,
		})
	}

	return trades, changes
}

// getLowestAsk 获取最低卖价（跳表头节点）
func (ob *OrderBook) getLowestAsk() (int64, *OrderList) {
	node := ob.askPrices.First()
	if node == nil || node.List == nil || node.List.Head == nil {
		return 0, nil
	}
	return node.Price, node.List
}

// getHighestBid 获取最高买价（跳表头节点）
func (ob *OrderBook) getHighestBid() (int64, *OrderList) {
	node := ob.bidPrices.First()
	if node == nil || node.List == nil || node.List.Head == nil {
		return 0, nil
	}
	return node.Price, node.List
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
				ob.bidPrices.Remove(price)
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
				ob.askPrices.Remove(price)
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
func (ob *OrderBook) GetDepth(symbol string, limit int) Depth {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	depth := Depth{
		Symbol: symbol,
		Bids:   []DepthLevel{},
		Asks:   []DepthLevel{},
	}

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

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
