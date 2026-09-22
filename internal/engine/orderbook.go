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

// OrderType 订单类型
type OrderType string

const (
	TypeLimit  OrderType = "LIMIT"
	TypeMarket OrderType = "MARKET"
	TypeIOC    OrderType = "IOC"
	TypeFOK    OrderType = "FOK"
)

// Order 订单结构体
type Order struct {
	ID        string
	UserID    string
	Side      Side
	Type      OrderType
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
	Op        string
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
	orders    map[string]*Order // ✅ OrderID → Order 节点索引，撤单 O(1)
	mu        sync.RWMutex
}

// NewOrderBook 创建订单簿
func NewOrderBook() *OrderBook {
	return &OrderBook{
		bids:      make(map[int64]*OrderList),
		asks:      make(map[int64]*OrderList),
		bidPrices: NewSkipList(false),
		askPrices: NewSkipList(true),
		orders:    make(map[string]*Order),
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

	// ✅ 写索引
	ob.orders[order.ID] = order
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

// Match 撮合（支持 LIMIT / MARKET / IOC / FOK）
func (ob *OrderBook) Match(order *Order) ([]Trade, []OrderChange) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var trades []Trade
	var changes []OrderChange

	// FOK 预检查
	if order.Type == TypeFOK {
		if !ob.canFillCompletely(order) {
			return trades, changes
		}
	}

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

			// ✅ 删索引
			delete(ob.orders, matchOrder.ID)

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

	// 当前订单还有剩余 → 判断是否挂单
	if order.Remaining > 0 {
		if order.Type != TypeIOC && order.Type != TypeFOK {
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
	}

	return trades, changes
}

// canFillCompletely 检查对手盘能否完全满足 order（FOK 用）
func (ob *OrderBook) canFillCompletely(order *Order) bool {
	need := order.Remaining

	if order.Side == Buy {
		for _, node := range ob.askPrices.GetAll() {
			if order.Price != 0 && node.Price > order.Price {
				break
			}
			list := node.List
			if list == nil || list.Head == nil {
				continue
			}
			for o := list.Head; o != nil; o = o.Next {
				need -= o.Remaining
				if need <= 0 {
					return true
				}
			}
		}
	} else {
		for _, node := range ob.bidPrices.GetAll() {
			if order.Price != 0 && node.Price < order.Price {
				break
			}
			list := node.List
			if list == nil || list.Head == nil {
				continue
			}
			for o := list.Head; o != nil; o = o.Next {
				need -= o.Remaining
				if need <= 0 {
					return true
				}
			}
		}
	}

	return need <= 0
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

// CancelOrder 从订单簿中移除订单（✅ O(1) 索引定位）
func (ob *OrderBook) CancelOrder(orderID string) (*Order, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	// ✅ O(1) 从索引查
	order, ok := ob.orders[orderID]
	if !ok {
		return nil, fmt.Errorf("订单不在订单簿中")
	}

	// 根据 side 找到对应的价格档
	var list *OrderList
	if order.Side == Buy {
		list = ob.bids[order.Price]
	} else {
		list = ob.asks[order.Price]
	}

	if list == nil || list.Head == nil {
		// 索引有但链表空，说明数据不一致，清理索引
		delete(ob.orders, orderID)
		return nil, fmt.Errorf("订单不在订单簿中")
	}

	// 从链表摘掉
	removed := ob.removeFromList(list, orderID)
	if removed == nil {
		// 链表里没找到，清索引
		delete(ob.orders, orderID)
		return nil, fmt.Errorf("订单不在订单簿中")
	}

	// 价格档空了 → 从 map + 跳表删
	if list.Head == nil {
		if order.Side == Buy {
			delete(ob.bids, order.Price)
			ob.bidPrices.Remove(order.Price)
		} else {
			delete(ob.asks, order.Price)
			ob.askPrices.Remove(order.Price)
		}
	}

	// ✅ 删索引
	delete(ob.orders, orderID)

	return removed, nil
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
