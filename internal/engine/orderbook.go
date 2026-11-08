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

	// Deprecated: 兼容旧代码/旧数据，新逻辑请使用 TimeInForce
	TypeIOC OrderType = "IOC"
	TypeFOK OrderType = "FOK"
)

// TimeInForce 时间策略
type TimeInForce string

const (
	TIFGTC TimeInForce = "GTC"
	TIFIOC TimeInForce = "IOC"
	TIFFOK TimeInForce = "FOK"
	TIFGTD TimeInForce = "GTD"
)

// normalizeTIF 兼容推断：旧数据/零值 → 新 TIF
func normalizeTIF(order *Order) {
	if order == nil {
		return
	}
	if order.TIF == "" {
		if order.Type == TypeMarket {
			order.TIF = TIFIOC
		} else {
			order.TIF = TIFGTC
		}
	}
	if order.Type == TypeIOC {
		order.Type = TypeLimit
		order.TIF = TIFIOC
	}
	if order.Type == TypeFOK {
		order.Type = TypeLimit
		order.TIF = TIFFOK
	}
}

// Order 订单结构体
type Order struct {
	ID        string
	UserID    string
	Side      Side
	Type      OrderType
	TIF       TimeInForce
	ExpireAt  int64
	Price     int64
	Amount    int64
	Remaining int64
	Timestamp int64
	Next      *Order
}

// OrderList 价格链表（同价格按时间排序）
type OrderList struct {
	Head   *Order
	Tail   *Order
	Volume int64
	Count  int
}

// OrderChange 订单变更事件（用于写 WAL）
type OrderChange struct {
	Op        string
	OrderID   string
	UserID    string
	Side      string
	Type      OrderType
	TIF       TimeInForce
	ExpireAt  int64
	Price     int64
	Amount    int64
	Remaining int64
	Timestamp int64
}

// OrderBook 订单簿
type OrderBook struct {
	bids      map[int64]*OrderList
	asks      map[int64]*OrderList
	bidPrices *SkipList
	askPrices *SkipList
	orders    map[string]*Order
	mu        sync.RWMutex
	// tradeMu 串行化当前 symbol 从撮合到 WAL 持久化的完整交易操作。
	// 与 mu 分开：mu 只保护订单簿数据结构，tradeMu 保护状态变更与 WAL 顺序。
	tradeMu sync.Mutex
}

// LockTrading 获取当前 symbol 的交易操作锁。
// 调用方必须在同一临界区内完成 Match、DB 结算和 WAL 追加。
func (ob *OrderBook) LockTrading() {
	ob.tradeMu.Lock()
}

// UnlockTrading 释放当前 symbol 的交易操作锁。
func (ob *OrderBook) UnlockTrading() {
	ob.tradeMu.Unlock()
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
	list.Volume += order.Remaining
	list.Count++

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
func (ob *OrderBook) Match(order *Order) ([]Trade, []OrderChange, RollbackData) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	normalizeTIF(order)

	var trades []Trade
	var changes []OrderChange
	rollback := RollbackData{
		AddedOrderIDs: []string{},
		RemovedMakers: []Order{},
		PartialMakers: make(map[string]int64),
	}

	if order.TIF == TIFFOK {
		if !ob.canFillCompletely(order) {
			return trades, changes, rollback
		}
	}

	// GTD 过期：直接不成交
	if order.TIF == TIFGTD && order.ExpireAt > 0 && time.Now().UnixMilli() > order.ExpireAt {
		return trades, changes, rollback
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

		// GTD 过期挂单：摘除并继续（TODO: 上层需同步 DB 状态/解冻）
		if matchOrder.TIF == TIFGTD && matchOrder.ExpireAt > 0 && time.Now().UnixMilli() > matchOrder.ExpireAt {
			matchList.Volume -= matchOrder.Remaining
			matchList.Count--
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
			delete(ob.orders, matchOrder.ID)
			changes = append(changes, OrderChange{
				Op:        WALOpRemove,
				OrderID:   matchOrder.ID,
				UserID:    matchOrder.UserID,
				Side:      string(matchOrder.Side),
				Type:      matchOrder.Type,
				TIF:       matchOrder.TIF,
				ExpireAt:  matchOrder.ExpireAt,
				Price:     matchOrder.Price,
				Remaining: 0,
				Timestamp: time.Now().UnixMilli(),
			})
			continue
		}

		qty := min(order.Remaining, matchOrder.Remaining)

		prevMakerRemaining := matchOrder.Remaining

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
		matchList.Volume -= qty

		if matchOrder.Remaining == 0 {
			matchList.Count--
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

			delete(ob.orders, matchOrder.ID)

			removedMaker := *matchOrder
			removedMaker.Remaining = prevMakerRemaining
			if prev, ok := rollback.PartialMakers[matchOrder.ID]; ok {
				removedMaker.Remaining = prev
			}
			removedMaker.Next = nil
			rollback.RemovedMakers = append(rollback.RemovedMakers, removedMaker)
			delete(rollback.PartialMakers, matchOrder.ID)

			changes = append(changes, OrderChange{
				Op:        WALOpRemove,
				OrderID:   matchOrder.ID,
				UserID:    matchOrder.UserID,
				Side:      string(matchOrder.Side),
				Price:     matchOrder.Price,
				Remaining: 0,
				Timestamp: time.Now().UnixMilli(),
			})
		} else {
			if _, ok := rollback.PartialMakers[matchOrder.ID]; !ok {
				rollback.PartialMakers[matchOrder.ID] = prevMakerRemaining
			}

			// Maker 部分成交：Remaining 已减少但订单仍在订单簿中。
			// 必须记录更新后的 Remaining 绝对值，否则崩溃后只能恢复到成交前的数量。
			changes = append(changes, OrderChange{
				Op:        WALOpUpdateRemaining,
				OrderID:   matchOrder.ID,
				UserID:    matchOrder.UserID,
				Side:      string(matchOrder.Side),
				Type:      matchOrder.Type,
				TIF:       matchOrder.TIF,
				ExpireAt:  matchOrder.ExpireAt,
				Price:     matchOrder.Price,
				Amount:    matchOrder.Amount,
				Remaining: matchOrder.Remaining,
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}

	if order.Remaining > 0 {
		canRest := order.Type == TypeLimit &&
			(order.TIF == TIFGTC || order.TIF == TIFGTD) &&
			!(order.TIF == TIFGTD && order.ExpireAt > 0 && time.Now().UnixMilli() > order.ExpireAt)

		if canRest {
			ob.addOrderLocked(order)
			changes = append(changes, OrderChange{
				Op:        WALOpAdd,
				OrderID:   order.ID,
				UserID:    order.UserID,
				Side:      string(order.Side),
				Type:      order.Type,
				TIF:       order.TIF,
				ExpireAt:  order.ExpireAt,
				Price:     order.Price,
				Amount:    order.Amount,
				Remaining: order.Remaining,
				Timestamp: order.Timestamp,
			})
			rollback.AddedOrderIDs = append(rollback.AddedOrderIDs, order.ID)
		}
	}

	return trades, changes, rollback
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
				if o.TIF == TIFGTD && o.ExpireAt > 0 && time.Now().UnixMilli() > o.ExpireAt {
					continue
				}
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
				if o.TIF == TIFGTD && o.ExpireAt > 0 && time.Now().UnixMilli() > o.ExpireAt {
					continue
				}
				need -= o.Remaining
				if need <= 0 {
					return true
				}
			}
		}
	}

	return need <= 0
}

// EstimateMatchAvgPrice 模拟吃单，返回预估成交均价（不修改订单簿）
// 返回 (均价, 是否能吃满)
func (ob *OrderBook) EstimateMatchAvgPrice(order *Order) (int64, bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	need := order.Remaining
	totalValue := int64(0)
	filled := int64(0)

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
				if o.TIF == TIFGTD && o.ExpireAt > 0 && time.Now().UnixMilli() > o.ExpireAt {
					continue
				}
				take := o.Remaining
				if take > need {
					take = need
				}
				totalValue += node.Price * take
				filled += take
				need -= take
				if need == 0 {
					break
				}
			}
			if need == 0 {
				break
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
				take := o.Remaining
				if take > need {
					take = need
				}
				totalValue += node.Price * take
				filled += take
				need -= take
				if need == 0 {
					break
				}
			}
			if need == 0 {
				break
			}
		}
	}

	if filled == 0 {
		return 0, false
	}
	return totalValue / filled, filled == order.Remaining
}

// getLowestAsk 获取最低卖价
func (ob *OrderBook) getLowestAsk() (int64, *OrderList) {
	node := ob.askPrices.First()
	if node == nil || node.List == nil || node.List.Head == nil {
		return 0, nil
	}
	return node.Price, node.List
}

// getHighestBid 获取最高买价
func (ob *OrderBook) getHighestBid() (int64, *OrderList) {
	node := ob.bidPrices.First()
	if node == nil || node.List == nil || node.List.Head == nil {
		return 0, nil
	}
	return node.Price, node.List
}

// CancelOrder 从订单簿中移除订单（O(1) 索引定位）
func (ob *OrderBook) CancelOrder(orderID string) (*Order, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	order, ok := ob.orders[orderID]
	if !ok {
		return nil, fmt.Errorf("订单不在订单簿中")
	}

	var list *OrderList
	if order.Side == Buy {
		list = ob.bids[order.Price]
	} else {
		list = ob.asks[order.Price]
	}

	if list == nil || list.Head == nil {
		delete(ob.orders, orderID)
		return nil, fmt.Errorf("订单不在订单簿中")
	}

	removed := ob.removeFromList(list, orderID)
	if removed == nil {
		delete(ob.orders, orderID)
		return nil, fmt.Errorf("订单不在订单簿中")
	}

	if list.Head == nil {
		if order.Side == Buy {
			delete(ob.bids, order.Price)
			ob.bidPrices.Remove(order.Price)
		} else {
			delete(ob.asks, order.Price)
			ob.askPrices.Remove(order.Price)
		}
	}

	delete(ob.orders, orderID)

	return removed, nil
}

// RollbackMatch 按 RollbackData 逆向恢复内存订单簿（DB 事务失败时调用）
// 调用方需在事务失败后调用；本方法自行获取写锁。
func (ob *OrderBook) RollbackMatch(rb RollbackData) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	for _, id := range rb.AddedOrderIDs {
		ob.removeOrderLocked(id)
	}

	for _, maker := range rb.RemovedMakers {
		m := maker
		m.Next = nil
		if _, exists := ob.orders[m.ID]; !exists {
			ob.addOrderLocked(&m)
		}
	}

	for id, prevRemaining := range rb.PartialMakers {
		if o, ok := ob.orders[id]; ok {
			old := o.Remaining
			o.Remaining = prevRemaining
			var list *OrderList
			if o.Side == Buy {
				list = ob.bids[o.Price]
			} else {
				list = ob.asks[o.Price]
			}
			if list != nil {
				list.Volume += prevRemaining - old
			}
		}
	}
}

// removeOrderLocked 从订单簿移除订单（不加锁，内部使用）
func (ob *OrderBook) removeOrderLocked(orderID string) {
	order, ok := ob.orders[orderID]
	if !ok {
		return
	}

	var list *OrderList
	if order.Side == Buy {
		list = ob.bids[order.Price]
	} else {
		list = ob.asks[order.Price]
	}

	if list == nil || list.Head == nil {
		delete(ob.orders, orderID)
		return
	}

	removed := ob.removeFromList(list, orderID)
	if removed == nil {
		delete(ob.orders, orderID)
		return
	}

	if list.Head == nil {
		if order.Side == Buy {
			delete(ob.bids, order.Price)
			ob.bidPrices.Remove(order.Price)
		} else {
			delete(ob.asks, order.Price)
			ob.askPrices.Remove(order.Price)
		}
	}

	delete(ob.orders, orderID)
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
		list.Volume -= removed.Remaining
		list.Count--
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
			list.Volume -= curr.Remaining
			list.Count--
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
		depth.Bids = append(depth.Bids, DepthLevel{
			Price:      node.Price,
			Amount:     list.Volume,
			OrderCount: list.Count,
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
		depth.Asks = append(depth.Asks, DepthLevel{
			Price:      node.Price,
			Amount:     list.Volume,
			OrderCount: list.Count,
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
