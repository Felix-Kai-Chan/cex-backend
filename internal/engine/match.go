package engine

import (
	"fmt"
	"time"
)

// Trade 成交记录
type Trade struct {
	TradeID   string
	BuyOrder  string
	SellOrder string
	Price     int64
	Quantity  int64
	Timestamp int64
}

// Match 撮合（支持限价单 + 市价单）
func (ob *OrderBook) Match(order *Order) []Trade {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var trades []Trade

	for order.Remaining > 0 {
		var matchPrice int64
		var matchList *OrderList

		if order.Side == Buy {
			// 买单：找最低卖价
			matchPrice, matchList = ob.getLowestAsk()
			if matchList == nil {
				break
			}
			// ✅ 限价单检查价格，市价单（price=0）不检查
			if order.Price != 0 && matchPrice > order.Price {
				break
			}
		} else {
			// 卖单：找最高买价
			matchPrice, matchList = ob.getHighestBid()
			if matchList == nil {
				break
			}
			// ✅ 限价单检查价格，市价单（price=0）不检查
			if order.Price != 0 && matchPrice < order.Price {
				break
			}
		}

		if matchList == nil || matchList.Head == nil {
			break
		}

		// 取对手方第一个订单
		matchOrder := matchList.Head

		// 计算成交量
		qty := min(order.Remaining, matchOrder.Remaining)

		// 生成成交记录
		trade := Trade{
			TradeID:   fmt.Sprintf("trade_%d", time.Now().UnixNano()),
			BuyOrder:  "",
			SellOrder: "",
			Price:     matchPrice,
			Quantity:  qty,
			Timestamp: time.Now().UnixMilli(),
		}

		if order.Side == Buy {
			trade.BuyOrder = order.ID
			trade.SellOrder = matchOrder.ID
		} else {
			trade.BuyOrder = matchOrder.ID
			trade.SellOrder = order.ID
		}

		trades = append(trades, trade)

		// 更新剩余量
		order.Remaining -= qty
		matchOrder.Remaining -= qty

		// 如果对手订单完全成交，移出链表
		if matchOrder.Remaining == 0 {
			matchList.Head = matchOrder.Next
			if matchList.Head == nil {
				matchList.Tail = nil
			}
		}
	}

	// 如果订单还没完全成交，挂入订单簿
	if order.Remaining > 0 {
		ob.addOrder(order)
	}

	return trades
}

// getLowestAsk 获取最低卖价
func (ob *OrderBook) getLowestAsk() (int64, *OrderList) {
	var minPrice int64 = 1<<63 - 1
	var minList *OrderList

	for price, list := range ob.asks {
		if list.Head != nil && price < minPrice {
			minPrice = price
			minList = list
		}
	}

	if minList == nil {
		return 0, nil
	}
	return minPrice, minList
}

// getHighestBid 获取最高买价
func (ob *OrderBook) getHighestBid() (int64, *OrderList) {
	var maxPrice int64
	var maxList *OrderList

	for price, list := range ob.bids {
		if list.Head != nil && price > maxPrice {
			maxPrice = price
			maxList = list
		}
	}

	if maxList == nil {
		return 0, nil
	}
	return maxPrice, maxList
}

// addOrder 挂单到订单簿（内部使用，不加锁）
func (ob *OrderBook) addOrder(order *Order) {
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

	if list.Head == nil {
		list.Head = order
		list.Tail = order
	} else {
		list.Tail.Next = order
		list.Tail = order
	}
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
