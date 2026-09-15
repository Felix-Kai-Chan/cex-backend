package engine

import (
	"fmt"
	"time"
)

// Trade 成交记录
type Trade struct {
	TradeID    string
	BuyOrder   string
	SellOrder  string
	BuyUserID  string // ✅ 新增：买方用户 ID
	SellUserID string // ✅ 新增：卖方用户 ID
	Price      int64
	Quantity   int64
	Timestamp  int64
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

		// ✅ 填充买卖双方信息
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
		}
	}

	if order.Remaining > 0 {
		ob.addOrder(order)
	}

	return trades
}

func (ob *OrderBook) getLowestAsk() (int64, *OrderList) {
	node := ob.askPrices.First()
	if node == nil || node.List == nil || node.List.Head == nil {
		return 0, nil
	}
	return node.Price, node.List
}

func (ob *OrderBook) getHighestBid() (int64, *OrderList) {
	node := ob.bidPrices.First()
	if node == nil || node.List == nil || node.List.Head == nil {
		return 0, nil
	}
	return node.Price, node.List
}

func (ob *OrderBook) addOrder(order *Order) {
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

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
