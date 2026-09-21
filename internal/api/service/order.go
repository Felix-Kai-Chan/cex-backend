package service

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cex-backend/internal/engine"
	"cex-backend/internal/persistence"
	"cex-backend/internal/websocket"
)

func getBaseAsset(symbol string) string {
	parts := strings.Split(symbol, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return symbol
}

func getQuoteAsset(symbol string) string {
	parts := strings.Split(symbol, "/")
	if len(parts) > 1 {
		return parts[1]
	}
	return "USDT"
}

type OrderService struct {
	eng         *engine.Engine
	repo        *persistence.TradeRepo
	ledger      *persistence.LedgerRepo
	hub         *websocket.Hub
	balanceRepo *persistence.BalanceRepo
}

func NewOrderService(
	eng *engine.Engine,
	repo *persistence.TradeRepo,
	ledger *persistence.LedgerRepo,
	hub *websocket.Hub,
	balanceRepo *persistence.BalanceRepo,
) *OrderService {
	return &OrderService{
		eng:         eng,
		repo:        repo,
		ledger:      ledger,
		hub:         hub,
		balanceRepo: balanceRepo,
	}
}

type CreateOrderRequest struct {
	UserID    string `json:"user_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	OrderType string `json:"order_type"`
	Price     int64  `json:"price"`
	Amount    int64  `json:"amount"`
}

type CreateOrderResponse struct {
	OrderID string          `json:"order_id"`
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Trades  []TradeResponse `json:"trades,omitempty"`
}

type TradeResponse struct {
	Price    int64 `json:"price"`
	Quantity int64 `json:"quantity"`
}

func (s *OrderService) CreateOrder(req *CreateOrderRequest) (*CreateOrderResponse, error) {
	if req.UserID == "" {
		return nil, fmt.Errorf("user_id 不能为空")
	}
	if req.Symbol == "" {
		req.Symbol = "BTC/USDT"
	}
	if req.Side != "BUY" && req.Side != "SELL" {
		return nil, fmt.Errorf("side 必须是 BUY 或 SELL")
	}
	if req.OrderType != "MARKET" && req.Price <= 0 {
		return nil, fmt.Errorf("限价单 price 必须大于 0")
	}
	if req.Amount <= 0 {
		return nil, fmt.Errorf("amount 必须大于 0")
	}

	side := engine.Buy
	if req.Side == "SELL" {
		side = engine.Sell
	}

	var price int64
	if req.OrderType == "MARKET" {
		price = 0
	} else {
		price = req.Price
	}

	baseAsset := getBaseAsset(req.Symbol)
	quoteAsset := getQuoteAsset(req.Symbol)

	// 冻结
	var freezeAmount float64
	var freezeAsset string

	if side == engine.Buy {
		freezeAsset = quoteAsset
		if req.OrderType == "MARKET" {
			ob := s.eng.GetOrderBook(req.Symbol)
			bestPrice := ob.BestAsk()
			if bestPrice == 0 {
				bestPrice = ob.BestBid()
			}
			if bestPrice == 0 {
				return nil, fmt.Errorf("订单簿为空，无法估算市价单冻结金额")
			}
			freezeAmount = float64(bestPrice * req.Amount)
		} else {
			freezeAmount = float64(price * req.Amount)
		}
	} else {
		freezeAsset = baseAsset
		freezeAmount = float64(req.Amount)
	}

	if err := s.balanceRepo.FreezeBalance(req.UserID, freezeAsset, freezeAmount); err != nil {
		return nil, fmt.Errorf("冻结余额失败: %w", err)
	}

	order := &engine.Order{
		ID:        fmt.Sprintf("%s_%d", req.UserID, time.Now().UnixNano()),
		UserID:    req.UserID,
		Side:      side,
		Price:     price,
		Amount:    req.Amount,
		Remaining: req.Amount,
		Timestamp: time.Now().UnixMilli(),
	}

	_ = s.repo.SaveOrder(order.ID, order.UserID, req.Symbol, req.Side, order.Price, order.Amount, order.Remaining, "PENDING")

	ob := s.eng.GetOrderBook(req.Symbol)
	trades, changes := ob.Match(order)

	// ✅ 写 WAL：把本次撮合产生的订单变更记录到 Redis
	if snapshotter := s.eng.GetSnapshotter(req.Symbol); snapshotter != nil {
		var walEntries []engine.WALEntry
		for _, ch := range changes {
			walEntries = append(walEntries, engine.WALEntry{
				Op:        ch.Op,
				OrderID:   ch.OrderID,
				UserID:    ch.UserID,
				Side:      ch.Side,
				Price:     ch.Price,
				Remaining: ch.Remaining,
				Timestamp: ch.Timestamp,
			})
		}
		if err := snapshotter.AppendWALBatch(walEntries); err != nil {
			slog.Warn("写 WAL 失败", "order_id", order.ID, "error", err)
		}
	}

	slog.Info("match completed",
		"order_id", order.ID,
		"user_id", order.UserID,
		"symbol", req.Symbol,
		"trades", len(trades),
		"remaining", order.Remaining,
	)

	var tradeResponses []TradeResponse
	for _, trade := range trades {
		tradeResponses = append(tradeResponses, TradeResponse{
			Price:    trade.Price,
			Quantity: trade.Quantity,
		})

		_ = s.repo.SaveTrade(
			trade.TradeID,
			trade.BuyOrder,
			trade.SellOrder,
			trade.Price,
			trade.Quantity,
			trade.Timestamp,
		)

		tradeAmount := float64(trade.Price * trade.Quantity)
		tradeQty := float64(trade.Quantity)

		_ = s.balanceRepo.DeductFrozen(trade.BuyUserID, quoteAsset, tradeAmount)
		_ = s.balanceRepo.AddBalance(trade.BuyUserID, baseAsset, tradeQty)

		_ = s.balanceRepo.DeductFrozen(trade.SellUserID, baseAsset, tradeQty)
		_ = s.balanceRepo.AddBalance(trade.SellUserID, quoteAsset, tradeAmount)

		var buyerOrder persistence.OrderModel
		if err := s.repo.GetOrderByID(trade.BuyOrder, &buyerOrder); err == nil {
			newRemaining := buyerOrder.Remaining - trade.Quantity
			status := "PENDING"
			if newRemaining == 0 {
				status = "FILLED"
			} else if newRemaining < buyerOrder.Amount {
				status = "PARTIAL"
			}
			_ = s.repo.UpdateOrder(trade.BuyOrder, newRemaining, status)
		}

		var sellerOrder persistence.OrderModel
		if err := s.repo.GetOrderByID(trade.SellOrder, &sellerOrder); err == nil {
			newRemaining := sellerOrder.Remaining - trade.Quantity
			status := "PENDING"
			if newRemaining == 0 {
				status = "FILLED"
			} else if newRemaining < sellerOrder.Amount {
				status = "PARTIAL"
			}
			_ = s.repo.UpdateOrder(trade.SellOrder, newRemaining, status)
		}

		_ = s.ledger.Record(
			trade.BuyUserID, quoteAsset, -tradeAmount,
			0, 0, "TRADE", trade.BuyOrder, trade.TradeID,
			fmt.Sprintf("买入 %d 数量，价格 %d", trade.Quantity, trade.Price),
		)
		_ = s.ledger.Record(
			trade.SellUserID, quoteAsset, tradeAmount,
			0, 0, "TRADE", trade.SellOrder, trade.TradeID,
			fmt.Sprintf("卖出 %d 数量，价格 %d", trade.Quantity, trade.Price),
		)
	}

	status := "PENDING"
	if order.Remaining == 0 {
		status = "FILLED"
	} else if order.Remaining < order.Amount {
		status = "PARTIAL"
	}
	_ = s.repo.UpdateOrder(order.ID, order.Remaining, status)

	if order.Remaining > 0 && order.Remaining < order.Amount {
		var unfreezeAmount float64
		var unfreezeAsset string
		if side == engine.Buy {
			unfreezeAsset = quoteAsset
			unfreezeAmount = float64(order.Price * order.Remaining)
		} else {
			unfreezeAsset = baseAsset
			unfreezeAmount = float64(order.Remaining)
		}
		_ = s.balanceRepo.UnfreezeBalance(order.UserID, unfreezeAsset, unfreezeAmount)
	}

	if len(tradeResponses) > 0 {
		s.hub.BroadcastTrade(tradeResponses)
	}

	return &CreateOrderResponse{
		OrderID: order.ID,
		Status:  status,
		Message: fmt.Sprintf("订单已处理，成交 %d 笔", len(trades)),
		Trades:  tradeResponses,
	}, nil
}

func (s *OrderService) GetOrder(orderID string) (*persistence.OrderModel, error) {
	var order persistence.OrderModel
	err := s.repo.GetOrderByID(orderID, &order)
	if err != nil {
		return nil, err
	}
	return &order, nil
}

func (s *OrderService) GetOrdersByUser(userID string) ([]persistence.OrderModel, error) {
	return s.repo.GetOrdersByUser(userID)
}

func (s *OrderService) CancelOrder(orderID, userID string) error {
	var order persistence.OrderModel
	if err := s.repo.GetOrderByID(orderID, &order); err != nil {
		return fmt.Errorf("订单不存在")
	}

	if order.UserID != userID {
		return fmt.Errorf("无权撤销此订单")
	}

	if order.Status == "FILLED" {
		return fmt.Errorf("订单已成交，无法撤销")
	}
	if order.Status == "CANCELLED" {
		return fmt.Errorf("订单已撤销")
	}

	symbol := order.Symbol
	if symbol == "" {
		symbol = "BTC/USDT"
	}

	ob := s.eng.GetOrderBook(symbol)
	_, _ = ob.CancelOrder(orderID)

	if err := s.repo.CancelOrder(orderID); err != nil {
		return err
	}

	// ✅ 写 WAL：撤单也是一次订单变更
	if snapshotter := s.eng.GetSnapshotter(symbol); snapshotter != nil {
		_ = snapshotter.AppendWAL(engine.WALEntry{
			Op:        "REMOVE",
			OrderID:   orderID,
			UserID:    userID,
			Side:      order.Side,
			Price:     order.Price,
			Remaining: 0,
			Timestamp: time.Now().UnixMilli(),
		})
	}

	baseAsset := getBaseAsset(symbol)
	quoteAsset := getQuoteAsset(symbol)

	var unfreezeAmount float64
	var unfreezeAsset string

	if order.Side == "BUY" {
		unfreezeAsset = quoteAsset
		unfreezeAmount = float64(order.Price * order.Remaining)
	} else {
		unfreezeAsset = baseAsset
		unfreezeAmount = float64(order.Remaining)
	}

	if err := s.balanceRepo.UnfreezeBalance(userID, unfreezeAsset, unfreezeAmount); err != nil {
		slog.Warn("unfreeze failed",
			"order_id", orderID,
			"user_id", userID,
			"asset", unfreezeAsset,
			"amount", unfreezeAmount,
			"error", err,
		)
	}

	_ = s.ledger.Record(
		userID, unfreezeAsset, 0, 0, 0,
		"UNFREEZE", orderID, "", "撤单解冻",
	)

	s.hub.BroadcastOrder(map[string]interface{}{
		"order_id": orderID,
		"status":   "CANCELLED",
		"user_id":  userID,
	})

	return nil
}
