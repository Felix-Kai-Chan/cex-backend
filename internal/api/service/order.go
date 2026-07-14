package service

import (
	"fmt"
	"time"

	"cex-backend/internal/engine"
	"cex-backend/internal/persistence"
	"cex-backend/internal/websocket"
)

type OrderService struct {
	eng    *engine.Engine
	repo   *persistence.TradeRepo
	ledger *persistence.LedgerRepo
	hub    *websocket.Hub
}

func NewOrderService(eng *engine.Engine, repo *persistence.TradeRepo, ledger *persistence.LedgerRepo, hub *websocket.Hub) *OrderService {
	return &OrderService{
		eng:    eng,
		repo:   repo,
		ledger: ledger,
		hub:    hub,
	}
}

// CreateOrderRequest 下单请求
type CreateOrderRequest struct {
	UserID    string `json:"user_id"`
	Symbol    string `json:"symbol"`     // BTC/USDT, ETH/USDT 等
	Side      string `json:"side"`       // BUY / SELL
	OrderType string `json:"order_type"` // LIMIT / MARKET
	Price     int64  `json:"price"`      // 限价单必填，市价单忽略
	Amount    int64  `json:"amount"`
}

// CreateOrderResponse 下单响应
type CreateOrderResponse struct {
	OrderID string          `json:"order_id"`
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Trades  []TradeResponse `json:"trades,omitempty"`
}

// TradeResponse 成交响应
type TradeResponse struct {
	Price    int64 `json:"price"`
	Quantity int64 `json:"quantity"`
}

// CreateOrder 下单
func (s *OrderService) CreateOrder(req *CreateOrderRequest) (*CreateOrderResponse, error) {
	// 1. 校验订单
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

	// 2. 确定买卖方向
	side := engine.Buy
	if req.Side == "SELL" {
		side = engine.Sell
	}

	// 3. 市价单 price = 0，限价单用传入的价格
	var price int64
	if req.OrderType == "MARKET" {
		price = 0
	} else {
		price = req.Price
	}

	// 4. 构造订单
	order := &engine.Order{
		ID:        fmt.Sprintf("%s_%d", req.UserID, time.Now().UnixNano()),
		UserID:    req.UserID,
		Side:      side,
		Price:     price,
		Amount:    req.Amount,
		Remaining: req.Amount,
		Timestamp: time.Now().UnixMilli(),
	}

	// 5. 保存订单到数据库（初始状态 PENDING）
	_ = s.repo.SaveOrder(order.ID, order.UserID, req.Side, order.Price, order.Amount, order.Remaining, "PENDING")

	// 6. 获取该交易对的订单簿
	ob := s.eng.GetOrderBook(req.Symbol)

	// 7. 执行撮合
	trades := ob.Match(order)
	fmt.Printf("🔍 撮合结果: %d 笔成交，订单剩余: %d\n", len(trades), order.Remaining)

	// 8. 处理成交结果
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

		_ = s.repo.UpdateOrder(trade.BuyOrder, 0, "FILLED")
		_ = s.repo.UpdateOrder(trade.SellOrder, 0, "FILLED")

		// ✅ 记录资金流水（成交扣款）
		// 注意：这里需要获取用户余额，简化处理
		_ = s.ledger.Record(
			order.UserID,
			"USDT",
			-float64(trade.Price*trade.Quantity)/1e8,
			0, // 余额由 balance_repo 管理，这里只记录流水
			0,
			"TRADE",
			order.ID,
			trade.TradeID,
			fmt.Sprintf("成交 %d 数量，价格 %d", trade.Quantity, trade.Price),
		)
	}

	// 9. 更新当前订单状态
	status := "PENDING"
	if order.Remaining == 0 {
		status = "FILLED"
	} else if order.Remaining < order.Amount {
		status = "PARTIAL"
	}
	_ = s.repo.UpdateOrder(order.ID, order.Remaining, status)

	// 10. WebSocket 广播成交
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

// GetOrder 查询订单
func (s *OrderService) GetOrder(orderID string) (*persistence.OrderModel, error) {
	var order persistence.OrderModel
	err := s.repo.GetOrderByID(orderID, &order)
	if err != nil {
		return nil, err
	}
	return &order, nil
}

// GetOrdersByUser 查询用户的所有订单
func (s *OrderService) GetOrdersByUser(userID string) ([]persistence.OrderModel, error) {
	return s.repo.GetOrdersByUser(userID)
}

// CancelOrder 撤销订单
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

	ob := s.eng.GetOrderBook("BTC/USDT")
	_, err := ob.CancelOrder(orderID)
	if err != nil {
		// 可能已经部分成交，不在簿里了，继续
	}

	if err := s.repo.CancelOrder(orderID); err != nil {
		return err
	}

	// ✅ 记录资金流水（撤单解冻）
	_ = s.ledger.Record(
		userID,
		"USDT",
		0,
		0,
		0,
		"UNFREEZE",
		orderID,
		"",
		"撤单解冻",
	)

	s.hub.BroadcastOrder(map[string]interface{}{
		"order_id": orderID,
		"status":   "CANCELLED",
		"user_id":  userID,
	})

	return nil
}
