package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"cex-backend/internal/engine"
	"cex-backend/internal/mq"
	"cex-backend/internal/persistence"
	"cex-backend/internal/websocket"

	"gorm.io/gorm"
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
	db          *gorm.DB
	outboxRepo  *persistence.OutboxRepo
	kafkaTopic  string
}

func NewOrderService(
	eng *engine.Engine,
	repo *persistence.TradeRepo,
	ledger *persistence.LedgerRepo,
	hub *websocket.Hub,
	balanceRepo *persistence.BalanceRepo,
	db *gorm.DB,
	outboxRepo *persistence.OutboxRepo,
) *OrderService {
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "cex.order.events"
	}
	return &OrderService{
		eng:         eng,
		repo:        repo,
		ledger:      ledger,
		hub:         hub,
		balanceRepo: balanceRepo,
		db:          db,
		outboxRepo:  outboxRepo,
		kafkaTopic:  topic,
	}
}

type CreateOrderRequest struct {
	UserID      string `json:"user_id"`
	Symbol      string `json:"symbol"`
	Side        string `json:"side"`
	OrderType   string `json:"order_type"`
	TimeInForce string `json:"time_in_force"`
	ExpireAt    int64  `json:"expire_at"`
	Price       int64  `json:"price"`
	Amount      int64  `json:"amount"`
}

type CreateOrderResponse struct {
	OrderID     string          `json:"order_id"`
	OrderType   string          `json:"order_type"`
	TimeInForce string          `json:"time_in_force"`
	Status      string          `json:"status"`
	Message     string          `json:"message"`
	Trades      []TradeResponse `json:"trades,omitempty"`
}

type TradeResponse struct {
	Price    int64 `json:"price"`
	Quantity int64 `json:"quantity"`
}

func (s *OrderService) CreateOrder(req *CreateOrderRequest) (*CreateOrderResponse, error) {
	// 1. 基础校验
	if req.UserID == "" {
		return nil, fmt.Errorf("user_id 不能为空")
	}
	if req.Symbol == "" {
		req.Symbol = "BTC/USDT"
	}
	if req.Side != "BUY" && req.Side != "SELL" {
		return nil, fmt.Errorf("side 必须是 BUY 或 SELL")
	}
	if req.Amount <= 0 {
		return nil, fmt.Errorf("amount 必须大于 0")
	}

	// 2. 熔断检查
	breaker := s.eng.GetBreaker(req.Symbol)
	if breaker.IsOpen() {
		_, remainSec, lastPrice := breaker.Status()
		slog.Warn("circuit breaker open, reject order",
			"symbol", req.Symbol,
			"remain_sec", remainSec,
			"last_price", lastPrice,
		)
		return nil, fmt.Errorf("交易对 %s 因价格波动过大已熔断，剩余 %d 秒", req.Symbol, remainSec)
	}

	// 3. 订单类型校验 + 兼容映射
	if req.OrderType == "" {
		req.OrderType = "LIMIT"
	}
	if req.TimeInForce == "" {
		// 兼容旧请求：按 order_type 推断 TIF
		switch req.OrderType {
		case "MARKET":
			req.TimeInForce = "IOC"
		case "IOC":
			req.TimeInForce = "IOC"
		case "FOK":
			req.TimeInForce = "FOK"
		default:
			req.TimeInForce = "GTC"
		}
	}

	// 兼容旧 order_type=IOC/FOK
	switch req.OrderType {
	case "IOC", "FOK":
		req.OrderType = "LIMIT"
	}

	var orderType engine.OrderType
	switch req.OrderType {
	case "LIMIT":
		orderType = engine.TypeLimit
	case "MARKET":
		orderType = engine.TypeMarket
	default:
		return nil, fmt.Errorf("order_type 必须是 LIMIT / MARKET")
	}

	var tif engine.TimeInForce
	switch req.TimeInForce {
	case "GTC":
		tif = engine.TIFGTC
	case "IOC":
		tif = engine.TIFIOC
	case "FOK":
		tif = engine.TIFFOK
	case "GTD":
		tif = engine.TIFGTD
	default:
		return nil, fmt.Errorf("time_in_force 必须是 GTC / IOC / FOK / GTD")
	}

	if tif == engine.TIFGTD && req.ExpireAt <= 0 {
		return nil, fmt.Errorf("GTD 订单必须提供 expire_at")
	}

	// 4. 价格校验
	if orderType == engine.TypeMarket {
		if req.Price != 0 {
			req.Price = 0
		}
	} else {
		if req.Price <= 0 {
			return nil, fmt.Errorf("限价/IOC/FOK 单 price 必须大于 0")
		}
	}

	side := engine.Buy
	if req.Side == "SELL" {
		side = engine.Sell
	}

	var price int64
	if orderType == engine.TypeMarket {
		price = 0
	} else {
		price = req.Price
	}

	// ✅ 5. 市价单保护（新增）
	if orderType == engine.TypeMarket {
		// 5.1 数量上限
		if err := breaker.CheckMarketAmount(req.Amount); err != nil {
			slog.Warn("market order rejected by amount check",
				"symbol", req.Symbol,
				"amount", req.Amount,
				"error", err,
			)
			return nil, err
		}

		// 5.2 滑点保护
		ob := s.eng.GetOrderBook(req.Symbol)
		var refPrice int64
		if side == engine.Buy {
			refPrice = ob.BestAsk()
		} else {
			refPrice = ob.BestBid()
		}

		if refPrice > 0 {
			tempOrder := &engine.Order{
				ID:        "estimate",
				UserID:    req.UserID,
				Side:      side,
				Type:      engine.TypeMarket,
				Price:     0,
				Amount:    req.Amount,
				Remaining: req.Amount,
			}
			estAvg, _ := ob.EstimateMatchAvgPrice(tempOrder)
			if estAvg > 0 {
				if err := breaker.CheckSlippage(side, refPrice, estAvg); err != nil {
					slog.Warn("market order rejected by slippage check",
						"symbol", req.Symbol,
						"ref_price", refPrice,
						"est_avg", estAvg,
						"error", err,
					)
					return nil, err
				}
			}
		}
	}

	baseAsset := getBaseAsset(req.Symbol)
	quoteAsset := getQuoteAsset(req.Symbol)

	// 6. 创建订单对象（冻结与落库在同一事务中完成）
	order := &engine.Order{
		ID:        fmt.Sprintf("%s_%d", req.UserID, time.Now().UnixNano()),
		UserID:    req.UserID,
		Side:      side,
		Type:      orderType,
		TIF:       tif,
		ExpireAt:  req.ExpireAt,
		Price:     price,
		Amount:    req.Amount,
		Remaining: req.Amount,
		Timestamp: time.Now().UnixMilli(),
	}

	// 7. 获取 symbol 级交易锁，保证撮合顺序、DB 结算顺序与 WAL 序列号顺序一致。
	// 该锁只按 symbol 独立，不会串行化不同交易对。
	ob := s.eng.GetOrderBook(req.Symbol)
	snapshotter := s.eng.GetSnapshotter(req.Symbol)
	if snapshotter == nil {
		return nil, fmt.Errorf("交易对 %s 的 WAL 未初始化，拒绝下单", req.Symbol)
	}
	ob.LockTrading()
	defer ob.UnlockTrading()

	if err := snapshotter.TradingError(); err != nil {
		return nil, fmt.Errorf("交易对 %s 已暂停交易: %w", req.Symbol, err)
	}

	// 7. 冻结
	var freezeAmount float64
	var freezeAsset string

	if side == engine.Buy {
		freezeAsset = quoteAsset
		if orderType == engine.TypeMarket {
			ob := s.eng.GetOrderBook(req.Symbol)
			tempOrder := &engine.Order{
				ID:        "estimate",
				UserID:    req.UserID,
				Side:      side,
				Type:      engine.TypeMarket,
				Price:     0,
				Amount:    req.Amount,
				Remaining: req.Amount,
			}
			estAvg, _ := ob.EstimateMatchAvgPrice(tempOrder)
			if estAvg <= 0 {
				bestPrice := ob.BestAsk()
				if bestPrice == 0 {
					bestPrice = ob.BestBid()
				}
				if bestPrice == 0 {
					return nil, fmt.Errorf("订单簿为空，无法估算市价单冻结金额")
				}
				estAvg = bestPrice
			}
			freezeAmount = float64(estAvg * req.Amount)
		} else {
			freezeAmount = float64(price * req.Amount)
		}
	} else {
		freezeAsset = baseAsset
		freezeAmount = float64(req.Amount)
	}

	if err := s.freezeAndSaveOrderWithOutbox(order, req, orderType, tif, freezeAsset, freezeAmount); err != nil {
		return nil, fmt.Errorf("订单创建失败: %w", err)
	}

	// 8. 撮合
	matchStart := time.Now()
	trades, changes, rollback := ob.Match(order)
	matchDuration := time.Since(matchStart)

	// 9. 记 metrics + 更新熔断器
	s.eng.GetMetrics().Record(matchDuration)
	for _, trade := range trades {
		breaker.UpdatePrice(trade.Price)
	}

	// 10. WAL 将在 DB 事务提交成功后追加（见下方结算事务之后）

	slog.Info("match completed",
		"order_id", order.ID,
		"user_id", order.UserID,
		"symbol", req.Symbol,
		"order_type", req.OrderType,
		"trades", len(trades),
		"remaining", order.Remaining,
		"match_us", matchDuration.Microseconds(),
	)

	// 11. 事务化结算（内存撮合已完成，此处持久化所有副作用）
	tradeResponses := make([]TradeResponse, 0, len(trades))
	orderSpentQuote := 0.0
	for _, trade := range trades {
		tradeResponses = append(tradeResponses, TradeResponse{
			Price:    trade.Price,
			Quantity: trade.Quantity,
		})
		if trade.BuyOrder == order.ID {
			orderSpentQuote += float64(trade.Price * trade.Quantity)
		}
	}

	status := persistence.OrderStatusNew
	if order.Remaining == 0 {
		status = persistence.OrderStatusFilled
	} else if order.TIF == engine.TIFIOC || order.TIF == engine.TIFFOK {
		status = persistence.OrderStatusCanceled
	} else if order.TIF == engine.TIFGTD && order.ExpireAt > 0 && time.Now().UnixMilli() > order.ExpireAt {
		status = persistence.OrderStatusExpired
	} else if order.Remaining < order.Amount {
		status = persistence.OrderStatusPartiallyFilled
	}

	var settleErr error
	// 纯挂单且无成交、无状态变化时，订单已由 freezeAndSaveOrderWithOutbox 写入，跳过重复结算事务
	if len(trades) > 0 || order.Remaining != order.Amount || status != persistence.OrderStatusNew {
		settleErr = retryDBTransaction(3, func() error {
			tx := s.db.Begin()
			if tx.Error != nil {
				return tx.Error
			}
			defer func() {
				if r := recover(); r != nil {
					tx.Rollback()
					panic(r)
				}
			}()

			for _, trade := range trades {
				if err := s.repo.SaveTradeTx(tx, trade.TradeID, trade.BuyOrder, trade.SellOrder, trade.Price, trade.Quantity, trade.Timestamp); err != nil {
					tx.Rollback()
					return err
				}

				tradeAmount := float64(trade.Price * trade.Quantity)
				tradeQty := float64(trade.Quantity)

				if err := s.balanceRepo.DeductFrozenTx(tx, trade.BuyUserID, quoteAsset, tradeAmount); err != nil {
					tx.Rollback()
					return err
				}
				if err := s.balanceRepo.AddBalanceTx(tx, trade.BuyUserID, baseAsset, tradeQty); err != nil {
					tx.Rollback()
					return err
				}
				if err := s.balanceRepo.DeductFrozenTx(tx, trade.SellUserID, baseAsset, tradeQty); err != nil {
					tx.Rollback()
					return err
				}
				if err := s.balanceRepo.AddBalanceTx(tx, trade.SellUserID, quoteAsset, tradeAmount); err != nil {
					tx.Rollback()
					return err
				}

				// 买家订单状态
				var buyerOrder persistence.OrderModel
				if err := tx.Where("order_id = ?", trade.BuyOrder).First(&buyerOrder).Error; err == nil {
					newRemaining := buyerOrder.Remaining - trade.Quantity
					buyerStatus := persistence.OrderStatusNew
					if newRemaining == 0 {
						buyerStatus = persistence.OrderStatusFilled
					} else if newRemaining < buyerOrder.Amount {
						buyerStatus = persistence.OrderStatusPartiallyFilled
					}
					if err := s.repo.UpdateOrderTx(tx, trade.BuyOrder, newRemaining, buyerStatus); err != nil {
						tx.Rollback()
						return err
					}
				}

				// 卖家订单状态
				var sellerOrder persistence.OrderModel
				if err := tx.Where("order_id = ?", trade.SellOrder).First(&sellerOrder).Error; err == nil {
					newRemaining := sellerOrder.Remaining - trade.Quantity
					sellerStatus := persistence.OrderStatusNew
					if newRemaining == 0 {
						sellerStatus = persistence.OrderStatusFilled
					} else if newRemaining < sellerOrder.Amount {
						sellerStatus = persistence.OrderStatusPartiallyFilled
					}
					if err := s.repo.UpdateOrderTx(tx, trade.SellOrder, newRemaining, sellerStatus); err != nil {
						tx.Rollback()
						return err
					}
				}

				_ = s.ledger.RecordTx(tx, trade.BuyUserID, quoteAsset, -tradeAmount, 0, 0, "TRADE", trade.BuyOrder, trade.TradeID,
					fmt.Sprintf("买入 %d 数量，价格 %d", trade.Quantity, trade.Price))
				_ = s.ledger.RecordTx(tx, trade.SellUserID, quoteAsset, tradeAmount, 0, 0, "TRADE", trade.SellOrder, trade.TradeID,
					fmt.Sprintf("卖出 %d 数量，价格 %d", trade.Quantity, trade.Price))

				// 成交事件 Outbox
				event := mq.OrderEvent{
					EventID:    fmt.Sprintf("evt_%d", time.Now().UnixNano()),
					EventType:  mq.EventTradeFilled,
					OrderID:    trade.BuyOrder,
					UserID:     trade.BuyUserID,
					Symbol:     req.Symbol,
					Side:       "BUY",
					TradeID:    trade.TradeID,
					BuyOrder:   trade.BuyOrder,
					SellOrder:  trade.SellOrder,
					BuyUserID:  trade.BuyUserID,
					SellUserID: trade.SellUserID,
					Price:      trade.Price,
					Amount:     trade.Quantity,
					Status:     "FILLED",
					Timestamp:  trade.Timestamp,
				}
				payload, _ := json.Marshal(event)
				outboxMsg := &persistence.OutboxModel{
					AggregateID: trade.TradeID,
					KafkaKey:    req.Symbol,
					Topic:       s.kafkaTopic,
					Payload:     string(payload),
					Status:      "PENDING",
				}
				if err := s.outboxRepo.Create(tx, outboxMsg); err != nil {
					tx.Rollback()
					return err
				}
			}

			// 当前订单状态
			if err := s.repo.UpdateOrderTx(tx, order.ID, order.Remaining, status); err != nil {
				tx.Rollback()
				return err
			}

			// 解冻未成交部分（事务内）
			if order.Remaining > 0 {
				shouldUnfreeze := false
				if order.TIF == engine.TIFIOC || order.TIF == engine.TIFFOK {
					shouldUnfreeze = true
				} else if orderType == engine.TypeMarket {
					shouldUnfreeze = true
				}

				if shouldUnfreeze {
					var unfreezeAmount float64
					var unfreezeAsset string
					if side == engine.Buy {
						unfreezeAsset = quoteAsset
						unfreezeAmount = freezeAmount - orderSpentQuote
						if unfreezeAmount < 0 {
							unfreezeAmount = 0
						}
					} else {
						unfreezeAsset = baseAsset
						unfreezeAmount = float64(order.Remaining)
					}
					if err := s.balanceRepo.UnfreezeBalanceTx(tx, order.UserID, unfreezeAsset, unfreezeAmount); err != nil {
						tx.Rollback()
						return err
					}
				}
			}

			return tx.Commit().Error
		})
	}

	if settleErr != nil {
		// 逆向恢复内存订单簿
		ob := s.eng.GetOrderBook(req.Symbol)
		ob.RollbackMatch(rollback)

		// DB 清理：标记订单取消、解冻资金
		_ = s.repo.CancelOrder(order.ID)
		_ = s.balanceRepo.UnfreezeBalance(req.UserID, freezeAsset, freezeAmount)

		slog.Error("order settlement failed, rolled back",
			"order_id", order.ID,
			"error", settleErr,
		)
		return nil, fmt.Errorf("订单结算失败: %w", settleErr)
	}

	// 12. DB 提交成功后追加 WAL。WAL 失败时订单簿和 DB 已经发生状态变化，
	// 不能假装成功；标记该 symbol 的 WAL 失败并拒绝后续交易。
	walEntries := engine.WALEntriesFromChanges(req.Symbol, changes)
	if err := snapshotter.AppendWALBatch(walEntries); err != nil {
		return nil, fmt.Errorf("WAL 持久化失败，交易对 %s 已暂停: %w", req.Symbol, err)
	}

	if len(tradeResponses) > 0 {
		s.hub.BroadcastTrade(tradeResponses)
	}

	return &CreateOrderResponse{
		OrderID:     order.ID,
		OrderType:   string(orderType),
		TimeInForce: string(tif),
		Status:      status,
		Message:     fmt.Sprintf("订单已处理，成交 %d 笔", len(trades)),
		Trades:      tradeResponses,
	}, nil
}

func (s *OrderService) freezeAndSaveOrderWithOutbox(order *engine.Order, req *CreateOrderRequest, orderType engine.OrderType, tif engine.TimeInForce, freezeAsset string, freezeAmount float64) error {
	tx := s.db.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()

	if err := s.balanceRepo.FreezeBalanceTx(tx, order.UserID, freezeAsset, freezeAmount); err != nil {
		tx.Rollback()
		return fmt.Errorf("冻结余额失败: %w", err)
	}

	if err := s.repo.SaveOrderTx(tx, order.ID, order.UserID, req.Symbol, req.Side, string(orderType), string(tif), order.Price, order.Amount, order.Remaining, order.ExpireAt, persistence.OrderStatusNew); err != nil {
		tx.Rollback()
		return err
	}

	event := mq.OrderEvent{
		EventID:     fmt.Sprintf("evt_%d", time.Now().UnixNano()),
		EventType:   mq.EventOrderCreated,
		OrderID:     order.ID,
		UserID:      order.UserID,
		Symbol:      req.Symbol,
		Side:        string(order.Side),
		OrderType:   string(orderType),
		TimeInForce: string(tif),
		ExpireAt:    order.ExpireAt,
		Price:       order.Price,
		Amount:      order.Amount,
		Status:      persistence.OrderStatusNew,
		Timestamp:   order.Timestamp,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		tx.Rollback()
		return err
	}

	outboxMsg := &persistence.OutboxModel{
		AggregateID: order.ID,
		KafkaKey:    req.Symbol,
		Topic:       s.kafkaTopic,
		Payload:     string(payload),
		Status:      "PENDING",
	}
	if err := s.outboxRepo.Create(tx, outboxMsg); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
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

func (s *OrderService) GetOrdersByUserAndStatuses(userID string, statuses []string, limit int) ([]persistence.OrderModel, error) {
	return s.repo.GetOrdersByUserAndStatuses(userID, statuses, limit)
}

func (s *OrderService) GetTradesByUser(userID string, limit int) ([]persistence.TradeHistory, error) {
	return s.repo.GetTradesByUser(userID, limit)
}

func (s *OrderService) CancelOrder(orderID, userID string) error {
	// 锁外只做定位，后续所有状态和资金计算必须使用 tradeMu + 行锁内重新读取的订单。
	var located persistence.OrderModel
	if err := s.repo.GetOrderByID(orderID, &located); err != nil {
		return fmt.Errorf("订单不存在")
	}
	if located.UserID != userID {
		return fmt.Errorf("无权撤销此订单")
	}

	symbol := located.Symbol
	if symbol == "" {
		symbol = "BTC/USDT"
	}

	ob := s.eng.GetOrderBook(symbol)
	snapshotter := s.eng.GetSnapshotter(symbol)
	if snapshotter == nil {
		return fmt.Errorf("交易对 %s 的 WAL 未初始化，拒绝撤单", symbol)
	}

	ob.LockTrading()
	defer ob.UnlockTrading()

	if err := snapshotter.TradingError(); err != nil {
		return fmt.Errorf("交易对 %s 已暂停交易: %w", symbol, err)
	}

	tx := s.db.Begin()
	if tx.Error != nil {
		return fmt.Errorf("撤单事务启动失败: %w", tx.Error)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback().Error
		}
	}()

	var order persistence.OrderModel
	if err := s.repo.GetOrderByIDForUpdateTx(tx, orderID, &order); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("订单不存在")
		}
		return fmt.Errorf("读取待撤销订单失败: %w", err)
	}
	if order.UserID != userID {
		return fmt.Errorf("无权撤销此订单")
	}
	if order.Symbol != symbol {
		return fmt.Errorf("订单 symbol 不一致: DB=%s, 锁=%s", order.Symbol, symbol)
	}

	switch order.Status {
	case persistence.OrderStatusCanceled:
		// MySQL 已经撤销但内存仍存在，说明内存订单簿与权威状态不一致。
		// 不能简单当作重复撤单，必须停止该 symbol 的后续交易。
		if ob.OrderExists(orderID) {
			failure := fmt.Errorf("订单 %s 在 MySQL 已撤销但内存订单簿仍存在", orderID)
			snapshotter.MarkSymbolFailure(failure)
			return fmt.Errorf("交易对 %s 进入故障保护: %w", symbol, failure)
		}
		return fmt.Errorf("订单已撤销")
	case persistence.OrderStatusFilled:
		if ob.OrderExists(orderID) {
			failure := fmt.Errorf("订单 %s 在 MySQL 已成交但内存订单簿仍存在", orderID)
			snapshotter.MarkSymbolFailure(failure)
			return fmt.Errorf("交易对 %s 进入故障保护: %w", symbol, failure)
		}
		return fmt.Errorf("订单已成交，无法撤销")
	case persistence.OrderStatusNew, persistence.OrderStatusPartiallyFilled:
	default:
		return fmt.Errorf("订单状态 %s 不支持撤单", order.Status)
	}

	if order.Remaining <= 0 {
		failure := fmt.Errorf("订单 %s 状态为 %s 但剩余数量为 %d", orderID, order.Status, order.Remaining)
		snapshotter.MarkSymbolFailure(failure)
		return fmt.Errorf("交易对 %s 进入故障保护: %w", symbol, failure)
	}

	// 市场单不挂入订单簿，且原始冻结金额没有持久化。
	// 不使用撤单时市场价重新估算，避免错误解冻。
	if order.OrderType == "MARKET" {
		return fmt.Errorf("市场单不在订单簿中，拒绝撤单")
	}

	if !ob.OrderExists(orderID) {
		failure := fmt.Errorf("订单 %s 在 MySQL 状态为 %s 但内存订单簿不存在", orderID, order.Status)
		snapshotter.MarkSymbolFailure(failure)
		return fmt.Errorf("交易对 %s 进入故障保护: %w", symbol, failure)
	}

	quoteAsset := getQuoteAsset(symbol)
	baseAsset := getBaseAsset(symbol)

	var unfreezeAsset string
	var unfreezeAmount float64
	if order.Side == "BUY" {
		unfreezeAsset = quoteAsset
		// BUY LIMIT 的 Amount 是原始下单数量，撮合只修改 Remaining。
		// 剩余冻结 = 原始冻结 - 实际成交金额。
		trades, err := s.repo.GetTradesByOrderTx(tx, orderID)
		if err != nil {
			return fmt.Errorf("查询撤单成交记录失败: %w", err)
		}
		spent := 0.0
		for _, trade := range trades {
			if trade.BuyOrderID == orderID {
				spent += float64(trade.Price * trade.Quantity)
			}
		}
		unfreezeAmount = float64(order.Price*order.Amount) - spent
		if unfreezeAmount < 0 {
			unfreezeAmount = 0
		}
	} else if order.Side == "SELL" {
		unfreezeAsset = baseAsset
		unfreezeAmount = float64(order.Remaining)
	} else {
		return fmt.Errorf("订单方向 %s 不支持撤单", order.Side)
	}

	rows, err := s.repo.CancelOrderTx(tx, orderID)
	if err != nil {
		return fmt.Errorf("撤销订单失败: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("撤销订单失败: 期望更新 1 行，实际更新 %d 行", rows)
	}

	if unfreezeAmount > 0 {
		if err := s.balanceRepo.UnfreezeBalanceTx(tx, order.UserID, unfreezeAsset, unfreezeAmount); err != nil {
			return fmt.Errorf("撤单解冻失败: %w", err)
		}
	}

	if err := s.ledger.RecordTx(
		tx, order.UserID, unfreezeAsset, 0, 0, 0,
		"UNFREEZE", orderID, "", "撤单解冻",
	); err != nil {
		return fmt.Errorf("撤单账务记录失败: %w", err)
	}

	cancelEvent := mq.OrderEvent{
		EventID:     fmt.Sprintf("evt_%d", time.Now().UnixNano()),
		EventType:   mq.EventOrderCancelled,
		OrderID:     orderID,
		UserID:      order.UserID,
		Symbol:      symbol,
		Side:        order.Side,
		OrderType:   order.OrderType,
		TimeInForce: order.TimeInForce,
		ExpireAt:    order.ExpireAt,
		Price:       order.Price,
		Amount:      order.Remaining,
		Status:      persistence.OrderStatusCanceled,
		Timestamp:   time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(cancelEvent)
	if err != nil {
		return fmt.Errorf("撤单事件序列化失败: %w", err)
	}
	if err := s.outboxRepo.Create(tx, &persistence.OutboxModel{
		AggregateID: orderID,
		KafkaKey:    symbol,
		Topic:       s.kafkaTopic,
		Payload:     string(payload),
		Status:      "PENDING",
	}); err != nil {
		return fmt.Errorf("撤单 Outbox 写入失败: %w", err)
	}

	if err := tx.Commit().Error; err != nil {
		failure := fmt.Errorf("撤单事务提交失败: %w", err)
		snapshotter.MarkSymbolFailure(failure)
		return fmt.Errorf("交易对 %s 进入故障保护: %w", symbol, failure)
	}
	committed = true

	// 事务已经提交后才删除内存订单。事务失败时内存完全不变，不需要回滚订单簿。
	if _, err := ob.CancelOrder(orderID); err != nil {
		failure := fmt.Errorf("撤单事务已提交但内存订单删除失败: %w", err)
		snapshotter.MarkSymbolFailure(failure)
		return fmt.Errorf("交易对 %s 进入故障保护: %w", symbol, failure)
	}

	walErr := snapshotter.AppendWAL(engine.WALEntry{
		Op:        engine.WALOpRemove,
		Symbol:    symbol,
		OrderID:   orderID,
		UserID:    order.UserID,
		Side:      order.Side,
		Price:     order.Price,
		Remaining: 0,
		Timestamp: time.Now().UnixMilli(),
	})
	if walErr != nil {
		return fmt.Errorf("撤单已生效，但 WAL 持久化失败，交易对 %s 已暂停: %w", symbol, walErr)
	}

	s.hub.BroadcastOrder(map[string]interface{}{
		"order_id": orderID,
		"status":   persistence.OrderStatusCanceled,
		"user_id":  order.UserID,
	})
	return nil
}

// retryDBTransaction 对可重试的数据库错误进行有限次重试
func retryDBTransaction(attempts int, fn func() error) error {
	if attempts <= 0 {
		attempts = 1
	}
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !isRetryableDBError(err) {
			return err
		}
		time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
	}
	return err
}

func isRetryableDBError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "error 1040") ||
		strings.Contains(msg, "too many connections") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "deadlock") ||
		strings.Contains(msg, "lock wait timeout")
}
