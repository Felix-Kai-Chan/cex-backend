package persistence

import (
	"time"

	"gorm.io/gorm"
)

// OrderModel 订单表（历史记录）
type OrderModel struct {
	ID          uint   `gorm:"primaryKey"`
	OrderID     string `gorm:"uniqueIndex;size:64"`
	UserID      string `gorm:"index;size:64"`
	Symbol      string `gorm:"index;size:32"`
	Side        string `gorm:"size:8"`
	OrderType   string `gorm:"size:16;column:order_type"`   // LIMIT / MARKET
	TimeInForce string `gorm:"size:8;column:time_in_force"` // GTC / IOC / FOK / GTD
	ExpireAt    int64  `gorm:"column:expire_at"`
	Price       int64
	Amount      int64
	Remaining   int64
	Status      string `gorm:"size:20"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (OrderModel) TableName() string {
	return "orders"
}

// TradeModel 成交记录表
type TradeModel struct {
	ID          uint   `gorm:"primaryKey"`
	TradeID     string `gorm:"uniqueIndex;size:64"`
	BuyOrderID  string `gorm:"index;size:64"`
	SellOrderID string `gorm:"index;size:64"`
	Price       int64
	Quantity    int64
	Timestamp   int64
	CreatedAt   time.Time
}

func (TradeModel) TableName() string {
	return "trades"
}

// TradeHistory 用户成交明细（带订单方向/交易对）
type TradeHistory struct {
	TradeID   string `json:"trade_id"`
	OrderID   string `json:"order_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Price     int64  `json:"price"`
	Quantity  int64  `json:"quantity"`
	Timestamp int64  `json:"timestamp"`
}

// TradeRepo 数据访问层
type TradeRepo struct {
	db *gorm.DB
}

// NewTradeRepo 创建数据访问层
func NewTradeRepo(db *gorm.DB) *TradeRepo {
	return &TradeRepo{db: db}
}

// SaveOrder 保存订单
// ✅ 新增 symbol 参数
func (r *TradeRepo) SaveOrder(orderID, userID, symbol, side string, orderType, timeInForce string, price, amount, remaining int64, expireAt int64, status string) error {
	order := OrderModel{
		OrderID:     orderID,
		UserID:      userID,
		Symbol:      symbol,
		Side:        side,
		OrderType:   orderType,
		TimeInForce: timeInForce,
		ExpireAt:    expireAt,
		Price:       price,
		Amount:      amount,
		Remaining:   remaining,
		Status:      status,
	}
	return r.db.Create(&order).Error
}

// UpdateOrder 更新订单状态
func (r *TradeRepo) UpdateOrder(orderID string, remaining int64, status string) error {
	return r.db.Model(&OrderModel{}).
		Where("order_id = ?", orderID).
		Updates(map[string]interface{}{
			"remaining":  remaining,
			"status":     status,
			"updated_at": time.Now(),
		}).Error
}

// UpdateOrderTx 在指定事务内更新订单状态
func (r *TradeRepo) UpdateOrderTx(tx *gorm.DB, orderID string, remaining int64, status string) error {
	return tx.Model(&OrderModel{}).
		Where("order_id = ?", orderID).
		Updates(map[string]interface{}{
			"remaining":  remaining,
			"status":     status,
			"updated_at": time.Now(),
		}).Error
}

// SaveTrade 保存成交记录
func (r *TradeRepo) SaveTrade(tradeID, buyOrderID, sellOrderID string, price, quantity, timestamp int64) error {
	trade := TradeModel{
		TradeID:     tradeID,
		BuyOrderID:  buyOrderID,
		SellOrderID: sellOrderID,
		Price:       price,
		Quantity:    quantity,
		Timestamp:   timestamp,
	}
	return r.db.Create(&trade).Error
}

// SaveTradeTx 在指定事务内保存成交记录
func (r *TradeRepo) SaveTradeTx(tx *gorm.DB, tradeID, buyOrderID, sellOrderID string, price, quantity, timestamp int64) error {
	trade := TradeModel{
		TradeID:     tradeID,
		BuyOrderID:  buyOrderID,
		SellOrderID: sellOrderID,
		Price:       price,
		Quantity:    quantity,
		Timestamp:   timestamp,
	}
	return tx.Create(&trade).Error
}

// GetTradesByOrder 查询某个订单的所有成交记录
func (r *TradeRepo) GetTradesByOrder(orderID string) ([]TradeModel, error) {
	var trades []TradeModel
	err := r.db.Where("buy_order_id = ? OR sell_order_id = ?", orderID, orderID).
		Order("timestamp DESC").
		Find(&trades).Error
	return trades, err
}

// GetOrderByID 根据订单 ID 查询
func (r *TradeRepo) GetOrderByID(orderID string, order *OrderModel) error {
	return r.db.Where("order_id = ?", orderID).First(order).Error
}

// GetOrdersByUser 查询用户的所有订单
func (r *TradeRepo) GetOrdersByUser(userID string) ([]OrderModel, error) {
	var orders []OrderModel
	err := r.db.Where("user_id = ?", userID).Order("created_at DESC").Find(&orders).Error
	return orders, err
}

// CancelOrder 撤销订单（更新状态为 CANCELLED）
func (r *TradeRepo) CancelOrder(orderID string) error {
	return r.db.Model(&OrderModel{}).
		Where("order_id = ? AND status IN ?", orderID, []string{OrderStatusNew, OrderStatusPartiallyFilled}).
		Updates(map[string]interface{}{
			"status":     OrderStatusCanceled,
			"updated_at": time.Now(),
		}).Error
}

// SaveOrderTx 在指定事务内保存订单
func (r *TradeRepo) SaveOrderTx(tx *gorm.DB, orderID, userID, symbol, side string, orderType, timeInForce string, price, amount, remaining int64, expireAt int64, status string) error {
	order := OrderModel{
		OrderID:     orderID,
		UserID:      userID,
		Symbol:      symbol,
		Side:        side,
		OrderType:   orderType,
		TimeInForce: timeInForce,
		ExpireAt:    expireAt,
		Price:       price,
		Amount:      amount,
		Remaining:   remaining,
		Status:      status,
	}
	return tx.Create(&order).Error
}

// GetOrdersByUserAndStatuses 按用户和状态列表查询订单
func (r *TradeRepo) GetOrdersByUserAndStatuses(userID string, statuses []string, limit int) ([]OrderModel, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	query := r.db.Where("user_id = ?", userID)
	if len(statuses) > 0 {
		query = query.Where("status IN ?", statuses)
	}

	var orders []OrderModel
	err := query.Order("created_at DESC").Limit(limit).Find(&orders).Error
	return orders, err
}

// GetTradesByUser 查询用户的成交明细
func (r *TradeRepo) GetTradesByUser(userID string, limit int) ([]TradeHistory, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	var orderIDs []string
	if err := r.db.Model(&OrderModel{}).
		Where("user_id = ?", userID).
		Pluck("order_id", &orderIDs).Error; err != nil {
		return nil, err
	}
	if len(orderIDs) == 0 {
		return []TradeHistory{}, nil
	}

	var trades []TradeModel
	if err := r.db.
		Where("buy_order_id IN ? OR sell_order_id IN ?", orderIDs, orderIDs).
		Order("timestamp DESC").
		Limit(limit).
		Find(&trades).Error; err != nil {
		return nil, err
	}
	if len(trades) == 0 {
		return []TradeHistory{}, nil
	}

	var orders []OrderModel
	if err := r.db.Where("order_id IN ?", orderIDs).Find(&orders).Error; err != nil {
		return nil, err
	}
	orderMap := make(map[string]OrderModel, len(orders))
	for _, o := range orders {
		orderMap[o.OrderID] = o
	}

	result := make([]TradeHistory, 0, len(trades))
	for _, t := range trades {
		order, ok := orderMap[t.BuyOrderID]
		if !ok {
			order, ok = orderMap[t.SellOrderID]
		}
		if !ok {
			continue
		}
		result = append(result, TradeHistory{
			TradeID:   t.TradeID,
			OrderID:   order.OrderID,
			Symbol:    order.Symbol,
			Side:      order.Side,
			Price:     t.Price,
			Quantity:  t.Quantity,
			Timestamp: t.Timestamp,
		})
	}
	return result, nil
}
