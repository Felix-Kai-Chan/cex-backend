package persistence

import (
	"time"

	"gorm.io/gorm"
)

// OrderModel 订单表（历史记录）
type OrderModel struct {
	ID        uint   `gorm:"primaryKey"`
	OrderID   string `gorm:"uniqueIndex;size:64"`
	UserID    string `gorm:"index;size:64"`
	Symbol    string `gorm:"index;size:32"` // ✅ 新增：交易对
	Side      string `gorm:"size:8"`        // BUY / SELL
	Price     int64
	Amount    int64
	Remaining int64
	Status    string `gorm:"size:20"` // PENDING / PARTIAL / FILLED / CANCELLED
	CreatedAt time.Time
	UpdatedAt time.Time
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
func (r *TradeRepo) SaveOrder(orderID, userID, symbol, side string, price, amount, remaining int64, status string) error {
	order := OrderModel{
		OrderID:   orderID,
		UserID:    userID,
		Symbol:    symbol,
		Side:      side,
		Price:     price,
		Amount:    amount,
		Remaining: remaining,
		Status:    status,
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
		Where("order_id = ? AND status IN ?", orderID, []string{"PENDING", "PARTIAL"}).
		Updates(map[string]interface{}{
			"status":     "CANCELLED",
			"updated_at": time.Now(),
		}).Error
}

// SaveOrderTx 在指定事务内保存订单
func (r *TradeRepo) SaveOrderTx(tx *gorm.DB, orderID, userID, symbol, side string, price, amount, remaining int64, status string) error {
	order := OrderModel{
		OrderID:   orderID,
		UserID:    userID,
		Symbol:    symbol,
		Side:      side,
		Price:     price,
		Amount:    amount,
		Remaining: remaining,
		Status:    status,
	}
	return tx.Create(&order).Error
}
