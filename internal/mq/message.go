package mq

// OrderEvent 订单事件（发到 Kafka 的消息体）
type OrderEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"` // ORDER_CREATED / ORDER_CANCELLED / TRADE_FILLED
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Price     int64  `json:"price"`
	Amount    int64  `json:"amount"`
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}

// 事件类型
const (
	EventOrderCreated   = "ORDER_CREATED"
	EventOrderCancelled = "ORDER_CANCELLED"
	EventTradeFilled    = "TRADE_FILLED"
)
