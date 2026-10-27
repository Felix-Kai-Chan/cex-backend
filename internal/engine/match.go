package engine

// Trade 成交记录
type Trade struct {
	TradeID    string
	BuyOrder   string
	SellOrder  string
	BuyUserID  string
	SellUserID string
	Price      int64
	Quantity   int64
	Timestamp  int64
}

// RollbackData 撮合回滚描述（用于 DB 事务失败时逆向恢复内存订单簿）
// 仅记录本次 Match 对订单簿的变更，不包含业务结算信息。
type RollbackData struct {
	// 本次撮合后挂入订单簿的 taker 订单 ID（限价残余）
	AddedOrderIDs []string
	// 被完全吃掉的 maker 原始订单（恢复时重新挂入）
	RemovedMakers []Order
	// 部分成交的 maker → 成交前的 Remaining（恢复时还原）
	PartialMakers map[string]int64
}
