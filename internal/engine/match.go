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
