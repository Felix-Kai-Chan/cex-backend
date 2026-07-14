package websocket

// MessageType 消息类型
type MessageType string

const (
	TypeTrade MessageType = "trade" // 成交推送
	TypeOrder MessageType = "order" // 订单状态更新
	TypeDepth MessageType = "depth" // 深度更新
	TypePong  MessageType = "pong"  // 心跳响应
)

// Message WebSocket 消息结构
type Message struct {
	Type      MessageType `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp int64       `json:"timestamp"`
}
