package websocket

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

const (
	// HistorySize 消息历史缓冲区大小（保存最近 N 条消息）
	HistorySize = 1000
)

// Hub 管理所有 WebSocket 连接
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	Register   chan *Client
	Unregister chan *Client
	mu         sync.RWMutex

	// ✅ 新增：消息历史缓冲区（用于断线重连补发）
	history   []Message
	historyMu sync.RWMutex
}

// NewHub 创建 Hub
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		history:    make([]Message, 0, HistorySize),
	}
}

// Run 启动 Hub
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("✅ WebSocket 客户端连接: %s", client.UserID)

			// ✅ 新增：重连时补发历史消息
			go h.ReplayForClient(client)

		case client := <-h.Unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Send)
			}
			h.mu.Unlock()
			log.Printf("❌ WebSocket 客户端断开: %s", client.UserID)

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.Send <- message:
				default:
					// 客户端发送缓冲满了，关闭连接
					close(client.Send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// BroadcastTrade 广播成交消息
func (h *Hub) BroadcastTrade(tradeData interface{}) {
	msg := Message{
		Type:      TypeTrade,
		Data:      tradeData,
		Timestamp: time.Now().UnixMilli(),
	}
	h.pushHistory(msg) // ✅ 加入历史缓冲区
	data, _ := json.Marshal(msg)
	h.broadcast <- data
}

// BroadcastOrder 广播订单状态更新
func (h *Hub) BroadcastOrder(orderData interface{}) {
	msg := Message{
		Type:      TypeOrder,
		Data:      orderData,
		Timestamp: time.Now().UnixMilli(),
	}
	h.pushHistory(msg) // ✅ 加入历史缓冲区
	data, _ := json.Marshal(msg)
	h.broadcast <- data
}

// ✅ 新增：只推送给指定用户
func (h *Hub) BroadcastToUser(userID string, orderData interface{}) {
	msg := Message{
		Type:      TypeOrder,
		Data:      orderData,
		Timestamp: time.Now().UnixMilli(),
	}
	h.pushHistory(msg)
	data, _ := json.Marshal(msg)

	h.mu.RLock()
	defer h.mu.RUnlock()
	for client := range h.clients {
		if client.UserID == userID {
			select {
			case client.Send <- data:
			default:
				// 发送失败，跳过
			}
		}
	}
}

// ✅ 新增：推送消息到历史缓冲区
func (h *Hub) pushHistory(msg Message) {
	h.historyMu.Lock()
	defer h.historyMu.Unlock()

	h.history = append(h.history, msg)
	// 保持固定大小，超出则移除最旧的
	if len(h.history) > HistorySize {
		h.history = h.history[len(h.history)-HistorySize:]
	}
}

// ✅ 新增：客户端重连时补发历史消息
// 补发逻辑：发送连接建立前 N 秒内、且与该用户相关的消息
func (h *Hub) ReplayForClient(client *Client) {
	// 等待客户端初始化完成
	time.Sleep(100 * time.Millisecond)

	h.historyMu.RLock()
	defer h.historyMu.RUnlock()

	// 只补发最近 60 秒内的消息
	cutoff := time.Now().Add(-60 * time.Second).UnixMilli()

	sentCount := 0
	for _, msg := range h.history {
		if msg.Timestamp < cutoff {
			continue
		}

		// 判断消息是否与该用户相关
		if !isMessageRelevantToUser(msg, client.UserID) {
			continue
		}

		data, _ := json.Marshal(msg)
		select {
		case client.Send <- data:
			sentCount++
		default:
			// 发送失败，跳过
		}
	}

	if sentCount > 0 {
		log.Printf("📤 补发 %d 条历史消息给用户 %s", sentCount, client.UserID)
	}
}

// ✅ 新增：判断消息是否与该用户相关
func isMessageRelevantToUser(msg Message, userID string) bool {
	// 将 Data 序列化后再反序列化为 map，检查 user_id 字段
	data, err := json.Marshal(msg.Data)
	if err != nil {
		return false
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return false
	}

	// 检查常见字段：user_id / buyer_id / seller_id / maker_id / taker_id
	for _, key := range []string{"user_id", "buyer_id", "seller_id", "maker_id", "taker_id"} {
		if v, ok := m[key]; ok {
			if v == userID {
				return true
			}
		}
	}

	// 如果消息是数组（多笔成交），检查每个元素
	if arr, ok := msg.Data.([]interface{}); ok {
		for _, item := range arr {
			itemData, _ := json.Marshal(item)
			var itemMap map[string]interface{}
			if err := json.Unmarshal(itemData, &itemMap); err == nil {
				for _, key := range []string{"user_id", "buyer_id", "seller_id", "maker_id", "taker_id"} {
					if v, ok := itemMap[key]; ok && v == userID {
						return true
					}
				}
			}
		}
	}

	return false
}
