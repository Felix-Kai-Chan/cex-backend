package websocket

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

// Hub 管理所有 WebSocket 连接
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	Register   chan *Client
	Unregister chan *Client
	mu         sync.RWMutex
}

// NewHub 创建 Hub
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
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
	data, _ := json.Marshal(msg)
	h.broadcast <- data
}
