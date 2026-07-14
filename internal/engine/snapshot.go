package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Snapshot 快照结构
type Snapshot struct {
	Bids      map[int64][]OrderItem `json:"bids"`
	Asks      map[int64][]OrderItem `json:"asks"`
	Timestamp int64                 `json:"timestamp"`
}

// OrderItem 快照用的订单精简结构
type OrderItem struct {
	ID        string `json:"id"`
	UserID    string `json:"user_id"`
	Price     int64  `json:"price"`
	Remaining int64  `json:"remaining"`
	Timestamp int64  `json:"timestamp"`
}

// Snapshotter 快照管理
type Snapshotter struct {
	rdb  *redis.Client
	key  string
	ob   *OrderBook
	stop chan struct{}
}

// NewSnapshotter 创建快照管理器
func NewSnapshotter(rdb *redis.Client, ob *OrderBook, symbol string) *Snapshotter {
	return &Snapshotter{
		rdb:  rdb,
		key:  fmt.Sprintf("orderbook:snapshot:%s", symbol),
		ob:   ob,
		stop: make(chan struct{}),
	}
}

// Save 保存快照到 Redis
func (s *Snapshotter) Save() error {
	s.ob.mu.RLock()
	defer s.ob.mu.RUnlock()

	snapshot := Snapshot{
		Bids:      s.convertOrderBook(s.ob.bids),
		Asks:      s.convertOrderBook(s.ob.asks),
		Timestamp: time.Now().Unix(),
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	return s.rdb.Set(context.Background(), s.key, data, 0).Err()
}

// Load 从 Redis 恢复快照
func (s *Snapshotter) Load() error {
	data, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil // 没有快照，正常启动
		}
		return err
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	s.ob.mu.Lock()
	defer s.ob.mu.Unlock()

	// 恢复买盘
	s.ob.bids = s.restoreOrderBook(snapshot.Bids, Buy)
	s.ob.asks = s.restoreOrderBook(snapshot.Asks, Sell)

	return nil
}

// StartAutoSave 定时自动保存快照（每 5 秒）
func (s *Snapshotter) StartAutoSave() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := s.Save(); err != nil {
					fmt.Printf("⚠️ 快照保存失败: %v\n", err)
				} else {
					fmt.Println("✅ 快照已保存到 Redis")
				}
			case <-s.stop:
				return
			}
		}
	}()
}

// Stop 停止自动保存
func (s *Snapshotter) Stop() {
	close(s.stop)
}

// convertOrderBook 转换订单簿为可序列化格式
func (s *Snapshotter) convertOrderBook(orders map[int64]*OrderList) map[int64][]OrderItem {
	result := make(map[int64][]OrderItem)

	for price, list := range orders {
		if list == nil || list.Head == nil {
			continue
		}

		var items []OrderItem
		for order := list.Head; order != nil; order = order.Next {
			if order.Remaining > 0 {
				items = append(items, OrderItem{
					ID:        order.ID,
					UserID:    order.UserID,
					Price:     order.Price,
					Remaining: order.Remaining,
					Timestamp: order.Timestamp,
				})
			}
		}
		if len(items) > 0 {
			result[price] = items
		}
	}

	return result
}

// restoreOrderBook 从快照恢复订单簿
func (s *Snapshotter) restoreOrderBook(items map[int64][]OrderItem, side Side) map[int64]*OrderList {
	result := make(map[int64]*OrderList)

	for price, orderItems := range items {
		list := &OrderList{}

		for _, item := range orderItems {
			order := &Order{
				ID:        item.ID,
				UserID:    item.UserID,
				Side:      side,
				Price:     item.Price,
				Amount:    item.Remaining,
				Remaining: item.Remaining,
				Timestamp: item.Timestamp,
			}

			if list.Head == nil {
				list.Head = order
				list.Tail = order
			} else {
				list.Tail.Next = order
				list.Tail = order
			}
		}

		if list.Head != nil {
			result[price] = list
		}
	}

	return result
}
