package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// WALEntry WAL 操作日志条目
type WALEntry struct {
	Op        string `json:"op"`
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	Side      string `json:"side"`
	Price     int64  `json:"price"`
	Remaining int64  `json:"remaining"`
	Timestamp int64  `json:"timestamp"`
	TradeID   string `json:"trade_id,omitempty"`
}

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
	rdb    *redis.Client
	key    string
	walKey string
	ob     *OrderBook
	db     *gorm.DB
	stop   chan struct{}
}

// NewSnapshotter 创建快照管理器
func NewSnapshotter(rdb *redis.Client, ob *OrderBook, db *gorm.DB, symbol string) *Snapshotter {
	return &Snapshotter{
		rdb:    rdb,
		key:    fmt.Sprintf("orderbook:snapshot:%s", symbol),
		walKey: fmt.Sprintf("orderbook:wal:%s", symbol),
		ob:     ob,
		db:     db,
		stop:   make(chan struct{}),
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

	if err := s.rdb.Set(context.Background(), s.key, data, 0).Err(); err != nil {
		return err
	}

	s.TruncateWAL()
	return nil
}

// Load 从 Redis 恢复快照 + WAL 回放 + MySQL 对账
func (s *Snapshotter) Load() error {
	data, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		if err == redis.Nil {
			fmt.Println("⚠️ 无快照，从 MySQL 全量重建订单簿")
			return s.rebuildFromDB()
		}
		return err
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	s.ob.mu.Lock()
	s.ob.bids = s.restoreOrderBook(snapshot.Bids, Buy)
	s.ob.asks = s.restoreOrderBook(snapshot.Asks, Sell)
	// ✅ 重建跳表
	s.rebuildSkipList()
	s.ob.mu.Unlock()

	fmt.Printf("✅ 快照恢复完成（时间戳: %d）\n", snapshot.Timestamp)

	if err := s.ReplayWAL(); err != nil {
		fmt.Printf("⚠️ WAL 回放失败: %v\n", err)
	}

	return s.Reconcile()
}

// AppendWAL 追加 WAL 操作日志
func (s *Snapshotter) AppendWAL(entry WALEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return s.rdb.RPush(context.Background(), s.walKey, data).Err()
}

// ReplayWAL 回放 WAL 操作日志
func (s *Snapshotter) ReplayWAL() error {
	entries, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		fmt.Println("📋 WAL 为空，无需回放")
		return nil
	}

	s.ob.mu.Lock()
	defer s.ob.mu.Unlock()

	replayedCount := 0
	for _, entryStr := range entries {
		var entry WALEntry
		if err := json.Unmarshal([]byte(entryStr), &entry); err != nil {
			continue
		}

		switch entry.Op {
		case "ADD":
			side := Buy
			if entry.Side == "SELL" {
				side = Sell
			}
			order := &Order{
				ID:        entry.OrderID,
				UserID:    entry.UserID,
				Side:      side,
				Price:     entry.Price,
				Amount:    entry.Remaining,
				Remaining: entry.Remaining,
				Timestamp: entry.Timestamp,
			}
			s.addOrderLocked(order)
			replayedCount++

		case "REMOVE":
			s.removeOrderLocked(entry.OrderID)
			replayedCount++
		}
	}

	if replayedCount > 0 {
		s.rebuildSkipList()
		fmt.Printf("✅ WAL 回放完成：%d 条操作\n", replayedCount)
	}

	return nil
}

// TruncateWAL 清空 WAL
func (s *Snapshotter) TruncateWAL() error {
	return s.rdb.Del(context.Background(), s.walKey).Err()
}

// rebuildFromDB 从 MySQL 全量重建订单簿
func (s *Snapshotter) rebuildFromDB() error {
	var orders []struct {
		OrderID   string
		UserID    string
		Side      string
		Price     int64
		Remaining int64
		CreatedAt int64
	}

	query := `SELECT order_id, user_id, side, price, remaining, created_at 
	          FROM orders 
	          WHERE status IN ('PENDING', 'PARTIAL') 
	          ORDER BY created_at ASC`

	if err := s.db.Raw(query).Scan(&orders).Error; err != nil {
		return fmt.Errorf("从 MySQL 重建订单簿失败: %w", err)
	}

	s.ob.mu.Lock()
	defer s.ob.mu.Unlock()

	s.ob.bids = make(map[int64]*OrderList)
	s.ob.asks = make(map[int64]*OrderList)
	s.ob.bidPrices = NewSkipList(false)
	s.ob.askPrices = NewSkipList(true)

	for _, o := range orders {
		side := Buy
		if o.Side == "SELL" {
			side = Sell
		}
		order := &Order{
			ID:        o.OrderID,
			UserID:    o.UserID,
			Side:      side,
			Price:     o.Price,
			Amount:    o.Remaining,
			Remaining: o.Remaining,
			Timestamp: o.CreatedAt,
		}
		s.addOrderLocked(order)
	}

	s.rebuildSkipList()
	fmt.Printf("✅ 从 MySQL 重建订单簿完成，共 %d 条挂单\n", len(orders))
	return nil
}

// Reconcile 与 MySQL 对账
func (s *Snapshotter) Reconcile() error {
	var dbOrders []struct {
		OrderID   string
		UserID    string
		Side      string
		Price     int64
		Remaining int64
		CreatedAt int64
	}

	query := `SELECT order_id, user_id, side, price, remaining, created_at 
	          FROM orders 
	          WHERE status IN ('PENDING', 'PARTIAL') 
	          ORDER BY created_at ASC`

	if err := s.db.Raw(query).Scan(&dbOrders).Error; err != nil {
		return fmt.Errorf("对账查询失败: %w", err)
	}

	dbOrderMap := make(map[string]bool)
	for _, o := range dbOrders {
		dbOrderMap[o.OrderID] = true
	}

	s.ob.mu.Lock()
	defer s.ob.mu.Unlock()

	memOrderMap := make(map[string]bool)
	for _, list := range s.ob.bids {
		for o := list.Head; o != nil; o = o.Next {
			memOrderMap[o.ID] = true
		}
	}
	for _, list := range s.ob.asks {
		for o := list.Head; o != nil; o = o.Next {
			memOrderMap[o.ID] = true
		}
	}

	addedCount := 0
	removedCount := 0

	for _, o := range dbOrders {
		if !memOrderMap[o.OrderID] {
			side := Buy
			if o.Side == "SELL" {
				side = Sell
			}
			order := &Order{
				ID:        o.OrderID,
				UserID:    o.UserID,
				Side:      side,
				Price:     o.Price,
				Amount:    o.Remaining,
				Remaining: o.Remaining,
				Timestamp: o.CreatedAt,
			}
			s.addOrderLocked(order)
			addedCount++
		}
	}

	for orderID := range memOrderMap {
		if !dbOrderMap[orderID] {
			s.removeOrderLocked(orderID)
			removedCount++
		}
	}

	if addedCount > 0 || removedCount > 0 {
		s.rebuildSkipList()
		fmt.Printf("✅ 对账完成：补入 %d 条，移除 %d 条\n", addedCount, removedCount)
	} else {
		fmt.Println("✅ 对账完成：无差异")
	}

	return nil
}

// addOrderLocked 内部使用，不加锁
// ✅ 同步维护跳表
func (s *Snapshotter) addOrderLocked(order *Order) {
	var list *OrderList
	if order.Side == Buy {
		list = s.ob.bids[order.Price]
		if list == nil {
			list = &OrderList{}
			s.ob.bids[order.Price] = list
			s.ob.bidPrices.Insert(order.Price, list) // ✅ 跳表
		}
	} else {
		list = s.ob.asks[order.Price]
		if list == nil {
			list = &OrderList{}
			s.ob.asks[order.Price] = list
			s.ob.askPrices.Insert(order.Price, list) // ✅ 跳表
		}
	}

	if list.Head == nil {
		list.Head = order
		list.Tail = order
	} else {
		list.Tail.Next = order
		list.Tail = order
	}
}

// removeOrderLocked 内部使用，不加锁
// ✅ 同步维护跳表
func (s *Snapshotter) removeOrderLocked(orderID string) {
	for price, list := range s.ob.bids {
		if list == nil || list.Head == nil {
			continue
		}
		if removed := s.removeFromList(list, orderID); removed != nil {
			if list.Head == nil {
				delete(s.ob.bids, price)
				s.ob.bidPrices.Remove(price) // ✅ 跳表
			}
			return
		}
	}
	for price, list := range s.ob.asks {
		if list == nil || list.Head == nil {
			continue
		}
		if removed := s.removeFromList(list, orderID); removed != nil {
			if list.Head == nil {
				delete(s.ob.asks, price)
				s.ob.askPrices.Remove(price) // ✅ 跳表
			}
			return
		}
	}
}

// removeFromList 从链表中移除（内部使用）
func (s *Snapshotter) removeFromList(list *OrderList, orderID string) *Order {
	if list == nil || list.Head == nil {
		return nil
	}

	if list.Head.ID == orderID {
		removed := list.Head
		list.Head = list.Head.Next
		if list.Head == nil {
			list.Tail = nil
		}
		removed.Next = nil
		return removed
	}

	prev := list.Head
	curr := prev.Next
	for curr != nil {
		if curr.ID == orderID {
			prev.Next = curr.Next
			if curr == list.Tail {
				list.Tail = prev
			}
			curr.Next = nil
			return curr
		}
		prev = curr
		curr = curr.Next
	}

	return nil
}

// ✅ 新增：重建跳表（替代原来的 rebuildBestPriceCache）
func (s *Snapshotter) rebuildSkipList() {
	s.ob.bidPrices = NewSkipList(false)
	s.ob.askPrices = NewSkipList(true)

	for price, list := range s.ob.bids {
		if list.Head != nil {
			s.ob.bidPrices.Insert(price, list)
		}
	}
	for price, list := range s.ob.asks {
		if list.Head != nil {
			s.ob.askPrices.Insert(price, list)
		}
	}
}

// StartAutoSave 定时自动保存快照
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
