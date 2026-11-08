package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// walAppendScript 在 Redis 中原子追加整批 WAL 事件，避免 Pipeline 中途失败
// 造成一部分事件已写入、一部分未写入。
const walAppendScript = `
local key = KEYS[1]
for i = 1, #ARGV do
    redis.call('RPUSH', key, ARGV[i])
end
return #ARGV
`

// WALEntry WAL 操作日志条目
type WALEntry struct {
	Op        string      `json:"op"`
	EventID   string      `json:"event_id"`
	Seq       uint64      `json:"seq"`
	Symbol    string      `json:"symbol,omitempty"`
	OrderID   string      `json:"order_id"`
	UserID    string      `json:"user_id"`
	Side      string      `json:"side"`
	Type      OrderType   `json:"type,omitempty"`
	TIF       TimeInForce `json:"tif,omitempty"`
	Price     int64       `json:"price"`
	Amount    int64       `json:"amount,omitempty"`
	Remaining int64       `json:"remaining"`
	ExpireAt  int64       `json:"expire_at,omitempty"`
	Timestamp int64       `json:"timestamp"`
	TradeID   string      `json:"trade_id,omitempty"`
}

const (
	WALOpAdd             = "ADD"
	WALOpRemove          = "REMOVE"
	WALOpUpdateRemaining = "UPDATE_REMAINING"
)

// WALEntriesFromChanges 将撮合产生的订单变更转换为 WAL 记录。
// 所有事件都带上 symbol；UPDATE_REMAINING 记录的是更新后的 Remaining 绝对值。
func WALEntriesFromChanges(symbol string, changes []OrderChange) []WALEntry {
	entries := make([]WALEntry, 0, len(changes))
	for _, ch := range changes {
		entries = append(entries, WALEntry{
			Op:        ch.Op,
			Symbol:    symbol,
			OrderID:   ch.OrderID,
			UserID:    ch.UserID,
			Side:      ch.Side,
			Type:      ch.Type,
			TIF:       ch.TIF,
			ExpireAt:  ch.ExpireAt,
			Price:     ch.Price,
			Amount:    ch.Amount,
			Remaining: ch.Remaining,
			Timestamp: ch.Timestamp,
		})
	}
	return entries
}

// Snapshot 快照结构
type Snapshot struct {
	Bids      map[int64][]OrderItem `json:"bids"`
	Asks      map[int64][]OrderItem `json:"asks"`
	Timestamp int64                 `json:"timestamp"`
}

// OrderItem 快照用的订单精简结构
type OrderItem struct {
	ID        string      `json:"id"`
	UserID    string      `json:"user_id"`
	Type      OrderType   `json:"type,omitempty"`
	TIF       TimeInForce `json:"tif,omitempty"`
	Price     int64       `json:"price"`
	Amount    int64       `json:"amount,omitempty"`
	Remaining int64       `json:"remaining"`
	ExpireAt  int64       `json:"expire_at,omitempty"`
	Timestamp int64       `json:"timestamp"`
}

// orderRecord 是 Snapshot 与 MySQL 恢复路径共用的订单记录。
type orderRecord struct {
	OrderID   string
	UserID    string
	Symbol    string
	Side      Side
	Type      OrderType
	TIF       TimeInForce
	ExpireAt  int64
	Price     int64
	Amount    int64
	Remaining int64
	Timestamp int64
}

// Snapshotter 快照管理
type Snapshotter struct {
	rdb    *redis.Client
	key    string
	walKey string
	ob     *OrderBook
	db     *gorm.DB
	symbol string
	stop   chan struct{}

	walMu          sync.Mutex
	nextSeq        uint64
	walInitialized bool

	walFailureMu sync.RWMutex
	walFailure   error
}

// NewSnapshotter 创建快照管理器
func NewSnapshotter(rdb *redis.Client, ob *OrderBook, db *gorm.DB, symbol string) *Snapshotter {
	return &Snapshotter{
		rdb:    rdb,
		key:    fmt.Sprintf("orderbook:snapshot:%s", symbol),
		walKey: fmt.Sprintf("orderbook:wal:%s", symbol),
		ob:     ob,
		db:     db,
		symbol: symbol,
		stop:   make(chan struct{}),
	}
}

// Save 保存快照到 Redis
// ✅ 锁内深拷贝，锁外序列化 + 写 Redis（避免锁内 IO）
// ✅ 快照存成功，内存状态已被覆盖，WAL 可以全清
func (s *Snapshotter) Save() error {
	// 1. 锁内深拷贝
	s.ob.mu.RLock()
	snapshot := Snapshot{
		Bids:      s.convertOrderBook(s.ob.bids),
		Asks:      s.convertOrderBook(s.ob.asks),
		Timestamp: time.Now().Unix(),
	}
	s.ob.mu.RUnlock()

	// 2. 锁外序列化 + 写 Redis
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	if err := s.rdb.Set(context.Background(), s.key, data, 0).Err(); err != nil {
		return err
	}

	// 3. 快照存成功，内存状态已被覆盖，WAL 全部可清
	s.TruncateWAL()
	return nil
}

// Load 从 Redis 恢复快照 + WAL 回放 + MySQL 对账
func (s *Snapshotter) Load() error {
	data, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		if err == redis.Nil {
			slog.Info("no snapshot, rebuilding from MySQL", "symbol", s.symbol)
			if seqErr := s.initWALSequence(); seqErr != nil {
				return fmt.Errorf("WAL 序列号恢复失败: %w", seqErr)
			}
			return s.rebuildFromDB()
		}
		return err
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("解析快照失败: %w", err)
	}

	records := snapshotRecords(snapshot)

	s.ob.mu.Lock()
	s.restoreOrdersLocked(records)
	s.ob.mu.Unlock()

	slog.Info("snapshot restored", "symbol", s.symbol, "timestamp", snapshot.Timestamp, "orders", len(records))

	if err := s.ReplayWAL(); err != nil {
		return fmt.Errorf("WAL 回放失败: %w", err)
	}

	if err := s.Reconcile(); err != nil {
		return fmt.Errorf("MySQL 对账失败: %w", err)
	}

	return nil
}

func snapshotRecords(snapshot Snapshot) []orderRecord {
	records := make([]orderRecord, 0)
	for _, items := range snapshot.Bids {
		for _, item := range items {
			records = append(records, orderRecord{
				OrderID:   item.ID,
				UserID:    item.UserID,
				Side:      Buy,
				Type:      item.Type,
				TIF:       item.TIF,
				ExpireAt:  item.ExpireAt,
				Price:     item.Price,
				Amount:    item.Amount,
				Remaining: item.Remaining,
				Timestamp: item.Timestamp,
			})
		}
	}
	for _, items := range snapshot.Asks {
		for _, item := range items {
			records = append(records, orderRecord{
				OrderID:   item.ID,
				UserID:    item.UserID,
				Side:      Sell,
				Type:      item.Type,
				TIF:       item.TIF,
				ExpireAt:  item.ExpireAt,
				Price:     item.Price,
				Amount:    item.Amount,
				Remaining: item.Remaining,
				Timestamp: item.Timestamp,
			})
		}
	}
	return records
}

// WALFailed 当前 symbol 的 WAL 是否已经进入不可继续交易状态。
func (s *Snapshotter) WALFailed() bool {
	s.walFailureMu.RLock()
	defer s.walFailureMu.RUnlock()
	return s.walFailure != nil
}

// WALError 返回导致 WAL 失败的错误。
func (s *Snapshotter) WALError() error {
	s.walFailureMu.RLock()
	defer s.walFailureMu.RUnlock()
	return s.walFailure
}

func (s *Snapshotter) markWALFailure(err error) {
	if err == nil {
		return
	}
	s.walFailureMu.Lock()
	defer s.walFailureMu.Unlock()
	if s.walFailure == nil {
		s.walFailure = err
	}
}

func (s *Snapshotter) walFailureLocked() error {
	s.walFailureMu.RLock()
	defer s.walFailureMu.RUnlock()
	if s.walFailure == nil {
		return nil
	}
	return fmt.Errorf("WAL 已失败: %w", s.walFailure)
}

// AppendWAL 追加单条 WAL 操作日志。
func (s *Snapshotter) AppendWAL(entry WALEntry) error {
	return s.AppendWALBatch([]WALEntry{entry})
}

// AppendWALBatch 在符号级 WAL 锁内分配连续序列号并原子追加。
//
// 序列号不是由调用方传入，而是由 Snapshotter 在追加时生成。调用方（Service）
// 必须在同一 symbol 的交易操作锁内完成撮合、DB 结算并调用本方法，才能保证
// 序列号顺序与实际订单簿变更顺序一致。
func (s *Snapshotter) AppendWALBatch(entries []WALEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.walMu.Lock()
	defer s.walMu.Unlock()

	if err := s.walFailureLocked(); err != nil {
		return err
	}
	if !s.walInitialized {
		if err := s.initWALSequenceLocked(); err != nil {
			s.markWALFailure(err)
			return err
		}
	}

	base := s.nextSeq
	for i := range entries {
		entries[i].Seq = base + uint64(i)
		entries[i].EventID = s.newWALEventID(entries[i].Seq)
	}

	args := make([]interface{}, 0, len(entries))
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			s.markWALFailure(err)
			return err
		}
		args = append(args, string(data))
	}

	if err := s.rdb.Eval(context.Background(), walAppendScript, []string{s.walKey}, args...).Err(); err != nil {
		s.markWALFailure(err)
		return err
	}

	s.nextSeq = base + uint64(len(entries))
	return nil
}

func (s *Snapshotter) newWALEventID(seq uint64) string {
	return fmt.Sprintf("%s:%d:%d", s.symbol, seq, time.Now().UnixNano())
}

func (s *Snapshotter) initWALSequence() error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	if s.walInitialized {
		return nil
	}
	return s.initWALSequenceLocked()
}

func (s *Snapshotter) initWALSequenceLocked() error {
	entries, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		s.nextSeq = 1
		s.walInitialized = true
		return nil
	}

	parsed, err := s.parseAndValidateWAL(entries)
	if err != nil {
		return err
	}
	s.nextSeq = parsed[len(parsed)-1].Seq + 1
	s.walInitialized = true
	return nil
}

// ReplayWAL 回放 WAL 操作日志。
//
// 回放前会先校验整段 WAL 的连续序列号、唯一 EventID、symbol 和 Op 类型；
// 出现缺失、重复、乱序或未知事件时直接返回错误，不进行“跳过继续恢复”。
// UPDATE_REMAINING 使用绝对值而不是成交增量，因此同一事件重复回放时
// 只会把 Remaining 设置为同一个值，不会重复扣减；ADD 也会按 OrderID
// 去重，避免重复回放产生重复订单。
func (s *Snapshotter) ReplayWAL() error {
	s.walMu.Lock()

	entries, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		s.walMu.Unlock()
		return err
	}

	if len(entries) == 0 {
		s.nextSeq = 1
		s.walInitialized = true
		s.walMu.Unlock()
		slog.Info("WAL is empty, no replay needed")
		return nil
	}

	parsed, err := s.parseAndValidateWAL(entries)
	if err != nil {
		s.walMu.Unlock()
		return err
	}

	// 只有整段 WAL 校验通过后才开始修改订单簿，避免校验失败时留下半恢复状态。
	s.ob.mu.Lock()
	replayedCount := 0
	for _, entry := range parsed {
		applied, applyErr := s.applyWALEntryLocked(entry)
		if applyErr != nil {
			s.ob.mu.Unlock()
			s.walMu.Unlock()
			return applyErr
		}
		if applied {
			replayedCount++
		}
	}

	if replayedCount > 0 {
		s.rebuildOrdersIndexLocked()
		s.rebuildListAggregatesLocked()
		s.rebuildSkipList()
	}
	s.ob.mu.Unlock()

	nextSeq := parsed[len(parsed)-1].Seq + 1
	s.nextSeq = nextSeq
	s.walInitialized = true
	s.walMu.Unlock()

	if replayedCount > 0 {
		slog.Info("WAL replay completed", "count", replayedCount, "next_seq", nextSeq)
	}
	return nil
}

func (s *Snapshotter) parseAndValidateWAL(rawEntries []string) ([]WALEntry, error) {
	parsed := make([]WALEntry, 0, len(rawEntries))
	seenEventIDs := make(map[string]struct{}, len(rawEntries))
	expectedSeq := uint64(1)

	for i, entryStr := range rawEntries {
		var entry WALEntry
		if err := json.Unmarshal([]byte(entryStr), &entry); err != nil {
			return nil, fmt.Errorf("WAL 第 %d 条事件解析失败: %w", i+1, err)
		}
		if entry.Seq == 0 {
			return nil, fmt.Errorf("WAL 第 %d 条事件缺少序列号", i+1)
		}
		if entry.Seq != expectedSeq {
			return nil, fmt.Errorf("WAL 序列号断层或乱序: 第 %d 条 seq=%d, 期望 seq=%d", i+1, entry.Seq, expectedSeq)
		}
		if entry.EventID == "" {
			return nil, fmt.Errorf("WAL 第 %d 条事件缺少 event_id", i+1)
		}
		if _, exists := seenEventIDs[entry.EventID]; exists {
			return nil, fmt.Errorf("WAL 第 %d 条事件 event_id 重复: %s", i+1, entry.EventID)
		}
		seenEventIDs[entry.EventID] = struct{}{}
		if entry.Symbol != s.symbol {
			return nil, fmt.Errorf("WAL 第 %d 条事件 symbol=%q 与当前 symbol=%q 不一致", i+1, entry.Symbol, s.symbol)
		}
		switch entry.Op {
		case WALOpAdd, WALOpRemove, WALOpUpdateRemaining:
		default:
			return nil, fmt.Errorf("WAL 第 %d 条事件 op=%q 不受支持", i+1, entry.Op)
		}

		parsed = append(parsed, entry)
		expectedSeq++
	}
	return parsed, nil
}

func (s *Snapshotter) applyWALEntryLocked(entry WALEntry) (bool, error) {
	switch entry.Op {
	case WALOpAdd:
		// 重复回放同一个 ADD 时不能再次挂单，否则会产生重复订单。
		if s.ob.orders[entry.OrderID] != nil {
			return false, nil
		}

		side := Buy
		if entry.Side == "SELL" {
			side = Sell
		}
		orderType := entry.Type
		if orderType == "" {
			orderType = TypeLimit
		}
		amount := entry.Amount
		if amount == 0 {
			amount = entry.Remaining
		}
		order := &Order{
			ID:        entry.OrderID,
			UserID:    entry.UserID,
			Side:      side,
			Type:      orderType,
			TIF:       entry.TIF,
			Price:     entry.Price,
			Amount:    amount,
			Remaining: entry.Remaining,
			ExpireAt:  entry.ExpireAt,
			Timestamp: entry.Timestamp,
		}
		normalizeTIF(order)
		s.addOrderLocked(order)
		return true, nil

	case WALOpUpdateRemaining:
		order := s.ob.orders[entry.OrderID]
		if order == nil {
			return false, fmt.Errorf("WAL UPDATE_REMAINING 找不到订单: %s", entry.OrderID)
		}
		s.updateOrderRemainingLocked(order, entry.Remaining)
		return true, nil

	case WALOpRemove:
		if s.ob.orders[entry.OrderID] == nil {
			return false, nil
		}
		s.removeOrderLocked(entry.OrderID)
		return true, nil
	}

	return false, fmt.Errorf("未知 WAL 事件类型: %s", entry.Op)
}

// updateOrderRemainingLocked 使用绝对值更新订单剩余量。
// 只调整 Remaining 和对应价格档位的 Volume，不修改原始 Amount。
func (s *Snapshotter) updateOrderRemainingLocked(order *Order, remaining int64) {
	if order == nil {
		return
	}
	if remaining <= 0 {
		s.removeOrderLocked(order.ID)
		return
	}
	if order.Amount > 0 && remaining > order.Amount {
		slog.Warn("WAL remaining exceeds amount, clamping",
			"order_id", order.ID,
			"remaining", remaining,
			"amount", order.Amount,
		)
		remaining = order.Amount
	}
	if order.Remaining == remaining {
		return
	}

	old := order.Remaining
	order.Remaining = remaining

	var list *OrderList
	if order.Side == Buy {
		list = s.ob.bids[order.Price]
	} else {
		list = s.ob.asks[order.Price]
	}
	if list != nil {
		list.Volume += remaining - old
	}
}

// TruncateWAL 清空 WAL，并开启一个新的 WAL 段（序列号从 1 重新开始）。
// 调用方必须已经保证对应快照包含清空前订单簿状态；Snapshot 与 WAL 截断的
// 原子性不属于本次范围。
func (s *Snapshotter) TruncateWAL() error {
	s.walMu.Lock()
	defer s.walMu.Unlock()

	if err := s.rdb.Del(context.Background(), s.walKey).Err(); err != nil {
		return err
	}
	s.nextSeq = 1
	s.walInitialized = true
	return nil
}

// loadOpenOrdersFromDB 按当前 symbol 查询 MySQL 开放订单，并保持
// created_at ASC, id ASC 的顺序。恢复和启动对账都必须复用该顺序，
// 不能先放进 map 再遍历，否则同价订单的 FIFO 会丢失。
func (s *Snapshotter) loadOpenOrdersFromDB() ([]orderRecord, error) {
	var rows []struct {
		ID          uint64
		OrderID     string
		UserID      string
		Symbol      string
		Side        string
		OrderType   string
		TimeInForce string
		ExpireAt    int64
		Price       int64
		Amount      int64
		Remaining   int64
		CreatedAt   time.Time
	}

	query := `SELECT id, order_id, user_id, symbol, side, order_type, time_in_force, expire_at, price, amount, remaining, created_at
              FROM orders
              WHERE symbol = ? AND status IN ('NEW', 'PARTIALLY_FILLED')
              ORDER BY created_at ASC, id ASC`

	if err := s.db.Raw(query, s.symbol).Scan(&rows).Error; err != nil {
		return nil, err
	}

	records := make([]orderRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, orderRecord{
			OrderID:   row.OrderID,
			UserID:    row.UserID,
			Symbol:    row.Symbol,
			Side:      parseSide(row.Side),
			Type:      parseOrderType(row.OrderType),
			TIF:       parseTIF(row.TimeInForce),
			ExpireAt:  row.ExpireAt,
			Price:     row.Price,
			Amount:    row.Amount,
			Remaining: row.Remaining,
			Timestamp: row.CreatedAt.UnixMilli(),
		})
	}
	return records, nil
}

// rebuildFromDB 从 MySQL 全量重建当前 symbol 的订单簿。
func (s *Snapshotter) rebuildFromDB() error {
	records, err := s.loadOpenOrdersFromDB()
	if err != nil {
		return fmt.Errorf("从 MySQL 重建订单簿失败: %w", err)
	}

	s.ob.mu.Lock()
	defer s.ob.mu.Unlock()

	s.restoreOrdersLocked(records)
	slog.Info("orderbook rebuilt from MySQL", "symbol", s.symbol, "count", len(records))
	return nil
}

// Reconcile 当前只在启动恢复阶段由 Load 调用。
// MySQL 是权威数据源，因此直接按 MySQL 的顺序全量重建当前 symbol 的订单簿，
// 而不是把 DB 订单追加到内存订单尾部。这样可以保证：
//   - 同价订单严格遵循 created_at ASC, id ASC；
//   - 比内存中已有订单更早的缺失订单不会被放到队尾；
//   - orders、链表、跳表、Volume、Count 一次重建完成。
//
// 如果未来需要在运行期间调用 Reconcile，必须重新设计：运行期不能随意
// 重排正在撮合的订单，需要基于内存订单的时间优先级做有序合并。
func (s *Snapshotter) Reconcile() error {
	records, err := s.loadOpenOrdersFromDB()
	if err != nil {
		return fmt.Errorf("对账查询失败: %w", err)
	}

	s.ob.mu.Lock()
	s.restoreOrdersLocked(records)
	s.ob.mu.Unlock()

	slog.Info("reconcile completed, orderbook rebuilt from MySQL", "symbol", s.symbol, "count", len(records))
	return nil
}

// restoreOrdersLocked 用完整订单记录重建订单簿。调用方必须持有 s.ob.mu 写锁。
func (s *Snapshotter) restoreOrdersLocked(records []orderRecord) {
	s.ob.bids = make(map[int64]*OrderList)
	s.ob.asks = make(map[int64]*OrderList)
	s.ob.orders = make(map[string]*Order)
	s.ob.bidPrices = NewSkipList(false)
	s.ob.askPrices = NewSkipList(true)

	for _, rec := range records {
		if rec.Remaining <= 0 {
			continue
		}
		s.ob.addOrderLocked(newOrderFromRecord(rec))
	}

	s.rebuildOrdersIndexLocked()
	s.rebuildListAggregatesLocked()
	s.rebuildSkipList()
}

func newOrderFromRecord(rec orderRecord) *Order {
	side := rec.Side
	if side != Buy && side != Sell {
		side = Buy
	}
	orderType := rec.Type
	if orderType == "" {
		orderType = TypeLimit
	}
	amount := rec.Amount
	if amount <= 0 {
		amount = rec.Remaining
	}

	order := &Order{
		ID:        rec.OrderID,
		UserID:    rec.UserID,
		Side:      side,
		Type:      orderType,
		TIF:       rec.TIF,
		ExpireAt:  rec.ExpireAt,
		Price:     rec.Price,
		Amount:    amount,
		Remaining: rec.Remaining,
		Timestamp: rec.Timestamp,
	}
	normalizeTIF(order)
	return order
}

func parseSide(raw string) Side {
	if strings.EqualFold(strings.TrimSpace(raw), "SELL") {
		return Sell
	}
	return Buy
}

func parseOrderType(raw string) OrderType {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "MARKET":
		return TypeMarket
	case "IOC":
		return TypeIOC
	case "FOK":
		return TypeFOK
	default:
		return TypeLimit
	}
}

func parseTIF(raw string) TimeInForce {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "GTC":
		return TIFGTC
	case "IOC":
		return TIFIOC
	case "FOK":
		return TIFFOK
	case "GTD":
		return TIFGTD
	default:
		return ""
	}
}

// rebuildOrdersIndexLocked 从价格链表重建 OrderBook.orders。调用方必须持有写锁。
func (s *Snapshotter) rebuildOrdersIndexLocked() {
	s.ob.orders = make(map[string]*Order)
	for _, list := range s.ob.bids {
		for order := list.Head; order != nil; order = order.Next {
			s.ob.orders[order.ID] = order
		}
	}
	for _, list := range s.ob.asks {
		for order := list.Head; order != nil; order = order.Next {
			s.ob.orders[order.ID] = order
		}
	}
}

// rebuildListAggregatesLocked 按订单剩余量重算价格档位的聚合值。调用方必须持有写锁。
func (s *Snapshotter) rebuildListAggregatesLocked() {
	for _, list := range s.ob.bids {
		recalculateOrderList(list)
	}
	for _, list := range s.ob.asks {
		recalculateOrderList(list)
	}
}

func recalculateOrderList(list *OrderList) {
	var volume int64
	count := 0
	for order := list.Head; order != nil; order = order.Next {
		volume += order.Remaining
		count++
	}
	list.Volume = volume
	list.Count = count
}

// addOrderLocked 内部使用，不加锁。必须复用 OrderBook 的 canonical 逻辑，
// 以同时维护 orders、Volume、Count 和跳表。
func (s *Snapshotter) addOrderLocked(order *Order) {
	s.ob.addOrderLocked(order)
}

// removeOrderLocked 内部使用，不加锁。
func (s *Snapshotter) removeOrderLocked(orderID string) {
	s.ob.removeOrderLocked(orderID)
}

// rebuildSkipList 重建跳表
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
					slog.Warn("snapshot save failed", "error", err)
				} else {
					slog.Debug("snapshot saved", "key", s.key)
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
					Type:      order.Type,
					TIF:       order.TIF,
					Price:     order.Price,
					Amount:    order.Amount,
					Remaining: order.Remaining,
					ExpireAt:  order.ExpireAt,
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
