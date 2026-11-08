package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"cex-backend/internal/persistence"

	"github.com/redis/go-redis/v9"
)

func setupSnapshotRedis(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:16379"
	}
	db := 13
	if raw := os.Getenv("TEST_REDIS_DB"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("invalid TEST_REDIS_DB: %v", err)
		}
		db = parsed
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("Redis unavailable: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	previous := slog.Default()
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func seedSnapshot(t *testing.T, rdb *redis.Client, key string, snapshot Snapshot) {
	t.Helper()

	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if err := rdb.Set(context.Background(), key, data, 0).Err(); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
}

func seedWAL(t *testing.T, rdb *redis.Client, key string, entries []WALEntry) {
	t.Helper()

	for i := range entries {
		if entries[i].Seq == 0 {
			entries[i].Seq = uint64(i + 1)
		}
		if entries[i].EventID == "" {
			entries[i].EventID = fmt.Sprintf("test-event-%s-%d", key, i+1)
		}
	}
	seedWALRaw(t, rdb, key, entries)
}

func seedWALRaw(t *testing.T, rdb *redis.Client, key string, entries []WALEntry) {
	t.Helper()

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("marshal WAL entry: %v", err)
		}
		if err := rdb.RPush(context.Background(), key, data).Err(); err != nil {
			t.Fatalf("seed WAL: %v", err)
		}
	}
}

func TestReplayWALAddsAndRemovesOrders(t *testing.T) {
	rdb := setupSnapshotRedis(t)
	const symbol = "TESTWAL/USDT"

	s := NewSnapshotter(rdb, NewOrderBook(), nil, symbol)
	ctx := context.Background()
	_ = rdb.Del(ctx, s.key, s.walKey)
	t.Cleanup(func() { _ = rdb.Del(context.Background(), s.key, s.walKey) })

	seedWAL(t, rdb, s.walKey, []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "wal-a", UserID: "u1", Side: "BUY",
			Type: TypeLimit, TIF: TIFGTD, ExpireAt: 999,
			Price: 100, Amount: 5, Remaining: 5, Timestamp: 1,
		},
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "wal-b", UserID: "u2", Side: "BUY",
			Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 3, Remaining: 3, Timestamp: 2,
		},
		{Op: WALOpRemove, Symbol: symbol, OrderID: "wal-a"},
	})

	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}

	ob := s.ob
	if len(ob.orders) != 1 || ob.orders["wal-a"] != nil || ob.orders["wal-b"] == nil {
		t.Fatalf("orders after WAL replay = %v, want only wal-b", ob.orders)
	}
	list := ob.bids[100]
	if list == nil || list.Head == nil || list.Tail == nil {
		t.Fatal("bids[100] is nil after WAL replay")
	}
	if list.Head.ID != "wal-b" || list.Tail.ID != "wal-b" || list.Head.Next != nil {
		t.Fatalf("bids[100] order after WAL replay = %s -> %s", list.Head.ID, list.Tail.ID)
	}
	if list.Volume != 3 || list.Count != 1 {
		t.Fatalf("bids[100] volume=%d count=%d, want 3/1", list.Volume, list.Count)
	}
	if got := ob.BestBid(); got != 100 {
		t.Fatalf("best bid after WAL replay = %d, want 100", got)
	}
}

func TestLoadSnapshotWALReconcileSuccess(t *testing.T) {
	db := setupSnapshotMySQL(t)
	rdb := setupSnapshotRedis(t)

	const symbol = "TESTLOAD/USDT"
	const other = "TESTLOAD-OTHER/USDT"
	cleanupOrders(t, db, symbol, other)
	t.Cleanup(func() { cleanupOrders(t, db, symbol, other) })

	now := time.Now()
	rows := []persistence.OrderModel{
		{
			OrderID: "load-a", UserID: "u1", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 100, Amount: 10, Remaining: 10,
			Status: "NEW", CreatedAt: now.Add(-4 * time.Minute),
		},
		{
			OrderID: "load-b", UserID: "u2", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTD", ExpireAt: 999,
			Price: 100, Amount: 5, Remaining: 3,
			Status: "PARTIALLY_FILLED", CreatedAt: now.Add(-3 * time.Minute),
		},
		{
			OrderID: "load-c", UserID: "u3", Symbol: symbol, Side: "SELL",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 105, Amount: 7, Remaining: 2,
			Status: "PARTIALLY_FILLED", CreatedAt: now.Add(-2 * time.Minute),
		},
		{
			OrderID: "load-d", UserID: "u4", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 99, Amount: 4, Remaining: 4,
			Status: "NEW", CreatedAt: now.Add(-time.Minute),
		},
		{
			OrderID: "load-filled", UserID: "u5", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 98, Amount: 1, Remaining: 1,
			Status: "FILLED", CreatedAt: now.Add(-5 * time.Minute),
		},
		{
			OrderID: "load-other", UserID: "u6", Symbol: other, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 200, Amount: 9, Remaining: 9,
			Status: "NEW", CreatedAt: now,
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("insert orders: %v", err)
	}

	s := NewSnapshotter(rdb, NewOrderBook(), db, symbol)
	ctx := context.Background()
	_ = rdb.Del(ctx, s.key, s.walKey)
	t.Cleanup(func() { _ = rdb.Del(context.Background(), s.key, s.walKey) })

	// Snapshot 故意把同价的 load-b 放在 load-a 前面，验证 Reconcile 后按
	// MySQL 的 created_at,id 顺序纠正为 load-a -> load-b。
	seedSnapshot(t, rdb, s.key, Snapshot{
		Timestamp: now.Unix(),
		Bids: map[int64][]OrderItem{
			100: {
				{
					ID: "load-b", UserID: "u2", Type: TypeLimit, TIF: TIFGTD,
					Price: 100, Amount: 5, Remaining: 3, ExpireAt: 999, Timestamp: now.Add(-3 * time.Minute).UnixMilli(),
				},
				{
					ID: "load-a", UserID: "u1", Type: TypeLimit, TIF: TIFGTC,
					Price: 100, Amount: 10, Remaining: 10, Timestamp: now.Add(-4 * time.Minute).UnixMilli(),
				},
			},
			102: {
				{
					ID: "snapshot-only", UserID: "u7", Type: TypeLimit, TIF: TIFGTC,
					Price: 102, Amount: 1, Remaining: 1, Timestamp: now.UnixMilli(),
				},
			},
		},
		Asks: map[int64][]OrderItem{
			105: {
				{
					ID: "load-c", UserID: "u3", Type: TypeLimit, TIF: TIFGTC,
					Price: 105, Amount: 7, Remaining: 2, Timestamp: now.Add(-2 * time.Minute).UnixMilli(),
				},
			},
		},
	})

	seedWAL(t, rdb, s.walKey, []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "wal-only", UserID: "u8", Side: "BUY",
			Type: TypeLimit, TIF: TIFGTC, Price: 101, Amount: 2, Remaining: 2,
			Timestamp: now.UnixMilli(),
		},
	})

	logs := captureLogs(t)
	if err := s.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.Contains(logs.String(), "snapshot restored") {
		t.Fatalf("Load did not restore snapshot: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "WAL replay completed") {
		t.Fatalf("Load did not replay WAL: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "reconcile completed") {
		t.Fatalf("Load did not reconcile with MySQL: %s", logs.String())
	}

	ob := s.ob
	if len(ob.orders) != 4 {
		t.Fatalf("orders after Load = %d, want 4", len(ob.orders))
	}
	for _, orderID := range []string{"snapshot-only", "wal-only", "load-filled", "load-other"} {
		if ob.orders[orderID] != nil {
			t.Fatalf("non-authoritative order %s should have been removed", orderID)
		}
	}

	a := ob.orders["load-a"]
	if a == nil || a.Amount != 10 || a.Remaining != 10 || a.Price != 100 || a.TIF != TIFGTC {
		t.Fatalf("load-a fields not restored: %+v", a)
	}
	b := ob.orders["load-b"]
	if b == nil || b.Amount != 5 || b.Remaining != 3 || b.Price != 100 || b.TIF != TIFGTD || b.ExpireAt != 999 {
		t.Fatalf("load-b fields not restored: %+v", b)
	}
	c := ob.orders["load-c"]
	if c == nil || c.Amount != 7 || c.Remaining != 2 || c.Price != 105 {
		t.Fatalf("load-c fields not restored: %+v", c)
	}
	if ob.orders["load-d"] == nil {
		t.Fatal("load-d missing after Load")
	}

	buy100 := ob.bids[100]
	if buy100 == nil || buy100.Head == nil || buy100.Tail == nil {
		t.Fatal("bids[100] is nil after Load")
	}
	if buy100.Head.ID != "load-a" || buy100.Tail.ID != "load-b" {
		t.Fatalf("bids[100] FIFO = %s -> %s, want load-a -> load-b", buy100.Head.ID, buy100.Tail.ID)
	}
	if buy100.Head.Next != buy100.Tail || buy100.Tail.Next != nil {
		t.Fatal("bids[100] linked list is not connected")
	}
	if buy100.Volume != 13 || buy100.Count != 2 {
		t.Fatalf("bids[100] volume=%d count=%d, want 13/2", buy100.Volume, buy100.Count)
	}
	if ob.bids[102] != nil {
		t.Fatal("snapshot-only price level should have been removed")
	}
	if ask105 := ob.asks[105]; ask105 == nil || ask105.Volume != 2 || ask105.Count != 1 {
		t.Fatal("asks[105] was not rebuilt correctly")
	}
	if got := ob.BestBid(); got != 100 {
		t.Fatalf("best bid = %d, want 100", got)
	}
	if got := ob.BestAsk(); got != 105 {
		t.Fatalf("best ask = %d, want 105", got)
	}

	if _, err := ob.CancelOrder("load-b"); err != nil {
		t.Fatalf("cancel load-b failed: %v", err)
	}
	if ob.orders["load-b"] != nil || buy100.Head.ID != "load-a" || buy100.Head.Next != nil {
		t.Fatal("load-b was not removed from list/map")
	}
	if buy100.Volume != 10 || buy100.Count != 1 {
		t.Fatalf("bids[100] after cancel volume=%d count=%d, want 10/1", buy100.Volume, buy100.Count)
	}

	if _, err := ob.CancelOrder("load-a"); err != nil {
		t.Fatalf("cancel load-a failed: %v", err)
	}
	if ob.bids[100] != nil {
		t.Fatal("bids[100] should be removed when the last order is cancelled")
	}
	if got := ob.BestBid(); got != 99 {
		t.Fatalf("best bid after cancelling price 100 = %d, want 99", got)
	}
}
