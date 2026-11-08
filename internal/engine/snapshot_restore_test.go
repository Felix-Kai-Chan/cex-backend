package engine

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newTestSnapshotter(symbol string) (*Snapshotter, *OrderBook) {
	ob := NewOrderBook()
	return NewSnapshotter(nil, ob, nil, symbol), ob
}

func restoreForTest(s *Snapshotter, records []orderRecord) {
	s.ob.mu.Lock()
	defer s.ob.mu.Unlock()
	s.restoreOrdersLocked(records)
}

func orderID(order *Order) string {
	if order == nil {
		return "<nil>"
	}
	return order.ID
}

func TestRestoreOrdersRebuildsAllStructures(t *testing.T) {
	s, ob := newTestSnapshotter("TEST/USDT")

	records := []orderRecord{
		{
			OrderID: "buy-a", UserID: "u1", Side: Buy, Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 5, Remaining: 5, Timestamp: 1,
		},
		{
			OrderID: "buy-b", UserID: "u2", Side: Buy, Type: TypeLimit, TIF: TIFGTD,
			ExpireAt: 123456789, Price: 100, Amount: 4, Remaining: 2, Timestamp: 2,
		},
		{
			OrderID: "buy-c", UserID: "u3", Side: Buy, Type: TypeLimit, TIF: TIFGTC,
			Price: 101, Amount: 3, Remaining: 3, Timestamp: 3,
		},
		{
			OrderID: "sell-d", UserID: "u4", Side: Sell, Type: TypeLimit, TIF: TIFGTC,
			Price: 105, Amount: 7, Remaining: 7, Timestamp: 4,
		},
		{
			OrderID: "sell-e", UserID: "u5", Side: Sell, Type: TypeLimit, TIF: TIFGTC,
			Price: 104, Amount: 2, Remaining: 2, Timestamp: 5,
		},
	}

	restoreForTest(s, records)

	if got := ob.BestBid(); got != 101 {
		t.Fatalf("best bid = %d, want 101", got)
	}
	if got := ob.BestAsk(); got != 104 {
		t.Fatalf("best ask = %d, want 104", got)
	}

	buy100 := ob.bids[100]
	if buy100 == nil {
		t.Fatal("bids[100] is nil")
	}
	if buy100.Head == nil || buy100.Head.ID != "buy-a" || buy100.Tail == nil || buy100.Tail.ID != "buy-b" {
		t.Fatalf("bids[100] FIFO order incorrect: head=%v tail=%v", orderID(buy100.Head), orderID(buy100.Tail))
	}
	if buy100.Head.Next != buy100.Tail {
		t.Fatal("bids[100] linked list is not connected")
	}
	if buy100.Volume != 7 || buy100.Count != 2 {
		t.Fatalf("bids[100] volume=%d count=%d, want volume=7 count=2", buy100.Volume, buy100.Count)
	}

	sell105 := ob.asks[105]
	if sell105 == nil {
		t.Fatal("asks[105] is nil")
	}
	if sell105.Volume != 7 || sell105.Count != 1 {
		t.Fatalf("asks[105] volume=%d count=%d, want volume=7 count=1", sell105.Volume, sell105.Count)
	}

	partial := ob.orders["buy-b"]
	if partial == nil {
		t.Fatal("buy-b is missing from OrderBook.orders")
	}
	if partial.Amount != 4 || partial.Remaining != 2 || partial.Price != 100 {
		t.Fatalf("buy-b fields = amount:%d remaining:%d price:%d, want 4/2/100", partial.Amount, partial.Remaining, partial.Price)
	}
	if partial.TIF != TIFGTD || partial.ExpireAt != 123456789 {
		t.Fatalf("buy-b TIF/ExpireAt = %s/%d, want GTD/123456789", partial.TIF, partial.ExpireAt)
	}

	if len(ob.orders) != 5 {
		t.Fatalf("OrderBook.orders size = %d, want 5", len(ob.orders))
	}
}

func TestRestoredOrderCanBeCancelled(t *testing.T) {
	s, ob := newTestSnapshotter("TEST/USDT")

	restoreForTest(s, []orderRecord{
		{
			OrderID: "buy-a", UserID: "u1", Side: Buy, Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 5, Remaining: 5, Timestamp: 1,
		},
		{
			OrderID: "buy-b", UserID: "u2", Side: Buy, Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 3, Remaining: 2, Timestamp: 2,
		},
	})

	cancelled, err := ob.CancelOrder("buy-b")
	if err != nil {
		t.Fatalf("CancelOrder returned error: %v", err)
	}
	if cancelled.ID != "buy-b" {
		t.Fatalf("cancelled order = %s, want buy-b", cancelled.ID)
	}
	if ob.orders["buy-b"] != nil {
		t.Fatal("buy-b still exists in OrderBook.orders")
	}

	list := ob.bids[100]
	if list == nil {
		t.Fatal("bids[100] is nil after cancel")
	}
	if list.Head == nil || list.Head.ID != "buy-a" || list.Head.Next != nil {
		t.Fatal("buy-a should be the only remaining order at price 100")
	}
	if list.Volume != 5 || list.Count != 1 {
		t.Fatalf("bids[100] volume=%d count=%d, want volume=5 count=1", list.Volume, list.Count)
	}
}

func TestSnapshotRecordsPreservesFields(t *testing.T) {
	snapshot := Snapshot{
		Bids: map[int64][]OrderItem{
			100: {
				{
					ID: "buy-a", UserID: "u1", Type: TypeLimit, TIF: TIFGTD,
					Price: 100, Amount: 10, Remaining: 4, ExpireAt: 999, Timestamp: 123,
				},
			},
		},
		Asks: map[int64][]OrderItem{
			105: {
				{
					ID: "sell-b", UserID: "u2", Type: TypeIOC, TIF: TIFIOC,
					Price: 105, Amount: 7, Remaining: 2, Timestamp: 456,
				},
			},
		},
	}

	records := snapshotRecords(snapshot)
	if len(records) != 2 {
		t.Fatalf("snapshot records = %d, want 2", len(records))
	}

	byID := make(map[string]orderRecord, len(records))
	for _, rec := range records {
		byID[rec.OrderID] = rec
	}

	buy := byID["buy-a"]
	if buy.Side != Buy || buy.Type != TypeLimit || buy.TIF != TIFGTD ||
		buy.Price != 100 || buy.Amount != 10 || buy.Remaining != 4 ||
		buy.ExpireAt != 999 || buy.Timestamp != 123 || buy.UserID != "u1" {
		t.Fatalf("buy record fields not preserved: %+v", buy)
	}

	sell := byID["sell-b"]
	if sell.Side != Sell || sell.Type != TypeIOC || sell.TIF != TIFIOC ||
		sell.Price != 105 || sell.Amount != 7 || sell.Remaining != 2 ||
		sell.Timestamp != 456 || sell.UserID != "u2" {
		t.Fatalf("sell record fields not preserved: %+v", sell)
	}
}

func TestLoadReturnsErrorWhenRedisUnavailable(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  50 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
		MaxRetries:   0,
	})
	defer rdb.Close()

	s := NewSnapshotter(rdb, NewOrderBook(), nil, "TEST/USDT")
	if err := s.Load(); err == nil {
		t.Fatal("Load returned nil when Redis is unavailable")
	}
}

func TestLoadReturnsErrorOnInvalidSnapshot(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:16379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis unavailable: %v", err)
	}

	symbol := "TEST_INVALID/USDT"
	s := NewSnapshotter(rdb, NewOrderBook(), nil, symbol)
	defer rdb.Del(ctx, s.key)

	if err := rdb.Set(ctx, s.key, "{invalid-json", 0).Err(); err != nil {
		t.Fatalf("seed invalid snapshot failed: %v", err)
	}
	if err := s.Load(); err == nil {
		t.Fatal("Load returned nil for invalid snapshot")
	}
}
