package engine

import (
	"os"
	"testing"
	"time"

	"cex-backend/internal/persistence"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupSnapshotMySQL(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local"
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Skipf("MySQL unavailable: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Skipf("MySQL pool unavailable: %v", err)
	}
	if err := sqlDB.Ping(); err != nil {
		t.Skipf("MySQL ping failed: %v", err)
	}
	if err := db.AutoMigrate(&persistence.OrderModel{}); err != nil {
		t.Fatalf("migrate orders: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func cleanupOrders(t *testing.T, db *gorm.DB, symbols ...string) {
	t.Helper()
	if err := db.Where("symbol IN ?", symbols).Delete(&persistence.OrderModel{}).Error; err != nil {
		t.Fatalf("cleanup orders: %v", err)
	}
}

func TestRebuildFromDBIsSymbolScopedAndPreservesFields(t *testing.T) {
	db := setupSnapshotMySQL(t)
	const symbol = "TESTA/USDT"
	const other = "TESTB/USDT"
	cleanupOrders(t, db, symbol, other)
	t.Cleanup(func() { cleanupOrders(t, db, symbol, other) })

	now := time.Now()
	rows := []persistence.OrderModel{
		{
			OrderID: "a1", UserID: "u1", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 100, Amount: 5, Remaining: 5,
			Status: "NEW", CreatedAt: now.Add(-time.Minute),
		},
		{
			OrderID: "a2", UserID: "u2", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTD", ExpireAt: 999,
			Price: 100, Amount: 4, Remaining: 2,
			Status: "PARTIALLY_FILLED", CreatedAt: now.Add(-30 * time.Second),
		},
		{
			OrderID: "a3", UserID: "u3", Symbol: symbol, Side: "SELL",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 105, Amount: 7, Remaining: 3,
			Status: "PARTIALLY_FILLED", CreatedAt: now.Add(-20 * time.Second),
		},
		{
			OrderID: "a4", UserID: "u4", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 99, Amount: 1, Remaining: 1,
			Status: "FILLED", CreatedAt: now.Add(-10 * time.Second),
		},
		{
			OrderID: "a5", UserID: "u5", Symbol: symbol, Side: "SELL",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 106, Amount: 1, Remaining: 1,
			Status: "CANCELED", CreatedAt: now.Add(-5 * time.Second),
		},
		{
			OrderID: "b1", UserID: "u6", Symbol: other, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 200, Amount: 9, Remaining: 9,
			Status: "NEW", CreatedAt: now,
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("insert orders: %v", err)
	}

	ob := NewOrderBook()
	s := NewSnapshotter(nil, ob, db, symbol)
	if err := s.rebuildFromDB(); err != nil {
		t.Fatalf("rebuildFromDB: %v", err)
	}

	if len(ob.orders) != 3 {
		t.Fatalf("rebuilt orders = %d, want 3", len(ob.orders))
	}
	if ob.orders["b1"] != nil {
		t.Fatal("cross-symbol order b1 was loaded")
	}
	if ob.orders["a4"] != nil || ob.orders["a5"] != nil {
		t.Fatal("filled/canceled orders were loaded")
	}

	partial := ob.orders["a2"]
	if partial == nil {
		t.Fatal("a2 missing after rebuild")
	}
	if partial.Amount != 4 || partial.Remaining != 2 || partial.Price != 100 {
		t.Fatalf("a2 fields = amount:%d remaining:%d price:%d, want 4/2/100", partial.Amount, partial.Remaining, partial.Price)
	}
	if partial.TIF != TIFGTD || partial.ExpireAt != 999 {
		t.Fatalf("a2 TIF/ExpireAt = %s/%d, want GTD/999", partial.TIF, partial.ExpireAt)
	}

	buy100 := ob.bids[100]
	if buy100 == nil || buy100.Head == nil || buy100.Head.ID != "a1" || buy100.Tail == nil || buy100.Tail.ID != "a2" {
		t.Fatal("bids[100] FIFO restored incorrectly")
	}
	if buy100.Volume != 7 || buy100.Count != 2 {
		t.Fatalf("bids[100] volume=%d count=%d, want 7/2", buy100.Volume, buy100.Count)
	}
	if ask105 := ob.asks[105]; ask105 == nil || ask105.Volume != 3 || ask105.Count != 1 {
		t.Fatal("asks[105] aggregate restored incorrectly")
	}

	if _, err := ob.CancelOrder("a2"); err != nil {
		t.Fatalf("cancel restored order: %v", err)
	}
	if ob.orders["a2"] != nil {
		t.Fatal("a2 still exists after cancel")
	}
	if buy100.Volume != 5 || buy100.Count != 1 {
		t.Fatalf("bids[100] after cancel volume=%d count=%d, want 5/1", buy100.Volume, buy100.Count)
	}
}

func TestReconcileUsesActualStatusesAndSymbolScope(t *testing.T) {
	db := setupSnapshotMySQL(t)
	const symbol = "TESTC/USDT"
	const other = "TESTD/USDT"
	cleanupOrders(t, db, symbol, other)
	t.Cleanup(func() { cleanupOrders(t, db, symbol, other) })

	now := time.Now()
	rows := []persistence.OrderModel{
		{
			OrderID: "c1", UserID: "u1", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 100, Amount: 5, Remaining: 2,
			Status: "PARTIALLY_FILLED", CreatedAt: now.Add(-time.Minute),
		},
		{
			OrderID: "c2", UserID: "u2", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 99, Amount: 4, Remaining: 4,
			Status: "NEW", CreatedAt: now.Add(-30 * time.Second),
		},
		{
			OrderID: "c3", UserID: "u3", Symbol: symbol, Side: "SELL",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 105, Amount: 2, Remaining: 2,
			Status: "FILLED", CreatedAt: now.Add(-10 * time.Second),
		},
		{
			OrderID: "d1", UserID: "u4", Symbol: other, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 200, Amount: 9, Remaining: 9,
			Status: "NEW", CreatedAt: now,
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("insert orders: %v", err)
	}

	ob := NewOrderBook()
	s := NewSnapshotter(nil, ob, db, symbol)
	restoreForTest(s, []orderRecord{
		{
			OrderID: "c1", UserID: "u1", Side: Buy, Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 5, Remaining: 5, Timestamp: now.Add(-time.Minute).UnixMilli(),
		},
		{
			OrderID: "c5", UserID: "u5", Side: Sell, Type: TypeLimit, TIF: TIFGTC,
			Price: 104, Amount: 3, Remaining: 3, Timestamp: now.UnixMilli(),
		},
	})

	if err := s.Reconcile(); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if len(ob.orders) != 2 {
		t.Fatalf("reconciled orders = %d, want 2", len(ob.orders))
	}
	if ob.orders["d1"] != nil {
		t.Fatal("cross-symbol order d1 was loaded")
	}
	if ob.orders["c3"] != nil {
		t.Fatal("filled order c3 was loaded")
	}
	if ob.orders["c5"] != nil {
		t.Fatal("stale order c5 was not removed")
	}

	c1 := ob.orders["c1"]
	if c1 == nil {
		t.Fatal("c1 missing after reconcile")
	}
	if c1.Amount != 5 || c1.Remaining != 2 {
		t.Fatalf("c1 amount/remaining = %d/%d, want 5/2", c1.Amount, c1.Remaining)
	}
	if ob.bids[100] == nil || ob.bids[100].Volume != 2 || ob.bids[100].Count != 1 {
		t.Fatal("bids[100] aggregate after reconcile is incorrect")
	}
	if ob.orders["c2"] == nil {
		t.Fatal("new order c2 was not added")
	}
	if ob.bids[99] == nil || ob.bids[99].Volume != 4 || ob.bids[99].Count != 1 {
		t.Fatal("bids[99] aggregate after reconcile is incorrect")
	}
}

func TestReconcileRebuildsFIFOFromMySQL(t *testing.T) {
	db := setupSnapshotMySQL(t)
	const symbol = "TESTFIFO/USDT"
	cleanupOrders(t, db, symbol)
	t.Cleanup(func() { cleanupOrders(t, db, symbol) })

	now := time.Now()
	rows := []persistence.OrderModel{
		{
			OrderID: "fifo-old", UserID: "u1", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 100, Amount: 6, Remaining: 6,
			Status: "NEW", CreatedAt: now.Add(-2 * time.Minute),
		},
		{
			OrderID: "fifo-new", UserID: "u2", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 100, Amount: 5, Remaining: 3,
			Status: "PARTIALLY_FILLED", CreatedAt: now.Add(-time.Minute),
		},
		{
			OrderID: "fifo-other", UserID: "u3", Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 99, Amount: 4, Remaining: 4,
			Status: "NEW", CreatedAt: now.Add(-30 * time.Second),
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("insert orders: %v", err)
	}

	ob := NewOrderBook()
	s := NewSnapshotter(nil, ob, db, symbol)

	// 故意把内存同价顺序做成 new -> old，并放入 stale 订单。
	// Reconcile 必须按 MySQL 的 created_at,id 顺序重建为 old -> new。
	restoreForTest(s, []orderRecord{
		{
			OrderID: "fifo-new", UserID: "u2", Side: Buy, Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 5, Remaining: 99, Timestamp: now.Add(-time.Minute).UnixMilli(),
		},
		{
			OrderID: "fifo-old", UserID: "u1", Side: Buy, Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 6, Remaining: 99, Timestamp: now.Add(-2 * time.Minute).UnixMilli(),
		},
		{
			OrderID: "fifo-stale", UserID: "u4", Side: Buy, Type: TypeLimit, TIF: TIFGTC,
			Price: 98, Amount: 1, Remaining: 1, Timestamp: now.UnixMilli(),
		},
	})

	if err := s.Reconcile(); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	list := ob.bids[100]
	if list == nil || list.Head == nil || list.Tail == nil {
		t.Fatal("bids[100] is nil after reconcile")
	}
	if list.Head.ID != "fifo-old" || list.Tail.ID != "fifo-new" {
		t.Fatalf("bids[100] FIFO = %s -> %s, want fifo-old -> fifo-new", list.Head.ID, list.Tail.ID)
	}
	if list.Head.Next != list.Tail || list.Tail.Next != nil {
		t.Fatal("bids[100] linked list is not connected correctly")
	}
	if list.Volume != 9 || list.Count != 2 {
		t.Fatalf("bids[100] volume=%d count=%d, want 9/2", list.Volume, list.Count)
	}
	if ob.orders["fifo-stale"] != nil {
		t.Fatal("stale order was not removed")
	}
	if got := ob.BestBid(); got != 100 {
		t.Fatalf("best bid = %d, want 100", got)
	}

	if _, err := ob.CancelOrder("fifo-new"); err != nil {
		t.Fatalf("cancel fifo-new failed: %v", err)
	}
	if ob.orders["fifo-new"] != nil || list.Head.ID != "fifo-old" || list.Head.Next != nil {
		t.Fatal("fifo-new was not removed from list/map")
	}
	if list.Volume != 6 || list.Count != 1 {
		t.Fatalf("bids[100] after cancel volume=%d count=%d, want 6/1", list.Volume, list.Count)
	}

	if _, err := ob.CancelOrder("fifo-old"); err != nil {
		t.Fatalf("cancel fifo-old failed: %v", err)
	}
	if ob.bids[100] != nil {
		t.Fatal("bids[100] should be removed when the last order is cancelled")
	}
	if got := ob.BestBid(); got != 99 {
		t.Fatalf("best bid after cancelling price 100 = %d, want 99", got)
	}
}
