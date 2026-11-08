package engine

import (
	"context"
	"testing"
)

func newWALTestSnapshotter(t *testing.T, symbol string) (*Snapshotter, *OrderBook) {
	t.Helper()

	rdb := setupSnapshotRedis(t)
	ob := NewOrderBook()
	s := NewSnapshotter(rdb, ob, nil, symbol)
	ctx := context.Background()
	_ = rdb.Del(ctx, s.key, s.walKey)
	t.Cleanup(func() { _ = rdb.Del(context.Background(), s.key, s.walKey) })
	return s, ob
}

func newWALTestOrder(id string, side Side, price, amount, timestamp int64) *Order {
	return &Order{
		ID:        id,
		UserID:    "user-" + id,
		Side:      side,
		Type:      TypeLimit,
		TIF:       TIFGTC,
		Price:     price,
		Amount:    amount,
		Remaining: amount,
		Timestamp: timestamp,
	}
}

func TestWALPartialFillRestoresRemaining(t *testing.T) {
	const symbol = "WAL-PARTIAL-1/USDT"

	makerBook := NewOrderBook()
	makerBook.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))

	taker := newWALTestOrder("taker-1", Buy, 100, 4, 2)
	trades, changes, _ := makerBook.Match(taker)
	if len(trades) != 1 || trades[0].Quantity != 4 {
		t.Fatalf("trades = %+v, want one trade with quantity 4", trades)
	}
	if makerBook.orders["maker-1"].Remaining != 6 {
		t.Fatalf("maker remaining after match = %d, want 6", makerBook.orders["maker-1"].Remaining)
	}

	entries := WALEntriesFromChanges(symbol, changes)
	if len(entries) != 1 {
		t.Fatalf("WAL entries = %d, want 1: %+v", len(entries), entries)
	}
	if entries[0].Op != WALOpUpdateRemaining || entries[0].OrderID != "maker-1" ||
		entries[0].Symbol != symbol || entries[0].Remaining != 6 || entries[0].Amount != 10 {
		t.Fatalf("unexpected WAL entry: %+v", entries[0])
	}

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))
	seedWAL(t, s.rdb, s.walKey, entries)
	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}

	restored := ob.orders["maker-1"]
	if restored == nil {
		t.Fatal("maker-1 missing after replay")
	}
	if restored.Remaining != 6 || restored.Amount != 10 {
		t.Fatalf("restored maker fields = amount:%d remaining:%d, want 10/6", restored.Amount, restored.Remaining)
	}
	if ob.asks[100] == nil || ob.asks[100].Volume != 6 || ob.asks[100].Count != 1 {
		t.Fatalf("asks[100] after replay = %+v, want volume 6 count 1", ob.asks[100])
	}
}

func TestWALConsecutivePartialFillsRestoreRemaining(t *testing.T) {
	const symbol = "WAL-PARTIAL-2/USDT"

	makerBook := NewOrderBook()
	makerBook.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))

	var changes []OrderChange
	_, firstChanges, _ := makerBook.Match(newWALTestOrder("taker-1", Buy, 100, 3, 2))
	changes = append(changes, firstChanges...)
	_, secondChanges, _ := makerBook.Match(newWALTestOrder("taker-2", Buy, 100, 2, 3))
	changes = append(changes, secondChanges...)

	if makerBook.orders["maker-1"].Remaining != 5 {
		t.Fatalf("maker remaining after two fills = %d, want 5", makerBook.orders["maker-1"].Remaining)
	}

	entries := WALEntriesFromChanges(symbol, changes)
	if len(entries) != 2 {
		t.Fatalf("WAL entries = %d, want 2: %+v", len(entries), entries)
	}
	if entries[0].Remaining != 7 || entries[1].Remaining != 5 {
		t.Fatalf("WAL remaining sequence = %d,%d, want 7,5", entries[0].Remaining, entries[1].Remaining)
	}

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))
	seedWAL(t, s.rdb, s.walKey, entries)
	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}

	restored := ob.orders["maker-1"]
	if restored == nil || restored.Remaining != 5 || restored.Amount != 10 {
		t.Fatalf("restored maker = %+v, want amount 10 remaining 5", restored)
	}
	if ob.asks[100] == nil || ob.asks[100].Volume != 5 || ob.asks[100].Count != 1 {
		t.Fatalf("asks[100] after replay = %+v, want volume 5 count 1", ob.asks[100])
	}
}

func TestWALPartialThenFullRemovesOrder(t *testing.T) {
	const symbol = "WAL-PARTIAL-3/USDT"

	makerBook := NewOrderBook()
	makerBook.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))

	var changes []OrderChange
	_, firstChanges, _ := makerBook.Match(newWALTestOrder("taker-1", Buy, 100, 4, 2))
	changes = append(changes, firstChanges...)
	_, secondChanges, _ := makerBook.Match(newWALTestOrder("taker-2", Buy, 100, 6, 3))
	changes = append(changes, secondChanges...)

	if makerBook.orders["maker-1"] != nil {
		t.Fatal("maker-1 still in memory after full fill")
	}

	entries := WALEntriesFromChanges(symbol, changes)
	if len(entries) != 2 {
		t.Fatalf("WAL entries = %d, want update + remove: %+v", len(entries), entries)
	}
	if entries[0].Op != WALOpUpdateRemaining || entries[0].Remaining != 6 {
		t.Fatalf("first WAL entry = %+v, want UPDATE_REMAINING 6", entries[0])
	}
	if entries[1].Op != WALOpRemove {
		t.Fatalf("second WAL entry = %+v, want REMOVE", entries[1])
	}

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))
	seedWAL(t, s.rdb, s.walKey, entries)
	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}

	if ob.orders["maker-1"] != nil {
		t.Fatal("maker-1 reappeared after partial + full replay")
	}
	if len(ob.asks) != 0 {
		t.Fatalf("asks after replay = %+v, want empty", ob.asks)
	}
	if got := ob.BestAsk(); got != 0 {
		t.Fatalf("best ask after replay = %d, want 0", got)
	}
}

func TestWALPartialThenCancelKeepsOrderRemoved(t *testing.T) {
	const symbol = "WAL-PARTIAL-4/USDT"

	makerBook := NewOrderBook()
	makerBook.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))

	_, changes, _ := makerBook.Match(newWALTestOrder("taker-1", Buy, 100, 4, 2))
	entries := WALEntriesFromChanges(symbol, changes)
	entries = append(entries, WALEntry{
		Op:        WALOpRemove,
		Symbol:    symbol,
		OrderID:   "maker-1",
		UserID:    "user-maker-1",
		Side:      string(Sell),
		Price:     100,
		Remaining: 0,
		Timestamp: 4,
	})

	if _, err := makerBook.CancelOrder("maker-1"); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))
	seedWAL(t, s.rdb, s.walKey, entries)
	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}

	if ob.orders["maker-1"] != nil {
		t.Fatal("cancelled maker-1 reappeared after replay")
	}
	if len(ob.asks) != 0 || ob.BestAsk() != 0 {
		t.Fatalf("asks after cancel replay = %+v, best ask %d", ob.asks, ob.BestAsk())
	}
}

func TestWALDuplicateReplayIsIdempotent(t *testing.T) {
	const symbol = "WAL-DUP/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	entries := []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "maker-1", UserID: "user-maker-1",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
		},
		{
			Op: WALOpUpdateRemaining, Symbol: symbol, OrderID: "maker-1", UserID: "user-maker-1",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 10, Remaining: 6, Timestamp: 2,
		},
	}
	seedWAL(t, s.rdb, s.walKey, entries)

	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("first ReplayWAL: %v", err)
	}
	assertSingleMaker(t, ob, 6)

	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("second ReplayWAL: %v", err)
	}
	assertSingleMaker(t, ob, 6)
}

func TestWALReplayRebuildsAllDataStructures(t *testing.T) {
	const symbol = "WAL-STRUCT/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-100", Sell, 100, 10, 1))
	ob.AddOrder(newWALTestOrder("ask-100b", Sell, 100, 4, 2))
	ob.AddOrder(newWALTestOrder("ask-101", Sell, 101, 5, 3))
	ob.AddOrder(newWALTestOrder("bid-99", Buy, 99, 7, 4))

	seedWAL(t, s.rdb, s.walKey, []WALEntry{
		{
			Op: WALOpUpdateRemaining, Symbol: symbol, OrderID: "ask-100", UserID: "user-ask-100",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 10, Remaining: 6, Timestamp: 5,
		},
	})
	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}

	if len(ob.orders) != 4 {
		t.Fatalf("orders map size = %d, want 4", len(ob.orders))
	}
	if got := ob.orders["ask-100"]; got == nil || got.Amount != 10 || got.Remaining != 6 {
		t.Fatalf("ask-100 = %+v, want amount 10 remaining 6", got)
	}

	ask100 := ob.asks[100]
	if ask100 == nil || ask100.Head == nil || ask100.Tail == nil {
		t.Fatal("asks[100] is nil")
	}
	if ask100.Head.ID != "ask-100" || ask100.Tail.ID != "ask-100b" {
		t.Fatalf("asks[100] list = %s -> %s, want ask-100 -> ask-100b", ask100.Head.ID, ask100.Tail.ID)
	}
	if ask100.Head.Next != ask100.Tail || ask100.Tail.Next != nil {
		t.Fatal("asks[100] linked list is not connected")
	}
	if ask100.Volume != 10 || ask100.Count != 2 {
		t.Fatalf("asks[100] volume=%d count=%d, want 10/2", ask100.Volume, ask100.Count)
	}

	ask101 := ob.asks[101]
	if ask101 == nil || ask101.Volume != 5 || ask101.Count != 1 {
		t.Fatalf("asks[101] = %+v, want volume 5 count 1", ask101)
	}
	bid99 := ob.bids[99]
	if bid99 == nil || bid99.Volume != 7 || bid99.Count != 1 {
		t.Fatalf("bids[99] = %+v, want volume 7 count 1", bid99)
	}
	if len(ob.asks) != 2 || len(ob.bids) != 1 {
		t.Fatalf("price levels = asks:%d bids:%d, want asks:2 bids:1", len(ob.asks), len(ob.bids))
	}
	if got := ob.BestAsk(); got != 100 {
		t.Fatalf("best ask = %d, want 100", got)
	}
	if got := ob.BestBid(); got != 99 {
		t.Fatalf("best bid = %d, want 99", got)
	}
	if got := ob.askPrices.First(); got == nil || got.Price != 100 {
		t.Fatalf("ask skip list first = %+v, want price 100", got)
	}
	if got := ob.bidPrices.First(); got == nil || got.Price != 99 {
		t.Fatalf("bid skip list first = %+v, want price 99", got)
	}
}

func assertSingleMaker(t *testing.T, ob *OrderBook, remaining int64) {
	t.Helper()

	if len(ob.orders) != 1 {
		t.Fatalf("orders map size = %d, want 1", len(ob.orders))
	}
	maker := ob.orders["maker-1"]
	if maker == nil || maker.Remaining != remaining || maker.Amount != 10 {
		t.Fatalf("maker-1 = %+v, want amount 10 remaining %d", maker, remaining)
	}
	list := ob.asks[100]
	if list == nil || list.Head == nil || list.Tail == nil {
		t.Fatal("asks[100] is nil")
	}
	if list.Head.ID != "maker-1" || list.Tail.ID != "maker-1" || list.Head.Next != nil {
		t.Fatalf("asks[100] list = %+v, want only maker-1", list)
	}
	if list.Volume != remaining || list.Count != 1 {
		t.Fatalf("asks[100] volume=%d count=%d, want %d/1", list.Volume, list.Count, remaining)
	}
}

func TestWALTakerPartialRestPreservesRemaining(t *testing.T) {
	const symbol = "WAL-TAKER/USDT"

	makerBook := NewOrderBook()
	makerBook.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))

	taker := newWALTestOrder("taker-1", Buy, 100, 20, 2)
	trades, changes, _ := makerBook.Match(taker)
	if len(trades) != 1 || trades[0].Quantity != 10 {
		t.Fatalf("trades = %+v, want one trade with quantity 10", trades)
	}
	if taker.Remaining != 10 {
		t.Fatalf("taker remaining = %d, want 10", taker.Remaining)
	}

	entries := WALEntriesFromChanges(symbol, changes)
	if len(entries) != 2 {
		t.Fatalf("WAL entries = %d, want remove + add: %+v", len(entries), entries)
	}
	if entries[0].Op != WALOpRemove || entries[0].OrderID != "maker-1" {
		t.Fatalf("first WAL entry = %+v, want REMOVE maker-1", entries[0])
	}
	if entries[1].Op != WALOpAdd || entries[1].OrderID != "taker-1" ||
		entries[1].Amount != 20 || entries[1].Remaining != 10 || entries[1].Symbol != symbol {
		t.Fatalf("second WAL entry = %+v, want ADD taker-1 amount 20 remaining 10", entries[1])
	}

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("maker-1", Sell, 100, 10, 1))
	seedWAL(t, s.rdb, s.walKey, entries)
	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}

	if ob.orders["maker-1"] != nil {
		t.Fatal("filled maker-1 reappeared after replay")
	}
	restored := ob.orders["taker-1"]
	if restored == nil || restored.Amount != 20 || restored.Remaining != 10 {
		t.Fatalf("restored taker = %+v, want amount 20 remaining 10", restored)
	}
	list := ob.bids[100]
	if list == nil || list.Volume != 10 || list.Count != 1 {
		t.Fatalf("bids[100] = %+v, want volume 10 count 1", list)
	}
	if got := ob.BestBid(); got != 100 {
		t.Fatalf("best bid = %d, want 100", got)
	}
}
