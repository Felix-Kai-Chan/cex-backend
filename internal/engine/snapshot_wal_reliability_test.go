package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestWALSequenceFollowsSymbolMatchOrder(t *testing.T) {
	const symbol = "WAL-SEQUENCE/USDT"
	const workers = 32

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("maker-1", Sell, 100, workers+1, 1))

	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			ob.LockTrading()
			defer ob.UnlockTrading()

			taker := newWALTestOrder(fmt.Sprintf("taker-%d", i), Buy, 100, 1, int64(i+2))
			_, changes, _ := ob.Match(taker)
			entries := WALEntriesFromChanges(symbol, changes)
			if err := s.AppendWALBatch(entries); err != nil {
				errs <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("append WAL: %v", err)
	}

	raw, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange WAL: %v", err)
	}
	if len(raw) != workers {
		t.Fatalf("WAL length = %d, want %d", len(raw), workers)
	}

	seen := make(map[string]struct{}, workers)
	for i, entryStr := range raw {
		var entry WALEntry
		if err := json.Unmarshal([]byte(entryStr), &entry); err != nil {
			t.Fatalf("unmarshal WAL[%d]: %v", i, err)
		}
		if entry.Seq != uint64(i+1) {
			t.Fatalf("WAL[%d].Seq = %d, want %d", i, entry.Seq, i+1)
		}
		if entry.EventID == "" {
			t.Fatalf("WAL[%d] missing event_id", i)
		}
		if _, ok := seen[entry.EventID]; ok {
			t.Fatalf("duplicate event_id %s", entry.EventID)
		}
		seen[entry.EventID] = struct{}{}
		if entry.Symbol != symbol || entry.Op != WALOpUpdateRemaining {
			t.Fatalf("WAL[%d] = %+v, want symbol %s op %s", i, entry, symbol, WALOpUpdateRemaining)
		}
		// 每次撮合买入 1，Maker 初始剩余 workers+1；WAL 顺序必须对应撮合顺序。
		wantRemaining := int64(workers - i)
		if entry.Remaining != wantRemaining {
			t.Fatalf("WAL[%d].Remaining = %d, want %d (match/WAL order mismatch)", i, entry.Remaining, wantRemaining)
		}
	}
}

func TestWALSequencesAreIndependentPerSymbol(t *testing.T) {
	symbolA := "WAL-SYMBOL-A/USDT"
	symbolB := "WAL-SYMBOL-B/USDT"
	const count = 10

	sA, _ := newWALTestSnapshotter(t, symbolA)
	sB, _ := newWALTestSnapshotter(t, symbolB)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup

	appendFor := func(s *Snapshotter, symbol string) {
		defer wg.Done()
		<-start
		for i := 0; i < count; i++ {
			entry := WALEntry{
				Op: WALOpAdd, Symbol: symbol, OrderID: fmt.Sprintf("%s-%d", symbol, i),
				UserID: "user", Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
				Price: 100, Amount: 1, Remaining: 1, Timestamp: int64(i + 1),
			}
			if err := s.AppendWALBatch([]WALEntry{entry}); err != nil {
				errs <- err
				return
			}
		}
	}

	wg.Add(2)
	go appendFor(sA, symbolA)
	go appendFor(sB, symbolB)
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("append WAL: %v", err)
	}

	for _, tc := range []struct {
		s      *Snapshotter
		symbol string
	}{{sA, symbolA}, {sB, symbolB}} {
		raw, err := tc.s.rdb.LRange(context.Background(), tc.s.walKey, 0, -1).Result()
		if err != nil {
			t.Fatalf("LRange %s: %v", tc.symbol, err)
		}
		if len(raw) != count {
			t.Fatalf("%s WAL length = %d, want %d", tc.symbol, len(raw), count)
		}
		for i, entryStr := range raw {
			var entry WALEntry
			if err := json.Unmarshal([]byte(entryStr), &entry); err != nil {
				t.Fatalf("unmarshal %s WAL[%d]: %v", tc.symbol, i, err)
			}
			if entry.Seq != uint64(i+1) || entry.Symbol != tc.symbol {
				t.Fatalf("%s WAL[%d] = %+v, want seq %d symbol %s", tc.symbol, i, entry, i+1, tc.symbol)
			}
		}
	}
}

func TestWALReplayRejectsMissingSequence(t *testing.T) {
	const symbol = "WAL-MISSING-SEQ/USDT"
	s, ob := newWALTestSnapshotter(t, symbol)

	seedWALRaw(t, s.rdb, s.walKey, []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, EventID: "event-1", OrderID: "order-1",
			UserID: "user", Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 1, Remaining: 1, Timestamp: 1,
		},
	})

	err := s.ReplayWAL()
	if err == nil || !strings.Contains(err.Error(), "序列号") {
		t.Fatalf("ReplayWAL error = %v, want missing sequence error", err)
	}
	if len(ob.orders) != 0 {
		t.Fatalf("orders changed before WAL validation completed: %d", len(ob.orders))
	}
}

func TestWALReplayRejectsSequenceGap(t *testing.T) {
	const symbol = "WAL-GAP/USDT"
	s, ob := newWALTestSnapshotter(t, symbol)

	seedWALRaw(t, s.rdb, s.walKey, []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, Seq: 1, EventID: "event-1", OrderID: "order-1",
			UserID: "user", Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 1, Remaining: 1, Timestamp: 1,
		},
		{
			Op: WALOpAdd, Symbol: symbol, Seq: 3, EventID: "event-3", OrderID: "order-2",
			UserID: "user", Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 101, Amount: 1, Remaining: 1, Timestamp: 2,
		},
	})

	err := s.ReplayWAL()
	if err == nil || !strings.Contains(err.Error(), "断层") {
		t.Fatalf("ReplayWAL error = %v, want sequence gap error", err)
	}
	if len(ob.orders) != 0 {
		t.Fatalf("orders changed before WAL validation completed: %d", len(ob.orders))
	}
}

func TestWALReplayRejectsOutOfOrderSequence(t *testing.T) {
	const symbol = "WAL-OUT-OF-ORDER/USDT"
	s, ob := newWALTestSnapshotter(t, symbol)

	seedWALRaw(t, s.rdb, s.walKey, []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, Seq: 2, EventID: "event-2", OrderID: "order-2",
			UserID: "user", Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 101, Amount: 1, Remaining: 1, Timestamp: 2,
		},
		{
			Op: WALOpAdd, Symbol: symbol, Seq: 1, EventID: "event-1", OrderID: "order-1",
			UserID: "user", Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 1, Remaining: 1, Timestamp: 1,
		},
	})

	err := s.ReplayWAL()
	if err == nil || !strings.Contains(err.Error(), "乱序") {
		t.Fatalf("ReplayWAL error = %v, want out-of-order error", err)
	}
	if len(ob.orders) != 0 {
		t.Fatalf("orders changed before WAL validation completed: %d", len(ob.orders))
	}
}

func TestWALReplayRejectsUnknownOp(t *testing.T) {
	const symbol = "WAL-UNKNOWN-OP/USDT"
	s, _ := newWALTestSnapshotter(t, symbol)

	seedWALRaw(t, s.rdb, s.walKey, []WALEntry{
		{Op: "UNKNOWN", Symbol: symbol, Seq: 1, EventID: "event-1", OrderID: "order-1"},
	})

	err := s.ReplayWAL()
	if err == nil || !strings.Contains(err.Error(), "不受支持") {
		t.Fatalf("ReplayWAL error = %v, want unknown op error", err)
	}
}

func TestWALAppendFailureMarksSymbolUnavailable(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  50 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
		MaxRetries:   0,
	})
	defer rdb.Close()

	s := NewSnapshotter(rdb, NewOrderBook(), nil, "WAL-APPEND-FAIL/USDT")
	err := s.AppendWALBatch([]WALEntry{
		{Op: WALOpAdd, Symbol: s.symbol, OrderID: "order-1"},
	})
	if err == nil {
		t.Fatal("AppendWALBatch succeeded with unavailable Redis")
	}
	if !s.WALFailed() {
		t.Fatal("WAL failure state not set after append failure")
	}
	if s.WALError() == nil {
		t.Fatal("WALError is nil after append failure")
	}

	if err := s.AppendWALBatch([]WALEntry{
		{Op: WALOpAdd, Symbol: s.symbol, OrderID: "order-2"},
	}); err == nil {
		t.Fatal("AppendWALBatch succeeded after WAL was marked failed")
	}
}

func TestWALReplayAddUpdateRemoveSequence(t *testing.T) {
	const symbol = "WAL-ADD-UPDATE-REMOVE/USDT"
	s, ob := newWALTestSnapshotter(t, symbol)

	seedWAL(t, s.rdb, s.walKey, []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "maker-1", UserID: "user-maker",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
		},
		{
			Op: WALOpUpdateRemaining, Symbol: symbol, OrderID: "maker-1", UserID: "user-maker",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 10, Remaining: 6, Timestamp: 2,
		},
		{
			Op: WALOpRemove, Symbol: symbol, OrderID: "maker-1", UserID: "user-maker",
			Side: string(Sell), Price: 100, Timestamp: 3,
		},
	})

	if err := s.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}
	if len(ob.orders) != 0 || len(ob.asks) != 0 || len(ob.bids) != 0 {
		t.Fatalf("orderbook not empty after ADD/UPDATE/REMOVE: orders=%d asks=%d bids=%d", len(ob.orders), len(ob.asks), len(ob.bids))
	}
	if ob.BestAsk() != 0 || ob.BestBid() != 0 {
		t.Fatalf("best prices after ADD/UPDATE/REMOVE = %d/%d, want 0/0", ob.BestBid(), ob.BestAsk())
	}
	if s.nextSeq != 4 || !s.walInitialized {
		t.Fatalf("next sequence = %d initialized=%v, want 4/true", s.nextSeq, s.walInitialized)
	}
}

func TestWALSequenceContinuesAfterRestart(t *testing.T) {
	const symbol = "WAL-RESTART/USDT"
	s1, _ := newWALTestSnapshotter(t, symbol)
	seedWAL(t, s1.rdb, s1.walKey, []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "order-1", UserID: "user",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 1, Remaining: 1, Timestamp: 1,
		},
	})

	// 新 Snapshotter 模拟重启：必须从 WAL 最后一条事件的 Seq 继续。
	s2 := NewSnapshotter(s1.rdb, NewOrderBook(), nil, symbol)
	entry := WALEntry{
		Op: WALOpUpdateRemaining, Symbol: symbol, OrderID: "order-1", UserID: "user",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 1, Remaining: 1, Timestamp: 2,
	}
	if err := s2.AppendWALBatch([]WALEntry{entry}); err != nil {
		t.Fatalf("AppendWALBatch after restart: %v", err)
	}
	raw, err := s1.rdb.LRange(context.Background(), s1.walKey, -1, -1).Result()
	if err != nil {
		t.Fatalf("LRange WAL: %v", err)
	}
	var last WALEntry
	if err := json.Unmarshal([]byte(raw[0]), &last); err != nil {
		t.Fatalf("unmarshal last WAL: %v", err)
	}
	if last.Seq != 2 {
		t.Fatalf("sequence after restart = %d, want 2", last.Seq)
	}
}
