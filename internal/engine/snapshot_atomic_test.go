package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedLogBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedLogBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func captureSnapshotForTest(ob *OrderBook, walSeq uint64) Snapshot {
	helper := &Snapshotter{}
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return Snapshot{
		Bids:      helper.convertOrderBook(ob.bids),
		Asks:      helper.convertOrderBook(ob.asks),
		Timestamp: 1,
		WALSeq:    walSeq,
	}
}

func restoreSnapshotAndWALForTest(t *testing.T, source *Snapshotter) (*Snapshotter, *OrderBook) {
	t.Helper()

	data, err := source.rdb.Get(context.Background(), source.key).Bytes()
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}

	restored := NewSnapshotter(source.rdb, NewOrderBook(), nil, source.symbol)
	restored.ob.mu.Lock()
	restored.restoreOrdersLocked(snapshotRecords(snapshot))
	restored.ob.mu.Unlock()
	restored.snapshotWALSeq.Store(snapshot.WALSeq)
	if err := restored.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}
	return restored, restored.ob
}

func walLength(t *testing.T, s *Snapshotter) int {
	t.Helper()
	n, err := s.rdb.LLen(context.Background(), s.walKey).Result()
	if err != nil {
		t.Fatalf("LLEN WAL: %v", err)
	}
	return int(n)
}

func TestSnapshotSaveRejectedWhenWALFailed(t *testing.T) {
	const symbol = "ATOMIC-WAL-FAILED/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-1", Sell, 100, 10, 1))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-1", UserID: "user-1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
	}}); err != nil {
		t.Fatalf("append first WAL: %v", err)
	}
	if err := s.Save(); err != nil {
		t.Fatalf("initial Save: %v", err)
	}
	ob.AddOrder(newWALTestOrder("ask-2", Sell, 101, 5, 2))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-2", UserID: "user-2",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 101, Amount: 5, Remaining: 5, Timestamp: 2,
	}}); err != nil {
		t.Fatalf("append second WAL: %v", err)
	}

	oldSnapshot, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("read old snapshot: %v", err)
	}
	oldWAL, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read old WAL: %v", err)
	}

	// 模拟订单簿已变化，但对应 WAL 追加失败。
	ob.AddOrder(newWALTestOrder("ask-unlogged", Sell, 102, 3, 3))
	s.markWALFailure(errors.New("injected WAL append failure"))

	switchCalled := false
	s.beforeSnapshotSwitchHook = func() error {
		switchCalled = true
		return errors.New("snapshot switch should not be reached")
	}
	err = s.Save()
	s.beforeSnapshotSwitchHook = nil
	if err == nil {
		t.Fatal("Save succeeded while WAL failed")
	}
	if switchCalled {
		t.Fatal("Save reached snapshot switch while WAL failed")
	}
	if !strings.Contains(err.Error(), "WAL 已失败") {
		t.Fatalf("Save error = %v, want explicit WAL failure", err)
	}

	newSnapshot, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("read snapshot after rejected Save: %v", err)
	}
	if string(newSnapshot) != string(oldSnapshot) {
		t.Fatal("snapshot was overwritten while WAL failed")
	}
	newWAL, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read WAL after rejected Save: %v", err)
	}
	if len(newWAL) != len(oldWAL) {
		t.Fatalf("WAL changed while WAL failed: got %d entries, want %d", len(newWAL), len(oldWAL))
	}
	for i := range oldWAL {
		if oldWAL[i] != newWAL[i] {
			t.Fatalf("WAL entry %d changed while WAL failed", i)
		}
	}
}

func TestSnapshotSaveSucceedsWhenWALHealthy(t *testing.T) {
	const symbol = "ATOMIC-WAL-HEALTHY/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-1", Sell, 100, 10, 1))
	ob.AddOrder(newWALTestOrder("bid-1", Buy, 99, 4, 2))
	entries := []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "ask-1", UserID: "user-1",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
		},
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "bid-1", UserID: "user-2",
			Side: string(Buy), Type: TypeLimit, TIF: TIFGTC,
			Price: 99, Amount: 4, Remaining: 4, Timestamp: 2,
		},
	}
	if err := s.AppendWALBatch(entries); err != nil {
		t.Fatalf("append WAL: %v", err)
	}
	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got := walLength(t, s); got != 0 {
		t.Fatalf("WAL length after Save = %d, want 0", got)
	}

	var snapshot Snapshot
	data, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snapshot.WALSeq != 2 {
		t.Fatalf("snapshot checkpoint = %d, want 2", snapshot.WALSeq)
	}
	if got := s.nextSeq.Load(); got != 3 {
		t.Fatalf("next sequence after Save = %d, want 3", got)
	}
	if s.WALFailed() {
		t.Fatal("healthy WAL unexpectedly marked failed")
	}
}

func TestAutoSnapshotRejectedWhenWALFailed(t *testing.T) {
	const symbol = "ATOMIC-AUTO-WAL-FAILED/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-1", Sell, 100, 10, 1))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-1", UserID: "user-1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
	}}); err != nil {
		t.Fatalf("append first WAL: %v", err)
	}
	if err := s.Save(); err != nil {
		t.Fatalf("initial Save: %v", err)
	}
	ob.AddOrder(newWALTestOrder("ask-2", Sell, 101, 5, 2))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-2", UserID: "user-2",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 101, Amount: 5, Remaining: 5, Timestamp: 2,
	}}); err != nil {
		t.Fatalf("append second WAL: %v", err)
	}
	oldSnapshot, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("read old snapshot: %v", err)
	}
	oldWAL, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read old WAL: %v", err)
	}

	ob.AddOrder(newWALTestOrder("ask-unlogged", Sell, 102, 3, 3))
	s.markWALFailure(errors.New("injected automation WAL failure"))

	logs := &lockedLogBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	s.autoSaveInterval = 10 * time.Millisecond
	s.StartAutoSave()
	t.Cleanup(s.Stop)

	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logs.String(), "snapshot save failed") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), "snapshot save failed") {
		t.Fatalf("auto snapshot did not fail: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "WAL 已失败") {
		t.Fatalf("auto snapshot failure did not mention WAL failure: %s", logs.String())
	}

	newSnapshot, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("read snapshot after auto failure: %v", err)
	}
	if string(newSnapshot) != string(oldSnapshot) {
		t.Fatal("auto snapshot overwrote old snapshot while WAL failed")
	}
	newWAL, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read WAL after auto failure: %v", err)
	}
	if len(newWAL) != len(oldWAL) {
		t.Fatalf("auto snapshot changed WAL: got %d entries, want %d", len(newWAL), len(oldWAL))
	}
}

func TestSnapshotAtomicSwitchAndRestart(t *testing.T) {
	const symbol = "ATOMIC-SWITCH/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-100", Sell, 100, 10, 1))
	ob.AddOrder(newWALTestOrder("bid-99", Buy, 99, 7, 2))
	if err := s.AppendWALBatch([]WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "ask-100", UserID: "user-ask",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
		},
		{
			Op: WALOpAdd, Symbol: symbol, OrderID: "bid-99", UserID: "user-bid",
			Side: string(Buy), Type: TypeLimit, TIF: TIFGTC,
			Price: 99, Amount: 7, Remaining: 7, Timestamp: 2,
		},
	}); err != nil {
		t.Fatalf("append initial WAL: %v", err)
	}

	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got := walLength(t, s); got != 0 {
		t.Fatalf("WAL length after save = %d, want 0", got)
	}
	snapshotData, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(snapshotData, &snapshot); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snapshot.WALSeq != 2 {
		t.Fatalf("snapshot checkpoint = %d, want 2", snapshot.WALSeq)
	}

	ob.AddOrder(newWALTestOrder("ask-101", Sell, 101, 5, 3))
	newEntries := []WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-101", UserID: "user-ask-2",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 101, Amount: 5, Remaining: 5, Timestamp: 3,
	}}
	if err := s.AppendWALBatch(newEntries); err != nil {
		t.Fatalf("append post-save WAL: %v", err)
	}
	if newEntries[0].Seq != 3 {
		t.Fatalf("post-save sequence = %d, want 3", newEntries[0].Seq)
	}

	restarted, restoredOB := restoreSnapshotAndWALForTest(t, s)
	if len(restoredOB.orders) != 3 {
		t.Fatalf("orders after restart = %d, want 3", len(restoredOB.orders))
	}
	if restoredOB.orders["ask-100"] == nil || restoredOB.orders["bid-99"] == nil || restoredOB.orders["ask-101"] == nil {
		t.Fatalf("orders after restart incomplete: %+v", restoredOB.orders)
	}
	if got := restoredOB.asks[100]; got == nil || got.Volume != 10 || got.Count != 1 {
		t.Fatalf("asks[100] after restart = %+v, want volume 10 count 1", got)
	}
	if got := restarted.nextSeq.Load(); got != 4 {
		t.Fatalf("next sequence after restart = %d, want 4", got)
	}
}

func TestSnapshotSwitchFailureKeepsOldSnapshotAndWAL(t *testing.T) {
	const symbol = "ATOMIC-BEFORE-SWITCH/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-1", Sell, 100, 10, 1))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-1", UserID: "user-1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
	}}); err != nil {
		t.Fatalf("append first WAL: %v", err)
	}
	seedSnapshot(t, s.rdb, s.key, captureSnapshotForTest(ob, 1))

	oldSnapshot, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("read old snapshot: %v", err)
	}
	oldWAL, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read old WAL: %v", err)
	}

	ob.AddOrder(newWALTestOrder("ask-2", Sell, 101, 5, 2))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-2", UserID: "user-2",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 101, Amount: 5, Remaining: 5, Timestamp: 2,
	}}); err != nil {
		t.Fatalf("append second WAL: %v", err)
	}

	s.beforeSnapshotSwitchHook = func() error {
		return errors.New("injected crash before snapshot switch")
	}
	if err := s.Save(); err == nil {
		t.Fatal("Save succeeded despite injected pre-switch failure")
	}
	s.beforeSnapshotSwitchHook = nil

	newSnapshot, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("read snapshot after failed save: %v", err)
	}
	if string(newSnapshot) != string(oldSnapshot) {
		t.Fatal("old snapshot was overwritten after failed save")
	}
	newWAL, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read WAL after failed save: %v", err)
	}
	if len(newWAL) != len(oldWAL)+1 {
		t.Fatalf("WAL length after failed save = %d, want %d", len(newWAL), len(oldWAL)+1)
	}

	_, restoredOB := restoreSnapshotAndWALForTest(t, s)
	if len(restoredOB.orders) != 2 {
		t.Fatalf("orders after old snapshot + WAL recovery = %d, want 2", len(restoredOB.orders))
	}
	if restoredOB.orders["ask-1"] == nil || restoredOB.orders["ask-2"] == nil {
		t.Fatalf("old snapshot + WAL recovery lost orders: %+v", restoredOB.orders)
	}
}

func TestSnapshotCrashAfterSnapshotBeforeTrimIsRecoverable(t *testing.T) {
	const symbol = "ATOMIC-AFTER-SNAPSHOT/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-1", Sell, 100, 10, 1))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-1", UserID: "user-1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
	}}); err != nil {
		t.Fatalf("append first WAL: %v", err)
	}
	ob.AddOrder(newWALTestOrder("ask-2", Sell, 101, 5, 2))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-2", UserID: "user-2",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 101, Amount: 5, Remaining: 5, Timestamp: 2,
	}}); err != nil {
		t.Fatalf("append second WAL: %v", err)
	}

	// 模拟：新 Snapshot 已写入，但 WAL 尚未截断。
	seedSnapshot(t, s.rdb, s.key, captureSnapshotForTest(ob, 2))

	// 模拟切换窗口之后又产生一笔新事件。
	ob.AddOrder(newWALTestOrder("ask-3", Sell, 102, 3, 3))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-3", UserID: "user-3",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 102, Amount: 3, Remaining: 3, Timestamp: 3,
	}}); err != nil {
		t.Fatalf("append third WAL: %v", err)
	}

	if got := walLength(t, s); got != 3 {
		t.Fatalf("simulated crash WAL length = %d, want 3", got)
	}

	_, restoredOB := restoreSnapshotAndWALForTest(t, s)
	if len(restoredOB.orders) != 3 {
		t.Fatalf("orders after snapshot-before-trim recovery = %d, want 3", len(restoredOB.orders))
	}
	if restoredOB.orders["ask-1"] == nil || restoredOB.orders["ask-2"] == nil || restoredOB.orders["ask-3"] == nil {
		t.Fatalf("snapshot-before-trim recovery lost orders: %+v", restoredOB.orders)
	}
	if got := restoredOB.asks[102]; got == nil || got.Volume != 3 || got.Count != 1 {
		t.Fatalf("asks[102] after recovery = %+v, want volume 3 count 1", got)
	}
}

func TestSnapshotAtomicSwitchFailureDoesNotOverwriteSnapshot(t *testing.T) {
	const symbol = "ATOMIC-SWITCH-FAIL/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-1", Sell, 100, 10, 1))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-1", UserID: "user-1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
	}}); err != nil {
		t.Fatalf("append WAL: %v", err)
	}
	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	oldSnapshot, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("read old snapshot: %v", err)
	}

	// 在 WAL 末尾注入一条缺少 seq 的事件，使 Lua 在 SET Snapshot 之前失败。
	seedWALRaw(t, s.rdb, s.walKey, []WALEntry{{
		Op: WALOpAdd, Symbol: symbol, EventID: "bad-event", OrderID: "bad-order",
	}})
	if err := s.Save(); err == nil {
		t.Fatal("Save succeeded despite malformed WAL")
	}

	newSnapshot, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("read snapshot after switch failure: %v", err)
	}
	if string(newSnapshot) != string(oldSnapshot) {
		t.Fatal("snapshot was overwritten although atomic switch failed")
	}
	if got := walLength(t, s); got != 1 {
		t.Fatalf("WAL length after switch failure = %d, want 1", got)
	}
}

func TestSnapshotSwitchPreservesEventsAfterCheckpoint(t *testing.T) {
	const symbol = "ATOMIC-TAIL/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-1", Sell, 100, 10, 1))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-1", UserID: "user-1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
	}}); err != nil {
		t.Fatalf("append first WAL: %v", err)
	}

	var tailEntries []WALEntry
	s.beforeSnapshotSwitchHook = func() error {
		// Simulate a trade that finishes after Save captured the snapshot but
		// before the atomic snapshot/WAL switch executes.
		ob.AddOrder(newWALTestOrder("ask-2", Sell, 101, 5, 2))
		tailEntries = []WALEntry{{
			Op: WALOpAdd, Symbol: symbol, OrderID: "ask-2", UserID: "user-2",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 101, Amount: 5, Remaining: 5, Timestamp: 2,
		}}
		return s.AppendWALBatch(tailEntries)
	}
	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	s.beforeSnapshotSwitchHook = nil

	if len(tailEntries) != 1 || tailEntries[0].Seq != 2 {
		t.Fatalf("tail WAL sequence = %+v, want seq 2", tailEntries)
	}
	if got := walLength(t, s); got != 1 {
		t.Fatalf("WAL length after switch = %d, want 1 tail event", got)
	}

	var snapshot Snapshot
	data, err := s.rdb.Get(context.Background(), s.key).Bytes()
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snapshot.WALSeq != 1 {
		t.Fatalf("snapshot checkpoint = %d, want 1", snapshot.WALSeq)
	}

	_, restoredOB := restoreSnapshotAndWALForTest(t, s)
	if len(restoredOB.orders) != 2 {
		t.Fatalf("orders after tail recovery = %d, want 2", len(restoredOB.orders))
	}
	if restoredOB.orders["ask-1"] == nil || restoredOB.orders["ask-2"] == nil {
		t.Fatalf("tail recovery lost orders: %+v", restoredOB.orders)
	}
}

func TestSnapshotSwitchThenContinueAndRestart(t *testing.T) {
	const symbol = "ATOMIC-CONTINUE/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-1", Sell, 100, 10, 1))
	if err := s.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-1", UserID: "user-1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
	}}); err != nil {
		t.Fatalf("append first WAL: %v", err)
	}
	if err := s.Save(); err != nil {
		t.Fatalf("first Save: %v", err)
	}

	ob.AddOrder(newWALTestOrder("ask-2", Sell, 101, 5, 2))
	secondEntries := []WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-2", UserID: "user-2",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 101, Amount: 5, Remaining: 5, Timestamp: 2,
	}}
	if err := s.AppendWALBatch(secondEntries); err != nil {
		t.Fatalf("append second WAL: %v", err)
	}
	if secondEntries[0].Seq != 2 {
		t.Fatalf("second sequence = %d, want 2", secondEntries[0].Seq)
	}
	if err := s.Save(); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	if got := walLength(t, s); got != 0 {
		t.Fatalf("WAL after second Save = %d, want 0", got)
	}

	ob.AddOrder(newWALTestOrder("ask-3", Sell, 102, 3, 3))
	thirdEntries := []WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-3", UserID: "user-3",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 102, Amount: 3, Remaining: 3, Timestamp: 3,
	}}
	if err := s.AppendWALBatch(thirdEntries); err != nil {
		t.Fatalf("append third WAL: %v", err)
	}
	if thirdEntries[0].Seq != 3 {
		t.Fatalf("third sequence = %d, want 3", thirdEntries[0].Seq)
	}

	restarted, restoredOB := restoreSnapshotAndWALForTest(t, s)
	if len(restoredOB.orders) != 3 {
		t.Fatalf("orders after final restart = %d, want 3", len(restoredOB.orders))
	}
	if got := restarted.nextSeq.Load(); got != 4 {
		t.Fatalf("next sequence after final restart = %d, want 4", got)
	}
}

func TestSnapshotConcurrentTradingAndSave(t *testing.T) {
	const symbol = "ATOMIC-CONCURRENT/USDT"
	const workers = 8
	const fillsPerWorker = 10
	const totalFills = workers * fillsPerWorker
	const initialAmount = totalFills + 1

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("maker-1", Sell, 100, initialAmount, 1))

	start := make(chan struct{})
	errs := make(chan error, workers+1)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for j := 0; j < fillsPerWorker; j++ {
				ob.LockTrading()
				taker := newWALTestOrder(
					fmt.Sprintf("taker-%d-%d", worker, j),
					Buy, 100, 1, int64(worker*fillsPerWorker+j+2),
				)
				_, changes, _ := ob.Match(taker)
				err := s.AppendWALBatch(WALEntriesFromChanges(symbol, changes))
				ob.UnlockTrading()
				if err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 6; i++ {
			if err := s.Save(); err != nil {
				errs <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent trading/save: %v", err)
	}

	if err := s.Save(); err != nil {
		t.Fatalf("final Save: %v", err)
	}
	if got := walLength(t, s); got != 0 {
		t.Fatalf("WAL after final Save = %d, want 0", got)
	}

	restarted, restoredOB := restoreSnapshotAndWALForTest(t, s)
	if len(restoredOB.orders) != 1 {
		t.Fatalf("orders after concurrent recovery = %d, want 1", len(restoredOB.orders))
	}
	maker := restoredOB.orders["maker-1"]
	if maker == nil || maker.Amount != initialAmount || maker.Remaining != 1 {
		t.Fatalf("maker after concurrent recovery = %+v, want amount %d remaining 1", maker, initialAmount)
	}
	list := restoredOB.asks[100]
	if list == nil || list.Volume != 1 || list.Count != 1 || list.Head.ID != "maker-1" || list.Tail.ID != "maker-1" {
		t.Fatalf("asks[100] after concurrent recovery = %+v", list)
	}
	if got := restarted.nextSeq.Load(); got != totalFills+1 {
		t.Fatalf("next sequence after concurrent recovery = %d, want %d", got, totalFills+1)
	}
}

func TestSnapshotMultipleSymbolsIndependent(t *testing.T) {
	const symbolA = "ATOMIC-SYMBOL-A/USDT"
	const symbolB = "ATOMIC-SYMBOL-B/USDT"

	sA, obA := newWALTestSnapshotter(t, symbolA)
	sB, obB := newWALTestSnapshotter(t, symbolB)

	obA.AddOrder(newWALTestOrder("a-1", Sell, 100, 10, 1))
	obB.AddOrder(newWALTestOrder("b-1", Buy, 90, 7, 1))
	if err := sA.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbolA, OrderID: "a-1", UserID: "user-a",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 10, Remaining: 10, Timestamp: 1,
	}}); err != nil {
		t.Fatalf("append A WAL: %v", err)
	}
	if err := sB.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbolB, OrderID: "b-1", UserID: "user-b",
		Side: string(Buy), Type: TypeLimit, TIF: TIFGTC,
		Price: 90, Amount: 7, Remaining: 7, Timestamp: 1,
	}}); err != nil {
		t.Fatalf("append B WAL: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; errs <- sA.Save() }()
	go func() { defer wg.Done(); <-start; errs <- sB.Save() }()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("save symbol: %v", err)
		}
	}

	if walLength(t, sA) != 0 || walLength(t, sB) != 0 {
		t.Fatalf("symbol WALs not independent: A=%d B=%d", walLength(t, sA), walLength(t, sB))
	}

	obA.AddOrder(newWALTestOrder("a-2", Sell, 101, 3, 2))
	obB.AddOrder(newWALTestOrder("b-2", Buy, 89, 4, 2))
	if err := sA.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbolA, OrderID: "a-2", UserID: "user-a",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 101, Amount: 3, Remaining: 3, Timestamp: 2,
	}}); err != nil {
		t.Fatalf("append A tail WAL: %v", err)
	}
	if err := sB.AppendWALBatch([]WALEntry{{
		Op: WALOpAdd, Symbol: symbolB, OrderID: "b-2", UserID: "user-b",
		Side: string(Buy), Type: TypeLimit, TIF: TIFGTC,
		Price: 89, Amount: 4, Remaining: 4, Timestamp: 2,
	}}); err != nil {
		t.Fatalf("append B tail WAL: %v", err)
	}

	_, restoredA := restoreSnapshotAndWALForTest(t, sA)
	_, restoredB := restoreSnapshotAndWALForTest(t, sB)
	if len(restoredA.orders) != 2 || restoredA.orders["a-1"] == nil || restoredA.orders["a-2"] == nil {
		t.Fatalf("A recovery wrong: %+v", restoredA.orders)
	}
	if len(restoredB.orders) != 2 || restoredB.orders["b-1"] == nil || restoredB.orders["b-2"] == nil {
		t.Fatalf("B recovery wrong: %+v", restoredB.orders)
	}
}

func TestSnapshotCheckpointMismatchFails(t *testing.T) {
	const symbol = "ATOMIC-CHECKPOINT-MISMATCH/USDT"

	s, ob := newWALTestSnapshotter(t, symbol)
	ob.AddOrder(newWALTestOrder("ask-1", Sell, 100, 10, 1))
	seedSnapshot(t, s.rdb, s.key, captureSnapshotForTest(ob, 2))
	seedWAL(t, s.rdb, s.walKey, []WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-2", UserID: "user-2",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 101, Amount: 5, Remaining: 5, Timestamp: 2,
	}})
	// 手工把唯一 WAL 事件的 seq 改成 3，制造 Snapshot checkpoint=2 与 WAL seq 的断层。
	entries, err := s.rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange WAL: %v", err)
	}
	var entry WALEntry
	if err := json.Unmarshal([]byte(entries[0]), &entry); err != nil {
		t.Fatalf("unmarshal WAL: %v", err)
	}
	entry.Seq = 4
	data, _ := json.Marshal(entry)
	if err := s.rdb.LSet(context.Background(), s.walKey, 0, string(data)).Err(); err != nil {
		t.Fatalf("rewrite WAL seq: %v", err)
	}

	restored := NewSnapshotter(s.rdb, NewOrderBook(), nil, symbol)
	restored.ob.mu.Lock()
	restored.restoreOrdersLocked(snapshotRecords(captureSnapshotForTest(ob, 2)))
	restored.ob.mu.Unlock()
	restored.snapshotWALSeq.Store(2)
	if err := restored.ReplayWAL(); err == nil {
		t.Fatal("ReplayWAL succeeded despite checkpoint/sequence mismatch")
	}
}
