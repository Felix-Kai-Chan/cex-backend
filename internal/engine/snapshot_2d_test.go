package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

type commandFailureHook struct {
	mu          sync.Mutex
	failCommand string
	err         error
	calls       map[string]int
}

func newCommandFailureHook(command string, err error) *commandFailureHook {
	return &commandFailureHook{
		failCommand: strings.ToLower(command),
		err:         err,
		calls:       make(map[string]int),
	}
}

func (h *commandFailureHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *commandFailureHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		name := strings.ToLower(cmd.Name())
		h.mu.Lock()
		h.calls[name]++
		fail := name == h.failCommand
		h.mu.Unlock()
		if fail {
			return h.err
		}
		return next(ctx, cmd)
	}
}

func (h *commandFailureHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.mu.Lock()
		for _, cmd := range cmds {
			name := strings.ToLower(cmd.Name())
			h.calls[name]++
			if name == h.failCommand {
				h.mu.Unlock()
				return h.err
			}
		}
		h.mu.Unlock()
		return next(ctx, cmds)
	}
}

func (h *commandFailureHook) callCount(command string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[strings.ToLower(command)]
}

func TestLoadSnapshotGETFailureIsNotTreatedAsMissingSnapshot(t *testing.T) {
	rdb := setupSnapshotRedis(t)
	const symbol = "2D-GET-FAIL/USDT"

	s := NewSnapshotter(rdb, NewOrderBook(), nil, symbol)
	seedSnapshot(t, rdb, s.key, Snapshot{Timestamp: 1})
	defer rdb.Del(context.Background(), s.key, s.walKey)

	injected := errors.New("injected snapshot GET failure")
	hook := newCommandFailureHook("get", injected)
	rdb.AddHook(hook)

	err := s.Load()
	if err == nil {
		t.Fatal("Load succeeded when snapshot GET failed")
	}
	if !strings.Contains(err.Error(), "injected snapshot GET failure") {
		t.Fatalf("Load error = %v, want injected GET failure", err)
	}
	if hook.callCount("lrange") != 0 {
		t.Fatalf("Load fell back after GET failure; LRANGE calls = %d", hook.callCount("lrange"))
	}
	if s.nextSeq.Load() != 0 {
		t.Fatalf("nextSeq advanced after GET failure: %d", s.nextSeq.Load())
	}
}

func TestLoadWALLRANGEFailureIsNotTreatedAsEmptyWAL(t *testing.T) {
	rdb := setupSnapshotRedis(t)
	db := setupSnapshotMySQL(t)
	const symbol = "2D-LRANGE-FAIL/USDT"

	cleanupOrders(t, db, symbol)
	t.Cleanup(func() { cleanupOrders(t, db, symbol) })

	s := NewSnapshotter(rdb, NewOrderBook(), db, symbol)
	seedSnapshot(t, rdb, s.key, Snapshot{Timestamp: 1})
	seedWAL(t, rdb, s.walKey, []WALEntry{{
		Op: WALOpAdd, Symbol: symbol, OrderID: "ask-1", UserID: "u1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 1, Remaining: 1, Timestamp: 1,
	}})
	defer rdb.Del(context.Background(), s.key, s.walKey)

	injected := errors.New("injected WAL LRANGE failure")
	hook := newCommandFailureHook("lrange", injected)
	rdb.AddHook(hook)

	err := s.Load()
	if err == nil {
		t.Fatal("Load succeeded when WAL LRANGE failed")
	}
	if !strings.Contains(err.Error(), "injected WAL LRANGE failure") {
		t.Fatalf("Load error = %v, want injected LRANGE failure", err)
	}
	if hook.callCount("lrange") != 1 {
		t.Fatalf("LRANGE calls = %d, want 1", hook.callCount("lrange"))
	}
	if s.nextSeq.Load() != 0 || s.walInitialized {
		t.Fatalf("WAL treated as empty after LRANGE failure: nextSeq=%d initialized=%v", s.nextSeq.Load(), s.walInitialized)
	}
}

func TestLoadRejectsCorruptWALJSON(t *testing.T) {
	rdb := setupSnapshotRedis(t)
	const symbol = "2D-CORRUPT-JSON/USDT"

	s := NewSnapshotter(rdb, NewOrderBook(), nil, symbol)
	seedSnapshot(t, rdb, s.key, Snapshot{Timestamp: 1})
	if err := rdb.RPush(context.Background(), s.walKey, "{not-json").Err(); err != nil {
		t.Fatalf("seed corrupt WAL: %v", err)
	}
	defer rdb.Del(context.Background(), s.key, s.walKey)

	if err := s.Load(); err == nil {
		t.Fatal("Load succeeded with corrupt WAL JSON")
	}
}

func TestLoadRejectsWALSequenceGap(t *testing.T) {
	rdb := setupSnapshotRedis(t)
	const symbol = "2D-SEQ-GAP/USDT"

	s := NewSnapshotter(rdb, NewOrderBook(), nil, symbol)
	seedSnapshot(t, rdb, s.key, Snapshot{Timestamp: 1})
	seedWAL(t, rdb, s.walKey, []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, EventID: "gap-1", OrderID: "ask-1", UserID: "u1",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 1, Remaining: 1, Timestamp: 1,
		},
		{
			Op: WALOpAdd, Symbol: symbol, EventID: "gap-2", OrderID: "ask-2", UserID: "u2",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 101, Amount: 1, Remaining: 1, Timestamp: 2,
		},
	})
	// seedWAL assigns seq 1,2; rewrite the second entry to seq 3.
	raw, err := rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}
	var entry WALEntry
	if err := decodeWALForTest(raw[1], &entry); err != nil {
		t.Fatalf("decode WAL: %v", err)
	}
	entry.Seq = 3
	if err := overwriteWALForTest(rdb, s.walKey, 1, entry); err != nil {
		t.Fatalf("rewrite WAL: %v", err)
	}
	defer rdb.Del(context.Background(), s.key, s.walKey)

	if err := s.Load(); err == nil {
		t.Fatal("Load succeeded with WAL sequence gap")
	}
}

func TestLoadRejectsMissingWALEventID(t *testing.T) {
	rdb := setupSnapshotRedis(t)
	const symbol = "2D-MISSING-EVENT-ID/USDT"

	s := NewSnapshotter(rdb, NewOrderBook(), nil, symbol)
	seedSnapshot(t, rdb, s.key, Snapshot{Timestamp: 1})
	seedWALRaw(t, rdb, s.walKey, []WALEntry{{
		Op: WALOpAdd, Symbol: symbol, Seq: 1, OrderID: "ask-1", UserID: "u1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 1, Remaining: 1, Timestamp: 1,
	}})
	defer rdb.Del(context.Background(), s.key, s.walKey)

	if err := s.Load(); err == nil {
		t.Fatal("Load succeeded with missing WAL EventID")
	}
}

func TestLoadRejectsUpdateRemainingWithoutOrder(t *testing.T) {
	rdb := setupSnapshotRedis(t)
	const symbol = "2D-UPDATE-MISSING-ORDER/USDT"

	s := NewSnapshotter(rdb, NewOrderBook(), nil, symbol)
	seedSnapshot(t, rdb, s.key, Snapshot{Timestamp: 1})
	seedWAL(t, rdb, s.walKey, []WALEntry{{
		Op: WALOpUpdateRemaining, Symbol: symbol, EventID: "update-missing",
		OrderID: "never-existed", UserID: "u1", Remaining: 1,
		Price: 100, Amount: 1, Timestamp: 1,
	}})
	defer rdb.Del(context.Background(), s.key, s.walKey)

	if err := s.Load(); err == nil {
		t.Fatal("Load succeeded with UPDATE_REMAINING for a missing order")
	}
}

func TestLoadRejectsDuplicateWALEventID(t *testing.T) {
	rdb := setupSnapshotRedis(t)
	const symbol = "2D-DUP-EVENT-ID/USDT"

	s := NewSnapshotter(rdb, NewOrderBook(), nil, symbol)
	seedSnapshot(t, rdb, s.key, Snapshot{Timestamp: 1})
	seedWAL(t, rdb, s.walKey, []WALEntry{
		{
			Op: WALOpAdd, Symbol: symbol, EventID: "same-event", OrderID: "ask-1", UserID: "u1",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 100, Amount: 1, Remaining: 1, Timestamp: 1,
		},
		{
			Op: WALOpAdd, Symbol: symbol, EventID: "same-event", OrderID: "ask-2", UserID: "u2",
			Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
			Price: 101, Amount: 1, Remaining: 1, Timestamp: 2,
		},
	})
	defer rdb.Del(context.Background(), s.key, s.walKey)

	if err := s.Load(); err == nil {
		t.Fatal("Load succeeded with duplicate WAL EventID")
	}
}

func TestLoadRejectsSnapshotCheckpointMismatch(t *testing.T) {
	rdb := setupSnapshotRedis(t)
	const symbol = "2D-CHECKPOINT-MISMATCH/USDT"

	s := NewSnapshotter(rdb, NewOrderBook(), nil, symbol)
	seedSnapshot(t, rdb, s.key, Snapshot{Timestamp: 1, WALSeq: 2})
	seedWAL(t, rdb, s.walKey, []WALEntry{{
		Op: WALOpAdd, Symbol: symbol, EventID: "mismatch-1", OrderID: "ask-1", UserID: "u1",
		Side: string(Sell), Type: TypeLimit, TIF: TIFGTC,
		Price: 100, Amount: 1, Remaining: 1, Timestamp: 1,
	}})
	raw, err := rdb.LRange(context.Background(), s.walKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}
	var entry WALEntry
	if err := decodeWALForTest(raw[0], &entry); err != nil {
		t.Fatalf("decode WAL: %v", err)
	}
	entry.Seq = 4
	if err := overwriteWALForTest(rdb, s.walKey, 0, entry); err != nil {
		t.Fatalf("rewrite WAL: %v", err)
	}
	defer rdb.Del(context.Background(), s.key, s.walKey)

	if err := s.Load(); err == nil {
		t.Fatal("Load succeeded with snapshot checkpoint/WAL mismatch")
	}
}
