package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cex-backend/internal/engine"
	"cex-backend/internal/persistence"
	"cex-backend/internal/websocket"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type cancelTestEnv struct {
	t           *testing.T
	db          *gorm.DB
	rdb         *redis.Client
	eng         *engine.Engine
	svc         *OrderService
	repo        *persistence.TradeRepo
	balanceRepo *persistence.BalanceRepo
	outboxRepo  *persistence.OutboxRepo
	snapshotter *engine.Snapshotter
	ob          *engine.OrderBook
	symbol      string
	users       []string
}

func setupCancelTestDB(t *testing.T) *gorm.DB {
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
	if err := db.AutoMigrate(
		&persistence.OrderModel{},
		&persistence.TradeModel{},
		&persistence.BalanceModel{},
		&persistence.LedgerModel{},
		&persistence.OutboxModel{},
	); err != nil {
		t.Fatalf("migrate test tables: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func setupCancelTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:16379"
	}
	dbIndex := 11
	if raw := os.Getenv("TEST_REDIS_DB"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("invalid TEST_REDIS_DB: %v", err)
		}
		dbIndex = parsed
	}
	rdb := engine.NewRedisClient(addr, "", dbIndex)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("Redis unavailable: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func newCancelTestEnv(t *testing.T, symbol string, users ...string) *cancelTestEnv {
	t.Helper()
	db := setupCancelTestDB(t)
	rdb := setupCancelTestRedis(t)
	eng := engine.NewEngine()
	ob := eng.GetOrderBook(symbol)
	snapshotter := engine.NewSnapshotter(rdb, ob, db, symbol)

	ctx := context.Background()
	_ = rdb.Del(ctx, snapshotterKeyForTest(symbol), walKeyForTest(symbol))
	cleanupCancelTestData(t, db, users, symbol)
	t.Cleanup(func() {
		cleanupCancelTestData(t, db, users, symbol)
		_ = rdb.Del(context.Background(), snapshotterKeyForTest(symbol), walKeyForTest(symbol))
	})

	if err := snapshotter.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	repo := persistence.NewTradeRepo(db)
	balanceRepo := persistence.NewBalanceRepo(db)
	ledgerRepo := persistence.NewLedgerRepo(db)
	outboxRepo := persistence.NewOutboxRepo(db)
	hub := websocket.NewHub()
	go hub.Run()
	svc := NewOrderService(eng, repo, ledgerRepo, hub, balanceRepo, db, outboxRepo)
	eng.RegisterSnapshotter(symbol, snapshotter)

	return &cancelTestEnv{
		t:           t,
		db:          db,
		rdb:         rdb,
		eng:         eng,
		svc:         svc,
		repo:        repo,
		balanceRepo: balanceRepo,
		outboxRepo:  outboxRepo,
		snapshotter: snapshotter,
		ob:          ob,
		symbol:      symbol,
		users:       users,
	}
}

func snapshotterKeyForTest(symbol string) string { return "orderbook:snapshot:" + symbol }
func walKeyForTest(symbol string) string         { return "orderbook:wal:" + symbol }

func cleanupCancelTestData(t *testing.T, db *gorm.DB, users []string, symbol string) {
	t.Helper()
	var orderIDs []string
	if len(users) > 0 {
		_ = db.Model(&persistence.OrderModel{}).
			Where("user_id IN ?", users).
			Pluck("order_id", &orderIDs).Error
	}
	if len(orderIDs) > 0 {
		_ = db.Where("aggregate_id IN ?", orderIDs).Delete(&persistence.OutboxModel{}).Error
		_ = db.Where("buy_order_id IN ? OR sell_order_id IN ?", orderIDs, orderIDs).Delete(&persistence.TradeModel{}).Error
	}
	if len(users) > 0 {
		_ = db.Where("user_id IN ?", users).Delete(&persistence.LedgerModel{}).Error
		_ = db.Where("user_id IN ?", users).Delete(&persistence.OrderModel{}).Error
		_ = db.Where("user_id IN ?", users).Delete(&persistence.BalanceModel{}).Error
	}
}

func (e *cancelTestEnv) addBalance(user, asset string, amount float64) {
	e.t.Helper()
	if err := e.balanceRepo.AddBalance(user, asset, amount); err != nil {
		e.t.Fatalf("add balance %s %s: %v", user, asset, err)
	}
}

func (e *cancelTestEnv) createOrder(user, side, orderType, tif string, price, amount int64) *CreateOrderResponse {
	e.t.Helper()
	resp, err := e.svc.CreateOrder(&CreateOrderRequest{
		UserID:      user,
		Symbol:      e.symbol,
		Side:        side,
		OrderType:   orderType,
		TimeInForce: tif,
		Price:       price,
		Amount:      amount,
	})
	if err != nil {
		e.t.Fatalf("CreateOrder %s %s: %v", side, orderType, err)
	}
	return resp
}

func (e *cancelTestEnv) order(orderID string) persistence.OrderModel {
	e.t.Helper()
	var order persistence.OrderModel
	if err := e.db.Where("order_id = ?", orderID).First(&order).Error; err != nil {
		e.t.Fatalf("get order %s: %v", orderID, err)
	}
	return order
}

func (e *cancelTestEnv) balance(user, asset string) persistence.BalanceModel {
	e.t.Helper()
	var bal persistence.BalanceModel
	if err := e.db.Where("user_id = ? AND asset = ?", user, asset).First(&bal).Error; err != nil {
		e.t.Fatalf("get balance %s/%s: %v", user, asset, err)
	}
	return bal
}

func (e *cancelTestEnv) outboxCount(orderID string) int64 {
	e.t.Helper()
	var msgs []persistence.OutboxModel
	if err := e.db.Where("aggregate_id = ?", orderID).Find(&msgs).Error; err != nil {
		e.t.Fatalf("query outbox: %v", err)
	}
	var count int64
	for _, msg := range msgs {
		if strings.Contains(msg.Payload, `"event_type":"ORDER_CANCELLED"`) {
			count++
		}
	}
	return count
}

func (e *cancelTestEnv) walRemoveCount(orderID string) int {
	e.t.Helper()
	raw, err := e.rdb.LRange(context.Background(), walKeyForTest(e.symbol), 0, -1).Result()
	if err != nil {
		e.t.Fatalf("read WAL: %v", err)
	}
	count := 0
	for _, item := range raw {
		if strings.Contains(item, `"op":"REMOVE"`) && strings.Contains(item, orderID) {
			count++
		}
	}
	return count
}

func (e *cancelTestEnv) tradeCountFor(orderID string) int64 {
	e.t.Helper()
	var count int64
	if err := e.db.Model(&persistence.TradeModel{}).
		Where("buy_order_id = ? OR sell_order_id = ?", orderID, orderID).
		Count(&count).Error; err != nil {
		e.t.Fatalf("count trades: %v", err)
	}
	return count
}

func TestCancelConcurrentRequestsUnfreezeOnce(t *testing.T) {
	const symbol = "3A-CONCURRENT/USDT"
	user := "3a_concurrent_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, user)
	env.addBalance(user, "3A-CONCURRENT", 100)

	maker := env.createOrder(user, "SELL", "LIMIT", "GTC", 100, 10)
	orderID := maker.OrderID
	before := env.balance(user, "3A-CONCURRENT")
	if before.Frozen < 10 {
		t.Fatalf("maker frozen = %v, want >= 10", before.Frozen)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			errs <- env.svc.CancelOrder(orderID, user)
		}()
	}
	wg.Wait()
	close(errs)

	success := 0
	failures := 0
	for err := range errs {
		if err == nil {
			success++
		} else {
			failures++
		}
	}
	if success != 1 || failures != 1 {
		t.Fatalf("concurrent cancel results: success=%d failures=%d, want 1/1", success, failures)
	}

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusCanceled {
		t.Fatalf("DB status = %s, want CANCELED", order.Status)
	}
	after := env.balance(user, "3A-CONCURRENT")
	if after.Frozen != before.Frozen-10 {
		t.Fatalf("frozen = %v, want %v; balance=%+v", after.Frozen, before.Frozen-10, after)
	}
	if after.Available != before.Available+10 {
		t.Fatalf("available = %v, want %v", after.Available, before.Available+10)
	}
	if env.outboxCount(orderID) != 1 {
		t.Fatalf("cancel outbox count = %d, want 1", env.outboxCount(orderID))
	}
	if got := env.walRemoveCount(orderID); got != 1 {
		t.Fatalf("WAL REMOVE count = %d, want 1", got)
	}
}

func TestCancelAndMatchConcurrentForSameMaker(t *testing.T) {
	const symbol = "3A-CANCEL-MATCH/USDT"
	makerUser := "3a_cm_maker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	takerUser := "3a_cm_taker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, makerUser, takerUser)
	env.addBalance(makerUser, "3A-CANCEL-MATCH", 10)
	env.addBalance(takerUser, "USDT", 10000)

	maker := env.createOrder(makerUser, "SELL", "LIMIT", "GTC", 100, 1)
	orderID := maker.OrderID

	var wg sync.WaitGroup
	var cancelErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		cancelErr = env.svc.CancelOrder(orderID, makerUser)
	}()
	go func() {
		defer wg.Done()
		_, err := env.svc.CreateOrder(&CreateOrderRequest{
			UserID: takerUser, Symbol: symbol, Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", Price: 100, Amount: 1,
		})
		if err != nil {
			t.Errorf("taker CreateOrder: %v", err)
		}
	}()
	wg.Wait()

	order := env.order(orderID)
	trades := env.tradeCountFor(orderID)
	switch order.Status {
	case persistence.OrderStatusCanceled:
		if cancelErr != nil {
			t.Fatalf("maker CANCELED but cancel error = %v", cancelErr)
		}
		if trades != 0 {
			t.Fatalf("maker CANCELED but trades = %d, want 0", trades)
		}
	case persistence.OrderStatusFilled:
		if cancelErr == nil {
			t.Fatal("maker FILLED but cancel succeeded")
		}
		if trades != 1 {
			t.Fatalf("maker FILLED but trades = %d, want 1", trades)
		}
	default:
		t.Fatalf("maker status = %s, want CANCELED or FILLED", order.Status)
	}
}

func TestCancelMySQLTransactionFailureLeavesMemoryAndDBUnchanged(t *testing.T) {
	const symbol = "3A-DB-FAIL/USDT"
	user := "3a_dbfail_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, user)
	env.addBalance(user, "3A-DB-FAIL", 10)

	maker := env.createOrder(user, "SELL", "LIMIT", "GTC", 100, 10)
	orderID := maker.OrderID
	before := env.balance(user, "3A-DB-FAIL")
	beforeWAL := env.walRemoveCount(orderID)

	var fired atomic.Bool
	callbackName := fmt.Sprintf("test_fail_cancel_update_%d", time.Now().UnixNano())
	if err := env.db.Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table == "orders" && fired.CompareAndSwap(false, true) {
			tx.AddError(errors.New("injected cancel DB failure"))
		}
	}); err != nil {
		t.Fatalf("register callback: %v", err)
	}
	t.Cleanup(func() { _ = env.db.Callback().Update().Remove(callbackName) })

	err := env.svc.CancelOrder(orderID, user)
	if err == nil {
		t.Fatal("CancelOrder succeeded despite injected DB failure")
	}
	if !fired.Load() {
		t.Fatalf("DB failure callback did not fire: %v", err)
	}
	if !env.ob.OrderExists(orderID) {
		t.Fatal("memory order was removed despite DB failure")
	}
	order := env.order(orderID)
	if order.Status != persistence.OrderStatusNew {
		t.Fatalf("DB status = %s, want NEW", order.Status)
	}
	after := env.balance(user, "3A-DB-FAIL")
	if after.Frozen != before.Frozen || after.Available != before.Available {
		t.Fatalf("balance changed on DB failure: before=%+v after=%+v", before, after)
	}
	if got := env.walRemoveCount(orderID); got != beforeWAL {
		t.Fatalf("WAL REMOVE changed on DB failure: %d -> %d", beforeWAL, got)
	}
	if env.outboxCount(orderID) != 0 {
		t.Fatal("cancel outbox written despite DB failure")
	}
}

func TestCancelWALFailureKeepsCommittedDBAndBlocksTrades(t *testing.T) {
	const symbol = "3A-WAL-FAIL/USDT"
	user := "3a_walfail_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, user)
	env.addBalance(user, "3A-WAL-FAIL", 10)

	maker := env.createOrder(user, "SELL", "LIMIT", "GTC", 100, 10)
	orderID := maker.OrderID
	before := env.balance(user, "3A-WAL-FAIL")

	if err := env.rdb.Set(context.Background(), walKeyForTest(symbol), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("corrupt WAL key: %v", err)
	}
	err := env.svc.CancelOrder(orderID, user)
	if err == nil {
		t.Fatal("CancelOrder succeeded despite WAL failure")
	}
	if !strings.Contains(err.Error(), "WAL 持久化失败") {
		t.Fatalf("cancel error = %v, want WAL failure", err)
	}
	if !env.snapshotter.WALFailed() {
		t.Fatal("WALFailed = false after WAL append failure")
	}
	order := env.order(orderID)
	if order.Status != persistence.OrderStatusCanceled {
		t.Fatalf("DB status = %s, want CANCELED", order.Status)
	}
	if env.ob.OrderExists(orderID) {
		t.Fatal("memory order still exists after committed cancel")
	}
	after := env.balance(user, "3A-WAL-FAIL")
	if after.Frozen != before.Frozen-10 || after.Available != before.Available+10 {
		t.Fatalf("unexpected balance after WAL failure: before=%+v after=%+v", before, after)
	}
	if env.outboxCount(orderID) != 1 {
		t.Fatalf("cancel outbox count = %d, want 1", env.outboxCount(orderID))
	}
	if err := env.svc.CancelOrder(orderID, user); err == nil || !strings.Contains(err.Error(), "已暂停交易") {
		t.Fatalf("subsequent cancel error = %v, want paused", err)
	}
	if _, err := env.svc.CreateOrder(&CreateOrderRequest{
		UserID: user, Symbol: symbol, Side: "BUY",
		OrderType: "LIMIT", TimeInForce: "GTC", Price: 1, Amount: 1,
	}); err == nil || !strings.Contains(err.Error(), "已暂停交易") {
		t.Fatalf("CreateOrder after WAL failure = %v, want paused", err)
	}
	if err := env.snapshotter.Save(); err == nil || !strings.Contains(err.Error(), "WAL 已失败") {
		t.Fatalf("Save error = %v, want WAL failure", err)
	}
}

func TestCancelRepeatedRequestHasNoSideEffects(t *testing.T) {
	const symbol = "3A-REPEAT/USDT"
	user := "3a_repeat_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, user)
	env.addBalance(user, "3A-REPEAT", 10)

	maker := env.createOrder(user, "SELL", "LIMIT", "GTC", 100, 10)
	orderID := maker.OrderID
	if err := env.svc.CancelOrder(orderID, user); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	afterFirst := env.balance(user, "3A-REPEAT")
	walAfterFirst := env.walRemoveCount(orderID)
	outboxAfterFirst := env.outboxCount(orderID)

	err := env.svc.CancelOrder(orderID, user)
	if err == nil {
		t.Fatal("repeated cancel succeeded")
	}
	afterSecond := env.balance(user, "3A-REPEAT")
	if afterSecond.Frozen != afterFirst.Frozen || afterSecond.Available != afterFirst.Available {
		t.Fatalf("repeated cancel changed balance: first=%+v second=%+v", afterFirst, afterSecond)
	}
	if got := env.walRemoveCount(orderID); got != walAfterFirst {
		t.Fatalf("repeated cancel WAL REMOVE count changed: %d -> %d", walAfterFirst, got)
	}
	if got := env.outboxCount(orderID); got != outboxAfterFirst {
		t.Fatalf("repeated cancel outbox count changed: %d -> %d", outboxAfterFirst, got)
	}
}

func TestCancelFullyFilledOrderIsRejectedWithoutFundsChange(t *testing.T) {
	const symbol = "3A-FILLED/USDT"
	makerUser := "3a_filled_maker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	takerUser := "3a_filled_taker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, makerUser, takerUser)
	env.addBalance(makerUser, "3A-FILLED", 1)
	env.addBalance(takerUser, "USDT", 10000)

	maker := env.createOrder(makerUser, "SELL", "LIMIT", "GTC", 100, 1)
	orderID := maker.OrderID
	env.createOrder(takerUser, "BUY", "LIMIT", "GTC", 100, 1)

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusFilled {
		t.Fatalf("maker status = %s, want FILLED", order.Status)
	}
	before := env.balance(makerUser, "3A-FILLED")
	beforeQuote := env.balance(makerUser, "USDT")
	err := env.svc.CancelOrder(orderID, makerUser)
	if err == nil || !strings.Contains(err.Error(), "已成交") {
		t.Fatalf("cancel error = %v, want filled rejection", err)
	}
	after := env.balance(makerUser, "3A-FILLED")
	afterQuote := env.balance(makerUser, "USDT")
	if after.Frozen != before.Frozen || after.Available != before.Available ||
		afterQuote.Frozen != beforeQuote.Frozen || afterQuote.Available != beforeQuote.Available {
		t.Fatal("fully filled cancel changed maker balances")
	}
	if env.outboxCount(orderID) != 0 {
		t.Fatal("fully filled cancel wrote cancel outbox")
	}
}

func TestCancelPartiallyFilledSellUnfreezesRemaining(t *testing.T) {
	const symbol = "3A-PARTIAL-SELL/USDT"
	makerUser := "3a_ps_maker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	takerUser := "3a_ps_taker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, makerUser, takerUser)
	env.addBalance(makerUser, "3A-PARTIAL-SELL", 10)
	env.addBalance(takerUser, "USDT", 10000)

	maker := env.createOrder(makerUser, "SELL", "LIMIT", "GTC", 100, 10)
	orderID := maker.OrderID
	env.createOrder(takerUser, "BUY", "LIMIT", "GTC", 100, 4)

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusPartiallyFilled || order.Remaining != 6 || order.Amount != 10 {
		t.Fatalf("maker order = %+v, want amount 10, PARTIALLY_FILLED remaining 6", order)
	}
	before := env.balance(makerUser, "3A-PARTIAL-SELL")
	if err := env.svc.CancelOrder(orderID, makerUser); err != nil {
		t.Fatalf("cancel partial sell: %v", err)
	}
	after := env.balance(makerUser, "3A-PARTIAL-SELL")
	if after.Frozen != before.Frozen-6 {
		t.Fatalf("frozen = %v, want %v", after.Frozen, before.Frozen-6)
	}
	if after.Available != before.Available+6 {
		t.Fatalf("available = %v, want %v", after.Available, before.Available+6)
	}
	if env.walRemoveCount(orderID) != 1 {
		t.Fatalf("WAL REMOVE count = %d, want 1", env.walRemoveCount(orderID))
	}
}

func TestCancelPartiallyFilledBuyUnfreezesRemaining(t *testing.T) {
	const symbol = "3A-PARTIAL-BUY/USDT"
	makerUser := "3a_pb_maker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	takerUser := "3a_pb_taker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, makerUser, takerUser)
	env.addBalance(makerUser, "USDT", 100000)
	env.addBalance(takerUser, "3A-PARTIAL-BUY", 10)

	// 卖方以 90 挂单 4，买方限价 100 吃 4，成交价 90，剩余 6 挂在 100。
	// 买方初始冻结 1000，实际成交 360，剩余冻结应为 640。
	env.createOrder(takerUser, "SELL", "LIMIT", "GTC", 90, 4)
	maker := env.createOrder(makerUser, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := maker.OrderID

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusPartiallyFilled || order.Remaining != 6 || order.Amount != 10 {
		t.Fatalf("maker order = %+v, want amount 10, PARTIALLY_FILLED remaining 6", order)
	}
	before := env.balance(makerUser, "USDT")
	if before.Frozen != 640 {
		t.Fatalf("buyer frozen before cancel = %v, want 640", before.Frozen)
	}
	if err := env.svc.CancelOrder(orderID, makerUser); err != nil {
		t.Fatalf("cancel partial buy: %v", err)
	}
	after := env.balance(makerUser, "USDT")
	if after.Frozen != before.Frozen-640 {
		t.Fatalf("frozen = %v, want %v", after.Frozen, before.Frozen-640)
	}
	if after.Available != before.Available+640 {
		t.Fatalf("available = %v, want %v", after.Available, before.Available+640)
	}
}

func TestCancelThenRestartRecovery(t *testing.T) {
	const symbol = "3A-RESTART/USDT"
	user := "3a_restart_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, user)
	env.addBalance(user, "3A-RESTART", 10)

	maker := env.createOrder(user, "SELL", "LIMIT", "GTC", 100, 10)
	orderID := maker.OrderID
	if err := env.snapshotter.Save(); err != nil {
		t.Fatalf("initial Save: %v", err)
	}
	if err := env.svc.CancelOrder(orderID, user); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	restored := engine.NewEngine()
	restoredOB := restored.GetOrderBook(symbol)
	restoredSnapshotter := engine.NewSnapshotter(env.rdb, restoredOB, env.db, symbol)
	if err := restoredSnapshotter.Load(); err != nil {
		t.Fatalf("restart Load: %v", err)
	}
	if restoredOB.OrderExists(orderID) {
		t.Fatal("canceled order reappeared after restart")
	}
	if got := env.order(orderID); got.Status != persistence.OrderStatusCanceled {
		t.Fatalf("restarted DB status = %s, want CANCELED", got.Status)
	}
}

func TestCancelMarketBuyNotInBookIsRejectedWithoutFundsChange(t *testing.T) {
	const symbol = "3A-MARKET-BUY/USDT"
	makerUser := "3a_mb_maker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	buyerUser := "3a_mb_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, makerUser, buyerUser)
	env.addBalance(makerUser, "3A-MARKET-BUY", 1)
	env.addBalance(buyerUser, "USDT", 10000)

	// 创建一个卖单，使市场买单有部分流动性，但剩余部分不会挂入订单簿。
	env.createOrder(makerUser, "SELL", "LIMIT", "GTC", 100, 1)
	market := env.createOrder(buyerUser, "BUY", "MARKET", "GTC", 0, 2)
	orderID := market.OrderID

	if env.ob.OrderExists(orderID) {
		t.Fatal("market buy unexpectedly exists in order book")
	}
	beforeQuote := env.balance(buyerUser, "USDT")
	beforeBase := env.balance(buyerUser, "3A-MARKET-BUY")
	err := env.svc.CancelOrder(orderID, buyerUser)
	if err == nil || !strings.Contains(err.Error(), "市场单") {
		t.Fatalf("market cancel error = %v, want market rejection", err)
	}
	afterQuote := env.balance(buyerUser, "USDT")
	afterBase := env.balance(buyerUser, "3A-MARKET-BUY")
	if afterQuote.Frozen != beforeQuote.Frozen || afterQuote.Available != beforeQuote.Available ||
		afterBase.Frozen != beforeBase.Frozen || afterBase.Available != beforeBase.Available {
		t.Fatal("market buy cancel changed balances")
	}
	if env.outboxCount(orderID) != 0 {
		t.Fatal("market buy cancel wrote outbox")
	}
}

func TestCancelCanceledDBWithMemoryOrderEntersSymbolFailure(t *testing.T) {
	const symbol = "STALE/USDT"
	user := "3a_stale_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, user)
	env.addBalance(user, "STALE", 10)

	maker := env.createOrder(user, "SELL", "LIMIT", "GTC", 100, 10)
	orderID := maker.OrderID
	// 直接把 DB 改为 CANCELED，内存仍然保留，模拟不一致状态。
	if err := env.db.Model(&persistence.OrderModel{}).
		Where("order_id = ?", orderID).
		Update("status", persistence.OrderStatusCanceled).Error; err != nil {
		t.Fatalf("seed canceled DB state: %v", err)
	}

	err := env.svc.CancelOrder(orderID, user)
	if err == nil || !strings.Contains(err.Error(), "故障保护") {
		t.Fatalf("cancel error = %v, want symbol failure", err)
	}
	if !env.ob.OrderExists(orderID) {
		t.Fatal("memory order should remain for restart reconcile")
	}
	if err := env.snapshotter.Save(); err == nil {
		t.Fatal("Save succeeded after symbol failure")
	}
	if _, err := env.svc.CreateOrder(&CreateOrderRequest{
		UserID: user, Symbol: symbol, Side: "BUY",
		OrderType: "LIMIT", TimeInForce: "GTC", Price: 1, Amount: 1,
	}); err == nil || !strings.Contains(err.Error(), "已暂停交易") {
		t.Fatalf("CreateOrder after symbol failure = %v, want paused", err)
	}
}
