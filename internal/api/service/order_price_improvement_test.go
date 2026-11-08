package service

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"cex-backend/internal/persistence"

	"gorm.io/gorm"
)

func (e *cancelTestEnv) priceImprovementEntries(orderID string) []persistence.LedgerModel {
	e.t.Helper()
	var entries []persistence.LedgerModel
	if err := e.db.Where("order_id = ? AND type = ?", orderID, "PRICE_IMPROVEMENT").
		Order("id ASC").
		Find(&entries).Error; err != nil {
		e.t.Fatalf("query price improvement ledgers: %v", err)
	}
	return entries
}

func (e *cancelTestEnv) priceImprovementCount(orderID string) int {
	e.t.Helper()
	return len(e.priceImprovementEntries(orderID))
}

func TestPriceImprovementTakerFullAtBetterPrice(t *testing.T) {
	const symbol = "3A-PI-TB/USDT"
	buyer := "3a_pi_tb_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller := "3a_pi_tb_seller_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller, "3A-PI-TB", 10)

	env.createOrder(seller, "SELL", "LIMIT", "GTC", 90, 10)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusFilled || order.Remaining != 0 || order.Amount != 10 {
		t.Fatalf("buyer order = %+v, want FILLED amount 10 remaining 0", order)
	}
	if !order.PriceImprovementReleased {
		t.Fatal("PriceImprovementReleased = false, want true")
	}

	quote := env.balance(buyer, "USDT")
	if quote.Available != 9100 || quote.Frozen != 0 {
		t.Fatalf("buyer USDT = %+v, want available 9100 frozen 0", quote)
	}
	base := env.balance(buyer, "3A-PI-TB")
	if base.Available != 10 {
		t.Fatalf("buyer base available = %v, want 10", base.Available)
	}

	entries := env.priceImprovementEntries(orderID)
	if len(entries) != 1 {
		t.Fatalf("price improvement ledger count = %d, want 1", len(entries))
	}
	if entries[0].Amount != 100 {
		t.Fatalf("price improvement amount = %v, want 100", entries[0].Amount)
	}
	if entries[0].Balance != 9100 || entries[0].Frozen != 0 {
		t.Fatalf("ledger snapshot = available %v frozen %v, want 9100/0", entries[0].Balance, entries[0].Frozen)
	}
	if entries[0].TradeID != "" {
		t.Fatalf("price improvement trade_id = %q, want empty", entries[0].TradeID)
	}
}

func TestPriceImprovementTakerFullAtLimitMarksWithoutUnfreeze(t *testing.T) {
	const symbol = "3A-PI-TL/USDT"
	buyer := "3a_pi_tl_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller := "3a_pi_tl_seller_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller, "3A-PI-TL", 10)

	env.createOrder(seller, "SELL", "LIMIT", "GTC", 100, 10)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusFilled {
		t.Fatalf("status = %s, want FILLED", order.Status)
	}
	if !order.PriceImprovementReleased {
		t.Fatal("PriceImprovementReleased = false, want true even when improvement is zero")
	}
	quote := env.balance(buyer, "USDT")
	if quote.Available != 9000 || quote.Frozen != 0 {
		t.Fatalf("buyer USDT = %+v, want available 9000 frozen 0", quote)
	}
	if got := env.priceImprovementCount(orderID); got != 0 {
		t.Fatalf("price improvement ledger count = %d, want 0", got)
	}
}

func TestPriceImprovementMakerMultipleFillsAfterTakerPartial(t *testing.T) {
	const symbol = "3A-PI-MK/USDT"
	buyer := "3a_pi_mk_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller1 := "3a_pi_mk_seller1_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller2 := "3a_pi_mk_seller2_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller3 := "3a_pi_mk_seller3_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller1, seller2, seller3)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller1, "3A-PI-MK", 4)
	env.addBalance(seller2, "3A-PI-MK", 2)
	env.addBalance(seller3, "3A-PI-MK", 4)

	// 先作为 Taker 成交 4 @ 96，剩余 6 @ 100 挂入订单簿。
	env.createOrder(seller1, "SELL", "LIMIT", "GTC", 96, 4)
	buyerOrder := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := buyerOrder.OrderID

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusPartiallyFilled || order.Remaining != 6 {
		t.Fatalf("after taker partial order = %+v, want PARTIALLY_FILLED remaining 6", order)
	}
	if !env.ob.OrderExists(orderID) {
		t.Fatal("partially filled BUY LIMIT should rest in order book")
	}
	if order.PriceImprovementReleased {
		t.Fatal("PriceImprovementReleased = true before full fill")
	}
	if got := env.priceImprovementCount(orderID); got != 0 {
		t.Fatalf("price improvement ledger count before full fill = %d, want 0", got)
	}

	// 作为 Maker 第一次成交 2 @ 100，仍未完全成交。
	env.createOrder(seller2, "SELL", "LIMIT", "GTC", 100, 2)
	order = env.order(orderID)
	if order.Status != persistence.OrderStatusPartiallyFilled || order.Remaining != 4 {
		t.Fatalf("after first maker fill order = %+v, want PARTIALLY_FILLED remaining 4", order)
	}
	if order.PriceImprovementReleased {
		t.Fatal("PriceImprovementReleased = true after first maker fill")
	}

	// 作为 Maker 第二次成交 4 @ 100，最终完全成交。
	env.createOrder(seller3, "SELL", "LIMIT", "GTC", 100, 4)

	order = env.order(orderID)
	if order.Status != persistence.OrderStatusFilled || order.Remaining != 0 {
		t.Fatalf("after maker full fill order = %+v, want FILLED remaining 0", order)
	}
	if !order.PriceImprovementReleased {
		t.Fatal("PriceImprovementReleased = false after maker full fill")
	}
	quote := env.balance(buyer, "USDT")
	// 初始冻结 1000，成交 4*96 + 2*100 + 4*100 = 984，释放 16。
	if quote.Available != 9016 || quote.Frozen != 0 {
		t.Fatalf("buyer USDT = %+v, want available 9016 frozen 0", quote)
	}
	entries := env.priceImprovementEntries(orderID)
	if len(entries) != 1 || entries[0].Amount != 16 {
		t.Fatalf("price improvement entries = %+v, want one amount 16", entries)
	}
	if entries[0].Balance != 9016 || entries[0].Frozen != 0 {
		t.Fatalf("ledger snapshot = %v/%v, want 9016/0", entries[0].Balance, entries[0].Frozen)
	}
}

func TestPriceImprovementTakerMultipleTradesSingleRelease(t *testing.T) {
	const symbol = "3A-PI-MT/USDT"
	buyer := "3a_pi_mt_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller1 := "3a_pi_mt_seller1_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller2 := "3a_pi_mt_seller2_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller1, seller2)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller1, "3A-PI-MT", 4)
	env.addBalance(seller2, "3A-PI-MT", 6)

	env.createOrder(seller1, "SELL", "LIMIT", "GTC", 90, 4)
	env.createOrder(seller2, "SELL", "LIMIT", "GTC", 95, 6)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusFilled {
		t.Fatalf("status = %s, want FILLED", order.Status)
	}
	if !order.PriceImprovementReleased {
		t.Fatal("PriceImprovementReleased = false")
	}
	quote := env.balance(buyer, "USDT")
	// 成交 4*90 + 6*95 = 930，初始冻结 1000，释放 70。
	if quote.Available != 9070 || quote.Frozen != 0 {
		t.Fatalf("buyer USDT = %+v, want available 9070 frozen 0", quote)
	}
	entries := env.priceImprovementEntries(orderID)
	if len(entries) != 1 {
		t.Fatalf("price improvement ledger count = %d, want 1", len(entries))
	}
	if entries[0].Amount != 70 {
		t.Fatalf("price improvement amount = %v, want 70", entries[0].Amount)
	}
}

func TestPriceImprovementDuplicateReleaseIsIdempotent(t *testing.T) {
	const symbol = "3A-PI-ID/USDT"
	buyer := "3a_pi_id_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller := "3a_pi_id_seller_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller, "3A-PI-ID", 10)
	env.createOrder(seller, "SELL", "LIMIT", "GTC", 90, 10)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	before := env.balance(buyer, "USDT")
	beforeCount := env.priceImprovementCount(orderID)
	for i := 0; i < 2; i++ {
		err := env.db.Transaction(func(tx *gorm.DB) error {
			return env.svc.releaseBuyPriceImprovementTx(tx, orderID, "USDT")
		})
		if err != nil {
			t.Fatalf("duplicate release call %d: %v", i, err)
		}
	}
	after := env.balance(buyer, "USDT")
	if after.Available != before.Available || after.Frozen != before.Frozen {
		t.Fatalf("duplicate release changed balance: before=%+v after=%+v", before, after)
	}
	if got := env.priceImprovementCount(orderID); got != beforeCount {
		t.Fatalf("duplicate release added ledger: %d -> %d", beforeCount, got)
	}
}

func TestPriceImprovementNegativeRollsBack(t *testing.T) {
	const symbol = "3A-PI-NEG/USDT"
	buyer := "3a_pi_neg_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller := "3a_pi_neg_seller_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller, "3A-PI-NEG", 10)
	env.createOrder(seller, "SELL", "LIMIT", "GTC", 90, 10)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	// 构造负价差：额外插入一笔高于限价的买方成交。
	if err := env.db.Create(&persistence.TradeModel{
		TradeID:     "fake_negative_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		BuyOrderID:  orderID,
		SellOrderID: "fake_sell",
		Price:       200,
		Quantity:    1,
		Timestamp:   time.Now().UnixMilli(),
	}).Error; err != nil {
		t.Fatalf("insert fake trade: %v", err)
	}
	if err := env.db.Model(&persistence.OrderModel{}).Where("order_id = ?", orderID).
		Update("price_improvement_released", false).Error; err != nil {
		t.Fatalf("reset marker: %v", err)
	}

	before := env.balance(buyer, "USDT")
	beforeLedger := env.priceImprovementCount(orderID)
	err := env.db.Transaction(func(tx *gorm.DB) error {
		return env.svc.releaseBuyPriceImprovementTx(tx, orderID, "USDT")
	})
	if err == nil || !strings.Contains(err.Error(), "价差为负") {
		t.Fatalf("negative release error = %v, want negative", err)
	}
	after := env.balance(buyer, "USDT")
	if after.Available != before.Available || after.Frozen != before.Frozen {
		t.Fatalf("negative release changed balance: before=%+v after=%+v", before, after)
	}
	if got := env.priceImprovementCount(orderID); got != beforeLedger {
		t.Fatalf("negative release changed ledger: %d -> %d", beforeLedger, got)
	}
	order := env.order(orderID)
	if order.PriceImprovementReleased {
		t.Fatal("negative release left marker true, want rollback to false")
	}
}

func TestPriceImprovementUnfreezeFailureRollsBack(t *testing.T) {
	const symbol = "3A-PI-UF/USDT"
	buyer := "3a_pi_uf_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller := "3a_pi_uf_seller_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller, "3A-PI-UF", 10)
	env.createOrder(seller, "SELL", "LIMIT", "GTC", 90, 10)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	// 构造 frozen 不足，使 release 的 UnfreezeBalanceTx 失败。
	if err := env.db.Model(&persistence.BalanceModel{}).
		Where("user_id = ? AND asset = ?", buyer, "USDT").
		Updates(map[string]interface{}{"available": 9100, "frozen": 0}).Error; err != nil {
		t.Fatalf("seed balance: %v", err)
	}
	if err := env.db.Model(&persistence.OrderModel{}).Where("order_id = ?", orderID).
		Update("price_improvement_released", false).Error; err != nil {
		t.Fatalf("reset marker: %v", err)
	}

	before := env.balance(buyer, "USDT")
	beforeLedger := env.priceImprovementCount(orderID)
	err := env.db.Transaction(func(tx *gorm.DB) error {
		return env.svc.releaseBuyPriceImprovementTx(tx, orderID, "USDT")
	})
	if err == nil || !strings.Contains(err.Error(), "价差解冻失败") {
		t.Fatalf("unfreeze failure error = %v, want unfreeze failure", err)
	}
	after := env.balance(buyer, "USDT")
	if after.Available != before.Available || after.Frozen != before.Frozen {
		t.Fatalf("unfreeze failure changed balance: before=%+v after=%+v", before, after)
	}
	if got := env.priceImprovementCount(orderID); got != beforeLedger {
		t.Fatalf("unfreeze failure changed ledger: %d -> %d", beforeLedger, got)
	}
	if env.order(orderID).PriceImprovementReleased {
		t.Fatal("unfreeze failure left marker true")
	}
}

func TestPriceImprovementActualSettlementUnfreezeFailureRollsBack(t *testing.T) {
	const symbol = "3A-PI-AS/USDT"
	buyer := "3a_pi_as_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller1 := "3a_pi_as_seller1_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller2 := "3a_pi_as_seller2_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller1, seller2)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller1, "3A-PI-AS", 4)
	env.addBalance(seller2, "3A-PI-AS", 6)

	// Buyer 先 Taker 成交 4 @ 96，剩余 6 挂在 100。
	env.createOrder(seller1, "SELL", "LIMIT", "GTC", 96, 4)
	buyerOrder := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := buyerOrder.OrderID

	// 人为把 buyer 的 frozen 调整为刚好只能覆盖最后一笔成交，
	// 使最终交易写入后 release 的价差解冻失败。
	if err := env.db.Model(&persistence.BalanceModel{}).
		Where("user_id = ? AND asset = ?", buyer, "USDT").
		Update("frozen", 600).Error; err != nil {
		t.Fatalf("seed buyer frozen: %v", err)
	}
	before := env.balance(buyer, "USDT")
	beforeLedger := env.priceImprovementCount(orderID)

	// 最后一笔 Maker 成交触发价差释放，release 解冻失败，整个结算事务回滚。
	_, err := env.svc.CreateOrder(&CreateOrderRequest{
		UserID:      seller2,
		Symbol:      symbol,
		Side:        "SELL",
		OrderType:   "LIMIT",
		TimeInForce: "GTC",
		Price:       100,
		Amount:      6,
	})
	if err == nil {
		t.Fatal("expected settlement failure from unfreeze error")
	}

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusPartiallyFilled || order.Remaining != 6 {
		t.Fatalf("buyer order after failed settlement = %+v, want PARTIALLY_FILLED remaining 6", order)
	}
	if order.PriceImprovementReleased {
		t.Fatal("buyer order marker became true despite rolled-back settlement")
	}
	after := env.balance(buyer, "USDT")
	if after.Available != before.Available || after.Frozen != before.Frozen {
		t.Fatalf("failed settlement changed buyer balance: before=%+v after=%+v", before, after)
	}
	if got := env.priceImprovementCount(orderID); got != beforeLedger {
		t.Fatalf("failed settlement changed price ledger: %d -> %d", beforeLedger, got)
	}
	var tradeCount int64
	if err := env.db.Model(&persistence.TradeModel{}).Where("buy_order_id = ? OR sell_order_id = ?", orderID, orderID).
		Count(&tradeCount).Error; err != nil {
		t.Fatalf("count buyer trades: %v", err)
	}
	if tradeCount != 1 {
		t.Fatalf("buyer trade count after rollback = %d, want only the original partial trade", tradeCount)
	}
}

func TestPriceImprovementGetBalanceFailureRollsBack(t *testing.T) {
	const symbol = "3A-PI-GB/USDT"
	buyer := "3a_pi_gb_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller := "3a_pi_gb_seller_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller, "3A-PI-GB", 10)
	env.createOrder(seller, "SELL", "LIMIT", "GTC", 90, 10)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	if err := env.db.Model(&persistence.BalanceModel{}).
		Where("user_id = ? AND asset = ?", buyer, "USDT").
		Updates(map[string]interface{}{"available": 9000, "frozen": 100}).Error; err != nil {
		t.Fatalf("seed balance: %v", err)
	}
	if err := env.db.Model(&persistence.OrderModel{}).Where("order_id = ?", orderID).
		Update("price_improvement_released", false).Error; err != nil {
		t.Fatalf("reset marker: %v", err)
	}

	before := env.balance(buyer, "USDT")
	beforeLedger := env.priceImprovementCount(orderID)

	callbackName := fmt.Sprintf("test_fail_balance_query_%d", time.Now().UnixNano())
	if err := env.db.Callback().Query().Before("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table == "balances" {
			tx.AddError(errors.New("injected balance query failure"))
		}
	}); err != nil {
		t.Fatalf("register query callback: %v", err)
	}
	t.Cleanup(func() { _ = env.db.Callback().Query().Remove(callbackName) })

	err := env.db.Transaction(func(tx *gorm.DB) error {
		return env.svc.releaseBuyPriceImprovementTx(tx, orderID, "USDT")
	})
	_ = env.db.Callback().Query().Remove(callbackName)
	if err == nil || !strings.Contains(err.Error(), "读取价差释放后余额失败") {
		t.Fatalf("balance query failure error = %v, want query failure", err)
	}
	after := env.balance(buyer, "USDT")
	if after.Available != before.Available || after.Frozen != before.Frozen {
		t.Fatalf("balance query failure changed balance: before=%+v after=%+v", before, after)
	}
	if got := env.priceImprovementCount(orderID); got != beforeLedger {
		t.Fatalf("balance query failure changed ledger: %d -> %d", beforeLedger, got)
	}
	if env.order(orderID).PriceImprovementReleased {
		t.Fatal("balance query failure left marker true")
	}
}

func TestPriceImprovementLedgerFailureRollsBack(t *testing.T) {
	const symbol = "3A-PI-LG/USDT"
	buyer := "3a_pi_lg_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller := "3a_pi_lg_seller_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller, "3A-PI-LG", 10)
	env.createOrder(seller, "SELL", "LIMIT", "GTC", 90, 10)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	if err := env.db.Model(&persistence.BalanceModel{}).
		Where("user_id = ? AND asset = ?", buyer, "USDT").
		Updates(map[string]interface{}{"available": 9000, "frozen": 100}).Error; err != nil {
		t.Fatalf("seed balance: %v", err)
	}
	if err := env.db.Model(&persistence.OrderModel{}).Where("order_id = ?", orderID).
		Update("price_improvement_released", false).Error; err != nil {
		t.Fatalf("reset marker: %v", err)
	}

	callbackName := fmt.Sprintf("test_fail_ledger_create_%d", time.Now().UnixNano())
	if err := env.db.Callback().Create().Before("gorm:create").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table == "ledgers" {
			tx.AddError(errors.New("injected ledger failure"))
		}
	}); err != nil {
		t.Fatalf("register create callback: %v", err)
	}
	t.Cleanup(func() { _ = env.db.Callback().Create().Remove(callbackName) })

	before := env.balance(buyer, "USDT")
	beforeLedger := env.priceImprovementCount(orderID)
	err := env.db.Transaction(func(tx *gorm.DB) error {
		return env.svc.releaseBuyPriceImprovementTx(tx, orderID, "USDT")
	})
	if err == nil || !strings.Contains(err.Error(), "写入价差释放 Ledger 失败") {
		t.Fatalf("ledger failure error = %v, want ledger failure", err)
	}
	after := env.balance(buyer, "USDT")
	if after.Available != before.Available || after.Frozen != before.Frozen {
		t.Fatalf("ledger failure changed balance: before=%+v after=%+v", before, after)
	}
	if got := env.priceImprovementCount(orderID); got != beforeLedger {
		t.Fatalf("ledger failure changed ledger: %d -> %d", beforeLedger, got)
	}
	if env.order(orderID).PriceImprovementReleased {
		t.Fatal("ledger failure left marker true")
	}
}

func TestGetBalanceTxMissingRecordReturnsError(t *testing.T) {
	const symbol = "3B-GB/USDT"
	user := "3b_gb_user_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, user)

	err := env.db.Transaction(func(tx *gorm.DB) error {
		_, err := env.balanceRepo.GetBalanceTx(tx, user, "USDT")
		return err
	})
	if err == nil {
		t.Fatal("GetBalanceTx missing record returned nil error")
	}
	if !strings.Contains(err.Error(), user) || !strings.Contains(err.Error(), "USDT") {
		t.Fatalf("GetBalanceTx error = %v, want userID and asset", err)
	}
}

func TestPriceImprovementEmptyTradeIDRollsBack(t *testing.T) {
	const symbol = "3B-ET/USDT"
	buyer := "3b_et_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller := "3b_et_seller_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller, "3B-ET", 10)
	env.createOrder(seller, "SELL", "LIMIT", "GTC", 90, 10)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	if err := env.db.Create(&persistence.TradeModel{
		TradeID:     "",
		BuyOrderID:  orderID,
		SellOrderID: "fake_sell_empty_trade_id",
		Price:       1,
		Quantity:    1,
		Timestamp:   time.Now().UnixMilli(),
	}).Error; err != nil {
		t.Fatalf("insert empty TradeID: %v", err)
	}
	if err := env.db.Model(&persistence.OrderModel{}).Where("order_id = ?", orderID).
		Update("price_improvement_released", false).Error; err != nil {
		t.Fatalf("reset marker: %v", err)
	}

	before := env.balance(buyer, "USDT")
	beforeLedger := env.priceImprovementCount(orderID)
	err := env.db.Transaction(func(tx *gorm.DB) error {
		return env.svc.releaseBuyPriceImprovementTx(tx, orderID, "USDT")
	})
	if err == nil || !strings.Contains(err.Error(), "成交缺少 TradeID") {
		t.Fatalf("empty TradeID error = %v, want missing TradeID", err)
	}
	after := env.balance(buyer, "USDT")
	if after.Available != before.Available || after.Frozen != before.Frozen {
		t.Fatalf("empty TradeID changed balance: before=%+v after=%+v", before, after)
	}
	if got := env.priceImprovementCount(orderID); got != beforeLedger {
		t.Fatalf("empty TradeID changed ledger: %d -> %d", beforeLedger, got)
	}
	if env.order(orderID).PriceImprovementReleased {
		t.Fatal("empty TradeID left marker true")
	}
}

func TestPriceImprovementDuplicateTradeIDRollsBack(t *testing.T) {
	const symbol = "3B-DT/USDT"
	buyer := "3b_dt_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller := "3b_dt_seller_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller, "3B-DT", 10)
	env.createOrder(seller, "SELL", "LIMIT", "GTC", 90, 10)
	resp := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := resp.OrderID

	if err := env.db.Model(&persistence.OrderModel{}).Where("order_id = ?", orderID).
		Update("price_improvement_released", false).Error; err != nil {
		t.Fatalf("reset marker: %v", err)
	}

	before := env.balance(buyer, "USDT")
	beforeLedger := env.priceImprovementCount(orderID)
	callbackName := fmt.Sprintf("test_duplicate_trade_%d", time.Now().UnixNano())
	if err := env.db.Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table != "trades" {
			return
		}
		dest, ok := tx.Statement.Dest.(*[]persistence.TradeModel)
		if !ok {
			return
		}
		for _, trade := range *dest {
			if trade.BuyOrderID == orderID {
				*dest = append(*dest, trade)
				return
			}
		}
	}); err != nil {
		t.Fatalf("register duplicate trade callback: %v", err)
	}
	t.Cleanup(func() { _ = env.db.Callback().Query().Remove(callbackName) })

	err := env.db.Transaction(func(tx *gorm.DB) error {
		return env.svc.releaseBuyPriceImprovementTx(tx, orderID, "USDT")
	})
	_ = env.db.Callback().Query().Remove(callbackName)
	if err == nil || !strings.Contains(err.Error(), "重复 TradeID") {
		t.Fatalf("duplicate TradeID error = %v, want duplicate TradeID", err)
	}
	after := env.balance(buyer, "USDT")
	if after.Available != before.Available || after.Frozen != before.Frozen {
		t.Fatalf("duplicate TradeID changed balance: before=%+v after=%+v", before, after)
	}
	if got := env.priceImprovementCount(orderID); got != beforeLedger {
		t.Fatalf("duplicate TradeID changed ledger: %d -> %d", beforeLedger, got)
	}
	if env.order(orderID).PriceImprovementReleased {
		t.Fatal("duplicate TradeID left marker true")
	}
}

func TestPriceImprovementBuyerOrderQueryFailureRollsBack(t *testing.T) {
	const symbol = "3B-BQ/USDT"
	buyer := "3b_bq_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller1 := "3b_bq_seller1_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller2 := "3b_bq_seller2_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller1, seller2)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller1, "3B-BQ", 4)
	env.addBalance(seller2, "3B-BQ", 6)

	env.createOrder(seller1, "SELL", "LIMIT", "GTC", 96, 4)
	buyerOrder := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := buyerOrder.OrderID
	before := env.balance(buyer, "USDT")
	beforeTradeCount := env.tradeCountFor(orderID)

	callbackName := fmt.Sprintf("test_fail_buyer_order_query_%d", time.Now().UnixNano())
	if err := env.db.Callback().Query().Before("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table == "orders" {
			tx.AddError(errors.New("injected buyer order query failure"))
		}
	}); err != nil {
		t.Fatalf("register buyer query callback: %v", err)
	}
	t.Cleanup(func() { _ = env.db.Callback().Query().Remove(callbackName) })

	_, err := env.svc.CreateOrder(&CreateOrderRequest{
		UserID:      seller2,
		Symbol:      symbol,
		Side:        "SELL",
		OrderType:   "LIMIT",
		TimeInForce: "GTC",
		Price:       100,
		Amount:      6,
	})
	_ = env.db.Callback().Query().Remove(callbackName)
	if err == nil || !strings.Contains(err.Error(), "读取买方订单失败") {
		t.Fatalf("buyer query failure error = %v, want buyer query failure", err)
	}

	order := env.order(orderID)
	if order.Status != persistence.OrderStatusPartiallyFilled || order.Remaining != 6 {
		t.Fatalf("buyer order after query failure = %+v, want PARTIALLY_FILLED remaining 6", order)
	}
	if order.PriceImprovementReleased {
		t.Fatal("buyer query failure left marker true")
	}
	after := env.balance(buyer, "USDT")
	if after.Available != before.Available || after.Frozen != before.Frozen {
		t.Fatalf("buyer query failure changed balance: before=%+v after=%+v", before, after)
	}
	if got := env.tradeCountFor(orderID); got != beforeTradeCount {
		t.Fatalf("buyer query failure changed trade count: %d -> %d", beforeTradeCount, got)
	}
	if got := env.priceImprovementCount(orderID); got != 0 {
		t.Fatalf("buyer query failure wrote price ledger: %d", got)
	}
}

func TestPriceImprovementSellerOrderQueryFailureRollsBack(t *testing.T) {
	const symbol = "3B-SQ/USDT"
	buyer := "3b_sq_buyer_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller1 := "3b_sq_seller1_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seller2 := "3b_sq_seller2_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	env := newCancelTestEnv(t, symbol, buyer, seller1, seller2)
	env.addBalance(buyer, "USDT", 10000)
	env.addBalance(seller1, "3B-SQ", 4)
	env.addBalance(seller2, "3B-SQ", 6)

	// Buyer 先 Taker 成交 4 @ 96，剩余 6 挂在 100。
	env.createOrder(seller1, "SELL", "LIMIT", "GTC", 96, 4)
	buyerOrder := env.createOrder(buyer, "BUY", "LIMIT", "GTC", 100, 10)
	orderID := buyerOrder.OrderID

	beforeBuyerOrder := env.order(orderID)
	beforeBuyerBalance := env.balance(buyer, "USDT")
	beforeTradeCount := env.tradeCountFor(orderID)
	beforeLedger := env.priceImprovementCount(orderID)

	// 本次 CreateOrder 的结算循环中，第 1 次 orders 查询是 buyerOrder，
	// 第 2 次是 sellerOrder；让第 2 次失败。
	queryCount := 0
	callbackName := fmt.Sprintf("test_fail_seller_order_query_%d", time.Now().UnixNano())
	if err := env.db.Callback().Query().Before("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table != "orders" {
			return
		}
		queryCount++
		if queryCount >= 2 {
			tx.AddError(errors.New("injected seller order query failure"))
		}
	}); err != nil {
		t.Fatalf("register seller query callback: %v", err)
	}
	t.Cleanup(func() { _ = env.db.Callback().Query().Remove(callbackName) })

	_, err := env.svc.CreateOrder(&CreateOrderRequest{
		UserID:      seller2,
		Symbol:      symbol,
		Side:        "SELL",
		OrderType:   "LIMIT",
		TimeInForce: "GTC",
		Price:       100,
		Amount:      6,
	})
	_ = env.db.Callback().Query().Remove(callbackName)
	if err == nil || !strings.Contains(err.Error(), "读取卖方订单失败") {
		t.Fatalf("seller query failure error = %v, want seller query failure", err)
	}

	buyerAfter := env.order(orderID)
	if buyerAfter.Status != beforeBuyerOrder.Status || buyerAfter.Remaining != beforeBuyerOrder.Remaining {
		t.Fatalf("buyer order changed: before=%+v after=%+v", beforeBuyerOrder, buyerAfter)
	}
	if buyerAfter.PriceImprovementReleased {
		t.Fatal("buyer marker became true despite seller query failure")
	}
	buyerBalanceAfter := env.balance(buyer, "USDT")
	if buyerBalanceAfter.Available != beforeBuyerBalance.Available ||
		buyerBalanceAfter.Frozen != beforeBuyerBalance.Frozen {
		t.Fatalf("buyer balance changed: before=%+v after=%+v", beforeBuyerBalance, buyerBalanceAfter)
	}
	if got := env.tradeCountFor(orderID); got != beforeTradeCount {
		t.Fatalf("buyer trade count changed: %d -> %d", beforeTradeCount, got)
	}
	if got := env.priceImprovementCount(orderID); got != beforeLedger {
		t.Fatalf("seller query failure wrote price ledger: %d -> %d", beforeLedger, got)
	}

	// 现有 settleErr 清理会取消当前 seller2 订单并解冻其初始冻结；
	// 这里只确认该 seller 订单没有被成交结算提交为 FILLED。
	var sellerOrders []persistence.OrderModel
	if err := env.db.Where("user_id = ?", seller2).Find(&sellerOrders).Error; err != nil {
		t.Fatalf("query seller orders: %v", err)
	}
	for _, order := range sellerOrders {
		if order.Status == persistence.OrderStatusFilled {
			t.Fatalf("seller order %s was committed as FILLED after query failure", order.OrderID)
		}
	}
	sellerBalance := env.balance(seller2, "3B-SQ")
	if sellerBalance.Available != 6 || sellerBalance.Frozen != 0 {
		t.Fatalf("seller balance after existing cleanup = %+v, want available 6 frozen 0", sellerBalance)
	}
}
