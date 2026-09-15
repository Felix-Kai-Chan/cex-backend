package test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"cex-backend/internal/persistence"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *gorm.DB {
	dsn := "root:@tcp(127.0.0.1:3306)/cex?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("MySQL 连接失败: %v", err)
	}
	// ✅ 调大连接池
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(200)
	sqlDB.SetMaxIdleConns(50)
	return db
}

// TestConcurrentDeduct 并发扣减测试
// 场景：test_stress 用户有 100 USDT，500 个 goroutine 同时扣 10 USDT
// 预期：只有 10 个成功，490 个失败，最终余额 = 0，不会为负
func TestConcurrentDeduct(t *testing.T) {
	db := setupTestDB(t)
	repo := persistence.NewBalanceRepo(db)

	userID := "test_stress"
	asset := "USDT"

	// 1. 重置余额为 100
	db.Exec("DELETE FROM balances WHERE user_id = ? AND asset = ?", userID, asset)
	if err := repo.AddBalance(userID, asset, 100); err != nil {
		t.Fatalf("初始化余额失败: %v", err)
	}

	// 2. 500 个 goroutine 并发扣 10 USDT
	var successCount int64
	var failCount int64
	var wg sync.WaitGroup

	concurrency := 500
	deductAmount := 10.0
	expectedSuccess := int64(100 / deductAmount)         // ✅ 动态计算：10
	expectedFail := int64(concurrency) - expectedSuccess // ✅ 动态计算：490

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			err := repo.DeductBalance(userID, asset, deductAmount)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			} else {
				atomic.AddInt64(&failCount, 1)
			}
		}()
	}
	wg.Wait()

	// 3. 验证结果
	bal, _ := repo.GetBalance(userID, asset)

	fmt.Printf("\n===== 并发扣减测试结果 =====\n")
	fmt.Printf("并发数:     %d\n", concurrency)
	fmt.Printf("扣减金额:   %.2f USDT\n", deductAmount)
	fmt.Printf("初始余额:   100.00 USDT\n")
	fmt.Printf("成功次数:   %d（预期 %d）\n", successCount, expectedSuccess)
	fmt.Printf("失败次数:   %d（预期 %d）\n", failCount, expectedFail)
	fmt.Printf("最终余额:   %.2f USDT（预期 0）\n", bal.Available)
	fmt.Printf("===========================\n\n")

	// 断言
	if successCount != expectedSuccess {
		t.Errorf("❌ 成功次数错误：期望 %d，实际 %d", expectedSuccess, successCount)
	}
	if failCount != expectedFail {
		t.Errorf("❌ 失败次数错误：期望 %d，实际 %d", expectedFail, failCount)
	}
	if bal.Available != 0 {
		t.Errorf("❌ 最终余额错误：期望 0，实际 %.2f", bal.Available)
	}
	if bal.Available < 0 {
		t.Errorf("❌ 余额超卖！最终余额为负数：%.2f", bal.Available)
	}

	t.Logf("✅ 并发扣减测试通过：无超卖，余额精确归零")
}
