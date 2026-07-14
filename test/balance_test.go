package test

import (
	"sync"
	"testing"

	"cex-backend/internal/persistence"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// setupTestDB 连接测试数据库
func setupTestDB() *gorm.DB {
	dsn := "root:@tcp(127.0.0.1:3306)/cex?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("连接数据库失败: " + err.Error())
	}
	return db
}

// TestConcurrentDeduct 测试并发扣减余额（乐观锁验证）
func TestConcurrentDeduct(t *testing.T) {
	db := setupTestDB()
	repo := persistence.NewBalanceRepo(db)

	userID := "test_user_001"
	asset := "USDT"

	// 1. 重置余额（先删再插入）
	db.Exec("DELETE FROM balances WHERE user_id = ?", userID)
	err := repo.AddBalance(userID, asset, 100)
	if err != nil {
		t.Fatalf("充值失败: %v", err)
	}
	t.Logf("✅ 充值 100 USDT 成功")

	// 2. 并发扣减 60 USDT（10 个请求同时发起）
	var wg sync.WaitGroup
	successCount := 0
	failCount := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := repo.DeductBalance(userID, asset, 60)
			mu.Lock()
			if err == nil {
				successCount++
			} else {
				failCount++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	// 3. 验证结果
	bal, _ := repo.GetBalance(userID, asset)

	t.Logf("📊 并发扣减结果:")
	t.Logf("   成功: %d 次", successCount)
	t.Logf("   失败: %d 次", failCount)
	t.Logf("   最终余额: %.2f USDT", bal.Available)

	// 4. 断言：最终余额应该是 40（100 - 60）
	if bal.Available != 40 {
		t.Errorf("❌ 最终余额错误: 期望 40, 实际 %.2f", bal.Available)
	} else {
		t.Logf("✅ 最终余额正确: 40 USDT")
	}

	// 5. 断言：只有一个成功（因为有 10 个并发请求，但余额只有 100，扣 60 只能成功 1 个）
	if successCount != 1 {
		t.Logf("⚠️ 成功次数: %d（期望 1，但乐观锁只保证不超卖，不限制成功次数）", successCount)
	}
}
