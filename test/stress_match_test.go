package test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cex-backend/internal/engine"
)

// TestMatchEnginePerformance 撮合引擎性能压测
// 场景：预置 1000 个卖单，然后并发发 1000 个买单，测量 TPS 和延迟
func TestMatchEnginePerformance(t *testing.T) {
	ob := engine.NewOrderBook()

	// 1. 预置 1000 个卖单（价格 50000-51000）
	preloadCount := 1000
	for i := 0; i < preloadCount; i++ {
		order := &engine.Order{
			ID:        fmt.Sprintf("sell_%d", i),
			UserID:    "seller",
			Side:      engine.Sell,
			Price:     int64(50000 + i%100),
			Amount:    1,
			Remaining: 1,
			Timestamp: time.Now().UnixMilli(),
		}
		ob.AddOrder(order)
	}

	// 2. 并发发 1000 个买单
	var wg sync.WaitGroup
	var totalTrades int64
	var totalLatency int64

	concurrency := 1000
	wg.Add(concurrency)

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()

			order := &engine.Order{
				ID:        fmt.Sprintf("buy_%d", idx),
				UserID:    "buyer",
				Side:      engine.Buy,
				Price:     0, // 市价单
				Amount:    1,
				Remaining: 1,
				Timestamp: time.Now().UnixMilli(),
			}

			orderStart := time.Now()
			trades := ob.Match(order)
			latency := time.Since(orderStart).Microseconds()

			atomic.AddInt64(&totalTrades, int64(len(trades)))
			atomic.AddInt64(&totalLatency, latency)
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)

	// 3. 统计结果
	tps := float64(concurrency) / elapsed.Seconds()
	avgLatency := float64(totalLatency) / float64(concurrency)

	fmt.Printf("\n===== 撮合引擎压测结果 =====\n")
	fmt.Printf("预置卖单:    %d\n", preloadCount)
	fmt.Printf("并发买单:    %d\n", concurrency)
	fmt.Printf("总成交笔数:  %d\n", totalTrades)
	fmt.Printf("总耗时:      %v\n", elapsed)
	fmt.Printf("TPS:         %.0f\n", tps)
	fmt.Printf("平均延迟:    %.0f μs\n", avgLatency)
	fmt.Printf("===========================\n\n")

	// 断言
	if totalTrades != int64(concurrency) {
		t.Errorf("❌ 成交笔数错误：期望 %d，实际 %d", concurrency, totalTrades)
	}

	t.Logf("✅ 撮合引擎压测通过：TPS=%.0f, 平均延迟=%.0fμs", tps, avgLatency)
}

// BenchmarkMatch 基准测试（go test -bench 用）
func BenchmarkMatch(b *testing.B) {
	ob := engine.NewOrderBook()

	// 预置卖单
	for i := 0; i < 10000; i++ {
		order := &engine.Order{
			ID:        fmt.Sprintf("sell_%d", i),
			UserID:    "seller",
			Side:      engine.Sell,
			Price:     int64(50000 + i%100),
			Amount:    1,
			Remaining: 1,
			Timestamp: time.Now().UnixMilli(),
		}
		ob.AddOrder(order)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		order := &engine.Order{
			ID:        fmt.Sprintf("buy_%d", i),
			UserID:    "buyer",
			Side:      engine.Buy,
			Price:     0,
			Amount:    1,
			Remaining: 1,
			Timestamp: time.Now().UnixMilli(),
		}
		ob.Match(order)
	}
}
