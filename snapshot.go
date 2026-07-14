package main

import (
	"fmt"
	"time"

	"cex-backend/internal/engine"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 连接 Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// 创建订单簿
	ob := engine.NewOrderBook()

	// 挂一些订单
	ob.AddOrder(&engine.Order{
		ID:        "snap_buy_1",
		UserID:    "A",
		Side:      engine.Buy,
		Price:     100,
		Amount:    10,
		Remaining: 10,
		Timestamp: time.Now().UnixMilli(),
	})
	ob.AddOrder(&engine.Order{
		ID:        "snap_sell_1",
		UserID:    "B",
		Side:      engine.Sell,
		Price:     110,
		Amount:    5,
		Remaining: 5,
		Timestamp: time.Now().UnixMilli(),
	})

	// 创建快照管理器
	snapshotter := engine.NewSnapshotter(rdb, ob, "BTC/USDT")

	// 保存快照
	if err := snapshotter.Save(); err != nil {
		fmt.Println("❌ 保存失败:", err)
	} else {
		fmt.Println("✅ 快照保存成功")
	}

	// 重新加载快照到新订单簿
	newOb := engine.NewOrderBook()
	newSnapshotter := engine.NewSnapshotter(rdb, newOb, "BTC/USDT")
	if err := newSnapshotter.Load(); err != nil {
		fmt.Println("❌ 加载失败:", err)
	} else {
		fmt.Println("✅ 快照加载成功")
	}

	// 验证恢复的数据
	fmt.Printf("恢复后 BestBid: %d, BestAsk: %d\n", newOb.BestBid(), newOb.BestAsk())
}
