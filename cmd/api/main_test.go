package main

import (
	"context"
	"os"
	"strconv"
	"testing"

	"cex-backend/internal/engine"
	"cex-backend/internal/persistence"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func startupTestRedis(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:16379"
	}
	db := 13
	if raw := os.Getenv("TEST_REDIS_DB"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("invalid TEST_REDIS_DB: %v", err)
		}
		db = parsed
	}

	rdb := engine.NewRedisClient(addr, "", db)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("Redis unavailable: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func startupTestMySQL(t *testing.T) *gorm.DB {
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
	if err := db.AutoMigrate(&persistence.OrderModel{}); err != nil {
		t.Fatalf("migrate orders: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func TestRecoverSnapshottersFailsWholeStartupAndDoesNotStartAutoSave(t *testing.T) {
	rdb := startupTestRedis(t)
	db := startupTestMySQL(t)
	eng := engine.NewEngine()

	const good = "2D-STARTUP-GOOD/USDT"
	const bad = "2D-STARTUP-BAD/USDT"
	ctx := context.Background()
	_ = rdb.Del(ctx, "orderbook:snapshot:"+good, "orderbook:wal:"+good, "orderbook:snapshot:"+bad, "orderbook:wal:"+bad)
	t.Cleanup(func() {
		_ = rdb.Del(context.Background(), "orderbook:snapshot:"+good, "orderbook:wal:"+good, "orderbook:snapshot:"+bad, "orderbook:wal:"+bad)
	})

	// good 没有 Snapshot/WAL，走 MySQL 重建路径并成功。
	// bad 有损坏 WAL，Load 必须在启动边界失败。
	if err := rdb.RPush(ctx, "orderbook:wal:"+bad, "{broken-json").Err(); err != nil {
		t.Fatalf("seed corrupt WAL: %v", err)
	}

	loaded, err := recoverSnapshotters(eng, rdb, db, []string{good, bad})
	if err == nil {
		t.Fatal("recoverSnapshotters succeeded although one symbol failed")
	}
	if len(loaded) != 1 || loaded[0].symbol != good {
		t.Fatalf("partial recovery = %+v, want only %s", loaded, good)
	}
	if loaded[0].snapshotter.AutoSaveStarted() {
		t.Fatal("AutoSave started before all symbols recovered")
	}
	if eng.GetSnapshotter(good) != nil {
		t.Fatal("Snapshotter was registered before all symbols recovered")
	}
}

func TestStartSnapshottersOnlyAfterAllRecovered(t *testing.T) {
	rdb := startupTestRedis(t)
	db := startupTestMySQL(t)
	eng := engine.NewEngine()

	const symbol = "2D-STARTUP-OK/USDT"
	ctx := context.Background()
	_ = rdb.Del(ctx, "orderbook:snapshot:"+symbol, "orderbook:wal:"+symbol)
	t.Cleanup(func() {
		_ = rdb.Del(context.Background(), "orderbook:snapshot:"+symbol, "orderbook:wal:"+symbol)
	})

	loaded, err := recoverSnapshotters(eng, rdb, db, []string{symbol})
	if err != nil {
		t.Fatalf("recoverSnapshotters: %v", err)
	}
	startSnapshotters(eng, loaded)
	t.Cleanup(func() {
		for _, item := range loaded {
			item.snapshotter.Stop()
		}
	})

	if eng.GetSnapshotter(symbol) == nil {
		t.Fatal("Snapshotter not registered after successful recovery")
	}
	if !loaded[0].snapshotter.AutoSaveStarted() {
		t.Fatal("AutoSave not started after successful recovery")
	}
}
