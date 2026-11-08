package persistence

import (
	"fmt"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// InitDB 初始化数据库连接
func InitDB(dsn string) (*gorm.DB, error) {
	const (
		maxAttempts   = 10
		retryInterval = 2 * time.Second
	)

	var (
		db      *gorm.DB
		lastErr error
	)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		log.Printf("[DB] 连接 MySQL，第 %d/%d 次尝试...", attempt, maxAttempts)

		db, lastErr = gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent), // 关闭 SQL 日志，减少压测时的 IO 噪音
		})
		if lastErr != nil {
			log.Printf("[DB] gorm.Open 失败（第 %d/%d 次）: %v", attempt, maxAttempts, lastErr)
		} else {
			sqlDB, dbErr := db.DB()
			if dbErr != nil {
				lastErr = dbErr
				log.Printf("[DB] 获取底层 sql.DB 失败（第 %d/%d 次）: %v", attempt, maxAttempts, dbErr)
			} else if pingErr := sqlDB.Ping(); pingErr != nil {
				lastErr = pingErr
				log.Printf("[DB] Ping 失败（第 %d/%d 次）: %v", attempt, maxAttempts, pingErr)
				_ = sqlDB.Close()
			} else {
				// Ping 成功，连接可用，配置连接池
				sqlDB.SetMaxOpenConns(100)                // 最大打开连接数
				sqlDB.SetMaxIdleConns(50)                 // 最大空闲连接数
				sqlDB.SetConnMaxLifetime(5 * time.Minute) // 连接最大存活时间
				sqlDB.SetConnMaxIdleTime(1 * time.Minute) // 空闲连接回收时间

				log.Printf("[DB] Ping 成功，MySQL 已连接（第 %d/%d 次）", attempt, maxAttempts)
				lastErr = nil
				break
			}
		}

		if attempt < maxAttempts {
			log.Printf("[DB] %s 后重试...", retryInterval)
			time.Sleep(retryInterval)
		}
	}

	if lastErr != nil || db == nil {
		log.Printf("[DB][FATAL] MySQL 初始化失败，已重试 %d 次: %v", maxAttempts, lastErr)
		panic(fmt.Sprintf("MySQL 初始化失败: %v", lastErr))
	}

	// 自动迁移所有表结构
	if err := db.AutoMigrate(
		&OrderModel{},
		&TradeModel{},
		&BalanceModel{},
		&LedgerModel{},
		&OutboxModel{},        // 新增
		&ConsumedEventModel{}, // 消费者幂等去重
	); err != nil {
		return nil, err
	}

	log.Println("✅ MySQL 表迁移成功")
	return db, nil
}
