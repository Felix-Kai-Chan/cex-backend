package persistence

import (
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// InitDB 初始化数据库连接
func InitDB(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent), // ✅ 关闭 SQL 日志，减少压测时的 IO 噪音
	})
	if err != nil {
		return nil, err
	}

	// ✅ 配置连接池（压测 1000 并发时避免连接被打爆）
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(500)                 // 最大打开连接数
	sqlDB.SetMaxIdleConns(100)                 // 最大空闲连接数
	sqlDB.SetConnMaxLifetime(30 * time.Minute) // 连接最大存活时间
	sqlDB.SetConnMaxIdleTime(10 * time.Minute) // 连接最大空闲时间

	// 自动迁移所有表结构
	if err := db.AutoMigrate(
		&OrderModel{},
		&TradeModel{},
		&BalanceModel{},
		&LedgerModel{},
	); err != nil {
		return nil, err
	}

	log.Println("✅ MySQL 表迁移成功")
	return db, nil
}
