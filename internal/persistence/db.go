package persistence

import (
	"log"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// InitDB 初始化数据库连接
func InitDB(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// 自动迁移所有表结构
	if err := db.AutoMigrate(
		&OrderModel{},
		&TradeModel{},
		&BalanceModel{},
		&LedgerModel{}, // ✅ 新增流水表
	); err != nil {
		return nil, err
	}

	log.Println("✅ MySQL 表迁移成功")
	return db, nil
}
