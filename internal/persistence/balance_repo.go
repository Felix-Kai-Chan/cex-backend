package persistence

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

// BalanceModel 用户余额
type BalanceModel struct {
	ID        uint    `gorm:"primaryKey"`
	UserID    string  `gorm:"index;size:64;uniqueIndex:uk_user_asset"`
	Asset     string  `gorm:"size:16;uniqueIndex:uk_user_asset"`
	Available float64 `gorm:"type:decimal(40,8);default:0"`
	Frozen    float64 `gorm:"type:decimal(40,8);default:0"`
	Total     float64 `gorm:"type:decimal(40,8);default:0"`
	UpdatedAt time.Time
}

func (BalanceModel) TableName() string {
	return "balances"
}

type BalanceRepo struct {
	db *gorm.DB
}

func NewBalanceRepo(db *gorm.DB) *BalanceRepo {
	return &BalanceRepo{db: db}
}

// ========== 查询 ==========

// GetBalance 查询用户某个资产的余额
func (r *BalanceRepo) GetBalance(userID, asset string) (*BalanceModel, error) {
	var bal BalanceModel
	err := r.db.Where("user_id = ? AND asset = ?", userID, asset).First(&bal).Error
	if err == gorm.ErrRecordNotFound {
		return &BalanceModel{
			UserID:    userID,
			Asset:     asset,
			Available: 0,
			Frozen:    0,
			Total:     0,
		}, nil
	}
	return &bal, err
}

// ========== 入账 ==========

// AddBalance 入账（充值/成交后）
// ✅ 先更新 available，再单独更新 total（避免同一 SQL 里旧值计算问题）
func (r *BalanceRepo) AddBalance(userID, asset string, amount float64) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		// 尝试更新已有记录
		result := tx.Model(&BalanceModel{}).
			Where("user_id = ? AND asset = ?", userID, asset).
			Update("available", gorm.Expr("available + ?", amount))

		if result.Error != nil {
			return result.Error
		}

		if result.RowsAffected > 0 {
			// 同步更新 total = available + frozen
			return tx.Model(&BalanceModel{}).
				Where("user_id = ? AND asset = ?", userID, asset).
				Update("total", gorm.Expr("available + frozen")).Error
		}

		// 不存在则创建
		return tx.Create(&BalanceModel{
			UserID:    userID,
			Asset:     asset,
			Available: amount,
			Frozen:    0,
			Total:     amount,
		}).Error
	})
}

// ========== 扣减 ==========

// DeductBalance 扣减可用余额（条件更新）
func (r *BalanceRepo) DeductBalance(userID, asset string, amount float64) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&BalanceModel{}).
			Where("user_id = ? AND asset = ? AND available >= ?", userID, asset, amount).
			Update("available", gorm.Expr("available - ?", amount))

		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("余额不足或并发扣减冲突")
		}

		// 同步更新 total
		return tx.Model(&BalanceModel{}).
			Where("user_id = ? AND asset = ?", userID, asset).
			Update("total", gorm.Expr("available + frozen")).Error
	})
}

// ========== 冻结 / 解冻 ==========

// FreezeBalance 冻结余额（下单时调用）
// ✅ 原子条件更新：WHERE available >= amount
func (r *BalanceRepo) FreezeBalance(userID, asset string, amount float64) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&BalanceModel{}).
			Where("user_id = ? AND asset = ? AND available >= ?", userID, asset, amount).
			Updates(map[string]interface{}{
				"available": gorm.Expr("available - ?", amount),
				"frozen":    gorm.Expr("frozen + ?", amount),
			})

		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("余额不足或并发冻结冲突")
		}

		// total 不变（available 减少，frozen 增加，总和不变）
		return nil
	})
}

// UnfreezeBalance 解冻余额（撤单/成交失败时调用）
// ✅ 原子条件更新：WHERE frozen >= amount
func (r *BalanceRepo) UnfreezeBalance(userID, asset string, amount float64) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&BalanceModel{}).
			Where("user_id = ? AND asset = ? AND frozen >= ?", userID, asset, amount).
			Updates(map[string]interface{}{
				"available": gorm.Expr("available + ?", amount),
				"frozen":    gorm.Expr("frozen - ?", amount),
			})

		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("冻结余额不足或并发解冻冲突")
		}

		// total 不变（available 增加，frozen 减少，总和不变）
		return nil
	})
}

// DeductFrozen 从冻结余额中扣减（成交后调用）
// ✅ 先扣 frozen，再单独更新 total（避免同一 SQL 里旧值计算问题）
func (r *BalanceRepo) DeductFrozen(userID, asset string, amount float64) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&BalanceModel{}).
			Where("user_id = ? AND asset = ? AND frozen >= ?", userID, asset, amount).
			Update("frozen", gorm.Expr("frozen - ?", amount))

		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("冻结余额不足")
		}

		// 同步更新 total = available + frozen
		return tx.Model(&BalanceModel{}).
			Where("user_id = ? AND asset = ?", userID, asset).
			Update("total", gorm.Expr("available + frozen")).Error
	})
}
