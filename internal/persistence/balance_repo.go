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
func (r *BalanceRepo) AddBalance(userID, asset string, amount float64) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		var bal BalanceModel
		if err := tx.Where("user_id = ? AND asset = ?", userID, asset).First(&bal).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				bal = BalanceModel{
					UserID:    userID,
					Asset:     asset,
					Available: amount,
					Frozen:    0,
					Total:     amount,
				}
				return tx.Create(&bal).Error
			}
			return err
		}
		bal.Available += amount
		bal.Total = bal.Available + bal.Frozen
		return tx.Save(&bal).Error
	})
}

// ========== 乐观锁扣减（重点） ==========

// DeductBalance 扣减可用余额（乐观锁）
// 用 WHERE available >= amount 保证不超卖
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

		return nil
	})
}

// ========== 冻结 / 解冻 ==========

// FreezeBalance 冻结余额（下单时调用）
func (r *BalanceRepo) FreezeBalance(userID, asset string, amount float64) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		var bal BalanceModel
		if err := tx.Where("user_id = ? AND asset = ?", userID, asset).First(&bal).Error; err != nil {
			return err
		}
		if bal.Available < amount {
			return fmt.Errorf("余额不足")
		}
		bal.Available -= amount
		bal.Frozen += amount
		bal.Total = bal.Available + bal.Frozen
		return tx.Save(&bal).Error
	})
}

// UnfreezeBalance 解冻余额（撤单/成交失败时调用）
func (r *BalanceRepo) UnfreezeBalance(userID, asset string, amount float64) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		var bal BalanceModel
		if err := tx.Where("user_id = ? AND asset = ?", userID, asset).First(&bal).Error; err != nil {
			return err
		}
		bal.Available += amount
		bal.Frozen -= amount
		bal.Total = bal.Available + bal.Frozen
		return tx.Save(&bal).Error
	})
}
