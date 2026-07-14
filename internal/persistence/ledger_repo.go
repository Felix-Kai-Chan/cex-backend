package persistence

import (
	"time"

	"gorm.io/gorm"
)

// LedgerModel 资金流水表
type LedgerModel struct {
	ID        uint    `gorm:"primaryKey"`
	UserID    string  `gorm:"index;size:64"`
	Asset     string  `gorm:"size:16"`
	Amount    float64 `gorm:"type:decimal(40,8)"`
	Balance   float64 `gorm:"type:decimal(40,8)"`
	Frozen    float64 `gorm:"type:decimal(40,8)"`
	Type      string  `gorm:"size:32"` // DEPOSIT / WITHDRAW / TRADE / UNFREEZE
	OrderID   string  `gorm:"index;size:64"`
	TradeID   string  `gorm:"index;size:64"`
	Desc      string  `gorm:"size:128"`
	CreatedAt time.Time
}

func (LedgerModel) TableName() string {
	return "ledgers"
}

type LedgerRepo struct {
	db *gorm.DB
}

func NewLedgerRepo(db *gorm.DB) *LedgerRepo {
	return &LedgerRepo{db: db}
}

// Record 记录流水
func (r *LedgerRepo) Record(userID, asset string, amount, balance, frozen float64, ledgerType, orderID, tradeID, desc string) error {
	entry := LedgerModel{
		UserID:    userID,
		Asset:     asset,
		Amount:    amount,
		Balance:   balance,
		Frozen:    frozen,
		Type:      ledgerType,
		OrderID:   orderID,
		TradeID:   tradeID,
		Desc:      desc,
		CreatedAt: time.Now(),
	}
	return r.db.Create(&entry).Error
}

// GetLedgerByUser 查询用户流水
func (r *LedgerRepo) GetLedgerByUser(userID string, entries *[]LedgerModel) error {
	return r.db.Where("user_id = ?", userID).Order("created_at DESC").Find(entries).Error
}
