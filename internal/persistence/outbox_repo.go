package persistence

import (
	"time"

	"gorm.io/gorm"
)

// OutboxModel Outbox 表（待发消息）
type OutboxModel struct {
	ID          uint      `gorm:"primaryKey"`
	AggregateID string    `gorm:"index;size:64"` // 关联的业务 ID（如 order_id）
	KafkaKey    string    `gorm:"size:64"`       // ✅ Kafka 分区 key（symbol）
	Topic       string    `gorm:"size:64"`       // Kafka topic
	Payload     string    `gorm:"type:text"`     // 消息体（JSON）
	Status      string    `gorm:"index;size:16"` // PENDING / SENT / FAILED
	RetryCount  int       `gorm:"default:0"`     // 重试次数
	LastError   string    `gorm:"size:512"`      // 最后错误
	CreatedAt   time.Time `gorm:"index"`
	UpdatedAt   time.Time
	SentAt      *time.Time // 发送成功时间
}

func (OutboxModel) TableName() string {
	return "outbox"
}

// OutboxRepo Outbox 数据访问
type OutboxRepo struct {
	db *gorm.DB
}

func NewOutboxRepo(db *gorm.DB) *OutboxRepo {
	return &OutboxRepo{db: db}
}

// MaxOutboxRetry Outbox 最大重试次数，超过后标记为 DEAD
const MaxOutboxRetry = 5

// Create 在事务内写 outbox（和业务表同事务）
func (r *OutboxRepo) Create(tx *gorm.DB, msg *OutboxModel) error {
	return tx.Create(msg).Error
}

// FetchPendingLocked 在事务内锁定待发送消息（FOR UPDATE SKIP LOCKED）
func (r *OutboxRepo) FetchPendingLocked(tx *gorm.DB, limit int) ([]OutboxModel, error) {
	var msgs []OutboxModel
	err := tx.Raw(
		"SELECT * FROM outbox WHERE status = ? ORDER BY id ASC LIMIT ? FOR UPDATE SKIP LOCKED",
		"PENDING", limit,
	).Scan(&msgs).Error
	return msgs, err
}

// Transaction 包装数据库事务（供 publisher 使用）
func (r *OutboxRepo) Transaction(fn func(tx *gorm.DB) error) error {
	return r.db.Transaction(fn)
}

// FetchPending 拉取待发送的消息（带锁，多实例安全）
func (r *OutboxRepo) FetchPending(limit int) ([]OutboxModel, error) {
	var msgs []OutboxModel
	err := r.db.
		Where("status = ?", "PENDING").
		Order("id ASC").
		Limit(limit).
		Find(&msgs).Error
	return msgs, err
}

// MarkSent 标记为已发送
func (r *OutboxRepo) MarkSent(id uint) error {
	now := time.Now()
	return r.db.Model(&OutboxModel{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":     "SENT",
			"sent_at":    &now,
			"updated_at": now,
		}).Error
}

// MarkSentTx 在事务内标记为已发送
func (r *OutboxRepo) MarkSentTx(tx *gorm.DB, id uint) error {
	now := time.Now()
	return tx.Model(&OutboxModel{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":     "SENT",
			"sent_at":    &now,
			"updated_at": now,
		}).Error
}

// MarkFailed 标记为失败（保留重试，超过阈值转 DEAD）
func (r *OutboxRepo) MarkFailed(id uint, errMsg string) error {
	return r.db.Model(&OutboxModel{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":      gorm.Expr("CASE WHEN retry_count + 1 > ? THEN 'DEAD' ELSE 'PENDING' END", MaxOutboxRetry),
			"retry_count": gorm.Expr("retry_count + 1"),
			"last_error":  errMsg,
			"updated_at":  time.Now(),
		}).Error
}

// MarkFailedTx 在事务内标记为失败
func (r *OutboxRepo) MarkFailedTx(tx *gorm.DB, id uint, errMsg string) error {
	return tx.Model(&OutboxModel{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":      gorm.Expr("CASE WHEN retry_count + 1 > ? THEN 'DEAD' ELSE 'PENDING' END", MaxOutboxRetry),
			"retry_count": gorm.Expr("retry_count + 1"),
			"last_error":  errMsg,
			"updated_at":  time.Now(),
		}).Error
}
