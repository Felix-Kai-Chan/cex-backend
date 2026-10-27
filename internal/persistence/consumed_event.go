package persistence

import "time"

// ConsumedEventModel 已消费事件记录（用于消费者幂等去重）
type ConsumedEventModel struct {
	EventID     string    `gorm:"primaryKey;size:64"`
	ConsumedAt  time.Time `gorm:"not null"`
}

func (ConsumedEventModel) TableName() string {
	return "consumed_events"
}
