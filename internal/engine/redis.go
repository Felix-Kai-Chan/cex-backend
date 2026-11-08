package engine

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient 创建 Redis 客户端。
// 显式设置超时，避免 Redis 半连接导致恢复/写入无限等待。
func NewRedisClient(addr, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
}
