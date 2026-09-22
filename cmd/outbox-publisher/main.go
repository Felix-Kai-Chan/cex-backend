package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cex-backend/internal/mq"
	"cex-backend/internal/persistence"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// 1. 配置
	dsn := getEnv("MYSQL_DSN", "root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local")
	brokersStr := getEnv("KAFKA_BROKERS", "localhost:19092")
	defaultTopic := getEnv("KAFKA_TOPIC", "cex.order.events")
	pollInterval := getEnvInt("OUTBOX_POLL_INTERVAL_MS", 500)
	batchSize := getEnvInt("OUTBOX_BATCH_SIZE", 100)

	brokers := strings.Split(brokersStr, ",")

	// 2. 连接 MySQL
	db, err := persistence.InitDB(dsn)
	if err != nil {
		slog.Error("MySQL 连接失败", "error", err)
		os.Exit(1)
	}
	outboxRepo := persistence.NewOutboxRepo(db)

	// 3. 初始化 Kafka producer
	producer := mq.NewKafkaProducer(brokers, defaultTopic)
	defer producer.Close()

	slog.Info("outbox publisher started",
		"brokers", brokers,
		"default_topic", defaultTopic,
		"poll_interval_ms", pollInterval,
		"batch_size", batchSize,
	)

	// 4. 优雅退出
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("shutdown signal received")
		cancel()
	}()

	// 5. 主循环
	ticker := time.NewTicker(time.Duration(pollInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("outbox publisher stopped")
			return
		case <-ticker.C:
			if err := processBatch(ctx, outboxRepo, producer, batchSize); err != nil {
				slog.Warn("process batch failed", "error", err)
			}
		}
	}
}

// processBatch 拉一批待发消息，逐条发 Kafka
func processBatch(ctx context.Context, repo *persistence.OutboxRepo, producer *mq.KafkaProducer, limit int) error {
	msgs, err := repo.FetchPending(limit)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return nil
	}

	sentCount := 0
	for _, msg := range msgs {
		// 用 AggregateID 作为 Kafka key
		err := producer.SendRaw(ctx, msg.AggregateID, []byte(msg.Payload))
		if err != nil {
			slog.Warn("send failed",
				"outbox_id", msg.ID,
				"aggregate_id", msg.AggregateID,
				"error", err,
			)
			_ = repo.MarkFailed(msg.ID, err.Error())
			continue
		}
		_ = repo.MarkSent(msg.ID)
		sentCount++
	}

	if sentCount > 0 {
		slog.Info("outbox batch processed",
			"total", len(msgs),
			"sent", sentCount,
		)
	}

	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		n := 0
		for _, c := range value {
			if c < '0' || c > '9' {
				return defaultValue
			}
			n = n*10 + int(c-'0')
		}
		return n
	}
	return defaultValue
}
