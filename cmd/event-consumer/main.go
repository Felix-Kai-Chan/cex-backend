package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cex-backend/internal/mq"
	"cex-backend/internal/persistence"

	"github.com/segmentio/kafka-go"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	brokersStr := getEnv("KAFKA_BROKERS", "localhost:19092")
	topic := getEnv("KAFKA_TOPIC", "cex.order.events")
	groupID := getEnv("KAFKA_GROUP_ID", "cex-event-consumer")
	dsn := getEnv("MYSQL_DSN", "root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local")

	brokers := strings.Split(brokersStr, ",")

	// 初始化 MySQL（含 consumed_events 表迁移）
	db, err := persistence.InitDB(dsn)
	if err != nil {
		slog.Error("MySQL 连接失败", "error", err)
		os.Exit(1)
	}

	slog.Info("event consumer starting",
		"brokers", brokers,
		"topic", topic,
		"group_id", groupID,
	)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("shutdown signal received")
		cancel()
	}()

	var mu sync.Mutex
	symbolCount := make(map[string]int)
	partitionCount := make(map[int]int)

	for {
		select {
		case <-ctx.Done():
			slog.Info("consumer stopped",
				"symbols", symbolCount,
				"partitions", partitionCount,
			)
			return
		default:
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("fetch message failed", "error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var event mq.OrderEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Warn("unmarshal event failed",
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error", err,
			)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		key := string(msg.Key)

		// 幂等：事务内写 consumed_events，唯一键冲突则跳过
		idempotent := func() bool {
			tx := db.Begin()
			if tx.Error != nil {
				slog.Warn("begin tx failed", "error", tx.Error)
				return false
			}
			ce := persistence.ConsumedEventModel{
				EventID:    event.EventID,
				ConsumedAt: time.Now(),
			}
			if err := tx.Create(&ce).Error; err != nil {
				tx.Rollback()
				if strings.Contains(err.Error(), "Duplicate entry") {
					return true // 已消费，跳过
				}
				slog.Warn("insert consumed event failed", "error", err)
				return false
			}

			handleEvent(event)

			if err := tx.Commit().Error; err != nil {
				slog.Warn("commit consumed event failed", "error", err)
				return false
			}
			return true
		}()

		if !idempotent {
			// 处理失败，不提交 offset，稍后重试
			time.Sleep(500 * time.Millisecond)
			continue
		}

		mu.Lock()
		symbolCount[event.Symbol]++
		partitionCount[int(msg.Partition)]++
		mu.Unlock()

		slog.Info("event consumed",
			"partition", msg.Partition,
			"offset", msg.Offset,
			"key", key,
			"event_type", event.EventType,
			"order_id", event.OrderID,
			"symbol", event.Symbol,
			"user_id", event.UserID,
			"price", event.Price,
			"amount", event.Amount,
		)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			slog.Warn("commit message failed", "error", err)
		}
	}
}

func handleEvent(event mq.OrderEvent) {
	switch event.EventType {
	case mq.EventOrderCreated:
		slog.Info("handle ORDER_CREATED",
			"order_id", event.OrderID,
			"user_id", event.UserID,
			"symbol", event.Symbol,
		)
	case mq.EventOrderCancelled:
		slog.Info("handle ORDER_CANCELLED",
			"order_id", event.OrderID,
			"user_id", event.UserID,
			"symbol", event.Symbol,
		)
	case mq.EventTradeFilled:
		slog.Info("handle TRADE_FILLED",
			"trade_id", event.TradeID,
			"buy_order", event.BuyOrder,
			"sell_order", event.SellOrder,
			"buy_user", event.BuyUserID,
			"sell_user", event.SellUserID,
			"symbol", event.Symbol,
			"price", event.Price,
			"amount", event.Amount,
		)
	default:
		slog.Warn("unknown event type", "event_type", event.EventType)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}