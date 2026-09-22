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

	brokers := strings.Split(brokersStr, ",")

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

		handleMessage(msg, symbolCount, partitionCount, &mu)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			slog.Warn("commit message failed", "error", err)
		}
	}
}

// handleMessage 处理单条消息
// 注意：symbolCount 是 map[string]int，partitionCount 是 map[int]int，类型不同
func handleMessage(msg kafka.Message, symbolCount map[string]int, partitionCount map[int]int, mu *sync.Mutex) {
	var event mq.OrderEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		slog.Warn("unmarshal event failed",
			"partition", msg.Partition,
			"offset", msg.Offset,
			"error", err,
		)
		return
	}

	key := string(msg.Key)

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
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
