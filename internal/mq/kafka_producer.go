package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer Kafka 生产者封装
type KafkaProducer struct {
	writer *kafka.Writer
	topic  string
}

// NewKafkaProducer 创建 Kafka producer
func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // 按 key 哈希分区
		RequiredAcks: kafka.RequireOne,
		Async:        false, // 同步发送（保证可靠性）
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}

	return &KafkaProducer{
		writer: w,
		topic:  topic,
	}
}

// Send 发送单条消息
func (p *KafkaProducer) Send(ctx context.Context, key string, event *OrderEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event failed: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: data,
	})
	if err != nil {
		return fmt.Errorf("kafka write failed: %w", err)
	}

	slog.Debug("kafka message sent",
		"topic", p.topic,
		"key", key,
		"event_type", event.EventType,
		"event_id", event.EventID,
	)
	return nil
}

// SendRaw 发送原始字节（不做序列化，用于 outbox publisher）
func (p *KafkaProducer) SendRaw(ctx context.Context, key string, payload []byte) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("kafka write failed: %w", err)
	}
	return nil
}

// Close 关闭 producer
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
