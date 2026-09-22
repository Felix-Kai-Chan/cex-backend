#!/bin/bash

# Kafka 分区 + Consumer 验证
# 验证：同一 symbol 消息进同一 partition + Consumer 串行消费
# 用法：./test/test_kafka_consumer.sh

set -e

cd "$(dirname "$0")/.."

API="http://localhost:8080/api/v1"
KAFKA="docker exec cex-kafka /opt/kafka/bin"

echo "=========================================="
echo " Kafka 分区 + Consumer 验证"
echo "=========================================="
echo ""

# 0. 清环境
echo "[0/6] 清环境..."
pkill -9 -f "go-build.*main" 2>/dev/null || true
pkill -9 -f "go run cmd/api/main.go" 2>/dev/null || true
pkill -9 -f "go run cmd/outbox-publisher" 2>/dev/null || true
pkill -9 -f "go run cmd/event-consumer" 2>/dev/null || true
sleep 1

docker exec cex-mysql mysql -uroot -proot cex \
    -e "TRUNCATE TABLE orders; TRUNCATE TABLE trades; TRUNCATE TABLE outbox;" 2>/dev/null
redis-cli -p 6379 FLUSHDB > /dev/null

# 删 consumer group（避免 offset 状态干扰）
$KAFKA/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --delete --group cex-event-consumer 2>/dev/null || true

# 重建 topic
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic cex.order.events 2>/dev/null || true
sleep 2
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic cex.order.events --partitions 3 --replication-factor 1 2>/dev/null
echo "✅ 环境已清"
echo ""

# 1. 启动 API
echo "[1/6] 启动 API..."
go run cmd/api/main.go > /tmp/cex_consumer_api.log 2>&1 &
API_PID=$!

for i in $(seq 1 15); do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "   API 已就绪（等了 ${i}s）"
        break
    fi
    sleep 1
done
echo ""

# 2. 启动 Publisher
echo "[2/6] 启动 outbox-publisher..."
go run cmd/outbox-publisher/main.go > /tmp/cex_consumer_pub.log 2>&1 &
PUB_PID=$!
sleep 2
echo "   Publisher PID: $PUB_PID"
echo ""

# 3. 启动 Consumer
echo "[3/6] 启动 event-consumer..."
go run cmd/event-consumer/main.go > /tmp/cex_consumer_consumer.log 2>&1 &
CONSUMER_PID=$!
sleep 3
echo "   Consumer PID: $CONSUMER_PID"
echo ""

# 4. 下单
echo "[4/6] 下单：BTC/USDT x 5 + ETH/USDT x 5..."
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"part_test","asset":"USDT","amount":100000000}' > /dev/null

for i in 1 2 3 4 5; do
    curl -s -X POST "$API/orders" \
        -H "Content-Type: application/json" \
        -d "{\"user_id\":\"part_test\",\"symbol\":\"BTC/USDT\",\"side\":\"BUY\",\"order_type\":\"LIMIT\",\"price\":$((100+i)),\"amount\":1}" > /dev/null
done
echo "   BTC/USDT: 5 orders placed"

for i in 1 2 3 4 5; do
    curl -s -X POST "$API/orders" \
        -H "Content-Type: application/json" \
        -d "{\"user_id\":\"part_test\",\"symbol\":\"ETH/USDT\",\"side\":\"BUY\",\"order_type\":\"LIMIT\",\"price\":$((200+i)),\"amount\":1}" > /dev/null
done
echo "   ETH/USDT: 5 orders placed"
echo ""

# 5. 等 publisher + consumer 处理
echo "[5/6] 等 publisher 发送 + consumer 消费..."
sleep 5

OUTBOX_SENT=$(docker exec cex-mysql mysql -uroot -proot cex -N \
    -e "SELECT COUNT(*) FROM outbox WHERE status='SENT';" 2>/dev/null | tr -d ' ')
echo "   outbox SENT: ${OUTBOX_SENT} (expected 10)"

# 等 Consumer 消费 10 条
echo "   等 Consumer 消费 10 条..."
for i in $(seq 1 20); do
    CONSUMED=$(grep "event consumed" /tmp/cex_consumer_consumer.log 2>/dev/null | wc -l | tr -d ' ')
    if [ "$CONSUMED" -ge 10 ]; then
        echo "   consumer consumed: ${CONSUMED} (done)"
        break
    fi
    sleep 1
    if [ "$i" -eq 20 ]; then
        echo "   timeout waiting for consumer"
        tail -5 /tmp/cex_consumer_consumer.log
        kill $API_PID $PUB_PID $CONSUMER_PID 2>/dev/null || true
        exit 1
    fi
done
echo ""

# 6. 分析分区
echo "[6/6] 分析分区..."

BTC_PARTITIONS=$(grep "event consumed" /tmp/cex_consumer_consumer.log | grep '"symbol":"BTC/USDT"' | grep -o '"partition":[0-9]*' | sort -u)
ETH_PARTITIONS=$(grep "event consumed" /tmp/cex_consumer_consumer.log | grep '"symbol":"ETH/USDT"' | grep -o '"partition":[0-9]*' | sort -u)

echo "   BTC/USDT partitions:"
echo "$BTC_PARTITIONS" | sed 's/^/     /'
echo ""
echo "   ETH/USDT partitions:"
echo "$ETH_PARTITIONS" | sed 's/^/     /'
echo ""

BTC_COUNT=$(echo "$BTC_PARTITIONS" | grep -c "partition" || echo "0")
ETH_COUNT=$(echo "$ETH_PARTITIONS" | grep -c "partition" || echo "0")

if [ "$BTC_COUNT" = "1" ] && [ "$ETH_COUNT" = "1" ]; then
    BTC_P=$(echo "$BTC_PARTITIONS" | grep -o '[0-9]*' | head -1)
    ETH_P=$(echo "$ETH_PARTITIONS" | grep -o '[0-9]*' | head -1)
    if [ "$BTC_P" != "$ETH_P" ]; then
        echo "PASS: BTC -> partition ${BTC_P}, ETH -> partition ${ETH_P}"
    else
        echo "WARN: BTC and ETH both in partition ${BTC_P} (hash collision, low probability)"
    fi
else
    echo "FAIL: same symbol spread across multiple partitions"
    echo "   BTC partitions: $BTC_PARTITIONS"
    echo "   ETH partitions: $ETH_PARTITIONS"
    kill $API_PID $PUB_PID $CONSUMER_PID 2>/dev/null || true
    exit 1
fi
echo ""

echo "=========================================="
echo " Result"
echo "=========================================="
echo "outbox SENT: ${OUTBOX_SENT}/10"
echo "consumer consumed: ${CONSUMED}/10"
echo "BTC partition: ${BTC_P}"
echo "ETH partition: ${ETH_P}"
echo "=========================================="
echo ""
echo "API PID: $API_PID"
echo "Publisher PID: $PUB_PID"
echo "Consumer PID: $CONSUMER_PID"
echo "To stop: kill $API_PID $PUB_PID $CONSUMER_PID"
echo ""