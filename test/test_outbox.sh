#!/bin/bash

# Kafka + Outbox 完整链路测试
# 验证：下单 → outbox 表有记录 → 发件进程发 Kafka → outbox 变 SENT → Kafka 能消费到
# 用法：./test/test_outbox.sh

set -e

cd "$(dirname "$0")/.."

API="http://localhost:8080/api/v1"
MYSQL="docker exec cex-mysql mysql -uroot -proot cex"
KAFKA="docker exec cex-kafka /opt/kafka/bin"

echo "=========================================="
echo " Kafka + Outbox 完整链路测试"
echo "=========================================="
echo ""

# 0. 清环境
echo "[0/6] 清环境..."
pkill -9 -f "go-build.*main" 2>/dev/null || true
pkill -9 -f "go run cmd/api/main.go" 2>/dev/null || true
pkill -9 -f "go run cmd/outbox-publisher" 2>/dev/null || true
sleep 1

docker exec cex-mysql mysql -uroot -proot cex \
    -e "TRUNCATE TABLE orders; TRUNCATE TABLE trades; TRUNCATE TABLE outbox;" 2>/dev/null
redis-cli -p 6379 FLUSHDB > /dev/null
echo "✅ 环境已清"
echo ""

# 1. 启动 API 服务
echo "[1/6] 启动 API 服务..."
go run cmd/api/main.go > /tmp/cex_api.log 2>&1 &
API_PID=$!

for i in $(seq 1 15); do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ API 已就绪（等了 ${i}s）"
        break
    fi
    sleep 1
    if [ $i -eq 15 ]; then
        echo "❌ API 启动超时"
        cat /tmp/cex_api.log
        exit 1
    fi
done
echo ""

# 2. 充值 + 下单
echo "[2/6] 充值 + 下单..."
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"outbox_test","asset":"USDT","amount":1000000}' > /dev/null

ORDER=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"outbox_test","symbol":"BTC/USDT","side":"BUY","order_type":"LIMIT","price":100,"amount":5}')

ORDER_ID=$(echo "$ORDER" | grep -o '"order_id":"[^"]*"' | cut -d'"' -f4)
echo "   order_id: $ORDER_ID"
echo "   返回: $ORDER"

if [ -z "$ORDER_ID" ]; then
    echo "❌ 下单失败"
    kill $API_PID 2>/dev/null || true
    exit 1
fi
echo ""

# 3. 查 orders 表 + outbox 表
echo "[3/6] 查 orders 表 + outbox 表..."

ORDERS_COUNT=$($MYSQL -N -e "SELECT COUNT(*) FROM orders WHERE order_id='$ORDER_ID';" 2>/dev/null)
echo "   orders 表记录数: $ORDERS_COUNT"

if [ "$ORDERS_COUNT" != "1" ]; then
    echo "❌ orders 表没有这条记录"
    kill $API_PID 2>/dev/null || true
    exit 1
fi

OUTBOX_ROW=$($MYSQL -N -e "SELECT id, status, topic FROM outbox WHERE aggregate_id='$ORDER_ID';" 2>/dev/null)
echo "   outbox 行: $OUTBOX_ROW"

if [ -z "$OUTBOX_ROW" ]; then
    echo "❌ outbox 表没有这条记录"
    kill $API_PID 2>/dev/null || true
    exit 1
fi

OUTBOX_STATUS=$(echo "$OUTBOX_ROW" | awk '{print $2}')
if [ "$OUTBOX_STATUS" != "PENDING" ]; then
    echo "❌ outbox status 应该是 PENDING，实际 $OUTBOX_STATUS"
    kill $API_PID 2>/dev/null || true
    exit 1
fi
echo "✅ orders + outbox 同事务写入成功（status=PENDING）"
echo ""

# 4. 启动 outbox-publisher
echo "[4/6] 启动 outbox-publisher..."
go run cmd/outbox-publisher/main.go > /tmp/cex_outbox_pub.log 2>&1 &
PUB_PID=$!

sleep 3

if ! kill -0 $PUB_PID 2>/dev/null; then
    echo "❌ publisher 挂了"
    cat /tmp/cex_outbox_pub.log
    kill $API_PID 2>/dev/null || true
    exit 1
fi
echo "✅ publisher 运行中（PID=$PUB_PID）"
echo ""

# 5. 查 outbox 表状态
echo "[5/6] 等 publisher 处理完，查 outbox 状态..."
sleep 3

OUTBOX_STATUS=$($MYSQL -N -e "SELECT status FROM outbox WHERE aggregate_id='$ORDER_ID';" 2>/dev/null)
SENT_AT=$($MYSQL -N -e "SELECT sent_at FROM outbox WHERE aggregate_id='$ORDER_ID';" 2>/dev/null)

echo "   status: $OUTBOX_STATUS"
echo "   sent_at: $SENT_AT"

if [ "$OUTBOX_STATUS" != "SENT" ]; then
    echo "❌ outbox status 应该 SENT，实际 $OUTBOX_STATUS"
    echo "   publisher 日志："
    tail -20 /tmp/cex_outbox_pub.log
    kill $API_PID $PUB_PID 2>/dev/null || true
    exit 1
fi
echo "✅ outbox 已标记 SENT"
echo ""

# 6. 从 Kafka 消费验证
echo "[6/6] 从 Kafka 消费验证..."

KAFKA_MSG=$($KAFKA/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic cex.order.events \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null)

echo "   Kafka 收到: $KAFKA_MSG"

if echo "$KAFKA_MSG" | grep -q "$ORDER_ID"; then
    echo "✅ Kafka 消息包含 order_id"
else
    echo "❌ Kafka 消息没有 order_id"
    kill $API_PID $PUB_PID 2>/dev/null || true
    exit 1
fi
echo ""

echo "=========================================="
echo "✅ 完整链路通过"
echo "=========================================="
echo ""
echo "orders + outbox 同事务 ✅"
echo "outbox → Kafka ✅"
echo "Kafka 消息包含 order_id ✅"
echo ""
echo "API PID: $API_PID"
echo "Publisher PID: $PUB_PID"
echo "To stop: kill $API_PID $PUB_PID"
echo ""