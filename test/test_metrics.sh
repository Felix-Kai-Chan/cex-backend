#!/bin/bash

# 熔断 + 延迟分位验证脚本
# 自动清环境 + 启动服务 + 验证
# 用法：./test/test_metrics.sh

set -e

cd "$(dirname "$0")/.."

API="http://localhost:8080/api/v1"
METRICS="http://localhost:8080/metrics"

echo "=========================================="
echo " 熔断 + 延迟分位验证脚本"
echo "=========================================="
echo ""

# 0. 清环境
echo "[0/6] 清环境..."
pkill -9 -f "go-build.*main" 2>/dev/null || true
pkill -9 -f "go run cmd/api/main.go" 2>/dev/null || true
sleep 1

if pgrep -f "go-build.*main" > /dev/null; then
    echo "❌ 杀不干净"
    exit 1
fi

docker exec cex-mysql mysql -uroot -proot cex \
    -e "TRUNCATE TABLE orders; TRUNCATE TABLE trades;" 2>/dev/null
redis-cli -p 6379 FLUSHDB > /dev/null
echo "✅ 环境已清"
echo ""

# 1. 启动服务
echo "[1/6] 启动服务..."
go run cmd/api/main.go > /tmp/cex_metrics_test.log 2>&1 &
SERVER_PID=$!

for i in $(seq 1 15); do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ 服务已就绪（等了 ${i}s）"
        break
    fi
    sleep 1
    if [ $i -eq 15 ]; then
        echo "❌ 服务启动超时"
        cat /tmp/cex_metrics_test.log
        exit 1
    fi
done
echo ""

# 2. 充值
echo "[2/6] 充值..."
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"metrics_test","asset":"USDT","amount":10000000}' > /dev/null
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"metrics_seller","asset":"BTC","amount":1000}' > /dev/null
echo "✅ 充值完成"
echo ""

# 3. 下 5 单测延迟分位
echo "[3/6] 下 5 单（测延迟分位）..."
for i in 1 2 3 4 5; do
    PRICE=$((100 + i))
    curl -s -X POST "$API/orders" \
        -H "Content-Type: application/json" \
        -d "{\"user_id\":\"metrics_test\",\"symbol\":\"BTC/USDT\",\"side\":\"BUY\",\"order_type\":\"LIMIT\",\"price\":$PRICE,\"amount\":1}" > /dev/null
    echo "   order $i placed, price=$PRICE"

done
echo ""

# 查 metrics
echo "   查 /metrics..."
METRICS_OUT=$(curl -s "$METRICS")
echo "$METRICS_OUT" | head -20
echo ""

COUNT=$(echo "$METRICS_OUT" | grep -o '"count":[0-9]*' | head -1 | cut -d: -f2)
if [ "$COUNT" -ge 5 ]; then
    echo "✅ latency ok: count=${COUNT} (>=5)"
else
    echo "❌ 延迟分位：count=${COUNT} (应该 >=5)"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

# 检查 P95 字段存在
if echo "$METRICS_OUT" | grep -q '"p95_ms"'; then
    echo "✅ 延迟分位：P95 字段存在"
else
    echo "❌ 延迟分位：P95 字段缺失"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# 4. 测熔断触发
echo "[4/6] 测熔断（价格剧烈波动）..."

# 先挂一笔卖单，成交价 100
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"metrics_seller","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":100,"amount":1}' > /dev/null

# 买家买 1 个（成交价 100，熔断器记录基准价）
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"metrics_test","symbol":"BTC/USDT","side":"BUY","order_type":"LIMIT","price":100,"amount":1}' > /dev/null

# 再挂一笔卖单，价格 200（比 100 涨 100%，超 5% 阈值）
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"metrics_seller","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":200,"amount":1}' > /dev/null

# 再买 1 个（成交价 200，触发熔断）
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"metrics_test","symbol":"BTC/USDT","side":"BUY","order_type":"LIMIT","price":200,"amount":1}' > /dev/null

echo "   触发后查 /metrics 熔断状态..."
METRICS_OUT=$(curl -s "$METRICS")
echo "$METRICS_OUT" | grep -A 5 "circuit_breakers"
echo ""

if echo "$METRICS_OUT" | grep -q '"is_open":true'; then
    echo "✅ 熔断触发成功（is_open=true）"
else
    echo "❌ 熔断未触发（is_open 应该是 true）"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# 5. 熔断中下单被拒
echo "[5/6] 熔断中下单，应该被拒..."
RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"metrics_test","symbol":"BTC/USDT","side":"BUY","order_type":"LIMIT","price":100,"amount":1}')
echo "   返回: $RESULT"

if echo "$RESULT" | grep -q "熔断"; then
    echo "✅ 熔断中下单被拒"
else
    echo "❌ 熔断中下单未被拒"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# 6. 结果
echo "[6/6] 结果"
echo "=========================================="
echo "✅ 延迟分位正常（count / P95 有值）"
echo "✅ 熔断触发正常（is_open=true）"
echo "✅ 熔断中拒绝新单"
echo "=========================================="
echo ""
echo "Server log: /tmp/cex_metrics_test.log"
echo "Server PID: $SERVER_PID"
echo "To stop: kill $SERVER_PID"
echo ""