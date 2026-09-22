#!/bin/bash

# TPS + 延迟分位验证脚本（自动清环境 + 启动服务）
# 用法：./test/test_metrics_tps.sh

set -e

cd "$(dirname "$0")/.."

API="http://localhost:8080/api/v1"
METRICS="http://localhost:8080/metrics"

echo "=========================================="
echo " TPS + 延迟分位验证"
echo "=========================================="
echo ""

# 0. 清环境 + 停服务
echo "[0/6] 清环境..."
pkill -9 -f "go-build.*main" 2>/dev/null || true
pkill -9 -f "go run cmd/api/main.go" 2>/dev/null || true
pkill -9 -f "go run cmd/outbox-publisher" 2>/dev/null || true
sleep 1

if pgrep -f "go-build.*main" > /dev/null; then
    echo "❌ 杀不干净"
    exit 1
fi

docker exec cex-mysql mysql -uroot -proot cex \
    -e "TRUNCATE TABLE orders; TRUNCATE TABLE trades; TRUNCATE TABLE outbox;" 2>/dev/null
redis-cli -p 6379 FLUSHDB > /dev/null
echo "✅ 环境已清"
echo ""

# 1. 启动服务
echo "[1/6] 启动 API 服务..."
go run cmd/api/main.go > /tmp/cex_metrics_tps.log 2>&1 &
SERVER_PID=$!

for i in $(seq 1 15); do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ API 已就绪（等了 ${i}s）"
        break
    fi
    sleep 1
    if [ $i -eq 15 ]; then
        echo "❌ API 启动超时"
        cat /tmp/cex_metrics_tps.log
        exit 1
    fi
done
echo ""

# 2. 充值
echo "[2/6] 充值..."
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"tps_test","asset":"USDT","amount":10000000}' > /dev/null
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"tps_seller","asset":"BTC","amount":10000}' > /dev/null
echo "✅ 充值完成"
echo ""

# 3. 下 20 单
echo "[3/6] 快速下 20 单..."
START_TS=$(date +%s)

for i in $(seq 1 20); do
    PRICE=$((200 + i))
    curl -s -X POST "$API/orders" \
        -H "Content-Type: application/json" \
        -d "{\"user_id\":\"tps_test\",\"symbol\":\"BTC/USDT\",\"side\":\"BUY\",\"order_type\":\"LIMIT\",\"price\":$PRICE,\"amount\":1}" > /dev/null
done

END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))
echo "   20 单完成，耗时 ${ELAPSED}s"
echo ""

# 4. 查 metrics
echo "[4/6] 查 /metrics..."
METRICS_OUT=$(curl -s "$METRICS")
echo "$METRICS_OUT" | python3 -m json.tool 2>/dev/null || echo "$METRICS_OUT"
echo ""

# 5. 验证 TPS 字段
echo "[5/6] 验证 TPS 字段..."

if echo "$METRICS_OUT" | grep -q '"tps"'; then
    TPS=$(echo "$METRICS_OUT" | grep -o '"tps":[0-9.]*' | head -1 | cut -d: -f2)
    TPS_WINDOW=$(echo "$METRICS_OUT" | grep -o '"tps_window_s":[0-9]*' | head -1 | cut -d: -f2)
    TPS_RECENT=$(echo "$METRICS_OUT" | grep -o '"tps_recent":[0-9]*' | head -1 | cut -d: -f2)
    echo "   tps: $TPS"
    echo "   tps_window_s: $TPS_WINDOW"
    echo "   tps_recent: $TPS_RECENT"
    echo "✅ TPS 字段存在"
else
    echo "❌ TPS 字段缺失"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# 6. 验证 P95 字段
echo "[6/6] 验证延迟分位字段..."

if echo "$METRICS_OUT" | grep -q '"p95_ms"'; then
    P50=$(echo "$METRICS_OUT" | grep -o '"p50_ms":[0-9.]*' | head -1 | cut -d: -f2)
    P95=$(echo "$METRICS_OUT" | grep -o '"p95_ms":[0-9.]*' | head -1 | cut -d: -f2)
    P99=$(echo "$METRICS_OUT" | grep -o '"p99_ms":[0-9.]*' | head -1 | cut -d: -f2)
    echo "   p50_ms: $P50"
    echo "   p95_ms: $P95"
    echo "   p99_ms: $P99"
    echo "✅ 延迟分位字段存在"
else
    echo "❌ 延迟分位字段缺失"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

echo "=========================================="
echo "✅ TPS + 延迟分位验证通过"
echo "=========================================="
echo ""
echo "Server log: /tmp/cex_metrics_tps.log"
echo "Server PID: $SERVER_PID"
echo "To stop: kill $SERVER_PID"
echo ""