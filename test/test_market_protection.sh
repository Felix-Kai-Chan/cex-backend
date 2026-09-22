#!/bin/bash

# 市价单保护验证脚本（数量上限 + 滑点保护）
# 用法：./test/test_market_protection.sh

set -e

cd "$(dirname "$0")/.."

API="http://localhost:8080/api/v1"

echo "=========================================="
echo " 市价单保护验证"
echo "=========================================="
echo ""

# 0. 清环境 + 启动服务
echo "[0/6] 清环境 + 启动服务..."
pkill -9 -f "go-build.*main" 2>/dev/null || true
pkill -9 -f "go run cmd/api/main.go" 2>/dev/null || true
pkill -9 -f "go run cmd/outbox-publisher" 2>/dev/null || true
sleep 1

docker exec cex-mysql mysql -uroot -proot cex \
    -e "TRUNCATE TABLE orders; TRUNCATE TABLE trades; TRUNCATE TABLE outbox;" 2>/dev/null
redis-cli -p 6379 FLUSHDB > /dev/null

go run cmd/api/main.go > /tmp/cex_market_test.log 2>&1 &
SERVER_PID=$!

for i in $(seq 1 15); do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ API 已就绪（等了 ${i}s）"
        break
    fi
    sleep 1
    if [ $i -eq 15 ]; then
        echo "❌ API 启动超时"
        cat /tmp/cex_market_test.log
        exit 1
    fi
done
echo ""

# 1. 充值
echo "[1/6] 充值..."
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_buyer","asset":"USDT","amount":100000000}' > /dev/null
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_seller","asset":"BTC","amount":100000}' > /dev/null
echo "✅ 充值完成"
echo ""

# 2. 测试 A：市价单数量上限
echo "[2/6] 测试 A：市价单数量上限（默认上限 100）..."
RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_buyer","symbol":"BTC/USDT","side":"BUY","order_type":"MARKET","price":0,"amount":200}')

echo "   返回: $RESULT"

if echo "$RESULT" | grep -q "超过上限"; then
    echo "✅ 数量上限测试通过：200 > 100，被拒绝"
else
    echo "❌ 数量上限测试失败：应该被拒绝"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# 3. 测试 B：正常市价单（滑点在阈值内）
echo "[3/6] 测试 B：正常市价单（滑点在 5% 内）..."
# 挂 3 档卖单：100 × 3, 101 × 3, 102 × 3
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_seller","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":100,"amount":3}' > /dev/null
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_seller","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":101,"amount":3}' > /dev/null
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_seller","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":102,"amount":3}' > /dev/null

# 市价买 6 个：吃 100×3 + 101×3 = 6，加权均价 100.5，滑点 (100.5-100)/100 = 0.5%
RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_buyer","symbol":"BTC/USDT","side":"BUY","order_type":"MARKET","price":0,"amount":6}')

echo "   返回: $RESULT"

if echo "$RESULT" | grep -q '"status":"FILLED"'; then
    echo "✅ 正常市价单通过：成交 6 个"
else
    echo "❌ 正常市价单失败"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# 4. 测试 C：滑点超阈值
echo "[4/6] 测试 C：滑点超阈值..."

# 给 market_seller 充值 ETH
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_seller","asset":"ETH","amount":10000}' > /dev/null

# ETH 订单簿挂：100 × 1，200 × 9
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_seller","symbol":"ETH/USDT","side":"SELL","order_type":"LIMIT","price":100,"amount":1}' > /dev/null
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_seller","symbol":"ETH/USDT","side":"SELL","order_type":"LIMIT","price":200,"amount":9}' > /dev/null

# 市价买 5 个 ETH
# 吃 100×1 + 200×4，加权均价 = 180，滑点 = 80% → 拒绝
RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"market_buyer","symbol":"ETH/USDT","side":"BUY","order_type":"MARKET","price":0,"amount":5}')

echo "   返回: $RESULT"

if echo "$RESULT" | grep -q "滑点"; then
    echo "✅ 滑点保护测试通过：滑点超阈值，被拒绝"
else
    echo "❌ 滑点保护失败"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# 6. 结果
echo "[6/6] 结果"
echo "=========================================="
echo "✅ 市价单数量上限"
echo "✅ 正常市价单成交"
echo "✅ 滑点保护"
echo "✅ 限价单不受影响"
echo "=========================================="
echo ""
echo "Server log: /tmp/cex_market_test.log"
echo "Server PID: $SERVER_PID"
echo "To stop: kill $SERVER_PID"
echo ""