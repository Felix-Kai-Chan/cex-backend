#!/bin/bash

# 撤单验证脚本（O(1) 索引 + 重启恢复）
# 自动清环境 + 启动服务 + 验证
# 用法：./test/test_cancel.sh

set -e

cd "$(dirname "$0")/.."

API="http://localhost:8080/api/v1"

echo "=========================================="
echo " 撤单验证脚本"
echo "=========================================="
echo ""

# 0. 清环境
echo "[0/5] 清环境..."
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
echo "[1/5] 启动服务..."
go run cmd/api/main.go > /tmp/cex_cancel_test.log 2>&1 &
SERVER_PID=$!

for i in $(seq 1 15); do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ 服务已就绪（等了 ${i}s）"
        break
    fi
    sleep 1
    if [ $i -eq 15 ]; then
        echo "❌ 服务启动超时"
        cat /tmp/cex_cancel_test.log
        exit 1
    fi
done

echo "启动日志："
grep -E "rebuilt|reconcile|snapshot restored|WAL" /tmp/cex_cancel_test.log | head -5
echo ""

# 2. 充值 + 下单
echo "[2/5] 充值 + 下一单 BUY 5 @ 999..."
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"cancel_test","asset":"USDT","amount":100000}' > /dev/null

ORDER=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"cancel_test","symbol":"BTC/USDT","side":"BUY","order_type":"LIMIT","price":999,"amount":5}')

ORDER_ID=$(echo "$ORDER" | grep -o '"order_id":"[^"]*"' | cut -d'"' -f4)
echo "   order_id: $ORDER_ID"

if [ -z "$ORDER_ID" ]; then
    echo "❌ 下单失败: $ORDER"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

DEPTH=$(curl -s "$API/depth?symbol=BTC/USDT&limit=20")
if echo "$DEPTH" | grep -q '"price":999'; then
    echo "✅ 订单已在订单簿"
else
    echo "❌ 订单没进订单簿"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# 3. 撤单（新进程）
echo "[3/5] 撤单（新进程）..."
RESULT=$(curl -s -X DELETE "$API/orders/$ORDER_ID?user_id=cancel_test")
echo "   返回: $RESULT"

if echo "$RESULT" | grep -q '"error"'; then
    echo "❌ 撤单失败"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

DEPTH=$(curl -s "$API/depth?symbol=BTC/USDT&limit=20")
if echo "$DEPTH" | grep -q '"price":999'; then
    echo "❌ 999 档还在"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo "✅ 撤单成功，999 档已移除"
echo ""

# 4. 重启后撤单
echo "[4/5] 重启后撤单..."

# 下一单
ORDER=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"cancel_test","symbol":"BTC/USDT","side":"BUY","order_type":"LIMIT","price":888,"amount":5}')
ORDER_ID=$(echo "$ORDER" | grep -o '"order_id":"[^"]*"' | cut -d'"' -f4)
echo "   order_id: $ORDER_ID"

if [ -z "$ORDER_ID" ]; then
    echo "❌ 下单失败"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

# 停服务
echo "   停服务..."
kill -9 $SERVER_PID 2>/dev/null || true
pkill -9 -f "go-build.*main" 2>/dev/null || true
sleep 2

if pgrep -f "go-build.*main" > /dev/null; then
    echo "❌ 杀不干净"
    exit 1
fi

# 重启
echo "   重启..."
go run cmd/api/main.go > /tmp/cex_cancel_test.log 2>&1 &
SERVER_PID=$!

for i in $(seq 1 15); do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "   ✅ 服务已就绪（等了 ${i}s）"
        break
    fi
    sleep 1
    if [ $i -eq 15 ]; then
        echo "❌ 重启超时"
        cat /tmp/cex_cancel_test.log
        exit 1
    fi
done

echo "   重启日志："
grep -E "rebuilt|reconcile|snapshot restored|WAL" /tmp/cex_cancel_test.log | head -5

# 确认 888 档恢复
DEPTH=$(curl -s "$API/depth?symbol=BTC/USDT&limit=20")
if echo "$DEPTH" | grep -q '"price":888'; then
    echo "   ✅ 888 档已恢复"
else
    echo "   ❌ 888 档没恢复"
    echo "   depth: $DEPTH"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

# 撤单
echo "   撤单..."
RESULT=$(curl -s -X DELETE "$API/orders/$ORDER_ID?user_id=cancel_test")
echo "   返回: $RESULT"

if echo "$RESULT" | grep -q '"error"'; then
    echo "❌ 重启后撤单失败（orders 索引没重建）"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo "✅ 重启后撤单成功"
echo ""

# 5. 结果
echo "[5/5] 结果"
echo "=========================================="
echo "✅ 撤单 O(1) 索引正常"
echo "✅ 重启恢复后撤单正常"
echo "=========================================="
echo ""
echo "Server log: /tmp/cex_cancel_test.log"
echo "Server PID: $SERVER_PID"
echo "To stop: kill $SERVER_PID"
echo ""