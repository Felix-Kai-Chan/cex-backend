#!/bin/bash

# WAL 崩溃恢复验证脚本
# 场景：先让快照生成 → 下单 → kill 服务 → 重启 → 验证 snapshot + WAL replay
# 用法：./test/test_wal_recovery.sh

set -e

API="http://localhost:8080/api/v1"
REDIS="redis-cli -p 6379"
WAL_KEY="orderbook:wal:BTC/USDT"
SNAP_KEY="orderbook:snapshot:BTC/USDT"

echo "=========================================="
echo " WAL 崩溃恢复验证脚本"
echo "=========================================="
echo ""

# 1. 检查 API
echo "[1/8] 检查 API 服务..."
if ! curl -s "http://localhost:8080/health" > /dev/null 2>&1; then
    echo "❌ API 没启动，请先 go run cmd/api/main.go"
    exit 1
fi
echo "✅ API 正常"
echo ""

# 2. 清空旧 WAL（保留快照，这样才能测到 snapshot + WAL 恢复路径）
echo "[2/8] 清空旧 WAL（保留快照）..."
$REDIS DEL "$WAL_KEY" > /dev/null
echo "✅ 已清空 WAL"
echo ""

# 3. 等快照生成（最多等 12 秒）
echo "[3/8] 等待快照生成..."
for i in $(seq 1 12); do
    if $REDIS EXISTS "$SNAP_KEY" | grep -q "1"; then
        echo "✅ 快照已存在（等了 ${i}s）"
        break
    fi
    sleep 1
    if [ $i -eq 12 ]; then
        echo "❌ 等了 12 秒快照还没生成，检查 Save() 定时器"
        exit 1
    fi
done
echo ""

# 4. 充值
echo "[4/8] 给测试用户充值..."
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"recovery_test2","asset":"USDT","amount":1000000}' > /dev/null
echo "✅ 充值完成"
echo ""

# 5. 下单（记订单 ID）
echo "[5/8] 下单..."
ORDER=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"recovery_test2","symbol":"BTC/USDT","side":"BUY","order_type":"LIMIT","price":77777,"amount":2}')

ORDER_ID=$(echo "$ORDER" | grep -o '"order_id":"[^"]*"' | cut -d'"' -f4)
echo "   订单ID: $ORDER_ID"
echo ""

if [ -z "$ORDER_ID" ]; then
    echo "❌ 下单失败: $ORDER"
    exit 1
fi

# 6. 立刻查 WAL（快照还没跑）
echo "[6/8] 立刻查 WAL..."
IMMEDIATE=$($REDIS LRANGE "$WAL_KEY" 0 -1)
echo "$IMMEDIATE" | sed 's/^/     /'
echo ""

if ! echo "$IMMEDIATE" | grep -q "$ORDER_ID"; then
    echo "❌ WAL 没写入，不能继续"
    exit 1
fi
echo "✅ WAL 有内容"
echo ""

# 7. kill 服务（模拟崩溃，不等快照）
echo "[7/8] 模拟崩溃：kill 掉服务进程..."
pkill -f "go run cmd/api/main.go" || true
pkill -f "go-build.*main" || true
sleep 1

if pgrep -f "go-build.*main" > /dev/null; then
    echo "⚠️  还有残留进程，强杀"
    pkill -9 -f "go-build.*main" || true
    sleep 1
fi

if pgrep -f "go-build.*main" > /dev/null; then
    echo "❌ 杀不干净，请手动 kill"
    ps aux | grep "go-build.*main" | grep -v grep
    exit 1
fi
echo "✅ 进程已停"
echo ""

# 8. 验证 WAL 还在
echo "[8/8] kill 后查 WAL（应该还在）..."
AFTER_KILL=$($REDIS LRANGE "$WAL_KEY" 0 -1)
echo "$AFTER_KILL" | sed 's/^/     /'
echo ""

if ! echo "$AFTER_KILL" | grep -q "$ORDER_ID"; then
    echo "❌ kill 后 WAL 消失"
    exit 1
fi
echo "✅ WAL 保留成功"
echo ""

echo "=========================================="
echo " 现在手动做最后一步：重启 + 看日志"
echo "=========================================="
echo ""
echo "1. 新开终端，重启服务："
echo "   go run cmd/api/main.go"
echo ""
echo "2. 看日志里有没有这两行（关键）："
echo "   snapshot restored"
echo "   WAL replay completed"
echo ""
echo "3. 查订单簿还在不在："
echo "   curl 'http://localhost:8080/api/v1/depth?symbol=BTC/USDT&limit=10'"
echo "   期望看到 price: 77777"
echo ""
echo "4. 验证完成"
echo ""