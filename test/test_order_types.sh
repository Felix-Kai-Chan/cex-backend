#!/bin/bash

# FOK / IOC / LIMIT 订单类型验证脚本
# ⚠️ 前置条件：订单簿必须干净
# 使用前先执行：
#   ./test/reset_all.sh    （清库 + 停服务）
#   go run cmd/api/main.go （重启服务）
# 用法：./test/test_order_types.sh

API="http://localhost:8080/api/v1"

echo "=========================================="
echo " 订单类型验证脚本（LIMIT / IOC / FOK / MARKET）"
echo "=========================================="
echo ""

# 0. 检查 API
echo "[0/8] 检查 API 服务..."
if ! curl -s "http://localhost:8080/health" > /dev/null 2>&1; then
    echo "❌ API 没启动，请先 go run cmd/api/main.go"
    exit 1
fi
echo "✅ API 正常"
echo ""

# 0.5 检查订单簿是否为空
echo "[0.5/8] 检查订单簿是否干净..."
DEPTH=$(curl -s "$API/depth?symbol=BTC/USDT&limit=10")
PRICE_COUNT=$(echo "$DEPTH" | grep -o '"price"' | wc -l | tr -d ' ')

if [ "$PRICE_COUNT" -gt 0 ]; then
    echo "⚠️  订单簿不干净，有历史残留 $PRICE_COUNT 档"
    echo "   请先执行 ./test/reset_all.sh 然后重启服务"
    exit 1
fi
echo "✅ 订单簿干净"
echo ""

# 1. 充值测试用户
echo "[1/8] 充值测试用户..."
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_buyer","asset":"USDT","amount":1000000}' > /dev/null
curl -s -X POST "$API/balance/add" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_seller","asset":"BTC","amount":1000}' > /dev/null
echo "✅ 充值完成（buyer: USDT 1000000，seller: BTC 1000）"
echo ""

# 2. IOC 部分成交：挂 SELL 3，IOC 买 10，应成交 3 剩余 7 取消
echo "[2/8] IOC 部分成交：挂 SELL 3 @ 100，IOC 买 10 @ 100"
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_seller","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":100,"amount":3}' > /dev/null

RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_buyer","symbol":"BTC/USDT","side":"BUY","order_type":"IOC","price":100,"amount":10}')
echo "   返回: $RESULT"

if echo "$RESULT" | grep -q '"status":"CANCELLED"'; then
    TRADES=$(echo "$RESULT" | grep -o '"quantity"' | wc -l | tr -d ' ')
    if [ "$TRADES" -gt 0 ]; then
        echo "✅ IOC 通过：部分成交（$TRADES 笔）后取消"
    else
        echo "❌ IOC 失败：状态 CANCELLED 但成交 0 笔"
        exit 1
    fi
else
    echo "❌ IOC 失败：状态应该 CANCELLED"
    exit 1
fi
echo ""

# 3. FOK 失败：挂 SELL 3，FOK 买 10，对手不足，整单取消
echo "[3/8] FOK 失败场景：挂 SELL 3 @ 100，FOK 买 10 @ 100"
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_seller","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":100,"amount":3}' > /dev/null

RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_buyer","symbol":"BTC/USDT","side":"BUY","order_type":"FOK","price":100,"amount":10}')
echo "   返回: $RESULT"

if echo "$RESULT" | grep -q '"status":"CANCELLED"'; then
    TRADES=$(echo "$RESULT" | grep -o '"quantity"' | wc -l | tr -d ' ')
    if [ "$TRADES" -eq 0 ]; then
        echo "✅ FOK 失败场景通过：整单取消（0 笔成交）"
    else
        echo "❌ FOK 失败场景失败：状态 CANCELLED 但有成交"
        exit 1
    fi
else
    echo "❌ FOK 失败场景失败：状态应该 CANCELLED"
    exit 1
fi
echo ""

# 4. FOK 成功：挂 SELL 3，FOK 买 3，全部成交
echo "[4/8] FOK 成功场景：挂 SELL 3 @ 100，FOK 买 3 @ 100"
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_seller","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":100,"amount":3}' > /dev/null

RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_buyer","symbol":"BTC/USDT","side":"BUY","order_type":"FOK","price":100,"amount":3}')
echo "   返回: $RESULT"

if echo "$RESULT" | grep -q '"status":"FILLED"'; then
    echo "✅ FOK 成功场景通过：全部成交"
else
    echo "❌ FOK 成功场景失败：状态应该 FILLED"
    exit 1
fi
echo ""

# 5. LIMIT 部分成交 + 挂单：挂 SELL 5，LIMIT 买 10，成交 5 + 挂单 5
echo "[5/8] LIMIT 部分成交 + 挂单：挂 SELL 5 @ 50，LIMIT 买 10 @ 50"
curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_seller","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":50,"amount":5}' > /dev/null

RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_buyer","symbol":"BTC/USDT","side":"BUY","order_type":"LIMIT","price":50,"amount":10}')
echo "   返回: $RESULT"

if echo "$RESULT" | grep -q '"status":"PARTIAL"'; then
    echo "✅ LIMIT 通过：部分成交后挂单（PARTIAL）"
else
    echo "❌ LIMIT 失败：状态应该 PARTIAL"
    exit 1
fi
echo ""

# 6. LIMIT 纯挂单：买 5 @ 40，无对手盘，挂单 PENDING
echo "[6/8] LIMIT 纯挂单：买 5 @ 40"
RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_buyer","symbol":"BTC/USDT","side":"BUY","order_type":"LIMIT","price":40,"amount":5}')
echo "   返回: $RESULT"

if echo "$RESULT" | grep -q '"status":"PENDING"'; then
    echo "✅ LIMIT 纯挂单通过：状态 PENDING"
else
    echo "❌ LIMIT 纯挂单失败：状态应该 PENDING"
    exit 1
fi
echo ""

# 7. 非法 order_type
echo "[7/8] 非法 order_type：order_type=FOO"
RESULT=$(curl -s -X POST "$API/orders" \
    -H "Content-Type: application/json" \
    -d '{"user_id":"type_test_buyer","symbol":"BTC/USDT","side":"BUY","order_type":"FOO","price":40,"amount":5}')
echo "   返回: $RESULT"

if echo "$RESULT" | grep -q 'error'; then
    echo "✅ 非法 order_type 通过：正确报错"
else
    echo "❌ 非法 order_type 失败：应该报错"
    exit 1
fi
echo ""

# 8. 查余额
echo "[8/8] 查余额"
echo ""
echo "buyer USDT:"
curl -s "$API/balance/type_test_buyer/USDT"
echo ""
echo "buyer BTC:"
curl -s "$API/balance/type_test_buyer/BTC"
echo ""
echo "seller USDT:"
curl -s "$API/balance/type_test_seller/USDT"
echo ""
echo "seller BTC:"
curl -s "$API/balance/type_test_seller/BTC"
echo ""
echo ""

echo "=========================================="
echo "✅ 全部测试通过"
echo "=========================================="