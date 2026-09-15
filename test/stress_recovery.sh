#!/bin/bash

# ============================================
# CEX 崩溃恢复测试
# 场景：撮合中途 kill -9 服务，重启后验证数据一致性
# ============================================

BASE_URL="http://localhost:8080/api/v1"
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}   CEX 崩溃恢复测试${NC}"
echo -e "${YELLOW}========================================${NC}"

# 1. 重置测试数据
echo -e "\n${YELLOW}📦 1. 重置测试数据...${NC}"
mysql -u root -e "USE cex; TRUNCATE TABLE orders; TRUNCATE TABLE trades; TRUNCATE TABLE balances; TRUNCATE TABLE ledgers;" 2>/dev/null
redis-cli FLUSHALL > /dev/null

# 2. 启动服务（相对路径）
cd "$(dirname "$0")/.."
echo -e "\n${YELLOW}🚀 2. 启动服务...${NC}"
go run cmd/api/main.go > /tmp/cex_service.log 2>&1 &
SERVICE_PID=$!
echo "服务 PID: $SERVICE_PID"
sleep 5

# 3. 充值
echo -e "\n${YELLOW}💰 3. 充值...${NC}"
curl -s -X POST $BASE_URL/balance/add \
  -H "Content-Type: application/json" \
  -d '{"user_id":"crash_test_B","asset":"BTC","amount":10}' > /dev/null

curl -s -X POST $BASE_URL/balance/add \
  -H "Content-Type: application/json" \
  -d '{"user_id":"crash_test_A","asset":"USDT","amount":1000000}' > /dev/null

# 4. 挂卖单（触发撮合）
echo -e "\n${YELLOW}📝 4. 挂卖单并部分成交...${NC}"
ORDER_RESP=$(curl -s -X POST $BASE_URL/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id":"crash_test_B","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":50000,"amount":10}')

ORDER_ID=$(echo $ORDER_RESP | jq -r '.order_id')
echo "订单 ID: $ORDER_ID"

sleep 1

# 部分成交（买 3 BTC）
curl -s -X POST $BASE_URL/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id":"crash_test_A","symbol":"BTC/USDT","side":"BUY","order_type":"MARKET","amount":3}' > /dev/null

sleep 1

# 5. 记录崩溃前的状态
echo -e "\n${YELLOW}📊 5. 崩溃前的状态...${NC}"
BAL_BEFORE=$(curl -s $BASE_URL/balance/crash_test_B/BTC | jq -r '.available')
FROZEN_BEFORE=$(curl -s $BASE_URL/balance/crash_test_B/BTC | jq -r '.frozen')
ORDER_BEFORE=$(curl -s $BASE_URL/orders/$ORDER_ID | jq -r '.Status')

echo "test_B BTC available: $BAL_BEFORE"
echo "test_B BTC frozen:    $FROZEN_BEFORE"
echo "订单状态:              $ORDER_BEFORE"

# 6. kill -9 服务
echo -e "\n${RED}💀 6. kill -9 杀掉服务...${NC}"
kill -9 $SERVICE_PID 2>/dev/null
sleep 2

# 7. 重启服务
echo -e "\n${YELLOW}🔄 7. 重启服务...${NC}"
go run cmd/api/main.go > /tmp/cex_service_restart.log 2>&1 &
NEW_PID=$!
echo "新服务 PID: $NEW_PID"
sleep 5

# ✅ 打印恢复日志
echo -e "\n${YELLOW}📋 服务恢复日志...${NC}"
grep -E "快照|WAL|对账|恢复" /tmp/cex_service_restart.log

# 8. 验证恢复后的状态
echo -e "\n${YELLOW}📊 8. 崩溃恢复后的状态...${NC}"
BAL_AFTER=$(curl -s $BASE_URL/balance/crash_test_B/BTC | jq -r '.available')
FROZEN_AFTER=$(curl -s $BASE_URL/balance/crash_test_B/BTC | jq -r '.frozen')
ORDER_AFTER=$(curl -s $BASE_URL/orders/$ORDER_ID | jq -r '.Status')

echo "test_B BTC available: $BAL_AFTER"
echo "test_B BTC frozen:    $FROZEN_AFTER"
echo "订单状态:              $ORDER_AFTER"

# 9. 对比结果
echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}   崩溃恢复对比${NC}"
echo -e "${YELLOW}========================================${NC}"

PASS=0
FAIL=0

if [ "$BAL_BEFORE" == "$BAL_AFTER" ]; then
    echo -e "${GREEN}✅ PASS: available 一致 ($BAL_AFTER)${NC}"
    ((PASS++))
else
    echo -e "${RED}❌ FAIL: available 不一致（崩溃前 $BAL_BEFORE, 恢复后 $BAL_AFTER）${NC}"
    ((FAIL++))
fi

if [ "$FROZEN_BEFORE" == "$FROZEN_AFTER" ]; then
    echo -e "${GREEN}✅ PASS: frozen 一致 ($FROZEN_AFTER)${NC}"
    ((PASS++))
else
    echo -e "${RED}❌ FAIL: frozen 不一致（崩溃前 $FROZEN_BEFORE, 恢复后 $FROZEN_AFTER）${NC}"
    ((FAIL++))
fi

if [ "$ORDER_BEFORE" == "$ORDER_AFTER" ]; then
    echo -e "${GREEN}✅ PASS: 订单状态一致 ($ORDER_AFTER)${NC}"
    ((PASS++))
else
    echo -e "${RED}❌ FAIL: 订单状态不一致（崩溃前 $ORDER_BEFORE, 恢复后 $ORDER_AFTER）${NC}"
    ((FAIL++))
fi

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}   测试完成：$PASS PASS / $FAIL FAIL${NC}"
echo -e "${GREEN}========================================${NC}"

kill -9 $NEW_PID 2>/dev/null