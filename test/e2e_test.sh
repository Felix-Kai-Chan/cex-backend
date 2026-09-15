#!/bin/bash

# ============================================
# CEX 端到端集成测试（最终版）
# 覆盖：充值 → 冻结 → 下单 → 撮合 → 深度 → 流水 → 撤单解冻
# ============================================

BASE_URL="http://localhost:8080/api/v1"
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

assert_eq() {
    local actual=$1
    local expected=$2
    local msg=$3
    if [ "$actual" == "$expected" ]; then
        echo -e "${GREEN}✅ PASS: $msg${NC}"
        ((PASS++))
    else
        echo -e "${RED}❌ FAIL: $msg (期望 $expected, 实际 $actual)${NC}"
        ((FAIL++))
    fi
}

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}   CEX 端到端集成测试（最终版）${NC}"
echo -e "${YELLOW}========================================${NC}"

# ============================================
# 1. 重置测试账户余额
# ============================================
echo -e "\n${YELLOW}📦 1. 重置测试账户余额...${NC}"

# test_A：200000 USDT（足够买 3 BTC）
curl -s -X POST $BASE_URL/balance/add \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test_A","asset":"USDT","amount":200000}' > /dev/null

# test_B：10 BTC
curl -s -X POST $BASE_URL/balance/add \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test_B","asset":"BTC","amount":10}' > /dev/null

echo -e "${GREEN}✅ 测试账户已重置${NC}"

# ============================================
# 2. 查询初始余额
# ============================================
echo -e "\n${YELLOW}📊 2. 查询初始余额...${NC}"

BAL_A=$(curl -s $BASE_URL/balance/test_A/USDT | jq -r '.available')
BAL_B=$(curl -s $BASE_URL/balance/test_B/BTC | jq -r '.available')

echo "test_A USDT available: $BAL_A"
echo "test_B BTC available: $BAL_B"

# ============================================
# 3. test_B 挂卖单（验证冻结）
# ============================================
echo -e "\n${YELLOW}📝 3. test_B 挂卖单：10 BTC @ 50000...${NC}"

ORDER_RESP=$(curl -s -X POST $BASE_URL/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test_B","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":50000,"amount":10}')

echo $ORDER_RESP | jq '.'

ORDER_ID=$(echo $ORDER_RESP | jq -r '.order_id')
echo -e "${GREEN}✅ 订单 ID: $ORDER_ID${NC}"

# ✅ 验证冻结
echo -e "\n${YELLOW}🔒 验证 test_B 余额冻结...${NC}"
BAL_B_AFTER=$(curl -s $BASE_URL/balance/test_B/BTC)
B_AVAIL=$(echo $BAL_B_AFTER | jq -r '.available')
B_FROZEN=$(echo $BAL_B_AFTER | jq -r '.frozen')

echo "test_B BTC available: $B_AVAIL, frozen: $B_FROZEN"
assert_eq "$B_AVAIL" "0" "test_B BTC available 应为 0（全部冻结）"
assert_eq "$B_FROZEN" "10" "test_B BTC frozen 应为 10"

# ============================================
# 4. 查看深度（验证跳表排序）
# ============================================
echo -e "\n${YELLOW}📊 4. 查看订单簿深度（验证跳表排序）...${NC}"

DEPTH=$(curl -s "$BASE_URL/depth?symbol=BTC/USDT&limit=5")
echo $DEPTH | jq '.'

ASK_PRICE=$(echo $DEPTH | jq -r '.asks[0].price')
assert_eq "$ASK_PRICE" "50000" "最优卖价应为 50000（跳表排序）"

# ============================================
# 5. test_A 市价买入 3 BTC（验证成交扣减）
# ============================================
echo -e "\n${YELLOW}💰 5. test_A 市价买入 3 BTC...${NC}"

TRADE_RESP=$(curl -s -X POST $BASE_URL/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test_A","symbol":"BTC/USDT","side":"BUY","order_type":"MARKET","amount":3}')

echo $TRADE_RESP | jq '.'

# ✅ 验证 test_A 余额（买入后）
echo -e "\n${YELLOW}🔍 验证 test_A 余额（买入后）...${NC}"
BAL_A_AFTER=$(curl -s $BASE_URL/balance/test_A/USDT)
A_AVAIL=$(echo $BAL_A_AFTER | jq -r '.available')
A_FROZEN=$(echo $BAL_A_AFTER | jq -r '.frozen')

echo "test_A USDT available: $A_AVAIL, frozen: $A_FROZEN"

# ✅ 验证 test_B 余额（卖出后应收到 USDT）
echo -e "\n${YELLOW}🔍 验证 test_B 余额（卖出后）...${NC}"
BAL_B_USDT=$(curl -s $BASE_URL/balance/test_B/USDT)
B_USDT_AVAIL=$(echo $BAL_B_USDT | jq -r '.available')

echo "test_B USDT available: $B_USDT_AVAIL"

# test_B 卖出 3 BTC × 50000 = 150000 USDT
assert_eq "$B_USDT_AVAIL" "150000" "test_B 卖出后应收到 150000 USDT"

# ✅ 验证 test_B BTC 剩余（10 - 3 = 7 BTC）
echo -e "\n${YELLOW}🔍 验证 test_B BTC 剩余...${NC}"
BAL_B_BTC=$(curl -s $BASE_URL/balance/test_B/BTC)
B_BTC_FROZEN=$(echo $BAL_B_BTC | jq -r '.frozen')

echo "test_B BTC frozen: $B_BTC_FROZEN"
assert_eq "$B_BTC_FROZEN" "7" "test_B 卖出 3 BTC 后，剩余 7 BTC 应保持冻结"

# ============================================
# 6. 查资金流水
# ============================================
echo -e "\n${YELLOW}📊 6. test_A 资金流水...${NC}"
curl -s $BASE_URL/ledger/test_A | jq '.'

# ============================================
# 7. 查订单状态
# ============================================
echo -e "\n${YELLOW}📊 7. 查询订单状态...${NC}"
curl -s $BASE_URL/orders/$ORDER_ID | jq '.'

# ============================================
# 8. 撤单（验证解冻）
# ============================================
echo -e "\n${YELLOW}❌ 8. test_B 撤单（剩余 7 BTC）...${NC}"
curl -s -X DELETE "$BASE_URL/orders/$ORDER_ID?user_id=test_B" | jq '.'

# ✅ 验证解冻：test_B BTC frozen 应为 0，available 应为 7
echo -e "\n${YELLOW}🔓 验证 test_B 解冻...${NC}"
BAL_B_AFTER_CANCEL=$(curl -s $BASE_URL/balance/test_B/BTC)
B_AVAIL_AFTER=$(echo $BAL_B_AFTER_CANCEL | jq -r '.available')
B_FROZEN_AFTER=$(echo $BAL_B_AFTER_CANCEL | jq -r '.frozen')

echo "test_B BTC available: $B_AVAIL_AFTER, frozen: $B_FROZEN_AFTER"
assert_eq "$B_AVAIL_AFTER" "7" "撤单后 available 应为 7"
assert_eq "$B_FROZEN_AFTER" "0" "撤单后 frozen 应为 0"

# ============================================
# 9. 最终余额
# ============================================
echo -e "\n${YELLOW}📊 9. 最终余额...${NC}"

echo "test_A USDT:"
curl -s $BASE_URL/balance/test_A/USDT | jq '.'

echo "test_B BTC:"
curl -s $BASE_URL/balance/test_B/BTC | jq '.'

echo "test_B USDT:"
curl -s $BASE_URL/balance/test_B/USDT | jq '.'

# ============================================
# 测试结果汇总
# ============================================
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}   测试完成：$PASS PASS / $FAIL FAIL${NC}"
echo -e "${GREEN}========================================${NC}"

if [ $FAIL -gt 0 ]; then
    exit 1
fi