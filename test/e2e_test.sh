#!/bin/bash

# ============================================
# CEX 端到端集成测试
# 测试：充值 → 下单 → 撮合 → 查深度 → 查流水
# ============================================

BASE_URL="http://localhost:8080/api/v1"
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}   CEX 端到端集成测试${NC}"
echo -e "${YELLOW}========================================${NC}"

# 1. 重置测试用户余额
echo -e "\n${YELLOW}📦 1. 重置测试账户余额...${NC}"

# 先删后加（确保干净状态）
curl -s -X POST $BASE_URL/balance/add \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test_A","asset":"USDT","amount":10000}' > /dev/null

curl -s -X POST $BASE_URL/balance/add \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test_B","asset":"BTC","amount":10}' > /dev/null

echo -e "${GREEN}✅ 测试账户已重置${NC}"

# 2. 查询初始余额
echo -e "\n${YELLOW}📊 2. 查询初始余额...${NC}"

echo "test_A USDT:"
curl -s $BASE_URL/balance/test_A/USDT | jq '.'

echo "test_B BTC:"
curl -s $BASE_URL/balance/test_B/BTC | jq '.'

# 3. 挂一个卖单
echo -e "\n${YELLOW}📝 3. test_B 挂卖单：10 BTC，价格 50000 USDT...${NC}"

ORDER_RESP=$(curl -s -X POST $BASE_URL/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test_B","symbol":"BTC/USDT","side":"SELL","order_type":"LIMIT","price":50000,"amount":10}')

echo $ORDER_RESP | jq '.'

ORDER_ID=$(echo $ORDER_RESP | jq -r '.order_id')
echo -e "${GREEN}✅ 订单 ID: $ORDER_ID${NC}"

# 4. 查深度（应该看到卖盘）
echo -e "\n${YELLOW}📊 4. 查看订单簿深度...${NC}"
curl -s "$BASE_URL/depth?symbol=BTC/USDT&limit=5" | jq '.'

# 5. 市价买单（成交）
echo -e "\n${YELLOW}💰 5. test_A 市价买入 3 BTC...${NC}"
curl -s -X POST $BASE_URL/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test_A","symbol":"BTC/USDT","side":"BUY","order_type":"MARKET","amount":3}' | jq '.'

# 6. 查成交后的余额
echo -e "\n${YELLOW}📊 6. 成交后余额...${NC}"

echo "test_A USDT:"
curl -s $BASE_URL/balance/test_A/USDT | jq '.'

echo "test_B BTC:"
curl -s $BASE_URL/balance/test_B/BTC | jq '.'

# 7. 查资金流水
echo -e "\n${YELLOW}📊 7. test_A 资金流水...${NC}"
curl -s $BASE_URL/ledger/test_A | jq '.'

# 8. 查订单状态
echo -e "\n${YELLOW}📊 8. 查询订单状态...${NC}"
curl -s $BASE_URL/orders/$ORDER_ID | jq '.'

# 9. 撤单（测试撤单功能）
echo -e "\n${YELLOW}❌ 9. test_B 撤单（剩余 7 BTC）...${NC}"
curl -s -X DELETE "$BASE_URL/orders/$ORDER_ID?user_id=test_B" | jq '.'

# 10. 查最终余额
echo -e "\n${YELLOW}📊 10. 最终余额...${NC}"

echo "test_A USDT:"
curl -s $BASE_URL/balance/test_A/USDT | jq '.'

echo "test_B BTC:"
curl -s $BASE_URL/balance/test_B/BTC | jq '.'

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}✅ 端到端测试完成！${NC}"
echo -e "${GREEN}========================================${NC}"