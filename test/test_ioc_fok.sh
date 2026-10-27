#!/bin/bash
# 测试 IOC / FOK 的成交与资金解冻行为
# 前提：CEX API 已启动在 http://localhost:8080

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

avail() { # user asset
    curl -s "$BASE_URL/balance/$1/$2" | jq -r '.available'
}
frozen() {
    curl -s "$BASE_URL/balance/$1/$2" | jq -r '.frozen'
}

add_balance() {
    curl -s -X POST "$BASE_URL/balance/add" \
        -H "Content-Type: application/json" \
        -d "{\"user_id\":\"$1\",\"asset\":\"$2\",\"amount\":$3}" >/dev/null
}

place_order() {
    # $1 user $2 side $3 type $4 price $5 amount [$6 tif]
    local tif=${6:-}
    local body
    if [ -n "$tif" ]; then
        body="{\"user_id\":\"$1\",\"symbol\":\"BTC/USDT\",\"side\":\"$2\",\"order_type\":\"$3\",\"time_in_force\":\"$tif\",\"price\":$4,\"amount\":$5}"
    else
        body="{\"user_id\":\"$1\",\"symbol\":\"BTC/USDT\",\"side\":\"$2\",\"order_type\":\"$3\",\"price\":$4,\"amount\":$5}"
    fi
    curl -s -X POST "$BASE_URL/orders" -H "Content-Type: application/json" -d "$body"
}

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}   IOC / FOK 资金解冻测试${NC}"
echo -e "${YELLOW}========================================${NC}"

# ---------- 1. IOC 部分成交：解冻未成交部分 ----------
echo -e "\n${YELLOW}--- Test 1: IOC 部分成交 ---${NC}"
add_balance seller BTC 10
add_balance buyer USDT 1000

# 卖方挂 3 BTC @100
place_order seller SELL LIMIT 100 3 >/dev/null

# IOC 买单 5 BTC @100：应成交 3，取消 2，解冻 200 USDT
place_order buyer BUY LIMIT 100 5 IOC >/dev/null

B_AVAIL=$(avail buyer USDT)
B_FROZEN=$(frozen buyer USDT)
B_BTC=$(avail buyer BTC)
echo "buyer USDT available=$B_AVAIL frozen=$B_FROZEN BTC=$B_BTC"
assert_eq "$B_AVAIL" "700" "IOC 部分成交后 USDT available 应为 700"
assert_eq "$B_FROZEN" "0" "IOC 部分成交后 USDT frozen 应为 0"
assert_eq "$B_BTC" "3" "IOC 应成交 3 BTC"

# ---------- 2. FOK 无法完全成交：全部解冻 ----------
echo -e "\n${YELLOW}--- Test 2: FOK 失败（流动性不足）---${NC}"
add_balance seller2 BTC 10
add_balance buyer2 USDT 1000

place_order seller2 SELL LIMIT 100 3 >/dev/null

# FOK 买单 5 BTC @100：无法完全成交，整单取消，解冻 500 USDT
place_order buyer2 BUY LIMIT 100 5 FOK >/dev/null

B2_AVAIL=$(avail buyer2 USDT)
B2_FROZEN=$(frozen buyer2 USDT)
B2_BTC=$(avail buyer2 BTC)
echo "buyer2 USDT available=$B2_AVAIL frozen=$B2_FROZEN BTC=$B2_BTC"
assert_eq "$B2_AVAIL" "1000" "FOK 失败后 USDT 应全额解冻"
assert_eq "$B2_FROZEN" "0" "FOK 失败后 frozen 应为 0"
assert_eq "$B2_BTC" "0" "FOK 失败不应成交"

# ---------- 3. FOK 完全成交：无残余解冻 ----------
echo -e "\n${YELLOW}--- Test 3: FOK 成功（流动性充足）---${NC}"
add_balance seller3 BTC 10
add_balance buyer3 USDT 1000

place_order seller3 SELL LIMIT 100 5 >/dev/null

place_order buyer3 BUY LIMIT 100 5 FOK >/dev/null

B3_AVAIL=$(avail buyer3 USDT)
B3_FROZEN=$(frozen buyer3 USDT)
B3_BTC=$(avail buyer3 BTC)
echo "buyer3 USDT available=$B3_AVAIL frozen=$B3_FROZEN BTC=$B3_BTC"
assert_eq "$B3_AVAIL" "500" "FOK 成功后 USDT available 应为 500"
assert_eq "$B3_FROZEN" "0" "FOK 成功后 frozen 应为 0"
assert_eq "$B3_BTC" "5" "FOK 应成交 5 BTC"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}   测试完成：$PASS PASS / $FAIL FAIL${NC}"
echo -e "${GREEN}========================================${NC}"

if [ $FAIL -gt 0 ]; then
    exit 1
fi
