#!/bin/bash
set -euo pipefail

HTTP_PORT="${HTTP_PORT:-18080}"
BASE_URL="http://127.0.0.1:${HTTP_PORT}/api/v1"
MYSQL_DSN="${MYSQL_DSN:-root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local}"
REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:16379}"
REDIS_HOST="${REDIS_ADDR%%:*}"
REDIS_PORT="${REDIS_ADDR##*:}"
REDIS_DB="${REDIS_DB:-15}"
BIN="/tmp/cex-recovery-rebuild-test"
LOG="/tmp/cex-recovery-rebuild-test.log"
PIDS=()

TEST_NAMESPACE="recovery_rebuild_test_"
RUN_ID="$(date +%s)_$$"
USER_BTC="${TEST_NAMESPACE}btc_${RUN_ID}"
USER_ETH="${TEST_NAMESPACE}eth_${RUN_ID}"
AMOUNT=2
PRICE_BTC=""
PRICE_ETH=""

redis_cli() {
    if docker ps --format '{{.Names}}' | grep -qx cex-redis; then
        docker exec cex-redis redis-cli -n "$REDIS_DB" "$@"
    else
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" "$@"
    fi
}

mysql_exec() {
    local sql="$1"
    if docker ps --format '{{.Names}}' | grep -qx cex-mysql; then
        docker exec cex-mysql mysql -uroot -proot -N -e "USE cex; $sql"
    else
        mysql -u root -N -e "USE cex; $sql"
    fi
}

cleanup_test_data() {
    mysql_exec "
DELETE FROM outbox WHERE aggregate_id IN (
    SELECT order_id FROM orders WHERE user_id LIKE '${TEST_NAMESPACE}%'
);
DELETE FROM trades
 WHERE buy_order_id IN (SELECT order_id FROM orders WHERE user_id LIKE '${TEST_NAMESPACE}%')
    OR sell_order_id IN (SELECT order_id FROM orders WHERE user_id LIKE '${TEST_NAMESPACE}%');
DELETE FROM ledgers WHERE user_id LIKE '${TEST_NAMESPACE}%';
DELETE FROM orders WHERE user_id LIKE '${TEST_NAMESPACE}%';
DELETE FROM balances WHERE user_id LIKE '${TEST_NAMESPACE}%';
" >/dev/null 2>&1 || true
}

delete_redis_state() {
    redis_cli DEL \
        "orderbook:snapshot:BTC/USDT" "orderbook:wal:BTC/USDT" \
        "orderbook:snapshot:ETH/USDT" "orderbook:wal:ETH/USDT" >/dev/null 2>&1 || true
}

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    cleanup_test_data
    delete_redis_state
    rm -f "$BIN"
}
trap cleanup EXIT

wait_health() {
    for _ in $(seq 1 30); do
        if curl -sf "http://127.0.0.1:${HTTP_PORT}/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

start_api() {
    MYSQL_DSN="$MYSQL_DSN" REDIS_ADDR="$REDIS_ADDR" REDIS_DB="$REDIS_DB" HTTP_PORT="$HTTP_PORT" \
        "$BIN" >>"$LOG" 2>&1 &
    PIDS+=("$!")
}

stop_api() {
    local pid="${PIDS[${#PIDS[@]}-1]}"
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    unset 'PIDS[${#PIDS[@]}-1]'
}

# 选择一个当前订单簿中不存在、且不会立即成交的买价。
# 不使用随机数，重复运行时结果由当前深度决定。
choose_buy_price() {
    local symbol="$1"
    local base="$2"
    local depth
    depth=$(curl -sf "$BASE_URL/depth?symbol=$symbol&limit=1000")
    echo "$depth" | python3 -c '
import json, sys
depth = json.load(sys.stdin)
base = int(sys.argv[1])
bids = {x["price"] for x in depth["bids"]}
asks = {x["price"] for x in depth["asks"]}
best_ask = min(asks) if asks else 10**18
for price in range(base, base + 1000):
    if price not in bids and price not in asks and price < best_ask:
        print(price)
        break
else:
    raise SystemExit("no free non-crossing price")
' "$base"
}

go build -o "$BIN" ./cmd/api
: >"$LOG"

# 只清理本测试命名空间的历史残留，不触碰其他业务数据。
cleanup_test_data
delete_redis_state

start_api
if ! wait_health; then
    echo "API failed to start"
    cat "$LOG"
    exit 1
fi

curl -sf -X POST "$BASE_URL/balance/add" -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"$USER_BTC\",\"asset\":\"USDT\",\"amount\":1000000}" >/dev/null
curl -sf -X POST "$BASE_URL/balance/add" -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"$USER_ETH\",\"asset\":\"USDT\",\"amount\":1000000}" >/dev/null

PRICE_BTC=$(choose_buy_price "BTC/USDT" 1)
PRICE_ETH=$(choose_buy_price "ETH/USDT" 1001)

BTC_ORDER=$(curl -sf -X POST "$BASE_URL/orders" -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"$USER_BTC\",\"symbol\":\"BTC/USDT\",\"side\":\"BUY\",\"order_type\":\"LIMIT\",\"time_in_force\":\"GTC\",\"price\":$PRICE_BTC,\"amount\":$AMOUNT}")
ETH_ORDER=$(curl -sf -X POST "$BASE_URL/orders" -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"$USER_ETH\",\"symbol\":\"ETH/USDT\",\"side\":\"BUY\",\"order_type\":\"LIMIT\",\"time_in_force\":\"GTC\",\"price\":$PRICE_ETH,\"amount\":$AMOUNT}")

BTC_ID=$(echo "$BTC_ORDER" | python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])')
ETH_ID=$(echo "$ETH_ORDER" | python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])')

stop_api
redis_cli DEL \
    "orderbook:snapshot:BTC/USDT" "orderbook:wal:BTC/USDT" \
    "orderbook:snapshot:ETH/USDT" "orderbook:wal:ETH/USDT" >/dev/null

start_api
if ! wait_health; then
    echo "API failed to restart"
    cat "$LOG"
    exit 1
fi

BTC_DEPTH=$(curl -sf "$BASE_URL/depth?symbol=BTC/USDT&limit=200")
ETH_DEPTH=$(curl -sf "$BASE_URL/depth?symbol=ETH/USDT&limit=200")

if ! echo "$BTC_DEPTH" | python3 -c 'import json,sys; d=json.load(sys.stdin); p=int(sys.argv[1]); a=int(sys.argv[2]); q=int(sys.argv[3]); assert any(x["price"]==p and x["amount"]==a and x["order_count"]==1 for x in d["bids"]); assert not any(x["price"]==q for x in d["bids"])' "$PRICE_BTC" "$AMOUNT" "$PRICE_ETH"; then
    echo "BTC depth assertion failed: $BTC_DEPTH"
    exit 1
fi
if ! echo "$ETH_DEPTH" | python3 -c 'import json,sys; d=json.load(sys.stdin); p=int(sys.argv[1]); a=int(sys.argv[2]); q=int(sys.argv[3]); assert any(x["price"]==p and x["amount"]==a and x["order_count"]==1 for x in d["bids"]); assert not any(x["price"]==q for x in d["bids"])' "$PRICE_ETH" "$AMOUNT" "$PRICE_BTC"; then
    echo "ETH depth assertion failed: $ETH_DEPTH"
    exit 1
fi

curl -sf -X DELETE "$BASE_URL/orders/$BTC_ID?user_id=$USER_BTC" >/dev/null
BTC_DEPTH_AFTER=$(curl -sf "$BASE_URL/depth?symbol=BTC/USDT&limit=200")
if ! echo "$BTC_DEPTH_AFTER" | python3 -c 'import json,sys; d=json.load(sys.stdin); p=int(sys.argv[1]); assert not any(x["price"]==p for x in d["bids"])' "$PRICE_BTC"; then
    echo "recovered order was not removed by cancel: $BTC_DEPTH_AFTER"
    exit 1
fi

echo "PASS: rebuild is symbol-scoped, fields/depth are restored, and recovered orders can be cancelled"
