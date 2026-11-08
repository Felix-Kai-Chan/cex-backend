#!/bin/bash
set -euo pipefail

HTTP_PORT="${HTTP_PORT:-18084}"
BASE_URL="http://127.0.0.1:${HTTP_PORT}/api/v1"
MYSQL_DSN="${MYSQL_DSN:-root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local}"
REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:16379}"
REDIS_HOST="${REDIS_ADDR%%:*}"
REDIS_PORT="${REDIS_ADDR##*:}"
REDIS_DB="${REDIS_DB:-12}"
BIN="/tmp/cex-runtime-redis-failure-test"
LOG="/tmp/cex-runtime-redis-failure-test.log"
PIDS=()

TEST_NAMESPACE="runtime_redis_failure_test_"
RUN_ID="$(date +%s)_$$"
USER_ID="${TEST_NAMESPACE}${RUN_ID}"
SYMBOL="BTC/USDT"

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

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    mysql_exec "
DELETE FROM outbox WHERE aggregate_id IN (SELECT order_id FROM orders WHERE user_id LIKE '${TEST_NAMESPACE}%');
DELETE FROM trades WHERE buy_order_id IN (SELECT order_id FROM orders WHERE user_id LIKE '${TEST_NAMESPACE}%')
   OR sell_order_id IN (SELECT order_id FROM orders WHERE user_id LIKE '${TEST_NAMESPACE}%');
DELETE FROM ledgers WHERE user_id LIKE '${TEST_NAMESPACE}%';
DELETE FROM orders WHERE user_id LIKE '${TEST_NAMESPACE}%';
DELETE FROM balances WHERE user_id LIKE '${TEST_NAMESPACE}%';
" >/dev/null 2>&1 || true
    redis_cli DEL "orderbook:snapshot:${SYMBOL}" "orderbook:wal:${SYMBOL}" >/dev/null 2>&1 || true
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

create_order() {
    local price="$1"
    local body
    body=$(mktemp)
    local status
    status=$(curl -sS -o "$body" -w '%{http_code}' -X POST "$BASE_URL/orders" \
        -H 'Content-Type: application/json' \
        -d "{\"user_id\":\"$USER_ID\",\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"order_type\":\"LIMIT\",\"time_in_force\":\"GTC\",\"price\":$price,\"amount\":1}")
    echo "$status"
    cat "$body"
    rm -f "$body"
}

go build -o "$BIN" ./cmd/api
: >"$LOG"

redis_cli DEL "orderbook:snapshot:${SYMBOL}" "orderbook:wal:${SYMBOL}" >/dev/null
MYSQL_DSN="$MYSQL_DSN" REDIS_ADDR="$REDIS_ADDR" REDIS_DB="$REDIS_DB" HTTP_PORT="$HTTP_PORT" \
    "$BIN" >>"$LOG" 2>&1 &
PIDS+=("$!")

if ! wait_health; then
    echo "FAIL: API failed to start"
    cat "$LOG"
    exit 1
fi

curl -sf -X POST "$BASE_URL/balance/add" -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"$USER_ID\",\"asset\":\"USDT\",\"amount\":1000000}" >/dev/null

FIRST_STATUS=$(create_order 1 | head -n 1)
if [ "$FIRST_STATUS" != "200" ]; then
    echo "FAIL: first order status=$FIRST_STATUS, want 200"
    cat "$LOG"
    exit 1
fi

# 用错误类型覆盖 WAL key，模拟运行期间 Redis WAL 写入故障。
redis_cli SET "orderbook:wal:${SYMBOL}" "corrupted-wal-type" >/dev/null

SECOND_BODY=$(mktemp)
SECOND_STATUS=$(curl -sS -o "$SECOND_BODY" -w '%{http_code}' -X POST "$BASE_URL/orders" \
    -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"$USER_ID\",\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"order_type\":\"LIMIT\",\"time_in_force\":\"GTC\",\"price\":1,\"amount\":1}")
if [ "$SECOND_STATUS" = "200" ]; then
    echo "FAIL: order succeeded while WAL Redis write failed"
    cat "$SECOND_BODY"
    rm -f "$SECOND_BODY"
    exit 1
fi
if ! grep -q "WAL 持久化失败" "$SECOND_BODY"; then
    echo "FAIL: second order did not report WAL persistence failure"
    cat "$SECOND_BODY"
    rm -f "$SECOND_BODY"
    exit 1
fi
rm -f "$SECOND_BODY"

# 模拟 Redis 连接/存储恢复；但一致性恢复尚未完成，交易必须继续被拒绝。
redis_cli DEL "orderbook:wal:${SYMBOL}" >/dev/null

THIRD_BODY=$(mktemp)
THIRD_STATUS=$(curl -sS -o "$THIRD_BODY" -w '%{http_code}' -X POST "$BASE_URL/orders" \
    -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"$USER_ID\",\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"order_type\":\"LIMIT\",\"time_in_force\":\"GTC\",\"price\":2,\"amount\":1}")
if [ "$THIRD_STATUS" = "200" ]; then
    echo "FAIL: order succeeded after Redis recovered without consistency recovery"
    cat "$THIRD_BODY"
    rm -f "$THIRD_BODY"
    exit 1
fi
if ! grep -q "已暂停交易" "$THIRD_BODY"; then
    echo "FAIL: third order did not report symbol paused"
    cat "$THIRD_BODY"
    rm -f "$THIRD_BODY"
    exit 1
fi
rm -f "$THIRD_BODY"

echo "PASS: runtime WAL failure rejects trade and Redis recovery does not auto-clear protection"
