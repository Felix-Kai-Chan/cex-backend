#!/bin/bash
set -euo pipefail

MYSQL_DSN="${MYSQL_DSN:-root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local}"
REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:16379}"
REDIS_HOST="${REDIS_ADDR%%:*}"
REDIS_PORT="${REDIS_ADDR##*:}"
REDIS_DB="${REDIS_DB:-14}"
BIN="/tmp/cex-recovery-fail-fast-test"
LOG="/tmp/cex-recovery-fail-fast-test.log"

redis_cli() {
    if docker ps --format '{{.Names}}' | grep -qx cex-redis; then
        docker exec cex-redis redis-cli -n "$REDIS_DB" "$@"
    else
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" "$@"
    fi
}

cleanup() {
    rm -f "$BIN"
    redis_cli DEL \
        "orderbook:snapshot:BTC/USDT" "orderbook:wal:BTC/USDT" \
        "orderbook:snapshot:ETH/USDT" "orderbook:wal:ETH/USDT" >/dev/null 2>&1 || true
}
trap cleanup EXIT

go build -o "$BIN" ./cmd/api

run_expect_exit() {
    local name="$1"
    local port="$2"
    local redis_addr="$3"
    : >"$LOG"
    MYSQL_DSN="$MYSQL_DSN" REDIS_ADDR="$redis_addr" REDIS_DB="$REDIS_DB" HTTP_PORT="$port" \
        "$BIN" >"$LOG" 2>&1 &
    local pid=$!
    local exited=0
    for _ in $(seq 1 15); do
        if ! kill -0 "$pid" 2>/dev/null; then
            exited=1
            break
        fi
        sleep 1
    done

    if [ "$exited" -eq 0 ]; then
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        echo "FAIL: $name; API did not exit"
        cat "$LOG"
        return 1
    fi

    local status=0
    if wait "$pid"; then
        status=0
    else
        status=$?
    fi
    if [ "$status" -eq 0 ]; then
        echo "FAIL: $name; API exited with code 0"
        cat "$LOG"
        return 1
    fi
    if curl -sf "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
        echo "FAIL: $name; API is still serving"
        return 1
    fi
}

redis_cli SET "orderbook:snapshot:BTC/USDT" '{invalid-json' >/dev/null
INVALID_BEFORE=$(redis_cli GET "orderbook:snapshot:BTC/USDT")
run_expect_exit "invalid snapshot" 18081 "$REDIS_ADDR"
INVALID_AFTER=$(redis_cli GET "orderbook:snapshot:BTC/USDT")
if [ "$INVALID_BEFORE" != "$INVALID_AFTER" ]; then
    echo "FAIL: invalid snapshot was overwritten"
    exit 1
fi

run_expect_exit "redis unavailable" 18082 "127.0.0.1:1"

# 多 symbol 边界：BTC 正常（无 Snapshot/WAL，可从 MySQL 重建），ETH WAL 损坏。
# 任意 symbol 失败时整个 API 必须退出，不能部分恢复后继续服务。
redis_cli DEL \
    "orderbook:snapshot:BTC/USDT" "orderbook:wal:BTC/USDT" \
    "orderbook:snapshot:ETH/USDT" "orderbook:wal:ETH/USDT" >/dev/null
redis_cli RPUSH "orderbook:wal:ETH/USDT" '{invalid-json' >/dev/null
run_expect_exit "multi-symbol recovery failure" 18083 "$REDIS_ADDR"
if ! grep -q "symbol=ETH/USDT" "$LOG"; then
    echo "FAIL: multi-symbol failure did not report ETH/USDT"
    cat "$LOG"
    exit 1
fi

echo "PASS: API refuses to start when snapshot/WAL recovery is unavailable"
