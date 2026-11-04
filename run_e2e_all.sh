#!/usr/bin/env bash
# CEX 整体一键 E2E：
#   1. 功能 / 资金一致性 / IOC-FOK / 订单类型 / Outbox
#   2. Gateway 限流 + 反向代理
#   3. 300 并发压测 + 资金一致性
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

GATEWAY_MODE=""
LOCAL_API_PID=""
LOCAL_GATEWAY_PID=""
LOCAL_API_BIN="/tmp/cex-api-e2e"
LOCAL_GATEWAY_BIN="/tmp/cex-gateway-e2e"

echo "=========================================="
echo "   CEX 整体一键 E2E 测试"
echo "=========================================="

stop_local_gateway() {
    if [ -n "${LOCAL_GATEWAY_PID:-}" ]; then
        kill "$LOCAL_GATEWAY_PID" 2>/dev/null || true
        LOCAL_GATEWAY_PID=""
    fi
    if [ -n "${LOCAL_API_PID:-}" ]; then
        kill "$LOCAL_API_PID" 2>/dev/null || true
        LOCAL_API_PID=""
    fi
    pkill -9 -f "/tmp/cex-gateway-e2e" 2>/dev/null || true
    pkill -9 -f "/tmp/cex-api-e2e" 2>/dev/null || true
    if command -v lsof >/dev/null 2>&1; then
        lsof -ti tcp:8081 2>/dev/null | xargs -r kill -9 2>/dev/null || true
        lsof -ti tcp:8080 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    fi
}

cleanup_gateway() {
    if [ "$GATEWAY_MODE" = "docker" ]; then
        ./run_gateway.sh down >/dev/null 2>&1 || true
    elif [ "$GATEWAY_MODE" = "local" ]; then
        stop_local_gateway
    fi
    GATEWAY_MODE=""
}

cleanup() {
    echo ""
    echo "🧹 清理 E2E 进程..."
    cleanup_gateway
    rm -f "$LOCAL_API_BIN" "$LOCAL_GATEWAY_BIN"
}
trap cleanup EXIT

wait_http() {
    local url="$1"
    local name="$2"
    for i in $(seq 1 60); do
        if curl -s "$url" >/dev/null 2>&1; then
            echo "✅ ${name} 就绪"
            return 0
        fi
        sleep 1
    done
    echo "❌ ${name} 启动超时"
    return 1
}

start_local_gateway() {
    echo "🔧 使用本地进程启动 API + Gateway（Docker Gateway 不可用时降级）"

    pkill -9 -f "cmd/api/main.go" 2>/dev/null || true
    pkill -9 -f "cmd/gateway/main.go" 2>/dev/null || true
    pkill -9 -f "/exe/main" 2>/dev/null || true
    docker stop cex-api >/dev/null 2>&1 || true
    if command -v lsof >/dev/null 2>&1; then
        lsof -ti tcp:8080 2>/dev/null | xargs -r kill -9 2>/dev/null || true
        lsof -ti tcp:8081 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    fi
    sleep 1

    go build -o "$LOCAL_API_BIN" ./cmd/api
    go build -o "$LOCAL_GATEWAY_BIN" ./cmd/gateway

    MYSQL_DSN="root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local" \
    REDIS_ADDR="127.0.0.1:16379" \
    KAFKA_BROKERS="localhost:19092" \
    KAFKA_TOPIC="cex.order.events" \
    HTTP_PORT="8080" \
    CEX_ACCESS_LOG="0" \
    "$LOCAL_API_BIN" > /tmp/cex_e2e_api.log 2>&1 &
    LOCAL_API_PID=$!

    wait_http "http://localhost:8080/health" "API"

    BACKEND_URL="http://localhost:8080" \
    REDIS_ADDR="127.0.0.1:16379" \
    GATEWAY_PORT="8081" \
    RATE_LIMIT_RATE="50" \
    RATE_LIMIT_BURST="100" \
    RATE_LIMIT_FAIL_OPEN="false" \
    RATE_LIMIT_LOCAL_FALLBACK="true" \
    "$LOCAL_GATEWAY_BIN" > /tmp/cex_e2e_gateway.log 2>&1 &
    LOCAL_GATEWAY_PID=$!

    wait_http "http://localhost:8081/health" "Gateway"
}

run_gateway_phase() {
    echo ""
    echo "=========================================="
    echo "   [2/3] Gateway 限流 + 反向代理"
    echo "=========================================="

    if [ "${E2E_GATEWAY_MODE:-auto}" = "local" ]; then
        start_local_gateway
        GATEWAY_MODE="local"
    else
        if ./run_gateway.sh; then
            GATEWAY_MODE="docker"
        else
            echo ""
            echo "⚠️  Docker Gateway 启动失败，降级为本地进程模式"
            ./run_gateway.sh down >/dev/null 2>&1 || true
            start_local_gateway
            GATEWAY_MODE="local"
        fi
    fi

    bash test/test_gateway.sh

    cleanup_gateway
    echo "✅ Gateway 阶段通过"
}

echo ""
echo "=========================================="
echo "   [1/3] 功能 / 资金一致性 / Outbox"
echo "=========================================="
bash test_all.sh

run_gateway_phase

echo ""
echo "=========================================="
echo "   [3/3] 300 并发压测 + 资金一致性"
echo "=========================================="
./run_benchmark.sh

echo ""
echo "=========================================="
echo "   ✅ 整体 E2E 全部通过"
echo "   - 功能测试"
echo "   - Gateway 限流 / 代理"
echo "   - 300 并发压测 / 资金一致性"
echo "=========================================="